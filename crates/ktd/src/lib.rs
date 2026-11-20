//! Core state machine, process executor, and zero-GC HTTP/1.1 server for `ktd`
//! (supporting explicit `Expect: 100-continue` held requests, W3C `traceparent` spans, and `/debug/traces`).

pub mod platform;
mod pty;

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime};

use kt_protocol::{
    constant_time_eq, format_rfc3339_utc, generate_span_id, now_unix_nanos, ExecRequest,
    ExecResponse, FileWriteRequest, FlightRecorder, FreezeRequest, FreezeResponse, HealthResponse,
    InitPayload, InitResponse, ProcessInfo, RecordedSpan, TraceContext, HEADER_BOOTSTRAP_TOKEN,
    HEADER_KONTINUE_TOKEN, HEADER_TRACEPARENT, HEADER_TRACE_ID,
};

#[derive(Clone)]
pub struct KtdConfig {
    pub version: String,
    pub sandbox_id: String,
    pub bootstrap_token: String,
    pub initial_access_token: String,
    pub machine_id_path: String,
    pub workload_cgroup_dir: String,
    pub control_port: u16,
}

struct InnerState {
    sandbox_id: String,
    execution_id: i64,
    lifecycle_id: String,
    bootstrap_token: String,
    access_token: String,
    env_vars: BTreeMap<String, String>,
    frozen: bool,
    initialized: bool,
    in_flight_spawns: usize,
    active_fg_waits: usize,
    clock_skew_nano: i64,
    procs: HashMap<u32, ProcessInfo>,
}

pub struct KtdService {
    cfg: KtdConfig,
    started_at: Instant,
    state: Mutex<InnerState>,
    spawn_cv: Condvar,
    recorder: FlightRecorder,
}

impl KtdService {
    pub fn new(cfg: KtdConfig) -> Arc<Self> {
        Arc::new(Self {
            started_at: Instant::now(),
            recorder: FlightRecorder::new("ktd", 2048),
            state: Mutex::new(InnerState {
                sandbox_id: cfg.sandbox_id.clone(),
                execution_id: 0,
                lifecycle_id: String::new(),
                bootstrap_token: cfg.bootstrap_token.clone(),
                access_token: cfg.initial_access_token.clone(),
                env_vars: BTreeMap::new(),
                frozen: false,
                initialized: false,
                in_flight_spawns: 0,
                active_fg_waits: 0,
                clock_skew_nano: 0,
                procs: HashMap::new(),
            }),
            spawn_cv: Condvar::new(),
            cfg,
        })
    }

    pub fn start_pid1_reaper(self: &Arc<Self>) {
        if std::process::id() != 1 {
            return;
        }
        let svc = Arc::clone(self);
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(500));
            if let Ok(g) = svc.state.lock() {
                // Only reap reparented orphan grandchildren when no foreground `exec` is waiting on its child PID,
                // preventing `waitpid(-1)` from stealing exit status (`ECHILD`) from `wait_with_output()`!
                if g.active_fg_waits == 0 && g.in_flight_spawns == 0 {
                    platform::reap_orphans_once();
                }
            }
        });
    }

    pub fn recorder(&self) -> &FlightRecorder {
        &self.recorder
    }

    pub fn verify_bootstrap(&self, token: &str) -> bool {
        let Ok(g) = self.state.lock() else {
            return false;
        };
        constant_time_eq(&g.bootstrap_token, token)
    }

    pub fn verify_access(&self, token: &str) -> bool {
        let Ok(g) = self.state.lock() else {
            return false;
        };
        constant_time_eq(&g.access_token, token)
    }

    pub fn tenant_spawn_env(&self) -> Result<BTreeMap<String, String>, String> {
        let g = self.state.lock().map_err(|_| "poisoned lock".to_string())?;
        if g.frozen {
            return Err("sandbox workload cgroup is frozen for checkpointing".to_string());
        }
        let mut env = g.env_vars.clone();
        if !g.sandbox_id.is_empty() {
            env.insert("SANDBOX_ID".to_string(), g.sandbox_id.clone());
        }
        Ok(env)
    }

    pub fn enter_foreground_wait(&self) {
        if let Ok(mut g) = self.state.lock() {
            g.active_fg_waits += 1;
        }
    }

    pub fn leave_foreground_wait(&self) {
        if let Ok(mut g) = self.state.lock() {
            g.active_fg_waits = g.active_fg_waits.saturating_sub(1);
        }
    }

    pub fn current_sandbox_id(&self) -> String {
        self.state
            .lock()
            .map(|g| g.sandbox_id.clone())
            .unwrap_or_default()
    }

    pub fn health(&self) -> HealthResponse {
        let Ok(g) = self.state.lock() else {
            return HealthResponse {
                healthy: false,
                version: self.cfg.version.clone(),
                sandbox_id: String::new(),
                execution_id: 0,
                frozen: false,
                initialized: false,
                uptime_millis: 0,
                clock_skew_nano: 0,
            };
        };
        HealthResponse {
            healthy: true,
            version: self.cfg.version.clone(),
            sandbox_id: g.sandbox_id.clone(),
            execution_id: g.execution_id,
            frozen: g.frozen,
            initialized: g.initialized,
            uptime_millis: self.started_at.elapsed().as_millis() as i64,
            clock_skew_nano: g.clock_skew_nano,
        }
    }

    pub fn freeze(&self, req: FreezeRequest) -> Result<FreezeResponse, String> {
        let start = Instant::now();
        let wait_ms = if req.wait_in_flight_timeout > 0 {
            req.wait_in_flight_timeout as u64
        } else {
            3_000
        };
        let timeout = Duration::from_millis(wait_ms);

        let active_procs = {
            let mut g = self.state.lock().map_err(|_| "poisoned state lock")?;
            g.frozen = true;
            let deadline = Instant::now() + timeout;
            while g.in_flight_spawns > 0 {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                let (next_g, _) = self
                    .spawn_cv
                    .wait_timeout(g, deadline - now)
                    .map_err(|_| "condvar wait error")?;
                g = next_g;
            }
            g.procs.values().filter(|p| p.running).count()
        };

        let closed_conns = if req.drain_external_tcp {
            platform::close_external_established_tcp(self.cfg.control_port)
        } else {
            0
        };
        if req.sync_filesystems {
            platform::sync_filesystems();
        }
        platform::set_cgroup_freeze(&self.cfg.workload_cgroup_dir, true)?;

        Ok(FreezeResponse {
            frozen: true,
            synced: req.sync_filesystems,
            closed_external_conns: closed_conns,
            active_managed_procs: active_procs,
            quiesce_duration_micro: start.elapsed().as_micros() as i64,
        })
    }

    pub fn thaw(&self) -> Result<(), String> {
        {
            let mut g = self.state.lock().map_err(|_| "poisoned state lock")?;
            g.frozen = false;
        }
        platform::set_cgroup_freeze(&self.cfg.workload_cgroup_dir, false)
    }

    pub fn apply_init(&self, payload: InitPayload) -> Result<InitResponse, (u16, String)> {
        let local_now = now_unix_nanos();
        {
            let mut g = self
                .state
                .lock()
                .map_err(|_| (500u16, "poisoned state lock".to_string()))?;
            if payload.execution_id > 0 && payload.execution_id < g.execution_id {
                return Err((
                    409,
                    format!(
                        "stale executionId {} < current {}",
                        payload.execution_id, g.execution_id
                    ),
                ));
            }
            if !payload.sandbox_id.is_empty() {
                g.sandbox_id = payload.sandbox_id.clone();
            }
            if payload.execution_id > 0 {
                g.execution_id = payload.execution_id;
            }
            if !payload.lifecycle_id.is_empty() {
                g.lifecycle_id = payload.lifecycle_id.clone();
            }
            if !payload.access_token.is_empty() {
                g.access_token = payload.access_token.clone();
            }
            for (k, v) in payload.env_vars {
                g.env_vars.insert(k, v);
            }
            if payload.wall_clock_unix_n > 0 {
                g.clock_skew_nano = payload.wall_clock_unix_n - local_now;
            }
            g.initialized = true;
            if !payload.skip_thaw {
                g.frozen = false;
            }
        }

        let seed = if payload.entropy_seed.is_empty() {
            format!("{}-{}", payload.sandbox_id, local_now).into_bytes()
        } else {
            payload.entropy_seed
        };
        let _ = platform::mix_kernel_entropy(&seed);

        let machine_id = if payload.machine_id.is_empty() {
            format!("{:032x}", local_now as u128 ^ 0xa5a5_5a5a_f0f0_0f0f)
        } else {
            payload.machine_id
        };
        let mut machine_id_updated = false;
        if !self.cfg.machine_id_path.is_empty() {
            let p = Path::new(&self.cfg.machine_id_path);
            if let Some(parent) = p.parent() {
                let _ = fs::create_dir_all(parent);
            }
            if fs::write(p, format!("{}\n", machine_id)).is_ok() {
                machine_id_updated = true;
            }
        }

        let hostname_updated = if !payload.hostname.is_empty() {
            platform::set_system_hostname(&payload.hostname).is_ok()
        } else {
            false
        };

        let thawed = if !payload.skip_thaw {
            platform::set_cgroup_freeze(&self.cfg.workload_cgroup_dir, false).is_ok()
        } else {
            false
        };

        let delta_ms = if payload.wall_clock_unix_n > 0 {
            (payload.wall_clock_unix_n - local_now) / 1_000_000
        } else {
            0
        };

        Ok(InitResponse {
            sandbox_id: payload.sandbox_id,
            execution_id: payload.execution_id,
            clock_delta_millis: delta_ms,
            thawed,
            machine_id_updated,
            hostname_updated,
        })
    }

    pub fn list_processes(&self) -> Vec<ProcessInfo> {
        self.state
            .lock()
            .map(|g| g.procs.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn exec(self: &Arc<Self>, req: ExecRequest) -> Result<ExecResponse, (u16, String)> {
        if req.command.is_empty() {
            return Err((400, "command cannot be empty".to_string()));
        }

        let base_env = {
            let mut g = self
                .state
                .lock()
                .map_err(|_| (500, "poisoned lock".to_string()))?;
            if g.frozen {
                return Err((
                    503,
                    "sandbox workload cgroup is frozen for checkpointing".to_string(),
                ));
            }
            g.in_flight_spawns += 1;
            let mut m = g.env_vars.clone();
            if !g.sandbox_id.is_empty() {
                m.insert("SANDBOX_ID".to_string(), g.sandbox_id.clone());
            }
            m
        };

        let mut cmd = Command::new(&req.command[0]);
        if req.command.len() > 1 {
            cmd.args(&req.command[1..]);
        }
        if !req.working_dir.is_empty() {
            cmd.current_dir(&req.working_dir);
        }
        for (k, v) in &base_env {
            cmd.env(k, v);
        }
        for (k, v) in &req.env {
            cmd.env(k, v);
        }
        cmd.env_remove("KTD_BOOTSTRAP_TOKEN");
        cmd.env_remove("KTD_INITIAL_ACCESS_TOKEN");
        if !req.stdin.is_empty() {
            cmd.stdin(Stdio::piped());
        } else {
            cmd.stdin(Stdio::null());
        }
		if req.background {
            cmd.stdout(Stdio::null()).stderr(Stdio::null());
        } else {
            cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        }
        // Tenant processes must not inherit CAP_SYS_PTRACE or a user namespace.
        // The drop and seccomp filter run after fork, in the child.
        let mut spawn_guard = match platform::begin_spawn_guard() {
            Ok(guard) => guard,
            Err(e) => {
                if let Ok(mut g) = self.state.lock() {
                    g.in_flight_spawns = g.in_flight_spawns.saturating_sub(1);
                    if g.in_flight_spawns == 0 {
                        self.spawn_cv.notify_all();
                    }
                }
                return Err((500, e));
            }
        };
        let child_fd = spawn_guard.child_fd();
        #[cfg(unix)]
        unsafe {
            use std::os::unix::process::CommandExt;
            cmd.pre_exec(move || {
                platform::finish_spawn_in_child(child_fd)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            });
        }

        let start = Instant::now();
        let started_unix_ms = (now_unix_nanos() / 1_000_000).max(0);
        let spawn_res = cmd.spawn();
        if spawn_res.is_ok() {
            let parent_fd = spawn_guard.take_parent_fd();
            if parent_fd >= 0 {
                std::thread::spawn(move || platform::supervise_spawn_guard(parent_fd));
            }
        }

        // Always release spawn gate immediately after fork/exec returns.
        {
            if let Ok(mut g) = self.state.lock() {
                g.in_flight_spawns = g.in_flight_spawns.saturating_sub(1);
                if g.in_flight_spawns == 0 {
                    self.spawn_cv.notify_all();
                }
            }
        }

        let mut child = spawn_res.map_err(|e| (500, format!("spawn {:?}: {}", req.command[0], e)))?;
        let pid = child.id();

        if !req.stdin.is_empty() {
            if let Some(mut sin) = child.stdin.take() {
                let _ = sin.write_all(req.stdin.as_bytes());
            }
        }

        {
            if let Ok(mut g) = self.state.lock() {
                g.procs.insert(
                    pid,
                    ProcessInfo {
                        pid,
                        command: req.command.clone(),
                        started_at_unix_ms: started_unix_ms,
                        running: true,
                        exit_code: 0,
                    },
                );
            }
        }

        if req.background {
            let svc = Arc::clone(self);
            std::thread::spawn(move || {
                let code = child
                    .wait()
                    .ok()
                    .and_then(|s| s.code())
                    .unwrap_or(-1);
                if let Ok(mut g) = svc.state.lock() {
                    if let Some(info) = g.procs.get_mut(&pid) {
                        info.running = false;
                        info.exit_code = code;
                    }
                }
            });
            return Ok(ExecResponse {
                pid,
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
                duration_ms: start.elapsed().as_millis() as i64,
                timed_out: false,
            });
        }

        if let Ok(mut g) = self.state.lock() {
            g.active_fg_waits += 1;
        }

        let mut stdout_bytes = Vec::new();
        let mut stderr_bytes = Vec::new();
        if let Some(mut out_pipe) = child.stdout.take() {
            let _ = out_pipe.read_to_end(&mut stdout_bytes);
        }
        if let Some(mut err_pipe) = child.stderr.take() {
            let _ = err_pipe.read_to_end(&mut stderr_bytes);
        }

        let mut timed_out = false;
        if req.timeout_ms > 0 {
            let deadline = Instant::now() + Duration::from_millis(req.timeout_ms as u64);
            loop {
                match child.try_wait() {
                    Ok(Some(_)) => break,
                    Ok(None) => {
                        if Instant::now() >= deadline {
                            let _ = child.kill();
                            timed_out = true;
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        }

        let wait_res = child.wait();
        if let Ok(mut g) = self.state.lock() {
            g.active_fg_waits = g.active_fg_waits.saturating_sub(1);
        }

        let exit_code = if timed_out {
            124
        } else {
            match wait_res {
                Ok(st) => st.code().unwrap_or(0),
                // Graceful fallback if PID-1 reaper already collected the exited child (`ECHILD` = 10).
                Err(e) if e.raw_os_error() == Some(10) => 0,
                Err(e) => return Err((500, format!("wait child {}: {}", pid, e))),
            }
        };

        if let Ok(mut g) = self.state.lock() {
            if let Some(info) = g.procs.get_mut(&pid) {
                info.running = false;
                info.exit_code = exit_code;
            }
        }

        Ok(ExecResponse {
            pid,
            exit_code,
            stdout: String::from_utf8_lossy(&stdout_bytes).into_owned(),
            stderr: String::from_utf8_lossy(&stderr_bytes).into_owned(),
            duration_ms: start.elapsed().as_millis() as i64,
            timed_out,
        })
    }
}

/// Starts `ktd`'s multithreaded HTTP/1.1 server on the bound `TcpListener`.
pub fn serve_http(listener: TcpListener, svc: Arc<KtdService>) {
    for incoming in listener.incoming() {
        let Ok(stream) = incoming else {
            continue;
        };
        let svc_clone = Arc::clone(&svc);
        std::thread::spawn(move || {
            let _ = handle_connection(stream, svc_clone);
        });
    }
}

fn handle_connection(mut stream: TcpStream, svc: Arc<KtdService>) -> std::io::Result<()> {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
    let _ = stream.set_nodelay(true);

    loop {
        let mut reader = BufReader::new(&stream);
        let mut req_line = String::new();
        let n = reader.read_line(&mut req_line)?;
        if n == 0 {
            return Ok(());
        }
        let req_trimmed = req_line.trim();
        if req_trimmed.is_empty() {
            continue;
        }
        let parts: Vec<&str> = req_trimmed.split_whitespace().collect();
        if parts.len() < 2 {
            return write_json_resp(&mut stream, 400, "", br#"{"error":"bad request line"}"#, true);
        }
        let method = parts[0].to_string();
        let raw_target = parts[1].to_string();

        let mut headers = HashMap::new();
        loop {
            let mut line = String::new();
            let read_n = reader.read_line(&mut line)?;
            if read_n == 0 || line == "\r\n" || line == "\n" {
                break;
            }
            if let Some((k, v)) = line.split_once(':') {
                headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
            }
        }

        let expect_continue = headers
            .get("expect")
            .map(|v| v.eq_ignore_ascii_case("100-continue"))
            .unwrap_or(false);
        let (path, query) = match raw_target.split_once('?') {
            Some((p, q)) => (p.to_string(), q.to_string()),
            None => (raw_target.clone(), String::new()),
        };
        if method == "GET" && path == "/v1/pty" {
            let upgrade = headers
                .get("upgrade")
                .map(|v| v.eq_ignore_ascii_case("websocket"))
                .unwrap_or(false);
            if !upgrade {
                drop(reader);
                return write_json_resp(&mut stream, 426, "", br#"{"error":"websocket upgrade required"}"#, true);
            }
            let tok = headers.get(HEADER_KONTINUE_TOKEN).map(String::as_str).unwrap_or("");
            if !svc.verify_access(tok) {
                drop(reader);
                return write_json_resp(&mut stream, 401, "", br#"{"error":"invalid sandbox access token"}"#, true);
            }
            #[cfg(target_os = "linux")]
            {
                let buffered = reader.buffer().to_vec();
                drop(reader);
                let _ = stream.set_read_timeout(None);
                return pty::serve(stream, &buffered, svc, &headers, &query);
            }
            #[cfg(not(target_os = "linux"))]
            {
                drop(reader);
                return write_json_resp(&mut stream, 501, "", br#"{"error":"pty requires linux"}"#, true);
            }
        }

        let content_len: usize = headers
            .get("content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let close_conn = headers
            .get("connection")
            .map(|v| v.eq_ignore_ascii_case("close"))
            .unwrap_or(false);

        if content_len > 64 * 1024 * 1024 {
            drop(reader);
            return write_json_resp(&mut stream, 413, "", br#"{"error":"payload too large"}"#, true);
        }

        // E2B Held-Request protocol: flush 100 Continue right before streaming body!
        if expect_continue {
            (&stream).write_all(b"HTTP/1.1 100 Continue\r\n\r\n")?;
            (&stream).flush()?;
        }

        let mut body = vec![0u8; content_len];
        if content_len > 0 {
            reader.read_exact(&mut body)?;
        }
        drop(reader);

        if path == "/debug/traces" {
            let params = parse_query(&query);
            let errors_only = params
                .get("errors_only")
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false);
            let limit = params
                .get("limit")
                .and_then(|v| v.parse().ok())
                .unwrap_or(200);
            let resp = svc.recorder().query(
                params.get("trace_id").map(String::as_str),
                params.get("sandbox_id").map(String::as_str),
                errors_only,
                limit,
            );
            let raw = serde_json::to_vec(&resp).unwrap_or_default();
            write_json_resp(&mut stream, 200, "", &raw, close_conn)?;
            if close_conn {
                return Ok(());
            }
            continue;
        }

        let trace_ctx = headers
            .get(HEADER_TRACEPARENT)
            .and_then(|tp| TraceContext::parse_traceparent(tp))
            .unwrap_or_else(|| TraceContext::new_root(svc.recorder().next_counter()));
        let span_id = generate_span_id(svc.recorder().next_counter());
        let start_sys = SystemTime::now();
        let start_inst = Instant::now();

        let (status_code, content_type, resp_body, err_msg) =
            dispatch_route(&svc, &method, &path, &query, &headers, &body);

        let end_sys = SystemTime::now();
        let mut attrs = BTreeMap::new();
        attrs.insert("http.method".to_string(), method.clone());
        attrs.insert("http.path".to_string(), path.to_string());
        attrs.insert("http.status_code".to_string(), status_code.to_string());
        let sbx_id = svc.current_sandbox_id();
        if !sbx_id.is_empty() {
            attrs.insert("sandbox.id".to_string(), sbx_id);
        }
        svc.recorder().record(RecordedSpan {
            trace_id: trace_ctx.trace_id.clone(),
            span_id,
            parent_span_id: trace_ctx.parent_span_id,
            service: "ktd".to_string(),
            name: format!("{} {}", method, path),
            start_time: format_rfc3339_utc(start_sys),
            end_time: format_rfc3339_utc(end_sys),
            duration_ms: start_inst.elapsed().as_secs_f64() * 1000.0,
            status: if status_code >= 500 {
                "ERROR".to_string()
            } else {
                "OK".to_string()
            },
            error_message: err_msg,
            attributes: attrs,
        });

        write_http_resp(
            &mut stream,
            status_code,
            content_type,
            &trace_ctx.trace_id,
            &resp_body,
            close_conn,
        )?;
        if close_conn {
            return Ok(());
        }
    }
}

fn dispatch_route(
    svc: &Arc<KtdService>,
    method: &str,
    path: &str,
    query: &str,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> (u16, &'static str, Vec<u8>, String) {
    match (method, path) {
        ("GET", "/internal/health") => {
            let h = svc.health();
            let raw = serde_json::to_vec(&h).unwrap_or_default();
            (200, "application/json", raw, String::new())
        }
        ("POST", "/internal/freeze") => {
            let tok = headers.get(HEADER_BOOTSTRAP_TOKEN).map(String::as_str).unwrap_or("");
            if !svc.verify_bootstrap(tok) {
                return json_err(401, "invalid bootstrap token");
            }
            let req: FreezeRequest = if body.is_empty() {
                FreezeRequest::default()
            } else {
                match serde_json::from_slice(body) {
                    Ok(v) => v,
                    Err(e) => return json_err(400, &e.to_string()),
                }
            };
            match svc.freeze(req) {
                Ok(resp) => (
                    200,
                    "application/json",
                    serde_json::to_vec(&resp).unwrap_or_default(),
                    String::new(),
                ),
                Err(e) => json_err(500, &e),
            }
        }
        ("POST", "/internal/thaw") => {
            let tok = headers.get(HEADER_BOOTSTRAP_TOKEN).map(String::as_str).unwrap_or("");
            if !svc.verify_bootstrap(tok) {
                return json_err(401, "invalid bootstrap token");
            }
            match svc.thaw() {
                Ok(()) => (204, "application/json", Vec::new(), String::new()),
                Err(e) => json_err(500, &e),
            }
        }
        ("POST", "/internal/init") => {
            let tok = headers.get(HEADER_BOOTSTRAP_TOKEN).map(String::as_str).unwrap_or("");
            if !svc.verify_bootstrap(tok) {
                return json_err(401, "invalid bootstrap token");
            }
            let payload: InitPayload = match serde_json::from_slice(body) {
                Ok(v) => v,
                Err(e) => return json_err(400, &e.to_string()),
            };
            match svc.apply_init(payload) {
                Ok(resp) => (
                    200,
                    "application/json",
                    serde_json::to_vec(&resp).unwrap_or_default(),
                    String::new(),
                ),
                Err((code, msg)) => json_err(code, &msg),
            }
        }
        ("POST", "/v1/process/exec") | ("POST", "/v1/pty") => {
            let tok = headers.get(HEADER_KONTINUE_TOKEN).map(String::as_str).unwrap_or("");
            if !svc.verify_access(tok) {
                return json_err(401, "invalid sandbox access token");
            }
            let mut req: ExecRequest = match serde_json::from_slice(body) {
                Ok(v) => v,
                Err(e) => return json_err(400, &e.to_string()),
            };
            if path == "/v1/pty" && req.command.is_empty() {
                req.command = vec!["/bin/sh".to_string(), "-c".to_string(), "pwd".to_string()];
            }
            match svc.exec(req) {
                Ok(resp) => (
                    200,
                    "application/json",
                    serde_json::to_vec(&resp).unwrap_or_default(),
                    String::new(),
                ),
                Err((code, msg)) => json_err(code, &msg),
            }
        }
        ("GET", "/v1/process/list") => {
            let tok = headers.get(HEADER_KONTINUE_TOKEN).map(String::as_str).unwrap_or("");
            if !svc.verify_access(tok) {
                return json_err(401, "invalid sandbox access token");
            }
            let list = svc.list_processes();
            (
                200,
                "application/json",
                serde_json::to_vec(&list).unwrap_or_default(),
                String::new(),
            )
        }
        ("GET", "/v1/files") => {
            let tok = headers.get(HEADER_KONTINUE_TOKEN).map(String::as_str).unwrap_or("");
            if !svc.verify_access(tok) {
                return json_err(401, "invalid sandbox access token");
            }
            let params = parse_query(query);
            let Some(file_path) = params.get("path") else {
                return json_err(400, "missing path query parameter");
            };
            match fs::read(file_path) {
                Ok(bytes) => (200, "application/octet-stream", bytes, String::new()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => json_err(404, &e.to_string()),
                Err(e) => json_err(500, &e.to_string()),
            }
        }
        ("PUT", "/v1/files") | ("POST", "/v1/files") => {
            let tok = headers.get(HEADER_KONTINUE_TOKEN).map(String::as_str).unwrap_or("");
            if !svc.verify_access(tok) {
                return json_err(401, "invalid sandbox access token");
            }
            let req: FileWriteRequest = match serde_json::from_slice(body) {
                Ok(v) => v,
                Err(e) => return json_err(400, &e.to_string()),
            };
            if req.path.is_empty() {
                return json_err(400, "missing path");
            }
            let p = Path::new(&req.path);
            if let Some(parent) = p.parent() {
                if let Err(e) = fs::create_dir_all(parent) {
                    return json_err(500, &e.to_string());
                }
            }
            if let Err(e) = fs::write(p, &req.content) {
                return json_err(500, &e.to_string());
            }
            #[cfg(unix)]
            if req.mode != 0 {
                use std::os::unix::fs::PermissionsExt;
                let _ = fs::set_permissions(p, fs::Permissions::from_mode(req.mode));
            }
            (204, "application/json", Vec::new(), String::new())
        }
        _ => json_err(404, "route not found"),
    }
}

fn json_err(code: u16, msg: &str) -> (u16, &'static str, Vec<u8>, String) {
    let body = serde_json::to_vec(&serde_json::json!({ "error": msg })).unwrap_or_default();
    (code, "application/json", body, msg.to_string())
}

fn write_json_resp(
    stream: &mut TcpStream,
    code: u16,
    trace_id: &str,
    body: &[u8],
    close: bool,
) -> std::io::Result<()> {
    write_http_resp(stream, code, "application/json", trace_id, body, close)
}

fn write_http_resp(
    stream: &mut TcpStream,
    code: u16,
    content_type: &str,
    trace_id: &str,
    body: &[u8],
    close: bool,
) -> std::io::Result<()> {
    let reason = match code {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        409 => "Conflict",
        413 => "Payload Too Large",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Status",
    };
    let conn_hdr = if close { "close" } else { "keep-alive" };
    let mut hdr = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: {}\r\n",
        code,
        reason,
        content_type,
        body.len(),
        conn_hdr
    );
    if !trace_id.is_empty() {
        hdr.push_str(&format!("{}: {}\r\n", HEADER_TRACE_ID, trace_id));
    }
    hdr.push_str("\r\n");
    stream.write_all(hdr.as_bytes())?;
    if !body.is_empty() && code != 204 {
        stream.write_all(body)?;
    }
    stream.flush()
}

fn parse_query(q: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for pair in q.split('&') {
        if pair.is_empty() {
            continue;
        }
        if let Some((k, v)) = pair.split_once('=') {
            out.insert(percent_decode(k), percent_decode(v));
        }
    }
    out
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(val) = u8::from_str_radix(
                std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""),
                16,
            ) {
                out.push(val);
                i += 3;
                continue;
            }
        } else if bytes[i] == b'+' {
            out.push(b' ');
            i += 1;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_freeze_held_init_token_rotation_and_fencing() {
        let dir = std::env::temp_dir().join(format!("ktd-test-{}", now_unix_nanos()));
        let _ = fs::create_dir_all(&dir);
        let machine_id_file = dir.join("machine-id");
        let cgroup_dir = dir.join("cgroup");
        let _ = fs::create_dir_all(&cgroup_dir);
        let _ = fs::write(cgroup_dir.join("cgroup.freeze"), b"0\n");

        let svc = KtdService::new(KtdConfig {
            version: "0.1.0".to_string(),
            sandbox_id: "tpl-builder".to_string(),
            bootstrap_token: "boot-secret".to_string(),
            initial_access_token: "warmup-temp-token".to_string(),
            machine_id_path: machine_id_file.to_string_lossy().into_owned(),
            workload_cgroup_dir: cgroup_dir.to_string_lossy().into_owned(),
            control_port: 49983,
        });

        assert!(svc.verify_access("warmup-temp-token"));
        let res = svc
            .exec(ExecRequest {
                command: vec!["/bin/sh".into(), "-c".into(), "printf '%s' warm".into()],
                ..Default::default()
            })
            .expect("warmup exec");
        assert_eq!(res.stdout, "warm");

        // Freeze blocks new execs
        let f_res = svc
            .freeze(FreezeRequest {
                reason: "checkpoint".into(),
                drain_external_tcp: true,
                sync_filesystems: true,
                wait_in_flight_timeout: 500,
            })
            .expect("freeze");
        assert!(f_res.frozen);
        assert_eq!(
            fs::read_to_string(cgroup_dir.join("cgroup.freeze")).unwrap_or_default(),
            "1\n"
        );
        assert!(svc
            .exec(ExecRequest {
                command: vec!["/bin/sh".into(), "-c".into(), "true".into()],
                ..Default::default()
            })
            .is_err());

        // HeldInit rotates token, injects env secrets, updates machine-id, and thaws
        let mut env = BTreeMap::new();
        env.insert("TENANT_KEY".to_string(), "sk-live-999".to_string());
        let init_res = svc
            .apply_init(InitPayload {
                sandbox_id: "sbx-prod-1".into(),
                execution_id: 3,
                lifecycle_id: "life-1".into(),
                hostname: "sbx-prod-1".into(),
                machine_id: String::new(),
                access_token: "tenant-token-v3".into(),
                env_vars: env,
                entropy_seed: b"seed-bytes-12345".to_vec(),
                wall_clock_unix_n: now_unix_nanos() + 5_000_000_000,
                skip_thaw: false,
            })
            .expect("apply_init");
        assert!(init_res.thawed);
        assert!(init_res.machine_id_updated);
        assert!(!svc.verify_access("warmup-temp-token"));
        assert!(svc.verify_access("tenant-token-v3"));

        let live_exec = svc
            .exec(ExecRequest {
                command: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    "printf '%s|%s' \"$SANDBOX_ID\" \"$TENANT_KEY\"".into(),
                ],
                ..Default::default()
            })
            .expect("post-restore exec");
        assert_eq!(live_exec.stdout, "sbx-prod-1|sk-live-999");

        // Stale ExecutionID (< 3) must be rejected with 409
        let stale = svc.apply_init(InitPayload {
            sandbox_id: "sbx-prod-1".into(),
            execution_id: 2,
            access_token: "hijack".into(),
            ..Default::default()
        });
        assert_eq!(stale.expect_err("stale rejected").0, 409);

        let _ = fs::remove_dir_all(&dir);
    }
}
