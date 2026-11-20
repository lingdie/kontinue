//! Sub-millisecond Rust OCI wrapper (`kontinue-runsc`) for gVisor (`RuntimeClass: kontinue-gvisor`).

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Instant, SystemTime};

use kt_protocol::{
    format_rfc3339_utc, generate_span_id, RecordedSpan, TraceContext, ANNOTATION_RESTORE_DIRECT,
    ANNOTATION_RESTORE_HOST_PATH,
};
use serde::{Deserialize, Serialize};

const WATCH_RESTORE_FLAG: &str = "--kontinue-watch-restore";

const GLOBAL_VALUE_FLAGS: &[&str] = &[
    "--root",
    "--log",
    "--log-format",
    "--debug-log",
    "--network",
    "--overlay2",
    "--platform",
    "--pod-init-config",
    "--PodInitConfig",
    "--watchdog-action",
    "--panic-signal",
    "--profile",
    "--trace",
    "--file-access",
    "--host-uds",
];

const KNOWN_SUBCOMMANDS: &[&str] = &[
    "create",
    "start",
    "restore",
    "run",
    "checkpoint",
    "pause",
    "resume",
    "kill",
    "delete",
    "state",
    "events",
    "ps",
    "exec",
    "wait",
    "boot",
    "gofer",
    "tar",
    "spec",
    "list",
];

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct BundleRecord {
    container_id: String,
    bundle_path: String,
    restore_host_path: String,
    direct_io: bool,
    traceparent: String,
}

#[derive(Debug, Clone)]
pub struct RewritePlan {
    pub final_args: Vec<String>,
    pub subcommand: String,
    pub watch_container_id: Option<String>,
    pub watch_view_path: Option<String>,
    pub traceparent: Option<String>,
}

pub fn rewrite_args(raw_args: &[String], state_dir: &Path, default_platform: &str) -> RewritePlan {
    let (global_flags, sub, sub_args) = split_invocation(raw_args);
    if sub.is_empty() {
        return RewritePlan {
            final_args: raw_args.to_vec(),
            subcommand: String::new(),
            watch_container_id: None,
            watch_view_path: None,
            traceparent: None,
        };
    }

    let mut globals = global_flags;
    if !has_flag(&globals, "--platform") && !default_platform.is_empty() {
        globals.push(format!("--platform={}", default_platform));
    }
    if !has_flag(&globals, "--overlay2") {
        globals.push("--overlay2=root:self".to_string());
    }
    if !has_flag(&globals, "--network") {
        let net_mode = std::env::var("KONTINUE_GVISOR_NETWORK").unwrap_or_else(|_| "host".to_string());
        globals.push(format!("--network={}", net_mode));
    }

    let mut effective_sub = sub;
    let mut effective_sub_args = sub_args;
    let mut watch_cid = None;
    let mut watch_view = None;
    let mut found_tp = None;

    match effective_sub.as_str() {
        "create" => {
            let bundle = flag_value(&effective_sub_args, "--bundle")
                .or_else(|| flag_value(&effective_sub_args, "-b"));
            let cid = last_positional(&effective_sub_args);
            if let (Some(b), Some(c)) = (bundle, cid) {
                let rec = inspect_bundle(&c, Path::new(&b));
                if !rec.traceparent.is_empty() {
                    found_tp = Some(rec.traceparent.clone());
                }
                let _ = save_record(state_dir, &rec);
            }
        }
        "start" => {
            if let Some(cid) = last_positional(&effective_sub_args) {
                if let Some(rec) = load_record(state_dir, &cid) {
                    if !rec.traceparent.is_empty() {
                        found_tp = Some(rec.traceparent);
                    }
                    if !rec.restore_host_path.is_empty() {
                        effective_sub = "restore".to_string();
                        let direct = if rec.direct_io {
                            "--direct-io=on"
                        } else {
                            "--direct-io=off"
                        };
                        effective_sub_args = vec![
                            format!("--image-path={}", rec.restore_host_path),
                            "--background=true".to_string(),
                            direct.to_string(),
                            "--detach".to_string(),
                            cid.clone(),
                        ];
                        watch_cid = Some(cid);
                        watch_view = Some(rec.restore_host_path);
                    }
                }
            }
        }
        "restore" => {
            let cid = last_positional(&effective_sub_args);
            let img = flag_value(&effective_sub_args, "--image-path");
            if !has_flag(&effective_sub_args, "--background") {
                effective_sub_args.insert(0, "--background=true".to_string());
            }
            if !has_flag(&effective_sub_args, "--direct-io")
                && !has_flag(&effective_sub_args, "--direct")
            {
                let mut direct = "--direct-io=off";
                if let Some(ref c) = cid {
                    if let Some(rec) = load_record(state_dir, c) {
                        if rec.direct_io {
                            direct = "--direct-io=on";
                        }
                        if !rec.traceparent.is_empty() {
                            found_tp = Some(rec.traceparent);
                        }
                    }
                }
                effective_sub_args.insert(0, direct.to_string());
            }
            watch_cid = cid;
            watch_view = img;
        }
        "checkpoint" => {
            if !has_flag(&effective_sub_args, "--compression") {
                effective_sub_args.insert(0, "--compression=none".to_string());
            }
        }
        "delete" => {
            if let Some(cid) = last_positional(&effective_sub_args) {
                let _ = fs::remove_file(record_path(state_dir, &cid));
            }
        }
        _ => {}
    }

    let mut final_args = Vec::with_capacity(globals.len() + 1 + effective_sub_args.len());
    final_args.extend(globals);
    final_args.push(effective_sub.clone());
    final_args.extend(effective_sub_args);

    RewritePlan {
        final_args,
        subcommand: effective_sub,
        watch_container_id: watch_cid,
        watch_view_path: watch_view,
        traceparent: found_tp,
    }
}

fn split_invocation(args: &[String]) -> (Vec<String>, String, Vec<String>) {
    let mut i = 0;
    while i < args.len() {
        let tok = args[i].as_str();
        if KNOWN_SUBCOMMANDS.contains(&tok) {
            return (
                args[..i].to_vec(),
                tok.to_string(),
                args[i + 1..].to_vec(),
            );
        }
        if GLOBAL_VALUE_FLAGS.contains(&tok) && i + 1 < args.len() {
            i += 2;
            continue;
        }
        i += 1;
    }
    (args.to_vec(), String::new(), Vec::new())
}

fn has_flag(args: &[String], prefix: &str) -> bool {
    let with_eq = format!("{}=", prefix);
    args.iter()
        .any(|a| a == prefix || a.starts_with(&with_eq))
}

fn flag_value(args: &[String], flag_name: &str) -> Option<String> {
    let with_eq = format!("{}=", flag_name);
    let mut i = 0;
    while i < args.len() {
        if args[i] == flag_name && i + 1 < args.len() {
            return Some(args[i + 1].clone());
        }
        if let Some(rest) = args[i].strip_prefix(&with_eq) {
            return Some(rest.to_string());
        }
        i += 1;
    }
    None
}

fn last_positional(args: &[String]) -> Option<String> {
    args.iter().rev().find(|a| !a.starts_with('-')).cloned()
}

fn inspect_bundle(cid: &str, bundle: &Path) -> BundleRecord {
    let mut rec = BundleRecord {
        container_id: cid.to_string(),
        bundle_path: bundle.to_string_lossy().into_owned(),
        ..Default::default()
    };
    let cfg_path = bundle.join("config.json");
    let Ok(raw) = fs::read(&cfg_path) else {
        return rec;
    };
    let Ok(mut doc) = serde_json::from_slice::<serde_json::Value>(&raw) else {
        return rec;
    };
    let mut mutated = false;
    if let Some(ann) = doc.get_mut("annotations").and_then(|v| v.as_object_mut()) {
        if let Some(p) = ann.get(ANNOTATION_RESTORE_HOST_PATH).and_then(|v| v.as_str()) {
            rec.restore_host_path = p.to_string();
        }
        if ann.get(ANNOTATION_RESTORE_DIRECT).and_then(|v| v.as_str()) == Some("true") {
            rec.direct_io = true;
        }
        if let Some(tp) = ann.get("kontinue.sh/traceparent").and_then(|v| v.as_str()) {
            rec.traceparent = tp.to_string();
        }
        // Strip control-plane annotations before `runsc create` so `runsc restore` spec validation
        // sees identical workload annotations between checkpoint and restore!
        let before_len = ann.len();
        ann.retain(|k, _| !k.starts_with("dev.gvisor.") && !k.starts_with("kontinue.sh/"));
        if ann.len() != before_len {
            mutated = true;
        }
    }
    if mutated {
        if let Ok(cleaned) = serde_json::to_vec_pretty(&doc) {
            let _ = fs::write(&cfg_path, cleaned);
        }
    }
    rec
}

fn record_path(state_dir: &Path, cid: &str) -> PathBuf {
    let safe = Path::new(cid)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown");
    state_dir.join(format!("{}.json", safe))
}

fn save_record(state_dir: &Path, rec: &BundleRecord) -> std::io::Result<()> {
    fs::create_dir_all(state_dir)?;
    let raw = serde_json::to_vec(rec)?;
    fs::write(record_path(state_dir, &rec.container_id), raw)
}

fn load_record(state_dir: &Path, cid: &str) -> Option<BundleRecord> {
    let raw = fs::read(record_path(state_dir, cid)).ok()?;
    serde_json::from_slice(&raw).ok()
}

fn post_unix_json(sock_path: &str, http_path: &str, body: &[u8]) {
    if sock_path.is_empty() || !Path::new(sock_path).exists() {
        return;
    }
    let Ok(mut stream) = UnixStream::connect(sock_path) else {
        return;
    };
    let _ = stream.set_write_timeout(Some(std::time::Duration::from_secs(3)));
    let req = format!(
        "POST {} HTTP/1.1\r\nHost: unix\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        http_path,
        body.len()
    );
    let _ = stream.write_all(req.as_bytes());
    let _ = stream.write_all(body);
    let _ = stream.flush();
    let mut sink = [0u8; 256];
    let _ = stream.read(&mut sink);
}

fn main() {
    let real_runsc =
        std::env::var("KONTINUE_REAL_RUNSC").unwrap_or_else(|_| "/usr/local/bin/runsc".to_string());
    let state_dir = PathBuf::from(
        std::env::var("KONTINUE_RUNSC_STATE_DIR")
            .unwrap_or_else(|_| "/run/kontinue-runsc".to_string()),
    );
    let arena_sock = std::env::var("KONTINUE_ARENA_SOCK")
        .unwrap_or_else(|_| "/run/kontinue/arena-hooks.sock".to_string());
    let default_platform =
        std::env::var("KONTINUE_GVISOR_PLATFORM").unwrap_or_else(|_| "systrap".to_string());

    let raw_args: Vec<String> = std::env::args().skip(1).collect();
    if raw_args.len() >= 3 && raw_args[0] == WATCH_RESTORE_FLAG {
        let cid = &raw_args[1];
        let view_path = &raw_args[2];
        let root_flags = &raw_args[3..];

        let start_sys = SystemTime::now();
        let start_inst = Instant::now();
        let mut wait_args = root_flags.to_vec();
        wait_args.extend(["wait".to_string(), "--restore".to_string(), cid.clone()]);
        let status = Command::new(&real_runsc).args(&wait_args).status();

        let payload = serde_json::to_vec(&serde_json::json!({
            "containerId": cid,
            "viewPath": view_path,
        }))
        .unwrap_or_default();
        post_unix_json(&arena_sock, "/v1/arena/release-view", &payload);

        if let Some(tctx) = TraceContext::from_env() {
            let span = RecordedSpan {
                trace_id: tctx.trace_id,
                span_id: generate_span_id(99),
                parent_span_id: tctx.parent_span_id,
                service: "kontinue-runsc".to_string(),
                name: "runsc.wait_restore_complete".to_string(),
                start_time: format_rfc3339_utc(start_sys),
                end_time: format_rfc3339_utc(SystemTime::now()),
                duration_ms: start_inst.elapsed().as_secs_f64() * 1000.0,
                status: if status.map(|s| s.success()).unwrap_or(false) {
                    "OK".to_string()
                } else {
                    "ERROR".to_string()
                },
                error_message: String::new(),
                attributes: BTreeMap::new(),
            };
            if let Ok(raw) = serde_json::to_vec(&vec![span]) {
                post_unix_json(&arena_sock, "/debug/traces", &raw);
            }
        }
        return;
    }

    let plan = rewrite_args(&raw_args, &state_dir, &default_platform);
    let trace_ctx = TraceContext::from_env().or_else(|| {
        plan.traceparent
            .as_deref()
            .and_then(TraceContext::parse_traceparent)
    });

    let start_sys = SystemTime::now();
    let start_inst = Instant::now();
    let status = Command::new(&real_runsc).args(&plan.final_args).status();
    let exit_code = match status {
        Ok(s) => s.code().unwrap_or(1),
        Err(e) => {
            eprintln!("kontinue-runsc exec {} failed: {}", real_runsc, e);
            1
        }
    };

    if let Some(ref tctx) = trace_ctx {
        if matches!(plan.subcommand.as_str(), "restore" | "checkpoint") {
            let span_id = generate_span_id(1);
            let span = RecordedSpan {
                trace_id: tctx.trace_id.clone(),
                span_id: span_id.clone(),
                parent_span_id: tctx.parent_span_id.clone(),
                service: "kontinue-runsc".to_string(),
                name: format!("runsc.{}", plan.subcommand),
                start_time: format_rfc3339_utc(start_sys),
                end_time: format_rfc3339_utc(SystemTime::now()),
                duration_ms: start_inst.elapsed().as_secs_f64() * 1000.0,
                status: if exit_code == 0 {
                    "OK".to_string()
                } else {
                    "ERROR".to_string()
                },
                error_message: String::new(),
                attributes: BTreeMap::new(),
            };
            if let Ok(raw) = serde_json::to_vec(&vec![span]) {
                post_unix_json(&arena_sock, "/debug/traces", &raw);
            }
        }
    }

    if exit_code == 0 {
        if let (Some(cid), Some(view), Ok(self_bin)) = (
            plan.watch_container_id,
            plan.watch_view_path,
            std::env::current_exe(),
        ) {
            let mut bg = Command::new(self_bin);
            bg.stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .arg(WATCH_RESTORE_FLAG)
                .arg(cid)
                .arg(view);
            if let Some(root) = flag_value(&plan.final_args, "--root") {
                bg.arg("--root").arg(root);
            }
            if let Some(tctx) = trace_ctx {
                bg.env(
                    "TRACEPARENT",
                    TraceContext::format_traceparent(&tctx.trace_id, &generate_span_id(2)),
                );
            }
            let _ = bg.spawn();
        }
    }

    std::process::exit(exit_code);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_create_start_and_checkpoint() {
        let tmp = std::env::temp_dir().join("kontinue-runsc-test");
        let _ = fs::create_dir_all(&tmp);
        let state = tmp.join("state");
        let bundle = tmp.join("bundle");
        let _ = fs::create_dir_all(&bundle);

        let spec = serde_json::json!({
            "ociVersion": "1.0.2",
            "annotations": {
                "dev.gvisor.internal.restore.host-image-path": "/var/lib/kontinue-arena/views/v1",
                "dev.gvisor.internal.restore.direct": "false",
                "kontinue.sh/traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
            }
        });
        fs::write(bundle.join("config.json"), serde_json::to_vec(&spec).unwrap()).unwrap();

        let create_plan = rewrite_args(
            &[
                "--root".into(),
                "/run/containerd/runsc".into(),
                "create".into(),
                "--bundle".into(),
                bundle.to_string_lossy().into_owned(),
                "ctr-99".into(),
            ],
            &state,
            "systrap",
        );
        assert!(create_plan.final_args.contains(&"--platform=systrap".to_string()));
        assert!(create_plan.final_args.contains(&"--overlay2=root:self".to_string()));
        assert!(create_plan.final_args.contains(&"--network=host".to_string()));

        let start_plan = rewrite_args(
            &["--root".into(), "/run/containerd/runsc".into(), "start".into(), "ctr-99".into()],
            &state,
            "systrap",
        );
        assert_eq!(start_plan.subcommand, "restore");
        assert!(start_plan
            .final_args
            .contains(&"--image-path=/var/lib/kontinue-arena/views/v1".to_string()));
        assert!(start_plan.final_args.contains(&"--background=true".to_string()));
        assert!(start_plan.final_args.contains(&"--direct-io=off".to_string()));
        assert_eq!(start_plan.watch_container_id.as_deref(), Some("ctr-99"));

        let ckpt_plan = rewrite_args(
            &["checkpoint".into(), "--image-path=/tmp/x".into(), "ctr-99".into()],
            &state,
            "systrap",
        );
        assert!(ckpt_plan.final_args.contains(&"--compression=none".to_string()));

        let _ = fs::remove_dir_all(&tmp);
    }
}
