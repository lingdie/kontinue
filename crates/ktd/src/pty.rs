//! WebSocket PTY for `/v1/pty`. The gateway already forwards `Upgrade: websocket`.

#[cfg(target_os = "linux")]
use std::collections::{HashMap, VecDeque};
#[cfg(target_os = "linux")]
use std::fs::File;
#[cfg(target_os = "linux")]
use std::io::{self, Read, Write};
#[cfg(target_os = "linux")]
use std::net::{Shutdown, TcpStream};
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;
#[cfg(target_os = "linux")]
use std::process::{Command, Stdio};
#[cfg(target_os = "linux")]
use std::sync::{atomic::{AtomicBool, Ordering}, Arc, Condvar, Mutex, Weak};
#[cfg(target_os = "linux")]
use std::thread;
#[cfg(target_os = "linux")]
use std::time::Duration;

#[cfg(target_os = "linux")]
use super::KtdService;

#[cfg(any(target_os = "linux", test))]
const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

#[cfg(any(target_os = "linux", test))]
pub fn accept_key(client_key: &str) -> String {
    let mut material = Vec::with_capacity(client_key.len() + WS_GUID.len());
    material.extend_from_slice(client_key.as_bytes());
    material.extend_from_slice(WS_GUID.as_bytes());
    base64_encode(&sha1(&material))
}

#[cfg(target_os = "linux")]
const MAX_BACKLOG_BYTES: usize = 64 * 1024;

#[cfg(target_os = "linux")]
struct OutputBacklog {
    frames: VecDeque<Vec<u8>>,
    bytes: usize,
}

#[cfg(target_os = "linux")]
pub(crate) struct PtyProcess {
    pid: u32,
    master_write: Mutex<File>,
    client: Mutex<Option<Arc<Mutex<TcpStream>>>>,
    backlog: Mutex<OutputBacklog>,
    stop: AtomicBool,
    finished: AtomicBool,
    exit_code: Mutex<Option<i32>>,
    output_done: (Mutex<bool>, Condvar),
    svc: Weak<KtdService>,
}

#[cfg(target_os = "linux")]
impl PtyProcess {
    fn spawn(svc: &Arc<KtdService>, params: &HashMap<String, String>) -> io::Result<Arc<Self>> {
        let env = svc.tenant_spawn_env().map_err(io::Error::other)?;
        let argv = command_argv(params);
        let cols: u16 = params.get("cols").and_then(|v| v.parse().ok()).unwrap_or(80);
        let rows: u16 = params.get("rows").and_then(|v| v.parse().ok()).unwrap_or(24);

        let mut master: libc::c_int = -1;
        let mut slave: libc::c_int = -1;
        if unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null::<libc::termios>(),
                std::ptr::null::<libc::winsize>(),
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        let winsz = libc::winsize { ws_row: rows, ws_col: cols, ws_xpixel: 0, ws_ypixel: 0 };
        unsafe {
            libc::ioctl(master, libc::TIOCSWINSZ, &winsz);
            libc::fcntl(master, libc::F_SETFD, libc::FD_CLOEXEC);
        }

        let mut guard = match super::platform::begin_spawn_guard() {
            Ok(g) => g,
            Err(e) => {
                unsafe {
                    libc::close(master);
                    libc::close(slave);
                }
                return Err(io::Error::other(e));
            }
        };
        let child_fd = guard.child_fd();
        let mut cmd = Command::new(&argv[0]);
        if argv.len() > 1 {
            cmd.args(&argv[1..]);
        }
        cmd.current_dir(params.get("cwd").map(String::as_str).unwrap_or("/root"));
        for (k, v) in &env {
            cmd.env(k, v);
        }
        if let Some(raw_envs) = params.get("envs") {
            if let Ok(envs) = serde_json::from_str::<HashMap<String, String>>(raw_envs) {
                for (k, v) in envs {
                    cmd.env(k, v);
                }
            }
        }
        cmd.env_remove("KTD_BOOTSTRAP_TOKEN");
        cmd.env_remove("KTD_INITIAL_ACCESS_TOKEN");
        let tty_fd = unsafe {
            let s_in = libc::dup(slave);
            let s_out = libc::dup(slave);
            let s_err = libc::dup(slave);
            libc::close(slave);
            cmd.stdin(Stdio::from_raw_fd(s_in));
            cmd.stdout(Stdio::from_raw_fd(s_out));
            cmd.stderr(Stdio::from_raw_fd(s_err));
            s_in
        };
        unsafe {
            cmd.pre_exec(move || {
                if libc::setsid() < 0 {
                    return Err(io::Error::last_os_error());
                }
                let _ = libc::ioctl(tty_fd, libc::TIOCSCTTY, 1);
                super::platform::finish_spawn_in_child(child_fd).map_err(io::Error::other)
            });
        }

        let master_write = unsafe { File::from_raw_fd(master) };
        let master_read = master_write.try_clone()?;
        svc.enter_foreground_wait();
        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => {
                svc.leave_foreground_wait();
                return Err(e);
            }
        };
        drop(cmd);
        let parent_fd = guard.take_parent_fd();
        if parent_fd >= 0 {
            thread::spawn(move || super::platform::supervise_spawn_guard(parent_fd));
        }

        let session = Arc::new(Self {
            pid: child.id(),
            master_write: Mutex::new(master_write),
            client: Mutex::new(None),
            backlog: Mutex::new(OutputBacklog { frames: VecDeque::new(), bytes: 0 }),
            stop: AtomicBool::new(false),
            finished: AtomicBool::new(false),
            exit_code: Mutex::new(None),
            output_done: (Mutex::new(false), Condvar::new()),
            svc: Arc::downgrade(svc),
        });
        svc.insert_pty_session(Arc::clone(&session));

        let output_session = Arc::clone(&session);
        thread::spawn(move || output_session.output_loop(master_read));
        let wait_session = Arc::clone(&session);
        thread::spawn(move || {
            let exit_code = child.wait().ok().and_then(|status| status.code()).unwrap_or(-1);
            wait_session.wait_for_output();
            wait_session.finish(exit_code);
        });
        Ok(session)
    }

    pub(crate) fn pid(&self) -> u32 {
        self.pid
    }

    fn output_loop(self: Arc<Self>, mut master_read: File) {
        let mut fds = [libc::pollfd { fd: master_read.as_raw_fd(), events: libc::POLLIN, revents: 0 }];
        let mut buf = [0u8; 4096];
        loop {
            if self.stop.load(Ordering::Acquire) {
                break;
            }
            let rc = unsafe { libc::poll(fds.as_mut_ptr(), 1, 100) };
            if rc < 0 {
                if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                break;
            }
            if rc == 0 {
                continue;
            }
            match master_read.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => self.broadcast_output(&buf[..n]),
            }
        }
        if let Ok((mut done, cv)) = self.output_done.0.lock().map(|done| (done, &self.output_done.1)) {
            *done = true;
            cv.notify_all();
        }
    }

    fn wait_for_output(&self) {
        let Ok(mut done) = self.output_done.0.lock() else { return };
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !*done {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                self.stop.store(true, Ordering::Release);
                break;
            }
            let Ok((next, _)) = self.output_done.1.wait_timeout(done, remaining) else { return };
            done = next;
        }
    }

    fn broadcast_output(&self, data: &[u8]) {
        if let Ok(mut backlog) = self.backlog.lock() {
            let frame = data.to_vec();
            backlog.bytes += frame.len();
            backlog.frames.push_back(frame);
            while backlog.bytes > MAX_BACKLOG_BYTES {
                if let Some(old) = backlog.frames.pop_front() {
                    backlog.bytes = backlog.bytes.saturating_sub(old.len());
                }
            }
        }
        let writer = self.client.lock().ok().and_then(|client| client.clone());
        if let Some(writer) = writer {
            if let Ok(mut stream) = writer.lock() {
                if write_frame(&mut *stream, 0x1, data).is_err() {
                    let _ = stream.shutdown(Shutdown::Both);
                }
            }
        }
    }

    fn attach(&self, stream: TcpStream) -> io::Result<Arc<Mutex<TcpStream>>> {
        let writer = Arc::new(Mutex::new(stream));
        let mut client = self.client.lock().map_err(|_| io::Error::other("PTY client lock poisoned"))?;
        if let Some(previous) = client.replace(Arc::clone(&writer)) {
            if let Ok(previous) = previous.lock() {
                let _ = previous.shutdown(Shutdown::Both);
            }
        }
        let mut stream = writer.lock().map_err(|_| io::Error::other("PTY writer lock poisoned"))?;
        let ready = serde_json::json!({"type": "ready", "pid": self.pid}).to_string();
        write_frame(&mut *stream, 0x1, ready.as_bytes())?;
        if let Ok(backlog) = self.backlog.lock() {
            for frame in &backlog.frames {
                write_frame(&mut *stream, 0x1, frame)?;
            }
        }
        if self.finished.load(Ordering::Acquire) {
            let exit_code = self.exit_code.lock().ok().and_then(|code| *code).unwrap_or(-1);
            let exit = serde_json::json!({"type": "exit", "exitCode": exit_code}).to_string();
            write_frame(&mut *stream, 0x1, exit.as_bytes())?;
            let _ = write_frame(&mut *stream, 0x8, &[]);
        }
        drop(stream);
        Ok(writer)
    }

    fn detach(&self, writer: &Arc<Mutex<TcpStream>>) {
        if let Ok(mut client) = self.client.lock() {
            if client.as_ref().is_some_and(|current| Arc::ptr_eq(current, writer)) {
                if let Some(current) = client.take() {
                    if let Ok(current) = current.lock() {
                        let _ = current.shutdown(Shutdown::Both);
                    }
                }
            }
        }
    }

    fn handle_payload(&self, payload: &[u8]) -> io::Result<()> {
        if kill_message(payload) {
            self.kill();
            return Ok(());
        }
        if let Some((cols, rows)) = resize_message(payload) {
            let master = self.master_write.lock().map_err(|_| io::Error::other("PTY master lock poisoned"))?;
            let winsz = libc::winsize { ws_row: rows, ws_col: cols, ws_xpixel: 0, ws_ypixel: 0 };
            unsafe {
                libc::ioctl(master.as_raw_fd(), libc::TIOCSWINSZ, &winsz);
            }
            return Ok(());
        }
        let mut master = self.master_write.lock().map_err(|_| io::Error::other("PTY master lock poisoned"))?;
        master.write_all(payload)
    }

    fn kill(&self) {
        self.stop.store(true, Ordering::Release);
        unsafe {
            let _ = libc::kill(-(self.pid as libc::pid_t), libc::SIGKILL);
            let _ = libc::kill(self.pid as libc::pid_t, libc::SIGKILL);
        }
    }

    fn finish(&self, exit_code: i32) {
        if self.finished.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Ok(mut code) = self.exit_code.lock() {
            *code = Some(exit_code);
        }
        if let Ok(client) = self.client.lock() {
            if let Some(writer) = client.as_ref() {
                if let Ok(mut stream) = writer.lock() {
                    let exit = serde_json::json!({"type": "exit", "exitCode": exit_code}).to_string();
                    let _ = write_frame(&mut *stream, 0x1, exit.as_bytes());
                    let _ = write_frame(&mut *stream, 0x8, &[]);
                    let _ = stream.shutdown(Shutdown::Write);
                }
            }
        }
        if let Some(svc) = self.svc.upgrade() {
            svc.leave_foreground_wait();
            svc.remove_pty_session(self.pid);
        }
    }
}

#[cfg(target_os = "linux")]
pub fn serve(
    mut stream: TcpStream,
    prebuffered: &[u8],
    svc: Arc<KtdService>,
    headers: &HashMap<String, String>,
    query: &str,
) -> io::Result<()> {
    use std::io::{BufReader, Cursor};

    let key = headers.get("sec-websocket-key").map(String::as_str).unwrap_or("");
    if key.is_empty() {
        return write_http(&mut stream, 400, br#"{"error":"missing Sec-WebSocket-Key"}"#);
    }
    let params = parse_query(query);
    let session = if let Some(raw_pid) = params.get("pid") {
        let pid = match raw_pid.parse::<u32>() {
            Ok(pid) => pid,
            Err(_) => return write_http(&mut stream, 400, br#"{"error":"invalid PTY pid"}"#),
        };
        match svc.pty_session(pid) {
            Some(session) => session,
            None => return write_http(&mut stream, 404, br#"{"error":"PTY session not found"}"#),
        }
    } else {
        match PtyProcess::spawn(&svc, &params) {
            Ok(session) => session,
            Err(error) => {
                let body = format!(r#"{{"error":"{}"}}"#, error.to_string().replace('"', "'"));
                return write_http(&mut stream, 503, body.as_bytes());
            }
        }
    };
    let accept = accept_key(key);
    let selected_protocol = select_protocol(headers);
    let protocol_header = selected_protocol
        .as_deref()
        .map(|value| format!("Sec-WebSocket-Protocol: {value}\r\n"))
        .unwrap_or_default();
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n{protocol_header}\r\n"
    );
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    let _ = stream.set_read_timeout(None);
    let _ = stream.set_write_timeout(None);

    let writer = session.attach(stream.try_clone()?)?;
    let mut reader = BufReader::new(Cursor::new(prebuffered.to_vec()).chain(stream.try_clone()?));
    loop {
        match read_frame(&mut reader) {
            Ok(Some((0x8, _))) | Ok(None) => break,
            Ok(Some((0x9, payload))) => {
                if let Ok(mut w) = writer.lock() {
                    let _ = write_frame(&mut *w, 0xA, &payload);
                }
            }
            Ok(Some((0x1 | 0x2, payload))) => {
                if session.handle_payload(&payload).is_err() {
                    break;
                }
            }
            Ok(Some(_)) => {}
            Err(_) => break,
        }
    }
    let _ = stream.shutdown(Shutdown::Both);
    session.detach(&writer);
    Ok(())
}

#[cfg(target_os = "linux")]
fn shell_argv(command: &str) -> Vec<String> {
    if command.trim().is_empty() || command == "/bin/sh" {
        return vec!["/bin/sh".to_string()];
    }
    vec!["/bin/sh".to_string(), "-c".to_string(), command.to_string()]
}

#[cfg(target_os = "linux")]
fn command_argv(params: &HashMap<String, String>) -> Vec<String> {
    if let Some(cmd) = params.get("cmd") {
        let mut argv = vec![cmd.clone()];
        if let Some(raw_args) = params.get("args") {
            if let Ok(args) = serde_json::from_str::<Vec<String>>(raw_args) {
                argv.extend(args);
            }
        }
        return argv;
    }
    shell_argv(params.get("command").map(String::as_str).unwrap_or("/bin/sh"))
}

#[cfg(target_os = "linux")]
fn resize_message(payload: &[u8]) -> Option<(u16, u16)> {
    let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
    if value.get("type")?.as_str()? != "resize" {
        return None;
    }
    let cols = value.get("cols")?.as_u64()?.try_into().ok()?;
    let rows = value.get("rows")?.as_u64()?.try_into().ok()?;
    Some((cols, rows))
}

#[cfg(target_os = "linux")]
fn kill_message(payload: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(payload)
        .ok()
        .and_then(|value| value.get("type").and_then(serde_json::Value::as_str).map(|kind| kind == "kill"))
        .unwrap_or(false)
}

#[cfg(target_os = "linux")]
fn select_protocol(headers: &HashMap<String, String>) -> Option<String> {
    let offered = headers.get("sec-websocket-protocol")?;
    offered.split(',').map(str::trim).find(|value| *value == "kontinue").map(str::to_string)
}

#[cfg(target_os = "linux")]
fn parse_query(query: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        out.insert(percent_decode(k), percent_decode(v));
    }
    out
}

#[cfg(target_os = "linux")]
fn percent_decode(raw: &str) -> String {
    let bytes = raw.replace('+', " ").into_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(v) = u8::from_str_radix(std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""), 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(target_os = "linux")]
fn read_frame(reader: &mut impl std::io::BufRead) -> io::Result<Option<(u8, Vec<u8>)>> {
    let mut hdr = [0u8; 2];
    if reader.read_exact(&mut hdr).is_err() {
        return Ok(None);
    }
    let opcode = hdr[0] & 0x0f;
    let masked = hdr[1] & 0x80 != 0;
    let mut len = (hdr[1] & 0x7f) as u64;
    if len == 126 {
        let mut ext = [0u8; 2];
        reader.read_exact(&mut ext)?;
        len = u16::from_be_bytes(ext) as u64;
    } else if len == 127 {
        let mut ext = [0u8; 8];
        reader.read_exact(&mut ext)?;
        len = u64::from_be_bytes(ext);
    }
    if len > 1_048_576 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "websocket frame too large"));
    }
    let mut mask = [0u8; 4];
    if masked {
        reader.read_exact(&mut mask)?;
    }
    let mut data = vec![0u8; len as usize];
    if len > 0 {
        reader.read_exact(&mut data)?;
    }
    if masked {
        for (i, b) in data.iter_mut().enumerate() {
            *b ^= mask[i % 4];
        }
    }
    Ok(Some((opcode, data)))
}

#[cfg(target_os = "linux")]
fn write_frame(writer: &mut impl Write, opcode: u8, data: &[u8]) -> io::Result<()> {
    let mut hdr = Vec::with_capacity(10);
    hdr.push(0x80 | opcode);
    if data.len() < 126 {
        hdr.push(data.len() as u8);
    } else if data.len() < 65536 {
        hdr.push(126);
        hdr.extend_from_slice(&(data.len() as u16).to_be_bytes());
    } else {
        hdr.push(127);
        hdr.extend_from_slice(&(data.len() as u64).to_be_bytes());
    }
    writer.write_all(&hdr)?;
    writer.write_all(data)?;
    writer.flush()
}

#[cfg(target_os = "linux")]
fn write_http(stream: &mut TcpStream, code: u16, body: &[u8]) -> io::Result<()> {
    let reason = match code {
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        503 => "Service Unavailable",
        _ => "Error",
    };
    let head = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}

#[cfg(any(target_os = "linux", test))]
fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h = [
        0x6745_2301u32,
        0xEFCD_AB89,
        0x98BA_DCFE,
        0x1032_5476,
        0xC3D2_E1F0,
    ];
    let bit_len = (data.len() as u64).saturating_mul(8);
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in msg.chunks(64) {
        let mut w = [0u32; 80];
        for i in 0..16 {
            w[i] = u32::from_be_bytes(chunk[i * 4..i * 4 + 4].try_into().unwrap());
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let (mut a, mut b, mut c, mut d, mut e) = (h[0], h[1], h[2], h[3], h[4]);
        for i in 0..80 {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A82_7999),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let temp = a.rotate_left(5).wrapping_add(f).wrapping_add(e).wrapping_add(k).wrapping_add(w[i]);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
    }
    let mut out = [0u8; 20];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

#[cfg(any(target_os = "linux", test))]
fn base64_encode(data: &[u8]) -> String {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    let mut i = 0;
    while i + 3 <= data.len() {
        let n = ((data[i] as u32) << 16) | ((data[i + 1] as u32) << 8) | data[i + 2] as u32;
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        out.push(TABLE[((n >> 6) & 63) as usize] as char);
        out.push(TABLE[(n & 63) as usize] as char);
        i += 3;
    }
    if i < data.len() {
        let remain = data.len() - i;
        let mut n = (data[i] as u32) << 16;
        if remain == 2 {
            n |= (data[i + 1] as u32) << 8;
        }
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        if remain == 2 {
            out.push(TABLE[((n >> 6) & 63) as usize] as char);
            out.push('=');
        } else {
            out.push('=');
            out.push('=');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "linux")]
    use crate::KtdConfig;
    #[cfg(target_os = "linux")]
    use std::io::{Read, Write};
    #[cfg(target_os = "linux")]
    use std::net::{TcpListener, TcpStream};
    #[cfg(target_os = "linux")]
    use std::sync::Arc;
    #[cfg(target_os = "linux")]
    use std::time::Duration;

    #[test]
    fn rfc6455_accept_key() {
        assert_eq!(accept_key("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }

    #[cfg(target_os = "linux")]
    fn connect_pty(port: u16, target: &str) -> TcpStream {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let request = format!(
            "GET {target} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: kontinue\r\nX-Kontinue-Token: token\r\n\r\n"
        );
        stream.write_all(request.as_bytes()).unwrap();
        let mut response = Vec::new();
        let mut byte = [0u8; 1];
        while !response.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).unwrap();
            response.push(byte[0]);
        }
        assert!(String::from_utf8_lossy(&response).starts_with("HTTP/1.1 101"));
        stream
    }

    #[cfg(target_os = "linux")]
    fn read_server_frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
        let mut header = [0u8; 2];
        stream.read_exact(&mut header).unwrap();
        let opcode = header[0] & 0x0f;
        let mut length = (header[1] & 0x7f) as usize;
        if length == 126 {
            let mut extended = [0u8; 2];
            stream.read_exact(&mut extended).unwrap();
            length = u16::from_be_bytes(extended) as usize;
        } else if length == 127 {
            let mut extended = [0u8; 8];
            stream.read_exact(&mut extended).unwrap();
            length = u64::from_be_bytes(extended) as usize;
        }
        let mut payload = vec![0u8; length];
        stream.read_exact(&mut payload).unwrap();
        (opcode, payload)
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reconnects_pty_and_reports_natural_exit_code() {
        let svc = KtdService::new(KtdConfig {
            version: "test".into(),
            sandbox_id: "sandbox".into(),
            bootstrap_token: "bootstrap".into(),
            initial_access_token: "token".into(),
            machine_id_path: String::new(),
            workload_cgroup_dir: String::new(),
            control_port: 0,
        });
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server_svc = Arc::clone(&svc);
        std::thread::spawn(move || super::super::serve_http(listener, server_svc));

        let mut first = connect_pty(
            port,
            "/v1/pty?command=printf%20first%3B%20sleep%202%3B%20exit%207&cols=80&rows=24",
        );
        let (_, ready) = read_server_frame(&mut first);
        let ready: serde_json::Value = serde_json::from_slice(&ready).unwrap();
        let pid = ready["pid"].as_u64().unwrap();
        drop(first);

        let mut second = connect_pty(port, &format!("/v1/pty?pid={pid}"));
        let (_, ready) = read_server_frame(&mut second);
        let ready: serde_json::Value = serde_json::from_slice(&ready).unwrap();
        assert_eq!(ready["pid"].as_u64(), Some(pid));
        loop {
            let (opcode, payload) = read_server_frame(&mut second);
            if opcode != 1 {
                continue;
            }
            let Ok(message) = serde_json::from_slice::<serde_json::Value>(&payload) else {
                continue;
            };
            if message["type"] == "exit" {
                assert_eq!(message["exitCode"].as_i64(), Some(7));
                break;
            }
        }
    }
}
