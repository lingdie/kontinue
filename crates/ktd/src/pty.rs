//! WebSocket PTY for `/v1/pty`. The gateway already forwards `Upgrade: websocket`.

#[cfg(target_os = "linux")]
use std::collections::HashMap;
#[cfg(target_os = "linux")]
use std::io::{self, Read, Write};
#[cfg(target_os = "linux")]
use std::net::TcpStream;
#[cfg(target_os = "linux")]
use std::sync::{Arc, Mutex};

#[cfg(target_os = "linux")]
use super::KtdService;

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

pub fn accept_key(client_key: &str) -> String {
    let mut material = Vec::with_capacity(client_key.len() + WS_GUID.len());
    material.extend_from_slice(client_key.as_bytes());
    material.extend_from_slice(WS_GUID.as_bytes());
    base64_encode(&sha1(&material))
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
    use std::os::fd::FromRawFd;
    use std::process::{Command, Stdio};
    use std::thread;

    let key = headers.get("sec-websocket-key").map(String::as_str).unwrap_or("");
    if key.is_empty() {
        return write_http(&mut stream, 400, br#"{"error":"missing Sec-WebSocket-Key"}"#);
    }
    let env = match svc.tenant_spawn_env() {
        Ok(env) => env,
        Err(msg) => {
            let body = format!(r#"{{"error":"{}"}}"#, msg.replace('"', "'"));
            return write_http(&mut stream, 503, body.as_bytes());
        }
    };
    let accept = accept_key(key);
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    let _ = stream.set_read_timeout(None);
    let _ = stream.set_write_timeout(None);

    let params = parse_query(query);
    let cmdline = params.get("command").map(String::as_str).unwrap_or("/bin/sh");
    let argv = shell_argv(cmdline);
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
            return Err(io::Error::new(io::ErrorKind::Other, e));
        }
    };
    let child_fd = guard.child_fd();
    let mut cmd = Command::new(&argv[0]);
    if argv.len() > 1 {
        cmd.args(&argv[1..]);
    }
    cmd.current_dir("/root");
    for (k, v) in &env {
        cmd.env(k, v);
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
        use std::os::unix::process::CommandExt;
        cmd.pre_exec(move || {
            if libc::setsid() < 0 {
                return Err(io::Error::last_os_error());
            }
            let _ = libc::ioctl(tty_fd, libc::TIOCSCTTY, 1);
            super::platform::finish_spawn_in_child(child_fd).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
        });
    }

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => {
            unsafe { libc::close(master) };
            return Err(e);
        }
    };
    let parent_fd = guard.take_parent_fd();
    if parent_fd >= 0 {
        thread::spawn(move || super::platform::supervise_spawn_guard(parent_fd));
    }
    svc.enter_foreground_wait();
    let master_file = unsafe { std::fs::File::from_raw_fd(master) };
    let mut master_read = master_file.try_clone()?;
    let mut master_write = master_file;
    let writer = Arc::new(Mutex::new(stream.try_clone()?));
    let writer_bg = Arc::clone(&writer);
    let output = thread::spawn(move || {
        let mut buf = [0u8; 4096];
        loop {
            match master_read.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if let Ok(mut w) = writer_bg.lock() {
                        if write_frame(&mut *w, 0x1, &buf[..n]).is_err() {
                            break;
                        }
                    }
                }
            }
        }
    });

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
                if master_write.write_all(&payload).is_err() {
                    break;
                }
            }
            Ok(Some(_)) => {}
            Err(_) => break,
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    svc.leave_foreground_wait();
    let _ = output.join();
    drop(master_write);
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

    #[test]
    fn rfc6455_accept_key() {
        assert_eq!(accept_key("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }
}
