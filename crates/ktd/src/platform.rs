//! Platform-isolated OS operations for `ktd` (Linux syscalls vs portable dev/macOS fallbacks).

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

#[cfg(target_os = "linux")]
pub fn sync_filesystems() {
    // SAFETY: `libc::sync()` takes no arguments, touches no user pointers, and is thread-safe in POSIX.
    unsafe {
        libc::sync();
    }
}

#[cfg(not(target_os = "linux"))]
pub fn sync_filesystems() {}

#[cfg(target_os = "linux")]
pub fn set_system_hostname(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Ok(());
    }
    let bytes = name.as_bytes();
    // SAFETY: `bytes.as_ptr()` is valid for `bytes.len()` bytes during the synchronous syscall.
    let rc = unsafe { libc::sethostname(bytes.as_ptr() as *const libc::c_char, bytes.len()) };
    if rc == 0 {
        Ok(())
    } else {
        Err(format!("sethostname failed: {}", std::io::Error::last_os_error()))
    }
}

#[cfg(not(target_os = "linux"))]
pub fn set_system_hostname(_name: &str) -> Result<(), String> {
    Ok(())
}

pub fn set_cgroup_freeze(cgroup_dir: &str, freeze: bool) -> Result<(), String> {
    if cgroup_dir.is_empty() {
        return Ok(());
    }
    let freeze_file = Path::new(cgroup_dir).join("cgroup.freeze");
    if !freeze_file.exists() {
        return Ok(());
    }
    let val = if freeze { b"1\n" } else { b"0\n" };
    fs::write(&freeze_file, val).map_err(|e| format!("write {:?}: {}", freeze_file, e))
}

pub fn mix_kernel_entropy(seed: &[u8]) -> Result<(), String> {
    if seed.is_empty() {
        return Ok(());
    }
    let mut f = match OpenOptions::new().write(true).open("/dev/urandom") {
        Ok(f) => f,
        Err(_) => return Ok(()),
    };
    f.write_all(seed)
        .map_err(|e| format!("write /dev/urandom: {}", e))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpEndpoint {
    pub addr: [u8; 16],
    pub port: u16,
    pub v6: bool,
}

/// Parse a `/proc/net/tcp` local or remote field (`HEXIP:HEXPORT`).
pub fn parse_proc_tcp_endpoint(field: &str, v6: bool) -> Option<TcpEndpoint> {
    let (ip_hex, port_hex) = field.split_once(':')?;
    let port = u16::from_str_radix(port_hex, 16).ok()?;
    let mut addr = [0u8; 16];
    if !v6 {
        if ip_hex.len() != 8 {
            return None;
        }
        let raw = u32::from_str_radix(ip_hex, 16).ok()?;
        addr[..4].copy_from_slice(&raw.to_le_bytes());
    } else {
        if ip_hex.len() != 32 {
            return None;
        }
        for i in 0..4 {
            let word = u32::from_str_radix(&ip_hex[i * 8..i * 8 + 8], 16).ok()?;
            addr[i * 4..i * 4 + 4].copy_from_slice(&word.to_le_bytes());
        }
    }
    Some(TcpEndpoint { addr, port, v6 })
}

fn is_loopback(ep: &TcpEndpoint) -> bool {
    if !ep.v6 {
        return ep.addr[0] == 127;
    }
    let mut v4mapped = true;
    for b in &ep.addr[..10] {
        if *b != 0 {
            v4mapped = false;
            break;
        }
    }
    if v4mapped && ep.addr[10] == 0xff && ep.addr[11] == 0xff {
        return ep.addr[12] == 127;
    }
    ep.addr == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
}

pub fn external_established_from_proc(content: &str, v6: bool, exclude_port: u16) -> Vec<(TcpEndpoint, TcpEndpoint)> {
    external_established_rows(content, v6, exclude_port)
        .into_iter()
        .map(|(local, remote, _)| (local, remote))
        .collect()
}

/// Established external sockets as `(local, remote, inode)`.
pub fn external_established_rows(content: &str, v6: bool, exclude_port: u16) -> Vec<(TcpEndpoint, TcpEndpoint, u64)> {
    let mut out = Vec::new();
    for line in content.lines().skip(1) {
        let cols: Vec<&str> = line.split_whitespace().collect();
        if cols.len() < 10 || cols[3] != "01" {
            continue;
        }
        let Some(local) = parse_proc_tcp_endpoint(cols[1], v6) else {
            continue;
        };
        let Some(remote) = parse_proc_tcp_endpoint(cols[2], v6) else {
            continue;
        };
        if local.port == exclude_port || is_loopback(&remote) {
            continue;
        }
        let Ok(inode) = cols[9].parse::<u64>() else {
            continue;
        };
        if inode == 0 {
            continue;
        }
        out.push((local, remote, inode));
    }
    out
}

#[cfg(target_os = "linux")]
pub fn close_external_established_tcp(exclude_port: u16) -> usize {
    let mut closed = 0usize;
    for (path, v6) in [("/proc/net/tcp", false), ("/proc/net/tcp6", true)] {
        let Ok(content) = fs::read_to_string(path) else {
            continue;
        };
        for (_, _, inode) in external_established_rows(&content, v6, exclude_port) {
            if shutdown_socket_inode(inode) {
                closed += 1;
            }
        }
    }
    closed
}

#[cfg(not(target_os = "linux"))]
pub fn close_external_established_tcp(_exclude_port: u16) -> usize {
    0
}

#[cfg(target_os = "linux")]
fn shutdown_via_pidfd(pid: i32, target_fd: i32) -> bool {
    // gVisor rejects opening /proc/<pid>/fd/<n> for sockets (ENXIO) and does not
    // implement NETLINK_SOCK_DIAG. pidfd_getfd duplicates the live fd so shutdown works.
    // SAFETY: pidfd_open/pidfd_getfd/shutdown/close use fds created in this call.
    unsafe {
        let pidfd = libc::syscall(libc::SYS_pidfd_open, pid, 0);
        if pidfd < 0 {
            return false;
        }
        let dupfd = libc::syscall(libc::SYS_pidfd_getfd, pidfd, target_fd, 0);
        libc::close(pidfd as i32);
        if dupfd < 0 {
            return false;
        }
        let rc = libc::shutdown(dupfd as i32, libc::SHUT_RDWR);
        libc::close(dupfd as i32);
        rc == 0
    }
}

#[cfg(target_os = "linux")]
fn shutdown_socket_inode(inode: u64) -> bool {
    let needle = format!("socket:[{inode}]");
    let Ok(proc) = fs::read_dir("/proc") else {
        return false;
    };
    for ent in proc.flatten() {
        let name = ent.file_name();
        let Some(pid) = name.to_str() else {
            continue;
        };
        if !pid.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        let Ok(fds) = fs::read_dir(format!("/proc/{pid}/fd")) else {
            continue;
        };
        for fdent in fds.flatten() {
            let Ok(link) = fs::read_link(fdent.path()) else {
                continue;
            };
            if link.to_string_lossy() != needle {
                continue;
            }
            let Ok(pid_num) = pid.parse::<i32>() else {
                continue;
            };
            let Ok(fd_num) = fdent.file_name().to_string_lossy().parse::<i32>() else {
                continue;
            };
            if shutdown_via_pidfd(pid_num, fd_num) {
                return true;
            }
        }
    }
    false
}

pub const WORKLOAD_UID: u32 = 1000;
pub const WORKLOAD_GID: u32 = 1000;

/// Make `/root` writable by the unprivileged workload uid. `ktd` stays root.
#[cfg(target_os = "linux")]
pub fn prepare_workload_home() {
    let _ = fs::create_dir_all("/root");
    let _ = fs::create_dir_all("/tmp");
    unsafe {
        libc::chown(c"/root".as_ptr(), WORKLOAD_UID, WORKLOAD_GID);
        libc::chmod(c"/root".as_ptr(), 0o755);
        libc::chmod(c"/tmp".as_ptr(), 0o1777);
    }
}

#[cfg(not(target_os = "linux"))]
pub fn prepare_workload_home() {}

/// Parent side of a tenant spawn. The child sends back the seccomp listener, if
/// gVisor accepted one, so `clone3(CLONE_NEWUSER)` can be refused without
/// blocking ordinary thread creation.
#[cfg(target_os = "linux")]
pub struct SpawnGuard {
    parent_fd: i32,
    child_fd: i32,
}

#[cfg(target_os = "linux")]
impl Drop for SpawnGuard {
    fn drop(&mut self) {
        if self.parent_fd >= 0 {
            unsafe { libc::close(self.parent_fd) };
        }
        if self.child_fd >= 0 {
            unsafe { libc::close(self.child_fd) };
        }
    }
}

#[cfg(target_os = "linux")]
impl SpawnGuard {
    pub fn child_fd(&self) -> i32 {
        self.child_fd
    }

    pub fn take_parent_fd(&mut self) -> i32 {
        let fd = self.parent_fd;
        self.parent_fd = -1;
        fd
    }
}

#[cfg(target_os = "linux")]
pub fn begin_spawn_guard() -> Result<SpawnGuard, String> {
    let mut fds = [0; 2];
    if unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) } != 0 {
        return Err(format!("socketpair: {}", std::io::Error::last_os_error()));
    }
    unsafe {
        libc::fcntl(fds[0], libc::F_SETFD, libc::FD_CLOEXEC);
        libc::fcntl(fds[1], libc::F_SETFD, libc::FD_CLOEXEC);
    }
    Ok(SpawnGuard { parent_fd: fds[0], child_fd: fds[1] })
}

#[cfg(not(target_os = "linux"))]
pub struct SpawnGuard;

#[cfg(not(target_os = "linux"))]
pub fn begin_spawn_guard() -> Result<SpawnGuard, String> {
    Ok(SpawnGuard)
}

#[cfg(not(target_os = "linux"))]
impl SpawnGuard {
    pub fn child_fd(&self) -> i32 {
        -1
    }
    pub fn take_parent_fd(&mut self) -> i32 {
        -1
    }
}

/// Run in the forked child before exec. Drops the capabilities that would let a
/// tenant process become root again or ptrace `ktd`, blocks user namespaces,
/// then switches to uid 1000. `ktd` itself is a different process and keeps
/// its credentials.
#[cfg(target_os = "linux")]
pub fn enter_workload_identity() -> Result<(), String> {
    finish_spawn_in_child(-1)
}

#[cfg(target_os = "linux")]
pub fn finish_spawn_in_child(notify_fd: i32) -> Result<(), String> {
    const CAP_DAC_OVERRIDE: u32 = 1;
    const CAP_KILL: u32 = 5;
    const CAP_SETGID: u32 = 6;
    const CAP_SETUID: u32 = 7;
    const CAP_SETPCAP: u32 = 8;
    const CAP_NET_RAW: u32 = 13;
    const CAP_SYS_CHROOT: u32 = 18;
    const CAP_SYS_PTRACE: u32 = 19;
    const CAP_MKNOD: u32 = 27;
    const CAP_SETFCAP: u32 = 31;
    unsafe {
        for cap in [
            CAP_SYS_PTRACE,
            CAP_SETUID,
            CAP_SETGID,
            CAP_SETFCAP,
            CAP_DAC_OVERRIDE,
            CAP_KILL,
            CAP_NET_RAW,
            CAP_SYS_CHROOT,
            CAP_MKNOD,
            CAP_SETPCAP,
        ] {
            let _ = libc::prctl(libc::PR_CAP_AMBIENT, libc::PR_CAP_AMBIENT_LOWER, cap as libc::c_ulong, 0, 0);
            if libc::prctl(libc::PR_CAPBSET_DROP, cap as libc::c_ulong, 0, 0, 0) != 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() != Some(libc::EINVAL) {
                    return Err(format!("drop bounding cap {cap}: {err}"));
                }
            }
        }
        if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
            return Err(format!("no_new_privs: {}", std::io::Error::last_os_error()));
        }
        let listener = install_userns_filter(notify_fd >= 0)?;
        if notify_fd >= 0 {
            match listener {
                Some(fd) => {
                    send_fd(notify_fd, fd)?;
                    libc::close(fd);
                }
                None => {
                    let byte = [0u8];
                    if libc::write(notify_fd, byte.as_ptr() as *const libc::c_void, 1) != 1 {
                        return Err(format!("notify parent: {}", std::io::Error::last_os_error()));
                    }
                }
            }
        } else if let Some(fd) = listener {
            libc::close(fd);
        }
        if libc::setgroups(0, std::ptr::null()) != 0 {
            return Err(format!("clear supplementary groups: {}", std::io::Error::last_os_error()));
        }
        if libc::setgid(WORKLOAD_GID) != 0 {
            return Err(format!("setgid: {}", std::io::Error::last_os_error()));
        }
        if libc::setuid(WORKLOAD_UID) != 0 {
            return Err(format!("setuid: {}", std::io::Error::last_os_error()));
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub fn supervise_spawn_guard(parent_fd: i32) {
    if parent_fd < 0 {
        return;
    }
    let listener = recv_listener(parent_fd).ok().flatten();
    unsafe { libc::close(parent_fd) };
    let Some(listener) = listener else {
        return;
    };
    loop {
        let mut notif = SeccompNotif::zeroed();
        let rc = unsafe { libc::ioctl(listener, NOTIF_RECV, &mut notif) };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            break;
        }
        let deny = clone3_requests_user_ns(&notif);
        let resp = SeccompNotifResp {
            id: notif.id,
            val: 0,
            error: if deny { libc::EPERM } else { 0 },
            flags: if deny { 0 } else { SECCOMP_USER_NOTIF_FLAG_CONTINUE },
        };
        if unsafe { libc::ioctl(listener, NOTIF_SEND, &resp) } != 0 && !deny {
            let fallback = SeccompNotifResp {
                id: notif.id,
                val: 0,
                error: libc::EPERM,
                flags: 0,
            };
            let _ = unsafe { libc::ioctl(listener, NOTIF_SEND, &fallback) };
        }
    }
    unsafe { libc::close(listener) };
}

#[cfg(target_os = "linux")]
const SECCOMP_SET_MODE_FILTER: libc::c_ulong = 1;
#[cfg(target_os = "linux")]
const SECCOMP_FILTER_FLAG_NEW_LISTENER: libc::c_ulong = 8;
#[cfg(target_os = "linux")]
const SECCOMP_RET_ALLOW: u32 = 0x7fff_0000;
#[cfg(target_os = "linux")]
const SECCOMP_RET_ERRNO: u32 = 0x0005_0000;
#[cfg(target_os = "linux")]
const SECCOMP_RET_USER_NOTIF: u32 = 0x7fc0_0000;
#[cfg(target_os = "linux")]
const SECCOMP_USER_NOTIF_FLAG_CONTINUE: u32 = 1;
#[cfg(target_os = "linux")]
const BPF_LD_W_ABS: u16 = 0x20;
#[cfg(target_os = "linux")]
const BPF_JMP_JEQ_K: u16 = 0x15;
#[cfg(target_os = "linux")]
const BPF_RET_K: u16 = 0x06;
#[cfg(target_os = "linux")]
const BPF_ALU_AND_K: u16 = 0x54;

#[cfg(target_os = "linux")]
#[repr(C)]
struct SeccompData {
    nr: i32,
    arch: u32,
    instruction_pointer: u64,
    args: [u64; 6],
}

#[cfg(target_os = "linux")]
#[repr(C)]
struct SeccompNotif {
    id: u64,
    pid: u32,
    flags: u32,
    data: SeccompData,
}

#[cfg(target_os = "linux")]
impl SeccompNotif {
    fn zeroed() -> Self {
        Self {
            id: 0,
            pid: 0,
            flags: 0,
            data: SeccompData {
                nr: 0,
                arch: 0,
                instruction_pointer: 0,
                args: [0; 6],
            },
        }
    }
}

#[cfg(target_os = "linux")]
#[repr(C)]
struct SeccompNotifResp {
    id: u64,
    val: i64,
    error: i32,
    flags: u32,
}

#[cfg(target_os = "linux")]
const NOTIF_RECV: i32 = iowr_const(0, std::mem::size_of::<SeccompNotif>() as u64);
#[cfg(target_os = "linux")]
const NOTIF_SEND: i32 = iowr_const(1, std::mem::size_of::<SeccompNotifResp>() as u64);

#[cfg(target_os = "linux")]
const fn iowr_const(nr: u64, size: u64) -> i32 {
    ((3u64 << 30) | (size << 16) | ((b'!' as u64) << 8) | nr) as i32
}

#[cfg(target_os = "linux")]
fn userns_filter(notify_clone3: bool) -> Vec<libc::sock_filter> {
    let unshare_nr = libc::SYS_unshare as u32;
    let clone_nr = libc::SYS_clone as u32;
    let clone3_nr = libc::SYS_clone3 as u32;
    let newuser = libc::CLONE_NEWUSER as u32;
    let clone3_ret = if notify_clone3 { SECCOMP_RET_USER_NOTIF } else { SECCOMP_RET_ALLOW };
    let stmt = |code, k| libc::sock_filter { code, jt: 0, jf: 0, k };
    let jump = |k, jt, jf| libc::sock_filter { code: BPF_JMP_JEQ_K, jt, jf, k };
    vec![
        stmt(BPF_LD_W_ABS, 0),
        jump(unshare_nr, 4, 0),
        jump(clone_nr, 3, 0),
        jump(clone3_nr, 0, 1),
        stmt(BPF_RET_K, clone3_ret),
        stmt(BPF_RET_K, SECCOMP_RET_ALLOW),
        stmt(BPF_LD_W_ABS, 16),
        stmt(BPF_ALU_AND_K, newuser),
        jump(newuser, 0, 1),
        stmt(BPF_RET_K, SECCOMP_RET_ERRNO | 1),
        stmt(BPF_RET_K, SECCOMP_RET_ALLOW),
    ]
}

#[cfg(target_os = "linux")]
fn install_filter(filter: &[libc::sock_filter], flags: libc::c_ulong) -> Result<i64, String> {
    let prog = libc::sock_fprog {
        len: filter.len() as u16,
        filter: filter.as_ptr() as *mut libc::sock_filter,
    };
    let rc = unsafe { libc::syscall(libc::SYS_seccomp, SECCOMP_SET_MODE_FILTER, flags, &prog) };
    if rc < 0 {
        Err(format!("{}", std::io::Error::last_os_error()))
    } else {
        Ok(rc)
    }
}

/// Returns the user-notification fd when the kernel accepted one.
#[cfg(target_os = "linux")]
fn install_userns_filter(try_listener: bool) -> Result<Option<i32>, String> {
    if try_listener {
        if let Ok(fd) = install_filter(&userns_filter(true), SECCOMP_FILTER_FLAG_NEW_LISTENER) {
            return Ok(Some(fd as i32));
        }
    }
    install_filter(&userns_filter(false), 0).map_err(|e| format!("seccomp userns filter: {e}"))?;
    Ok(None)
}

#[cfg(target_os = "linux")]
fn send_fd(sock: i32, fd: i32) -> Result<(), String> {
    let mut byte = [1u8];
    let mut iov = libc::iovec {
        iov_base: byte.as_mut_ptr() as *mut libc::c_void,
        iov_len: 1,
    };
    let mut cbuf = [0u8; 64];
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    let space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<i32>() as libc::c_uint) } as usize;
    msg.msg_control = cbuf.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = space as _;
    unsafe {
        let hdr = libc::CMSG_FIRSTHDR(&msg);
        if hdr.is_null() {
            return Err("cmsg header missing".to_string());
        }
        (*hdr).cmsg_level = libc::SOL_SOCKET;
        (*hdr).cmsg_type = libc::SCM_RIGHTS;
        (*hdr).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<i32>() as libc::c_uint) as _;
        *(libc::CMSG_DATA(hdr) as *mut i32) = fd;
        if libc::sendmsg(sock, &msg, 0) < 0 {
            return Err(format!("sendmsg: {}", std::io::Error::last_os_error()));
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn recv_listener(sock: i32) -> Result<Option<i32>, String> {
    let mut byte = [0u8];
    let mut iov = libc::iovec {
        iov_base: byte.as_mut_ptr() as *mut libc::c_void,
        iov_len: 1,
    };
    let mut cbuf = [0u8; 64];
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cbuf.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = cbuf.len() as _;
    let n = unsafe { libc::recvmsg(sock, &mut msg, 0) };
    if n <= 0 {
        return Ok(None);
    }
    if byte[0] == 0 {
        return Ok(None);
    }
    unsafe {
        let hdr = libc::CMSG_FIRSTHDR(&msg);
        if hdr.is_null() {
            return Ok(None);
        }
        Ok(Some(*(libc::CMSG_DATA(hdr) as *const i32)))
    }
}

#[cfg(target_os = "linux")]
fn clone3_requests_user_ns(notif: &SeccompNotif) -> bool {
    let ptr = notif.data.args[0];
    if ptr == 0 {
        return true;
    }
    let mut flags: u64 = 0;
    let local = libc::iovec {
        iov_base: &mut flags as *mut u64 as *mut libc::c_void,
        iov_len: 8,
    };
    let remote = libc::iovec {
        iov_base: ptr as *mut libc::c_void,
        iov_len: 8,
    };
    let n = unsafe { libc::process_vm_readv(notif.pid as libc::pid_t, &local, 1, &remote, 1, 0) };
    if n != 8 {
        return true;
    }
    flags & (libc::CLONE_NEWUSER as u64) != 0
}

#[cfg(not(target_os = "linux"))]
pub fn enter_workload_identity() -> Result<(), String> {
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn finish_spawn_in_child(_notify_fd: i32) -> Result<(), String> {
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn supervise_spawn_guard(_parent_fd: i32) {}

#[cfg(target_os = "linux")]
pub fn reap_orphans_once() {
    loop {
        let mut status: libc::c_int = 0;
        // SAFETY: Passing a valid stack pointer `&mut status` with `WNOHANG` reaps exited children without blocking.
        let pid = unsafe { libc::waitpid(-1, &mut status as *mut libc::c_int, libc::WNOHANG) };
        if pid <= 0 {
            break;
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub fn reap_orphans_once() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_proc_ipv4_and_skip_loopback_remote() {
        let ep = parse_proc_tcp_endpoint("0100007F:0050", false).expect("parse");
        assert_eq!(ep.port, 80);
        assert_eq!(&ep.addr[..4], &[127, 0, 0, 1]);
        let table = "  sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n\
                      0: 00000000:C33F 0100007F:01BB 01 00000000:00000000 00:00000000 00000000 0 0 11\n\
                      1: 0B01A8C0:C340 08080808:01BB 01 00000000:00000000 00:00000000 00000000 0 0 42\n";
        let ext = external_established_from_proc(table, false, 49983);
        assert_eq!(ext.len(), 1);
        assert_eq!(ext[0].1.port, 443);
        assert_eq!(&ext[0].1.addr[..4], &[8, 8, 8, 8]);
        let rows = external_established_rows(table, false, 49983);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].2, 42);
    }
}
