//! Minimal Carrier Pod PID 1: park until SIGTERM/SIGINT, then exit 0.
//! Intentionally has no threads, allocator churn, or shell — keeps idle RSS near pause(8).

fn main() {
    unsafe {
        let handler = exit_on_signal as *const () as libc::sighandler_t;
        libc::signal(libc::SIGTERM, handler);
        libc::signal(libc::SIGINT, handler);
        // Ignore SIGCHLD so accidental children don't become zombies if a debug exec happens.
        libc::signal(libc::SIGCHLD, libc::SIG_IGN);
        loop {
            libc::pause();
        }
    }
}

extern "C" fn exit_on_signal(_: libc::c_int) {
    unsafe {
        libc::_exit(0);
    }
}
