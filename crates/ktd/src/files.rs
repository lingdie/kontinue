//! The helper runs under the same workload identity as exec before opening
//! any caller-selected path. Binary file data travels over its standard pipes.
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;
use std::process::{Command, Stdio};

use crate::KtdService;

fn file_io(operation: &str, path: &Path, mode: u32) -> io::Result<()> {
    match operation {
        "read" => {
            let mut file = File::open(path)?;
            io::copy(&mut file, &mut io::stdout().lock())?;
        }
        "write" => {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut file = File::create(path)?;
            io::copy(&mut io::stdin().lock(), &mut file)?;
            #[cfg(unix)]
            if mode != 0 {
                use std::os::unix::fs::PermissionsExt;
                file.set_permissions(fs::Permissions::from_mode(mode))?;
            }
        }
        _ => return Err(io::Error::new(io::ErrorKind::InvalidInput, "unknown file operation")),
    }
    Ok(())
}

pub fn helper(operation: &str, path: &Path, mode: u32) -> i32 {
    match file_io(operation, path, mode) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{e}");
            match e.kind() {
                io::ErrorKind::NotFound => 2,
                io::ErrorKind::PermissionDenied => 3,
                _ => 1,
            }
        }
    }
}

impl KtdService {
    pub(crate) fn file_request(&self, path: &str, write: Option<(&[u8], u32)>) -> Result<Vec<u8>, (u16, String)> {
        {
            let mut state = self.state.lock().map_err(|_| (500, "poisoned lock".into()))?;
            if state.frozen {
                return Err((503, "sandbox is frozen for checkpointing".into()));
            }
            state.in_flight_spawns += 1;
        }
        let result = run_helper(path, write);
        if let Ok(mut state) = self.state.lock() {
            state.in_flight_spawns -= 1;
            self.spawn_cv.notify_all();
        }
        result
    }
}

fn run_helper(path: &str, write: Option<(&[u8], u32)>) -> Result<Vec<u8>, (u16, String)> {
    let internal = |err: io::Error| (500, err.to_string());
    let exe = std::env::current_exe().map_err(internal)?;
    let mut cmd = Command::new(exe);
    cmd.env_clear()
        .arg("--workload-file")
        .arg(if write.is_some() { "write" } else { "read" })
        .arg(path)
        .arg(write.map_or(0, |(_, mode)| mode).to_string())
        .stdin(if write.is_some() { Stdio::piped() } else { Stdio::null() })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    unsafe {
        use std::os::unix::process::CommandExt;
        cmd.pre_exec(|| crate::platform::enter_workload_identity().map_err(io::Error::other));
    }

    let mut child = cmd.spawn().map_err(internal)?;
    let sent = if let Some((bytes, _)) = write {
        child.stdin.take().expect("piped helper stdin").write_all(bytes)
    } else {
        Ok(())
    };
    let out = child.wait_with_output().map_err(internal)?;
    if !out.status.success() {
        let status = match out.status.code() {
            Some(2) => 404,
            Some(3) => 403,
            _ => 500,
        };
        return Err((status, String::from_utf8_lossy(&out.stderr).trim().into()));
    }
    sent.map_err(internal)?;
    Ok(out.stdout)
}
