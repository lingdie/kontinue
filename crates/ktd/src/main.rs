use std::net::TcpListener;

use ktd::{serve_http, KtdConfig, KtdService};
use kt_protocol::ExecRequest;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("--workload-file") {
        let [_, operation, path, mode] = args.as_slice() else { std::process::exit(1) };
        let Ok(mode) = mode.parse::<u32>() else { std::process::exit(1) };
        std::process::exit(ktd::files::helper(operation, std::path::Path::new(path), mode));
    }
    let mut listen_addr = "0.0.0.0:49983".to_string();
    let mut bootstrap_token = std::env::var("KTD_BOOTSTRAP_TOKEN")
        .unwrap_or_else(|_| "kontinue-default-bootstrap-token".to_string());
    let mut initial_access_token =
        std::env::var("KTD_INITIAL_ACCESS_TOKEN").unwrap_or_default();
    let mut sandbox_id = std::env::var("SANDBOX_ID").unwrap_or_default();
    let mut machine_id_path = "/etc/machine-id".to_string();
    let mut workload_cgroup = "/sys/fs/cgroup/kontinue-workload".to_string();
    let mut startup_cmd = std::env::var("KTD_STARTUP_CMD").unwrap_or_default();

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--listen" if i + 1 < args.len() => {
                listen_addr = args[i + 1].clone();
                i += 2;
            }
            "--bootstrap-token" if i + 1 < args.len() => {
                bootstrap_token = args[i + 1].clone();
                i += 2;
            }
            "--initial-access-token" if i + 1 < args.len() => {
                initial_access_token = args[i + 1].clone();
                i += 2;
            }
            "--sandbox-id" if i + 1 < args.len() => {
                sandbox_id = args[i + 1].clone();
                i += 2;
            }
            "--machine-id-path" if i + 1 < args.len() => {
                machine_id_path = args[i + 1].clone();
                i += 2;
            }
            "--workload-cgroup" if i + 1 < args.len() => {
                workload_cgroup = args[i + 1].clone();
                i += 2;
            }
            "--startup-cmd" if i + 1 < args.len() => {
                startup_cmd = args[i + 1].clone();
                i += 2;
            }
            _ => i += 1,
        }
    }

    let svc = KtdService::new(KtdConfig {
        version: env!("CARGO_PKG_VERSION").to_string(),
        sandbox_id,
        bootstrap_token,
        initial_access_token,
        machine_id_path,
        workload_cgroup_dir: workload_cgroup,
        control_port: 49983,
    });
    svc.start_pid1_reaper();
    ktd::platform::prepare_workload_home();

    if !startup_cmd.is_empty() {
        let _ = svc.exec(ExecRequest {
            command: vec!["/bin/sh".into(), "-c".into(), startup_cmd],
            background: true,
            ..Default::default()
        });
    }

    let listener = match TcpListener::bind(&listen_addr) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("ktd failed to bind {}: {}", listen_addr, e);
            std::process::exit(1);
        }
    };
    eprintln!("ktd listening on {}", listen_addr);
    serve_http(listener, svc);
}
