use std::fs;
use std::path::PathBuf;

use kontinue_arena::{chunk_and_diff_snapshot, materialize_local_view, ChunkDiffInput};
use kt_protocol::{CompatibilityFingerprint, SnapshotManifest};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("Usage: kontinue-arena-helper <chunk-diff|materialize> [flags]");
        std::process::exit(1);
    }

    match args[0].as_str() {
        "chunk-diff" => run_chunk_diff(&args[1..]),
        "materialize" => run_materialize(&args[1..]),
        other => {
            eprintln!("unknown subcommand: {}", other);
            std::process::exit(1);
        }
    }
}

fn get_opt(args: &[String], flag: &str) -> String {
    let mut i = 0;
    while i + 1 < args.len() {
        if args[i] == flag {
            return args[i + 1].clone();
        }
        i += 1;
    }
    String::new()
}

fn run_chunk_diff(args: &[String]) {
    let snapshot_id = get_opt(args, "--snapshot-id");
    let kind = get_opt(args, "--kind");
    let template_id = get_opt(args, "--template-id");
    let base_image = get_opt(args, "--base-image");
    let origin_node = get_opt(args, "--origin-node");
    let origin_peer = get_opt(args, "--origin-peer");
    let bootstrap_token = get_opt(args, "--bootstrap-token");
    let input_dir = PathBuf::from(get_opt(args, "--input-dir"));
    let cas_dir = PathBuf::from(get_opt(args, "--cas-dir"));
    let parent_manifest_path = get_opt(args, "--parent-manifest");
    let output_manifest_path = PathBuf::from(get_opt(args, "--output-manifest"));
    let fp_json = get_opt(args, "--fingerprint-json");

    let parent: Option<SnapshotManifest> = if !parent_manifest_path.is_empty() {
        fs::read(&parent_manifest_path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
    } else {
        None
    };

    let fingerprint: CompatibilityFingerprint = if !fp_json.is_empty() {
        serde_json::from_str(&fp_json).unwrap_or_default()
    } else {
        CompatibilityFingerprint::default()
    };

    match chunk_and_diff_snapshot(ChunkDiffInput {
        snapshot_id: &snapshot_id,
        kind: if kind.is_empty() { "GoldenTemplate" } else { &kind },
        template_id: &template_id,
        base_image: &base_image,
        origin_node_id: &origin_node,
        origin_peer_addr: &origin_peer,
        bootstrap_token: &bootstrap_token,
        raw_ckpt_dir: &input_dir,
        cas_root: &cas_dir,
        parent_manifest: parent.as_ref(),
        fingerprint,
    }) {
        Ok(manifest) => {
            let raw = match serde_json::to_vec_pretty(&manifest) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("encode manifest error: {}", e);
                    std::process::exit(1);
                }
            };
            if let Some(p) = output_manifest_path.parent() {
                let _ = fs::create_dir_all(p);
            }
            if let Err(e) = fs::write(&output_manifest_path, raw) {
                eprintln!("write manifest error: {}", e);
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("chunk-diff failed: {}", e);
            std::process::exit(1);
        }
    }
}

fn run_materialize(args: &[String]) {
    let manifest_path = PathBuf::from(get_opt(args, "--manifest"));
    let cas_dir = PathBuf::from(get_opt(args, "--cas-dir"));
    let output_dir = PathBuf::from(get_opt(args, "--output-dir"));

    let raw = match fs::read(&manifest_path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("read manifest {:?}: {}", manifest_path, e);
            std::process::exit(1);
        }
    };
    let manifest: SnapshotManifest = match serde_json::from_slice(&raw) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("parse manifest: {}", e);
            std::process::exit(1);
        }
    };
    if let Err(e) = materialize_local_view(&manifest, &cas_dir, &output_dir) {
        eprintln!("materialize failed: {}", e);
        std::process::exit(1);
    }
}
