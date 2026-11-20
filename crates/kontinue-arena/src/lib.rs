//! High-throughput, bounded-memory Rust 2MiB zero-page scanner, SIMD BLAKE3 incremental chunker,
//! and sparse-file materializer for `Kontinue Arena`.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use kt_protocol::{
    format_rfc3339_utc, ChunkRef, CompatibilityFingerprint, SnapshotManifest, DEFAULT_CHUNK_SIZE,
    GVISOR_CHECKPOINT_IMG, GVISOR_PAGES_IMG, GVISOR_PAGES_META_IMG,
};

pub fn chunk_cas_rel_path(hash: &str) -> String {
    if hash.len() >= 4 {
        format!("cas/chunks/{}/{}.bin", &hash[..4], hash)
    } else {
        format!("cas/chunks/misc/{}.bin", hash)
    }
}

/// Word-aligned zero-page check over a slice without extra allocations.
pub fn is_all_zero(buf: &[u8]) -> bool {
    let (prefix, aligned, suffix) = {
        let ptr = buf.as_ptr();
        let align_offset = ptr.align_offset(std::mem::align_of::<u64>());
        if align_offset >= buf.len() {
            (buf, &[][..], &[][..])
        } else {
            let head = &buf[..align_offset];
            let rest = &buf[align_offset..];
            let u64_len = rest.len() / 8;
            // SAFETY: `rest.as_ptr()` is 8-byte aligned via `align_offset`, and `u64_len * 8 <= rest.len()`.
            let words = unsafe { std::slice::from_raw_parts(rest.as_ptr() as *const u64, u64_len) };
            let tail = &rest[u64_len * 8..];
            (head, words, tail)
        }
    };
    prefix.iter().all(|&b| b == 0)
        && aligned.iter().all(|&w| w == 0)
        && suffix.iter().all(|&b| b == 0)
}

pub struct ChunkDiffInput<'a> {
    pub snapshot_id: &'a str,
    pub kind: &'a str,
    pub template_id: &'a str,
    pub base_image: &'a str,
    pub origin_node_id: &'a str,
    pub origin_peer_addr: &'a str,
    pub bootstrap_token: &'a str,
    pub raw_ckpt_dir: &'a Path,
    pub cas_root: &'a Path,
    pub parent_manifest: Option<&'a SnapshotManifest>,
    pub fingerprint: CompatibilityFingerprint,
}

/// Streams `pages.img` in fixed 2MiB buffers, skips zero blocks, deduplicates unchanged blocks
/// against `parent_manifest` via BLAKE3, writes only dirty chunks into `cas_root`, and returns `SnapshotManifest`.
pub fn chunk_and_diff_snapshot(input: ChunkDiffInput<'_>) -> Result<SnapshotManifest, String> {
    let parent_hashes: HashSet<&str> = input
        .parent_manifest
        .map(|pm| {
            pm.chunks
                .iter()
                .filter(|c| !c.blake3_hash.is_empty())
                .map(|c| c.blake3_hash.as_str())
                .collect()
        })
        .unwrap_or_default();

    let (ckpt_hash, ckpt_size) = stage_artifact(
        &input.raw_ckpt_dir.join(GVISOR_CHECKPOINT_IMG),
        &input
            .cas_root
            .join("snapshots")
            .join(input.snapshot_id)
            .join(GVISOR_CHECKPOINT_IMG),
    )?;
    let (meta_hash, meta_size) = stage_artifact(
        &input.raw_ckpt_dir.join(GVISOR_PAGES_META_IMG),
        &input
            .cas_root
            .join("snapshots")
            .join(input.snapshot_id)
            .join(GVISOR_PAGES_META_IMG),
    )?;

    let pages_path = input.raw_ckpt_dir.join(GVISOR_PAGES_IMG);
    let mut chunks = Vec::new();
    let mut total_size: i64 = 0;
    let mut dirty_bytes: i64 = 0;
    let mut dirty_count: i32 = 0;
    let mut reused_count: i32 = 0;
    let mut zero_count: i32 = 0;

    if pages_path.exists() {
        let f = File::open(&pages_path).map_err(|e| format!("open pages.img: {}", e))?;
        let mut reader = BufReader::with_capacity(DEFAULT_CHUNK_SIZE, f);
        let mut buf = vec![0u8; DEFAULT_CHUNK_SIZE];
        let mut idx: u32 = 0;

        loop {
            let n = read_full_chunk(&mut reader, &mut buf)
                .map_err(|e| format!("read pages.img chunk {}: {}", idx, e))?;
            if n == 0 {
                break;
            }
            let slice = &buf[..n];
            let offset = total_size;
            total_size += n as i64;

            if is_all_zero(slice) {
                zero_count += 1;
                chunks.push(ChunkRef {
                    index: idx,
                    offset,
                    length: n as i32,
                    blake3_hash: String::new(),
                    zero_chunk: true,
                    reused_from_parent: false,
                });
            } else {
                let hash = blake3::hash(slice).to_hex().to_string();
                let reused = parent_hashes.contains(hash.as_str());
                let chunk_dest = input.cas_root.join(chunk_cas_rel_path(&hash));
                if !reused || !chunk_dest.exists() {
                    atomic_write(&chunk_dest, slice)?;
                }
                if reused {
                    reused_count += 1;
                } else {
                    dirty_count += 1;
                    dirty_bytes += n as i64;
                }
                chunks.push(ChunkRef {
                    index: idx,
                    offset,
                    length: n as i32,
                    blake3_hash: hash,
                    zero_chunk: false,
                    reused_from_parent: reused,
                });
            }
            idx += 1;
        }
    }

    let rootfs_src = input.raw_ckpt_dir.join("rootfs-upper.tar");
    let (rootfs_hash, rootfs_size) = if rootfs_src.exists() {
        stage_artifact(
            &rootfs_src,
            &input
                .cas_root
                .join("snapshots")
                .join(input.snapshot_id)
                .join("rootfs-upper.tar"),
        )?
    } else {
        (String::new(), 0)
    };

    Ok(SnapshotManifest {
        schema_version: 1,
        snapshot_id: input.snapshot_id.to_string(),
        kind: input.kind.to_string(),
        template_id: input.template_id.to_string(),
        base_image: input.base_image.to_string(),
        parent_snapshot_id: input
            .parent_manifest
            .map(|p| p.snapshot_id.clone())
            .unwrap_or_default(),
        created_at: format_rfc3339_utc(SystemTime::now()),
        include_memory: pages_path.exists(),
        chunk_size: DEFAULT_CHUNK_SIZE as i32,
        checkpoint_img_hash: ckpt_hash,
        checkpoint_img_size: ckpt_size,
        pages_meta_img_hash: meta_hash,
        pages_meta_img_size: meta_size,
        pages_img_total_size: total_size,
        dirty_memory_bytes: dirty_bytes,
        dirty_chunk_count: dirty_count,
        reused_chunk_count: reused_count,
        zero_chunk_count: zero_count,
        rootfs_upper_tar_hash: rootfs_hash,
        rootfs_upper_tar_size: rootfs_size,
        chunks,
        hot_page_indices: Vec::new(),
        origin_node_id: input.origin_node_id.to_string(),
        origin_peer_addr: input.origin_peer_addr.to_string(),
        bootstrap_token: input.bootstrap_token.to_string(),
        fingerprint: input.fingerprint,
    })
}

/// Materializes an L1 (`tmpfs`) or L2 (`NVMe`) gVisor restore view directory from local CAS chunks,
/// preserving zero-page sparse holes so zero blocks consume 0 physical RAM/disk blocks.
pub fn materialize_local_view(
    manifest: &SnapshotManifest,
    cas_root: &Path,
    view_dir: &Path,
) -> Result<(), String> {
    fs::create_dir_all(view_dir).map_err(|e| format!("mkdir {:?}: {}", view_dir, e))?;

    let snap_dir = cas_root.join("snapshots").join(&manifest.snapshot_id);
    for name in [GVISOR_CHECKPOINT_IMG, GVISOR_PAGES_META_IMG] {
        let src = snap_dir.join(name);
        let dst = view_dir.join(name);
        if src.exists() && !dst.exists() {
            if fs::hard_link(&src, &dst).is_err() {
                fs::copy(&src, &dst).map_err(|e| format!("copy {:?}: {}", src, e))?;
            }
        }
    }

    if manifest.include_memory {
        let pages_dst = view_dir.join(GVISOR_PAGES_IMG);
        let mut out = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&pages_dst)
            .map_err(|e| format!("create {:?}: {}", pages_dst, e))?;
        out.set_len(manifest.pages_img_total_size as u64)
            .map_err(|e| format!("set_len pages.img: {}", e))?;

        let mut buf = vec![0u8; DEFAULT_CHUNK_SIZE];
        for ch in &manifest.chunks {
            if ch.zero_chunk || ch.blake3_hash.is_empty() {
                continue;
            }
            let chunk_file = cas_root.join(chunk_cas_rel_path(&ch.blake3_hash));
            let mut cf = File::open(&chunk_file)
                .map_err(|e| format!("missing local CAS chunk {}: {}", ch.blake3_hash, e))?;
            let n = cf
                .read(&mut buf[..ch.length as usize])
                .map_err(|e| format!("read chunk {}: {}", ch.blake3_hash, e))?;
            out.seek(SeekFrom::Start(ch.offset as u64))
                .map_err(|e| format!("seek pages.img: {}", e))?;
            out.write_all(&buf[..n])
                .map_err(|e| format!("write pages.img: {}", e))?;
        }
        out.flush().map_err(|e| format!("flush pages.img: {}", e))?;
    }
    Ok(())
}

fn read_full_chunk<R: Read>(r: &mut R, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        let n = r.read(&mut buf[total..])?;
        if n == 0 {
            break;
        }
        total += n;
    }
    Ok(total)
}

fn stage_artifact(src: &Path, dst: &Path) -> Result<(String, i64), String> {
    if !src.exists() {
        return Ok((String::new(), 0));
    }
    let bytes = fs::read(src).map_err(|e| format!("read {:?}: {}", src, e))?;
    let hash = blake3::hash(&bytes).to_hex().to_string();
    atomic_write(dst, &bytes)?;
    Ok((hash, bytes.len() as i64))
}

fn atomic_write(dst: &Path, data: &[u8]) -> Result<(), String> {
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("mkdir {:?}: {}", parent, e))?;
    }
    let tmp = PathBuf::from(format!(
        "{}.tmp.{}",
        dst.to_string_lossy(),
        kt_protocol::now_unix_nanos()
    ));
    fs::write(&tmp, data).map_err(|e| format!("write {:?}: {}", tmp, e))?;
    fs::rename(&tmp, dst).map_err(|e| format!("rename {:?} -> {:?}: {}", tmp, dst, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_2mib_blake3_incremental_diff_and_sparse_materialize() {
        let tmp = std::env::temp_dir().join(format!("kt-arena-test-{}", kt_protocol::now_unix_nanos()));
        let cas = tmp.join("cas");
        let tpl_raw = tmp.join("tpl-raw");
        let sess_raw = tmp.join("sess-raw");
        let restored_view = tmp.join("view");
        let _ = fs::create_dir_all(&tpl_raw);
        let _ = fs::create_dir_all(&sess_raw);

        // Golden Template: Chunk 0 = 'A', Chunk 1 = ALL ZERO, Chunk 2 = 'B' (6 MiB total)
        let mut tpl_pages = vec![0u8; 3 * DEFAULT_CHUNK_SIZE];
        tpl_pages[0..DEFAULT_CHUNK_SIZE].fill(b'A');
        tpl_pages[2 * DEFAULT_CHUNK_SIZE..3 * DEFAULT_CHUNK_SIZE].fill(b'B');

        fs::write(tpl_raw.join(GVISOR_CHECKPOINT_IMG), b"kernel-v1").unwrap();
        fs::write(tpl_raw.join(GVISOR_PAGES_META_IMG), b"meta-v1").unwrap();
        fs::write(tpl_raw.join(GVISOR_PAGES_IMG), &tpl_pages).unwrap();

        let tpl_manifest = chunk_and_diff_snapshot(ChunkDiffInput {
            snapshot_id: "snap-golden-1",
            kind: "GoldenTemplate",
            template_id: "py-agent",
            base_image: "python:3.12",
            origin_node_id: "node-1",
            origin_peer_addr: "127.0.0.1:9091",
            bootstrap_token: "boot",
            raw_ckpt_dir: &tpl_raw,
            cas_root: &cas,
            parent_manifest: None,
            fingerprint: CompatibilityFingerprint::default(),
        })
        .expect("golden chunk");

        assert_eq!(tpl_manifest.zero_chunk_count, 1);
        assert_eq!(tpl_manifest.dirty_chunk_count, 2);
        assert_eq!(tpl_manifest.reused_chunk_count, 0);

        // Session Suspend: Chunk 0 unchanged ('A'), Chunk 1 still ZERO, Chunk 2 modified ('C')!
        let mut sess_pages = tpl_pages.clone();
        sess_pages[2 * DEFAULT_CHUNK_SIZE..3 * DEFAULT_CHUNK_SIZE].fill(b'C');
        fs::write(sess_raw.join(GVISOR_CHECKPOINT_IMG), b"kernel-v2").unwrap();
        fs::write(sess_raw.join(GVISOR_PAGES_META_IMG), b"meta-v2").unwrap();
        fs::write(sess_raw.join(GVISOR_PAGES_IMG), &sess_pages).unwrap();

        let sess_manifest = chunk_and_diff_snapshot(ChunkDiffInput {
            snapshot_id: "snap-sess-1",
            kind: "SessionSuspend",
            template_id: "py-agent",
            base_image: "python:3.12",
            origin_node_id: "node-1",
            origin_peer_addr: "127.0.0.1:9091",
            bootstrap_token: "boot",
            raw_ckpt_dir: &sess_raw,
            cas_root: &cas,
            parent_manifest: Some(&tpl_manifest),
            fingerprint: CompatibilityFingerprint::default(),
        })
        .expect("session diff chunk");

        // Only 1 dirty chunk ('C') should be newly counted; Chunk 0 ('A') is reused, Chunk 1 is zero!
        assert_eq!(sess_manifest.dirty_chunk_count, 1);
        assert_eq!(sess_manifest.reused_chunk_count, 1);
        assert_eq!(sess_manifest.zero_chunk_count, 1);
        assert_eq!(sess_manifest.dirty_memory_bytes, DEFAULT_CHUNK_SIZE as i64);

        // Materialize view and verify exact byte-for-byte fidelity!
        materialize_local_view(&sess_manifest, &cas, &restored_view).expect("materialize");
        let rebuilt = fs::read(restored_view.join(GVISOR_PAGES_IMG)).unwrap();
        assert_eq!(rebuilt, sess_pages);

        let _ = fs::remove_dir_all(&tmp);
    }
}
