//! Shared wire contracts, constant-time token comparison, W3C `traceparent` parser,
//! and in-memory `FlightRecorder` shared across Kontinue Rust data-plane binaries
//! (`ktd`, `kontinue-runsc`, `kontinue-arena-helper`) with 100% Go JSON wire compatibility.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

pub const DEFAULT_CHUNK_SIZE: usize = 2 * 1024 * 1024;
pub const HEADER_KONTINUE_TOKEN: &str = "x-kontinue-token";
pub const HEADER_BOOTSTRAP_TOKEN: &str = "x-bootstrap-token";
pub const HEADER_TRACEPARENT: &str = "traceparent";
pub const HEADER_TRACE_ID: &str = "X-Trace-ID";

pub const GVISOR_CHECKPOINT_IMG: &str = "checkpoint.img";
pub const GVISOR_PAGES_META_IMG: &str = "pages_meta.img";
pub const GVISOR_PAGES_IMG: &str = "pages.img";
pub const MANIFEST_FILE_NAME: &str = "kontinue-manifest.json";

pub const ANNOTATION_RESTORE_HOST_PATH: &str = "dev.gvisor.internal.restore.host-image-path";
pub const ANNOTATION_RESTORE_DIRECT: &str = "dev.gvisor.internal.restore.direct";

/// Go `encoding/json` encodes `[]byte` as Base64 strings.
/// This module ensures Rust `Vec<u8>` fields serialize to Base64 and deserialize from either Base64 or `[u8]`.
pub mod go_bytes {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ByteRepr {
        Base64(String),
        Raw(Vec<u8>),
    }

    pub fn serialize<S: Serializer>(bytes: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        if bytes.is_empty() {
            return s.serialize_str("");
        }
        s.serialize_str(&STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let opt = Option::<ByteRepr>::deserialize(d)?;
        match opt {
            None => Ok(Vec::new()),
            Some(ByteRepr::Base64(st)) if st.is_empty() => Ok(Vec::new()),
            Some(ByteRepr::Base64(st)) => STANDARD.decode(st).map_err(serde::de::Error::custom),
            Some(ByteRepr::Raw(v)) => Ok(v),
        }
    }
}

/// Constant-time byte comparison to prevent timing side-channel attacks on sandbox tokens.
pub fn constant_time_eq(a: &str, b: &str) -> bool {
    let ab = a.as_bytes();
    let bb = b.as_bytes();
    if ab.is_empty() || ab.len() != bb.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (&x, &y) in ab.iter().zip(bb.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CompatibilityFingerprint {
    pub architecture: String,
    pub cpu_profile_hash: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cpu_features: Vec<String>,
    pub runsc_version: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kernel_release: String,
    #[serde(default)]
    pub arm64_no_pauth: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub gpu_driver_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ChunkRef {
    pub index: u32,
    pub offset: i64,
    pub length: i32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub blake3_hash: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub zero_chunk: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub reused_from_parent: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotManifest {
    pub schema_version: i32,
    pub snapshot_id: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub template_id: String,
    #[serde(default)]
    pub base_image: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub parent_snapshot_id: String,
    pub created_at: String,
    pub include_memory: bool,
    pub chunk_size: i32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub checkpoint_img_hash: String,
    #[serde(default)]
    pub checkpoint_img_size: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub pages_meta_img_hash: String,
    #[serde(default)]
    pub pages_meta_img_size: i64,
    #[serde(default)]
    pub pages_img_total_size: i64,
    #[serde(default)]
    pub dirty_memory_bytes: i64,
    #[serde(default)]
    pub dirty_chunk_count: i32,
    #[serde(default)]
    pub reused_chunk_count: i32,
    #[serde(default)]
    pub zero_chunk_count: i32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub rootfs_upper_tar_hash: String,
    #[serde(default)]
    pub rootfs_upper_tar_size: i64,
    #[serde(default)]
    pub chunks: Vec<ChunkRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hot_page_indices: Vec<u32>,
    #[serde(default)]
    pub origin_node_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub origin_peer_addr: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub bootstrap_token: String,
    #[serde(default)]
    pub fingerprint: CompatibilityFingerprint,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct InitPayload {
    pub sandbox_id: String,
    pub execution_id: i64,
    #[serde(default)]
    pub lifecycle_id: String,
    #[serde(default)]
    pub hostname: String,
    #[serde(default)]
    pub machine_id: String,
    #[serde(default)]
    pub access_token: String,
    #[serde(default)]
    pub env_vars: BTreeMap<String, String>,
    #[serde(default, with = "go_bytes")]
    pub entropy_seed: Vec<u8>,
    #[serde(default, rename = "wallClockUnixNano")]
    pub wall_clock_unix_n: i64,
    #[serde(default)]
    pub skip_thaw: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitResponse {
    pub sandbox_id: String,
    pub execution_id: i64,
    pub clock_delta_millis: i64,
    pub thawed: bool,
    pub machine_id_updated: bool,
    pub hostname_updated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct FreezeRequest {
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub drain_external_tcp: bool,
    #[serde(default)]
    pub sync_filesystems: bool,
    #[serde(default, rename = "waitInFlightTimeoutMs")]
    pub wait_in_flight_timeout: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FreezeResponse {
    pub frozen: bool,
    pub synced: bool,
    pub closed_external_conns: usize,
    pub active_managed_procs: usize,
    #[serde(rename = "quiesceDurationMicros")]
    pub quiesce_duration_micro: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthResponse {
    pub healthy: bool,
    pub version: String,
    pub sandbox_id: String,
    pub execution_id: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub lifecycle_id: String,
    pub frozen: bool,
    pub initialized: bool,
    pub uptime_millis: i64,
    pub clock_skew_nano: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ExecRequest {
    pub command: Vec<String>,
    #[serde(default)]
    pub working_dir: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub stdin: String,
    #[serde(default)]
    pub timeout_ms: i64,
    #[serde(default)]
    pub background: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecResponse {
    pub pid: u32,
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub duration_ms: i64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub timed_out: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessInfo {
    pub pid: u32,
    pub command: Vec<String>,
    #[serde(rename = "startedAtUnixMs")]
    pub started_at_unix_ms: i64,
    pub running: bool,
    #[serde(default)]
    pub exit_code: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct FileWriteRequest {
    pub path: String,
    #[serde(default, with = "go_bytes")]
    pub content: Vec<u8>,
    #[serde(default)]
    pub mode: u32,
}

// --- W3C TraceContext & FlightRecorder (wire-compatible with Go pkg/telemetry) ---

#[derive(Debug, Clone)]
pub struct TraceContext {
    pub trace_id: String,
    pub parent_span_id: String,
}

impl TraceContext {
    /// Parses a W3C `traceparent` header (`00-<32hex>-<16hex>-<2hex>`).
    pub fn parse_traceparent(raw: &str) -> Option<Self> {
        let parts: Vec<&str> = raw.trim().split('-').collect();
        if parts.len() != 4 {
            return None;
        }
        let tid = parts[1];
        let sid = parts[2];
        if tid.len() != 32 || sid.len() != 16 {
            return None;
        }
        if hex::decode(tid).is_err() || hex::decode(sid).is_err() {
            return None;
        }
        Some(Self {
            trace_id: tid.to_ascii_lowercase(),
            parent_span_id: sid.to_ascii_lowercase(),
        })
    }

    pub fn from_env() -> Option<Self> {
        std::env::var("TRACEPARENT")
            .ok()
            .and_then(|v| Self::parse_traceparent(&v))
    }

    pub fn new_root(seed: u64) -> Self {
        let nanos = now_unix_nanos() as u64;
        let tid = format!("{:016x}{:016x}", nanos, seed ^ 0x9e37_79b9_7f4a_7c15);
        Self {
            trace_id: tid,
            parent_span_id: String::new(),
        }
    }

    pub fn format_traceparent(trace_id: &str, span_id: &str) -> String {
        format!("00-{}-{}-01", trace_id, span_id)
    }
}

pub fn generate_span_id(counter: u64) -> String {
    let nanos = now_unix_nanos() as u64;
    format!("{:016x}", nanos.wrapping_mul(0x517c_c1b7_2722_0a95) ^ counter)
}

pub fn now_unix_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Formats a `SystemTime` as RFC3339Nano UTC (`YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ`) matching Go `time.Time` JSON.
pub fn format_rfc3339_utc(t: SystemTime) -> String {
    let dur = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = dur.as_secs() as i64;
    let nanos = dur.subsec_nanos();

    // Civil calendar conversion (Howard Hinnant's algorithm, pure safe math, zero external crate needed).
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let hour = rem / 3600;
    let minute = (rem % 3600) / 60;
    let second = rem % 60;

    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:09}Z",
        year, m, d, hour, minute, second, nanos
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordedSpan {
    pub trace_id: String,
    pub span_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub parent_span_id: String,
    pub service: String,
    pub name: String,
    pub start_time: String,
    pub end_time: String,
    pub duration_ms: f64,
    pub status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error_message: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DebugTraceResponse {
    pub service: String,
    pub count: usize,
    pub spans: Vec<RecordedSpan>,
}

pub struct FlightRecorder {
    service: String,
    capacity: usize,
    ring: Mutex<(VecDeque<RecordedSpan>, u64)>,
}

impl FlightRecorder {
    pub fn new(service: impl Into<String>, capacity: usize) -> Self {
        let cap = capacity.max(64);
        Self {
            service: service.into(),
            capacity: cap,
            ring: Mutex::new((VecDeque::with_capacity(cap), 1)),
        }
    }

    pub fn next_counter(&self) -> u64 {
        if let Ok(mut guard) = self.ring.lock() {
            let c = guard.1;
            guard.1 = guard.1.wrapping_add(1);
            c
        } else {
            1
        }
    }

    pub fn record(&self, span: RecordedSpan) {
        if let Ok(mut guard) = self.ring.lock() {
            if guard.0.len() >= self.capacity {
                guard.0.pop_front();
            }
            guard.0.push_back(span);
        }
    }

    pub fn query(
        &self,
        trace_id: Option<&str>,
        sandbox_id: Option<&str>,
        errors_only: bool,
        limit: usize,
    ) -> DebugTraceResponse {
        let max = if limit == 0 { 200 } else { limit };
        let mut matched = Vec::new();
        if let Ok(guard) = self.ring.lock() {
            for sp in guard.0.iter().rev() {
                if let Some(tid) = trace_id {
                    if !tid.is_empty() && sp.trace_id != tid {
                        continue;
                    }
                }
                if let Some(sbx) = sandbox_id {
                    if !sbx.is_empty()
                        && sp.attributes.get("sandbox.id").map(String::as_str) != Some(sbx)
                    {
                        continue;
                    }
                }
                if errors_only && sp.status != "ERROR" {
                    continue;
                }
                matched.push(sp.clone());
                if matched.len() >= max {
                    break;
                }
            }
        }
        matched.reverse();
        DebugTraceResponse {
            service: self.service.clone(),
            count: matched.len(),
            spans: matched,
        }
    }

    pub fn service_name(&self) -> &str {
        &self.service
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq("secret-token-123", "secret-token-123"));
        assert!(!constant_time_eq("secret-token-123", "secret-token-124"));
        assert!(!constant_time_eq("", ""));
    }

    #[test]
    fn test_traceparent_and_flight_recorder() {
        let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let ctx = TraceContext::parse_traceparent(tp).expect("valid traceparent");
        assert_eq!(ctx.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(ctx.parent_span_id, "00f067aa0ba902b7");

        let rec = FlightRecorder::new("ktd", 32);
        let mut attrs = BTreeMap::new();
        attrs.insert("sandbox.id".to_string(), "sbx-01".to_string());
        let now = format_rfc3339_utc(SystemTime::now());
        rec.record(RecordedSpan {
            trace_id: ctx.trace_id.clone(),
            span_id: generate_span_id(rec.next_counter()),
            parent_span_id: ctx.parent_span_id,
            service: "ktd".to_string(),
            name: "ktd.held_init".to_string(),
            start_time: now.clone(),
            end_time: now,
            duration_ms: 0.42,
            status: "OK".to_string(),
            error_message: String::new(),
            attributes: attrs,
        });

        let res = rec.query(None, Some("sbx-01"), false, 10);
        assert_eq!(res.count, 1);
        assert_eq!(res.spans[0].name, "ktd.held_init");
    }

    #[test]
    fn test_go_base64_bytes_compat() {
        let json_from_go = r#"{"sandboxId":"sbx-9","executionId":2,"entropySeed":"aGVsbG8="}"#;
        let parsed: InitPayload = serde_json::from_str(json_from_go).expect("parse Go JSON");
        assert_eq!(parsed.entropy_seed, b"hello");

        let re_encoded = serde_json::to_string(&parsed).expect("serialize");
        assert!(re_encoded.contains(r#""entropySeed":"aGVsbG8=""#));
    }
}
