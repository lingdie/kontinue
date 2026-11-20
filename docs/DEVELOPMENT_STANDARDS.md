# Kontinue development standards (Rust + Go)

This document defines the language split, safety rules, cross-component contracts, tracing requirements, and test gates for Kontinue.

---

## 1. Rust and Go split

Control-plane work and ecosystem integration stay in Go. Memory-sensitive, low-GC, and syscall-heavy paths stay in Rust. Do not couple the languages through CGo.

| Component | Language | Path | Why |
| :--- | :--- | :--- | :--- |
| `ktd` (in-sandbox PID 1) | **Rust** | `crates/ktd` | One process per sandbox. The binary stays under **2MB** RSS and has **no background GC thread**, which keeps `pages.img` smaller and avoids a GC pause at restore. Syscalls such as `waitpid`, cgroup, and `urandom` stay in safe wrappers. |
| `kontinue-runsc` (OCI wrapper) | **Rust** | `crates/kontinue-runsc` | `containerd-shim` starts it on every lifecycle call. Cold start is under **0.5ms**, avoiding the 5–10ms Go runtime startup. |
| `kontinue-arena-helper` (chunk engine) | **Rust** | `crates/kontinue-arena` | Scans multi-GB `pages.img` files in 2MiB aligned chunks, skips zero pages, and diffs them with SIMD BLAKE3. A Go slice of that size would inflate the heap. |
| `kontinue-cri-shim` | **Go** | `cmd/kontinue-cri-shim` | Uses the upstream `k8s.io/cri-api` gRPC stubs with no protocol drift from Kubelet or containerd. |
| `kontinued` (node daemon) | **Go** | `cmd/kontinued` | CRI slot orchestration, FUSE view lifetime, cross-node gRPC P2P, and Redis heartbeats. |
| `kontinue-apiserver` | **Go** | `cmd/kontinue-apiserver` | `client-go` for Kubelet `/checkpoint` and Carrier replenishment, plus `pgx/v5` and `go-redis/v9`. |
| `kontinue-gateway` and `ktctl` | **Go** | `cmd/kontinue-gateway`, `cmd/ktctl` | HTTP/WebSocket `ReverseProxy`, `singleflight` resume, and multi-endpoint trace waterfalls. |

### Zero-CGo

- Every Go binary builds with `CGO_ENABLED=0`.
- Rust and Go talk only through versioned JSON (`SnapshotManifest`), HTTP, gRPC, Unix sockets, or CLI child processes that carry `TRACEPARENT`. No shared pointers.

---

## 2. Coding and memory safety

### 2.1 Rust (`crates/`)

1. **No panics on the request path.** Do not call `.unwrap()` or `.expect()` on external input, file I/O, network, or JSON. Return `Result<T, KtError>`.
2. **Audit every `unsafe`.** Limit it to POSIX calls such as `libc::sync`, `libc::sethostname`, and `libc::waitpid`. Each site needs a `// SAFETY:` comment that states the memory and concurrency preconditions.
3. **Fixed-size streaming.** Hash `pages.img` in 2MiB chunks (`BufReader` / `pread`). Do not read a multi-GB image into one buffer.
4. **Constant-time credentials.** Compare `X-Bootstrap-Token` and `X-Kontinue-Token` in constant time. Do not put tokens or injected `EnvVars` into logs or trace attributes.
5. **Cross-platform cfg.** Linux-only features (`cgroup.freeze`, `/proc/net/tcp`, `sethostname`) use `#[cfg(target_os = "linux")]` and a `#[cfg(not(target_os = "linux"))]` stub so `cargo test` runs on macOS.

### 2.2 Go (`cmd/`, `internal/`, `pkg/`)

1. **Context and races.** Network, storage, and lock methods take `ctx context.Context` first. Shared-state changes must pass `go test -race`.
2. **Platform file split.** Linux-only calls (`go-fuse/v2` mounts, `FICLONE`) live in `*_linux.go`, with a portable implementation in `*_other.go`.
3. **Monotonic `ExecutionID` fencing.** Publishing or deleting `kt:route:{sandbox_id}` goes through a Redis Lua script that compares `execution_id`. Do not `DEL` a route that a newer lifecycle owns.

---

## 3. Tracing

1. **W3C `traceparent` is required.**
   - HTTP carries it in the `traceparent` header. Gateway and daemon responses set `X-Trace-ID`.
   - gRPC carries it in metadata.
   - A parent that starts `kontinue-runsc` or `kontinue-arena-helper` sets `TRACEPARENT`.
2. **Span names and attributes.**
   - Name spans `<component>.<operation>` (`gateway.auto_resume`, `apiserver.hydrate_slot`, `arena.chunk_diff`, `ktd.held_init`).
   - Important spans set `sandbox.id`, `snapshot.id`, and `execution.id`. Storage paths set `arena.tier` and `chunk.source`.
3. **`/debug/traces`.**
   - `kontinue-gateway`, `kontinue-apiserver`, `kontinued`, and `ktd` expose `GET /debug/traces` so `ktctl trace` can aggregate a waterfall without an external collector.

---

## 4. Scope limits

- **GPU snapshots stay deferred.** `cuda-checkpoint` may appear only as an optional field such as `CompatibilityFingerprint.gpuDriverVersion`. Core code through M4 does not link NVIDIA drivers or the CUDA runtime.
- **Milestone gate.** A milestone is done only when all of these pass:
  1. `cargo test --workspace`
  2. `go test -race ./...`
  3. `GOOS=linux` cross-compiles for `GOARCH=arm64` and `GOARCH=amd64`
