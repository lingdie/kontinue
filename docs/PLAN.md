# Kontinue phased plan and progress ledger

> This is the executable plan and the progress ledger. It matches the code. Read it with [ARCHITECTURE.md](./ARCHITECTURE.md) and [DEVELOPMENT_STANDARDS.md](./DEVELOPMENT_STANDARDS.md).

---

## 0. Current progress

### Architecture review follow-up · 2026-11-20

The delivery target is a locally reproducible lifecycle on two Kind workers. After an interrupted operation, PostgreSQL must identify the intended execution and snapshot, Redis must admit traffic only to its verified owner, and recovery must either finish the operation or expose a failure that preserves the resources needed for cleanup. The same deployment must enforce the public credential boundary and measure the time a caller actually waits for a usable guest.

1. **Create, resume and node ownership.** Interrupt placement and the transition to `RUNNING`. Compare PostgreSQL generation, Redis route, Carrier UID, runsc ID and guest PID across replacement. Delay an old release until the new generation is serving. Node replacement, rejected placement writes, two rejected `RUNNING` commits and a delayed generation 1 release after generation 2 starts have Kind evidence.
2. **Suspend and fork.** Interrupt checkpoint publication, child creation and child activation. A successful guest write before suspend must survive resume. The gateway must drain admitted requests before capture. Fork a resumed parent and prove each intended child boots exactly once with its own identity and the parent's memory marker. Retrying a partially completed fork must recover the same children.
3. **Snapshot publication and collection.** Share a chunk across two snapshots in the cluster object store. Interrupt publication and make one live manifest unavailable. Record object existence before, during and after collection. Only exclusively unreachable data may disappear, and collection must resume when the live set is readable.
4. **Tenant and guest permissions.** Exercise lifecycle and template access with two test tenants and the guest API as the workload identity. Verify that read and suspend responses omit workload secrets, bootstrap credentials and access tokens. Verify permitted file access and ordinary Unix permission denial on each guest entry point.
5. **Startup cost and checkpoint path.** Measure each request from create submission through completion of its first command. Report sample size, concurrency, warm-pool hit rate, actual bytes copied and p50/p95 for warm hits and cold fallback. Use checkpoint traces to decide whether the Kubelet/CRI round trip warrants a change. Record the measured latency against the existing target.
6. **Integrated acceptance.** Deploy one recorded revision into `kontinue-recovery`. Run Go race tests, Rust workspace tests, PostgreSQL/Redis integration tests, stock Kind E2E, the fault cases above and the benchmark. Record image versions, commands, assertions and remaining failure boundaries. Close each stage against its own evidence.

The node recovery baseline and its scope are recorded in [NODE_RECOVERY.md](./NODE_RECOVERY.md). For create and resume, `hack/test-activation-replay.py` on revision `c10db69` rejected the database commit and terminated the API. Generation 1 retained runsc PID 15494 and generation 2 retained PID 15870. In each case the route stayed absent until the commit succeeded, and a gateway command then reached that same process.

**Lifecycle verification on revision `d64d246`.** The default suite in `hack/test-activation-replay.py` ran on `kontinue-recovery` with `docker.io/kontinue/python-agent:r4`. It injected errors with per-sandbox PostgreSQL triggers and removed those triggers after each fault.

- A replacement API recovered the original Redis Carrier reservation on `slot-kt-carrier-f487588a`. Separate create and resume cases retained runsc PIDs 15959 and 16606 while PostgreSQL rejected their `RUNNING` commits.
- A fork from a generation 2 parent produced exactly two generation 3 children. Rejecting the second child insert left zero child rows. Both child identities subsequently committed together. Rejecting one child's activation commit left its route absent. After API replacement the two children retained runsc PIDs 19013 and 19079, carried distinct tokens, and served the parent's in-memory marker. Repeating the request key returned the same two IDs.
- An admitted foreground write delayed suspend and survived restore. Later gateway writes failed throughout the interrupted capture. After replacing both the API and the node daemon, the node returned the original capture receipt for `snap-susp-424d4313d2d14f93839d2cad`. Restore contained only the acknowledged write.

**Release and GC on revision `d0a53de`.** The `release` case in the same harness replayed a generation 1 release and received its original Pod UID. A request carrying generation 1 against the new slot was refused. Generation 2 kept runsc PID 21827, its Redis route and working gateway access.

The opt-in test below used actual objects in the cluster MinIO. A missing live manifest and a separate PostgreSQL row representing an unpublished sandbox capture each prevented removal of the three fixture chunks. After publication, the API reclaimed the exclusive chunk and retained both shared chunks. The template fault test then stopped MinIO while a builder held an established upload connection, terminated the API and held the metadata commit open. The new API recovered snapshot `snap-golden-563174d2498ca5644c88d35e`. GC retained the referenced chunk, the builder allocation was released, and a clone served its warmup marker. The builder implementation was `bcb38fc`.

```sh
KUBECONFIG="$PWD/tmp/recovery-kind/kubeconfig" KONTINUE_GC_KIND=kontinue-recovery \
  go test -count=1 -v ./internal/e2e -run '^TestKindSnapshotGC$'
```

### Final Kind acceptance · 2026-11-21

The tested runtime revision was `d974d75` on `codex/lifecycle-correctness`. The dedicated `kontinue-recovery` cluster used two ARM64 workers sharing a 12-vCPU Docker VM with 11.7 GiB RAM, kindnet, Kubernetes v1.37.0, containerd 2.3.4 and runsc release-20261116.0. The API, gateway and node daemon used image tag `recovery-1795204582`. Both workers imported `docker.io/kontinue/python-agent:files-v2` with Linux ktd from `917650c`, manifest digest `sha256:31509fb97a65a4535aabee60983e24b33fb839ded6df6ff974f2a7da4c960788`. The CRI shim built from the same revision matched SHA-256 `cc08caadc2aeac48ea083c28d24c8781e3a7bb0dd3a5f2ee8af8748c9c2155eb` on both workers. The cluster used its own PostgreSQL, Redis and MinIO.

1. **Allocation and ownership.** An API replacement reused Redis reservation `slot-kt-carrier-0645ee49`. With PostgreSQL rejecting the final write, create retained runsc PID 82330 and resume retained PID 82786. Their routes appeared after commit. A late generation 1 release left generation 2 serving with PID 85502. An interrupted delete replayed the original node receipt and removed the metadata row. The new daemon also completed an already-requested delete for a disconnected mount created by an older binary. Sandbox `sbx-c388331f5a98` returned 404 and the worker no longer had that mount, view or staging directory.
2. **Suspend and fork.** A gateway write admitted before suspend survived snapshot `snap-susp-c70cc1fd339142b23e7896ba` after replacing both the API and node daemon. Later writes were rejected. A generation 2 parent produced two generation 3 children with the expected RAM marker, distinct credentials and unchanged runsc PIDs 84480 and 84538 across interrupted commits. Replaying the fork key returned the same children.
3. **Publication and GC.** The builder’s upload was interrupted at MinIO and its API process was killed. Publication recovered as `snap-golden-149e896e6de214ddfadf664e`, then a clone served its warmup marker. Fixture `gc-k3kgmzdliw42` independently withheld a live manifest and an unpublished capture. Both stopped physical collection. After publication, MinIO retained two shared chunks and removed the exclusive chunk.
4. **Permissions.** Tenant A’s GET and suspend responses excluded its workload environment and credentials. Tenant B could not list or use its template, control or wake its sandbox, or use its application port. Both trace endpoints rejected a tenant credential. The file API created a binary file owned by UID/GID 1000. Mode `000` denied reads and writes through files, exec and WebSocket PTY. Command exit emitted a WebSocket close frame and the next suspend succeeded.
5. **Checkpoint decision.** Retain native Kubelet checkpoint and its descriptor tar. For `snap-susp-3d54bc3b8c9794083325eb1f`, API-to-Kubelet took 113.82 ms, the CRI shim took 111.85 ms and node execution took 109.66 ms. The gVisor checkpoint took 26.48 ms and ingest took 79.24 ms, including 68.49 ms uploading to MinIO. These are nested durations. The measured interval outside node execution was 4.16 ms. The API and CRI trace IDs differ across Kubelet. The test matched the Carrier, timestamps and descriptor path and joined the shim and node by trace ID.
6. **Integrated regression.** The Go race suite, Rust workspace tests, `go vet`, Linux builds for amd64 and arm64, and integration tests against disposable PostgreSQL and Redis passed. On Kind, the stock E2E served all eight simultaneous wake callers in 207 ms and collected 21 spans. The authenticated Python demo exercised files, a live application port, fork, HTTP wake and cleanup. Both guest execution generations preserved the PID and SHA-256 of an 8 MiB guest buffer in the live restart case. Removing runsc or killing the serving ArenaFS process resulted in a PostgreSQL failure record, withdrawal of the Redis route and HTTP 503 at the gateway. After observing this quarantine on `restart-c09733257929`, explicit deletion returned success, removed the mount, staging and view, and left GET returning 404.

**Time to first command on Kind.** Each sample starts at the client immediately before submitting create and ends after verifying the gateway returned `ready` from `/bin/echo`. Each run baked a pinned template. The Redis idle count was sampled before requests started, and the replenisher continued running during each test. All 72 creates and commands succeeded. Every sample used an L1 tmpfs view and reported 0 bytes written to prepare that view. A cold Carrier can still use a hot memory snapshot.

| Requests / concurrency | Initial idle Carriers | Warm hits | Warm p50 / p95 (ms) | Cold fallbacks | Cold p50 / p95 (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| 16 / 1 | 3 | 13 (81.25%) | 60.39 / 96.65 | 3 | 728.94 / 904.85 |
| 16 / 4 | 20 | 16 (100%) | 143.98 / 162.60 | 0 | — |
| 40 / 24 | 9 | 14 (35%) | 244.00 / 1190.55 | 26 | 1686.75 / 3785.82 |

The earlier 16-request, four-concurrent run also had all warm hits (19 initially idle) and measured 449.47 / 509.12 ms p50 / p95. The final run combines same-filesystem L1 links and exact-reference rootfs preparation. The pinned file and clone view had identical device/inode pairs for all three checkpoint files. A separate L2 resume materialized 13,458,661 bytes. The 400 ms reference was met at p95 in the all-warm four-concurrent run. The burst exceeded it even among warm hits, and cold Carrier provisioning took seconds. PostgreSQL contained zero rows for the 72 benchmark sandbox IDs after cleanup.

**Cross-node wake with 1 GiB of resident data.** Three trials on `2d2f1cc` took the origin daemon offline, then verified an echo and the entire random buffer on the destination through a mounted ArenaFS view. Echo completed in 367.36, 413.08 and 373.34 ms. The matching PID and full-buffer SHA-256 returned in 4.55, 3.71 and 4.34 s respectively. The final runtime repeated the test in the reverse direction of the first trial. Sandbox `restart-625d36320885` returned its first echo in 470.00 ms. At 2.68 s, guest PID 5 returned the original SHA-256 `fac5aaa7ca70e06624e15c111408c5e2b7571276378fb3e0bc69a8a870020d09`. Explicit deletion removed the healthy FUSE mount and staging. Two of the four echoes exceeded 400 ms. The timings include the host Docker exec that starts the NodePort client.

**Scope of the result.** The public tenant API, gateway, guest Unix identity and process-replacement cases are covered here. The worker disk, Carrier and runsc must survive for adoption. Losing an ArenaFS serving process quarantines that allocation and withdraws traffic. Rebuilding base images and templates deploys a newer ktd. Guest memory snapshots retain the binary they captured. Per-tenant network egress, loss of worker storage and failover of the control-plane databases remain separate work.

**Reproduction on the prepared test cluster.** Set `KUBECONFIG` to the dedicated cluster, with two tenants in the Kind credentials Secret. For HTTP clients on this workstation, two separate `kubectl -n kontinue-system port-forward` processes exposed `svc/kontinue-apiserver 38281:8081` and `svc/kontinue-gateway 38280:8080`. Export the local tenant credential as `KONTINUE_TENANT_TOKEN` for the benchmark and demo, and the admin trace credential as `KONTINUE_TRACE_TOKEN` to retrieve the demo traces. The two fault harnesses use the control plane’s NodePorts so service replacement can interrupt an in-flight request.

```bash
export KUBECONFIG="$PWD/tmp/recovery-kind/kubeconfig"
export BASE_IMAGE=docker.io/kontinue/python-agent:files-v2
export API_URL=http://127.0.0.1:38281 GW_URL=http://127.0.0.1:38280
export KONTINUE_API_URL="$API_URL" KONTINUE_GATEWAY_URL="$GW_URL"
GOFLAGS=-count=1 make test
go vet ./...
make build-linux-go
# Point KONTINUE_TEST_POSTGRES and KONTINUE_TEST_REDIS at disposable instances.
go test -count=1 -race -v ./internal/e2e ./internal/store
make kind-e2e
python3 examples/demo_lifecycle.py
args=(--cluster kontinue-recovery --kubeconfig "$KUBECONFIG" --base-image "$BASE_IMAGE")
python3 hack/test-activation-replay.py "${args[@]}"
KONTINUE_GC_KIND=kontinue-recovery go test -count=1 -v ./internal/e2e -run '^TestKindSnapshotGC$'
python3 hack/test-node-recovery.py "${args[@]}"
BENCH_MODE=startup BENCH_SANDBOXES=16 BENCH_CONCURRENCY=4 python3 hack/benchmark_overflow.py
BENCH_MODE=startup BENCH_SANDBOXES=16 BENCH_CONCURRENCY=1 python3 hack/benchmark_overflow.py
BENCH_MODE=startup BENCH_SANDBOXES=40 BENCH_CONCURRENCY=24 python3 hack/benchmark_overflow.py
```

### Cold Carrier burst follow-up · 2026-11-21

The baseline was runtime `d974d75` (image tag `recovery-1795204582`). The optimized runtime was `f5d45a4` (image tag `recovery-1795206040`). Both ran with the same gateway configuration, Carrier image, default pool watermarks and guest image on the same two Kind workers. Each cell pools three runs of 40 requests with 24 concurrent clients. The clock starts immediately before the create POST and stops after the gateway returns the verified `/bin/echo ready` result. The pinned template is baked before timing. Percentiles use the benchmark's existing rounded rank over sorted successful requests.

| Initial idle Carriers | Runtime | Success / attempts | All successful p95 (ms) | Warm-hit p95 (ms) | Cold-fallback p95 (ms) |
| --- | --- | ---: | ---: | ---: | ---: |
| 19 (9 and 10) | Baseline | 120 / 120 | 2976.26 | 1198.87 (68) | 3551.79 (52) |
| 19 (9 and 10) | Optimized | 120 / 120 | 2772.15 | 695.54 (57) | 2785.87 (63) |
| 0 (0 and 0) | Baseline | 107 / 120 | 3063.95 | 1314.53 (15) | 3063.95 (92) |
| 0 (0 and 0) | Optimized | 120 / 120 | 2674.62 | — | 2674.62 (120) |

The baseline exposed cold Carriers to the common pool as soon as the worker discovered them. A later request could claim one as a warm hit while its creator waited until timeout. This accounts for warm hits in the zero-idle baseline and shifts the warm/cold counts in the default-tier comparison. The zero-idle baseline percentiles describe its 107 successful requests only. The 13 failures exceeded the 5% tail. With both pools initially empty the optimized per-run p95s were 2898.91, 2350.08 and 2226.92 ms.

Each primary cold Pod now carries a lifecycle label and a short-lived, exact slot expectation. The worker atomically publishes a ready Carrier to that lifecycle’s durable reservation. Once the expectation expires or is cancelled, a late Carrier can join the normal idle pool. Placement balances outstanding cold work across compatible nodes. A worker coalesces CRI start events with periodic discovery and checks four new Carriers in parallel. The API waits on Redis without running a worker-wide scan for each cold request.

With the default tier, the measured p95 for waiting on Carrier readiness fell from 2726.88 to 1361.40 ms. The optimized cold traces still measured p95 1336.21 ms to acquire a PostgreSQL lifecycle lock. A trial with 32 advisory-lock connections shortened that queue while increasing kubelet contention, taking default-tier cold p95 to 3556.06 ms. Capping pending cold Pod starts at six per worker worsened the zero-idle p95 to 3971.02 ms. The final runtime retains the original database connection budget and leaves both trials out of its provisioning path.

Run the following with the API and gateway forwards and tenant token from the prepared-cluster instructions above. The zero-idle mode uses a unique Carrier tier per run, outside the default tier's replenisher. It checks the Redis idle count for that tier on each worker before making requests.

```sh
BENCH_MODE=startup BENCH_SANDBOXES=40 BENCH_CONCURRENCY=24 python3 hack/benchmark_overflow.py
BENCH_EMPTY_POOL=1 BENCH_MODE=startup BENCH_SANDBOXES=40 BENCH_CONCURRENCY=24 python3 hack/benchmark_overflow.py
```

The Go race suite, Rust workspace tests, `go vet`, and store integration against PostgreSQL and Redis passed. The optimized deployment also passed `make kind-e2e`, both fault harnesses and physical MinIO GC. The node regression included the 1 GiB cross-node checksum, daemon adoption of a live guest, and explicit removal of a disconnected ArenaFS view. After cleanup, PostgreSQL contained no sandbox rows for the 1120 measured IDs across baseline, tuning trials and final runs.

### PostgreSQL lifecycle lock and Carrier readiness follow-up · 2026-11-21

This follow-up used the same two Kind workers and the zero-idle Carrier tier. Each cohort ran three rounds of 40 requests with 24 concurrent clients. The fresh baseline (`f5d45a4` runtime) had first-command p95 of 2576.51 ms and 0/120 failures. Lifecycle-lock acquisition took 862.24 / 1326.81 ms p50 / p95, against 743.92 / 1080.70 ms waiting for the Carrier to become claimable. The Pod Create step took 10.72 / 36.89 ms.

The instrumented control (`b7859f2`) retained the same lifecycle lock scope. Connection acquisition alone took 1022.40 / 1371.16 ms. The advisory query and surrounding overhead, measured as the difference within each request, took 0.28 / 3.40 ms. Trials with 16, 18 and 20 connections per pool yielded 2650.70, 2666.89 and 2656.16 ms first-command p95. Each trial completed 120/120 requests and raised the p95 of Carrier readiness relative to the fresh baseline. The final deployment retained the original connection budget.

Sandbox activation now persists its intent and starts or rejoins the exact cold Pod while holding the lifecycle lock. The API releases the lock during a read-only wait on the catalog. The node can publish the ready reservation only while the matching expectation remains. The API reacquires the lock and re-reads the execution generation, lifecycle ID and state before it claims shared capacity or activates a guest. Concurrent idempotent creates can reuse a pending Pod. A delete can clear the expectation or return an unused reservation before readiness, and a stale waiter cannot activate a replacement sandbox with the same ID.

Set `KONTINUE_TRACE_TOKEN` to the operator credential in the prepared-cluster instructions. The startup benchmark then writes per-request phase timings and the number of matched create traces to its JSON report. Lifecycle timing includes the initial acquisition and the elapsed time reacquiring the lock, including any retries. PostgreSQL connection time is nested within that phase. Pod Create and readiness use separate spans. The table reports per-request percentiles, so the phase percentiles are independent.

| Phase | Instrumented control p50 / p95 (ms) | Final runtime p50 / p95 (ms) |
| --- | ---: | ---: |
| Lifecycle lock acquisition and reacquisition | 1022.53 / 1371.22 | 5.53 / 63.88 |
| PostgreSQL connection acquisition | 1022.40 / 1371.16 | 0.40 / 59.66 |
| Carrier Pod Create | 15.54 / 50.73 | 18.61 / 43.31 |
| Carrier readiness | 923.96 / 1261.13 | 1381.40 / 2062.51 |
| Create through first command | 2150.08 / 2887.29 | 1719.44 / 2549.92 |

Both instrumented cohorts matched 120/120 request traces. The API initially ran `52633fc` with the unlocked wait and produced 2186.16 ms pooled first-command p95. The final API image `startup-5f1f3a7` added retry timing. Its three p95s were 2597.21, 2201.89 and 2402.06 ms. Pooling the two baseline cohorts and the two optimized cohorts gave **2784.50 → 2413.97 ms first-command p95 (13.3% lower)**. Failures were **0/240 → 0/240**. Every request in these four cohorts used the cold path. The 16/18/20 connection trials are excluded. Carrier readiness became the largest measured phase.

The lock-scope revision passed the Go race and Rust suites, `go vet`, Linux builds, PostgreSQL/Redis integration, live Kind E2E, lifecycle replay, snapshot GC and node recovery. The new `cold-placement` case paused both discovery processes and verified that delete completed during the wait, the stale request returned 409, and concurrent retries used one replacement Pod and runsc instance. The final instrumented image reran both Kind E2E and this case. PostgreSQL held no rows for the measured sandbox IDs after cleanup. Both workers were Ready.

### Carrier startup follow-up · 2026-11-21

The two-worker `kontinue-recovery` cluster reused the guest image, Carrier image, pool watermarks and PostgreSQL budget from the preceding round. The zero-idle baseline was API image `startup-5f1f3a7`. The final API and worker used `recovery-1795208410` (runtime `dbb4382`). Both used a 50m Carrier CPU ceiling. Each cohort used three rounds of 40 requests and 24 clients. The previous 2062.51 ms Carrier readiness p95 came from a different run on the same baseline code.

The instrumented cohorts joined 120 of 120 Carriers on each side. The API-side benchmark recorded the real Carrier Pod name and successful Pod Create response time. The containerd journal identified the Pod UID, sandbox ID and placeholder ID. CRI start events provided the placeholder's recorded start time and independently matched the UID. Temporary wrappers timed successful kindnet CNI ADD invocations. The node's timestamped registration log followed successful Redis registration. The wrappers were removed for the alternating normal-path comparison.

| Phase | 50m baseline p50 / p95 (ms) | Confirmed-network path p50 / p95 (ms) |
| --- | ---: | ---: |
| Pod Create acknowledgement to first CRI call | 66.88 / 166.80 | 74.12 / 225.82 |
| Kubelet gaps outside the measured CRI calls, including the preceding row | 110.24 / 208.71 | 153.61 / 339.14 |
| CRI `RunPodSandbox` | 631.51 / 949.68 | 833.82 / 1286.52 |
| CNI ADD window within `RunPodSandbox` | 19.70 / 101.60 | 22.71 / 155.38 |
| Last CNI ADD completed to `RunPodSandbox` return | 512.49 / 814.24 | 696.09 / 984.28 |
| Sandbox return to placeholder running | 346.10 / 654.60 | 579.52 / 1239.79 |
| Sandbox return to node's Redis registration log | 385.98 / 714.61 | 52.68 / 231.36 |
| Running placeholder to node's Redis registration log | 31.19 / 118.49 | −525.71 / −196.35 |

CNI timing is the wall interval from the first captured plugin start to the last finish. Plugins can overlap. Individual phase percentiles have different request ranks, and nested phases overlap. The largest baseline interval inside `RunPodSandbox` was the runtime work after CNI ADD.

CRI reports the infrastructure sandbox ready after its network and pause process start. The legacy worker then waits for kubelet to create and start the placeholder and probes its IP before publishing the slot. The new worker checks the exact reservation, sandbox identity, Pod UID, address and network namespace. The standalone driver can start the requested guest at that point. All 120 instrumented cold reservations entered Redis before their placeholders started. An unused reservation is removed on cancellation, and the worker can subsequently discover the fully started Pod for the shared pool. The native checkpoint binds the running placeholder by ID and UID on demand. Instrumented Carrier readiness p95 was 1698.81 → 1484.47 ms. First-command p95 was 2014.10 → 2198.50 ms in those captures.

The 500m trial reduced CFS throttling and the instrumented post-CNI interval. Its initial normal-path end-to-end p95 was effectively unchanged (2647.73 → 2646.71 ms). That change was reverted. The dedicated cluster's temporary 500m Carriers were resized back to 50m. Three probe guests on the final version had Carrier `cpu.max=5000 100000` and independently enforced 200m guest CPU and 64 MiB memory.

The final comparison alternated the API images in control, optimized, optimized, control, control, optimized order with the same workers and native CNI binaries. Both cohorts completed 120/120 zero-idle requests. Pooled Carrier readiness p95 was **1881.94 → 1506.87 ms (19.9% lower)**. Pooled first-command p95 was **2557.65 → 2215.65 ms (13.4% lower)**. The three optimized rounds recorded readiness p95s of 1320.15, 1602.85 and 1281.79 ms, with first-command p95s of 2067.45, 2340.96 and 2143.37 ms.

The optimized deployment passed Go race and Rust tests, `go vet`, Linux builds, real PostgreSQL/Redis integration, Kind end-to-end and lifecycle replay, physical GC in MinIO, and node recovery including the 1 GiB cross-node checksum. The cold-placement fault case verified cancellation, an idempotent retry on the original replacement Pod, and immediate native suspend followed by resume and successful command execution. Both workers were Ready and all 29 remaining Carrier Pods had a 50m CPU ceiling.

For offline analysis, run `python3 hack/measure-kind-carrier.py --directory tmp/recovery-kind/cold-phase --capture phase3-before --output tmp/recovery-kind/cold-phase/phase3-before-timelines.json`. Pass `phase3-network` in both positions for the confirmed-network capture. The raw journal, CRI event stream, CNI timings and timestamped node registration log are alongside the benchmark JSONs.

### Historical milestone measurements

| Milestone | Done | Summary | Verified | Still open |
| :--- | :---: | :--- | :--- | :--- |
| **M0: Base, protocol, and traces** | **100%** | Complete | Go (`-race`) and Rust workspace tests pass. `ktctl trace` aggregates a cross-language waterfall. | None |
| **M1: Single-node snapshot and fast clone** | **100%** | Closed | Kind + gVisor clone **23.7ms–48ms**. User processes are uid 1000. `/v1/pty` is a WebSocket terminal. | Egress allowlist is deferred |
| **M2: Control plane and auto-resume** | **100%** | Complete | Postgres advisory leader lock and `kt:inflight` idle exemption verified on Kind. | None |
| **M3: Cross-node ArenaFS, P2P, prefetch** | **95%** | Independent MinIO. The view stays until sandbox release. | 1GB of random data. SHA-256 before and after restore is `2bd667fa429401ad3eba5c13e372b44aba3f7ac5c1ad7ee423d0d7cf8c0e9121`. 515/515 chunks landed on the destination. | First-instruction time is still above the 400ms target |
| **M4: Fallback, GC, and benchmarks** | **90%** | Cold fallback, physical GC, and node ownership reconciliation are in | Incompatible hardware is rejected. There is no filesystem-only restore. | Multi-tenant nftables and GPU remain deferred |

---

## 1. Milestone dependencies

```mermaid
flowchart TB
    subgraph m0 [M0: skeleton and traces - 100%]
        M0_1["M0.1 Go + Rust workspaces"]
        M0_2["M0.2 Cross-language contracts and CAS Range storage"]
        M0_3["M0.3 OpenTelemetry + FlightRecorder + ktctl trace"]
        M0_1 --> M0_2 --> M0_3
    end

    subgraph m1 [M1: single-node clone - 100%]
        M1_1["M1.1 In-sandbox ktd"]
        M1_2["M1.2 OCI wrapper kontinue-runsc"]
        M1_3["M1.3 kontinue-cri-shim"]
        M1_4["M1.4 kontinued: GVisor driver + L1 tmpfs"]
        M1_1 --> M1_4
        M1_2 --> M1_4
        M1_3 --> M1_4
    end

    subgraph m2 [M2: control plane and gateway - 100%]
        M2_1["M2.1 Postgres and Redis Lua"]
        M2_2["M2.2 Rust 2MiB BLAKE3 chunker"]
        M2_3["M2.3 kontinue-apiserver: Best-of-K, replenish, fork"]
        M2_4["M2.4 kontinue-gateway: host routing + singleflight resume"]
        M2_1 --> M2_3
        M2_2 --> M2_3
        M2_3 --> M2_4
    end

    subgraph m3 [M3: cross-node page faults and P2P - 95%]
        M3_1["M3.1 Arena P2P gRPC"]
        M3_2["M3.2 ArenaFS FUSE virtual pages.img"]
        M3_3["M3.3 Hot-page recording and prefetcher"]
        M3_1 --> M3_2 --> M3_3
    end

    subgraph m4 [M4: production hardening - 90%]
        M4_1["M4.1 Cold carrier fallback when the warm pool is empty"]
        M4_2["M4.2 Physical CAS reference-count GC and node ownership recovery"]
        M4_3["M4.3 Kubernetes manifests, Kind lifecycle, overflow bench"]
        M4_1 --> M4_3
        M4_2 --> M4_3
    end

    m0 --> m1 --> m2 --> m3 --> m4
```

---

## 2. What each phase actually shipped

### M0: Foundation, contracts, and traces (100%)

- [x] **M0.1 Workspaces and build.** `go.mod`, `Cargo.toml`, `Makefile`, `.dockerignore`, and `docs/DEVELOPMENT_STANDARDS.md`.
- [x] **M0.2 Shared protocol.** `pkg/protocol` (`constants.go`, `manifest.go`, `ktd.go`, `rpc.go`) and a JSON-compatible Rust crate at `crates/kt-protocol`, including Go's base64 `[]byte` encoding.
- [x] **M0.3 Content-addressed storage.** `LocalFSBackend` and `HTTPObjectStoreBackend` with `Range: bytes=start-end`. Covered by `pkg/storage/cas_test.go`.
- [x] **M0.4 Tracing.** W3C `traceparent` on HTTP, gRPC, and child-process env. Responses set `X-Trace-ID`. Go and Rust ring-buffer `FlightRecorder` at `/debug/traces`. `ktctl trace` aggregates the waterfall.

### M1: Single-node snapshot and Carrier clone (100%)

- [x] **M1.1 `ktd`.** Static Linux binary around 1.0MB, no GC thread. `/internal/freeze` gates new processes, calls `sync()`, and freezes the cgroup. `/internal/init` supports `Expect: 100-continue`, fences `ExecutionID`, rotates the bootstrap token to an access token, injects env, mixes `/dev/urandom`, and rewrites `machine-id`. `/v1/process/exec` runs foreground and background processes. The PID-1 `waitpid(-1)` `ECHILD` race under 240 concurrent execs is fixed. `/v1/files` reads and writes files.
  - [x] Freeze finds external TCP sockets from `/proc/net/tcp` inodes, duplicates them with `pidfd_getfd`, and calls `shutdown`. gVisor returns `EPROTONOSUPPORT` for netlink `SOCK_DIAG` and `ENXIO` for a direct open of `/proc/pid/fd`. The sandbox process needs `CAP_SYS_PTRACE` for `pidfd_getfd`. That capability is dropped before the user process runs.
  - [x] `/v1/pty` is `GET` plus `Upgrade: websocket`, then `openpty`. Frames carry terminal bytes. A Kind check through the gateway received `pty-ok`.
- [x] **M1.2 `kontinue-runsc`.** Injects `--platform=systrap --overlay2=root:self --network=host` so a restored sandbox takes the Carrier Pod CNI netns and its IP. `create` strips `dev.gvisor.*` and `kontinue.sh/*` annotations so `validateRestoreSpec` passes. Restore is rewritten to `--background=true --direct-io=off`. A detached `runsc wait --restore` reports completion. That callback does not unmount the ArenaFS view.
- [x] **M1.3 `kontinue-cri-shim`.** A host systemd daemon. It proxies ordinary CRI, forwards Kubelet `CheckpointContainer` to `kontinued`, and keeps the placeholder-to-active container map so Kubelet PLEG does not kill the sandbox on swap.
- [x] **M1.4 `kontinued`.** `GVisorSandboxDriver` caches a read-only base rootfs with `ctr images mount` and uses separate file handles so `runsc-sandbox` does not inherit a blocking pipe. `CarrierPodWatcher` discovers ready Carrier Pods and their CNI netns. L1 is a tmpfs hardlink pool.
- [x] **Measured.** Kind v1.37.0 and gVisor `release-20261116.0`: 1-to-N clone **23.7ms–48ms**, under the 200ms target.

### M2: Database control plane, incremental suspend, and the gateway (100%)

- [x] **M2.1 Stores.** Postgres via `pgxpool` for templates, snapshots, and sandboxes. Redis Lua for idle-slot `SPOP` plus `kt:carrier:used:*`, and execution/slot fencing on `kt:route:{id}`. PostgreSQL session locks serialize lifecycle transitions. The memory store implements the same operations. `MetadataStore.TryLeader` uses `pg_try_advisory_lock` so only one replenisher and one evictor run.
- [x] **M2.2 Chunker.** `kontinue-arena-helper` streams 2MiB chunks, skips aligned zero pages, hashes with SIMD BLAKE3, and reuses parent hashes. Go `arena.Manager` can fall back to the same diff and upload.
- [x] **M2.3 API server.** Lifecycle REST with `Idempotency-Key`. Best-of-K placement by memory headroom and hardware fingerprint, with jitter so equal nodes do not herd. Kubelet HTTPS client for the v1.30–v1.37 checkpoint API. Online fork and an idle sweeper. Lifecycle release retries UID-fenced Carrier deletion from the old allocation. The gateway records request leases in `kt:admissions:{id}`. The sweeper skips suspend while `Inflight > 0`.
- [x] **M2.4 Gateway.** `<port>-<sandbox-id>.<domain>` and `X-Kontinue-Sandbox-ID` routing. `/internal/*` returns 404. `singleflight.Group` collapses concurrent resumes. A suspended in-memory Python server was resumed through this path.

### M3: Cross-node ArenaFS, P2P, and hot-page prefetch (95%)

- [x] **M3.1 P2P.** `NodeArenaP2PService` serves `GetManifest`, `GetChunk`, and `StreamChunks`. A restoring node tries the origin node's memory or NVMe, then remote CAS. Covered by `internal/node/arena/arena_test.go`.
- [x] **M3.2 ArenaFS.** `go-fuse/v2` exposes a read-only virtual `pages.img`. A bitmap and `singleflight` fetch missing 2MiB chunks. `wait --restore` does not unmount the view. The view stays until `ReleaseSlot`, so a later guest read can still fault a page in.
  - [x] On Kind, `CAS_HTTP_URL` points at a dedicated MinIO. After local chunk files were deleted and the origin node was removed, a 1GB random buffer restored with the same SHA-256, `2bd667fa429401ad3eba5c13e372b44aba3f7ac5c1ad7ee423d0d7cf8c0e9121`. All 515 chunks were on the destination node. The resume API returned in 0.40s.
- [x] **M3.3 Hot pages.** `StartHotPagePrefetcher` and `RecordedHotPages` exist. `HotPageIndices` is the order of real `pages.img` reads, recorded when the L3 view is released. An earlier Kind trace started `0, 1, 2, 3, 4, 26, 14, 16`.

### M4: Fallback, GC, and the cluster bundle (90%)

- [x] **M4.1 Cold carrier fallback.** When Redis has no idle slot, the API binds a new Carrier Pod to a compatible worker. Discovery assigns the ready Pod to its waiting lifecycle. The watcher uses CRI start events and a 100 ms polling fallback. The carrier image starts `pause`. See the cold burst follow-up for current results.
- [x] **M4.2 GC and heal.** After Postgres metadata is deleted, `RunEvictionAndGCSweep` builds the live BLAKE3 set from remaining manifests and deletes unreferenced CAS objects. On startup, the node verifies each journaled owner against CRI, runsc, the guest, and the restore view before restoring routing. [Recovery evidence](NODE_RECOVERY.md)
- [x] **M4.3 Manifests and benches.** `deploy/00` through `deploy/05`, `hack/deploy-kind.sh`, `hack/e2e-kind-test.sh`, `examples/demo_lifecycle.py`, and `hack/benchmark_overflow.py` (`overflow`, `warm-only`, `sustained-qps`, plus a JSON report with QPS, span rollup, and carrier RSS).
- [ ] **After M4.** Per-sandbox nftables allowlists on the Carrier netns. GPU `cuda-checkpoint`.

---

## 3. Defects, unfinished code, and the next route (2026-11-20)

Working path: template bake, memory clone, exec, files, suspend, resume, fork, gateway singleflight resume, and traces. A warm-pool create is about 90ms. User code runs as uid 1000.

### 3.1 Confirmed defects

| Defect | Evidence | Status |
| :--- | :--- | :--- |
| Create after the warm pool is empty takes about 1.3–3.5s | Kind overflow bench. Time is in Kubelet and CNI. | Known limit. Not scheduled. |
| A sandbox can reach the node Kubernetes API | A live connection to `192.168.97.4:6443` | Deferred until network isolation |
| Old templates break after a spec change | OCI args or capability changes fail `validateRestoreSpec` | Rebake templates after a spec change |

### 3.2 Unfinished code

- `AllowFilesystemOnlyFallback` is still on the API. Placement no longer uses a disk-only restore.
- Nothing checks that the Carrier image is already on the node before create.
- Cold Carrier discovery uses CRI start events with periodic checks to cover missed events.
- The 1GB random-buffer digest matches before and after restore, and all 515 chunks landed on the destination. The view unmounts when the sandbox is released, and that close deletes the view directory and the staging directory. An interrupted ArenaFS server leaves its allocation quarantined across daemon restart.

Out of scope: heterogeneous filesystem-only restore, `hostNetwork` Carriers, and GPU snapshots. The network allowlist waits.

### 3.3 Route, verified on Kind on 2026-11-20

1. **Real resources.** Create accepts `cpuMillis` and `memoryBytes`. After `runsc` starts, the host cgroup v2 directory `/sys/fs/cgroup/kontinue/<container>/` gets `cpu.max` and `memory.max`. Those values are not in the OCI spec and do not change the Carrier Pod. Placement rejects a request that would pass 80% of node memory or `nproc`. A 64MiB and 200 millicpu sandbox wrote `memory.max=67108864` and `cpu.max=20000 100000`. A 1TB request was rejected.
2. **No fake root inside the sandbox.** Before `exec`, the user process drops `DAC_OVERRIDE`, `KILL`, `NET_RAW`, `SYS_CHROOT`, and `MKNOD`. Seccomp rejects `unshare` and `clone` when the flags include `CLONE_NEWUSER`. If the kernel accepts seccomp user notifications, `ktd` also checks `clone3` flags. On Kind, `unshare --user` returns `Operation not permitted`. `CapBnd=0000000020000419` (`CHOWN`, `FOWNER`, `FSETID`, `NET_BIND_SERVICE`, `AUDIT_WRITE`). `/etc/shadow` is unreadable. Python threads still start.
3. **Interactive terminal.** `GET /v1/pty` completes the WebSocket handshake and calls `openpty`. The gateway forwards the upgrade. Kind returned `101` and the command output `pty-ok`.
4. **Cross-node restore.** `CAS_HTTP_URL` points at in-cluster MinIO. The ArenaFS view stays mounted after `runsc wait --restore` and is released with the sandbox, so a later read can still fetch a page from MinIO. A 1GB random buffer was checksummed, local chunks were deleted, and the origin node was taken offline. The same process hashed the buffer again and got `2bd667fa429401ad3eba5c13e372b44aba3f7ac5c1ad7ee423d0d7cf8c0e9121`. All 515 chunks were on the destination. The resume API returned in 0.40s.
5. **Network isolation.** An egress allowlist on the Carrier netns. Not started.
6. **Cold start.** Only if an empty warm pool becomes a real complaint. The lever is a pre-warmed sandbox pool, not more Kubelet tuning.

## 4. Kubelet sandbox cold path (not scheduled)

Cold create is `API Create → kubelet sync → RunPodSandbox → CNI ADD → pause → watcher inserts Redis`. On Kind the time is kubelet sync and `RunPodSandbox`, not the pause process. Config tweaks usually save 10–30%. A step change needs a pre-warmed sandbox or a path that skips `RunPodSandbox`.

| Priority | Item | Status |
| :--- | :--- | :--- |
| Short | Disable Carrier `AutomountServiceAccountToken` | Done |
| Short | Refuse a cold image pull when the Carrier image is not on the node | Open |
| Short | Trigger Carrier discovery on CRI start events | Done with polling fallback |
| Short | Cap in-flight cold creates per node | Partial (`createSem=8`) |
| Medium | A local pool of pre-warmed PodSandboxes, so cold create claims one | Open |
| Medium | Raise the idle watermark from recent create QPS | Open |
| Long | A netns pool that is not created by Kubelet, with a dedicated checkpoint entry | Open |
| No | `hostNetwork` Carriers. That breaks one sandbox per netns. | Rejected |

Cilium did not meaningfully change cold-create p50 (about 1.5s). It did improve cold wake (p50 about 680ms).
