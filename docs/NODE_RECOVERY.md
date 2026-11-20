# Node daemon restart recovery

A worker can replace `kontinued` while a standalone runsc sandbox continues to hold guest memory. The recovery unit is one allocation identified by node, Carrier slot and Pod UID, sandbox ID, execution ID, lifecycle ID and runsc container ID.

## Ownership and routing

1. Before allocating a view or creating a runtime, the node writes a private claim to `/var/lib/kontinue-arena/cas/scratch/ownership`. Each claim records the expected resources and transitions through preparation, active use and release. File sync, atomic publication and directory sync protect the journal. A node-local lock admits one daemon writer. The active record includes the guest bootstrap credential.
2. On startup the daemon withdraws routes for its node, reserves journaled slots and inspects runsc state and bundle identity. It checks the Carrier through containerd CRI, including its Pod UID, address and network namespace, then probes guest identity and verifies the original restore view. Materialized views retain their directory and file identities across daemon replacement. The node restores CRI shim mappings and checkpoint ownership for verified guests.
3. Redis compares node, slot, sandbox, execution and lifecycle when revoking a route. A node report authorizes route repair for a running sandbox. The API serializes that repair with suspend and delete. A terminal loss writes its reason to the journal and the API persists `FAILED` and `failureReason` in PostgreSQL. A suspend with an already committed snapshot can finish through its normal release path.
4. Release checks the original allocation and records a receipt that can survive a lost RPC response. Pod deletion uses a UID precondition. A failed inspection or cleanup retains the resource claim for retry or operator inspection.

## Running the fault tests

Use a separate two-worker Kind cluster with Kontinue, PostgreSQL, Redis, a shared object store, the gVisor runtime, the CRI shim and functional `/dev/fuse` on both workers. Load a base image with Python and ktd into both workers. The script selects the supplied kubeconfig and context and sends HTTP requests through the Kind control plane’s NodePorts. Its only node-label change temporarily removes the origin from placement for the cross-node restore.

```sh
python3 hack/test-node-recovery.py \
  --cluster kontinue-recovery \
  --kubeconfig tmp/recovery-kind/kubeconfig \
  --base-image docker.io/kontinue/python-agent:files-v2
```

The `live` case starts a Python listener that generates 8 MiB of random bytes inside the guest process. For each gateway probe the listener recomputes the SHA-256 of the live buffer and returns the digest and its PID. The test compares both values before and after each daemon replacement and also checks the adopted slot, runsc container ID and Redis owner. It suspends, resumes from a materialized view, repeats the kill, deletes the sandbox and checks disappearance of the runtime and view.

The `views` case creates a pinned template, checks that the active L1 view shares the device and inode of each of the three snapshot files on tmpfs, and checks the reported copy count. It replaces the daemon, reads the guest RAM marker again, and verifies that releasing the view preserves the pin. The `live` case also records a decoded checkpoint trace from the API, shim and node, and checks that a materialized L2 restore reports bytes written.

The `missing` case removes the identified runsc instance. The `arenafs` case suspends on the origin, restores onto a cold worker, verifies a real FUSE mount and reads the guest’s original memory marker before terminating its serving daemon. Both cases require a PostgreSQL failure row, an absent Redis route and three failed gateway requests. The harness inspects the retained ArenaFS bundle, then explicitly deletes it and checks that the mount, staging directory, view and sandbox row disappear.

The `large` case keeps 1 GiB of random data resident across suspend, takes the origin daemon offline and wakes the guest on the other worker. It times the first successful gateway echo and the subsequent full-buffer checksum from the same start. The checksum and PID must both match. Deletion must release the destination mount. Run just this case by adding `--case large`.

## Recovery boundaries

- The recovery path requires the original worker storage, the CRI Carrier and a surviving runsc process. Node reboot, host disk loss, migration and gVisor process reconstruction are outside this test.
- The lazy ArenaFS server runs inside `kontinued`. Its death invalidates the view used by the guest’s open page file. The node retains the failed allocation until explicit release. Release stops the owned runtime and checks the disconnected mount's filesystem type, source, root and device against the claim. It removes the empty mountpoint and verified staging directory. New journals record the underlying directory inode for retry after unmount. A working directory fallback is recoverable when its recorded materialized files match.
- Route interruption is observable. Managed routes use a 30-second ownership lease, refreshed every five seconds. Startup withdraws routes before probing. Existing forwarded requests can last until their gateway admission renewal. Interrupted requests can require client retry.
- Transient inspection errors keep the slot reserved and trigger another reconciliation attempt. Unjournaled allocations require operator inspection. Release receipts persist on worker storage. Large-fleet startup time and receipt retention have not been benchmarked.

## Verification record — 2026-11-20

The tested implementation was `e2f8fbc` on `codex/lifecycle-correctness`. The dedicated `kontinue-recovery` cluster had a control plane, two ARM64 workers, Kind v0.33.0, Kubernetes v1.37.0, kindnet and runsc release-20261116.0. It used its own PostgreSQL, Redis and MinIO. The Linux ktd binary from the worker image matched the local build artifact by SHA-256.

- **Live guest.** Sandbox `restart-c0ac609fbb4a` returned guest PID `5` and marker SHA-256 `4fffbef64830552eeeabfb1230d3e1e6c23764d08a956c1cbee398bce63bb40e` before and after both daemon terminations. Generation 1 retained runsc `kt-WZY7NOIT7DLNAPBAPJJCB3RBP5`. Suspend committed `snap-susp-0d435060bad4842bc2e9c911`. Generation 2 retained runsc `kt-Y5ROI27IB2C2J6FWHWRHRHSF5F` across its daemon restart. Gateway exec returned the same marker after resume. Delete removed the generation 2 runsc instance and materialized view.
- **Missing runsc.** After removal of `kt-TJH54BZCMJOAHVONM2BLYKF7JL`, sandbox `restart-6f8086abb2d1` entered `FAILED` with reason `runsc instance is unavailable`. The test checked the PostgreSQL row, the missing Redis route, three rejected gateway calls and the retained node journal. Explicit deletion succeeded.
- **Disconnected ArenaFS.** The cold worker restored sandbox `restart-b1467dee57aa` onto an actual FUSE mount and served its original RAM marker. On daemon replacement, generation 2 entered `FAILED` with reason `restore view is unavailable: ArenaFS serving process exited`. PostgreSQL retained slot `slot-kt-carrier-2ec8574a`. The Redis route was absent, the gateway returned HTTP 503 and the worker retained the ownership journal and bundle `kt-STUS4CQXA35EDJTECC52PV74FZ`.
- **Cluster regression.** `make kind-e2e` passed template baking, snapshot clone, exec, two live-fork branches, suspend, eight concurrent auto-resume callers and trace collection. An initial template bake immediately after restoring the origin’s node label hit connection refusal on port 9090 during startup. The full rerun passed after the daemon began serving. Placement can briefly see the preceding heartbeat during a restart.
- **Regression commands.** `GOFLAGS=-count=1 make test` passed the Go race suite and Rust workspace tests. `go vet ./...` and `make build-linux-go` passed. A separate disposable PostgreSQL and Redis backed `go test -count=1 -race -v ./internal/e2e ./internal/store` with `KONTINUE_TEST_POSTGRES` and `KONTINUE_TEST_REDIS` set. That run included a delayed failed-owner report after publication of a newer route and verified successful admission to the newer generation.
