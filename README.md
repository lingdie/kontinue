# Kontinue

Kontinue runs untrusted agent code in a gVisor sandbox, snapshots memory, and resumes that process on another machine. Kubernetes provisions Carrier Pods and their networks. A warm allocation reuses a Carrier. An empty pool causes a synchronous Pod creation.

## Docs

| Doc | Contents |
| :--- | :--- |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Components, data flow, and milestones |
| [docs/TEMPLATE_TO_SANDBOX.md](docs/TEMPLATE_TO_SANDBOX.md) | How a golden template becomes a sandbox |
| [docs/PLAN.md](docs/PLAN.md) | What is implemented and what is still open |
| [docs/DEVELOPMENT_STANDARDS.md](docs/DEVELOPMENT_STANDARDS.md) | Rust / Go split, tracing, and test gates |

## Layout

- `crates/`: `ktd`, `kontinue-runsc`, `kontinue-arena-helper`, and `kontinue-carrier`. No CGo between Rust and Go.
- `cmd/`, `internal/`, `pkg/`: `kontinue-apiserver`, `kontinue-gateway`, `kontinued`, `kontinue-cri-shim`, and `ktctl`.
- `deploy/`: manifests for the Kind development cluster. The Postgres, Redis, and MinIO passwords are for that local environment only.
- `hack/`: cluster setup, the end-to-end test, and the overflow benchmark.

## Local Kind cluster

Requires Docker, kind, kubectl, Helm or the Cilium CLI, Go, Rust, and Python 3. Worker nodes must be able to mount `/dev/fuse`.

```bash
make kind-up
make kind-e2e
```

`make kind-up` creates the `kontinue-dev` cluster, installs Cilium, builds and imports images, and applies `deploy/`. The Kind config maps the API to `127.0.0.1:18081` and the gateway to `127.0.0.1:18080`.

The end-to-end script bakes a Python template, clones a sandbox, runs a command, then suspends and resumes it. The walkthrough exercises authenticated file access, application ports, fork and auto-resume. With the local Kind context selected, run it with the tenant credential:

```bash
export KONTINUE_TENANT_TOKEN="$(kubectl -n kontinue-system get secret kontinue-api-credentials -o json | python3 -c 'import base64,json,sys; print(json.loads(base64.b64decode(json.load(sys.stdin)["data"]["tenants.json"]))["local"])')"
python3 examples/demo_lifecycle.py
```

Set `KONTINUE_TRACE_TOKEN` to the operator credential to include the trace waterfall.

Tear the cluster down:

```bash
make kind-down
```

## Local build

```bash
make build
make test
```

Go tests run with `CGO_ENABLED=0` and `-race`. Rust tests cover the workspace. Linux-only code lives in `*_linux.go` or `#[cfg(target_os = "linux")]`, with stubs that compile on macOS.

Cross-compile the Go binaries:

```bash
make build-linux-go
```

## What works now

Template bake, memory-snapshot clone, `exec`, files, the WebSocket PTY, suspend, resume, fork, and cross-node restore through MinIO. Per-request CPU and memory are applied to a host cgroup, not to the Carrier Pod. User processes run as uid 1000 and cannot become root through a user namespace.

After a `kontinued` restart, a verified runsc instance with a valid materialized restore view can keep serving from the same guest memory. A guest that depended on the previous daemon’s ArenaFS server loses its route and exposes a failure reason. [Node recovery and verification](docs/NODE_RECOVERY.md)

Egress allowlists and cold-start tuning are not done. See [docs/PLAN.md](docs/PLAN.md).
