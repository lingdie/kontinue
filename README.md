# Kontinue

Kontinue runs untrusted agent code inside gVisor sandboxes and lets the control plane suspend, snapshot, move, and resume that process state across Kubernetes workers. A sandbox keeps its lifecycle state in PostgreSQL, its active route and admission lease in Redis, and its guest network identity inside a pre-warmed Carrier Pod.

The repository contains the control plane, node runtime, Rust guest components, SDKs, CLI, deployment manifests, an Ilograph architecture model, and a standalone Node.js landing page that can create a real sandbox against a running cluster and display the resulting trace.

## What Kontinue does

Kontinue separates a tenant sandbox from the Kubernetes infrastructure that hosts it.

- **The API Server** owns lifecycle state, idempotent creation, placement, checkpoint coordination, recovery, template builds, and fenced route publication.
- **The Gateway** resolves a sandbox route, admits traffic through Redis, resumes suspended sandboxes with singleflight, and proxies HTTP, WebSocket, and SDK traffic to the guest.
- **Carrier Pods** are pre-warmed Kubernetes shells. They provide the Pod UID, CNI network namespace, Pod IP, and Kubelet-visible container identity that should survive a hot swap.
- **`kontinued`** owns a node’s Carrier slots, snapshot restore view, gVisor activation, recovery journal, route readiness report, and release path.
- **Arena** restores a snapshot from three locality tiers. L1 uses a pinned tmpfs view, L2 materializes chunks from the local CAS, and L3 exposes a lazy ArenaFS view that reads from a peer or the shared object store.
- **`ktd`** is the in-guest PID 1. HeldInit validates the execution fence, injects tenant state, resets guest identity, and thaws the restored workload.
- **The Flight Recorder** keeps a bounded local span buffer. Go services and Rust components use the same recorded span shape so `ktctl trace` and the landing page can render a distributed waterfall.

The system does not require an external collector for the local trace workflow. HTTP uses W3C `traceparent`, responses expose `X-Trace-ID`, and important spans carry sandbox, snapshot, execution, slot, node, restore mode, and Arena tier attributes.

## Request lifecycle

The main path starts at `POST /v1/sandboxes` and ends when the guest route is safe to publish.

1. The caller submits a template or base image, resource limits, environment variables, volumes, and an idempotency key.
2. The API Server creates a durable `CREATING` record with `ExecutionID=1` and a lifecycle identifier.
3. Placement filters hardware compatibility, ranks snapshot locality, and claims an idle Carrier slot through Redis. An empty pool can request a cold Carrier through Kubernetes.
4. The API Server calls `HydrateSlot` on the node daemon. The daemon prepares an L1, L2, L3, or cold restore path and asks the gVisor wrapper to start the guest inside the Carrier network namespace.
5. `ktd` runs HeldInit. It validates the lifecycle fence, injects tenant environment, resets guest identity, and resumes the workload.
6. The node reports activation. The API Server commits `RUNNING` in PostgreSQL and publishes a Redis route fenced by `ExecutionID`, slot identity, and lifecycle identity.
7. The caller receives the sandbox response and its access token. Subsequent traffic goes through the Gateway directly to the guest route.

Suspension drains the route before checkpointing. The split checkpoint is stored as a manifest plus non-zero 2 MiB BLAKE3 chunks. Resume advances the execution fence before restoring and publishing a new route, so an old lifecycle cannot overwrite a newer one.

## Architecture walkthrough

[`ilograph.yaml`](ilograph.yaml) (and its Chinese counterpart [`ilograph.zh.yaml`](ilograph.zh.yaml)) is the source model for the architecture views. It includes resource ownership, external dependencies, the Create Sandbox sequence, the hot Serve Request path, Auto Resume, Suspend Sandbox, Build Template, runtime topology, and restore tiers. The sequence descriptions include the implementation file and line that owns each important transition.

The standalone landing page embeds the complete standalone HTML export produced by Ilograph. The embedded view keeps the native perspective tray, resource canvas, notes, detail control, walkthrough, and fullscreen controls inside the page. The landing page does not expose the YAML source to visitors. `landing/public/ilograph.html` and `landing/public/ilograph.zh.html` are the browser artifacts and `ilograph.yaml` / `ilograph.zh.yaml` remain the repository source models.

## Standalone landing page

The landing page lives in [`landing/`](landing/) and uses Node.js built-ins only. It serves the English page at `/` and the Chinese page at `/zh/`, and proxies `/api/*` to the configured Kontinue API Server, so browser requests stay same-origin while the page talks to a real cluster.

Start it against a local API Server:

```bash
cd landing
KONTINUE_API_URL=http://127.0.0.1:8081 npm start
```

Open `http://127.0.0.1:3000` (or `http://127.0.0.1:3000/zh/` for the Chinese version). The page runs the following workflow without a setup form.

1. The Node process connects to the cluster selected by `KONTINUE_API_URL` and injects the configured tenant and trace credentials into upstream requests.
2. The page checks the API and selects the first `Ready` template from `/v1/templates`.
3. Click **Create sandbox**. The page submits the template ID with the default lifecycle timeout, reads `X-Trace-ID`, queries the Flight Recorder, and renders spans grouped by service with duration bars.
4. After activation, the page opens an interactive `/v1/pty` shell through the Node relay. The terminal accepts keyboard input, control keys, arrow keys, and paste events.

The Node proxy uses `KONTINUE_TRACE_ENDPOINTS` as the default trace endpoint list. The common local setup is:

```bash
cd landing
KONTINUE_API_URL=http://127.0.0.1:8081 \
KONTINUE_GATEWAY_URL=http://127.0.0.1:18080 \
KONTINUE_TRACE_ENDPOINTS=http://127.0.0.1:8080,http://127.0.0.1:8081,http://127.0.0.1:8082 \
npm start
```

Use `npm run sync-architecture` after updating the repository-level `ilograph.yaml` or `ilograph.zh.yaml` so the source used for the next official Ilograph HTML export stays aligned with the checked-in model. Replace `landing/public/ilograph.html` (and regenerate `landing/public/ilograph.zh.html`) when updating the diagrams.

The landing page does not ask visitors for credentials, cluster URLs, trace endpoints, template IDs, or runtime parameters. These values stay in the Node process configuration and are forwarded by the local proxy.

## Repository layout

| Path | Responsibility |
| --- | --- |
| `cmd/kontinue-apiserver` | Lifecycle API process |
| `cmd/kontinue-gateway` | Edge HTTP and WebSocket proxy |
| `cmd/kontinued` | Node-local slot and restore daemon |
| `cmd/kontinue-cri-shim` | CRI proxy and checkpoint interception |
| `cmd/ktctl` | CLI for traces, sandboxes, templates, dashboard, and signed files |
| `internal/apiserver` | Lifecycle orchestration, placement, checkpoint, recovery, auth, and webhook handlers |
| `internal/gateway` | Route admission, proxying, and auto-resume singleflight |
| `internal/node` | Hydration, Arena restore, gVisor driver, recovery, and node ownership |
| `internal/store` | PostgreSQL and Redis contracts plus in-memory test stores |
| `pkg/protocol` | Versioned Go/Rust JSON, HTTP, gRPC, and lifecycle contracts |
| `pkg/telemetry` | OpenTelemetry setup, W3C propagation, Flight Recorder, and debug trace handler |
| `crates/ktd` | In-guest PID 1 and file, PTY, exec, and HeldInit operations |
| `crates/kontinue-runsc` | Rust gVisor OCI wrapper |
| `crates/kontinue-arena` | Rust snapshot chunk helper |
| `sdk/python` | Python SDK and PTY tests |
| `sdk/typescript` | TypeScript SDK entry point and PTY e2e test |
| `deploy` | Kind development manifests |
| `hack` | Kind setup, deployment, e2e, benchmark, and recovery scripts |
| `landing` | Standalone Node.js product and architecture page |

## Local Kind cluster

The development deployment expects Docker, kind, kubectl, Helm or the Cilium CLI, Go, Rust, Python, and worker nodes that can mount `/dev/fuse`.

```bash
make kind-up
make kind-e2e
```

`make kind-up` creates the `kontinue-dev` cluster, installs Cilium, builds and imports images, and applies `deploy/`. The Kind config maps the API Server to `127.0.0.1:18081` and the Gateway to `127.0.0.1:18080`.

The e2e script bakes a Python template, creates a sandbox, executes a command, suspends and resumes it, and exercises authenticated files, application ports, fork, and auto-resume. To run the same lifecycle demo with the local tenant credential:

```bash
export KONTINUE_TENANT_TOKEN="$(kubectl -n kontinue-system get secret kontinue-api-credentials -o json | python3 -c 'import base64,json,sys; print(json.loads(base64.b64decode(json.load(sys.stdin)["data"]["tenants.json"]))["local"])')"
python3 examples/demo_lifecycle.py
```

Set `KONTINUE_TRACE_TOKEN` to the operator credential from the cluster secret when the trace endpoint is protected. Then point the landing page at the forwarded API and trace endpoints. Tear the cluster down with:

```bash
make kind-down
```

## API examples

Create a sandbox from a ready template:

```bash
curl -X POST http://127.0.0.1:8081/v1/sandboxes \
  -H "Authorization: Bearer $KONTINUE_TENANT_TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-create-001" \
  -d '{
    "templateId": "tpl-python",
    "idleTimeoutSeconds": 900,
    "metadata": {"source": "readme"}
  }'
```

Inspect the tenant-scoped dashboard:

```bash
curl http://127.0.0.1:8081/v1/dashboard \
  -H "Authorization: Bearer $KONTINUE_TENANT_TOKEN"
```

Inspect a trace by the `X-Trace-ID` returned from the create request:

```bash
curl "http://127.0.0.1:8081/debug/traces?trace_id=$TRACE_ID" \
  -H "Authorization: Bearer $KONTINUE_TRACE_TOKEN"
```

`ktctl trace` aggregates the same endpoint shape from the command line:

```bash
go run ./cmd/ktctl trace \
  --endpoints http://127.0.0.1:8080,http://127.0.0.1:8081,http://127.0.0.1:8082 \
  --trace-id "$TRACE_ID"
```

## SDKs

The public SDK entry points follow the E2B-compatible shape used by the lifecycle API.

### Python

```python
from kontinue import Client, Sandbox

client = Client(
    base_url="http://127.0.0.1:8081",
    gateway_url="http://127.0.0.1:8080",
    api_key="$KONTINUE_TENANT_TOKEN",
)
sandbox = Sandbox.create("tpl-python", client=client)
result = sandbox.commands.run("python -c 'print(42)'")
print(result.stdout)
sandbox.kill()
```

### TypeScript

```ts
import { Sandbox } from "./sdk/typescript/index.ts";

const sandbox = await Sandbox.create({
  template: "tpl-python",
  clientOptions: {
    apiUrl: "http://127.0.0.1:8081",
    gatewayUrl: "http://127.0.0.1:8080",
    apiKey: process.env.KONTINUE_TENANT_TOKEN,
  },
});

const result = await sandbox.commands.run("python -c 'print(42)'");
console.log(result.stdout);
await sandbox.kill();
```

The repository also retains the flat Go client in `pkg/sdk` for Go callers that need direct control over the REST resources.

## Build and test

```bash
make build
make test
```

Go tests run with `CGO_ENABLED=0` and the race detector. Rust tests cover the workspace. Linux-only code stays in `*_linux.go` or `#[cfg(target_os = "linux")]` with portable stubs for macOS.

Cross-compile Go binaries for Linux:

```bash
make build-linux-go
```

The development standards document lists the milestone gate, which includes the Rust workspace tests, `go test -race ./...`, and Linux arm64 and amd64 cross-compilation.

## Current boundaries

The implemented path covers template bake, memory-snapshot clone, exec, files, WebSocket PTY, suspend, resume, fork, and cross-node restore through MinIO. Per-request CPU and memory limits apply to the host cgroup. User processes run as uid 1000 and cannot become root through a user namespace.

Egress allowlists and cold-start tuning remain open work. See [`docs/PLAN.md`](docs/PLAN.md) for the current milestone state, [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for component details, [`docs/TEMPLATE_TO_SANDBOX.md`](docs/TEMPLATE_TO_SANDBOX.md) for the template-to-sandbox path, and [`docs/DEVELOPMENT_STANDARDS.md`](docs/DEVELOPMENT_STANDARDS.md) for cross-language contracts and tracing rules.
