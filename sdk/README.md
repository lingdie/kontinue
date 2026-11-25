# Kontinue SDK

The public SDKs expose an E2B-compatible sandbox resource model while retaining the existing low-level Kontinue clients.

## Python

```python
from kontinue import Sandbox

with Sandbox.create("template-id", timeout=300) as sandbox:
    result = sandbox.commands.run("python -c 'print(42)'")
    sandbox.files.write("/tmp/result.txt", result.stdout)
```

The Python SDK also exports `AsyncSandbox` and `E2B`. The explicit client exposes `client.Sandbox`, `client.Volume`, and `client.Template`.

## TypeScript

```ts
import { Sandbox } from "kontinue"

const sandbox = await Sandbox.create("template-id", { timeoutMs: 300_000 })
try {
  const result = await sandbox.commands.run("printf 42")
  await sandbox.files.write("/tmp/result.txt", result.stdout)
} finally {
  await sandbox.kill()
}
```

The TypeScript SDK also exports `E2B` and `Client`. `new E2B(options)` exposes `client.Sandbox`, `client.Volume`, and `client.Template`.

## Configuration

Lifecycle requests use the API endpoint and guest commands and files use the Gateway endpoint. Both SDKs accept explicit URLs and tokens. Environment defaults are `KONTINUE_API_URL`, `KONTINUE_GATEWAY_URL`, and `KONTINUE_TENANT_TOKEN`. `E2B_API_URL`, `E2B_GATEWAY_URL`, and `E2B_API_KEY` are accepted as aliases.

The resource SDK sends `X-Kontinue-Sandbox-ID` and `X-Kontinue-Token` to the Gateway for guest operations. File transfer supports text and bytes, directory operations, metadata, signed upload URLs, and signed download URLs. Command callbacks consume the existing NDJSON process log stream.

Interactive PTY sessions use `sandbox.pty.create()` with `sendInput`, `resize`, `wait`, `kill`, and `connect(pid)`. PTY sessions emit natural process exit codes and survive temporary WebSocket disconnects. Browser WebSocket clients authenticate through the `kontinue-auth` WebSocket subprotocol, while Python and Go clients send the sandbox headers directly.
