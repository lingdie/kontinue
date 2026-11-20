#!/usr/bin/env python3
"""
Kontinue Sandbox Full-Lifecycle Demo (Zero external dependencies - Python stdlib only)

Walks through the complete AI Agent Sandbox lifecycle against a live Kontinue cluster:
  1. Golden Memory Template Baking   (POST /v1/templates)
  2. 1-to-N Fast Clone + Idempotency (POST /v1/sandboxes)
  3. File Upload/Download & Exec     (PUT/GET /v1/files, POST /v1/process/exec)
  4. Background Web App + Port Proxy (Host: 8000-<sandboxId>.sandbox.local)
  5. 1-to-K Live Stateful Fork       (POST /v1/sandboxes/{id}/fork)
  6. 1-to-1 Suspend (Scale-to-Zero)  (POST /v1/sandboxes/{id}/suspend)
  7. Traffic-Triggered Auto-Resume   (GET via Gateway wakes the background server)
  8. Teardown & Cleanup              (DELETE /v1/sandboxes/{id})
"""

import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request

API_BASE = os.environ.get("KONTINUE_API_URL", "http://127.0.0.1:18081")
GATEWAY_BASE = os.environ.get("KONTINUE_GATEWAY_URL", "http://127.0.0.1:18080")


def http_call(method: str, url: str, body=None, headers=None):
    hdrs = dict(headers or {})
    if url.startswith(API_BASE.rstrip("/") + "/") and os.environ.get("KONTINUE_TENANT_TOKEN"):
        hdrs["Authorization"] = "Bearer " + os.environ["KONTINUE_TENANT_TOKEN"]
    data = None
    if body is not None:
        if isinstance(body, (dict, list)):
            data = json.dumps(body).encode("utf-8")
            hdrs.setdefault("Content-Type", "application/json")
        elif isinstance(body, bytes):
            data = body
        else:
            data = str(body).encode("utf-8")

    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    t0 = time.perf_counter()
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        trace_id = resp.headers.get("X-Trace-ID", "")
        ctype = resp.headers.get("Content-Type", "")
        parsed = json.loads(raw.decode("utf-8")) if "application/json" in ctype and raw else raw
        return resp.status, parsed, trace_id, elapsed_ms


def step(num: int, title: str):
    print(f"\n{'=' * 78}")
    print(f"[Step {num}] {title}")
    print(f"{'=' * 78}")


def main():
    # -------------------------------------------------------------------------
    # 1. Bake Golden Memory Template (Control Plane: POST /v1/templates)
    # -------------------------------------------------------------------------
    step(1, "Bake Golden Memory Template (POST /v1/templates)")
    status, tpl, trace_id, ms = http_call(
        "POST",
        f"{API_BASE}/v1/templates",
        body={
            "name": "agent-python-web-demo",
            "baseImage": os.environ.get("BASE_IMAGE", "docker.io/kontinue/python-agent:dev"),
            "warmupScript": (
                "python3 -c 'import json; "
                "open(\"/root/golden_env.json\", \"w\").write(json.dumps({\"framework\": \"kontinue\", \"prewarmed\": True}))'"
            ),
            "pinInL1Tmpfs": True,
            "traceHotPages": True,
        },
    )
    tpl_id = tpl["templateId"]
    print(f"  Status           : {status} ({ms:.1f} ms) | X-Trace-ID: {trace_id}")
    print(f"  Template ID      : {tpl_id}")
    print(f"  Golden Snapshot  : {tpl['goldenSnapshotId']} (Phase={tpl['phase']})")

    # -------------------------------------------------------------------------
    # 2. Fast 1-to-N Sandbox Clone + Idempotency Replay (POST /v1/sandboxes)
    # -------------------------------------------------------------------------
    step(2, "Fast 1-to-N Sandbox Clone + Idempotency (POST /v1/sandboxes)")
    idem_key = f"demo-idem-{int(time.time() * 1000)}"
    create_body = {
        "templateId": tpl_id,
        "envVars": {
            "OPENAI_API_KEY": "sk-demo-injected-at-init-never-baked",
            "AGENT_USER": "demo-engineer",
        },
        "idleTimeoutSeconds": 300,
    }
    status, sbx, trace_id, ms = http_call(
        "POST",
        f"{API_BASE}/v1/sandboxes",
        body=create_body,
        headers={"Idempotency-Key": idem_key},
    )
    sbx_id = sbx["sandboxId"]
    token = sbx["accessToken"]
    print(f"  Clone Latency    : {ms:.1f} ms (HTTP {status}) | X-Trace-ID: {trace_id}")
    print(f"  Sandbox ID       : {sbx_id} (Node={sbx['currentNodeId']}, Carrier={sbx['carrierPodName']})")

    # Retry with same Idempotency-Key -> guaranteed exact replay, no duplicate sandbox!
    _, replay_sbx, _, replay_ms = http_call(
        "POST",
        f"{API_BASE}/v1/sandboxes",
        body=create_body,
        headers={"Idempotency-Key": idem_key},
    )
    assert replay_sbx["sandboxId"] == sbx_id
    print(f"  Idempotent Replay: {replay_ms:.1f} ms -> returned identical sandboxId={replay_sbx['sandboxId']}")

    gw_headers = {
        "X-Kontinue-Sandbox-ID": sbx_id,
        "X-Kontinue-Token": token,
    }

    # -------------------------------------------------------------------------
    # 3. Data Plane: Upload File, Run Code, Download File (via kontinue-gateway)
    # -------------------------------------------------------------------------
    step(3, "Upload File + Execute Python + Download Result (via kontinue-gateway)")
    app_script = """import http.server, json, os

counter = {"hits": 0, "user": os.environ.get("AGENT_USER", ""), "has_secret": bool(os.environ.get("OPENAI_API_KEY"))}

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        counter["hits"] += 1
        payload = json.dumps(counter).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
    def log_message(self, fmt, *args):
        pass

http.server.HTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
"""
    import base64
    http_call(
        "PUT",
        f"{GATEWAY_BASE}/v1/files",
        body={
            "path": "/root/counter_server.py",
            "content": base64.b64encode(app_script.encode()).decode(),
            "mode": 0o644,
        },
        headers=gw_headers,
    )
    print("  Uploaded /root/counter_server.py via PUT /v1/files")

    _, exec_res, _, exec_ms = http_call(
        "POST",
        f"{GATEWAY_BASE}/v1/process/exec",
        body={
            "command": [
                "/bin/sh",
                "-c",
                "python3 -c 'import json, os; g=json.load(open(\"/root/golden_env.json\")); "
                "g[\"injected_user\"]=os.environ[\"AGENT_USER\"]; "
                "open(\"/root/report.json\",\"w\").write(json.dumps(g)); print(json.dumps(g))'",
            ]
        },
        headers=gw_headers,
    )
    print(f"  Foreground Exec ({exec_ms:.1f} ms): exit={exec_res['exitCode']} stdout={exec_res['stdout'].strip()}")

    _, downloaded, _, _ = http_call(
        "GET",
        f"{GATEWAY_BASE}/v1/files?" + urllib.parse.urlencode({"path": "/root/report.json"}),
        headers=gw_headers,
    )
    print(f"  Downloaded /root/report.json: {downloaded.decode().strip()}")

    # -------------------------------------------------------------------------
    # 4. Start In-Memory Stateful HTTP Server (:8000) & Access via Subdomain
    # -------------------------------------------------------------------------
    step(4, "Start Stateful In-Memory HTTP App (:8000) & Route via 8000-<id>.sandbox.local")
    _, bg_res, _, _ = http_call(
        "POST",
        f"{GATEWAY_BASE}/v1/process/exec",
        body={
            "command": ["python3", "/root/counter_server.py"],
            "background": True,
        },
        headers=gw_headers,
    )
    print(f"  Started background Python HTTP server (PID={bg_res['pid']}) on port 8000")

    preview_host = f"8000-{sbx_id}.sandbox.local"
    preview_headers = {"Host": preview_host, "X-Kontinue-Token": token}
    # Wait for Python's http.server module import & socket bind to become ready
    for _ in range(60):
        try:
            http_call("GET", f"{GATEWAY_BASE}/", headers=preview_headers)
            break
        except Exception:
            time.sleep(0.1)

    _, web_state, _, web_ms = http_call(
        "GET",
        f"{GATEWAY_BASE}/",
        headers=preview_headers,
    )
    print(f"  GET http://{preview_host}/ ({web_ms:.1f} ms) -> {web_state} (in-memory hits=2!)")

    # -------------------------------------------------------------------------
    # 5. Live 1-to-K Stateful Fork (Branching In-Memory Process State!)
    # -------------------------------------------------------------------------
    step(5, "Live 1-to-K Stateful Fork (POST /v1/sandboxes/{id}/fork)")
    _, forks, _, fork_ms = http_call(
        "POST",
        f"{API_BASE}/v1/sandboxes/{sbx_id}/fork",
        body={"count": 2},
    )
    fork_a, fork_b = forks[0], forks[1]
    print(f"  Spawned 2 live forks in {fork_ms:.1f} ms: {fork_a['sandboxId']}, {fork_b['sandboxId']}")

    # Hit Fork A's port 8000: its in-memory Python server was already at hits=2, so this request returns hits=3!
    _, fork_a_web, _, _ = http_call(
        "GET",
        f"{GATEWAY_BASE}/",
        headers={"Host": f"8000-{fork_a['sandboxId']}.sandbox.local", "X-Kontinue-Token": fork_a["accessToken"]},
    )
    assert fork_a_web["hits"] == web_state["hits"] + 1
    print(f"  Fork A port 8000 response -> {fork_a_web} (inherited live PID + hits=2 -> 3!)")

    # -------------------------------------------------------------------------
    # 6. 1-to-1 Suspend (Scale-to-Zero: Checkpoint Memory & Destroy Container)
    # -------------------------------------------------------------------------
    step(6, "1-to-1 Suspend / Scale-to-Zero (POST /v1/sandboxes/{id}/suspend)")
    _, susp_snap, _, susp_ms = http_call("POST", f"{API_BASE}/v1/sandboxes/{sbx_id}/suspend")
    _, sbx_info, _, _ = http_call("GET", f"{API_BASE}/v1/sandboxes/{sbx_id}")
    print(f"  Suspended in {susp_ms:.1f} ms -> Snapshot={susp_snap['snapshotId']}, State={sbx_info['state']}, PodIP='{sbx_info.get('podIp', '')}' (0 RAM used!)")

    # -------------------------------------------------------------------------
    # 7. Traffic-Triggered Auto-Resume (Wake Suspended Web App on HTTP GET!)
    # -------------------------------------------------------------------------
    step(7, "Traffic-Triggered Auto-Resume (GET http://8000-<id>.sandbox.local while SUSPENDED!)")
    _, resumed_web, wake_trace, wake_ms = http_call(
        "GET",
        f"{GATEWAY_BASE}/",
        headers=preview_headers,
    )
    _, sbx_after, _, _ = http_call("GET", f"{API_BASE}/v1/sandboxes/{sbx_id}")
    assert resumed_web["hits"] == web_state["hits"] + 1 and sbx_after["executionId"] == sbx["executionId"] + 1
    print(f"  Auto-Resumed & Served HTTP in {wake_ms:.1f} ms! | X-Trace-ID: {wake_trace}")
    print(f"  Web App Response : {resumed_web} (in-memory counter continued 2 -> 3 across hibernation!)")
    print(f"  New ExecutionID  : {sbx_after['executionId']} (New Carrier Pod={sbx_after['carrierPodName']}, New PodIP={sbx_after['podIp']})")

    # -------------------------------------------------------------------------
    # 8. Cleanup & Trace Waterfall Inspection
    # -------------------------------------------------------------------------
    step(8, "Cleanup Sandboxes (DELETE /v1/sandboxes/{id}) & Inspect Trace Waterfall")
    for target_id in [sbx_id, fork_a["sandboxId"], fork_b["sandboxId"]]:
        http_call("DELETE", f"{API_BASE}/v1/sandboxes/{target_id}")
        print(f"  Deleted sandbox {target_id}")

    ktctl_bin = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "bin", "ktctl")
    if os.path.exists(ktctl_bin) and os.environ.get("KONTINUE_TRACE_TOKEN"):
        print("\n--- Full Lifecycle Trace Waterfall (ktctl trace) ---")
        out = subprocess.check_output(
            [ktctl_bin, "trace", "--sandbox-id", sbx_id, "--endpoints", f"{GATEWAY_BASE},{API_BASE}"],
            text=True,
        )
        print(out.strip())


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Demo failed: {exc}", file=sys.stderr)
        sys.exit(1)
