#!/usr/bin/env python3
"""Exercise daemon loss on a dedicated two-worker Kind cluster with Kontinue installed."""
import argparse
import base64
import json
import secrets
import subprocess
import time
from datetime import datetime

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--cluster", required=True, help="dedicated recovery test Kind cluster")
parser.add_argument("--kubeconfig", required=True)
parser.add_argument("--base-image", required=True, help="preloaded image with Python and ktd")
parser.add_argument("--case", choices=["all", "live", "views", "missing", "arenafs", "large"], default="all")
args = parser.parse_args()


def run(*command, data=None):
    result = subprocess.run(command, input=data, text=True, capture_output=True, timeout=120)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout.strip()


def kubectl(*command):
    return run("kubectl", "--kubeconfig", args.kubeconfig, "--context", "kind-" + args.cluster, *command)


def node_exec(node, *command, data=None):
    assert node in nodes, node
    return run("docker", "exec", "-i", node, *command, data=data)


def http(path, method="GET", body=None, sandbox=None, expect=None):
    port = 30080 if sandbox else 30081
    headers = ["-H", "Content-Type: application/json"]
    if sandbox:
        headers += ["-H", "X-Kontinue-Sandbox-ID: " + sandbox["sandboxId"],
                    "-H", "X-Kontinue-Token: " + sandbox["accessToken"]]
    else:
        headers += ["-H", "Authorization: Bearer " + tenant_token]
    raw = node_exec(control, "curl", "-sS", "--max-time", "100", "-w", "\n%{http_code}",
                    "-X", method, *headers, *(["--data-binary", "@-"] if body else []),
                    f"http://127.0.0.1:{port}{path}", data=json.dumps(body) if body else None)
    raw_body, status = raw.rsplit("\n", 1) if "\n" in raw else ("", raw)
    status = int(status)
    if expect is None and not 200 <= status < 300:
        raise RuntimeError(f"{method} {path}: {status} {raw_body}")
    if expect is not None:
        assert status in expect, (path, status)
    return status, json.loads(raw_body) if status < 300 and raw_body.startswith("{") else {}


def eventually(fn, timeout=60):
    deadline = time.monotonic() + timeout
    while True:
        try:
            return fn()
        except (AssertionError, RuntimeError) as err:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"timed out: {err}") from err
            time.sleep(1)


def create():
    sandbox_id = "restart-" + secrets.token_hex(6)
    _, sb = http("/v1/sandboxes", "POST", {"sandboxId": sandbox_id,
                 "baseImage": args.base_image, "idleTimeoutSeconds": 86400})
    return sb


def api(sb, action="", method="GET"):
    return http("/v1/sandboxes/" + sb["sandboxId"] + action, method)[1]


def execute(sb, code, background=False):
    _, result = http("/v1/process/exec", "POST", {"command": ["python3", "-c", code],
                     "background": background}, sb)
    assert result["exitCode"] == 0, result.get("stderr")
    return result.get("stdout", "").strip()


def start_marker(sb, memory_mib=8):
    # Recompute the digest for each request so the destination must fault in the entire buffer.
    execute(sb, f"""import hashlib,os,socket
pages=os.urandom({memory_mib}*1024*1024)
sock=socket.socket()
sock.bind(('127.0.0.1',23465))
sock.listen()
while True:
 conn,_=sock.accept()
 conn.sendall((str(os.getpid()) + " " + hashlib.sha256(pages).hexdigest()).encode())
 conn.close()""", True)
    return eventually(lambda: read_marker(sb))


def read_marker(sb):
    return execute(sb, "import socket; s=socket.create_connection(('127.0.0.1',23465),2); s.settimeout(75); print(s.makefile().read())")


def owner(sb):
    node = api(sb)["currentNodeId"]
    # Return only public identity fields, excluding the journal credential.
    raw = node_exec(node, "sh", "-c",
                    """jq -cs --arg id "$1" '[.[]|select(.sandboxId==$id)|{phase,containerId,slot:.slot.slotId,viewPath,arenaFS:.view.arenaFs,executionId,failureReason}]' /var/lib/kontinue-arena/cas/scratch/ownership/*.json""",
                    "sh", sb["sandboxId"])
    owners = json.loads(raw)
    assert len(owners) == 1, owners
    return node, owners[0]


def daemon(node):
    endpoint = ["crictl", "--runtime-endpoint", "unix:///run/containerd/containerd.sock"]
    cid = node_exec(node, *endpoint, "ps", "--name", "^kontinued$", "-q")
    assert cid and "\n" not in cid, cid
    pid = json.loads(node_exec(node, *endpoint, "inspect", cid))["info"]["pid"]
    assert pid > 1 and node_exec(node, "readlink", f"/proc/{pid}/exe") == "/usr/local/bin/service"
    return cid, pid


def restart(node):
    before, pid = daemon(node)
    node_exec(node, "kill", "-KILL", str(pid))

    def replaced():
        after, _ = daemon(node)
        assert before != after
        node_exec(node, "curl", "-fsS", "http://127.0.0.1:8082/healthz")
        return after

    # Repeated SIGKILL on the same Pod can reach kubelet's five-minute restart backoff.
    after = eventually(replaced, timeout=360)
    print(f"{node} kontinued {before} -> {after}", flush=True)


def redis(*command):
    return kubectl("-n", "kontinue-system", "exec", "deploy/kontinue-redis", "--", "redis-cli", "--raw", *command)


def prove_adoption(sb, marker, node, initial):
    def check():
        assert read_marker(sb) == marker, "guest RAM marker changed"
        route = json.loads(redis("GET", "kt:route:" + sb["sandboxId"]) or "{}")
        assert route.get("ownerKey") and route["executionId"] == initial["executionId"], route
        assert redis("HGET", route["ownerKey"], "state") == "ready"
        logs = kubectl("-n", "kontinue-system", "get", "pods", "-l", "app=kontinued",
                       "--field-selector", "spec.nodeName=" + node, "-o", "name")
        assert "adopted slot " + initial["slot"] in kubectl("-n", "kontinue-system", "logs", logs)
    eventually(check)
    assert owner(sb)[1]["containerId"] == initial["containerId"]
    pid, value = marker.split()
    print(f"{sb['sandboxId']} generation {initial['executionId']} runsc {initial['containerId']} "
          f"guest PID {pid} before/after 8MiB guest memory SHA256 {value}", flush=True)


def assert_failed(sb, reason):
    def check():
        state = api(sb)
        assert state["state"] == "FAILED" and reason in state["failureReason"], (state["state"], state.get("failureReason"))
        assert not redis("GET", "kt:route:" + sb["sandboxId"])
    eventually(check)
    for _ in range(3):
        status, _ = http("/v1/process/exec", "POST", {"command": ["echo", "should-not-forward"]},
                         sb, expect=range(400, 600))
    rows = kubectl("-n", "kontinue-system", "exec", "deploy/kontinue-postgres", "--",
                   "psql", "-U", "kontinue", "-d", "kontinue", "-At", "-c",
                   "SELECT sandbox_id,state,payload_json->>'failureReason' FROM kt_sandboxes WHERE state='FAILED'")
    assert any(row.startswith(sb["sandboxId"] + "|FAILED|") and reason in row for row in rows.splitlines())
    node, rec = owner(sb)
    assert rec["phase"] == "lost", rec
    print(f"{sb['sandboxId']} {node} {rec['containerId']} FAILED {reason}; PostgreSQL row verified; Redis route absent; gateway HTTP {status}", flush=True)


def checkpoint_trace(node, pod, snapshot):
    def spans(url, *curl_options):
        return json.loads(node_exec(node, "curl", "-fsS", *curl_options, url))["spans"]

    def one(rows, name):
        matches = [r for r in rows if r["name"] == name]
        assert len(matches) == 1, (name, len(matches))
        return matches[0]

    by_snapshot = spans("http://127.0.0.1:8082/debug/traces?snapshot_id=" + snapshot)
    execute = one(by_snapshot, "hydrator.execute_checkpoint")
    driver = one(by_snapshot, "hydrator.driver_checkpoint")
    ingest = one(by_snapshot, "arena.ingest_checkpoint")
    upload = one(by_snapshot, "arena.upload_snapshot")
    shim = one(spans("http://localhost/debug/traces?trace_id=" + execute["traceId"],
                     "--unix-socket", "/run/kontinue/cri-shim-admin.sock"), "crishim.checkpoint_container")
    api_rows = spans("http://127.0.0.1:30081/debug/traces?limit=4096", "-H", "Authorization: Bearer " + trace_token)
    # The Kubelet starts its own CRI trace. Match this unique Carrier and capture interval.
    kubelet = one([r for r in api_rows if r.get("attributes", {}).get("k8s.pod") == pod
                   and datetime.fromisoformat(r["startTime"]) <= datetime.fromisoformat(execute["startTime"])
                   and datetime.fromisoformat(r["endTime"]) >= datetime.fromisoformat(execute["endTime"])], "kubelet.checkpoint_api")
    assert pod in shim["attributes"]["cri.location"]
    assert node_exec(node, "test", "-f", shim["attributes"]["cri.location"]) == ""
    print("checkpoint " + json.dumps({"snapshot": snapshot, "pod": pod, "apiTrace": kubelet["traceId"],
          "nodeTrace": execute["traceId"], "ms": {r["name"]: r["durationMs"]
          for r in (kubelet, shim, execute, driver, ingest, upload)}}), flush=True)


def views():
    _, tpl = http("/v1/templates", "POST", {"name": "restart-l1", "baseImage": args.base_image, "pinInL1Tmpfs": True})
    _, sb = http("/v1/sandboxes", "POST", {"templateId": tpl["templateId"], "idleTimeoutSeconds": 86400})
    try:
        assert sb["arenaTier"] == "L1_TMPFS" and sb["restoreViewCopiedBytes"] == 0
        marker = start_marker(sb)
        node, rec = owner(sb)
        root = "/run/kontinue-arena/l1-tmpfs"
        assert node_exec(node, "findmnt", "-n", "-T", rec["viewPath"], "-o", "FSTYPE") == "tmpfs"
        pin = node_exec(node, "find", root, "-maxdepth", "1", "-type", "d", "-name", tpl["goldenSnapshotId"] + "-*")
        files = ("checkpoint.img", "pages_meta.img", "pages.img")
        identities = {}
        for filename in files:
            identity = node_exec(node, "stat", "-c", "%d:%i", pin + "/" + filename)
            assert node_exec(node, "stat", "-c", "%d:%i", rec["viewPath"] + "/" + filename) == identity
            identities[filename] = identity
        restart(node)
        prove_adoption(sb, marker, node, rec)
    finally:
        api(sb, method="DELETE")
    node_exec(node, "test", "!", "-e", rec["viewPath"])
    for filename in files:
        assert node_exec(node, "stat", "-c", "%d:%i", pin + "/" + filename) == identities[filename]
    print(f"L1 zero copied bytes, identical pin/view device and inodes {identities}; pin survived clone release", flush=True)


def live():
    sb = create()
    marker = start_marker(sb)
    node, rec = owner(sb)
    restart(node)
    prove_adoption(sb, marker, node, rec)
    snap = api(sb, "/suspend", "POST")
    assert snap["snapshotId"] and api(sb)["state"] == "SUSPENDED"
    checkpoint_trace(node, sb["carrierPodName"], snap["snapshotId"])
    resumed = api(sb, "/resume", "POST")
    assert resumed["arenaTier"] == "L2_NVME" and resumed["restoreViewCopiedBytes"] > 0, resumed.get("arenaTier")
    print(f'L2 materialized {resumed["restoreViewCopiedBytes"]} bytes', flush=True)
    node, rec = owner(sb)
    assert rec["viewPath"] and rec["arenaFS"] is False, rec
    assert read_marker(sb) == marker, "checkpoint/resume changed RAM"
    restart(node)
    prove_adoption(sb, marker, node, rec)
    api(sb, method="DELETE")
    assert rec["containerId"] not in node_exec(node, "kontinue-runsc", "--root=/run/kontinue/runsc", "list", "--quiet").split()
    node_exec(node, "test", "!", "-e", rec["viewPath"])
    print(f"snapshot {snap['snapshotId']}; adopted materialized view and runtime released", flush=True)


def missing():
    sb = create()
    assert execute(sb, "print(42)") == "42"
    node, rec = owner(sb)
    node_exec(node, "kontinue-runsc", "--root=/run/kontinue/runsc", "delete", "--force", rec["containerId"])
    restart(node)
    assert_failed(sb, "runsc instance is unavailable")
    api(sb, method="DELETE")


def cross_node(memory_mib, lose_daemon):
    sb = create()
    marker = start_marker(sb, memory_mib)
    origin, _ = owner(sb)
    api(sb, "/suspend", "POST")
    # Evict only the daemon from this dedicated test node so the next restore
    # cannot select the origin's materialized cache.
    label = "kontinue.sh/memory-ckpt-ready"
    kubectl("label", "node", origin, label + "-")
    try:
        def offline():
            assert not kubectl("-n", "kontinue-system", "get", "pod", "-l", "app=kontinued",
                               "--field-selector", "spec.nodeName=" + origin, "-o", "name")
        eventually(offline)
        time.sleep(22)  # heartbeat TTL is 20 seconds
        start = time.monotonic()
        _, echo = http("/v1/process/exec", "POST", {"command": ["/bin/echo", "ready"]}, sb)
        wake_ms = (time.monotonic() - start) * 1000
        assert echo["exitCode"] == 0 and echo["stdout"].strip() == "ready"
        assert read_marker(sb) == marker
        verified_ms = (time.monotonic() - start) * 1000
        restored = api(sb)
        assert restored["executionId"] == sb["executionId"] + 1 and restored["arenaTier"] == "L3_ARENAFS"
        node, rec = owner(sb)
        assert node != origin and rec["arenaFS"] is True, rec
        assert "fuse" in node_exec(node, "findmnt", "-n", "-T", rec["viewPath"], "-o", "FSTYPE")
        staging = "/var/lib/kontinue-arena/cas/staging/" + rec["viewPath"].rsplit("/", 1)[1]
        node_exec(node, "test", "-d", staging)
        pid, digest = marker.split()
        print(f"{sb['sandboxId']} {origin} -> {node} guest PID {pid}, FUSE gateway wake through echo {wake_ms:.2f} ms, "
              f"through {memory_mib}MiB SHA256 {digest} {verified_ms:.2f} ms; "
              f"view preparation copied {restored['restoreViewCopiedBytes']} bytes", flush=True)
        if lose_daemon:
            restart(node)
            assert_failed(sb, "ArenaFS")
            node_exec(node, "test", "-d", "/var/lib/kontinue-arena/bundles/" + rec["containerId"])
        api(sb, method="DELETE")
        http("/v1/sandboxes/" + sb["sandboxId"], expect=[404])
        assert not node_exec(node, "awk", "-v", "p=" + rec["viewPath"], "$5 == p {print $5}", "/proc/self/mountinfo")
        node_exec(node, "test", "!", "-e", rec["viewPath"])
        node_exec(node, "test", "!", "-e", staging)
        print("explicit delete removed the mount, view, staging and sandbox row", flush=True)
    finally:
        kubectl("label", "node", origin, label + "=true", "--overwrite")
        kubectl("-n", "kontinue-system", "rollout", "status", "ds/kontinued", "--timeout=90s")


nodes = set(run("kind", "get", "nodes", "--name", args.cluster).split())
control = args.cluster + "-control-plane"
assert control in nodes and args.cluster != "kontinue-dev"
for node in nodes:
    assert run("docker", "inspect", "--format", '{{index .Config.Labels "io.x-k8s.kind.cluster"}}', node) == args.cluster
secret = kubectl("-n", "kontinue-system", "get", "secret", "kontinue-api-credentials",
                 "-o", r"jsonpath={.data.tenants\.json}")
tenant_token = json.loads(base64.b64decode(secret))["local"]
trace_token = base64.b64decode(kubectl("-n", "kontinue-system", "get", "secret", "kontinue-api-credentials",
                                     "-o", "jsonpath={.data.trace-token}")).decode()
cases = {"live": live, "views": views, "missing": missing, "arenafs": lambda: cross_node(8, True),
         "large": lambda: cross_node(1024, False)}
for name, test_case in cases.items():
    if args.case in ("all", name):
        print(f"Testing {name}", flush=True)
        test_case()
