#!/usr/bin/env python3
"""Check that API replacement replays placement, activation and fork on the original resources."""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import json
import secrets
import subprocess
import time
from urllib.parse import urlencode

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--cluster", required=True)
parser.add_argument("--kubeconfig", required=True)
parser.add_argument("--base-image", required=True)
parser.add_argument("--case", choices=("activation", "placement", "cold-placement", "fork", "suspend", "release", "delete", "template", "files", "tenants", "all"), default="all")
args = parser.parse_args()


def run(*argv, stdin=None):
    proc = subprocess.run(argv, input=stdin, text=True, capture_output=True, timeout=90)
    if proc.returncode:
        raise RuntimeError(proc.stderr.strip())
    return proc.stdout.strip()


def kube(*argv, stdin=None):
    return run("kubectl", "--kubeconfig", args.kubeconfig, "--context", "kind-" + args.cluster,
               "-n", "kontinue-system", *argv, stdin=stdin)


def sql(statement):
    return kube("exec", "-i", "deploy/kontinue-postgres", "--", "psql", "-XAtq", "-v",
                "ON_ERROR_STOP=1", "-U", "kontinue", "-d", "kontinue", stdin=statement)


def http(method, path, body=None, credential=None, sandbox_id=None, key=None, tenant="local", host=None):
    port = 30081
    headers = ["-H", "Content-Type: application/json"]
    if key:
        headers += ["-H", "Idempotency-Key: " + key]
    if credential is not None:
        port = 30080
        headers += ["-H", ("Host: " + host if host else "X-Kontinue-Sandbox-ID: " + (sandbox_id or sandbox)),
                    "-H", "X-Kontinue-Token: " + credential]
    elif tenant is not None:
        headers += ["-H", "Authorization: Bearer " + tenant_keys[tenant]]
    result = run("docker", "exec", "-i", control, "curl", "--connect-timeout", "3", "--max-time", "80",
                 "-sS", "-w", "\n%{http_code}", "-X", method, *headers,
                 *(["--data-binary", "@-"] if body is not None else []), f"http://127.0.0.1:{port}{path}",
                 stdin=json.dumps(body) if body is not None else None)
    raw, _, code = result.rpartition("\n")
    return int(code), raw


def until(check, timeout=90):
    end = time.monotonic() + timeout
    while True:
        try:
            return check()
        except (AssertionError, RuntimeError) as err:
            if time.monotonic() >= end:
                raise
            time.sleep(1)


def replace_service(app="kontinue-apiserver", worker=None):
    selector = ["-l", "app=" + app]
    if worker:
        selector += ["--field-selector", "spec.nodeName=" + worker]
    def ready():
        pods = json.loads(kube("get", "pods", *selector, "-o", "json"))["items"]
        for p in pods:
            if p["metadata"].get("deletionTimestamp") is None and any(s["ready"] for s in p["status"].get("containerStatuses", [])):
                return p
        raise AssertionError("waiting for ready " + app)
    pod = until(ready)
    node = pod["spec"]["nodeName"]
    assert node in nodes
    status = pod["status"]["containerStatuses"][0]
    cid = status["containerID"].removeprefix("containerd://")
    details = json.loads(run("docker", "exec", node, "crictl", "--runtime-endpoint",
                             "unix:///run/containerd/containerd.sock", "inspect", cid))
    pid = details["info"]["pid"]
    assert pid > 1 and run("docker", "exec", node, "readlink", f"/proc/{pid}/exe") == "/usr/local/bin/service"
    run("docker", "exec", node, "kill", "-KILL", str(pid))

    # A new pod avoids the kubelet backoff accumulated by repeated SIGKILLs.
    kube("delete", "pod", pod["metadata"]["name"], "--wait=false")
    def replaced():
        replacement = ready()
        assert replacement["metadata"]["uid"] != pod["metadata"]["uid"]
        current = replacement["status"]["containerStatuses"][0]
        assert current["ready"] and current["containerID"] != status["containerID"]
        if worker:
            run("docker", "exec", worker, "curl", "-fsS", "http://127.0.0.1:8082/healthz")
        else:
            assert http("GET", "/healthz")[0] == 200
    until(replaced)


def row(sandbox_id=None):
    result = sql(f"SELECT payload_json FROM kt_sandboxes WHERE sandbox_id='{sandbox_id or sandbox}';")
    assert result
    return json.loads(result)


def runtime(rec):
    node = rec["currentNodeId"]
    assert node in nodes
    raw = run("docker", "exec", node, "sh", "-c",
              """jq -sc --arg id "$1" '[.[]|select(.sandboxId==$id)|{containerId,phase,executionId}]' /var/lib/kontinue-arena/cas/scratch/ownership/*.json""",
              "sh", rec["sandboxId"])
    owners = json.loads(raw)
    assert len(owners) == 1 and owners[0]["phase"] == "active"
    cid = owners[0]["containerId"]
    state = json.loads(run("docker", "exec", node, "kontinue-runsc", "--root=/run/kontinue/runsc", "state", cid))
    assert state["status"] == "running"
    return cid, state["pid"]


@contextmanager
def reject(operation, predicate, table="kt_sandboxes"):
    trigger = "test_activation_" + secrets.token_hex(6)
    sql(f"""CREATE FUNCTION {trigger}() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'injected lifecycle commit rejection'; END $$;
        CREATE TRIGGER {trigger} BEFORE {operation} ON {table} FOR EACH ROW
        WHEN ({predicate}) EXECUTE FUNCTION {trigger}();""")
    try:
        yield
    finally:
        sql(f"DROP TRIGGER IF EXISTS {trigger} ON {table}; DROP FUNCTION IF EXISTS {trigger}();")


def redis_get(key):
    return kube("exec", "deploy/kontinue-redis", "--", "redis-cli", "--raw", "GET", key)


def create():
    return http("POST", "/v1/sandboxes", {"sandboxId": sandbox, "baseImage": args.base_image,
                                          "idleTimeoutSeconds": 86400})


def exercise(action, generation, state):
    with reject("UPDATE", f"NEW.sandbox_id='{sandbox}' AND NEW.state='RUNNING' AND OLD.state='{state}'"):
        if action == "create":
            code, _ = http("POST", "/v1/sandboxes", {"sandboxId": sandbox, "baseImage": args.base_image, "idleTimeoutSeconds": 86400})
        else:
            code, _ = http("POST", path + "/resume")
        assert code == 500, code
        intent = row()
        assert intent["state"] == state and intent["executionId"] == generation
        before = runtime(intent)
        def route_absent():
            assert not kube("exec", "deploy/kontinue-redis", "--", "redis-cli", "--raw", "GET", "kt:route:" + sandbox)
        route_absent()
        replace_service()
        route_absent()


    def committed():
        rec = row()
        assert rec["state"] == "RUNNING" and rec["lifecycleId"] == intent["lifecycleId"]
        assert runtime(rec) == before
        code, body = http("POST", "/v1/process/exec", {"command": ["echo", "committed"]}, rec["accessToken"])
        assert code == 200 and json.loads(body)["stdout"].strip() == "committed"
    until(committed)
    print(f"{action} {sandbox} generation={generation} runsc={before[0]} pid={before[1]} reused after API SIGKILL", flush=True)


nodes = set(run("kind", "get", "nodes", "--name", args.cluster).split())
control = args.cluster + "-control-plane"
assert args.cluster != "kontinue-dev" and control in nodes
secret = kube("get", "secret", "kontinue-api-credentials", "-o", "jsonpath={.data.tenants\\.json}")
tenant_keys = json.loads(base64.b64decode(secret))

def placement():
    with reject("UPDATE", f"NEW.sandbox_id='{sandbox}' AND NEW.payload_json ? 'pendingPlacement'"):
        assert create()[0] == 500
        rec = row()
        assert rec["state"] == "CREATING" and "pendingPlacement" not in rec
        key = "kt:placement:" + rec["lifecycleId"]
        slot = json.loads(redis_get(key))["slot"]
        assert not redis_get("kt:route:" + sandbox)
        replace_service()
        assert json.loads(redis_get(key))["slot"] == slot
    def committed():
        current = row()
        assert current["state"] == "RUNNING" and current["lifecycleId"] == rec["lifecycleId"]
        assert current["currentNodeId"] == slot["nodeId"] and current["currentSlotId"] == slot["slotId"]
        assert not redis_get(key)
        assert runtime(current)[1] > 1
    until(committed)
    print(f"placement {sandbox} reused {slot['nodeId']}/{slot['slotId']} after API SIGKILL", flush=True)

@contextmanager
def pause_carrier_discovery():
    pods = json.loads(kube("get", "pods", "-l", "app=kontinued", "-o", "json"))["items"]
    assert len(pods) == 2
    paused = []
    try:
        for pod in pods:
            node = pod["spec"]["nodeName"]
            container = pod["status"]["containerStatuses"][0]
            assert node in nodes and node != control and container["ready"]
            cid = container["containerID"].removeprefix("containerd://")
            details = json.loads(run("docker", "exec", node, "crictl", "--runtime-endpoint",
                                     "unix:///run/containerd/containerd.sock", "inspect", cid))
            pid = str(details["info"]["pid"])
            assert int(pid) > 1 and run("docker", "exec", node, "readlink", "/proc/" + pid + "/exe") == "/usr/local/bin/service"
            run("docker", "exec", node, "kill", "-STOP", pid)
            paused.append((node, pid))
        yield
    finally:
        for node, pid in paused:
            run("docker", "exec", node, "kill", "-CONT", pid)


def cold_placement():
    def start(tier):
        return http("POST", "/v1/sandboxes",
                    {"sandboxId": sandbox, "baseImage": args.base_image, "tier": tier, "idleTimeoutSeconds": 86400},
                    key=tier)

    def pending(tier):
        rec = row()
        assert rec["tier"] == tier and rec["state"] == "CREATING" and not rec.get("pendingPlacement")
        life = rec["lifecycleId"]
        slot = redis_get("kt:cold:expected:" + life)
        pods = json.loads(kube("-n", "kontinue-sandboxes", "get", "pods",
                               "-l", "kontinue.sh/cold-lifecycle=" + life, "-o", "json"))["items"]
        assert len(pods) == 1 and slot == "slot-" + pods[0]["metadata"]["name"]
        return life, pods[0]["metadata"]["name"]

    old_tier = "cold-test-" + secrets.token_hex(6)
    new_tier = old_tier + "-next"
    with ThreadPoolExecutor(max_workers=2) as pool:
        with pause_carrier_discovery():
            deleted = pool.submit(start, old_tier)
            old_life, old_pod = until(lambda: pending(old_tier), timeout=8)
            assert not deleted.done()

            def delete_during_wait():
                assert http("DELETE", path)[0] == 204

            until(delete_during_wait, timeout=4)
            assert deleted.result(timeout=4)[0] == 409
            assert not redis_get("kt:cold:expected:" + old_life)
            assert not redis_get("kt:placement:" + old_life)
            assert not redis_get("kt:route:" + sandbox)

            replacement = pool.submit(start, new_tier)
            life, pod = until(lambda: pending(new_tier), timeout=8)
            assert life != old_life and pod != old_pod
            replay = pool.submit(start, new_tier)
            assert (life, pod) == pending(new_tier)
        for request in (replacement, replay):
            status, body = request.result(timeout=10)
            assert status == 201, status
            assert json.loads(body)["carrierPodName"] == pod

    rec = row()
    assert rec["lifecycleId"] == life and rec["state"] == "RUNNING"
    cid, pid = runtime(rec)
    assert guest(rec, "print('cold-ready')") == "cold-ready"
    assert not redis_get("kt:placement:" + old_life)
    raw = run("docker", "exec", rec["currentNodeId"], "sh", "-c",
              """jq -cs --arg cid "$1" '[.[]|select(.containerId==$cid)|.slot.networkReadyOnly]' /var/lib/kontinue-arena/cas/scratch/ownership/*.json""",
              "sh", cid)
    assert json.loads(raw) == [True]
    assert http("POST", path + "/suspend")[0] == 200
    code, _ = http("POST", path + "/resume")
    assert code == 200
    restored = row()
    assert restored["executionId"] == 2 and guest(restored, "print('checkpoint-ready')") == "checkpoint-ready"
    # The cancelled Pod belongs only to this test's isolated tier.
    kube("-n", "kontinue-sandboxes", "delete", "pod", old_pod,
         "--ignore-not-found=true", "--wait=false")
    print(f"cold placement {sandbox}: DELETE won before readiness, stale request rejected, "
          f"retried create used one Pod {pod} and runtime {cid} PID {pid}; native checkpoint bound the placeholder on demand", flush=True)


def guest(rec, code, background=False):
    status, raw = http("POST", "/v1/process/exec", {"command": ["python3", "-c", code], "background": background},
                       rec["accessToken"], rec["sandboxId"])
    assert status == 200, status
    result = json.loads(raw)
    assert result["exitCode"] == 0
    return result.get("stdout", "").strip()


def fork():
    assert create()[0] == 201
    parent = row()
    guest(parent, """import secrets,socket
marker=secrets.token_hex(32).encode()
s=socket.socket()
s.bind(('127.0.0.1',23247))
s.listen()
while True:
 c,_=s.accept()
 c.sendall(marker)
 c.close()""", True)
    read_marker = "import socket; s=socket.create_connection(('127.0.0.1',23247),2); print(s.recv(80).decode())"
    marker = until(lambda: guest(parent, read_marker))
    assert len(marker) == 64
    assert http("POST", path + "/suspend")[0] == 200
    assert http("POST", path + "/resume")[0] == 200
    assert row()["executionId"] == 2 and guest(parent, read_marker) == marker
    key = secrets.token_hex(12)
    request = {"count": 2}
    child_prefix = f"NEW.sandbox_id LIKE '{sandbox}-fork-2-%'"
    with reject("UPDATE", child_prefix + " AND NEW.state='RUNNING'"):
        with reject("INSERT", child_prefix):
            assert http("POST", path + "/fork", request, key=key)[0] == 500
            pending = row()["pendingFork"]
            ids = pending["childIds"]
            assert len(set(ids)) == 2 and pending["snapshotId"]
            assert sql(f"SELECT count(*) FROM kt_snapshots WHERE snapshot_id='{pending['snapshotId']}';") == "1"
            assert sql(f"SELECT count(*) FROM kt_sandboxes WHERE sandbox_id LIKE '{sandbox}-fork-%';") == "0"
            replace_service()
            assert row()["pendingFork"] == pending
        def one_committed():
            assert "pendingFork" not in row()
            left, right = [row(child) for child in ids]
            assert left["state"] == "RUNNING" and right["state"] == "CREATING"
            assert not redis_get("kt:route:" + ids[1])
            return [runtime(left), runtime(right)]
        runtimes = until(one_committed)
        replace_service()
    def both_committed():
        children = [row(child) for child in ids]
        assert all(c["state"] == "RUNNING" and c["executionId"] == 3 for c in children)
        assert [runtime(c) for c in children] == runtimes
        assert all(guest(c, read_marker) == marker for c in children)
        assert len({c["accessToken"] for c in children + [parent]}) == 3
    until(both_committed)
    status, raw = http("POST", path + "/fork", request, key=key)
    assert status == 201 and [r["sandboxId"] for r in json.loads(raw)] == ids
    assert http("POST", path + "/fork", {"count": 1}, key=key)[0] == 409
    assert guest(parent, read_marker) == marker
    for child in ids:
        assert http("DELETE", "/v1/sandboxes/" + child)[0] == 204
    print(f"fork {sandbox} generation 2 -> two generation 3 branches, same RAM marker and "
          f"runsc PIDs {[pid for _, pid in runtimes]} across interrupted commits and two API replacements", flush=True)


def suspend():
    assert create()[0] == 201
    parent = row()
    node = parent["currentNodeId"]
    with ThreadPoolExecutor(max_workers=1) as pool:
        write = pool.submit(guest, parent, "import time; time.sleep(4); open('/tmp/suspend-marker','w').write('acknowledged'); print('ok')")
        def admitted():
            count = kube("exec", "deploy/kontinue-redis", "--", "redis-cli", "ZCARD", "kt:admissions:" + sandbox)
            assert int(count) >= 1
        until(admitted, timeout=5)
        assert http("POST", path + "/suspend")[0] == 409
        assert write.result() == "ok"

    def unavailable():
        status, _ = http("POST", "/v1/process/exec", {"command": ["touch", "/tmp/late-write"]}, parent["accessToken"])
        assert status >= 400, status

    with reject("INSERT", f"NEW.source_sandbox_id='{sandbox}'", table="kt_snapshots"):
        assert http("POST", path + "/suspend")[0] == 500
        pending = row()
        assert pending["state"] == "SUSPENDING"
        snap = pending["pendingSnapshotId"]
        def capture_time():
            value = run("docker", "exec", node, "sh", "-c",
                        """jq -rs --arg id "$1" '.[] | select(.sandboxId==$id) | .lastCheckpoint.result.manifest.createdAt // empty' /var/lib/kontinue-arena/cas/scratch/ownership/*.json""",
                        "sh", sandbox)
            assert value
            return value
        captured = capture_time()
        unavailable()
        replace_service()
        unavailable()
        replace_service("kontinued", node)
        unavailable()
        assert capture_time() == captured
    def committed():
        current = row()
        assert current["state"] == "SUSPENDED" and not current.get("currentSlotId")
        assert current["activeSnapshotId"] == snap
    until(committed)
    assert http("POST", path + "/resume")[0] == 200
    assert guest(parent, "import os; assert not os.path.exists('/tmp/late-write'); print(open('/tmp/suspend-marker').read())") == "acknowledged"
    print(f"suspend {sandbox} recovered {snap} captured at {captured} after API and worker daemon SIGKILL; "
          "admitted write survived and late writes were rejected", flush=True)


def delayed_release():
    assert create()[0] == 201
    old = row()
    assert http("POST", path + "/suspend")[0] == 200
    assert http("POST", path + "/resume")[0] == 200
    current = row()
    assert current["executionId"] > old["executionId"] and current["lifecycleId"] != old["lifecycleId"]
    before = runtime(current)
    def send(node, slot):
        assert node in nodes
        payload = json.dumps({"slotId": slot, "sandboxId": sandbox, "executionId": old["executionId"],
                              "lifecycleId": old["lifecycleId"]}).encode()
        # The project uses a JSON gRPC codec. Send one uncompressed unary message.
        framed = bytes(1) + len(payload).to_bytes(4, "big") + payload
        out = subprocess.run(["docker", "exec", "-i", node, "curl", "-fsS", "--max-time", "8",
                              "--http2-prior-knowledge", "-H", "content-type: application/grpc+kontinue-json",
                              "-H", "te: trailers", "--data-binary", "@-",
                              "http://127.0.0.1:9090/kontinue.node.v1.NodeControlService/ReleaseSlot"],
                             input=framed, capture_output=True, timeout=12, check=True).stdout
        assert len(out) >= 5 and out[0] == 0 and int.from_bytes(out[1:5], "big") == len(out) - 5
        return json.loads(out[5:])
    receipt = send(old["currentNodeId"], old["currentSlotId"])
    assert receipt["released"] and receipt.get("podUid")
    if current["currentSlotId"] != old["currentSlotId"]:
        assert not send(current["currentNodeId"], current["currentSlotId"])["released"]
    assert runtime(current) == before
    route = json.loads(redis_get("kt:route:" + sandbox))
    assert route["slotId"] == current["currentSlotId"] and route["lifecycleId"] == current["lifecycleId"]
    assert guest(old, "print('same owner')") == "same owner"
    print(f"delayed release {sandbox} generation 1 received original Pod UID; generation 2 retained "
          f"runsc PID {before[1]}, route and gateway access", flush=True)


def interrupted_delete():
    assert create()[0] == 201
    before = row()
    cid, _ = runtime(before)
    with reject("DELETE", f"OLD.sandbox_id='{sandbox}'"):
        assert http("DELETE", path)[0] >= 500
        assert row()["state"] == "DELETING"
        assert not redis_get("kt:route:" + sandbox)
        assert cid not in run("docker", "exec", before["currentNodeId"], "kontinue-runsc", "--root=/run/kontinue/runsc", "list", "--quiet").splitlines()
        replace_service()
        assert row()["state"] == "DELETING" and http("POST", path + "/resume")[0] >= 400
    def gone():
        assert http("GET", path)[0] == 404
    until(gone)
    print(f"delete {sandbox}: API replacement replayed release of {cid} and completed PostgreSQL deletion", flush=True)


def template():
    template_id = "golden-" + secrets.token_hex(6)
    builder = "builder-" + template_id
    candidate = "unused-" + template_id
    cas_ip = kube("get", "svc", "kontinue-minio", "-o", "jsonpath={.spec.clusterIP}")
    cas_base = f"http://{cas_ip}:9000/kontinue/"
    def put(key, data):
        run("docker", "exec", "-i", control, "curl", "-fsS", "-X", "PUT", "--data-binary", "@-", cas_base + key, stdin=data)
    def build_info():
        raw = sql(f"SELECT build FROM kt_templates WHERE template_id='{template_id}' AND phase='Capturing';")
        assert raw
        return json.loads(raw)
    with reject("INSERT", f"NEW.template_id='{template_id}'", table="kt_snapshots"):
        status, _ = http("POST", "/v1/templates", {"templateId": template_id,
                         "baseImage": args.base_image, "warmupScript": "echo recovered > /tmp/golden-marker"})
        assert status == 500, status
        b = build_info()
        snap = b["snapshotId"]
        manifest_url = cas_base + f"snapshots/{snap}/kontinue-manifest.json"
        manifest = run("docker", "exec", control, "curl", "-fsS", manifest_url)
        chunk = next(c for c in json.loads(manifest)["chunks"] if not c.get("zeroChunk") and c.get("blake3Hash"))
        replace_service()
        assert not redis_get("kt:route:" + builder)
        h = chunk["blake3Hash"]
        chunk_key = f"cas/chunks/{h[:4]}/{h}.bin"
        put(f"snapshots/{candidate}/kontinue-manifest.json", json.dumps({"snapshotId": candidate,
            "pagesImgTotalSize": chunk["length"], "chunks": [dict(chunk, offset=0, index=0)]}))
        sql(f"""INSERT INTO kt_snapshots
            (snapshot_id,kind,base_image,manifest_uri,origin_node_id,total_memory_bytes,superseded,fingerprint_json,created_at)
            VALUES ('{candidate}','SESSION_SUSPEND','fixture','fixture','fixture',{chunk['length']},true,'{{}}',now());""")
        def gc_blocked():
            assert f"snapshot {snap} awaits publication" in kube("logs", "deploy/kontinue-apiserver", "--since=3m", "--tail=500")
        until(gc_blocked)
        assert sql(f"SELECT count(*) FROM kt_snapshots WHERE snapshot_id='{candidate}';") == "1"
        assert sql(f"SELECT count(*) FROM kt_snapshots WHERE snapshot_id='{snap}';") == "0"
        run("docker", "exec", control, "curl", "-fsSI", cas_base + chunk_key)
    def finished():
        state = sql(f"SELECT phase,golden_snapshot_id,build IS NULL FROM kt_templates WHERE template_id='{template_id}';")
        assert state == "Ready|" + snap + "|t", state
        assert sql(f"SELECT count(*) FROM kt_snapshots WHERE snapshot_id='{candidate}';") == "0"
        worker = b["slot"]["nodeId"]
        owners = run("docker", "exec", worker, "sh", "-c",
                     """jq -sc --arg id "$1" '[.[]|select(.sandboxId==$id)]|length' /var/lib/kontinue-arena/cas/scratch/ownership/*.json""", "sh", builder)
        assert owners == "0"
    until(finished)
    assert run("docker", "exec", control, "curl", "-fsS", manifest_url) == manifest
    run("docker", "exec", control, "curl", "-fsSI", cas_base + chunk_key)
    status, raw = http("POST", "/v1/sandboxes", {"sandboxId": sandbox, "templateId": template_id})
    assert status == 201, status
    assert guest(json.loads(raw), "print(open('/tmp/golden-marker').read().strip())") == "recovered"
    print(f"template {template_id}: CAS-published checkpoint recovered before PostgreSQL publication as {snap}; GC retained shared "
          f"chunk {h[:12]}, builder released, clone returned warmup marker", flush=True)


def terminal_permissions(rec, path):
    # The command exits on its own. Its PTY must send a close frame and release gateway admission.
    command = f"if [ -r {path} ] || [ -w {path} ]; then exit 1; fi; echo uid-$(id -u)-gid-$(id -g)-denied"
    url = "http://127.0.0.1:30080/v1/pty?" + urlencode({"command": command})
    key = base64.b64encode(secrets.token_bytes(16)).decode()
    res = subprocess.run(["docker", "exec", control, "curl", "-siN", "--http1.1", "--max-time", "4",
                          "-H", "Connection: Upgrade", "-H", "Upgrade: websocket", "-H", "Sec-WebSocket-Version: 13",
                          "-H", "Sec-WebSocket-Key: " + key, "-H", "X-Kontinue-Sandbox-ID: " + rec["sandboxId"],
                          "-H", "X-Kontinue-Token: " + rec["accessToken"], url], capture_output=True, timeout=12)
    head, separator, frames = res.stdout.partition(b"\r\n\r\n")
    assert res.returncode == 0 and separator and head.startswith(b"HTTP/1.1 101"), "WebSocket upgrade failed"
    assert frames.endswith(b"\x88\x00"), "terminal did not send a WebSocket close frame"
    assert b"uid-1000-gid-1000-denied" in frames, "PTY did not confirm workload identity and denied access"


def file_permissions():
    status, raw = create()
    assert status == 201
    rec = json.loads(raw)
    token = rec["accessToken"]
    path = "/tmp/kt-file-test/nested/data"
    content = b"zero\0binary\x7f"
    body = {"path": path, "content": base64.b64encode(content).decode(), "mode": 0o640}
    assert http("PUT", "/v1/files", body, token)[0] == 204
    status, data = http("GET", "/v1/files?path=" + path, credential=token)
    assert status == 200 and data.encode() == content
    stat = guest(rec, f"import os; s=os.stat('{path}'); print(s.st_uid,s.st_gid,oct(s.st_mode & 0o777),os.getgroups())")
    assert stat == "1000 1000 0o640 []", stat
    guest(rec, f"import os; os.chmod('{path}',0)")
    assert http("GET", "/v1/files?path=" + path, credential=token)[0] == 403
    assert http("POST", "/v1/files", body, token)[0] == 403
    assert guest(rec, f"import os; print(os.getuid(), os.getgid(), os.access('{path}', os.R_OK), os.access('{path}', os.W_OK))") == "1000 1000 False False"
    terminal_permissions(rec, path)
    assert http("GET", "/v1/files?path=/tmp/absent-" + sandbox, credential=token)[0] == 404
    guest(rec, f"import os; os.chmod('{path}',0o600)")
    assert http("POST", "/v1/files", body, token)[0] == 204
    assert http("POST", "/v1/sandboxes/" + sandbox + "/suspend")[0] == 200, "closed PTY blocked suspend"
    print(f"files {sandbox}: UID/GID 1000:1000, no supplementary groups, binary round trip, "
          "mode 000 denied file reads/writes and access via exec and WebSocket PTY, missing path returned 404", flush=True)


def tenant_permissions():
    other = next((t for t in tenant_keys if t != "local"), None)
    assert other is not None, "two test tenants must be provisioned in this cluster"
    tpl = "tpl-" + sandbox
    status, raw = http("POST", "/v1/templates", {"templateId": tpl, "baseImage": args.base_image})
    assert status == 201 and "build" not in json.loads(raw)
    assert http("GET", "/v1/templates", tenant=None)[0] == 401
    assert http("GET", "/v1/templates/" + tpl, tenant=other)[0] == 404
    assert all(t["templateId"] != tpl for t in json.loads(http("GET", "/v1/templates", tenant=other)[1]))
    assert http("POST", "/v1/sandboxes", {"templateId": tpl}, tenant=other)[0] == 404
    key = "key-" + sandbox
    body = {"sandboxId": sandbox, "templateId": tpl, "envVars": {"DEMO_VALUE": "tenant fixture"}}
    status, raw = http("POST", "/v1/sandboxes", body, key=key)
    primary = json.loads(raw)
    assert status == 201 and "envVars" not in primary and "bootstrapToken" not in primary
    token = primary["accessToken"]
    assert json.loads(http("POST", "/v1/sandboxes", body, key=key)[1])["accessToken"] == token
    assert http("POST", "/v1/sandboxes", dict(body, idleTimeoutSeconds=42), key=key)[0] == 409
    peer_id = sandbox + "-peer"
    status, raw = http("POST", "/v1/sandboxes", {"sandboxId": peer_id,"baseImage":args.base_image}, key=key, tenant=other)
    peer = json.loads(raw)
    assert status == 201 and peer["accessToken"] != token
    private_fields = {"accessToken", "bootstrapToken", "envVars", "build", "pendingPlacement", "pendingFork"}
    def check_public(raw):
        assert not private_fields.intersection(json.loads(raw))
        assert token not in raw and "tenant fixture" not in raw
    try:
        assert http("GET", path, tenant=None)[0] == 401
        assert http("GET", path, tenant=other)[0] == 404
        assert http("DELETE", path, tenant=other)[0] == 404
        for action in ("fork", "suspend", "resume"):
            assert http("POST", path + "/" + action, tenant=other)[0] == 404
        status, raw = http("GET", path)
        assert status == 200
        check_public(raw)
        for port in (30080, 30081):
            status = run("docker", "exec", control, "curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}",
                         "-H", "Authorization: Bearer " + tenant_keys["local"], f"http://127.0.0.1:{port}/debug/traces")
            assert status == "401", "tenant credential accepted on operator trace endpoint"
        host = "23480-" + sandbox + ".sandbox.local"
        guest(primary, "import http.server; http.server.HTTPServer(('0.0.0.0',23480),http.server.SimpleHTTPRequestHandler).serve_forever()", True)
        def serving():
            assert http("GET", "/", credential=token, host=host)[0] == 200
        until(serving)
        assert http("GET", "/", credential=peer["accessToken"], host=host)[0] == 401
        assert http("GET", "/", credential="", host=host)[0] == 401
        status, raw = http("POST", path + "/suspend")
        assert status == 200
        check_public(raw)
        assert http("GET", "/", credential=peer["accessToken"], host=host)[0] == 401
        assert row()["state"] == "SUSPENDED" and not redis_get("kt:route:"+sandbox)
        until(serving)
        assert row()["executionId"] == 2
        status, raw = http("POST", path + "/fork", {"count":1})
        branch = json.loads(raw)[0]
        assert status == 201
        child = "/v1/sandboxes/" + branch["sandboxId"]
        assert http("GET", child, tenant=other)[0] == 404
        assert http("DELETE", child)[0] == 204
    finally:
        assert http("DELETE", "/v1/sandboxes/" + peer_id, tenant=other)[0] == 204
    print(f"tenants {sandbox}: lifecycle, template, fork and idempotency isolated; wrong token denied on "
          "application port and left suspended target asleep; owner resumed generation 2", flush=True)


for case in ("placement", "cold-placement", "activation", "fork", "suspend", "release", "delete", "template", "files", "tenants"):
    if args.case not in (case, "all"):
        continue
    sandbox = "intent-" + secrets.token_hex(6)
    path = "/v1/sandboxes/" + sandbox
    if case == "activation":
        exercise("create", 1, "CREATING")
        assert http("POST", path + "/suspend")[0] == 200
        exercise("resume", 2, "RESUMING")
    elif case == "placement":
        placement()
    elif case == "cold-placement":
        cold_placement()
    elif case == "fork":
        fork()
    elif case == "suspend":
        suspend()
    elif case == "release":
        delayed_release()
    elif case == "delete":
        interrupted_delete()
    elif case == "template":
        template()
    elif case == "files":
        file_permissions()
    else:
        tenant_permissions()
    assert http("DELETE", path)[0] == 204
