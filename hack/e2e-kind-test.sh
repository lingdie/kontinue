#!/usr/bin/env bash
set -euo pipefail

export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API_URL="${API_URL:-http://127.0.0.1:18081}"
GW_URL="${GW_URL:-http://127.0.0.1:18080}"

if [[ -z "${KONTINUE_TENANT_TOKEN:-}" ]]; then
  KONTINUE_TENANT_TOKEN="$(kubectl -n kontinue-system get secret kontinue-api-credentials -o json | python3 -c 'import base64,json,sys; print(json.loads(base64.b64decode(json.load(sys.stdin)["data"]["tenants.json"]))["local"])')"
fi
if [[ -z "${KONTINUE_TRACE_TOKEN:-}" ]]; then
  KONTINUE_TRACE_TOKEN="$(kubectl -n kontinue-system get secret kontinue-api-credentials -o json | python3 -c 'import base64,json,sys; print(base64.b64decode(json.load(sys.stdin)["data"]["trace-token"]).decode())')"
fi
export KONTINUE_TRACE_TOKEN
api_curl() { curl -H "Authorization: Bearer ${KONTINUE_TENANT_TOKEN}" "$@"; }

echo "==> [Step 0] Waiting for Carrier Pod pool to warm up in kontinue-sandboxes..."
for i in $(seq 1 45); do
  READY_COUNT="$(kubectl -n kontinue-sandboxes get pods --no-headers 2>/dev/null | grep -c Running || true)"
  if [[ "${READY_COUNT}" -ge 4 ]]; then
    echo "  -> ${READY_COUNT} pre-warmed Carrier Pods are Running!"
    break
  fi
  sleep 2
done
kubectl -n kontinue-sandboxes get pods -o wide

echo ""
echo "==> [Step 1] Building Golden Memory Template (Python3 + Pre-baked state via Kubelet :10250/checkpoint)..."
export BASE_IMAGE="${BASE_IMAGE:-docker.io/kontinue/python-agent:dev}"
TPL_JSON="$(api_curl -fsS -X POST "${API_URL}/v1/templates" \
  -H "Content-Type: application/json" \
  --data-binary @- <<JSON
{
  "name": "python-agent-golden",
  "baseImage": "${BASE_IMAGE}",
  "warmupScript": "python3 -c 'import json; open(\"/root/baked_state.json\", \"w\").write(json.dumps({\"baked\": True, \"python\": True}))'",
  "pinInL1Tmpfs": true,
  "traceHotPages": true
}
JSON
)"
echo "  Template Response: ${TPL_JSON}"
TPL_ID="$(echo "${TPL_JSON}" | python3 -c 'import sys,json; print(json.load(sys.stdin)["templateId"])')"
SNAP_ID="$(echo "${TPL_JSON}" | python3 -c 'import sys,json; print(json.load(sys.stdin)["goldenSnapshotId"])')"
echo "  -> Golden Template Ready: templateId=${TPL_ID}, goldenSnapshotId=${SNAP_ID}"

echo ""
echo "==> [Step 2] Fast 1-to-N Sandbox Clone from Golden Memory Snapshot (0 K8s Pod Creation on Hot Path!)..."
T0="$(python3 -c 'import time; print(int(time.time()*1000))')"
SBX_JSON="$(api_curl -fsS -X POST "${API_URL}/v1/sandboxes" \
  -H "Content-Type: application/json" \
  -d "{
    \"templateId\": \"${TPL_ID}\",
    \"envVars\": {\"AGENT_TENANT_SECRET\": \"sk-live-kind-verified-999\"},
    \"idleTimeoutSeconds\": 120
  }")"
T1="$(python3 -c 'import time; print(int(time.time()*1000))')"
echo "  -> Cloned Sandbox in $((T1 - T0)) ms! sandbox ready"

SBX_ID="$(echo "${SBX_JSON}" | python3 -c 'import sys,json; print(json.load(sys.stdin)["sandboxId"])')"
TOKEN="$(echo "${SBX_JSON}" | python3 -c 'import sys,json; print(json.load(sys.stdin)["accessToken"])')"

echo ""
echo "==> [Step 3] Executing Python inside gVisor Sandbox via kontinue-gateway..."
EXEC1="$(curl -fsS -D /tmp/kt_hdrs -X POST "${GW_URL}/v1/process/exec" \
  -H "Content-Type: application/json" \
  -H "X-Kontinue-Sandbox-ID: ${SBX_ID}" \
  -H "X-Kontinue-Token: ${TOKEN}" \
  --data-binary @- <<'JSON'
{
  "command": ["/bin/sh", "-c", "python3 -c 'import json, os; d=json.load(open(\"/root/baked_state.json\")); d[\"secret\"]=os.environ[\"AGENT_TENANT_SECRET\"]; print(json.dumps(d)); open(\"/root/turn1.txt\",\"w\").write(\"turn-1-survived-suspend\")'"]
}
JSON
)"
TRACE_ID="$(grep -i '^X-Trace-ID:' /tmp/kt_hdrs | awk '{print $2}' | tr -d '\r')"
echo "  Exec completed (X-Trace-ID=${TRACE_ID})"

echo ""
echo "==> [Step 4] Live 1-to-K Stateful Fork (spawning 2 parallel branch sandboxes)..."
FORK_JSON="$(api_curl -fsS -X POST "${API_URL}/v1/sandboxes/${SBX_ID}/fork" \
  -H "Content-Type: application/json" \
  -d '{"count": 2}')"
FORK1_ID="$(echo "${FORK_JSON}" | python3 -c 'import sys,json; print(json.load(sys.stdin)[0]["sandboxId"])')"
FORK1_TOK="$(echo "${FORK_JSON}" | python3 -c 'import sys,json; print(json.load(sys.stdin)[0]["accessToken"])')"
FORK_EXEC="$(curl -fsS -X POST "${GW_URL}/v1/process/exec" \
  -H "Content-Type: application/json" \
  -H "X-Kontinue-Sandbox-ID: ${FORK1_ID}" \
  -H "X-Kontinue-Token: ${FORK1_TOK}" \
  -d '{"command": ["/bin/sh", "-c", "cat /root/turn1.txt"]}')"
echo "  -> Forked Sandbox (${FORK1_ID}) inherited pre-fork state: ${FORK_EXEC}"

echo ""
echo "==> [Step 5] 1-to-1 Suspend (Scale-to-Zero via Kubelet /checkpoint + 2MiB Blake3 Incremental Diff)..."
SUSP_JSON="$(api_curl -fsS -X POST "${API_URL}/v1/sandboxes/${SBX_ID}/suspend")"
echo "  Suspend Snapshot: ${SUSP_JSON}"

echo ""
echo "==> [Step 6] Firing 8 Concurrent Requests at kontinue-gateway to Trigger Singleflight Auto-Resume..."
python3 - "${GW_URL}" "${SBX_ID}" "${TOKEN}" <<'PY'
import concurrent.futures, json, sys, time, urllib.request

gw, sbx_id, token = sys.argv[1], sys.argv[2], sys.argv[3]
t0 = time.time()

def call(idx):
    req = urllib.request.Request(
        f"{gw}/v1/process/exec",
        data=json.dumps({"command": ["/bin/sh", "-c", "cat /root/turn1.txt"]}).encode(),
        headers={
            "Content-Type": "application/json",
            "X-Kontinue-Sandbox-ID": sbx_id,
            "X-Kontinue-Token": token,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = json.loads(resp.read().decode())
        assert body["stdout"] == "turn-1-survived-suspend", f"unexpected stdout: {body}"
        return resp.headers.get("X-Trace-ID", ""), body["stdout"]

with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
    results = list(ex.map(call, range(8)))

elapsed_ms = int((time.time() - t0) * 1000)
print(f"  -> All 8 concurrent callers succeeded via singleflight auto-resume in {elapsed_ms} ms! Sample trace={results[0][0]}")
PY

echo ""
echo "==> [Step 7] Inspecting End-to-End Trace Waterfall via ktctl trace..."
"${ROOT_DIR}/bin/ktctl" trace \
  --sandbox-id "${SBX_ID}" \
  --endpoints "${GW_URL},${API_URL}"

echo ""
echo "ALL LIVE KIND + GVISOR E2E CHECKS PASSED!"
