#!/usr/bin/env python3
"""
Kontinue performance harness.

Modes (BENCH_MODE):
  overflow      - create beyond warm pool, exec, suspend, wake storm (default)
  warm-only     - create N <= pool size (all WARM), measure QPS/latency
  sustained-qps - open-loop fixed concurrency creates for BENCH_DURATION_SEC

Emits stdout summary + JSON report under tmp/bench-*.json with latency, QPS,
warm/cold split, pool depth samples, optional /debug/traces rollup, carrier RSS.
"""

from __future__ import annotations

import collections
import concurrent.futures
import json
import os
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

API_BASE = os.environ.get("KONTINUE_API_URL", "http://127.0.0.1:18081")
GATEWAY_BASE = os.environ.get("KONTINUE_GATEWAY_URL", "http://127.0.0.1:18080")
BENCH_MODE = os.environ.get("BENCH_MODE", "overflow")
TOTAL_SANDBOXES = int(os.environ.get("BENCH_SANDBOXES", "40"))
WAKE_SANDBOXES = int(os.environ.get("BENCH_WAKE_SANDBOXES", "30"))
CALLERS_PER_WAKE = int(os.environ.get("BENCH_CALLERS_PER_WAKE", "4"))
SUSTAINED_CONC = int(os.environ.get("BENCH_CONCURRENCY", "12"))
SUSTAINED_SEC = float(os.environ.get("BENCH_DURATION_SEC", "15"))
REPORT_DIR = os.environ.get("BENCH_REPORT_DIR", os.path.join(os.path.dirname(__file__), "..", "tmp"))


def http_req(method: str, url: str, body=None, headers=None, timeout: int = 90):
    hdrs = dict(headers or {})
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        hdrs.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    t0 = time.perf_counter()
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        ms = (time.perf_counter() - t0) * 1000.0
        parsed = json.loads(raw.decode("utf-8")) if raw else None
        return resp.status, parsed, ms


def percentile(sorted_vals, pct: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, int(round((pct / 100.0) * (len(sorted_vals) - 1)))))
    return sorted_vals[idx]


def fmt_stats(latencies):
    if not latencies:
        return "count=0"
    s = sorted(latencies)
    return (
        f"count={len(s):2d} | min={s[0]:6.1f}ms | p50={percentile(s, 50):6.1f}ms | "
        f"p90={percentile(s, 90):6.1f}ms | p99={percentile(s, 99):6.1f}ms | max={s[-1]:6.1f}ms | "
        f"avg={statistics.mean(s):6.1f}ms"
    )


def stats_dict(latencies):
    if not latencies:
        return {"count": 0}
    s = sorted(latencies)
    return {
        "count": len(s),
        "min": round(s[0], 2),
        "p50": round(percentile(s, 50), 2),
        "p90": round(percentile(s, 90), 2),
        "p99": round(percentile(s, 99), 2),
        "max": round(s[-1], 2),
        "avg": round(statistics.mean(s), 2),
    }


def query_redis_idle_pool():
    out = {}
    for node in ["kontinue-dev-worker", "kontinue-dev-worker2"]:
        try:
            res = subprocess.check_output(
                [
                    "kubectl",
                    "-n",
                    "kontinue-system",
                    "exec",
                    "deploy/kontinue-redis",
                    "--",
                    "redis-cli",
                    "SCARD",
                    f"kt:pool:idle:{node}:default",
                ],
                text=True,
            ).strip()
            out[node] = int(res)
        except Exception:
            out[node] = -1
    return out


def sample_carrier_rss():
    """Best-effort RSS (KiB) for carrier containers via crictl on Kind workers."""
    rows = []
    for node in ["kontinue-dev-worker", "kontinue-dev-worker2"]:
        try:
            raw = subprocess.check_output(
                [
                    "docker",
                    "exec",
                    node,
                    "bash",
                    "-lc",
                    "crictl ps -a --name sandbox 2>/dev/null | awk 'NR>1{print $1}' | head -20",
                ],
                text=True,
                stderr=subprocess.DEVNULL,
            )
            ids = [x.strip() for x in raw.splitlines() if x.strip()]
            for cid in ids[:8]:
                try:
                    st = subprocess.check_output(
                        ["docker", "exec", node, "crictl", "stats", "--id", cid, "--no-stream"],
                        text=True,
                        stderr=subprocess.DEVNULL,
                    )
                    # MEMORY column varies; also try /proc
                    mem = None
                    for line in st.splitlines()[1:]:
                        parts = line.split()
                        if len(parts) >= 3:
                            mem = parts[2]
                    rows.append({"node": node, "containerId": cid[:12], "memory": mem})
                except Exception:
                    continue
        except Exception:
            continue
    # Fallback: docker stats on pause-like processes is hard; report pod count
    try:
        pods = subprocess.check_output(
            ["kubectl", "-n", "kontinue-sandboxes", "get", "pods", "-l", "kontinue.sh/role=carrier-pod", "--no-headers"],
            text=True,
        )
        rows.append({"carrierPods": len([l for l in pods.splitlines() if l.strip()])})
    except Exception:
        pass
    return rows


def fetch_trace_rollup():
    out = collections.defaultdict(list)
    try:
        with urllib.request.urlopen(f"{API_BASE}/debug/traces?limit=4000", timeout=5) as r:
            data = json.loads(r.read().decode())
        for s in data.get("spans", []):
            name = s.get("name") or ""
            if any(
                x in name
                for x in (
                    "create_sandbox",
                    "claim",
                    "hydrate",
                    "restore",
                    "wait_slot",
                    "create_carrier",
                    "allocate_cold",
                    "init",
                )
            ):
                out[name].append(float(s.get("durationMs") or 0))
    except Exception as e:
        return {"error": str(e)}
    roll = {}
    for k, vals in out.items():
        roll[k] = stats_dict(vals)
    return roll


def bake_template():
    _, tpl, tpl_ms = http_req(
        "POST",
        f"{API_BASE}/v1/templates",
        body={
            "name": f"bench-{BENCH_MODE}",
            "baseImage": "docker.io/kontinue/python-agent:dev",
            "warmupScript": "python3 -c 'open(\"/root/tpl_ready\",\"w\").write(\"ok\")'",
            "pinInL1Tmpfs": True,
            "traceHotPages": True,
        },
    )
    return tpl["templateId"], tpl_ms


def create_sandbox(tpl_id, idx):
    _, sbx, ms = http_req(
        "POST",
        f"{API_BASE}/v1/sandboxes",
        body={"templateId": tpl_id, "envVars": {"BENCH_IDX": str(idx)}, "idleTimeoutSeconds": 600},
    )
    return sbx, ms


def delete_all(sandboxes):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(8, len(sandboxes))) as pool:
        list(
            pool.map(
                lambda it: http_req("DELETE", f"{API_BASE}/v1/sandboxes/{it[1]['sandboxId']}"),
                sandboxes,
            )
        )


def run_overflow(report):
    idle_before = query_redis_idle_pool()
    total_warm = sum(v for v in idle_before.values() if v >= 0)
    print(f"[Baseline] Idle pool={idle_before} total={total_warm}")
    report["idle_before"] = idle_before
    report["carrier_rss_before"] = sample_carrier_rss()

    tpl_id, tpl_ms = bake_template()
    report["template_ms"] = tpl_ms
    print(f"[Setup] template {tpl_id} in {tpl_ms:.1f}ms")

    pool_series = [{"t": 0.0, "idle": query_redis_idle_pool()}]
    t_wave1 = time.perf_counter()

    def create_one(idx):
        sbx, ms = create_sandbox(tpl_id, idx)
        return idx, sbx, ms

    sandboxes = []
    warm_lat, cold_lat = [], []
    node_dist = collections.Counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=TOTAL_SANDBOXES) as pool:
        futs = [pool.submit(create_one, i) for i in range(TOTAL_SANDBOXES)]
        for f in concurrent.futures.as_completed(futs):
            idx, sbx, ms = f.result()
            sandboxes.append((idx, sbx))
            node_dist[sbx["currentNodeId"]] += 1
            if sbx.get("poolAllocation") == "COLD_FALLBACK":
                cold_lat.append(ms)
            else:
                warm_lat.append(ms)
    wave1_wall = (time.perf_counter() - t_wave1) * 1000.0
    sandboxes.sort(key=lambda x: x[0])
    create_qps = TOTAL_SANDBOXES / (wave1_wall / 1000.0) if wave1_wall > 0 else 0
    print(f"  Creates wall={wave1_wall:.1f}ms QPS={create_qps:.2f}")
    print(f"  Nodes {dict(node_dist)}")
    print(f"  WARM  {fmt_stats(warm_lat)}")
    print(f"  COLD  {fmt_stats(cold_lat)}")
    pool_series.append({"t": round(wave1_wall, 1), "idle": query_redis_idle_pool()})
    report["wave1"] = {
        "wall_ms": round(wave1_wall, 2),
        "qps": round(create_qps, 2),
        "nodes": dict(node_dist),
        "warm": stats_dict(warm_lat),
        "cold": stats_dict(cold_lat),
    }

    # Wave 2 exec all (collect failures instead of aborting the whole suite)
    t2 = time.perf_counter()
    exec_errors = []

    def exec_one(item):
        idx, sbx = item
        cmd = (
            "python3 -c 'import hashlib,json,os; idx=int(os.environ[\"BENCH_IDX\"]); "
            "dig=hashlib.sha256(f\"kontinue-{idx}\".encode()).hexdigest()[:12]; "
            "d={\"idx\":idx,\"digest\":dig}; open(\"/root/proof.json\",\"w\").write(json.dumps(d)); print(json.dumps(d))'"
        )
        last_err = None
        for attempt in range(4):
            try:
                _, res, ms = http_req(
                    "POST",
                    f"{GATEWAY_BASE}/v1/process/exec",
                    body={"command": ["/bin/sh", "-c", cmd]},
                    headers={"X-Kontinue-Sandbox-ID": sbx["sandboxId"], "X-Kontinue-Token": sbx["accessToken"]},
                )
                parsed = json.loads(res["stdout"].strip())
                assert parsed["idx"] == idx
                return ms
            except Exception as e:
                last_err = e
                time.sleep(0.1 * (attempt + 1))
        exec_errors.append({"sandboxId": sbx["sandboxId"], "idx": idx, "error": str(last_err)})
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=TOTAL_SANDBOXES) as pool:
        raw_exec = list(pool.map(exec_one, sandboxes))
    exec_lats = [x for x in raw_exec if x is not None]
    w2 = (time.perf_counter() - t2) * 1000
    print(f"  Exec wall={w2:.1f}ms ok={len(exec_lats)}/{len(sandboxes)} QPS={len(exec_lats)/(w2/1000):.2f} | {fmt_stats(exec_lats)}")
    if exec_errors:
        print(f"  Exec errors ({len(exec_errors)}): {exec_errors[:3]}...")
    report["wave2_exec"] = {
        "wall_ms": round(w2, 2),
        "qps": round(len(exec_lats) / (w2 / 1000), 2) if w2 else 0,
        "ok": len(exec_lats),
        "errors": len(exec_errors),
        "latency": stats_dict(exec_lats),
    }

    # create -> first exec E2E sample after bulk exec
    e2e = []
    for idx, sbx in sandboxes[: min(10, len(sandboxes))]:
        try:
            t0 = time.perf_counter()
            http_req(
                "POST",
                f"{GATEWAY_BASE}/v1/process/exec",
                body={"command": ["/bin/sh", "-c", "echo ok"]},
                headers={"X-Kontinue-Sandbox-ID": sbx["sandboxId"], "X-Kontinue-Token": sbx["accessToken"]},
            )
            e2e.append((time.perf_counter() - t0) * 1000)
        except Exception:
            continue
    print(f"  First-exec sample {fmt_stats(e2e)}")
    report["first_exec_sample"] = stats_dict(e2e)

    # Suspend
    t3 = time.perf_counter()

    def suspend_one(item):
        _, sbx = item
        _, _, ms = http_req("POST", f"{API_BASE}/v1/sandboxes/{sbx['sandboxId']}/suspend")
        return ms

    with concurrent.futures.ThreadPoolExecutor(max_workers=TOTAL_SANDBOXES) as pool:
        susp = list(pool.map(suspend_one, sandboxes))
    w3 = (time.perf_counter() - t3) * 1000
    print(f"  Suspend wall={w3:.1f}ms QPS={len(susp)/(w3/1000):.2f} | {fmt_stats(susp)}")
    report["wave3_suspend"] = {"wall_ms": round(w3, 2), "qps": round(len(susp) / (w3 / 1000), 2), "latency": stats_dict(susp)}
    pool_series.append({"t": "post-suspend", "idle": query_redis_idle_pool()})

    # Wake
    target = sandboxes[:WAKE_SANDBOXES]
    tasks = [(idx, sbx, c) for idx, sbx in target for c in range(CALLERS_PER_WAKE)]
    t4 = time.perf_counter()

    wake_errors = []

    def wake(task):
        idx, sbx, _ = task
        try:
            _, res, ms = http_req(
                "POST",
                f"{GATEWAY_BASE}/v1/process/exec",
                body={"command": ["/bin/sh", "-c", "cat /root/proof.json"]},
                headers={"X-Kontinue-Sandbox-ID": sbx["sandboxId"], "X-Kontinue-Token": sbx["accessToken"]},
            )
            proof = json.loads(res["stdout"].strip())
            assert proof["idx"] == idx
            return ms
        except Exception as e:
            wake_errors.append(str(e))
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(128, len(tasks))) as pool:
        wake_raw = list(pool.map(wake, tasks))
    wake_lats = [x for x in wake_raw if x is not None]
    w4 = (time.perf_counter() - t4) * 1000
    wake_alloc = collections.Counter()
    for _, sbx in target:
        try:
            _, info, _ = http_req("GET", f"{API_BASE}/v1/sandboxes/{sbx['sandboxId']}")
            wake_alloc[info.get("poolAllocation", "UNKNOWN")] += 1
        except Exception:
            wake_alloc["ERR"] += 1
    print(f"  Wake wall={w4:.1f}ms ok={len(wake_lats)}/{len(tasks)} QPS={len(wake_lats)/(w4/1000) if w4 else 0:.2f} split={dict(wake_alloc)}")
    print(f"  Wake {fmt_stats(wake_lats)}")
    report["wave4_wake"] = {
        "wall_ms": round(w4, 2),
        "qps": round(len(wake_lats) / (w4 / 1000), 2) if w4 else 0,
        "ok": len(wake_lats),
        "errors": len(wake_errors),
        "split": dict(wake_alloc),
        "latency": stats_dict(wake_lats),
    }

    delete_all(sandboxes)
    report["pool_series"] = pool_series
    report["trace_rollup"] = fetch_trace_rollup()
    report["carrier_rss_after"] = sample_carrier_rss()
    print("OVERFLOW BENCH PASSED")


def run_warm_only(report):
    idle = query_redis_idle_pool()
    total = sum(v for v in idle.values() if v >= 0)
    n = min(TOTAL_SANDBOXES, max(1, total))
    print(f"[warm-only] idle={idle} creating n={n}")
    report["idle_before"] = idle
    report["carrier_rss_before"] = sample_carrier_rss()
    tpl_id, _ = bake_template()
    t0 = time.perf_counter()
    warm, cold, sandboxes = [], [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=n) as pool:
        futs = [pool.submit(create_sandbox, tpl_id, i) for i in range(n)]
        for f in concurrent.futures.as_completed(futs):
            sbx, ms = f.result()
            sandboxes.append((0, sbx))
            (cold if sbx.get("poolAllocation") == "COLD_FALLBACK" else warm).append(ms)
    wall = (time.perf_counter() - t0) * 1000
    qps = n / (wall / 1000)
    print(f"  wall={wall:.1f}ms QPS={qps:.2f}")
    print(f"  WARM {fmt_stats(warm)}")
    print(f"  COLD {fmt_stats(cold)}")
    report["warm_only"] = {
        "n": n,
        "wall_ms": round(wall, 2),
        "qps": round(qps, 2),
        "warm": stats_dict(warm),
        "cold": stats_dict(cold),
    }
    report["trace_rollup"] = fetch_trace_rollup()
    delete_all(sandboxes)
    print("WARM-ONLY BENCH PASSED")


def run_sustained(report):
    idle = query_redis_idle_pool()
    print(f"[sustained-qps] conc={SUSTAINED_CONC} duration={SUSTAINED_SEC}s idle={idle}")
    tpl_id, _ = bake_template()
    import threading

    latencies = []
    alloc = collections.Counter()
    sandboxes = []
    stop_at = time.perf_counter() + SUSTAINED_SEC
    counter = {"i": 0}
    mu = threading.Lock()

    def worker():
        while time.perf_counter() < stop_at:
            with mu:
                i = counter["i"]
                counter["i"] += 1
            try:
                sbx, ms = create_sandbox(tpl_id, i)
                with mu:
                    latencies.append(ms)
                    alloc[sbx.get("poolAllocation", "?")] += 1
                    sandboxes.append((i, sbx))
            except Exception as e:
                with mu:
                    alloc[f"ERR:{type(e).__name__}"] += 1

    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=SUSTAINED_CONC) as pool:
        futs = [pool.submit(worker) for _ in range(SUSTAINED_CONC)]
        for f in futs:
            f.result()
    wall = time.perf_counter() - t0
    n = len(latencies)
    qps = n / wall if wall > 0 else 0
    print(f"  completed={n} wall={wall:.2f}s QPS={qps:.2f} alloc={dict(alloc)}")
    print(f"  latency {fmt_stats(latencies)}")
    report["sustained"] = {
        "completed": n,
        "wall_s": round(wall, 3),
        "qps": round(qps, 2),
        "alloc": dict(alloc),
        "latency": stats_dict(latencies),
    }
    delete_all(sandboxes)
    report["trace_rollup"] = fetch_trace_rollup()
    print("SUSTAINED BENCH PASSED")


def main():
    print("=" * 86)
    print(f"Kontinue Bench mode={BENCH_MODE} sandboxes={TOTAL_SANDBOXES}")
    print("=" * 86)
    report = {
        "mode": BENCH_MODE,
        "startedAt": datetime.now(timezone.utc).isoformat(),
        "api": API_BASE,
        "gateway": GATEWAY_BASE,
    }
    try:
        if BENCH_MODE == "warm-only":
            run_warm_only(report)
        elif BENCH_MODE == "sustained-qps":
            run_sustained(report)
        else:
            run_overflow(report)
    except Exception as e:
        report["error"] = str(e)
        raise
    finally:
        os.makedirs(REPORT_DIR, exist_ok=True)
        path = os.path.join(
            REPORT_DIR,
            f"bench-{BENCH_MODE}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json",
        )
        report["finishedAt"] = datetime.now(timezone.utc).isoformat()
        with open(path, "w") as f:
            json.dump(report, f, indent=2)
        print(f"Report: {path}")


if __name__ == "__main__":
    try:
        main()
    except urllib.error.URLError as e:
        print(f"API unreachable: {e}", file=sys.stderr)
        sys.exit(2)
