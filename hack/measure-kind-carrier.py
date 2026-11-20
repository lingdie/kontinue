#!/usr/bin/env python3
"""Join a zero-idle startup benchmark to Kind worker CRI, CNI and registration captures."""
import argparse
from collections import defaultdict
from datetime import datetime
import json
from pathlib import Path
import re


def nanoseconds(timestamp):
    return round(datetime.fromisoformat(timestamp).timestamp() * 1_000_000) * 1000


def containerd_records(path):
    sandboxes, containers, starts = {}, {}, {}
    decoder = json.JSONDecoder()
    for line in path.read_text().splitlines():
        match = re.match(r'time="([^"]+)".*? msg=', line)
        if not match:
            continue
        timestamp = nanoseconds(match[1])
        message, _ = decoder.raw_decode(line[match.end():])
        if match := re.fullmatch(r'RunPodSandbox for name:"(kt-carrier-[^"]+)"\s+uid:"([^"]+)"\s+namespace:"kontinue-sandboxes"(?: returns sandbox id "([^"]+)")?', message):
            name, uid, sandbox = match.groups()
            if sandbox is None:
                sandboxes[name] = {"uid": uid, "run_start": timestamp}
            else:
                row = sandboxes.get(name)
                if row is None:
                    continue
                assert row["uid"] == uid
                row.update(sandbox=sandbox, run_end=timestamp)
        elif match := re.match(r'CreateContainer within sandbox "([^"]+)" for container name:"sandbox"', message):
            containers.setdefault(match[1], {})["create_start"] = timestamp
        elif match := re.fullmatch(r'CreateContainer within sandbox "([^"]+)" for name:"sandbox" returns container id "([^"]+)"', message):
            containers.setdefault(match[1], {}).update(container=match[2], create_end=timestamp)
        elif match := re.fullmatch(r'StartContainer for "([^"]+)"', message):
            starts[match[1]] = timestamp
    return sandboxes, containers, starts


def started_containers(path):
    started = {}
    data = path.read_text()
    decoder = json.JSONDecoder()
    for match in re.finditer(r"(?m)^\{\n", data):
        try:
            event, _ = decoder.raw_decode(data, match.start())
        except ValueError:
            # The capture can stop partway through its last event.
            continue
        if event.get("containerEventType") != "CONTAINER_STARTED_EVENT":
            continue
        pod = event.get("podSandboxStatus", {})
        for container in event.get("containersStatuses", []):
            if container["id"] == event["containerId"] and container["metadata"]["name"] == "sandbox":
                started[container["id"]] = pod["metadata"]["uid"], int(container["startedAt"])
    return started


def cni_calls(path):
    by_sandbox = defaultdict(list)
    for line in path.read_text().splitlines():
        plugin, sandbox, start, end, code = line.split("\t")
        if code == "0":
            by_sandbox[sandbox].append((plugin, int(start), int(end)))
    return by_sandbox


def registration_times(path):
    records = defaultdict(list)
    for line in path.read_text().splitlines():
        match = re.search(r'(\S+Z) .*registered idle Carrier Slot .*\(pod=kontinue-sandboxes/(kt-carrier-[^ ]+) ', line)
        if match:
            uid = re.search(r" uid=([^ ]+) networkOnly=true", line)
            records[match[2]].append((nanoseconds(match[1]), uid[1] if uid else None))
    return records


def measure(sample, records, registrations):
    pod = sample["carrierPodName"]
    sandboxes, containers, starts, started, cni = records[sample["node"]]
    sandbox = sandboxes[pod]
    uid = sandbox["uid"]
    plugin_calls = cni[sandbox["sandbox"]]
    assert {"ptp", "portmap"}.issubset(plugin for plugin, _, _ in plugin_calls), "CNI ADD timing incomplete"
    cni_begin = min(begin for _, begin, _ in plugin_calls)
    cni_end = max(end for _, _, end in plugin_calls)
    # The container log is timestamped immediately after RegisterColdCarrier returns.
    ack = nanoseconds(sample["carrier_created_at"])
    observed = nanoseconds(sample["carrier_observed_at"])
    registration, registration_uid = next(record for record in registrations[pod] if record[0] >= sandbox["run_start"])
    if registration_uid is not None:
        assert registration_uid == uid, "registration Pod UID mismatch"
    # The kubelet can receive the Pod watch event before the Create reply.
    assert sandbox["run_start"] <= cni_begin <= cni_end <= sandbox["run_end"], "CNI outside sandbox creation"
    # The API can observe the Redis write before the worker has printed its log.
    phases = {
        "kubelet_before_cri": sandbox["run_start"] - ack,
        "run_pod_sandbox": sandbox["run_end"] - sandbox["run_start"],
        "cni_window": cni_end - cni_begin,
        "sandbox_before_cni": cni_begin - sandbox["run_start"],
        "sandbox_after_cni": sandbox["run_end"] - cni_end,
        "sandbox_return_to_redis_log": registration - sandbox["run_end"],
        "redis_log_to_api": observed - registration,
    }
    cri = containers.get(sandbox["sandbox"], {})
    container_id = cri.get("container")
    if container_id in started:
        event_uid, running = started[container_id]
        assert event_uid == uid, "CRI event Pod UID mismatch"
        assert sandbox["run_end"] <= cri["create_start"] <= cri["create_end"] <= starts[container_id] <= running, "out-of-order placeholder events"
        kubelet_gaps = cri["create_start"] - sandbox["run_end"] + starts[container_id] - cri["create_end"]
        phases.update(
            kubelet_outside_cri=sandbox["run_start"] - ack + kubelet_gaps,
            placeholder_after_sandbox=running - sandbox["run_end"],
            placeholder_create_cri=cri["create_end"] - cri["create_start"],
            placeholder_start_cri=running - starts[container_id],
            running_to_redis_log=registration - running,
        )
    else:
        # An early Carrier may be released before kubelet starts the placeholder.
        assert registration_uid is not None, "missing CRI placeholder start event"
    return {
        "sandboxId": sample["sandboxId"], "pod": pod, "uid": uid, "node": sample["node"],
        "networkOnlyRegistration": registration_uid is not None,
        "ms": {phase: round(duration / 1_000_000, 3) for phase, duration in phases.items()},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--capture", required=True, help="prefix of captured logs and benchmark directories")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    directory, prefix = args.directory, args.capture
    samples = []
    for report in sorted(directory.glob(f"{prefix}-empty-*/bench-startup-*.json")):
        data = json.loads(report.read_text())
        assert data["requested"] == 40 and data["concurrency"] == 24
        assert len(data["idle_before"]) == 2 and not any(data["idle_before"].values())
        samples.extend(data["samples"])
    assert samples, "no benchmark samples in the capture"
    records = {}
    for node in {sample["node"] for sample in samples}:
        base = directory / f"{prefix}-{node}"
        records[node] = (
            *containerd_records(base.with_suffix(".cri")),
            started_containers(base.with_suffix(".events")),
            cni_calls(base.with_suffix(".cni")),
        )
    registers = registration_times(directory / f"{prefix}-registration.log")
    timelines, unmatched = [], []
    for sample in samples:
        try:
            timelines.append(measure(sample, records, registers))
        except (KeyError, StopIteration, AssertionError) as exc:
            unmatched.append({"sandboxId": sample["sandboxId"], "pod": sample["carrierPodName"], "why": str(exc)})
    phases = defaultdict(list)
    for row in timelines:
        for phase, latency in row["ms"].items():
            phases[phase].append(latency)
    stats = {}
    for phase, values in phases.items():
        values.sort()
        stats[phase] = {"n": len(values), **{f"p{pct}": values[round((len(values) - 1) * pct / 100)] for pct in (50, 95)}}
    result = {"matched": len(timelines), "stats_ms": stats, "unmatched": unmatched, "timelines": timelines}
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"matched": len(timelines), "unmatched": len(unmatched), "stats_ms": stats}, indent=2))


if __name__ == "__main__":
    main()
