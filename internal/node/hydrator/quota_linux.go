package hydrator

import (
	"context"
	"fmt"
	"strings"
)

func (d *GVisorSandboxDriver) applyHostQuota(ctx context.Context, containerID string, cpuMillis, memoryBytes int64) error {
	if cpuMillis <= 0 && memoryBytes <= 0 {
		return nil
	}
	// cpu.max quota is microseconds per 100ms. 1000 CPU-millis = one core = 100000/100000.
	script := fmt.Sprintf(`set -eu
add_mem=%d
add_cpu=%d
cid=%s
if [ "$add_mem" -gt 0 ]; then
  total_kb=$(awk '/MemTotal:/ {print $2}' /proc/meminfo)
  budget=$((total_kb * 1024 * 8 / 10))
  used=0
  for f in /sys/fs/cgroup/kontinue/*/memory.max; do
    [ -f "$f" ] || continue
    v=$(cat "$f")
    case "$v" in max|"") ;; *) used=$((used + v)) ;; esac
  done
  if [ $((used + add_mem)) -gt "$budget" ]; then
    echo "node memory budget exceeded: used $used + request $add_mem > $budget" >&2
    exit 1
  fi
fi
if [ "$add_cpu" -gt 0 ]; then
  ncpu=$(nproc)
  cpu_budget=$((ncpu * 1000))
  cpu_used=0
  for f in /sys/fs/cgroup/kontinue/*/cpu.max; do
    [ -f "$f" ] || continue
    q=max
    p=1
    read -r q p < "$f" || true
    if [ "$q" != "max" ] && [ -n "${p:-}" ] && [ "$p" -gt 0 ]; then
      cpu_used=$((cpu_used + q * 1000 / p))
    fi
  done
  if [ $((cpu_used + add_cpu)) -gt "$cpu_budget" ]; then
    echo "node cpu budget exceeded: used ${cpu_used}m + request ${add_cpu}m > ${cpu_budget}m" >&2
    exit 1
  fi
fi
mkdir -p /sys/fs/cgroup/kontinue
echo '+cpu +memory' > /sys/fs/cgroup/kontinue/cgroup.subtree_control 2>/dev/null || true
cg=/sys/fs/cgroup/kontinue/$cid
mkdir -p "$cg"
if [ "$add_mem" -gt 0 ]; then echo "$add_mem" > "$cg/memory.max"; fi
if [ "$add_cpu" -gt 0 ]; then echo "$((add_cpu * 100)) 100000" > "$cg/cpu.max"; fi
moved=0
for p in /proc/[0-9]*; do
  pid=${p#/proc/}
  cmd=$(tr '\0' ' ' < "$p/cmdline" 2>/dev/null || true)
  case "$cmd" in
    *no\ runsc\ process\ matched*|*nsenter*) continue ;;
  esac
  case "$cmd" in
    *"$cid"*)
      if echo "$pid" > "$cg/cgroup.procs" 2>/dev/null; then
        moved=$((moved + 1))
      fi
      ;;
  esac
done
if [ "$moved" -lt 1 ]; then
  echo "no runsc process matched $cid" >&2
  exit 1
fi
`, memoryBytes, cpuMillis, containerID)
	out, err := d.hostCommand(ctx, "sh", "-c", script).CombinedOutput()
	if err != nil {
		return fmt.Errorf("apply sandbox cgroup quota: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (d *GVisorSandboxDriver) releaseHostQuota(ctx context.Context, containerID string) error {
	out, err := d.hostCommand(ctx, "sh", "-c", `if [ -d "$1" ]; then rmdir "$1"; fi`, "sh", "/sys/fs/cgroup/kontinue/"+containerID).CombinedOutput()
	if err != nil {
		return fmt.Errorf("remove sandbox cgroup: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	return nil
}
