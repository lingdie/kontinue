package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/redis/go-redis/v9"
)

// Reports expire with node liveness. Permanent loss is retained in the node
// journal and copied into PostgreSQL under the sandbox lifecycle lock.
const OwnershipTTL = 30 * time.Second

const (
	OwnerReady    = "ready"
	OwnerChecking = "checking"
	OwnerFailed   = "failed"
)

type ExecutionReport struct {
	State  string
	Reason string
}

func OwnerKey(r protocol.RouteRecord) string {
	fields := []any{r.NodeID, r.SlotID, r.SandboxID, r.ExecutionID, r.LifecycleID}
	raw, _ := json.Marshal(fields)
	return fmt.Sprintf("kt:owner:%x", sha256.Sum256(raw))
}

var luaReportOwner = redis.NewScript(`
redis.call("HSET",KEYS[2],"state",ARGV[1],"reason",ARGV[2])
redis.call("EXPIRE",KEYS[2],ARGV[3])
if ARGV[1] ~= "ready" then
    local raw=redis.call("GET",KEYS[1])
    if raw then
        local r=cjson.decode(raw)
        if r.nodeId==ARGV[4] and r.slotId==ARGV[5] and tonumber(r.executionId)==tonumber(ARGV[6]) and r.lifecycleId==ARGV[7] then
            redis.call("DEL",KEYS[1],KEYS[3])
        end
    end
end
return 1
`)

func (c *RedisRuntimeCatalog) ReportExecution(ctx context.Context, r protocol.RouteRecord, report ExecutionReport) error {
	return luaReportOwner.Run(ctx, c.rdb, []string{protocol.RouteKey(r.SandboxID), OwnerKey(r), admissionsKey(r.SandboxID)}, report.State, report.Reason, int(OwnershipTTL.Seconds()), r.NodeID, r.SlotID, r.ExecutionID, r.LifecycleID).Err()
}

func (c *RedisRuntimeCatalog) ExecutionReport(ctx context.Context, r protocol.RouteRecord) (ExecutionReport, error) {
	m, err := c.rdb.HGetAll(ctx, OwnerKey(r)).Result()
	if err != nil {
		return ExecutionReport{}, err
	}
	if m["state"] == "" {
		return ExecutionReport{}, ErrNotFound
	}
	return ExecutionReport{State: m["state"], Reason: m["reason"]}, nil
}

type timedReport struct {
	report  ExecutionReport
	expires time.Time
}

func (c *MemoryRuntimeCatalog) ReportExecution(_ context.Context, r protocol.RouteRecord, report ExecutionReport) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := OwnerKey(r)
	c.reports[key] = timedReport{report, time.Now().Add(OwnershipTTL)}
	if cur, ok := c.routes[r.SandboxID]; ok && report.State != OwnerReady && OwnerKey(cur) == key {
		delete(c.routes, r.SandboxID)
		delete(c.admissions, r.SandboxID)
	}
	return nil
}
func (c *MemoryRuntimeCatalog) reportLocked(key string) (ExecutionReport, error) {
	e, ok := c.reports[key]
	if !ok || !time.Now().Before(e.expires) {
		delete(c.reports, key)
		return ExecutionReport{}, ErrNotFound
	}
	return e.report, nil
}
func (c *MemoryRuntimeCatalog) ExecutionReport(_ context.Context, r protocol.RouteRecord) (ExecutionReport, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reportLocked(OwnerKey(r))
}
func (c *MemoryRuntimeCatalog) canPublishLocked(r protocol.RouteRecord) bool {
	report, err := c.reportLocked(OwnerKey(r))
	return report.State == OwnerReady || (r.OwnerKey == "" && errors.Is(err, ErrNotFound))
}

// Withdraw all previously advertised routes on this node, including routes
// that predate journaling or whose local claim is missing. Each withdrawal
// compares the full observed identity again inside Redis.
func (c *RedisRuntimeCatalog) BeginNodeRecovery(ctx context.Context, nodeID string) error {
	it := c.rdb.Scan(ctx, 0, protocol.RedisPrefixRoute+"*", 128).Iterator()
	for it.Next(ctx) {
		raw, err := c.rdb.Get(ctx, it.Val()).Bytes()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return err
		}
		var r protocol.RouteRecord
		if err := json.Unmarshal(raw, &r); err != nil {
			return err
		}
		if r.NodeID == nodeID {
			if err := c.ReportExecution(ctx, r, ExecutionReport{State: OwnerChecking}); err != nil {
				return err
			}
		}
	}
	return it.Err()
}

func (c *MemoryRuntimeCatalog) BeginNodeRecovery(_ context.Context, nodeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, r := range c.routes {
		if r.NodeID == nodeID {
			c.reports[OwnerKey(r)] = timedReport{ExecutionReport{State: OwnerChecking}, time.Now().Add(OwnershipTTL)}
			delete(c.routes, id)
			delete(c.admissions, id)
		}
	}
	return nil
}
