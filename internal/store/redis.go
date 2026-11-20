package store

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/redis/go-redis/v9"
)

// Lua Script 1: Atomically pop an idle Carrier Slot from kt:pool:idle:{node}:{tier} and return its metadata JSON.
var luaClaimIdleSlot = redis.NewScript(`
local slotId = redis.call("SPOP", KEYS[1])
if not slotId then
    return nil
end
local metaKey = ARGV[1] .. slotId
local meta = redis.call("GET", metaKey)
redis.call("DEL", metaKey)
redis.call("SET", "kt:carrier:used:" .. slotId, "1", "EX", 86400)
return meta
`)

// Lua Script 2: Monotonic ExecutionID-fenced route upsert (prevents stale starts overwriting newer executions).
var luaPublishRouteFenced = redis.NewScript(`
local state=redis.call("HGET",KEYS[2],"state")
if (state or ARGV[6]~="") and state~="ready" then return 0 end
local cur = redis.call("GET", KEYS[1])
if cur then
    local parsed = cjson.decode(cur)
    local generation = tonumber(parsed.executionId)
    if generation > tonumber(ARGV[1]) or
       (generation == tonumber(ARGV[1]) and ((parsed.slotId or "") ~= ARGV[4] or (parsed.lifecycleId or "") ~= ARGV[5])) then
        return 0
    end
end
redis.call("SET", KEYS[1], ARGV[2], "EX", tonumber(ARGV[3]))
return 1
`)

// Lua Script 3: Monotonic ExecutionID-fenced route deletion (prevents delayed teardown deleting a freshly resumed route).
var luaDeleteRouteFenced = redis.NewScript(`
local cur = redis.call("GET", KEYS[1])
if not cur then
    return 1
end
local parsed = cjson.decode(cur)
if tonumber(parsed["executionId"]) ~= tonumber(ARGV[1]) or (parsed["slotId"] or "") ~= ARGV[2] then
    return 0
end
redis.call("DEL", KEYS[1])
return 1
`)

var luaAdmitRoute = redis.NewScript(`
local route = redis.call("GET", KEYS[1])
if not route then return nil end
local owner=cjson.decode(route).ownerKey
if owner and redis.call("HGET",owner,"state")~="ready" then return nil end
local now = tonumber(redis.call("TIME")[1])
redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", now)
redis.call("ZADD", KEYS[2], now+tonumber(ARGV[2]), ARGV[1])
redis.call("EXPIRE", KEYS[2], ARGV[2])
return route
`)

var luaDrainRoute = redis.NewScript(`
local raw = redis.call("GET", KEYS[1])
if raw then
    local route = cjson.decode(raw)
    if tonumber(route.executionId) ~= tonumber(ARGV[1]) or route.slotId ~= ARGV[2] then return -1 end
end
redis.call("ZREMRANGEBYSCORE", KEYS[3], "-inf", redis.call("TIME")[1])
if redis.call("ZCARD", KEYS[3]) > 0 or tonumber(redis.call("GET", KEYS[2]) or "0") > 0 then return 0 end
redis.call("DEL", KEYS[1])
return 1
`)

// RedisRuntimeCatalog implements RuntimeCatalog over Redis with atomic Lua ExecutionID fencing.
type RedisRuntimeCatalog struct {
	rdb *redis.Client
}

func NewRedisRuntimeCatalog(addr, password string, db int) *RedisRuntimeCatalog {
	return &RedisRuntimeCatalog{
		rdb: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
			DB:       db,
		}),
	}
}

func (c *RedisRuntimeCatalog) Close() error { return c.rdb.Close() }

func (c *RedisRuntimeCatalog) RegisterIdleSlot(ctx context.Context, slot protocol.CarrierSlot) error {
	if slot.Tier == "" {
		slot.Tier = protocol.DefaultCarrierTier
	}
	if used, err := c.rdb.Exists(ctx, "kt:carrier:used:"+slot.SlotID).Result(); err == nil && used > 0 {
		return nil
	}
	raw, err := json.Marshal(slot)
	if err != nil {
		return err
	}
	pipe := c.rdb.TxPipeline()
	pipe.Set(ctx, protocol.CarrierMetaKey(slot.SlotID), raw, 24*time.Hour)
	pipe.SAdd(ctx, protocol.IdlePoolKey(slot.NodeID, slot.Tier), slot.SlotID)
	_, err = pipe.Exec(ctx)
	return err
}

func (c *RedisRuntimeCatalog) RemoveIdleSlot(ctx context.Context, nodeID, tier, slotID string) error {
	if tier == "" {
		tier = protocol.DefaultCarrierTier
	}
	pipe := c.rdb.TxPipeline()
	pipe.SRem(ctx, protocol.IdlePoolKey(nodeID, tier), slotID)
	pipe.Del(ctx, protocol.CarrierMetaKey(slotID))
	_, err := pipe.Exec(ctx)
	return err
}

func (c *RedisRuntimeCatalog) ClaimIdleSlot(ctx context.Context, nodeID, tier string) (*protocol.CarrierSlot, error) {
	if tier == "" {
		tier = protocol.DefaultCarrierTier
	}
	res, err := luaClaimIdleSlot.Run(ctx, c.rdb, []string{protocol.IdlePoolKey(nodeID, tier)}, protocol.RedisPrefixCarrierMeta).Result()
	if errors.Is(err, redis.Nil) || res == nil {
		return nil, ErrNoIdleCarrierSlot
	}
	if err != nil {
		return nil, err
	}
	str, ok := res.(string)
	if !ok || str == "" {
		return nil, ErrNoIdleCarrierSlot
	}
	var slot protocol.CarrierSlot
	if err := json.Unmarshal([]byte(str), &slot); err != nil {
		return nil, err
	}
	return &slot, nil
}

func (c *RedisRuntimeCatalog) CountIdleSlots(ctx context.Context, nodeID, tier string) (int, error) {
	if tier == "" {
		tier = protocol.DefaultCarrierTier
	}
	n, err := c.rdb.SCard(ctx, protocol.IdlePoolKey(nodeID, tier)).Result()
	return int(n), err
}

func (c *RedisRuntimeCatalog) PublishRoute(ctx context.Context, route protocol.RouteRecord) error {
	raw, err := json.Marshal(route)
	if err != nil {
		return err
	}
	res, err := luaPublishRouteFenced.Run(ctx, c.rdb, []string{protocol.RouteKey(route.SandboxID), OwnerKey(route)},
		route.ExecutionID, string(raw), int((24 * time.Hour).Seconds()), route.SlotID, route.LifecycleID, route.OwnerKey).Int()
	if err != nil {
		return err
	}
	if res == 0 {
		return ErrStaleExecutionFence
	}
	return nil
}

func (c *RedisRuntimeCatalog) GetRoute(ctx context.Context, sandboxID string) (*protocol.RouteRecord, error) {
	raw, err := c.rdb.Get(ctx, protocol.RouteKey(sandboxID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var rec protocol.RouteRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (c *RedisRuntimeCatalog) DeleteRouteFenced(ctx context.Context, sandboxID string, executionID int64, slotID string) error {
	res, err := luaDeleteRouteFenced.Run(ctx, c.rdb, []string{protocol.RouteKey(sandboxID)}, executionID, slotID).Int()
	if err != nil {
		return err
	}
	if res == 0 {
		return ErrStaleExecutionFence
	}
	return nil
}

func (c *RedisRuntimeCatalog) AdmitRoute(ctx context.Context, id, requestID string) (*protocol.RouteRecord, error) {
	raw, err := luaAdmitRoute.Run(ctx, c.rdb, []string{protocol.RouteKey(id), admissionsKey(id)}, requestID, int(AdmissionTTL.Seconds())).Text()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var r protocol.RouteRecord
	if err = json.Unmarshal([]byte(raw), &r); err != nil {
		_ = c.FinishAdmission(context.WithoutCancel(ctx), id, requestID)
		return nil, err
	}
	return &r, nil
}

func admissionsKey(id string) string { return "kt:admissions:" + id }

var luaRenewAdmission = redis.NewScript(`
local now = tonumber(redis.call("TIME")[1])
local route=redis.call("GET",KEYS[2])
if not route then return 0 end
local owner=cjson.decode(route).ownerKey
if owner and redis.call("HGET",owner,"state")~="ready" then return 0 end
local expires = redis.call("ZSCORE", KEYS[1], ARGV[1])
if not expires or tonumber(expires) <= now then return 0 end
redis.call("ZADD", KEYS[1], now+tonumber(ARGV[2]), ARGV[1])
redis.call("EXPIRE", KEYS[1], ARGV[2])
return 1
`)

func (c *RedisRuntimeCatalog) RenewAdmission(ctx context.Context, id, requestID string) error {
	n, err := luaRenewAdmission.Run(ctx, c.rdb, []string{admissionsKey(id), protocol.RouteKey(id)}, requestID, int(AdmissionTTL.Seconds())).Int()
	if err == nil && n == 0 {
		return ErrNotFound
	}
	return err
}
func (c *RedisRuntimeCatalog) FinishAdmission(ctx context.Context, id, requestID string) error {
	return c.rdb.ZRem(ctx, admissionsKey(id), requestID).Err()
}

func (c *RedisRuntimeCatalog) DrainRoute(ctx context.Context, r protocol.RouteRecord) error {
	result, err := luaDrainRoute.Run(ctx, c.rdb, []string{protocol.RouteKey(r.SandboxID), protocol.InflightKey(r.SandboxID), admissionsKey(r.SandboxID)}, r.ExecutionID, r.SlotID).Int()
	if err != nil {
		return err
	}
	switch result {
	case -1:
		return ErrStaleExecutionFence
	case 0:
		return ErrTransitionLocked
	}
	return nil
}

func (c *RedisRuntimeCatalog) TouchSandboxActive(ctx context.Context, sandboxID string, at time.Time) error {
	return c.rdb.Set(ctx, protocol.ActiveAtKey(sandboxID), strconv.FormatInt(at.UnixNano(), 10), 24*time.Hour).Err()
}

var luaAdjustInflight = redis.NewScript(`
local count = redis.call("INCRBY", KEYS[1], ARGV[1])
if count <= 0 then redis.call("DEL", KEYS[1]); return 0 end
return count
`)

func (c *RedisRuntimeCatalog) AdjustInflight(ctx context.Context, id string, delta int64) (int64, error) {
	return luaAdjustInflight.Run(ctx, c.rdb, []string{protocol.InflightKey(id)}, delta).Int64()
}

var luaCountInflight = redis.NewScript(`
redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", redis.call("TIME")[1])
return tonumber(redis.call("GET", KEYS[1]) or "0") + redis.call("ZCARD", KEYS[2])
`)

func (c *RedisRuntimeCatalog) Inflight(ctx context.Context, sandboxID string) (int64, error) {
	return luaCountInflight.Run(ctx, c.rdb, []string{protocol.InflightKey(sandboxID), admissionsKey(sandboxID)}).Int64()
}

func (c *RedisRuntimeCatalog) GetLastActive(ctx context.Context, sandboxID string) (time.Time, bool, error) {
	val, err := c.rdb.Get(ctx, protocol.ActiveAtKey(sandboxID)).Result()
	if errors.Is(err, redis.Nil) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	ns, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return time.Time{}, false, err
	}
	return time.Unix(0, ns).UTC(), true, nil
}

func (c *RedisRuntimeCatalog) UpsertNodeHeartbeat(ctx context.Context, hb protocol.NodeHeartbeat, ttl time.Duration) error {
	hb.UpdatedAt = time.Now().UTC()
	raw, err := json.Marshal(hb)
	if err != nil {
		return err
	}
	pipe := c.rdb.TxPipeline()
	pipe.Set(ctx, protocol.NodeStatusKey(hb.NodeID), raw, ttl)
	pipe.SAdd(ctx, protocol.RedisPrefixNodeSet, hb.NodeID)
	_, err = pipe.Exec(ctx)
	return err
}

func (c *RedisRuntimeCatalog) ListHealthyNodes(ctx context.Context) ([]protocol.NodeHeartbeat, error) {
	ids, err := c.rdb.SMembers(ctx, protocol.RedisPrefixNodeSet).Result()
	if err != nil {
		return nil, err
	}
	var out []protocol.NodeHeartbeat
	for _, id := range ids {
		raw, gErr := c.rdb.Get(ctx, protocol.NodeStatusKey(id)).Bytes()
		if errors.Is(gErr, redis.Nil) {
			_ = c.rdb.SRem(ctx, protocol.RedisPrefixNodeSet, id).Err()
			continue
		}
		if gErr != nil {
			continue
		}
		var hb protocol.NodeHeartbeat
		if json.Unmarshal(raw, &hb) == nil {
			out = append(out, hb)
		}
	}
	return out, nil
}

type idempotencyEnvelope struct {
	Status int    `json:"status"`
	Body   []byte `json:"body"`
}

func (c *RedisRuntimeCatalog) SaveIdempotency(ctx context.Context, key string, statusCode int, body []byte, ttl time.Duration) error {
	raw, _ := json.Marshal(idempotencyEnvelope{Status: statusCode, Body: body})
	return c.rdb.Set(ctx, protocol.RedisPrefixIdempotency+key, raw, ttl).Err()
}

func (c *RedisRuntimeCatalog) GetIdempotency(ctx context.Context, key string) (int, []byte, bool, error) {
	raw, err := c.rdb.Get(ctx, protocol.RedisPrefixIdempotency+key).Bytes()
	if errors.Is(err, redis.Nil) {
		return 0, nil, false, nil
	}
	if err != nil {
		return 0, nil, false, err
	}
	var env idempotencyEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return 0, nil, false, err
	}
	return env.Status, env.Body, true, nil
}
