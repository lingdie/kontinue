package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
local cur = redis.call("GET", KEYS[1])
if cur then
    local parsed = cjson.decode(cur)
    if parsed["executionId"] and tonumber(parsed["executionId"]) > tonumber(ARGV[1]) then
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
if parsed["executionId"] and tonumber(parsed["executionId"]) > tonumber(ARGV[1]) then
    return 0
end
redis.call("DEL", KEYS[1])
return 1
`)

var luaReleaseLockIfOwner = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0
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
	res, err := luaPublishRouteFenced.Run(ctx, c.rdb, []string{protocol.RouteKey(route.SandboxID)},
		route.ExecutionID, string(raw), int((24 * time.Hour).Seconds())).Int()
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

func (c *RedisRuntimeCatalog) DeleteRouteFenced(ctx context.Context, sandboxID string, maxExecutionID int64) error {
	res, err := luaDeleteRouteFenced.Run(ctx, c.rdb, []string{protocol.RouteKey(sandboxID)}, maxExecutionID).Int()
	if err != nil {
		return err
	}
	if res == 0 {
		return ErrStaleExecutionFence
	}
	return nil
}

func (c *RedisRuntimeCatalog) AcquireTransitionLock(ctx context.Context, sandboxID string, ttl time.Duration) (func(), error) {
	tokBytes := make([]byte, 16)
	_, _ = rand.Read(tokBytes)
	tok := hex.EncodeToString(tokBytes)
	key := protocol.LockKey(sandboxID)
	ok, err := c.rdb.SetNX(ctx, key, tok, ttl).Result()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrTransitionLocked
	}
	return func() {
		_, _ = luaReleaseLockIfOwner.Run(context.Background(), c.rdb, []string{key}, tok).Result()
	}, nil
}

func (c *RedisRuntimeCatalog) TouchSandboxActive(ctx context.Context, sandboxID string, at time.Time) error {
	return c.rdb.Set(ctx, protocol.ActiveAtKey(sandboxID), strconv.FormatInt(at.UnixNano(), 10), 24*time.Hour).Err()
}

func (c *RedisRuntimeCatalog) AdjustInflight(ctx context.Context, sandboxID string, delta int64) (int64, error) {
	key := protocol.InflightKey(sandboxID)
	n, err := c.rdb.IncrBy(ctx, key, delta).Result()
	if err != nil {
		return 0, err
	}
	if n <= 0 {
		_ = c.rdb.Del(ctx, key).Err()
		return 0, nil
	}
	_ = c.rdb.Expire(ctx, key, 2*time.Hour).Err()
	return n, nil
}

func (c *RedisRuntimeCatalog) Inflight(ctx context.Context, sandboxID string) (int64, error) {
	n, err := c.rdb.Get(ctx, protocol.InflightKey(sandboxID)).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return n, err
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
