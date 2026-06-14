package throttle

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

// tokenBucketLua is the same Lua script as the NestJS version.
// Redis executes it atomically — no race between the 2 Go servers.
const tokenBucketLua = `
local key        = KEYS[1]
local limit      = tonumber(ARGV[1])
local refillRate = tonumber(ARGV[2])
local now        = tonumber(ARGV[3])
local cost       = tonumber(ARGV[4])

local data       = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens     = tonumber(data[1]) or limit
local lastRefill = tonumber(data[2]) or now

local elapsed = math.max(0, (now - lastRefill) / 1000)
tokens = math.min(limit, tokens + elapsed * refillRate)

local allowed   = 0
local remaining = math.floor(tokens)

if tokens >= cost then
  tokens    = tokens - cost
  remaining = math.floor(tokens)
  allowed   = 1
end

redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
redis.call('EXPIRE', key, 3600)

return { allowed, remaining }
`

// RedisTokenBucket runs the token bucket logic on Redis via Lua.
type RedisTokenBucket struct {
	rdb        *redis.Client
	globalLimit int
	refillRate  float64
	script      *redis.Script
}

func NewRedisTokenBucket(rdb *redis.Client, globalLimit int, refillRate float64) *RedisTokenBucket {
	return &RedisTokenBucket{
		rdb:         rdb,
		globalLimit: globalLimit,
		refillRate:  refillRate,
		script:      redis.NewScript(tokenBucketLua),
	}
}

// Consume tries to take `cost` tokens from entityID's bucket in Redis.
func (r *RedisTokenBucket) Consume(ctx context.Context, entityID string, cost int) (ConsumeResult, error) {
	key := fmt.Sprintf("throttle:%s", entityID)
	nowMs := time.Now().UnixMilli()

	res, err := r.script.Run(ctx, r.rdb, []string{key},
		r.globalLimit,
		r.refillRate,
		nowMs,
		cost,
	).Slice()
	if err != nil {
		return ConsumeResult{}, fmt.Errorf("redis eval: %w", err)
	}

	allowed := res[0].(int64) == 1
	remaining := int(res[1].(int64))

	tokensNeeded := float64(r.globalLimit - remaining)
	resetAt := time.Now().Add(time.Duration(tokensNeeded/r.refillRate) * time.Second)

	return ConsumeResult{Allowed: allowed, Remaining: remaining, ResetAt: resetAt}, nil
}

// Ping checks Redis connectivity — used by the health probe.
func (r *RedisTokenBucket) Ping(ctx context.Context) error {
	return r.rdb.Ping(ctx).Err()
}

// SeedFromSnapshot pushes local bucket state to Redis on recovery.
// Jitter is applied by the caller to avoid thundering herd.
func (r *RedisTokenBucket) SeedFromSnapshot(ctx context.Context, snap map[string]float64) error {
	pipe := r.rdb.Pipeline()
	now := time.Now().UnixMilli()
	for entityID, tokens := range snap {
		key := fmt.Sprintf("throttle:%s", entityID)
		pipe.HMSet(ctx, key, "tokens", math.Floor(tokens), "last_refill", now)
		pipe.Expire(ctx, key, time.Hour)
	}
	_, err := pipe.Exec(ctx)
	return err
}
