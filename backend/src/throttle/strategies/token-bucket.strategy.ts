import { Injectable, Logger } from '@nestjs/common';
import { InjectRedis } from '@nestjs-modules/ioredis';
import Redis from 'ioredis';

/**
 * @architecture Token Bucket via Redis Lua Script
 *
 * Why Lua?
 *   Redis executes Lua atomically — no race conditions between
 *   read-modify-write on the same key from 2 NestJS servers.
 *
 * Token Bucket behaviour:
 *   - Bucket starts full (GLOBAL_LIMIT tokens).
 *   - Each request consumes `cost` tokens.
 *   - Tokens refill at REFILL_RATE per second up to the cap.
 *   - Allows bursting (unlike leaky bucket) — good for API workloads.
 *
 * Key format:  throttle:{entityId}
 * TTL:         3600s — prevents orphan keys from inactive entities.
 */

const TOKEN_BUCKET_LUA = `
local key        = KEYS[1]
local limit      = tonumber(ARGV[1])
local refillRate = tonumber(ARGV[2])  -- tokens per second
local now        = tonumber(ARGV[3])  -- current time ms
local cost       = tonumber(ARGV[4])  -- tokens to consume

local data = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens     = tonumber(data[1]) or limit
local lastRefill = tonumber(data[2]) or now

-- Refill: add tokens based on elapsed seconds
local elapsed = math.max(0, (now - lastRefill) / 1000)
tokens = math.min(limit, tokens + elapsed * refillRate)

local allowed = 0
local remaining = math.floor(tokens)

if tokens >= cost then
  tokens = tokens - cost
  remaining = math.floor(tokens)
  allowed = 1
end

redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
redis.call('EXPIRE', key, 3600)

return { allowed, remaining }
`;

@Injectable()
export class TokenBucketStrategy {
  private readonly logger = new Logger(TokenBucketStrategy.name);

  private readonly GLOBAL_LIMIT: number;
  private readonly REFILL_RATE:  number;

  constructor(@InjectRedis() private readonly redis: Redis) {
    this.GLOBAL_LIMIT = parseInt(process.env.THROTTLE_GLOBAL_LIMIT ?? '100', 10);
    this.REFILL_RATE  = parseFloat(process.env.THROTTLE_REFILL_RATE  ?? '10');  // tokens/sec
  }

  async consume(
    entityId: string,
    cost = 1,
  ): Promise<{ allowed: boolean; remaining: number; resetAt: number }> {
    const key = `throttle:${entityId}`;
    const now = Date.now();

    const [allowed, remaining] = await this.redis.eval(
      TOKEN_BUCKET_LUA, 1, key,
      this.GLOBAL_LIMIT,
      this.REFILL_RATE,
      now,
      cost,
    ) as [number, number];

    // resetAt = time until bucket is full again
    const tokensNeeded = this.GLOBAL_LIMIT - remaining;
    const resetAt = now + Math.ceil(tokensNeeded / this.REFILL_RATE) * 1000;

    return { allowed: allowed === 1, remaining, resetAt };
  }

  async ping(): Promise<void> {
    await this.redis.ping();
  }

  // Called on Redis recovery — seed from local snapshot to avoid thundering herd
  async seedFromSnapshot(
    snapshot: Record<string, number>,
    limit: number,
  ): Promise<void> {
    const pipeline = this.redis.pipeline();
    const now = Date.now();
    for (const [entityId, tokens] of Object.entries(snapshot)) {
      const key = `throttle:${entityId}`;
      pipeline.hmset(key, 'tokens', tokens, 'last_refill', now);
      pipeline.expire(key, 3600);
    }
    await pipeline.exec();
  }
}
