import { Injectable } from '@nestjs/common';
import { InjectRedis } from '@nestjs-modules/ioredis';
import Redis from 'ioredis';

/**
 * @architecture Sliding Window Counter via Redis Sorted Sets
 *
 * Alternative to Token Bucket — more precise, slightly more Redis ops (O(log N)).
 * Use when you need exact sliding window semantics (no burst allowed).
 *
 * Pattern (ClassDojo / Stripe style):
 *   - Key: `sw:{entityId}`
 *   - Members: unique request IDs (uuid or timestamp+random)
 *   - Score: request timestamp in ms
 *   - On each request:
 *     1. ZREMRANGEBYSCORE — remove entries older than window
 *     2. ZCARD — count remaining
 *     3. If count < limit → ZADD current request
 *     4. EXPIRE key
 *
 * Comparison vs Token Bucket:
 *   Token Bucket  — allows burst, O(1) ops, simpler.
 *   Sliding Window — no burst, O(log N) ops, more precise.
 */

const SLIDING_WINDOW_LUA = `
local key       = KEYS[1]
local limit     = tonumber(ARGV[1])
local windowMs  = tonumber(ARGV[2])
local now       = tonumber(ARGV[3])
local requestId = ARGV[4]

-- Remove stale entries outside the window
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - windowMs)

local count = redis.call('ZCARD', key)

if count < limit then
  redis.call('ZADD', key, now, requestId)
  redis.call('EXPIRE', key, math.ceil(windowMs / 1000) + 1)
  return { 1, limit - count - 1 }
else
  return { 0, 0 }
end
`;

@Injectable()
export class SlidingWindowStrategy {
  private readonly LIMIT     = parseInt(process.env.THROTTLE_GLOBAL_LIMIT ?? '100', 10);
  private readonly WINDOW_MS = parseInt(process.env.THROTTLE_WINDOW_MS    ?? '60000', 10); // 1 min

  constructor(@InjectRedis() private readonly redis: Redis) {}

  async consume(entityId: string): Promise<{ allowed: boolean; remaining: number; resetAt: number }> {
    const key       = `sw:${entityId}`;
    const now       = Date.now();
    const requestId = `${now}-${Math.random().toString(36).slice(2)}`;
    const resetAt   = now + this.WINDOW_MS;

    const [allowed, remaining] = await this.redis.eval(
      SLIDING_WINDOW_LUA, 1, key,
      this.LIMIT,
      this.WINDOW_MS,
      now,
      requestId,
    ) as [number, number];

    return { allowed: allowed === 1, remaining, resetAt };
  }
}
