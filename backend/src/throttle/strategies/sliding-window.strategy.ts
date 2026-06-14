import { Injectable } from '@nestjs/common';
import { InjectRedis } from '@nestjs-modules/ioredis';
import { ConfigService } from '@nestjs/config';
import Redis from 'ioredis';

/**
 * @architecture Sliding Window Counter using Redis Sorted Set.
 *
 * Rationale vs Token Bucket:
 * - More precise: counts ACTUAL requests in the last N ms (not virtual tokens)
 * - Slightly more expensive: O(log N) ZADD + ZREMRANGEBYSCORE
 * - Best for: SLA enforcement, billing, audit logs
 *
 * Use Token Bucket for high-frequency, Sliding Window for billing-grade accuracy.
 */
@Injectable()
export class SlidingWindowStrategy {
  constructor(
    @InjectRedis() private readonly redis: Redis,
    private readonly config: ConfigService,
  ) {}

  async consume(entityId: string): Promise<{ allowed: boolean; remaining: number; resetAt: number }> {
    const limit = this.config.get<number>('THROTTLE_GLOBAL_LIMIT', 100);
    const windowMs = this.config.get<number>('THROTTLE_WINDOW_MS', 60000);

    const now = Date.now();
    const windowStart = now - windowMs;
    const key = `swthrottle:${entityId}`;

    const pipeline = this.redis.pipeline();
    pipeline.zremrangebyscore(key, '-inf', windowStart);    // remove old entries
    pipeline.zadd(key, now, `${now}-${Math.random()}`);    // add current request
    pipeline.zcard(key);                                   // count in window
    pipeline.expire(key, Math.ceil(windowMs / 1000) + 60); // TTL

    const results = await pipeline.exec();
    const count = (results?.[2]?.[1] as number) ?? 0;

    return {
      allowed: count <= limit,
      remaining: Math.max(0, limit - count),
      resetAt: now + windowMs,
    };
  }
}
