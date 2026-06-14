import { Injectable, Logger } from '@nestjs/common';
import { InjectRedis } from '@nestjs-modules/ioredis';
import { ConfigService } from '@nestjs/config';
import Redis from 'ioredis';

/**
 * @architecture Token Bucket via Lua atomic script.
 *
 * Rationale:
 * - Lua script runs atomically in Redis — no race condition across 2 servers
 * - O(1) time/space per entity
 * - Uses Redis TIME command (server-side) to avoid client clock skew
 * - TTL auto-expires idle entity keys (no memory leak)
 *
 * Shopify uses this exact pattern per app_id:shop_id pair.
 */

// Language: Lua
// Keys[1] = throttle:entity_id
// ARGV[1] = max_tokens, ARGV[2] = refill_rate (tokens/sec), ARGV[3] = ttl_seconds
const TOKEN_BUCKET_SCRIPT = `
local key = KEYS[1]
local max_tokens = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

-- Use Redis server time to avoid clock skew
local now_arr = redis.call('TIME')
local now_ms = tonumber(now_arr[1]) * 1000 + math.floor(tonumber(now_arr[2]) / 1000)

local data = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(data[1]) or max_tokens
local last_refill = tonumber(data[2]) or now_ms

-- Calculate tokens to add since last refill
local elapsed_sec = (now_ms - last_refill) / 1000
local add_tokens = math.floor(elapsed_sec * refill_rate)
tokens = math.min(max_tokens, tokens + add_tokens)

if add_tokens > 0 then
  last_refill = now_ms
end

local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end

redis.call('HSET', key, 'tokens', tokens, 'last_refill', last_refill)
redis.call('EXPIRE', key, ttl)

-- Return: allowed(0/1), remaining_tokens, reset_timestamp_ms
local reset_ms = last_refill + math.ceil((max_tokens - tokens) / refill_rate) * 1000
return {allowed, tokens, reset_ms}
`;

@Injectable()
export class TokenBucketStrategy {
  private readonly logger = new Logger(TokenBucketStrategy.name);
  private scriptSha: string | null = null;

  constructor(
    @InjectRedis() private readonly redis: Redis,
    private readonly config: ConfigService,
  ) {}

  /**
   * Load Lua script using SCRIPT LOAD for efficiency (only send hash after first load)
   */
  private async getScriptSha(): Promise<string> {
    if (!this.scriptSha) {
      this.scriptSha = await this.redis.script('LOAD', TOKEN_BUCKET_SCRIPT) as string;
      this.logger.log(`Token bucket Lua script loaded: sha=${this.scriptSha}`);
    }
    return this.scriptSha;
  }

  async consume(entityId: string): Promise<{ allowed: boolean; remaining: number; resetAt: number }> {
    const maxTokens = this.config.get<number>('THROTTLE_GLOBAL_LIMIT', 100);
    const refillRate = this.config.get<number>('THROTTLE_REFILL_RATE', 10);
    const ttl = 3600; // 1 hour — prevents key memory leak

    const key = `throttle:${entityId}`;

    try {
      const sha = await this.getScriptSha();
      const result = await this.redis.evalsha(
        sha, 1, key,
        String(maxTokens), String(refillRate), String(ttl)
      ) as [number, number, number];

      return {
        allowed: result[0] === 1,
        remaining: result[1],
        resetAt: result[2],
      };
    } catch (err: any) {
      // NOSCRIPT error means Redis flushed scripts — reload and retry once
      if (err.message?.includes('NOSCRIPT')) {
        this.scriptSha = null;
        const sha = await this.getScriptSha();
        const result = await this.redis.evalsha(
          sha, 1, key,
          String(maxTokens), String(refillRate), String(ttl)
        ) as [number, number, number];
        return { allowed: result[0] === 1, remaining: result[1], resetAt: result[2] };
      }
      throw err;
    }
  }
}
