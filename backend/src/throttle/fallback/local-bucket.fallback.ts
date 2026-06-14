import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

interface LocalBucket {
  tokens: number;
  lastRefill: number;
}

/**
 * @architecture Local in-memory Token Bucket — Redis outage fallback.
 *
 * Strategy: limit / NODE_COUNT per server.
 * Rationale: If SERVER_1 allows 50 and SERVER_2 allows 50,
 * combined they allow exactly the global limit of 100 —
 * even with zero coordination. Safe by default.
 *
 * Weak point: If load is uneven (80/20 split), the busy server
 * throttles correctly but the idle server has unused capacity.
 * This is acceptable during an outage window (usually <60s).
 */
@Injectable()
export class LocalBucketFallback {
  private readonly logger = new Logger(LocalBucketFallback.name);
  private readonly buckets = new Map<string, LocalBucket>();

  constructor(private readonly config: ConfigService) {}

  private get perNodeLimit(): number {
    const globalLimit = this.config.get<number>('THROTTLE_GLOBAL_LIMIT', 100);
    const nodeCount = this.config.get<number>('THROTTLE_NODE_COUNT', 1);
    return Math.floor(globalLimit / nodeCount);
  }

  private get refillRate(): number {
    const globalRate = this.config.get<number>('THROTTLE_REFILL_RATE', 10);
    const nodeCount = this.config.get<number>('THROTTLE_NODE_COUNT', 1);
    return globalRate / nodeCount;
  }

  consume(entityId: string): { allowed: boolean; remaining: number; resetAt: number } {
    const now = Date.now();
    const limit = this.perNodeLimit;
    const rate = this.refillRate;

    if (!this.buckets.has(entityId)) {
      this.buckets.set(entityId, { tokens: limit, lastRefill: now });
    }

    const bucket = this.buckets.get(entityId)!;

    // Refill based on elapsed time
    const elapsedSec = (now - bucket.lastRefill) / 1000;
    const addTokens = Math.floor(elapsedSec * rate);
    if (addTokens > 0) {
      bucket.tokens = Math.min(limit, bucket.tokens + addTokens);
      bucket.lastRefill = now;
    }

    const allowed = bucket.tokens >= 1;
    if (allowed) bucket.tokens -= 1;

    const resetAt = now + Math.ceil((limit - bucket.tokens) / rate) * 1000;

    return { allowed, remaining: bucket.tokens, resetAt };
  }

  /**
   * Called by PeerSyncService to merge peer counters.
   * Conservative merge: take the MINIMUM tokens (most restrictive view wins).
   * This prevents overshoot when peer and local are both active.
   */
  mergePeerSnapshot(snapshot: Record<string, number>): void {
    for (const [entityId, peerTokens] of Object.entries(snapshot)) {
      const local = this.buckets.get(entityId);
      if (local) {
        local.tokens = Math.min(local.tokens, peerTokens);
      }
    }
  }

  getSnapshot(): Record<string, number> {
    const snap: Record<string, number> = {};
    this.buckets.forEach((bucket, id) => { snap[id] = bucket.tokens; });
    return snap;
  }
}
