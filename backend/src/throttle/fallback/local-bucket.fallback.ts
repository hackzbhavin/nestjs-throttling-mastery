import { Injectable, Logger } from '@nestjs/common';

interface Bucket {
  tokens:     number;
  lastRefill: number; // unix ms
}

/**
 * @architecture In-Memory Token Bucket Fallback
 *
 * Used when Redis is unavailable (PEER or LOCAL throttle mode).
 *
 * In PEER mode  : limit = GLOBAL_LIMIT — peer sync keeps total honest.
 * In LOCAL mode : limit = GLOBAL_LIMIT / NODE_COUNT — conservative cap per node.
 *
 * getSnapshot() / mergePeerSnapshot() are called by PeerSyncService
 * to gossip state between the 2 servers in the same DC.
 *
 * Merge rule: take the MINIMUM — most restrictive view wins.
 * This prevents overshoot when 2 servers have diverged counters.
 *
 * Memory management: keys expire after 1hr of inactivity to prevent leaks.
 */
@Injectable()
export class LocalBucketFallback {
  private readonly logger  = new Logger(LocalBucketFallback.name);
  private readonly buckets = new Map<string, Bucket>();

  private readonly REFILL_RATE = parseFloat(process.env.THROTTLE_REFILL_RATE ?? '10'); // tokens/sec
  private readonly TTL_MS      = 60 * 60 * 1000; // 1hr — evict inactive entities

  consume(
    entityId: string,
    cost   = 1,
    limit  = 100,
  ): { allowed: boolean; remaining: number; resetAt: number } {
    const now    = Date.now();
    let   bucket = this.buckets.get(entityId);

    if (!bucket) {
      bucket = { tokens: limit, lastRefill: now };
      this.buckets.set(entityId, bucket);
    }

    // Refill based on elapsed time
    const elapsed = Math.max(0, (now - bucket.lastRefill) / 1000);
    bucket.tokens    = Math.min(limit, bucket.tokens + elapsed * this.REFILL_RATE);
    bucket.lastRefill = now;

    if (bucket.tokens < cost) {
      const tokensNeeded = cost - bucket.tokens;
      const resetAt      = now + Math.ceil(tokensNeeded / this.REFILL_RATE) * 1000;
      return { allowed: false, remaining: Math.floor(bucket.tokens), resetAt };
    }

    bucket.tokens -= cost;
    const tokensNeeded = limit - bucket.tokens;
    const resetAt      = now + Math.ceil(tokensNeeded / this.REFILL_RATE) * 1000;
    return { allowed: true, remaining: Math.floor(bucket.tokens), resetAt };
  }

  // ─── Peer Sync API ──────────────────────────────────────────────────────────

  getSnapshot(): Record<string, number> {
    const snap: Record<string, number> = {};
    const cutoff = Date.now() - this.TTL_MS;
    this.buckets.forEach((bucket, entityId) => {
      if (bucket.lastRefill > cutoff) {
        snap[entityId] = bucket.tokens;
      } else {
        this.buckets.delete(entityId); // evict stale
      }
    });
    return snap;
  }

  mergePeerSnapshot(peerSnap: Record<string, number>): void {
    for (const [entityId, peerTokens] of Object.entries(peerSnap)) {
      const local = this.buckets.get(entityId);
      if (local) {
        // MINIMUM wins — prevents overshoot across 2 servers
        local.tokens = Math.min(local.tokens, peerTokens);
      }
      // If entity only on peer, don't create local entry — it will be created on first local hit
    }
  }
}
