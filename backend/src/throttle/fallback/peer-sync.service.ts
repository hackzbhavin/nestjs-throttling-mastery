import { Injectable, Logger, OnModuleInit, Controller, Post, Body, Headers, UnauthorizedException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { LocalBucketFallback } from './local-bucket.fallback';

/**
 * @architecture Gossip-based Peer Sync for 2-server same-DC setup.
 *
 * Problem: When Redis is down, 2 servers each have LOCAL buckets.
 * Without sync, Server1 and Server2 together allow 2x the intended limit.
 *
 * Solution: Every 100ms, exchange snapshots and take the minimum.
 * Convergence time: 100ms (1 sync cycle).
 * Overshoot window: at most 100ms worth of requests — acceptable.
 *
 * Netflix uses a similar "delta sync" pattern between edge nodes.
 */
@Injectable()
export class PeerSyncService implements OnModuleInit {
  private readonly logger = new Logger(PeerSyncService.name);
  private readonly SYNC_INTERVAL_MS = 100;

  constructor(
    private readonly localBucket: LocalBucketFallback,
    private readonly config: ConfigService,
  ) {}

  onModuleInit() {
    const peerUrls = this.config.get<string>('THROTTLE_PEER_URLS', '');
    if (!peerUrls) {
      this.logger.warn('No THROTTLE_PEER_URLS set — peer sync disabled (single node mode)');
      return;
    }
    setInterval(() => this.syncWithPeers(), this.SYNC_INTERVAL_MS);
    this.logger.log(`Peer sync enabled — syncing every ${this.SYNC_INTERVAL_MS}ms with: ${peerUrls}`);
  }

  private async syncWithPeers(): Promise<void> {
    const peerUrls = this.config.get<string>('THROTTLE_PEER_URLS', '').split(',').filter(Boolean);
    const internalKey = this.config.get<string>('THROTTLE_INTERNAL_KEY', '');
    const localSnapshot = this.localBucket.getSnapshot();

    for (const peerUrl of peerUrls) {
      try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 50); // 50ms hard timeout

        const res = await fetch(`${peerUrl.trim()}/internal/throttle-sync`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Internal-Key': internalKey,
          },
          body: JSON.stringify(localSnapshot),
          signal: controller.signal,
        });

        clearTimeout(timeout);

        if (res.ok) {
          const peerSnapshot = await res.json() as Record<string, number>;
          this.localBucket.mergePeerSnapshot(peerSnapshot);
        }
      } catch {
        // Peer unreachable — silent fail, continue with local bucket
        // Do NOT log every failure at error level — this floods logs during DC outage
      }
    }
  }
}

/**
 * Internal-only HTTP endpoint.
 * Only reachable between backend servers (private VPC / docker network).
 * NEVER expose this to the public internet.
 */
@Controller('internal')
export class ThrottleSyncController {
  constructor(
    private readonly localBucket: LocalBucketFallback,
    private readonly config: ConfigService,
  ) {}

  @Post('throttle-sync')
  sync(
    @Body() peerSnapshot: Record<string, number>,
    @Headers('x-internal-key') key: string,
  ): Record<string, number> {
    const expected = this.config.get<string>('THROTTLE_INTERNAL_KEY', '');
    if (!expected || key !== expected) throw new UnauthorizedException();

    // Merge peer data and return our snapshot
    this.localBucket.mergePeerSnapshot(peerSnapshot);
    return this.localBucket.getSnapshot();
  }
}
