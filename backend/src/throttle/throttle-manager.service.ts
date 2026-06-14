import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { CircuitBreakerService, CircuitState } from './circuit-breaker.service';
import { TokenBucketStrategy } from './strategies/token-bucket.strategy';
import { LocalBucketFallback } from './fallback/local-bucket.fallback';
import { PeerSyncService } from './fallback/peer-sync.service';
import { MySQLFlushService } from './fallback/mysql-flush.service';
import { ThrottleResult } from './throttle.service';

/**
 * @architecture 3-Mode State Machine — Strategy Pattern + Circuit Breaker
 *
 * Transitions:
 *   REDIS  ──(3 failures)──► PEER  ──(5 peer failures)──► LOCAL
 *   LOCAL  ◄──(Redis ping ok)── PEER  ◄──(peer responds)──
 *   Any mode ──(Redis ping ok)──► REDIS  (recovery via health probe)
 *
 * Mode Behaviour:
 *   REDIS : Lua atomic token bucket on Redis primary. Full GLOBAL_LIMIT.
 *   PEER  : In-memory bucket + 100ms gossip sync. Full GLOBAL_LIMIT (peer keeps honest).
 *   LOCAL : In-memory bucket, NO sync. PER_NODE_LIMIT = GLOBAL_LIMIT / NODE_COUNT (conservative).
 *
 * @rationale
 *   - REDIS is primary: single source of truth, no overshoot.
 *   - PEER bridges short Redis outages without halving capacity.
 *   - LOCAL is the safety net: accepts overshoot risk is halved by divided limit.
 *   - Recovery always tries Redis first — LOCAL/PEER are purely fallback.
 */
export enum ThrottleMode {
  REDIS = 'REDIS',
  PEER  = 'PEER',
  LOCAL = 'LOCAL',
}

@Injectable()
export class ThrottleManagerService implements OnModuleInit {
  private readonly logger = new Logger(ThrottleManagerService.name);

  private mode: ThrottleMode = ThrottleMode.REDIS;
  private peerFailures = 0;
  private redisRecoveryTimer: NodeJS.Timeout | null = null;

  private readonly GLOBAL_LIMIT: number;
  private readonly NODE_COUNT: number;

  constructor(
    private readonly config: ConfigService,
    private readonly circuit: CircuitBreakerService,
    private readonly tokenBucket: TokenBucketStrategy,
    private readonly localBucket: LocalBucketFallback,
    private readonly peerSync: PeerSyncService,
    private readonly mysqlFlush: MySQLFlushService,
  ) {
    this.GLOBAL_LIMIT = this.config.get<number>('THROTTLE_GLOBAL_LIMIT', 100);
    this.NODE_COUNT   = this.config.get<number>('THROTTLE_NODE_COUNT', 2);
  }

  onModuleInit() {
    // Health probe every 5s — tries to recover back to Redis
    setInterval(() => this.probeRedisHealth(), 5_000);
    this.logger.log(`ThrottleManager started in ${this.mode} mode | global=${this.GLOBAL_LIMIT} nodes=${this.NODE_COUNT}`);
  }

  get currentMode(): ThrottleMode {
    return this.mode;
  }

  // ─── PRIMARY ENTRY POINT ────────────────────────────────────────────────────
  async check(entityId: string, cost = 1): Promise<ThrottleResult & { mode: string }> {
    switch (this.mode) {
      case ThrottleMode.REDIS:
        return this.checkViaRedis(entityId, cost);
      case ThrottleMode.PEER:
        return this.checkViaLocal(entityId, cost, this.GLOBAL_LIMIT);
      case ThrottleMode.LOCAL:
        return this.checkViaLocal(entityId, cost, this.perNodeLimit);
    }
  }

  // ─── MODE 1: REDIS ──────────────────────────────────────────────────────────
  private async checkViaRedis(entityId: string, cost: number): Promise<ThrottleResult & { mode: string }> {
    if (this.circuit.state === CircuitState.OPEN) {
      this.transitionTo(ThrottleMode.PEER, 'Circuit OPEN');
      return this.checkViaLocal(entityId, cost, this.GLOBAL_LIMIT);
    }

    try {
      const result = await this.tokenBucket.consume(entityId, cost);
      this.circuit.recordSuccess();
      this.peerFailures = 0;
      return { ...result, mode: ThrottleMode.REDIS, entityId };
    } catch (err) {
      this.circuit.recordFailure();
      if (this.circuit.state === CircuitState.OPEN) {
        this.transitionTo(ThrottleMode.PEER, `Redis error: ${(err as Error).message}`);
      }
      return this.checkViaLocal(entityId, cost, this.GLOBAL_LIMIT);
    }
  }

  // ─── MODE 2 & 3: LOCAL (used by both PEER and LOCAL modes) ─────────────────
  private checkViaLocal(
    entityId: string,
    cost: number,
    limit: number,
  ): ThrottleResult & { mode: string } {
    const result = this.localBucket.consume(entityId, cost, limit);
    return { ...result, mode: this.mode, entityId };
  }

  // ─── PEER FAILURE TRACKING ──────────────────────────────────────────────────
  recordPeerFailure() {
    this.peerFailures++;
    if (this.peerFailures >= 5 && this.mode === ThrottleMode.PEER) {
      this.transitionTo(ThrottleMode.LOCAL, `Peer unreachable after ${this.peerFailures} failures`);
    }
  }

  recordPeerSuccess() {
    this.peerFailures = 0;
    if (this.mode === ThrottleMode.LOCAL) {
      this.transitionTo(ThrottleMode.PEER, 'Peer recovered');
    }
  }

  // ─── REDIS RECOVERY PROBE ───────────────────────────────────────────────────
  private async probeRedisHealth(): Promise<void> {
    if (this.mode === ThrottleMode.REDIS) return;

    try {
      await this.tokenBucket.ping();
      await this.warmUpRedis();
      this.circuit.reset();
      this.transitionTo(ThrottleMode.REDIS, 'Redis recovered');
    } catch {
      // still down — stay in current fallback mode
    }
  }

  // ─── REDIS WARM-UP ──────────────────────────────────────────────────────────
  // Seed Redis from local state on recovery to avoid thundering herd
  private async warmUpRedis(): Promise<void> {
    const snapshot = this.localBucket.getSnapshot();
    const jitterMs = Math.random() * 3_000; // 0–3s per-node jitter
    await new Promise(r => setTimeout(r, jitterMs));
    await this.tokenBucket.seedFromSnapshot(snapshot, this.GLOBAL_LIMIT);
    this.logger.log(`Redis warm-up complete — seeded ${Object.keys(snapshot).length} entities`);
  }

  // ─── STATE TRANSITION ───────────────────────────────────────────────────────
  private transitionTo(newMode: ThrottleMode, reason: string): void {
    if (this.mode === newMode) return;
    this.logger.warn(`ThrottleMode: ${this.mode} → ${newMode} | reason: ${reason}`);
    this.mode = newMode;

    // Start MySQL flush if extended outage (LOCAL mode)
    if (newMode === ThrottleMode.LOCAL) {
      this.mysqlFlush.startFlush();
    } else {
      this.mysqlFlush.stopFlush();
    }
  }

  private get perNodeLimit(): number {
    return Math.floor(this.GLOBAL_LIMIT / this.NODE_COUNT);
  }
}
