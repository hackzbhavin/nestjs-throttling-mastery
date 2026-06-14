import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { LocalBucketFallback } from './local-bucket.fallback';
import { ThrottleEntity } from '../entities/throttle-fallback.entity';

/**
 * @architecture MySQL Flush — Durable fallback for extended Redis outages.
 *
 * Activated ONLY when ThrottleMode = LOCAL (Redis + peer both unreachable).
 * Writes local counter state to MySQL every 5s.
 *
 * On server restart during outage: seeds local buckets from MySQL
 * so counters survive restarts — prevents quota reset on server bounce.
 *
 * This is Strategy 4 from the outage decision tree:
 *   outage > 5min → MySQL flush active
 *
 * @rationale
 *   MySQL is slower than Redis but durable.
 *   5s batch interval balances durability vs. query load.
 *   We never query MySQL on the hot path — only in background flush.
 */
@Injectable()
export class MySQLFlushService implements OnModuleDestroy {
  private readonly logger    = new Logger(MySQLFlushService.name);
  private          timer: NodeJS.Timeout | null = null;
  private readonly FLUSH_INTERVAL_MS = 5_000;

  constructor(
    private readonly localBucket: LocalBucketFallback,
    @InjectRepository(ThrottleEntity)
    private readonly repo: Repository<ThrottleEntity>,
  ) {}

  startFlush(): void {
    if (this.timer) return; // already running
    this.logger.warn('MySQLFlushService: starting periodic flush (Redis+Peer both down)');
    this.timer = setInterval(() => this.flush(), this.FLUSH_INTERVAL_MS);
  }

  stopFlush(): void {
    if (!this.timer) return;
    clearInterval(this.timer);
    this.timer = null;
    this.logger.log('MySQLFlushService: stopped (primary store recovered)');
  }

  async seedFromMySQL(): Promise<void> {
    const windowStart = this.getWindowStart();
    const rows = await this.repo
      .createQueryBuilder('t')
      .where('t.windowStart = :windowStart', { windowStart })
      .andWhere('t.updatedAt > NOW() - INTERVAL 2 MINUTE')
      .getMany();

    for (const row of rows) {
      const globalLimit = parseInt(process.env.THROTTLE_GLOBAL_LIMIT ?? '100', 10);
      const nodeCount   = parseInt(process.env.THROTTLE_NODE_COUNT   ?? '2',   10);
      const nodeLimit   = Math.floor(globalLimit / nodeCount);
      const remaining   = Math.max(0, nodeLimit - row.usedTokens);
      this.localBucket.consume(row.entityId, 0, remaining); // seed by consuming 0
    }

    this.logger.log(`Seeded ${rows.length} entities from MySQL on startup`);
  }

  onModuleDestroy(): void {
    this.stopFlush();
  }

  private async flush(): Promise<void> {
    const snapshot     = this.localBucket.getSnapshot();
    const windowStart  = this.getWindowStart();
    const globalLimit  = parseInt(process.env.THROTTLE_GLOBAL_LIMIT ?? '100', 10);
    const nodeCount    = parseInt(process.env.THROTTLE_NODE_COUNT   ?? '2',   10);
    const nodeLimit    = Math.floor(globalLimit / nodeCount);
    const entities     = Object.entries(snapshot);

    if (!entities.length) return;

    try {
      // Upsert in batch
      for (const [entityId, tokens] of entities) {
        const usedTokens = Math.max(0, nodeLimit - Math.floor(tokens));
        await this.repo.upsert(
          { entityId, usedTokens, windowStart },
          ['entityId', 'windowStart'],
        );
      }
      this.logger.debug(`MySQL flush: ${entities.length} entities written`);
    } catch (err) {
      this.logger.error(`MySQL flush failed: ${(err as Error).message}`);
    }
  }

  private getWindowStart(): string {
    const d = new Date();
    d.setSeconds(0, 0);
    return d.toISOString();
  }
}
