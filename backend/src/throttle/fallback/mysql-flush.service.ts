import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { LocalBucketFallback } from './local-bucket.fallback';
import { ThrottleEntity } from '../entities/throttle-fallback.entity';
import { ConfigService } from '@nestjs/config';

/**
 * @architecture MySQL Persistence — Extended Outage Fallback.
 *
 * When Redis is down for minutes+ (not just seconds),
 * local in-memory state is lost on server restart.
 * This service:
 *   1. Flushes local counter snapshots to MySQL every 5s
 *   2. Seeds local counters from MySQL on startup (if Redis still down)
 *
 * Rationale: MySQL is slower but durable. Use only as last resort.
 * This is the "Shopify offline mode" — degraded but correct.
 */
@Injectable()
export class MySQLFlushService implements OnModuleInit {
  private readonly logger = new Logger(MySQLFlushService.name);
  private readonly FLUSH_INTERVAL_MS = 5000;

  constructor(
    @InjectRepository(ThrottleEntity)
    private readonly repo: Repository<ThrottleEntity>,
    private readonly localBucket: LocalBucketFallback,
    private readonly config: ConfigService,
  ) {}

  async onModuleInit() {
    await this.seedFromMySQL();
    setInterval(() => this.flushToMySQL(), this.FLUSH_INTERVAL_MS);
  }

  private getWindowStart(): Date {
    const windowMs = this.config.get<number>('THROTTLE_WINDOW_MS', 60000);
    const now = Date.now();
    return new Date(Math.floor(now / windowMs) * windowMs);
  }

  async flushToMySQL(): Promise<void> {
    const snapshot = this.localBucket.getSnapshot();
    const entries = Object.entries(snapshot);
    if (!entries.length) return;

    const windowStart = this.getWindowStart();
    const globalLimit = this.config.get<number>('THROTTLE_GLOBAL_LIMIT', 100);

    try {
      // Upsert all entities in one query
      const values = entries.map(([entityId, tokens]) => ({
        entityId,
        usedTokens: Math.max(0, globalLimit - tokens),
        windowStart,
      }));

      await this.repo
        .createQueryBuilder()
        .insert()
        .into(ThrottleEntity)
        .values(values)
        .orUpdate(['used_tokens', 'updated_at'], ['entity_id', 'window_start'])
        .execute();
    } catch (err) {
      this.logger.error('MySQL flush failed', err);
    }
  }

  async seedFromMySQL(): Promise<void> {
    const windowStart = this.getWindowStart();
    const globalLimit = this.config.get<number>('THROTTLE_GLOBAL_LIMIT', 100);

    try {
      const rows = await this.repo.find({ where: { windowStart } });
      if (!rows.length) return;

      this.logger.log(`Seeding ${rows.length} entity counters from MySQL`);

      // Reconstruct local buckets from MySQL state
      const snapshot: Record<string, number> = {};
      rows.forEach(row => {
        snapshot[row.entityId] = Math.max(0, globalLimit - row.usedTokens);
      });
      this.localBucket.mergePeerSnapshot(snapshot);
    } catch (err) {
      this.logger.error('MySQL seed failed', err);
    }
  }
}
