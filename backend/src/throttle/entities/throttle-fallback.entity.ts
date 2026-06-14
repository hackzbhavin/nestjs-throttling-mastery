import { Entity, Column, PrimaryGeneratedColumn, Index, UpdateDateColumn } from 'typeorm';

/**
 * MySQL fallback table — used only during extended Redis outages.
 *
 * Tracks used tokens per entity per time window.
 * Upserted every 5s by MySQLFlushService when in LOCAL mode.
 */
@Entity('throttle_fallback')
@Index(['entityId', 'windowStart'], { unique: true })
export class ThrottleEntity {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({ name: 'entity_id', length: 255 })
  entityId: string;

  @Column({ name: 'used_tokens', type: 'int', default: 0 })
  usedTokens: number;

  @Column({ name: 'window_start', type: 'varchar', length: 30 })
  windowStart: string;

  @UpdateDateColumn({ name: 'updated_at' })
  updatedAt: Date;
}
