import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
  Index,
  CreateDateColumn,
} from 'typeorm';

/**
 * @architecture Throttle fallback persistence entity.
 * Only used during extended Redis outages.
 * Schema is intentionally minimal for fast upserts.
 */
@Entity('throttle_fallback')
@Index(['entityId', 'windowStart'], { unique: true })
export class ThrottleEntity {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({ name: 'entity_id', type: 'varchar', length: 128 })
  entityId: string;

  @Column({ name: 'used_tokens', type: 'int', default: 0 })
  usedTokens: number;

  @Column({ name: 'window_start', type: 'datetime' })
  windowStart: Date;

  @CreateDateColumn({ name: 'created_at' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at' })
  updatedAt: Date;
}
