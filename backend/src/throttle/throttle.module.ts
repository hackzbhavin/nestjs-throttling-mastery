import { Module, Global } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ThrottleEntity } from './entities/throttle-fallback.entity';
import { TokenBucketStrategy } from './strategies/token-bucket.strategy';
import { SlidingWindowStrategy } from './strategies/sliding-window.strategy';
import { CircuitBreakerService } from './circuit-breaker.service';
import { LocalBucketFallback } from './fallback/local-bucket.fallback';
import { PeerSyncService, ThrottleSyncController } from './fallback/peer-sync.service';
import { MySQLFlushService } from './fallback/mysql-flush.service';
import { ThrottleService } from './throttle.service';
import { ThrottleManagerService } from './throttle-manager.service';
import { EntityThrottleGuard } from './guards/entity-throttle.guard';

/**
 * @architecture Global throttle module — Layered Strategy Pattern
 *
 * Layer 1 : Redis Token Bucket (primary, atomic Lua)
 * Layer 2 : Circuit Breaker (Redis health detection)
 * Layer 3 : Peer Sync (100ms gossip between 2 servers in same DC)
 * Layer 4 : Local In-Memory (per-node fallback, limit / NODE_COUNT)
 * Layer 5 : MySQL Flush (durable state for extended Redis outages >5min)
 *
 * ThrottleManagerService is the state machine that switches between layers.
 * ThrottleService is the facade callers use — they never see layer details.
 * EntityThrottleGuard applies throttling at the controller level via @UseGuards.
 */
@Global()
@Module({
  imports: [
    TypeOrmModule.forFeature([ThrottleEntity]),
  ],
  controllers: [
    ThrottleSyncController, // internal-only peer sync endpoint
  ],
  providers: [
    TokenBucketStrategy,
    SlidingWindowStrategy,
    CircuitBreakerService,
    LocalBucketFallback,
    PeerSyncService,
    MySQLFlushService,
    ThrottleManagerService,
    ThrottleService,
    EntityThrottleGuard,
  ],
  exports: [
    ThrottleService,
    ThrottleManagerService,
    EntityThrottleGuard,
  ],
})
export class ThrottleModule {}
