import { Module, Global } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { InjectRedis, RedisModule } from '@nestjs-modules/ioredis';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ThrottleEntity } from './entities/throttle-fallback.entity';
import { TokenBucketStrategy } from './strategies/token-bucket.strategy';
import { SlidingWindowStrategy } from './strategies/sliding-window.strategy';
import { CircuitBreakerService } from './circuit-breaker.service';
import { LocalBucketFallback } from './fallback/local-bucket.fallback';
import { PeerSyncService } from './fallback/peer-sync.service';
import { MySQLFlushService } from './fallback/mysql-flush.service';
import { ThrottleService } from './throttle.service';

/**
 * @architecture Global throttle module — Layered Strategy Pattern
 *
 * Layer 1: Redis Token Bucket (primary, atomic Lua)
 * Layer 2: Circuit Breaker (Redis health detection)
 * Layer 3: Local In-Memory (per-node fallback, limit / NODE_COUNT)
 * Layer 4: Peer Sync (100ms gossip between 2 servers in same DC)
 * Layer 5: MySQL Flush (durable state for extended Redis outages)
 */
@Global()
@Module({
  imports: [
    TypeOrmModule.forFeature([ThrottleEntity]),
  ],
  providers: [
    TokenBucketStrategy,
    SlidingWindowStrategy,
    CircuitBreakerService,
    LocalBucketFallback,
    PeerSyncService,
    MySQLFlushService,
    ThrottleService,
  ],
  exports: [ThrottleService],
})
export class ThrottleModule {}
