import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { CircuitBreakerService, CircuitState } from './circuit-breaker.service';
import { TokenBucketStrategy } from './strategies/token-bucket.strategy';
import { LocalBucketFallback } from './fallback/local-bucket.fallback';

export interface ThrottleResult {
  allowed: boolean;
  remaining: number;
  resetAt: number;   // unix ms
  mode: 'redis' | 'local' | 'mysql_fallback';
  entityId: string;
}

/**
 * @architecture Facade over all throttle strategies.
 * Callers never know which layer is active — clean separation of concerns.
 */
@Injectable()
export class ThrottleService {
  private readonly logger = new Logger(ThrottleService.name);

  constructor(
    private readonly circuit: CircuitBreakerService,
    private readonly tokenBucket: TokenBucketStrategy,
    private readonly localBucket: LocalBucketFallback,
    private readonly config: ConfigService,
  ) {}

  async check(entityId: string): Promise<ThrottleResult> {
    // Layer 1: Try Redis via Circuit Breaker
    if (this.circuit.state !== CircuitState.OPEN) {
      try {
        const result = await this.tokenBucket.consume(entityId);
        this.circuit.recordSuccess();
        return { ...result, mode: 'redis', entityId };
      } catch (err) {
        this.circuit.recordFailure();
        this.logger.warn(`Redis throttle failed for ${entityId}, falling back to local`);
      }
    }

    // Layer 2: Local in-memory fallback (limit / NODE_COUNT)
    const local = this.localBucket.consume(entityId);
    return { ...local, mode: 'local', entityId };
  }
}
