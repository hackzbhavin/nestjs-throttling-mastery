import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectRedis } from '@nestjs-modules/ioredis';
import Redis from 'ioredis';

export enum CircuitState {
  CLOSED = 'closed',   // Normal: Redis OK, pass through
  OPEN = 'open',       // Tripped: Redis down, use fallback
  HALF_OPEN = 'half_open', // Recovery probe: test one request
}

/**
 * @architecture Circuit Breaker for Redis health.
 *
 * State Machine:
 *   CLOSED ─(N failures)─► OPEN ─(timeout)─► HALF_OPEN ─(success)─► CLOSED
 *                                                          └(failure)─► OPEN
 *
 * Rationale: Without this, every request waits for Redis timeout (5s default)
 * when Redis is down. Circuit opens fast (3 failures) and probes recovery (15s).
 * This is the Uber AIMD pattern — cut fast, recover linearly.
 */
@Injectable()
export class CircuitBreakerService implements OnModuleInit {
  private readonly logger = new Logger(CircuitBreakerService.name);
  public state: CircuitState = CircuitState.CLOSED;
  private failureCount = 0;
  private lastOpenedAt: number | null = null;

  private get FAILURE_THRESHOLD() {
    return this.config.get<number>('THROTTLE_CIRCUIT_FAILURE_THRESHOLD', 3);
  }

  private get RECOVERY_PROBE_MS() {
    return this.config.get<number>('THROTTLE_CIRCUIT_RECOVERY_PROBE_MS', 15000);
  }

  constructor(
    @InjectRedis() private readonly redis: Redis,
    private readonly config: ConfigService,
  ) {}

  onModuleInit() {
    // Periodic check every 15s to attempt recovery from OPEN state
    setInterval(() => this.maybeTransitionToHalfOpen(), this.RECOVERY_PROBE_MS);
  }

  recordSuccess(): void {
    this.failureCount = 0;
    if (this.state === CircuitState.HALF_OPEN) {
      this.logger.log('Circuit Breaker: HALF_OPEN → CLOSED (Redis recovered)');
      this.state = CircuitState.CLOSED;
    }
  }

  recordFailure(): void {
    this.failureCount++;
    if (this.failureCount >= this.FAILURE_THRESHOLD && this.state === CircuitState.CLOSED) {
      this.state = CircuitState.OPEN;
      this.lastOpenedAt = Date.now();
      this.logger.error(`Circuit Breaker: CLOSED → OPEN after ${this.failureCount} failures`);
    }
  }

  private maybeTransitionToHalfOpen(): void {
    if (this.state === CircuitState.OPEN && this.lastOpenedAt) {
      const elapsed = Date.now() - this.lastOpenedAt;
      if (elapsed >= this.RECOVERY_PROBE_MS) {
        this.state = CircuitState.HALF_OPEN;
        this.logger.log('Circuit Breaker: OPEN → HALF_OPEN (probing Redis)');
      }
    }
  }
}
