import { Injectable, Logger } from '@nestjs/common';

export enum CircuitState {
  CLOSED   = 'CLOSED',   // healthy — requests pass through
  OPEN     = 'OPEN',     // unhealthy — requests blocked, use fallback
  HALF_OPEN = 'HALF_OPEN', // recovery probe — 1 request allowed through
}

/**
 * @architecture Circuit Breaker for Redis health detection.
 *
 * States:
 *   CLOSED    → healthy, all requests go to Redis.
 *   OPEN      → tripped (≥3 failures), route to fallback immediately.
 *   HALF_OPEN → after RESET_TIMEOUT, allow 1 probe request.
 *               On success → CLOSED. On failure → OPEN again.
 *
 * This prevents thundering herd on a recovering Redis:
 * without it, all servers hammer Redis the moment it comes back.
 *
 * Thresholds (tunable via env):
 *   FAILURE_THRESHOLD  = 3   — failures before trip
 *   RESET_TIMEOUT_MS   = 10s — OPEN → HALF_OPEN wait
 */
@Injectable()
export class CircuitBreakerService {
  private readonly logger = new Logger(CircuitBreakerService.name);

  private _state:          CircuitState = CircuitState.CLOSED;
  private failureCount:    number       = 0;
  private lastFailureTime: number       = 0;

  private readonly FAILURE_THRESHOLD = parseInt(process.env.CB_FAILURE_THRESHOLD ?? '3',     10);
  private readonly RESET_TIMEOUT_MS  = parseInt(process.env.CB_RESET_TIMEOUT_MS  ?? '10000', 10);

  get state(): CircuitState {
    if (
      this._state === CircuitState.OPEN &&
      Date.now() - this.lastFailureTime >= this.RESET_TIMEOUT_MS
    ) {
      this._state = CircuitState.HALF_OPEN;
      this.logger.log('Circuit → HALF_OPEN (probing Redis)');
    }
    return this._state;
  }

  recordSuccess(): void {
    if (this._state !== CircuitState.CLOSED) {
      this.logger.log('Circuit → CLOSED (Redis healthy)');
    }
    this.failureCount    = 0;
    this.lastFailureTime = 0;
    this._state          = CircuitState.CLOSED;
  }

  recordFailure(): void {
    this.failureCount++;
    this.lastFailureTime = Date.now();
    if (this.failureCount >= this.FAILURE_THRESHOLD) {
      if (this._state !== CircuitState.OPEN) {
        this.logger.warn(`Circuit → OPEN (${this.failureCount} failures)`);
      }
      this._state = CircuitState.OPEN;
    }
  }

  reset(): void {
    this.failureCount    = 0;
    this.lastFailureTime = 0;
    this._state          = CircuitState.CLOSED;
  }
}
