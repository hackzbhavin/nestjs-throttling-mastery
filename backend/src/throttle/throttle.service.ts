import { Injectable, Logger } from '@nestjs/common';
import { ThrottleManagerService } from './throttle-manager.service';

export interface ThrottleResult {
  allowed: boolean;
  remaining: number;
  resetAt: number;    // unix ms
  entityId: string;
}

/**
 * @architecture Facade over ThrottleManagerService.
 *
 * Controllers and guards call ThrottleService.check().
 * They never know which layer (Redis / Peer / Local) is currently active.
 * This keeps the public API stable even as internals switch modes.
 */
@Injectable()
export class ThrottleService {
  private readonly logger = new Logger(ThrottleService.name);

  constructor(private readonly manager: ThrottleManagerService) {}

  async check(entityId: string, cost = 1): Promise<ThrottleResult & { mode: string }> {
    return this.manager.check(entityId, cost);
  }

  get activeMode(): string {
    return this.manager.currentMode;
  }
}
