import { Controller, Get, Headers, Query, UseGuards } from '@nestjs/common';
import { ThrottleService } from '../throttle/throttle.service';
import { EntityThrottleGuard, SkipThrottle } from '../throttle/guards/entity-throttle.guard';

/**
 * @architecture Demo controller — shows throttle in action.
 *
 * Endpoints:
 *   GET /api/demo/ping          — throttled (uses x-entity-id header)
 *   GET /api/demo/status        — not throttled, shows current throttle mode
 *   GET /api/demo/check/:id     — inspect token count for any entity_id
 *
 * Usage with k6 or curl:
 *   curl -H "x-entity-id: customer_001" http://localhost:3000/api/demo/ping
 */
@Controller('demo')
@UseGuards(EntityThrottleGuard)
export class DemoController {
  constructor(private readonly throttle: ThrottleService) {}

  @Get('ping')
  ping(@Headers('x-entity-id') entityId = 'anonymous') {
    return {
      message: 'pong',
      entityId,
      throttleMode: this.throttle.activeMode,
      ts: new Date().toISOString(),
    };
  }

  // Status endpoint — skip throttle so it always responds
  @SkipThrottle()
  @Get('status')
  status() {
    return {
      throttleMode: this.throttle.activeMode,
      ts: new Date().toISOString(),
    };
  }

  // Check token balance for a specific entity — useful for debugging
  @SkipThrottle()
  @Get('check')
  async check(@Query('entity_id') entityId = 'anonymous') {
    const result = await this.throttle.check(entityId, 0); // cost=0 → peek without consuming
    return result;
  }
}
