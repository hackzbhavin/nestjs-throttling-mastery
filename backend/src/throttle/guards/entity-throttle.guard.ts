import {
  Injectable,
  CanActivate,
  ExecutionContext,
  HttpException,
  HttpStatus,
  Logger,
} from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { ThrottleService } from '../throttle.service';

export const THROTTLE_SKIP = 'throttle_skip';
export const SkipThrottle = () => Reflect.metadata(THROTTLE_SKIP, true);

/**
 * @architecture NestJS Guard — per-entity throttle enforcement.
 *
 * Extracts entity_id from:
 *   1. Request header: x-entity-id
 *   2. JWT payload: req.user.entityId
 *   3. Query param: ?entity_id=
 *
 * Returns 429 with Retry-After header on throttle.
 * Isolated: entity_A throttled ≠ entity_B affected.
 */
@Injectable()
export class EntityThrottleGuard implements CanActivate {
  private readonly logger = new Logger(EntityThrottleGuard.name);

  constructor(
    private readonly throttle: ThrottleService,
    private readonly reflector: Reflector,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const skip = this.reflector.get<boolean>(THROTTLE_SKIP, context.getHandler());
    if (skip) return true;

    const req = context.switchToHttp().getRequest();
    const res = context.switchToHttp().getResponse();

    // Entity ID resolution: header > JWT > query param
    const entityId: string =
      req.headers['x-entity-id'] ||
      req.user?.entityId ||
      req.query?.entity_id ||
      'anonymous';

    const result = await this.throttle.check(entityId);

    // Always set informational headers (Shopify pattern)
    res.setHeader('X-RateLimit-Limit', process.env.THROTTLE_GLOBAL_LIMIT ?? '100');
    res.setHeader('X-RateLimit-Remaining', result.remaining);
    res.setHeader('X-RateLimit-Reset', result.resetAt);
    res.setHeader('X-RateLimit-Mode', result.mode);

    if (!result.allowed) {
      const retryAfterSec = Math.ceil((result.resetAt - Date.now()) / 1000);
      res.setHeader('Retry-After', retryAfterSec);

      this.logger.warn(`THROTTLED entity=${entityId} mode=${result.mode}`);

      throw new HttpException(
        {
          statusCode: HttpStatus.TOO_MANY_REQUESTS,
          error: 'Too Many Requests',
          message: `Rate limit exceeded for entity ${entityId}`,
          retryAfterSeconds: retryAfterSec,
          mode: result.mode,
        },
        HttpStatus.TOO_MANY_REQUESTS,
      );
    }

    return true;
  }
}
