/**
 * k6 Load Test — NestJS Throttling Mastery
 *
 * Tests per-entity isolation:
 *   - entity_001 fires at 20 req/s  → should stay within limit
 *   - entity_002 fires at 200 req/s → should get throttled (429s)
 *   - entity_003 fires at 5 req/s   → must NOT be affected by entity_002
 *
 * Key assertion: entity_003 should have 0% 429 responses
 * even while entity_002 is being fully throttled.
 *
 * Run:
 *   k6 run k6/throttle-load-test.js
 *   k6 run --env BASE_URL=http://server2:3000 k6/throttle-load-test.js
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:3000';

// Custom metrics
const throttledRate    = new Rate('throttled_rate');     // % of 429 responses
const entity003Blocked = new Counter('entity_003_blocked_MUST_BE_ZERO');

export const options = {
  scenarios: {
    // entity_001: moderate traffic — should pass fine
    entity_001_normal: {
      executor:        'constant-arrival-rate',
      rate:            20,     // 20 req/s
      timeUnit:        '1s',
      duration:        '60s',
      preAllocatedVUs: 5,
      env: { ENTITY_ID: 'entity_001' },
    },
    // entity_002: aggressive — should be throttled heavily
    entity_002_aggressive: {
      executor:        'constant-arrival-rate',
      rate:            200,    // 200 req/s — way over limit
      timeUnit:        '1s',
      duration:        '60s',
      preAllocatedVUs: 50,
      env: { ENTITY_ID: 'entity_002' },
    },
    // entity_003: low traffic — must NEVER be affected
    entity_003_innocent: {
      executor:        'constant-arrival-rate',
      rate:            5,      // 5 req/s — well within limit
      timeUnit:        '1s',
      duration:        '60s',
      preAllocatedVUs: 2,
      env: { ENTITY_ID: 'entity_003' },
    },
  },
  thresholds: {
    // entity_003 must never be blocked
    'entity_003_blocked_MUST_BE_ZERO': ['count==0'],
    // Overall 429 rate should be driven by entity_002 only
    'http_req_duration': ['p(95)<500'],
  },
};

export default function () {
  const entityId = __ENV.ENTITY_ID || 'default_entity';

  const res = http.get(`${BASE_URL}/api/demo/ping`, {
    headers: { 'x-entity-id': entityId },
  });

  const isThrottled = res.status === 429;
  throttledRate.add(isThrottled);

  // CRITICAL assertion: entity_003 must never get throttled
  if (entityId === 'entity_003' && isThrottled) {
    entity003Blocked.add(1);
  }

  check(res, {
    [`${entityId} status is 200 or 429`]: (r) => r.status === 200 || r.status === 429,
    [`${entityId} has X-RateLimit headers`]: (r) =>
      r.headers['X-Ratelimit-Remaining'] !== undefined,
  });

  sleep(0); // k6 handles rate via constant-arrival-rate executor
}

/**
 * Expected results after 60s:
 *
 *   entity_001 → ~0% throttled  (20 req/s < 100/min = 1.6/s avg — fits easily)
 *   entity_002 → ~80-90% throttled (200 req/s >> limit)
 *   entity_003 → 0% throttled  ← this is the isolation proof
 *
 *   entity_003_blocked_MUST_BE_ZERO threshold must pass.
 */
