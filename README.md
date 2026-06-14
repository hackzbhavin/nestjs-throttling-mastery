# 🚦 NestJS Throttling Mastery

> Production-grade throttling patterns — Token Bucket · Redis Lua · Circuit Breaker · Per-Entity Isolation · Multi-DC · Bull Queue

Built on real-world patterns from **Shopify**, **Netflix**, and **Uber**. Matches the architecture of `template-next-nestjs-my-way`.

---

## 📚 What You'll Learn

| Concept | File | Status |
|---|---|---|
| Token Bucket (Redis Lua) | `src/throttle/strategies/token-bucket.strategy.ts` | ✅ |
| Sliding Window (Redis) | `src/throttle/strategies/sliding-window.strategy.ts` | ✅ |
| Per-Entity Guard | `src/throttle/guards/entity-throttle.guard.ts` | ✅ |
| Circuit Breaker | `src/throttle/circuit-breaker.service.ts` | ✅ |
| In-Memory Fallback | `src/throttle/fallback/local-bucket.fallback.ts` | ✅ |
| Peer Sync (2-server) | `src/throttle/fallback/peer-sync.service.ts` | ✅ |
| Bull Queue Throttle | `src/queue/throttled-queue.module.ts` | ✅ |
| MySQL Fallback | `src/throttle/fallback/mysql-flush.service.ts` | ✅ |
| k6 Load Tests | `k6/throttle-test.js` | ✅ |

---

## 🏗️ Architecture Overview

```
Incoming Request
      │
      ▼
[NestJS Guard: EntityThrottleGuard]
      │
      ├─ Redis UP?  ──→ Lua Token Bucket (atomic, per entity_id)
      │
      └─ Redis DOWN? → Circuit Breaker trips
                            │
                            ├─ Local In-Memory Bucket (limit / node_count)
                            ├─ Peer Sync every 100ms (2-server coordination)
                            └─ MySQL flush every 5s (extended outage)
      │
      ▼
[Bull Queue] → concurrency limit → Worker → MySQL
```

---

## 🚀 Quick Start

```bash
# 1. Start infra
docker-compose up -d

# 2. Install deps
cd backend && npm install

# 3. Copy env
cp .env.example .env

# 4. Run
npm run start:dev

# 5. Test throttling
curl -X POST http://localhost:3000/api/demo/process \
  -H 'Content-Type: application/json' \
  -H 'x-entity-id: entity_abc' \
  -d '{"payload": "test"}'

# 6. Load test
cd k6 && k6 run throttle-test.js
```

---

## 📁 Project Structure

```
nestjs-throttling-mastery/
├── backend/
│   └── src/
│       ├── throttle/                    # 🎯 Core throttling module
│       │   ├── throttle.module.ts
│       │   ├── strategies/
│       │   │   ├── token-bucket.strategy.ts      # Redis Lua token bucket
│       │   │   └── sliding-window.strategy.ts    # Redis sliding window
│       │   ├── guards/
│       │   │   └── entity-throttle.guard.ts      # Per-entity NestJS guard
│       │   ├── fallback/
│       │   │   ├── local-bucket.fallback.ts      # In-memory fallback
│       │   │   ├── peer-sync.service.ts          # 2-server coordination
│       │   │   └── mysql-flush.service.ts        # MySQL persistence fallback
│       │   └── circuit-breaker.service.ts        # Redis circuit breaker
│       ├── queue/
│       │   └── throttled-queue.module.ts         # Bull + rate limiting
│       ├── demo/
│       │   ├── demo.controller.ts                # Demo endpoint to test
│       │   └── demo.module.ts
│       └── app.module.ts
├── k6/
│   └── throttle-test.js                         # Load test scenarios
├── docker-compose.yml
└── .env.example
```

---

## 🔑 Environment Variables

```env
# Redis
REDIS_HOST=localhost
REDIS_PORT=6379

# Throttle Config
THROTTLE_GLOBAL_LIMIT=100          # tokens per entity per window
THROTTLE_REFILL_RATE=10            # tokens added per second
THROTTLE_NODE_COUNT=2              # total app instances (for local fallback division)
THROTTLE_PEER_URLS=http://server-2:3000  # comma-separated peer URLs
THROTTLE_INTERNAL_KEY=super-secret-key

# Bull
BULL_QUEUE_LIMIT=200               # max jobs/sec globally
BULL_WORKER_CONCURRENCY=10         # parallel jobs per worker

# DB (for MySQL fallback)
DB_HOST=localhost
DB_PORT=3306
DB_DATABASE=throttle_db
DB_USERNAME=root
DB_PASSWORD=root
```

---

## 🧠 Real-World Patterns Implemented

### Shopify Pattern
Per entity isolation — `throttle:{entity_id}` Redis key. One entity's burst never affects others.

### Netflix Pattern  
Priority-based Bull jobs — premium `entity_id` gets `priority: 1`, free gets `priority: 10`.

### Uber Pattern
MySQL query latency as backpressure signal — if P99 rises, Bull concurrency drops automatically.

---

## ⚠️ Weak Points & Mitigations

| Weak Point | Mitigation in this repo |
|---|---|
| Redis SPOF | Circuit Breaker → local fallback |
| 2-server in-memory drift | Peer sync every 100ms |
| Clock skew | Redis TIME command in Lua |
| Bull queue starvation | Job priority tiers |
| Key memory leak | EXPIRE 3600 on every key |
| MySQL saturation | Concurrency = pool_size × 0.7 |

---

## 📊 Monitoring

This repo exposes Prometheus metrics:
- `throttle_allowed_total{entity_id}` — requests allowed
- `throttle_denied_total{entity_id}` — requests throttled
- `throttle_mode{mode}` — `redis` | `local` | `mysql_fallback`
- `circuit_breaker_state` — `closed` | `open`

Connect to Grafana at `http://localhost:3001` after `docker-compose up`.

---

*Built by [@hackzbhavin](https://github.com/hackzbhavin) — Pune 🇮🇳*
