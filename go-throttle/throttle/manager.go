package throttle

import (
	"context"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// ThrottleMode is the active strategy.
type ThrottleMode int32

const (
	// ModeRedis — Lua atomic token bucket on Redis. Full global limit.
	ModeRedis ThrottleMode = iota
	// ModePeer — in-memory + 100ms gossip sync. Full global limit.
	ModePeer
	// ModeLocal — in-memory, no sync. limit = global / nodeCount.
	ModeLocal
)

func (m ThrottleMode) String() string {
	switch m {
	case ModeRedis:
		return "REDIS"
	case ModePeer:
		return "PEER"
	case ModeLocal:
		return "LOCAL"
	}
	return "UNKNOWN"
}

// CheckResult is what every call to Manager.Check returns.
type CheckResult struct {
	Allowed   bool
	Remaining int
	ResetAt   time.Time
	Mode      ThrottleMode
	EntityID  string
}

// Manager is the 3-mode state machine.
//
// Transitions:
//
//	REDIS ──(3 failures)──► PEER ──(5 failures)──► LOCAL
//	Any ──(Redis ping ok)──► REDIS
//
// All state reads/writes go through atomics or the modeMu mutex
// so there are no races between the probe goroutine and the hot path.
type Manager struct {
	modeMu       sync.RWMutex
	mode         ThrottleMode
	peerFailures int32 // atomic

	globalLimit int
	nodeCount   int

	circuit  *CircuitBreaker
	redis    *RedisTokenBucket
	local    *LocalBucket
	peerSync *PeerSync
	mysql    *MySQLFlusher // may be nil if DB not configured
}

// NewManager wires everything together.
func NewManager(
	globalLimit int,
	nodeCount int,
	circuit *CircuitBreaker,
	redisBucket *RedisTokenBucket,
	local *LocalBucket,
	peerSync *PeerSync,
	mysql *MySQLFlusher,
) *Manager {
	m := &Manager{
		mode:        ModeRedis,
		globalLimit: globalLimit,
		nodeCount:   nodeCount,
		circuit:     circuit,
		redis:       redisBucket,
		local:       local,
		peerSync:    peerSync,
		mysql:       mysql,
	}

	// Wire peer failure callbacks into PeerSync
	// so it can tell us when peers are unreachable.
	if peerSync != nil {
		peerSync.onFailure = m.recordPeerFailure
		peerSync.onSuccess = m.recordPeerSuccess
	}

	return m
}

// Start launches background goroutines. Call once at startup.
func (m *Manager) Start(ctx context.Context) {
	slog.Info("ThrottleManager started",
		"mode", m.Mode(),
		"global_limit", m.globalLimit,
		"node_count", m.nodeCount,
	)

	// Peer sync loop
	if m.peerSync != nil {
		go m.peerSync.Start()
	}

	// Redis health probe every 5s
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				m.probeRedis(ctx)
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

// Mode returns the current throttle mode. Safe to call from multiple goroutines.
func (m *Manager) Mode() ThrottleMode {
	m.modeMu.RLock()
	defer m.modeMu.RUnlock()
	return m.mode
}

// Check is the hot path — called on every incoming request.
func (m *Manager) Check(ctx context.Context, entityID string, cost int) CheckResult {
	switch m.Mode() {
	case ModeRedis:
		return m.checkRedis(ctx, entityID, cost)
	case ModePeer:
		return m.checkLocal(entityID, cost, m.globalLimit)
	case ModeLocal:
		return m.checkLocal(entityID, cost, m.perNodeLimit())
	}
	return m.checkLocal(entityID, cost, m.perNodeLimit())
}

// ─── MODE 1: REDIS ────────────────────────────────────────────────────────────

func (m *Manager) checkRedis(ctx context.Context, entityID string, cost int) CheckResult {
	if m.circuit.State() == StateOpen {
		m.transitionTo(ModePeer, "circuit OPEN")
		return m.checkLocal(entityID, cost, m.globalLimit)
	}

	result, err := m.redis.Consume(ctx, entityID, cost)
	if err != nil {
		m.circuit.RecordFailure()
		if m.circuit.State() == StateOpen {
			m.transitionTo(ModePeer, "redis error: "+err.Error())
		}
		// Fail-open: serve from local while Redis is sick
		return m.checkLocal(entityID, cost, m.globalLimit)
	}

	m.circuit.RecordSuccess()
	atomic.StoreInt32(&m.peerFailures, 0)
	return CheckResult{
		Allowed:   result.Allowed,
		Remaining: result.Remaining,
		ResetAt:   result.ResetAt,
		Mode:      ModeRedis,
		EntityID:  entityID,
	}
}

// ─── MODE 2 & 3: LOCAL ───────────────────────────────────────────────────────

func (m *Manager) checkLocal(entityID string, cost int, limit int) CheckResult {
	result := m.local.Consume(entityID, cost, limit)
	return CheckResult{
		Allowed:   result.Allowed,
		Remaining: result.Remaining,
		ResetAt:   result.ResetAt,
		Mode:      m.Mode(),
		EntityID:  entityID,
	}
}

// ─── PEER FAILURE TRACKING ────────────────────────────────────────────────────

func (m *Manager) recordPeerFailure() {
	n := atomic.AddInt32(&m.peerFailures, 1)
	if n >= 5 && m.Mode() == ModePeer {
		m.transitionTo(ModeLocal, "peer unreachable")
	}
}

func (m *Manager) recordPeerSuccess() {
	atomic.StoreInt32(&m.peerFailures, 0)
	if m.Mode() == ModeLocal {
		m.transitionTo(ModePeer, "peer recovered")
	}
}

// ─── REDIS RECOVERY PROBE ────────────────────────────────────────────────────

func (m *Manager) probeRedis(ctx context.Context) {
	if m.Mode() == ModeRedis {
		return
	}
	if err := m.redis.Ping(ctx); err != nil {
		return // still down
	}
	if err := m.warmUpRedis(ctx); err != nil {
		slog.Warn("redis warm-up failed", "err", err)
		return
	}
	m.circuit.Reset()
	m.transitionTo(ModeRedis, "redis recovered")
}

func (m *Manager) warmUpRedis(ctx context.Context) error {
	snap := m.local.Snapshot()
	// Per-node jitter 0–3s — prevents thundering herd when both nodes recover together
	jitter := time.Duration(rand.Float64()*3) * time.Second //nolint:gosec
	time.Sleep(jitter)
	if err := m.redis.SeedFromSnapshot(ctx, snap); err != nil {
		return err
	}
	slog.Info("redis warm-up complete", "entities_seeded", len(snap))
	return nil
}

// ─── STATE TRANSITION ────────────────────────────────────────────────────────

func (m *Manager) transitionTo(newMode ThrottleMode, reason string) {
	m.modeMu.Lock()
	defer m.modeMu.Unlock()
	if m.mode == newMode {
		return
	}
	slog.Warn("ThrottleMode transition",
		"from", m.mode,
		"to", newMode,
		"reason", reason,
	)
	m.mode = newMode

	// MySQL flush only active in LOCAL mode (extended outage)
	if m.mysql != nil {
		if newMode == ModeLocal {
			m.mysql.Start()
		} else {
			m.mysql.Stop()
		}
	}
}

func (m *Manager) perNodeLimit() int {
	return int(math.Floor(float64(m.globalLimit) / float64(m.nodeCount)))
}
