package throttle

import (
	"math"
	"sync"
	"time"
)

// entry is one token bucket for a single entity.
type entry struct {
	tokens     float64
	lastRefill time.Time
}

// LocalBucket is an in-memory token bucket store.
//
// Used when Redis is unavailable (PEER or LOCAL throttle mode).
//
// Thread-safe: each entity has its own slot in the map,
// and a single RWMutex guards the map itself.
// We use a full Lock (not RLock) because every consume is a write.
type LocalBucket struct {
	mu         sync.Mutex
	buckets    map[string]*entry
	refillRate float64       // tokens per second
	ttl        time.Duration // evict inactive entities after this
}

func NewLocalBucket(refillRate float64) *LocalBucket {
	return &LocalBucket{
		buckets:    make(map[string]*entry),
		refillRate: refillRate,
		ttl:        1 * time.Hour,
	}
}

// ConsumeResult is what Consume returns.
type ConsumeResult struct {
	Allowed   bool
	Remaining int
	ResetAt   time.Time
}

// Consume takes `cost` tokens from entityID's bucket.
// limit is the cap for this bucket — callers pass different limits
// depending on ThrottleMode (global or global/nodeCount).
func (lb *LocalBucket) Consume(entityID string, cost int, limit int) ConsumeResult {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	now := time.Now()
	e, ok := lb.buckets[entityID]
	if !ok {
		e = &entry{tokens: float64(limit), lastRefill: now}
		lb.buckets[entityID] = e
	}

	// Refill: add tokens proportional to elapsed time
	elapsed := now.Sub(e.lastRefill).Seconds()
	e.tokens = math.Min(float64(limit), e.tokens+elapsed*lb.refillRate)
	e.lastRefill = now

	tokensNeeded := float64(limit) - e.tokens
	resetAt := now.Add(time.Duration(tokensNeeded/lb.refillRate) * time.Second)

	if e.tokens < float64(cost) {
		return ConsumeResult{
			Allowed:   false,
			Remaining: int(e.tokens),
			ResetAt:   resetAt,
		}
	}

	e.tokens -= float64(cost)
	tokensNeeded = float64(limit) - e.tokens
	resetAt = now.Add(time.Duration(tokensNeeded/lb.refillRate) * time.Second)
	return ConsumeResult{
		Allowed:   true,
		Remaining: int(e.tokens),
		ResetAt:   resetAt,
	}
}

// Snapshot returns current token counts for all active entities.
// Also evicts entries that haven't been touched in > ttl.
func (lb *LocalBucket) Snapshot() map[string]float64 {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	cutoff := time.Now().Add(-lb.ttl)
	snap := make(map[string]float64, len(lb.buckets))
	for id, e := range lb.buckets {
		if e.lastRefill.Before(cutoff) {
			delete(lb.buckets, id) // evict stale
			continue
		}
		snap[id] = e.tokens
	}
	return snap
}

// MergePeer applies a peer's snapshot using minimum-wins rule.
// Called by PeerSync every 100ms — prevents overshoot across 2 servers.
func (lb *LocalBucket) MergePeer(peerSnap map[string]float64) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	for id, peerTokens := range peerSnap {
		if e, ok := lb.buckets[id]; ok {
			e.tokens = math.Min(e.tokens, peerTokens)
		}
		// Don't create new entries for peer-only entities.
		// They'll be created on first local hit.
	}
}
