package throttle

import (
	"log/slog"
	"sync"
	"time"
)

// CircuitState represents the health state of the Redis connection.
type CircuitState int

const (
	StateClosed   CircuitState = iota // healthy — requests flow through
	StateOpen                         // tripped — use fallback
	StateHalfOpen                     // probing — 1 test request allowed
)

func (s CircuitState) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateOpen:
		return "OPEN"
	case StateHalfOpen:
		return "HALF_OPEN"
	}
	return "UNKNOWN"
}

// CircuitBreaker protects the Redis call path.
//
// Pattern: 3 consecutive failures → OPEN → fallback.
// After resetTimeout → HALF_OPEN → 1 probe.
// Probe success → CLOSED. Probe failure → OPEN again.
type CircuitBreaker struct {
	mu               sync.Mutex
	state            CircuitState
	failureCount     int
	lastFailure      time.Time
	failureThreshold int
	resetTimeout     time.Duration
}

func NewCircuitBreaker(failureThreshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:            StateClosed,
		failureThreshold: failureThreshold,
		resetTimeout:     resetTimeout,
	}
}

func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Auto-transition OPEN → HALF_OPEN after timeout
	if cb.state == StateOpen && time.Since(cb.lastFailure) >= cb.resetTimeout {
		cb.state = StateHalfOpen
		slog.Info("circuit → HALF_OPEN (probing Redis)")
	}
	return cb.state
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state != StateClosed {
		slog.Info("circuit → CLOSED (Redis healthy)")
	}
	cb.state = StateClosed
	cb.failureCount = 0
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failureCount++
	cb.lastFailure = time.Now()
	if cb.failureCount >= cb.failureThreshold && cb.state != StateOpen {
		slog.Warn("circuit → OPEN", "failures", cb.failureCount)
		cb.state = StateOpen
	}
}

func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = StateClosed
	cb.failureCount = 0
}
