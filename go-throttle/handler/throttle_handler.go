package handler

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/hackzbhavin/nestjs-throttling-mastery/go-throttle/throttle"
)

// ThrottleMiddleware enforces per-entity throttling on every request.
//
// Entity ID resolution order (same as NestJS guard):
//  1. Header:      X-Entity-Id
//  2. Query param: ?entity_id=
//  3. Fallback:    "anonymous"
//
// On throttle: 429 + Retry-After header.
// Always sets X-RateLimit-* headers so clients can self-regulate.
func ThrottleMiddleware(mgr *throttle.Manager, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entityID := entityID(r)
		result := mgr.Check(r.Context(), entityID, 1)

		setRateLimitHeaders(w, result)

		if !result.Allowed {
			retryAfter := int(time.Until(result.ResetAt).Seconds()) + 1
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]any{
				"statusCode":         429,
				"error":              "Too Many Requests",
				"message":            "rate limit exceeded for entity " + entityID,
				"retryAfterSeconds":  retryAfter,
				"mode":               result.Mode.String(),
			})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// InternalSyncHandler handles peer gossip requests.
// Must be registered under /internal/throttle-sync and blocked from public access.
func InternalSyncHandler(mgr *throttle.Manager, local *throttle.LocalBucket, key string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Internal-Key") != key {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var peerSnap map[string]float64
		if err := json.NewDecoder(r.Body).Decode(&peerSnap); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		local.MergePeer(peerSnap)
		snap := local.Snapshot()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(snap)
	}
}

func entityID(r *http.Request) string {
	if id := r.Header.Get("X-Entity-Id"); id != "" {
		return id
	}
	if id := r.URL.Query().Get("entity_id"); id != "" {
		return id
	}
	return "anonymous"
}

func setRateLimitHeaders(w http.ResponseWriter, result throttle.CheckResult) {
	w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(result.Remaining))
	w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(result.ResetAt.UnixMilli(), 10))
	w.Header().Set("X-RateLimit-Mode", result.Mode.String())
}
