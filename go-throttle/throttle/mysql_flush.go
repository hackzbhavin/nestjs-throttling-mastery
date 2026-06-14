package throttle

import (
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// MySQLFlusher persists local token state to MySQL every 5s.
//
// Activated ONLY in LOCAL mode (Redis + peer both unreachable).
// On server restart during extended outage, Seed() reloads state so
// limits survive server bounces.
//
// This is never on the hot path — it runs in a background goroutine.
type MySQLFlusher struct {
	db          *sql.DB
	local       *LocalBucket
	globalLimit int
	nodeCount   int

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
}

func NewMySQLFlusher(db *sql.DB, local *LocalBucket, globalLimit, nodeCount int) *MySQLFlusher {
	return &MySQLFlusher{
		db:          db,
		local:       local,
		globalLimit: globalLimit,
		nodeCount:   nodeCount,
	}
}

func (f *MySQLFlusher) Start() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.running {
		return
	}
	f.stopCh = make(chan struct{})
	f.running = true
	slog.Warn("MySQLFlusher: starting (Redis + peer both down)")
	go f.loop()
}

func (f *MySQLFlusher) Stop() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.running {
		return
	}
	close(f.stopCh)
	f.running = false
	slog.Info("MySQLFlusher: stopped (primary store recovered)")
}

func (f *MySQLFlusher) loop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			f.flush()
		case <-f.stopCh:
			return
		}
	}
}

func (f *MySQLFlusher) flush() {
	snap := f.local.Snapshot()
	if len(snap) == 0 {
		return
	}

	nodeLimit := f.globalLimit / f.nodeCount
	windowStart := windowStart()

	for entityID, tokens := range snap {
		usedTokens := nodeLimit - int(tokens)
		if usedTokens < 0 {
			usedTokens = 0
		}
		_, err := f.db.Exec(`
			INSERT INTO throttle_fallback (entity_id, used_tokens, window_start, updated_at)
			VALUES (?, ?, ?, NOW())
			ON DUPLICATE KEY UPDATE used_tokens = VALUES(used_tokens), updated_at = NOW()
		`, entityID, usedTokens, windowStart)
		if err != nil {
			slog.Error("mysql flush error", "entity", entityID, "err", err)
		}
	}
	slog.Debug("mysql flush done", "entities", len(snap))
}

// Seed loads state from MySQL on startup (after extended outage restart).
func (f *MySQLFlusher) Seed() error {
	nodeLimit := f.globalLimit / f.nodeCount
	rows, err := f.db.Query(`
		SELECT entity_id, used_tokens FROM throttle_fallback
		WHERE window_start = ? AND updated_at > NOW() - INTERVAL 2 MINUTE
	`, windowStart())
	if err != nil {
		return fmt.Errorf("seed query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var entityID string
		var usedTokens int
		if err := rows.Scan(&entityID, &usedTokens); err != nil {
			continue
		}
		remaining := nodeLimit - usedTokens
		if remaining < 0 {
			remaining = 0
		}
		// Peek-consume to seed the bucket at the right token level
		f.local.Consume(entityID, 0, remaining)
	}
	return rows.Err()
}

func windowStart() string {
	now := time.Now().UTC()
	return fmt.Sprintf("%d-%02d-%02dT%02d:%02d:00Z",
		now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute())
}
