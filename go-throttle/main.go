package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"

	"github.com/hackzbhavin/nestjs-throttling-mastery/go-throttle/config"
	"github.com/hackzbhavin/nestjs-throttling-mastery/go-throttle/handler"
	"github.com/hackzbhavin/nestjs-throttling-mastery/go-throttle/throttle"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg := config.Load()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// ── Redis ────────────────────────────────────────────────────────────────
	opt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		slog.Error("invalid redis URL", "err", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(opt)

	// ── MySQL (optional — only needed for LOCAL mode flush) ───────────────────
	var db *sql.DB
	db, err = sql.Open("mysql", cfg.DBDSName())
	if err != nil {
		slog.Warn("mysql unavailable — MySQL flush disabled", "err", err)
		db = nil
	}

	// ── Wire throttle components ──────────────────────────────────────────────
	circuit := throttle.NewCircuitBreaker(
		cfg.CBFailureThreshold,
		time.Duration(cfg.CBResetTimeoutSec)*time.Second,
	)

	redisBucket := throttle.NewRedisTokenBucket(rdb, cfg.GlobalLimit, cfg.RefillRate)
	localBucket := throttle.NewLocalBucket(cfg.RefillRate)

	// PeerSync callbacks are wired inside NewManager
	peerSync := throttle.NewPeerSync(localBucket, cfg.PeerURLs, cfg.InternalKey, nil, nil)

	var mysqlFlusher *throttle.MySQLFlusher
	if db != nil {
		mysqlFlusher = throttle.NewMySQLFlusher(db, localBucket, cfg.GlobalLimit, cfg.NodeCount)
		if seedErr := mysqlFlusher.Seed(); seedErr != nil {
			slog.Warn("mysql seed skipped", "err", seedErr)
		}
	}

	mgr := throttle.NewManager(
		cfg.GlobalLimit,
		cfg.NodeCount,
		circuit,
		redisBucket,
		localBucket,
		peerSync,
		mysqlFlusher,
	)
	mgr.Start(ctx)

	// ── Routes ────────────────────────────────────────────────────────────────
	mux := http.NewServeMux()

	// Demo: ping endpoint — throttled
	mux.HandleFunc("GET /api/demo/ping", func(w http.ResponseWriter, r *http.Request) {
		entityID := r.Header.Get("X-Entity-Id")
		if entityID == "" {
			entityID = "anonymous"
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"message":      "pong",
			"entityId":     entityID,
			"throttleMode": mgr.Mode().String(),
			"ts":           time.Now().Format(time.RFC3339),
		})
	})

	// Demo: status — not throttled
	mux.HandleFunc("GET /api/demo/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"throttleMode": mgr.Mode().String(),
			"ts":           time.Now().Format(time.RFC3339),
		})
	})

	// Internal: peer sync (private — block from public LB)
	mux.Handle("POST /internal/throttle-sync",
		handler.InternalSyncHandler(mgr, localBucket, cfg.InternalKey),
	)

	// Apply throttle middleware to everything under /api/
	root := http.NewServeMux()
	root.Handle("/api/", handler.ThrottleMiddleware(mgr, mux))
	root.Handle("/internal/", mux) // internal routes skip throttle middleware

	// ── Server ────────────────────────────────────────────────────────────────
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      root,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	slog.Info("Go throttle server starting",
		"port", cfg.Port,
		"global_limit", cfg.GlobalLimit,
		"node_count", cfg.NodeCount,
		"peers", cfg.PeerURLs,
	)

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	srv.Shutdown(shutdownCtx) //nolint:errcheck
}
