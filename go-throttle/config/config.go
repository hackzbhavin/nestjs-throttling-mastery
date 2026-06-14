package config

import (
	"os"
	"strconv"
	"strings"
)

// Config holds all runtime configuration.
// Loaded from environment variables — same keys as the NestJS .env.example.
type Config struct {
	Port        string
	RedisURL    string
	DBDSN       string
	GlobalLimit int
	RefillRate  float64 // tokens per second
	NodeCount   int
	PeerURLs    []string
	InternalKey string

	// Circuit breaker
	CBFailureThreshold int
	CBResetTimeoutSec  int
}

func Load() *Config {
	c := &Config{
		Port:               getStr("PORT", "3000"),
		RedisURL:           getStr("REDIS_URL", "redis://localhost:6379"),
		DBDSN:              getStr("DB_DSN", "root:password@tcp(localhost:3306)/throttle_db?parseTime=true"),
		GlobalLimit:        getInt("THROTTLE_GLOBAL_LIMIT", 100),
		RefillRate:         getFloat("THROTTLE_REFILL_RATE", 10),
		NodeCount:          getInt("THROTTLE_NODE_COUNT", 2),
		InternalKey:        getStr("THROTTLE_INTERNAL_KEY", "change-me"),
		CBFailureThreshold: getInt("CB_FAILURE_THRESHOLD", 3),
		CBResetTimeoutSec:  getInt("CB_RESET_TIMEOUT_SEC", 10),
	}

	raw := getStr("THROTTLE_PEER_URLS", "")
	for _, u := range strings.Split(raw, ",") {
		u = strings.TrimSpace(u)
		if u != "" {
			c.PeerURLs = append(c.PeerURLs, u)
		}
	}

	return c
}

// DBDSName returns DSN suitable for database/sql.
func (c *Config) DBDSName() string {
	return c.DBDSN
}

func getStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}
