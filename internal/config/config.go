// Package config reads settings from environment variables. Every setting
// has a default that works inside docker-compose, so no .env file is needed.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Reader collects parse errors so a binary can report every bad variable at
// once instead of failing on the first.
type Reader struct {
	lookup func(string) (string, bool)
	errs   []error
}

func NewReader() *Reader { return &Reader{lookup: os.LookupEnv} }

// NewReaderFrom reads from a map; used by tests.
func NewReaderFrom(env map[string]string) *Reader {
	return &Reader{lookup: func(k string) (string, bool) { v, ok := env[k]; return v, ok }}
}

func (r *Reader) String(key, def string) string {
	if v, ok := r.lookup(key); ok && v != "" {
		return v
	}
	return def
}

func (r *Reader) Int(key string, def int) int {
	v, ok := r.lookup(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %q is not an integer", key, v))
		return def
	}
	return n
}

func (r *Reader) Float(key string, def float64) float64 {
	v, ok := r.lookup(key)
	if !ok || v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %q is not a number", key, v))
		return def
	}
	return f
}

func (r *Reader) Duration(key string, def time.Duration) time.Duration {
	v, ok := r.lookup(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %q is not a duration", key, v))
		return def
	}
	return d
}

// Durations parses a comma separated list such as "5s,30s,2m,10m".
func (r *Reader) Durations(key string, def []time.Duration) []time.Duration {
	v, ok := r.lookup(key)
	if !ok || v == "" {
		return def
	}
	var out []time.Duration
	for _, part := range strings.Split(v, ",") {
		d, err := time.ParseDuration(strings.TrimSpace(part))
		if err != nil || d <= 0 {
			r.errs = append(r.errs, fmt.Errorf("%s: %q is not a list of positive durations", key, v))
			return def
		}
		out = append(out, d)
	}
	return out
}

// Err returns every parse error seen so far, or nil.
func (r *Reader) Err() error {
	if len(r.errs) == 0 {
		return nil
	}
	return fmt.Errorf("invalid configuration: %v", r.errs)
}

// Defaults shared by several binaries.
const (
	DefaultAMQPURL     = "amqp://notification:notification@localhost:5672/"                                 //nolint:gosec // local dev default, same as docker-compose
	DefaultDatabaseURL = "postgres://notification:notification@localhost:5432/notification?sslmode=disable" //nolint:gosec // local dev default, same as docker-compose
	DefaultLogLevel    = "info"
)

type API struct {
	HTTPAddr        string
	DatabaseURL     string
	AMQPURL         string
	RedisAddr       string
	LogLevel        string
	RetryDelays     []time.Duration
	ShutdownTimeout time.Duration
	// BackpressureLimit is the outbox backlog above which creates get 429.
	// 0 disables it.
	BackpressureLimit int
}

func LoadAPI(r *Reader) (API, error) {
	c := API{
		HTTPAddr:          r.String("HTTP_ADDR", ":8080"),
		DatabaseURL:       r.String("DATABASE_URL", DefaultDatabaseURL),
		AMQPURL:           r.String("AMQP_URL", DefaultAMQPURL),
		RedisAddr:         r.String("REDIS_ADDR", "localhost:6379"),
		LogLevel:          r.String("LOG_LEVEL", DefaultLogLevel),
		RetryDelays:       r.Durations("RETRY_DELAYS", DefaultRetryDelays),
		ShutdownTimeout:   r.Duration("SHUTDOWN_TIMEOUT", 10*time.Second),
		BackpressureLimit: r.Int("BACKPRESSURE_LIMIT", 50_000),
	}
	return c, r.Err()
}

// DefaultRetryDelays is the retry ladder: attempt 1 failed → 5s ... → 10m.
var DefaultRetryDelays = []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute}

type Relay struct {
	DatabaseURL  string
	AMQPURL      string
	MetricsAddr  string
	LogLevel     string
	BatchSize    int
	PollInterval time.Duration
	Lease        time.Duration
	RetryDelays  []time.Duration
}

func LoadRelay(r *Reader) (Relay, error) {
	c := Relay{
		DatabaseURL:  r.String("DATABASE_URL", DefaultDatabaseURL),
		AMQPURL:      r.String("AMQP_URL", DefaultAMQPURL),
		MetricsAddr:  r.String("METRICS_ADDR", ":9100"),
		LogLevel:     r.String("LOG_LEVEL", DefaultLogLevel),
		BatchSize:    r.Int("RELAY_BATCH_SIZE", 100),
		PollInterval: r.Duration("RELAY_POLL_INTERVAL", 200*time.Millisecond),
		Lease:        r.Duration("RELAY_LEASE", 30*time.Second),
		// Needed because every binary declares the full topology.
		RetryDelays: r.Durations("RETRY_DELAYS", DefaultRetryDelays),
	}
	return c, r.Err()
}

type Worker struct {
	DatabaseURL     string
	AMQPURL         string
	RedisAddr       string
	ProviderURL     string
	MetricsAddr     string
	LogLevel        string
	RatePerSecond   int
	Prefetch        int
	Lease           time.Duration
	TokenWait       time.Duration
	ProviderTimeout time.Duration
	RetryDelays     []time.Duration
	MaxAttempts     int
	ShutdownTimeout time.Duration
}

func LoadWorker(r *Reader) (Worker, error) {
	c := Worker{
		DatabaseURL:     r.String("DATABASE_URL", DefaultDatabaseURL),
		AMQPURL:         r.String("AMQP_URL", DefaultAMQPURL),
		RedisAddr:       r.String("REDIS_ADDR", "localhost:6379"),
		ProviderURL:     r.String("PROVIDER_URL", "http://localhost:9090/"),
		MetricsAddr:     r.String("METRICS_ADDR", ":9100"),
		LogLevel:        r.String("LOG_LEVEL", DefaultLogLevel),
		RatePerSecond:   r.Int("RATE_LIMIT_PER_SECOND", 100),
		Prefetch:        r.Int("WORKER_PREFETCH", 10),
		Lease:           r.Duration("WORKER_LEASE", 30*time.Second),
		TokenWait:       r.Duration("TOKEN_WAIT", 10*time.Second),
		ProviderTimeout: r.Duration("PROVIDER_TIMEOUT", 5*time.Second),
		RetryDelays:     r.Durations("RETRY_DELAYS", DefaultRetryDelays),
		MaxAttempts:     r.Int("MAX_ATTEMPTS", 5),
		ShutdownTimeout: r.Duration("SHUTDOWN_TIMEOUT", 10*time.Second),
	}
	if err := r.Err(); err != nil {
		return c, err
	}
	// A lease shorter than one provider call would let a second worker
	// claim a notification that is still being sent.
	if c.Lease <= c.ProviderTimeout {
		return c, fmt.Errorf("invalid configuration: WORKER_LEASE (%s) must be longer than PROVIDER_TIMEOUT (%s)",
			c.Lease, c.ProviderTimeout)
	}
	if c.RatePerSecond <= 0 || c.Prefetch <= 0 || c.MaxAttempts <= 0 {
		return c, fmt.Errorf("invalid configuration: RATE_LIMIT_PER_SECOND, WORKER_PREFETCH and MAX_ATTEMPTS must be positive")
	}
	return c, nil
}
