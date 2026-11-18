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
	LogLevel        string
	ShutdownTimeout time.Duration
}

func LoadAPI(r *Reader) (API, error) {
	c := API{
		HTTPAddr:        r.String("HTTP_ADDR", ":8080"),
		DatabaseURL:     r.String("DATABASE_URL", DefaultDatabaseURL),
		LogLevel:        r.String("LOG_LEVEL", DefaultLogLevel),
		ShutdownTimeout: r.Duration("SHUTDOWN_TIMEOUT", 10*time.Second),
	}
	return c, r.Err()
}

// DefaultRetryDelays is the retry ladder: attempt 1 failed → 5s ... → 10m.
var DefaultRetryDelays = []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute}

type Relay struct {
	DatabaseURL  string
	AMQPURL      string
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
		LogLevel:     r.String("LOG_LEVEL", DefaultLogLevel),
		BatchSize:    r.Int("RELAY_BATCH_SIZE", 100),
		PollInterval: r.Duration("RELAY_POLL_INTERVAL", 200*time.Millisecond),
		Lease:        r.Duration("RELAY_LEASE", 30*time.Second),
		// Needed because every binary declares the full topology.
		RetryDelays: r.Durations("RETRY_DELAYS", DefaultRetryDelays),
	}
	return c, r.Err()
}
