// Package config reads settings from environment variables. Every setting
// has a default that works inside docker-compose, so no .env file is needed.
package config

import (
	"fmt"
	"os"
	"strconv"
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

// Err returns every parse error seen so far, or nil.
func (r *Reader) Err() error {
	if len(r.errs) == 0 {
		return nil
	}
	return fmt.Errorf("invalid configuration: %v", r.errs)
}

// Defaults shared by several binaries.
const (
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
