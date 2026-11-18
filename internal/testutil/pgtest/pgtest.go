//go:build integration

// Package pgtest starts a throwaway PostgreSQL container with the real
// migrations applied, for integration tests.
package pgtest

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // pgx5:// driver
	_ "github.com/golang-migrate/migrate/v4/source/file"     // file:// source
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Image matches docker-compose.yml.
const Image = "postgres:16-alpine"

// DB is a migrated database inside a running container.
type DB struct {
	Pool      *pgxpool.Pool
	DSN       string
	container *tcpostgres.PostgresContainer
}

// Start runs a container and applies every up migration. Call it once per
// package from TestMain and Reset between tests; starting a container per
// test would dominate the run time.
func Start(ctx context.Context) (*DB, error) {
	c, err := tcpostgres.Run(ctx, Image,
		tcpostgres.WithDatabase("notification"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			// The server restarts once after initdb, so wait for the second
			// "ready" line rather than the first.
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		return nil, fmt.Errorf("start postgres container: %w", err)
	}

	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}
	if err := migrateUp(dsn); err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}
	return &DB{Pool: pool, DSN: dsn, container: c}, nil
}

func (db *DB) Close(ctx context.Context) {
	db.Pool.Close()
	_ = db.container.Terminate(ctx)
}

// Reset empties every table so tests don't see each other's rows.
func (db *DB) Reset(t testing.TB) {
	t.Helper()
	_, err := db.Pool.Exec(context.Background(),
		`TRUNCATE outbox, delivery_attempts, notifications, batches RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("reset database: %v", err)
	}
}

// MigrationsDir is the repository's migrations/ directory.
func MigrationsDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "migrations")
}

func migrateUp(dsn string) error {
	m, err := migrate.New("file://"+MigrationsDir(), "pgx5"+dsn[len("postgres"):])
	if err != nil {
		return fmt.Errorf("init migrate: %w", err)
	}
	defer m.Close()
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}
