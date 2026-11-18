// Command relay publishes outbox rows to RabbitMQ. Several relays can run
// side by side; SKIP LOCKED keeps them from claiming the same rows.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hkln4/go-notification-service/internal/broker"
	"github.com/hkln4/go-notification-service/internal/config"
	"github.com/hkln4/go-notification-service/internal/observability"
	"github.com/hkln4/go-notification-service/internal/relay"
	"github.com/hkln4/go-notification-service/internal/store/postgres"
)

func main() {
	if err := run(); err != nil {
		slog.Error("relay exited", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadRelay(config.NewReader())
	if err != nil {
		return err
	}
	log := observability.NewLogger(os.Stdout, "relay", cfg.LogLevel)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.SetupTracing(ctx, "relay", config.OTLPEndpoint(config.NewReader()))
	if err != nil {
		return err
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(flushCtx)
	}()

	pool, err := postgres.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	mq := broker.New(cfg.AMQPURL, broker.NewTopology(cfg.RetryDelays), log)
	defer mq.Close()

	ops := observability.OpsServer(cfg.MetricsAddr, observability.NewMetrics(), observability.ReadyHandler(2*time.Second,
		observability.Check{Name: "postgres", Ping: pool.Ping},
		observability.Check{Name: "rabbitmq", Ping: func(context.Context) error {
			if !mq.Connected() {
				return broker.ErrNotConnected
			}
			return nil
		}},
	))
	go func() {
		if err := ops.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("ops server failed", "err", err)
		}
	}()
	defer func() { _ = ops.Close() }()

	log.Info("relay started", "batch_size", cfg.BatchSize, "poll_interval", cfg.PollInterval.String())
	relay.New(postgres.New(pool), mq, relay.Config{
		BatchSize:    cfg.BatchSize,
		PollInterval: cfg.PollInterval,
		Lease:        cfg.Lease,
	}, log).Run(ctx)
	log.Info("relay stopped")
	return nil
}
