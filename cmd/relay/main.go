// Command relay publishes outbox rows to RabbitMQ. Several relays can run
// side by side; SKIP LOCKED keeps them from claiming the same rows.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

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

	pool, err := postgres.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	mq := broker.New(cfg.AMQPURL, broker.NewTopology(cfg.RetryDelays), log)
	defer mq.Close()

	log.Info("relay started", "batch_size", cfg.BatchSize, "poll_interval", cfg.PollInterval.String())
	relay.New(postgres.New(pool), mq, relay.Config{
		BatchSize:    cfg.BatchSize,
		PollInterval: cfg.PollInterval,
		Lease:        cfg.Lease,
	}, log).Run(ctx)
	log.Info("relay stopped")
	return nil
}
