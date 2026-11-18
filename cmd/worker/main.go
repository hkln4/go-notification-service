// Command worker consumes the sms, email and push queues and delivers
// notifications to the provider. Run several (docker compose --scale
// worker=3); the shared Redis limiter keeps each channel at its rate.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/redis/go-redis/v9"

	"github.com/hkln4/go-notification-service/internal/broker"
	"github.com/hkln4/go-notification-service/internal/config"
	"github.com/hkln4/go-notification-service/internal/domain"
	"github.com/hkln4/go-notification-service/internal/observability"
	"github.com/hkln4/go-notification-service/internal/provider"
	"github.com/hkln4/go-notification-service/internal/ratelimit"
	"github.com/hkln4/go-notification-service/internal/store/postgres"
	"github.com/hkln4/go-notification-service/internal/worker"
)

func main() {
	if err := run(); err != nil {
		slog.Error("worker exited", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadWorker(config.NewReader())
	if err != nil {
		return err
	}
	log := observability.NewLogger(os.Stdout, "worker", cfg.LogLevel)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer func() { _ = rdb.Close() }()

	mq := broker.New(cfg.AMQPURL, broker.NewTopology(cfg.RetryDelays), log)
	defer mq.Close()

	w := worker.New(
		postgres.New(pool),
		mq,
		provider.NewClient(cfg.ProviderURL, cfg.ProviderTimeout),
		ratelimit.NewRedis(rdb, cfg.RatePerSecond),
		worker.Config{Lease: cfg.Lease, TokenWait: cfg.TokenWait, RetryDelays: cfg.RetryDelays, MaxAttempts: cfg.MaxAttempts},
		log,
	)

	log.Info("worker started", "prefetch", cfg.Prefetch, "rate_per_second", cfg.RatePerSecond)
	var wg sync.WaitGroup
	for _, ch := range domain.Channels {
		wg.Go(func() {
			mq.Consume(ctx, broker.WorkQueue(string(ch)), cfg.Prefetch, func(d broker.Delivery) {
				w.Handle(context.Background(), d)
			})
		})
	}
	wg.Wait()
	log.Info("worker stopped")
	return nil
}
