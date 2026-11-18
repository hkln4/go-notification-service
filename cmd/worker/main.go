// Command worker consumes the sms, email and push queues and delivers
// notifications to the provider. Run several (docker compose --scale
// worker=3); the shared Redis limiter keeps each channel at its rate.
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

	"github.com/redis/go-redis/v9"

	"github.com/hkln4/go-notification-service/internal/breaker"
	"github.com/hkln4/go-notification-service/internal/broker"
	"github.com/hkln4/go-notification-service/internal/config"
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

	metrics := observability.NewMetrics()
	ops := observability.OpsServer(cfg.MetricsAddr, metrics, observability.ReadyHandler(2*time.Second,
		observability.Check{Name: "postgres", Ping: pool.Ping},
		observability.Check{Name: "rabbitmq", Ping: func(context.Context) error {
			if !mq.Connected() {
				return broker.ErrNotConnected
			}
			return nil
		}},
		observability.Check{Name: "redis", Ping: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }},
	))
	go serveOps(ops, log)
	defer func() { _ = ops.Close() }()

	var cb *breaker.Breaker
	if cfg.BreakerThreshold > 0 {
		cb = breaker.New(breaker.Config{
			Threshold: cfg.BreakerThreshold,
			Cooldown:  cfg.BreakerCooldown,
			OnChange: func(channel string, s breaker.State) {
				metrics.BreakerState(channel, int(s))
				log.Warn("circuit breaker state changed", "channel", channel, "state", s.String())
			},
		})
	}

	w := worker.New(
		postgres.New(pool),
		mq,
		provider.NewClient(cfg.ProviderURL, cfg.ProviderTimeout),
		ratelimit.NewRedis(rdb, cfg.RatePerSecond),
		worker.Config{
			Lease: cfg.Lease, TokenWait: cfg.TokenWait, RetryDelays: cfg.RetryDelays,
			MaxAttempts: cfg.MaxAttempts, Metrics: metrics, Breaker: cb,
		},
		log,
	)

	log.Info("worker started", "prefetch", cfg.Prefetch, "rate_per_second", cfg.RatePerSecond)
	w.Run(ctx, mq, cfg.Prefetch, cfg.ShutdownTimeout)
	log.Info("worker stopped")
	return nil
}

func serveOps(srv *http.Server, log *slog.Logger) {
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("ops server failed", "err", err)
	}
}
