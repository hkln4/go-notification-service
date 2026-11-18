// Command api serves the REST API. It writes notifications and their outbox
// rows to PostgreSQL and never publishes to RabbitMQ; it connects to the
// broker and Redis only to report queue depth and readiness.
//
//	@title			Notification Service API
//	@version		1.0
//	@description	Event-driven notification service for SMS, email and push. Creating a notification stores it with an outbox row and returns 202; delivery happens asynchronously with retries, a per-channel rate limit of 100/s and a dead letter queue.
//	@description	Every response carries X-Correlation-ID (echoed from the request or generated). Errors have the shape {"error":{"code","message","details"}}.
//	@BasePath		/api/v1
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	httpSwagger "github.com/swaggo/http-swagger/v2"

	_ "github.com/hkln4/go-notification-service/docs" // registers the generated OpenAPI spec
	"github.com/hkln4/go-notification-service/internal/broker"
	"github.com/hkln4/go-notification-service/internal/config"
	"github.com/hkln4/go-notification-service/internal/domain"
	httpapi "github.com/hkln4/go-notification-service/internal/http"
	"github.com/hkln4/go-notification-service/internal/observability"
	"github.com/hkln4/go-notification-service/internal/service"
	"github.com/hkln4/go-notification-service/internal/store/postgres"
	"github.com/hkln4/go-notification-service/internal/ws"
)

func main() {
	if err := run(); err != nil {
		slog.Error("api exited", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadAPI(config.NewReader())
	if err != nil {
		return err
	}
	log := observability.NewLogger(os.Stdout, "api", cfg.LogLevel)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.SetupTracing(ctx, "api", config.OTLPEndpoint(config.NewReader()))
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
	store := postgres.New(pool)

	mq := broker.New(cfg.AMQPURL, broker.NewTopology(cfg.RetryDelays), log)
	defer mq.Close()
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer func() { _ = rdb.Close() }()

	metrics := observability.NewMetrics()
	var backlog atomic.Int64
	go sample(ctx, store, mq, metrics, &backlog, log)

	// Live status updates: this instance's private queue on the events
	// fanout feeds the WebSocket hub.
	hub := ws.NewHub()
	forward := hub.Forward(log)
	go mq.Subscribe(ctx, broker.ExchangeEvents, func(d broker.Delivery) { forward(d.Body) })

	ready := observability.ReadyHandler(2*time.Second,
		observability.Check{Name: "postgres", Ping: store.Ping},
		observability.Check{Name: "rabbitmq", Ping: func(context.Context) error {
			if !mq.Connected() {
				return broker.ErrNotConnected
			}
			return nil
		}},
		observability.Check{Name: "redis", Ping: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }},
	)

	srv := &http.Server{
		Addr: cfg.HTTPAddr,
		Handler: httpapi.New(service.New(store), log, httpapi.Options{
			Metrics: metrics,
			Ready:   ready,
			Backpressure: httpapi.Backpressure{
				Backlog:    func() int { return int(backlog.Load()) },
				Limit:      cfg.BackpressureLimit,
				RetryAfter: 5 * time.Second,
			},
			Swagger:   httpSwagger.Handler(httpSwagger.URL("/swagger/doc.json")),
			WebSocket: ws.NewHandler(hub, store.GetNotification, cfg.WSOrigins, log),
		}),
		// Bound how long a slow client can hold a connection.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.HTTPAddr)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	// Stop accepting connections and let in-flight requests finish.
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// sample refreshes the gauges that come from outside the process: outbox
// backlog every second (also cached in backlog for backpressure) and queue
// depths every five.
func sample(ctx context.Context, store *postgres.Store, mq *broker.Client, m *observability.Metrics,
	backlog *atomic.Int64, log *slog.Logger) {
	queues := []string{broker.QueueDLQ}
	for _, ch := range domain.Channels {
		queues = append(queues, broker.WorkQueue(string(ch)))
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if n, err := store.OutboxBacklog(ctx); err == nil {
			m.OutboxBacklog(n)
			backlog.Store(int64(n))
		} else if ctx.Err() == nil {
			log.Warn("outbox backlog sample failed", "err", err)
		}
		if i%5 != 0 {
			continue
		}
		for _, q := range queues {
			if n, err := mq.QueueDepth(q); err == nil {
				m.QueueDepth(q, n)
			}
		}
	}
}
