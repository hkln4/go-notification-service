// Command mockprovider imitates the external notification provider:
// POST / answers 202 like the webhook.site setup in the assignment, with
// optional latency and failures. GET /stats shows what it received.
package main

import (
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/hkln4/go-notification-service/internal/config"
	"github.com/hkln4/go-notification-service/internal/observability"
	"github.com/hkln4/go-notification-service/internal/provider/mock"
)

func main() {
	r := config.NewReader()
	addr := r.String("HTTP_ADDR", ":9090")
	cfg := mock.Config{
		FailureRate: r.Float("FAILURE_RATE", 0),
		Latency:     time.Duration(r.Int("LATENCY_MS", 50)) * time.Millisecond,
	}
	log := observability.NewLogger(os.Stdout, "mockprovider", r.String("LOG_LEVEL", config.DefaultLogLevel))
	if err := r.Err(); err != nil {
		log.Error("bad configuration", "err", err)
		os.Exit(1)
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           mock.New(cfg).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Info("mock provider listening", "addr", addr, "failure_rate", cfg.FailureRate, "latency", cfg.Latency.String())
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("mock provider exited", "err", err)
		os.Exit(1)
	}
}
