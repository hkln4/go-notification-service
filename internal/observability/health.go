package observability

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Check is one dependency probed by /ready.
type Check struct {
	Name string
	Ping func(ctx context.Context) error
}

// HealthHandler answers /health: the process is up and serving HTTP. It
// checks nothing else, so an orchestrator doesn't restart a healthy process
// just because a dependency is down.
func HealthHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeStatus(w, http.StatusOK, map[string]any{"status": "ok"})
	})
}

// ReadyHandler answers /ready: every dependency answered within the timeout.
// 503 tells a load balancer to stop sending traffic until they recover.
func ReadyHandler(timeout time.Duration, checks ...Check) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		results := make(map[string]string, len(checks))
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, c := range checks {
			wg.Go(func() {
				res := "ok"
				if err := c.Ping(ctx); err != nil {
					res = err.Error()
				}
				mu.Lock()
				results[c.Name] = res
				mu.Unlock()
			})
		}
		wg.Wait()

		status, code := "ready", http.StatusOK
		for _, res := range results {
			if res != "ok" {
				status, code = "not ready", http.StatusServiceUnavailable
			}
		}
		writeStatus(w, code, map[string]any{"status": status, "checks": results})
	})
}

func writeStatus(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// OpsServer serves /metrics, /health and /ready for binaries without an API
// (relay, worker).
func OpsServer(addr string, m *Metrics, ready http.Handler) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	mux.Handle("GET /health", HealthHandler())
	mux.Handle("GET /ready", ready)
	return &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}
