package httpapi

import (
	"bufio"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/hkln4/go-notification-service/internal/observability"
)

// MaxBodyBytes caps request bodies. A full 1000-item batch of 160-char SMS is
// well under this.
const MaxBodyBytes = 1 << 20

// statusRecorder remembers the status code for the access log.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// Hijack is needed for the WebSocket upgrade, which checks for
// http.Hijacker directly instead of going through Unwrap.
func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	r.status = http.StatusSwitchingProtocols
	return http.NewResponseController(r.ResponseWriter).Hijack()
}

// correlation reads X-Correlation-ID (or creates one), echoes it on the
// response and puts it in the request context for logs and the database.
func correlation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := observability.SanitizeCorrelationID(r.Header.Get(observability.CorrelationHeader))
		w.Header().Set(observability.CorrelationHeader, id)
		next.ServeHTTP(w, r.WithContext(observability.WithCorrelationID(r.Context(), id)))
	})
}

// accessLog logs every request and records its latency. The metric is
// labelled with the route pattern (/api/v1/notifications/{id}), not the
// path, so IDs don't create a time series each.
func accessLog(log *slog.Logger, m *observability.Metrics, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		elapsed := time.Since(start)

		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		m.ObserveHTTP(r.Method, route, strconv.Itoa(rec.status), elapsed)
		if r.URL.Path == "/metrics" || r.URL.Path == "/health" || r.URL.Path == "/ready" {
			return // scraped every few seconds; not worth a log line
		}
		log.InfoContext(r.Context(), "http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration_ms", elapsed.Milliseconds(),
		)
	})
}

// recoverPanics turns a handler panic into a 500 instead of killing the
// connection without a response.
func recoverPanics(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				// ErrAbortHandler is the documented way to abort a response;
				// let net/http handle it.
				if err, ok := v.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					panic(v)
				}
				log.ErrorContext(r.Context(), "panic in handler", "panic", v, "stack", string(debug.Stack()))
				writeError(w, http.StatusInternalServerError, codeInternal, "internal server error", nil)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func limitBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
		next.ServeHTTP(w, r)
	})
}

// Backpressure sheds load at the front door. When the relay can't keep up
// (broker down, burst far above what the workers drain), accepted
// notifications pile up in the outbox. Past Limit, creating more only makes
// every notification later, so create requests get 429 with Retry-After and
// clients back off. Reads and cancels are never refused.
type Backpressure struct {
	// Backlog returns the latest sampled outbox backlog. It must be cheap:
	// it runs on every create request, so it reads a cached value rather
	// than counting rows.
	Backlog    func() int
	Limit      int
	RetryAfter time.Duration
}

func (b Backpressure) wrap(next http.Handler) http.Handler {
	if b.Backlog == nil || b.Limit <= 0 {
		return next
	}
	retryAfter := strconv.Itoa(int(max(b.RetryAfter, time.Second).Seconds()))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if b.Backlog() > b.Limit {
			w.Header().Set("Retry-After", retryAfter)
			writeError(w, http.StatusTooManyRequests, codeOverloaded,
				"too many notifications waiting to be sent; retry later", nil)
			return
		}
		next.ServeHTTP(w, r)
	})
}
