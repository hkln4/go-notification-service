package observability

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReadyHandler(t *testing.T) {
	ok := Check{Name: "postgres", Ping: func(context.Context) error { return nil }}
	down := Check{Name: "redis", Ping: func(context.Context) error { return errors.New("connection refused") }}
	slow := Check{Name: "rabbitmq", Ping: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}

	tests := []struct {
		name   string
		checks []Check
		want   int
	}{
		{"all ok", []Check{ok}, http.StatusOK},
		{"one down", []Check{ok, down}, http.StatusServiceUnavailable},
		{"one too slow", []Check{ok, slow}, http.StatusServiceUnavailable},
		{"no checks", nil, http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			start := time.Now()
			ReadyHandler(50*time.Millisecond, tt.checks...).ServeHTTP(rec, httptest.NewRequest("GET", "/ready", nil))
			require.Less(t, time.Since(start), time.Second, "timeout bounds slow checks")
			require.Equal(t, tt.want, rec.Code)

			var body struct {
				Checks map[string]string `json:"checks"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			require.Len(t, body.Checks, len(tt.checks))
		})
	}
}

func TestMetricsExposition(t *testing.T) {
	m := NewMetrics()
	m.Created("sms")
	m.Delivered("sms", "sent", 30*time.Millisecond)
	m.QueueDepth("q.sms", 7)

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	out := rec.Body.String()
	for _, want := range []string{
		`notifications_created_total{channel="sms"} 1`,
		`notifications_delivered_total{channel="sms",result="sent"} 1`,
		`notification_queue_depth{queue="q.sms"} 7`,
		`notification_delivery_duration_seconds_count{channel="sms"} 1`,
	} {
		require.True(t, strings.Contains(out, want), "missing %s", want)
	}

	var nilMetrics *Metrics
	nilMetrics.Created("sms") // must not panic
}

// Known label combinations exist from the start with 0, so queries never
// come back empty just because nothing has happened yet.
func TestInitCreatesZeroSeries(t *testing.T) {
	channels := []string{"sms", "email", "push"}
	tests := []struct {
		name    string
		init    func(*Metrics)
		want    []string
		notWant []string
	}{
		{"api", func(m *Metrics) { m.InitAPI(channels) },
			[]string{`notifications_created_total{channel="push"} 0`}, nil},
		{"worker with breaker", func(m *Metrics) { m.InitWorker(channels, []string{"sent", "dead_letter"}, true) },
			[]string{
				`notifications_delivered_total{channel="email",result="dead_letter"} 0`,
				`notifications_delivered_total{channel="sms",result="sent"} 0`,
				`notification_delivery_duration_seconds_count{channel="push"} 0`,
				`notification_end_to_end_seconds_count{channel="sms"} 0`,
				`circuit_breaker_state{channel="email"} 0`,
			}, nil},
		{"worker without breaker", func(m *Metrics) { m.InitWorker(channels, []string{"sent"}, false) },
			[]string{`notifications_delivered_total{channel="sms",result="sent"} 0`},
			[]string{"circuit_breaker_state{"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMetrics()
			tt.init(m)
			rec := httptest.NewRecorder()
			m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
			for _, w := range tt.want {
				require.Contains(t, rec.Body.String(), w)
			}
			for _, w := range tt.notWant {
				require.NotContains(t, rec.Body.String(), w)
			}
		})
	}

	var nilMetrics *Metrics
	nilMetrics.InitAPI(channels) // must not panic
	nilMetrics.InitWorker(channels, nil, true)
}
