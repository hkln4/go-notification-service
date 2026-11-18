package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSend(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		retryAfter string
		want       Result
		wantBody   bool // BodyErr expected
	}{
		{"accepted", 202, `{"messageId":"m-1","status":"accepted","timestamp":"2026-11-18T12:00:00Z"}`, "", Result{StatusCode: 202, MessageID: "m-1"}, false},
		{"accepted with bad body", 202, `not json`, "", Result{StatusCode: 202}, true},
		{"server error", 500, `oops`, "", Result{StatusCode: 500}, false},
		{"rate limited, seconds", 429, ``, "7", Result{StatusCode: 429, RetryAfter: 7 * time.Second}, false},
		{"rate limited, bad header", 429, ``, "soon", Result{StatusCode: 429}, false},
		{"bad request", 400, `{}`, "", Result{StatusCode: 400}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tt.retryAfter != "" {
					w.Header().Set("Retry-After", tt.retryAfter)
				}
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			got, err := NewClient(srv.URL, time.Second).Send(context.Background(), uuid.New(), "", Request{})
			require.NoError(t, err)
			require.Equal(t, tt.wantBody, got.BodyErr != nil, "BodyErr = %v", got.BodyErr)
			got.BodyErr, got.Latency = nil, 0
			require.Equal(t, tt.want, got)
		})
	}
}

func TestSendRequestFormat(t *testing.T) {
	id := uuid.New()
	var gotReq Request
	var gotHeaders http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// assert, not require: this runs on the server's goroutine.
		gotHeaders = r.Header.Clone()
		assert.Equal(t, http.MethodPost, r.Method)
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&gotReq))
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	_, err := NewClient(srv.URL, time.Second).Send(context.Background(), id, "corr-9",
		Request{To: "+905551234567", Channel: "sms", Content: "Your message"})
	require.NoError(t, err)
	require.Equal(t, Request{To: "+905551234567", Channel: "sms", Content: "Your message"}, gotReq)
	require.Equal(t, "application/json", gotHeaders.Get("Content-Type"))
	require.Equal(t, id.String(), gotHeaders.Get("Idempotency-Key"))
	require.Equal(t, "corr-9", gotHeaders.Get("X-Correlation-ID"))
}

func TestSendTransportErrors(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		release := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}))
		defer srv.Close()
		defer close(release)

		start := time.Now()
		_, err := NewClient(srv.URL, 100*time.Millisecond).Send(context.Background(), uuid.New(), "", Request{})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(t, time.Since(start), 2*time.Second)
	})

	t.Run("connection refused", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		url := srv.URL
		srv.Close()
		_, err := NewClient(url, time.Second).Send(context.Background(), uuid.New(), "", Request{})
		require.Error(t, err)
	})
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		in   string
		want time.Duration
	}{
		{"", 0},
		{"0", 0},
		{"120", 2 * time.Minute},
		{"-5", 0},
		{"Wed, 18 Nov 2026 12:00:30 GMT", 30 * time.Second},
		{"Wed, 18 Nov 2026 11:59:00 GMT", 0}, // in the past
		{"garbage", 0},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, parseRetryAfter(tt.in, now), tt.in)
	}
}
