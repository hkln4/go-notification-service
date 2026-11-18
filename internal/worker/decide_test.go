package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDecide(t *testing.T) {
	p := Policy{
		Delays:      []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute},
		MaxAttempts: 5,
	}
	netErr := errors.New("connection refused")
	retry := func(d time.Duration) Decision { return Decision{Action: ActionRetry, Delay: d} }
	sent := Decision{Action: ActionSent}
	dead := Decision{Action: ActionDeadLetter}
	perm := Decision{Action: ActionPermanentFail}

	tests := []struct {
		name       string
		status     int
		err        error
		attempt    int
		retryAfter time.Duration
		want       Decision
	}{
		{"200", 200, nil, 1, 0, sent},
		{"202", 202, nil, 1, 0, sent},
		{"2xx on the last attempt", 204, nil, 5, 0, sent},

		{"500 after attempt 1", 500, nil, 1, 0, retry(5 * time.Second)},
		{"503 after attempt 2", 503, nil, 2, 0, retry(30 * time.Second)},
		{"502 after attempt 3", 502, nil, 3, 0, retry(2 * time.Minute)},
		{"500 after attempt 4", 500, nil, 4, 0, retry(10 * time.Minute)},
		{"500 after attempt 5", 500, nil, 5, 0, dead},
		{"500 beyond max (crash recovery)", 500, nil, 6, 0, dead},

		{"timeout", 0, context.DeadlineExceeded, 1, 0, retry(5 * time.Second)},
		{"network error", 0, netErr, 3, 0, retry(2 * time.Minute)},
		{"network error on last attempt", 0, netErr, 5, 0, dead},

		{"429 without Retry-After uses ladder", 429, nil, 2, 0, retry(30 * time.Second)},
		{"429 Retry-After below min clamps up", 429, nil, 1, time.Second, retry(5 * time.Second)},
		{"429 Retry-After exact queue", 429, nil, 1, 30 * time.Second, retry(30 * time.Second)},
		{"429 Retry-After rounds up, never down", 429, nil, 1, 31 * time.Second, retry(2 * time.Minute)},
		{"429 Retry-After above max clamps down", 429, nil, 1, time.Hour, retry(10 * time.Minute)},
		{"429 still uses an attempt", 429, nil, 5, 5 * time.Second, dead},

		{"400", 400, nil, 1, 0, perm},
		{"401", 401, nil, 1, 0, perm},
		{"404", 404, nil, 2, 0, perm},
		{"422", 422, nil, 1, 0, perm},
		{"unexpected 3xx", 302, nil, 1, 0, perm},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, p.Decide(tt.status, tt.err, tt.attempt, tt.retryAfter))
		})
	}
}
