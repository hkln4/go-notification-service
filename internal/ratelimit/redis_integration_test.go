//go:build integration

package ratelimit_test

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/hkln4/go-notification-service/internal/ratelimit"
	"github.com/hkln4/go-notification-service/internal/testutil/rdtest"
)

var rd *rdtest.Redis

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	rd, err = rdtest.Start(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	rd.Close(ctx)
	os.Exit(code)
}

func newClient(t *testing.T) *redis.Client {
	c := redis.NewClient(&redis.Options{Addr: rd.Addr})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// Three workers, each with its own Redis connection and 10 concurrent
// senders (prefetch 10), share one channel's limit. Whatever the
// interleaving, no sliding one-second window may hold more than 100 grants.
//
// The guarantee is measured on grant (slot) times, which is where the
// limiter enforces it; send times at the provider add network jitter.
func TestRedisLimiterNeverExceedsRateInAnySlidingSecond(t *testing.T) {
	const (
		rate      = 100
		workers   = 3
		perWorker = 10
		total     = 300 // ~3 s at 100/s
	)
	key := t.Name()

	var mu sync.Mutex
	var grants []time.Time
	var wg sync.WaitGroup
	start := time.Now()
	for w := range workers {
		lim := ratelimit.NewRedis(newClient(t), rate)
		for g := range perWorker {
			n := total / (workers * perWorker)
			wg.Go(func() {
				for range n {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					slot, err := lim.Wait(ctx, key)
					cancel()
					if err != nil {
						t.Errorf("worker %d sender %d: %v", w, g, err)
						return
					}
					mu.Lock()
					grants = append(grants, slot)
					mu.Unlock()
				}
			})
		}
	}
	wg.Wait()
	elapsed := time.Since(start)

	require.Len(t, grants, total)
	require.LessOrEqual(t, ratelimit.MaxInWindow(grants, time.Second), rate)

	sort.Slice(grants, func(i, j int) bool { return grants[i].Before(grants[j]) })
	for i := 1; i < len(grants); i++ {
		require.GreaterOrEqual(t, grants[i].Sub(grants[i-1]), 10*time.Millisecond, "slots %d and %d too close", i-1, i)
	}
	// 300 slots 10 ms apart span 2.99 s; the limiter must not be slower
	// than needed either.
	require.InDelta(t, 3*time.Second, elapsed, float64(time.Second))
}

func TestRedisLimiterWaitTooLong(t *testing.T) {
	lim := ratelimit.NewRedis(newClient(t), 1)
	key := t.Name()
	_, err := lim.Wait(context.Background(), key)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = lim.Wait(ctx, key)
	require.ErrorIs(t, err, ratelimit.ErrWaitTooLong)
}

// Fail closed: without Redis nothing is sent.
func TestRedisLimiterFailsClosed(t *testing.T) {
	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	defer c.Close()
	_, err := ratelimit.NewRedis(c, 100).Wait(context.Background(), "sms")
	require.ErrorIs(t, err, ratelimit.ErrUnavailable)
}
