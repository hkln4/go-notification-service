// Package ratelimit spaces out provider calls per channel with GCRA (the
// generic cell rate algorithm), shared across workers through Redis.
//
// Why GCRA with burst 1 rather than a token bucket of 100: a bucket that
// holds 100 tokens and refills at 100/s lets 100 calls through at the end of
// one second and 100 more at the start of the next, ~200 inside a sliding
// one-second window. GCRA with burst 1 gives every call its own slot,
// 1/rate apart (10 ms at 100/s), so no one-second window can ever hold more
// than 100 slots.
//
// This package uses GCRA in reservation form: each call books the next free
// slot in one Redis round trip and then sleeps until it. Waiters don't poll
// Redis, and the order of booking is the order of sending.
package ratelimit

import (
	"context"
	"errors"
	"slices"
	"time"
)

var (
	// ErrWaitTooLong means the next free slot is later than the caller's
	// deadline. No slot was booked.
	ErrWaitTooLong = errors.New("ratelimit: next slot is past the deadline")
	// ErrUnavailable means the limiter backend could not be reached. The
	// caller must not send: the limiter fails closed.
	ErrUnavailable = errors.New("ratelimit: limiter unavailable")
)

// Limiter grants send slots per key (channel).
type Limiter interface {
	// Wait books the next slot for key, sleeps until it and returns the slot
	// time. If the slot would come after ctx's deadline it returns
	// ErrWaitTooLong without booking. If ctx ends while sleeping the slot is
	// wasted, which can only lower the rate, never raise it.
	Wait(ctx context.Context, key string) (time.Time, error)
}

// noDeadline is the booking horizon for a context without a deadline.
const noDeadline = time.Hour

func budget(ctx context.Context, now time.Time) time.Duration {
	if dl, ok := ctx.Deadline(); ok {
		return dl.Sub(now)
	}
	return noDeadline
}

func sleepUntil(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// MaxInWindow returns the largest number of times falling in any half-open
// window [t, t+window). Tests and the mock provider use it to check the
// "at most N per second" guarantee over sliding, not fixed, windows.
func MaxInWindow(times []time.Time, window time.Duration) int {
	sorted := slices.Clone(times)
	slices.SortFunc(sorted, func(a, b time.Time) int { return a.Compare(b) })
	best, lo := 0, 0
	for hi := range sorted {
		for sorted[hi].Sub(sorted[lo]) >= window {
			lo++
		}
		best = max(best, hi-lo+1)
	}
	return best
}
