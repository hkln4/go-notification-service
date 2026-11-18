package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeClock lets the test control time; sleep advances it instead of
// blocking, so slots can be checked exactly.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if d > 0 {
		c.now = c.now.Add(d)
	}
	return ctx.Err()
}

func newFakeMemory(perSecond int) (*Memory, *fakeClock) {
	clock := &fakeClock{now: time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)}
	m := NewMemory(perSecond)
	m.now, m.sleep = clock.Now, clock.Sleep
	return m, clock
}

func TestMemorySlotsAreEvenlySpaced(t *testing.T) {
	m, clock := newFakeMemory(100)
	start := clock.Now()

	var slots []time.Time
	for range 250 {
		slot, err := m.Wait(context.Background(), "sms")
		require.NoError(t, err)
		slots = append(slots, slot)
	}
	for i, s := range slots {
		require.Equal(t, start.Add(time.Duration(i)*10*time.Millisecond), s, "slot %d", i)
	}
	require.Equal(t, 100, MaxInWindow(slots, time.Second))
}

func TestMemoryKeysAreIndependent(t *testing.T) {
	m, clock := newFakeMemory(1) // one per second: a second call on a key must wait
	for _, key := range []string{"sms", "email", "push"} {
		slot, err := m.Wait(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, clock.Now(), slot, "%s gets an immediate slot", key)
	}
}

func TestMemoryIdleTimeIsNotBanked(t *testing.T) {
	m, clock := newFakeMemory(100)
	_, err := m.Wait(context.Background(), "sms")
	require.NoError(t, err)

	// An idle minute must not turn into a burst afterwards.
	clock.mu.Lock()
	clock.now = clock.now.Add(time.Minute)
	clock.mu.Unlock()
	first, err := m.Wait(context.Background(), "sms")
	require.NoError(t, err)
	second, err := m.Wait(context.Background(), "sms")
	require.NoError(t, err)
	require.Equal(t, 10*time.Millisecond, second.Sub(first))
}

func TestMemoryWaitTooLongDoesNotBook(t *testing.T) {
	m, clock := newFakeMemory(1)
	_, err := m.Wait(context.Background(), "sms") // next slot is now+1s
	require.NoError(t, err)

	ctx, cancel := context.WithDeadline(context.Background(), clock.Now().Add(500*time.Millisecond))
	defer cancel()
	m.now = clock.Now // deadline is compared with the fake clock
	_, err = m.Wait(ctx, "sms")
	require.ErrorIs(t, err, ErrWaitTooLong)

	// The refused call must not have pushed the next slot further out.
	slot, err := m.Wait(context.Background(), "sms")
	require.NoError(t, err)
	require.Equal(t, clock.Now(), slot)
}

func TestMemoryWaitRespectsCancel(t *testing.T) {
	m := NewMemory(1)
	_, err := m.Wait(context.Background(), "sms")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	start := time.Now()
	_, err = m.Wait(ctx, "sms")
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(start), 500*time.Millisecond)
}

func TestMaxInWindow(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(ms ...int) []time.Time {
		out := make([]time.Time, len(ms))
		for i, m := range ms {
			out[i] = base.Add(time.Duration(m) * time.Millisecond)
		}
		return out
	}
	tests := []struct {
		name  string
		times []time.Time
		want  int
	}{
		{"empty", nil, 0},
		{"one", at(0), 1},
		{"window is half open", at(0, 1000), 1},
		{"just inside", at(0, 999), 2},
		{"densest window wins", at(0, 100, 900, 950, 1500, 1600), 4},
		{"unsorted input", at(950, 0, 900, 100), 4},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, MaxInWindow(tt.times, time.Second), tt.name)
	}
}
