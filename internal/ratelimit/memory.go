package ratelimit

import (
	"context"
	"sync"
	"time"
)

// Memory is the same algorithm as Redis for a single process. Unit tests use
// it; it is not shared between workers.
type Memory struct {
	interval time.Duration
	now      func() time.Time
	sleep    func(context.Context, time.Duration) error

	mu  sync.Mutex
	tat map[string]time.Time
}

func NewMemory(perSecond int) *Memory {
	return &Memory{
		interval: time.Second / time.Duration(perSecond),
		now:      time.Now,
		sleep:    sleepUntil,
		tat:      map[string]time.Time{},
	}
}

func (m *Memory) Wait(ctx context.Context, key string) (time.Time, error) {
	slot, wait, err := m.reserve(ctx, key)
	if err != nil {
		return time.Time{}, err
	}
	if err := m.sleep(ctx, wait); err != nil {
		return time.Time{}, err
	}
	return slot, nil
}

func (m *Memory) reserve(ctx context.Context, key string) (time.Time, time.Duration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	tat := m.tat[key]
	if tat.Before(now) {
		tat = now
	}
	wait := tat.Sub(now)
	if wait > budget(ctx, now) {
		return time.Time{}, 0, ErrWaitTooLong
	}
	m.tat[key] = tat.Add(m.interval)
	return tat, wait, nil
}
