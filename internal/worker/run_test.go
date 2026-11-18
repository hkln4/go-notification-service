package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/hkln4/go-notification-service/internal/broker"
	"github.com/hkln4/go-notification-service/internal/provider"
)

// fakeConsumer hands one delivery per queue to the handler, then behaves
// like broker.Consume: returns once ctx is done and its handlers returned.
type fakeConsumer struct {
	body    []byte
	acks    []*fakeAck
	mu      sync.Mutex
	started atomic.Int32
}

func (f *fakeConsumer) Consume(ctx context.Context, _ string, _ int, handle broker.Handler) {
	ack := &fakeAck{}
	f.mu.Lock()
	f.acks = append(f.acks, ack)
	f.mu.Unlock()

	var wg sync.WaitGroup
	wg.Go(func() {
		f.started.Add(1)
		handle(amqp.Delivery{Acknowledger: ack, DeliveryTag: 1, Body: f.body})
	})
	<-ctx.Done()
	wg.Wait()
}

// blockingSender holds every call until release is closed or ctx ends.
type blockingSender struct{ release chan struct{} }

func (s blockingSender) Send(ctx context.Context, _ uuid.UUID, _ string, _ provider.Request) (provider.Result, error) {
	select {
	case <-s.release:
		return provider.Result{StatusCode: 202}, nil
	case <-ctx.Done():
		return provider.Result{}, ctx.Err()
	}
}

func runWorker(t *testing.T, sender Sender, grace time.Duration) (*harness, *fakeConsumer, context.CancelFunc, chan struct{}) {
	h := newHarness()
	w := New(h.store, h.pub, sender, h.lim,
		Config{Lease: time.Minute, TokenWait: time.Second, RetryDelays: delays, MaxAttempts: 5}, quietLog())
	c := &fakeConsumer{body: body(t)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx, c, 10, grace); close(done) }()
	require.Eventually(t, func() bool { return c.started.Load() == 3 }, time.Second, time.Millisecond)
	return h, c, cancel, done
}

func TestRunLetsInFlightWorkFinish(t *testing.T) {
	sender := blockingSender{release: make(chan struct{})}
	h, c, cancel, done := runWorker(t, sender, 5*time.Second)

	cancel() // SIGTERM
	select {
	case <-done:
		t.Fatal("Run returned while handlers were still running")
	case <-time.After(50 * time.Millisecond):
	}

	close(sender.release) // the provider answers
	<-done
	for _, ack := range c.acks {
		require.True(t, ack.acked)
	}
	require.Len(t, h.store.outcomes, 3)
	for _, o := range h.store.outcomes {
		require.Equal(t, "sent", string(o.Status))
	}
}

func TestRunAbortsWorkAfterGrace(t *testing.T) {
	sender := blockingSender{release: make(chan struct{})} // never released
	h, c, cancel, done := runWorker(t, sender, 100*time.Millisecond)

	start := time.Now()
	cancel()
	<-done
	require.Less(t, time.Since(start), 2*time.Second, "grace must bound shutdown")

	// Aborted calls are transient failures: retried later, nothing lost.
	for _, ack := range c.acks {
		require.True(t, ack.acked)
	}
	for _, o := range h.store.outcomes {
		require.Equal(t, "retrying", string(o.Status))
	}
}
