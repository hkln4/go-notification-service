package worker

import (
	"context"
	"sync"
	"time"

	"github.com/hkln4/go-notification-service/internal/broker"
	"github.com/hkln4/go-notification-service/internal/domain"
)

type Consumer interface {
	Consume(ctx context.Context, queue string, prefetch int, handle broker.Handler)
}

// Run consumes every channel's work queue until ctx is cancelled, then shuts
// down gracefully:
//
//  1. Consumers are cancelled at once, so no new message is taken.
//     Prefetched messages that no handler started return to the queue.
//  2. Running handlers get up to grace to finish (a provider call is at
//     most a few seconds, so normally they all do).
//  3. If grace runs out, the handlers' context is cancelled: a rate limit
//     wait or provider call is aborted, the attempt is recorded as a
//     transient failure and retried later.
//
// Run returns when every handler has returned.
func (w *Worker) Run(ctx context.Context, c Consumer, prefetch int, grace time.Duration) {
	// Handlers must outlive ctx by up to grace, so they get their own.
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()

	var wg sync.WaitGroup
	for _, ch := range domain.Channels {
		wg.Go(func() {
			c.Consume(ctx, broker.WorkQueue(string(ch)), prefetch, func(d broker.Delivery) {
				w.Handle(workCtx, d)
			})
		})
	}

	<-ctx.Done()
	w.log.Info("shutting down: no new messages; waiting for in-flight work", "grace", grace.String())
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		w.log.Info("in-flight work finished")
	case <-time.After(grace):
		w.log.Warn("grace period over; aborting in-flight work")
		cancelWork()
		<-done
	}
}
