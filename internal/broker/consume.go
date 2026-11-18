package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Delivery is re-exported so callers don't import amqp091-go for it.
type Delivery = amqp.Delivery

// Handler processes one delivery and must ack or nack it.
type Handler func(Delivery)

// Consume delivers messages from queue to handle, running up to prefetch
// handlers at once (manual ack). It survives reconnects: when the channel or
// connection drops, unacked messages go back to the queue on the broker side,
// and Consume subscribes again once the client is connected.
//
// When ctx is cancelled Consume stops taking new deliveries, waits for
// running handlers to return and then returns. Handlers get their own
// context from the caller, so the caller decides how long they may run.
func (c *Client) Consume(ctx context.Context, queue string, prefetch int, handle Handler) {
	for {
		if c.WaitReady(ctx) != nil {
			return // ctx done
		}
		err := c.consumeOnce(ctx, queue, prefetch, handle)
		if ctx.Err() != nil {
			return
		}
		c.log.Warn("consumer stopped, resubscribing", "queue", queue, "err", err)
		// Don't spin if subscribing fails straight away (e.g. channel error).
		select {
		case <-time.After(resubscribeDelay):
		case <-ctx.Done():
			return
		}
	}
}

const resubscribeDelay = time.Second

func (c *Client) consumeOnce(ctx context.Context, queue string, prefetch int, handle Handler) error {
	conn, _ := c.current()
	if conn == nil {
		return ErrNotConnected
	}
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	// Prefetch bounds unacked messages per consumer. Priority only reorders
	// messages still waiting in the queue, so a small prefetch keeps most
	// of the backlog where priority can act on it.
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("set qos: %w", err)
	}
	// Plain Consume plus an explicit Cancel below, rather than
	// ConsumeWithContext: the latter cancels from its own goroutine, which
	// races with our Close on the same channel and can hang it.
	tag := "consumer-" + uuid.NewString()
	deliveries, err := ch.Consume(queue, tag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %s: %w", queue, err)
	}
	c.log.Info("consuming", "queue", queue, "prefetch", prefetch)

	// Prefetch already caps in-flight deliveries; the semaphore just turns
	// that into a goroutine limit.
	sem := make(chan struct{}, prefetch)
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		select {
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("delivery channel closed")
			}
			sem <- struct{}{}
			wg.Go(func() {
				defer func() { <-sem }()
				handle(d)
			})
		case <-ctx.Done():
			// Stop deliveries. Messages already prefetched but not handed to
			// a handler stay unacked and return to the queue when the
			// channel closes, after running handlers have finished (the
			// deferred wg.Wait runs before the deferred Close).
			_ = ch.Cancel(tag, false)
			return nil
		}
	}
}

// Subscribe receives every message published to a fanout exchange through a
// private queue: server-named, exclusive and auto-deleted, so each process
// gets its own copy of every event and the queue disappears with the
// connection. Messages are auto-acked; events are best effort. Like Consume
// it resubscribes after a reconnect and returns when ctx is cancelled.
func (c *Client) Subscribe(ctx context.Context, exchange string, handle func(Delivery)) {
	for {
		if c.WaitReady(ctx) != nil {
			return
		}
		err := c.subscribeOnce(ctx, exchange, handle)
		if ctx.Err() != nil {
			return
		}
		c.log.Warn("subscription stopped, resubscribing", "exchange", exchange, "err", err)
		select {
		case <-time.After(resubscribeDelay):
		case <-ctx.Done():
			return
		}
	}
}

func (c *Client) subscribeOnce(ctx context.Context, exchange string, handle func(Delivery)) error {
	conn, _ := c.current()
	if conn == nil {
		return ErrNotConnected
	}
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return fmt.Errorf("declare private queue: %w", err)
	}
	if err := ch.QueueBind(q.Name, "", exchange, false, nil); err != nil {
		return fmt.Errorf("bind private queue: %w", err)
	}
	tag := "subscriber-" + uuid.NewString()
	deliveries, err := ch.Consume(q.Name, tag, true, true, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %s: %w", q.Name, err)
	}
	for {
		select {
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("delivery channel closed")
			}
			handle(d)
		case <-ctx.Done():
			_ = ch.Cancel(tag, false)
			return nil
		}
	}
}
