//go:build integration

package broker_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/hkln4/go-notification-service/internal/broker"
	"github.com/hkln4/go-notification-service/internal/testutil/mqtest"
)

var mq *mqtest.MQ

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	mq, err = mqtest.Start(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	mq.Close(ctx)
	os.Exit(code)
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// newClient connects with a topology whose channel name is unique to the
// test, so tests don't see each other's queues.
func newClient(t *testing.T, channel string, delays ...time.Duration) *broker.Client {
	t.Helper()
	c := broker.New(mq.URL, broker.Topology{Channels: []string{channel}, RetryDelays: delays}, quiet)
	t.Cleanup(c.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, c.WaitReady(ctx))

	queues := []string{broker.WorkQueue(channel), broker.QueueDLQ}
	for _, d := range delays {
		queues = append(queues, broker.RetryQueue(channel, d))
	}
	require.NoError(t, mq.Purge(queues...))
	return c
}

// collect consumes queue in the background and acks everything it gets.
func collect(t *testing.T, c *broker.Client, queue string) <-chan amqp.Delivery {
	t.Helper()
	out := make(chan amqp.Delivery, 100)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Consume(ctx, queue, 10, func(d amqp.Delivery) {
			_ = d.Ack(false)
			out <- d
		})
	}()
	t.Cleanup(func() { cancel(); <-done })
	return out
}

func next(t *testing.T, ch <-chan amqp.Delivery) amqp.Delivery {
	t.Helper()
	select {
	case d := <-ch:
		return d
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for a delivery")
		return amqp.Delivery{}
	}
}

func work(channel, id string, prio uint8) broker.Message {
	return broker.Message{
		Exchange: broker.ExchangeNotifications, RoutingKey: channel,
		MessageID: id, Priority: prio, Body: []byte(`{}`),
	}
}

func TestTopologyDeclareIsIdempotent(t *testing.T) {
	newClient(t, "idem", time.Second)
	newClient(t, "idem", time.Second) // declares everything again
}

func TestPublishConfirmsAndDetectsUnroutable(t *testing.T) {
	c := newClient(t, "pub")
	ctx := context.Background()

	errs := c.Publish(ctx,
		work("pub", "a", 0),
		work("no-such-channel", "b", 0), // nothing bound to this key
		work("pub", "c", 0),
		broker.Message{RoutingKey: broker.QueueDLQ, MessageID: "d"}, // default exchange
	)
	require.NoError(t, errs[0])
	require.ErrorIs(t, errs[1], broker.ErrUnroutable)
	require.NoError(t, errs[2])
	require.NoError(t, errs[3])

	depth, err := c.QueueDepth(broker.WorkQueue("pub"))
	require.NoError(t, err)
	require.Equal(t, 2, depth)
}

// Messages already waiting in the queue are delivered highest priority first.
func TestPriorityOrdersWaitingMessages(t *testing.T) {
	c := newClient(t, "prio")
	errs := c.Publish(context.Background(),
		work("prio", "low", 0), work("prio", "normal", 1), work("prio", "high", 2),
		work("prio", "too-high", 9), // above x-max-priority: treated as 2
	)
	for _, err := range errs {
		require.NoError(t, err)
	}

	got := collect(t, c, broker.WorkQueue("prio"))
	var ds []amqp.Delivery
	for range 4 {
		ds = append(ds, next(t, got))
	}
	// Handlers run concurrently, so arrival on the channel is not delivery
	// order; the delivery tag is.
	sort.Slice(ds, func(i, j int) bool { return ds[i].DeliveryTag < ds[j].DeliveryTag })
	var order []string
	for _, d := range ds {
		order = append(order, d.MessageId)
	}
	require.Equal(t, []string{"high", "too-high", "normal", "low"}, order)
}

// A message in a retry queue comes back to its channel's work queue once
// the queue TTL expires, with its priority intact.
func TestRetryQueueDeadLettersBackToWorkQueue(t *testing.T) {
	const delay = 300 * time.Millisecond
	c := newClient(t, "retry", delay)
	got := collect(t, c, broker.WorkQueue("retry"))

	start := time.Now()
	errs := c.Publish(context.Background(), broker.Message{
		Exchange: broker.ExchangeRetry, RoutingKey: broker.RetryRoutingKey("retry", delay),
		MessageID: "m", Priority: 2, Body: []byte(`{}`),
	})
	require.NoError(t, errs[0])

	d := next(t, got)
	require.GreaterOrEqual(t, time.Since(start), delay)
	require.Equal(t, "m", d.MessageId)
	require.Equal(t, uint8(2), d.Priority)
	require.Equal(t, "retry", d.RoutingKey, "x-dead-letter-routing-key pins the channel")
}

// After the broker drops every connection the client reconnects, publishing
// works again and the consumer resubscribes without help.
func TestReconnectsAfterConnectionLoss(t *testing.T) {
	c := newClient(t, "reconn")
	got := collect(t, c, broker.WorkQueue("reconn"))
	ctx := context.Background()

	require.NoError(t, c.Publish(ctx, work("reconn", "before", 0))[0])
	require.Equal(t, "before", next(t, got).MessageId)

	require.NoError(t, mq.CloseAllConnections(ctx))
	require.Eventually(t, func() bool {
		return c.Publish(ctx, work("reconn", "after", 0))[0] == nil
	}, 20*time.Second, 100*time.Millisecond, "publish never succeeded after reconnect")

	require.Equal(t, "after", next(t, got).MessageId)
}
