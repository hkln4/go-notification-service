// Package broker wraps RabbitMQ: topology, a self-healing connection,
// confirmed publishing and consuming.
package broker

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// ExchangeNotifications routes work by channel name (sms, email, push).
	ExchangeNotifications = "notifications"
	// ExchangeRetry routes to the delay queues by "{channel}.{delay}".
	ExchangeRetry = "retry"
	// ExchangeEvents fans status changes out to API instances (WebSocket).
	ExchangeEvents = "notification.events"
	// QueueDLQ holds messages that ran out of attempts. It is published to
	// through the default exchange, whose routing key is the queue name.
	QueueDLQ = "q.dlq"
	// MaxPriority matches domain.Priority (0 low, 1 normal, 2 high). A single
	// digit is what the RabbitMQ docs recommend for classic queues: each
	// level is an internal sub-queue.
	MaxPriority = 2
)

// DefaultRetryDelays is the retry ladder: attempt 1 failed → 5s, ... → 10m.
var DefaultRetryDelays = []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute}

type Topology struct {
	Channels    []string
	RetryDelays []time.Duration
}

func WorkQueue(channel string) string { return "q." + channel }

func RetryRoutingKey(channel string, delay time.Duration) string {
	return channel + "." + DelayName(delay)
}

func RetryQueue(channel string, delay time.Duration) string {
	return "q.retry." + RetryRoutingKey(channel, delay)
}

// DelayName renders a delay the way it appears in queue names: 5s, 2m, 250ms.
func DelayName(d time.Duration) string {
	switch {
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", d/time.Minute)
	case d%time.Second == 0:
		return fmt.Sprintf("%ds", d/time.Second)
	default:
		return fmt.Sprintf("%dms", d/time.Millisecond)
	}
}

// Declare creates every exchange, queue and binding. Declarations are
// idempotent, so it runs on every (re)connect. Declaring an existing queue
// with different arguments fails with PRECONDITION_FAILED, which is what we
// want: a silent mismatch would be worse.
func (t Topology) Declare(ch *amqp.Channel) error {
	for _, ex := range []struct{ name, kind string }{
		{ExchangeNotifications, amqp.ExchangeDirect},
		{ExchangeRetry, amqp.ExchangeDirect},
		{ExchangeEvents, amqp.ExchangeFanout},
	} {
		if err := ch.ExchangeDeclare(ex.name, ex.kind, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare exchange %s: %w", ex.name, err)
		}
	}

	for _, c := range t.Channels {
		if err := declareBound(ch, WorkQueue(c), ExchangeNotifications, c, amqp.Table{
			"x-max-priority": MaxPriority,
		}); err != nil {
			return err
		}

		// One queue per (channel, delay) with a fixed TTL. Classic queues only
		// expire messages at the head, so mixing TTLs in one queue would make
		// a 5s message wait behind a 10m one. With a fixed TTL per queue, the
		// head always expires first.
		//
		// On expiry the message is dead-lettered to the notifications
		// exchange. Without x-dead-letter-routing-key it would keep its
		// original key ("sms.5s"), which nothing there is bound to; the
		// argument pins it back to the channel's work queue.
		for _, d := range t.RetryDelays {
			if err := declareBound(ch, RetryQueue(c, d), ExchangeRetry, RetryRoutingKey(c, d), amqp.Table{
				"x-message-ttl":             d.Milliseconds(),
				"x-dead-letter-exchange":    ExchangeNotifications,
				"x-dead-letter-routing-key": c,
			}); err != nil {
				return err
			}
		}
	}

	if _, err := ch.QueueDeclare(QueueDLQ, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare queue %s: %w", QueueDLQ, err)
	}
	return nil
}

func declareBound(ch *amqp.Channel, queue, exchange, key string, args amqp.Table) error {
	if _, err := ch.QueueDeclare(queue, true, false, false, false, args); err != nil {
		return fmt.Errorf("declare queue %s: %w", queue, err)
	}
	if err := ch.QueueBind(queue, key, exchange, false, nil); err != nil {
		return fmt.Errorf("bind %s to %s/%s: %w", queue, exchange, key, err)
	}
	return nil
}
