package broker

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	// ErrUnroutable means a mandatory message matched no queue and was
	// returned by the broker.
	ErrUnroutable = errors.New("broker: message unroutable")
	// ErrNacked means the broker refused to take responsibility for a message.
	ErrNacked = errors.New("broker: message nacked")
)

// maxInFlight bounds how many messages one Publish call sends before waiting
// for confirms. It must stay below returnBuffer, see publisher.
const (
	maxInFlight  = 256
	returnBuffer = 1024
)

type Message struct {
	Exchange   string
	RoutingKey string
	Body       []byte
	Priority   uint8
	// MessageID is the notification ID; it ties returns back to messages.
	MessageID     string
	CorrelationID string
	Headers       amqp.Table
	// NotMandatory is for fanout events, which may legitimately have no
	// queue bound. Everything else is published mandatory.
	NotMandatory bool
}

type publisher struct {
	ch      *amqp.Channel
	returns chan amqp.Return
}

func newPublisher(ch *amqp.Channel) (*publisher, error) {
	if err := ch.Confirm(false); err != nil {
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}
	// RabbitMQ sends basic.return before the basic.ack of the same message,
	// and amqp091-go hands both to us from one goroutine in that order. So
	// once a confirm has arrived, any return for that message is already in
	// this buffer. The buffer must be larger than what one Publish call
	// keeps in flight, or the library would block delivering a return while
	// we block waiting for the ack that comes after it.
	returns := ch.NotifyReturn(make(chan amqp.Return, returnBuffer))
	return &publisher{ch: ch, returns: returns}, nil
}

// Publish sends msgs as persistent messages and waits for the broker to
// confirm each one. The result has one entry per message: nil means the
// broker stored it in at least one queue. Confirms are awaited in bulk
// (PublishWithDeferredConfirm) rather than one round trip per message.
func (c *Client) Publish(ctx context.Context, msgs ...Message) []error {
	errs := make([]error, len(msgs))
	c.pubMu.Lock()
	defer c.pubMu.Unlock()

	_, pub := c.current()
	if pub == nil {
		for i := range errs {
			errs[i] = ErrNotConnected
		}
		return errs
	}

	for start := 0; start < len(msgs); start += maxInFlight {
		end := min(start+maxInFlight, len(msgs))
		pub.publishChunk(ctx, msgs[start:end], errs[start:end])
	}
	return errs
}

func (p *publisher) publishChunk(ctx context.Context, msgs []Message, errs []error) {
	// Drop stale returns left by a previous call that gave up early.
	p.drainReturns()

	confirms := make([]*amqp.DeferredConfirmation, len(msgs))
	for i, m := range msgs {
		dc, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, m.Exchange, m.RoutingKey, !m.NotMandatory, false,
			amqp.Publishing{
				DeliveryMode:  amqp.Persistent,
				ContentType:   "application/json",
				Priority:      m.Priority,
				MessageId:     m.MessageID,
				CorrelationId: m.CorrelationID,
				Headers:       m.Headers,
				Timestamp:     time.Now(),
				Body:          m.Body,
			})
		if err != nil {
			errs[i] = fmt.Errorf("publish: %w", err)
			continue
		}
		confirms[i] = dc
	}

	for i, dc := range confirms {
		if dc == nil {
			continue
		}
		acked, err := dc.WaitContext(ctx)
		switch {
		case err != nil:
			errs[i] = fmt.Errorf("wait for confirm: %w", err)
		case !acked:
			errs[i] = ErrNacked
		}
	}

	returned := p.drainReturns()
	for i, m := range msgs {
		if errs[i] == nil && returned[m.MessageID] {
			errs[i] = fmt.Errorf("%w: exchange %q key %q", ErrUnroutable, m.Exchange, m.RoutingKey)
		}
	}
}

func (p *publisher) drainReturns() map[string]bool {
	returned := map[string]bool{}
	for {
		select {
		case r := <-p.returns:
			returned[r.MessageId] = true
		default:
			return returned
		}
	}
}
