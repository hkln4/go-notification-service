// Package worker delivers notifications: it consumes work messages, takes a
// rate limit slot, claims the notification, calls the provider and records
// the outcome.
//
// Delivery is at-least-once. Every path either acks a message after its
// outcome is durable (in PostgreSQL, or as a new message in a retry queue),
// or leaves it unacked so RabbitMQ redelivers it. Duplicates are expected
// and stopped by the claim.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/hkln4/go-notification-service/internal/broker"
	"github.com/hkln4/go-notification-service/internal/domain"
	"github.com/hkln4/go-notification-service/internal/observability"
	"github.com/hkln4/go-notification-service/internal/provider"
	"github.com/hkln4/go-notification-service/internal/store/postgres"
)

type Store interface {
	Claim(ctx context.Context, id, lease uuid.UUID, leaseFor time.Duration) (domain.Notification, bool, error)
	LeaseState(ctx context.Context, id uuid.UUID) (domain.Status, bool, error)
	CompleteAttempt(ctx context.Context, o postgres.Outcome) (bool, error)
}

type Publisher interface {
	Publish(ctx context.Context, msgs ...broker.Message) []error
}

type Sender interface {
	Send(ctx context.Context, id uuid.UUID, correlationID string, req provider.Request) (provider.Result, error)
}

type Limiter interface {
	Wait(ctx context.Context, key string) (time.Time, error)
}

type Config struct {
	// Lease must outlast one provider call plus the database writes, or a
	// healthy worker would lose its lease mid-call.
	Lease time.Duration
	// TokenWait caps how long a message waits for a rate limit slot.
	TokenWait   time.Duration
	RetryDelays []time.Duration
	MaxAttempts int
	Metrics     *observability.Metrics
}

type Worker struct {
	store   Store
	pub     Publisher
	sender  Sender
	limiter Limiter
	cfg     Config
	policy  Policy
	log     *slog.Logger
	newID   func() uuid.UUID
}

func New(store Store, pub Publisher, sender Sender, limiter Limiter, cfg Config, log *slog.Logger) *Worker {
	return &Worker{
		store: store, pub: pub, sender: sender, limiter: limiter, cfg: cfg,
		policy: Policy{Delays: cfg.RetryDelays, MaxAttempts: cfg.MaxAttempts},
		log:    log.With("component", "worker"),
		newID:  uuid.New,
	}
}

// Handle processes one delivery and always settles it (ack or nack). ctx
// bounds the waiting parts (rate limit slot, provider call); database and
// broker writes use their own short timeouts so an outcome is not lost
// mid-write.
func (w *Worker) Handle(ctx context.Context, d amqp.Delivery) {
	var msg domain.DeliveryMessage
	if err := json.Unmarshal(d.Body, &msg); err != nil || msg.NotificationID == uuid.Nil {
		w.poison(d, err)
		return
	}
	log := w.log.With("notification_id", msg.NotificationID, "channel", msg.Channel, "correlation_id", d.CorrelationId)

	// The slot is taken before the claim, so time spent waiting for it does
	// not eat into the lease and does not count as an attempt. If no slot is
	// free within TokenWait, or Redis is down (fail closed), the message
	// goes round a short retry queue instead of blocking the consumer.
	tokenCtx, cancel := context.WithTimeout(ctx, w.cfg.TokenWait)
	_, err := w.limiter.Wait(tokenCtx, string(msg.Channel))
	cancel()
	if err != nil {
		log.Warn("no rate limit slot; requeueing without using an attempt", "err", err)
		w.settle(d, log, w.republish(d, msg.Channel, w.cfg.RetryDelays[0]))
		return
	}

	n, lease, ok := w.claim(d, msg, log)
	if !ok {
		return // claim settled the delivery
	}
	log = log.With("attempt", n.Attempts)

	res, sendErr := w.sender.Send(ctx, n.ID, n.CorrelationID, provider.Request{
		To: n.Recipient, Channel: string(n.Channel), Content: n.Content,
	})
	if err := w.finish(d, n, lease, res, sendErr, log); err != nil {
		// The outcome is not durable. Leave the message for redelivery: the
		// lease expires and the next claim tries again.
		log.Error("could not record outcome; returning message to queue", "err", err)
		_ = d.Nack(false, true)
		return
	}
	_ = d.Ack(false)
}

// claim takes the lease. When it can't, it settles the delivery itself and
// returns ok=false.
func (w *Worker) claim(d amqp.Delivery, msg domain.DeliveryMessage, log *slog.Logger) (domain.Notification, uuid.UUID, bool) {
	ctx, cancel := dbContext()
	defer cancel()
	lease := w.newID()

	n, ok, err := w.store.Claim(ctx, msg.NotificationID, lease, w.cfg.Lease)
	if err != nil {
		log.Error("claim failed; requeueing", "err", err)
		w.settle(d, log, w.republish(d, msg.Channel, w.cfg.RetryDelays[0]))
		return domain.Notification{}, uuid.Nil, false
	}
	if ok {
		w.emit(n, domain.StatusProcessing)
		return n, lease, true
	}

	st, leased, err := w.store.LeaseState(ctx, msg.NotificationID)
	switch {
	case errors.Is(err, domain.ErrNotFound):
		log.Warn("message for unknown notification; dropping")
		_ = d.Ack(false)
	case err != nil:
		log.Error("lease state failed; requeueing", "err", err)
		w.settle(d, log, w.republish(d, msg.Channel, w.cfg.RetryDelays[0]))
	case st.IsTerminal():
		// Already sent, failed or cancelled: a duplicate, or the leftover
		// message of a cancelled notification.
		log.Info("notification already final; dropping message", "status", st)
		_ = d.Ack(false)
	case st == domain.StatusProcessing && leased:
		// Another worker is on it right now. Never just drop this copy: if
		// that worker dies, this message is what gets the notification
		// delivered once the lease expires. Park it for about a lease.
		log.Info("notification leased by another worker; parking message")
		w.settle(d, log, w.republish(d, msg.Channel, w.policy.atLeast(w.cfg.Lease)))
	default:
		// The state changed under us (e.g. a lease expired just now).
		w.settle(d, log, w.republish(d, msg.Channel, w.cfg.RetryDelays[0]))
	}
	return domain.Notification{}, uuid.Nil, false
}

// finish acts on the retry decision and records the attempt. The order is
// always: publish the message's next step (retry queue or DLQ, confirmed)
// → database update → ack (by the caller). If we crash after the publish
// but before the update, the copy in the retry queue finds a processing
// row whose lease expires and is claimed again: a repeat, never a loss.
func (w *Worker) finish(d amqp.Delivery, n domain.Notification, lease uuid.UUID, res provider.Result, sendErr error, log *slog.Logger) error {
	dec := w.policy.Decide(res.StatusCode, sendErr, n.Attempts, res.RetryAfter)
	o := postgres.Outcome{
		ID:      n.ID,
		Lease:   lease,
		Attempt: attemptRecord(n.Attempts, res, sendErr),
	}

	switch dec.Action {
	case ActionSent:
		o.Status, o.ProviderMessageID = domain.StatusSent, res.MessageID
		if res.BodyErr != nil {
			log.Warn("provider accepted the message but its response was malformed", "err", res.BodyErr)
		}
	case ActionRetry:
		o.Status, o.LastError = domain.StatusRetrying, describe(res, sendErr)
		if err := w.republish(d, n.Channel, dec.Delay); err != nil {
			return fmt.Errorf("publish retry: %w", err)
		}
	case ActionDeadLetter, ActionPermanentFail:
		o.Status, o.LastError = domain.StatusFailed, describe(res, sendErr)
		if err := w.deadLetter(d, n, dec.Action, o.LastError); err != nil {
			return fmt.Errorf("publish to dlq: %w", err)
		}
	}

	ctx, cancel := dbContext()
	defer cancel()
	applied, err := w.store.CompleteAttempt(ctx, o)
	if err != nil {
		return err
	}
	// Counted once the attempt is recorded, so a redelivery after a failed
	// write doesn't count the same call twice.
	w.cfg.Metrics.Delivered(string(n.Channel), dec.Action.String(), res.Latency)
	if applied && dec.Action == ActionSent {
		start := n.CreatedAt
		if n.ScheduledAt != nil {
			start = *n.ScheduledAt
		}
		w.cfg.Metrics.EndToEnd(string(n.Channel), time.Since(start))
	}
	if !applied {
		// Our lease expired and another worker took over; its result wins.
		log.Warn("lease lost before recording outcome; result discarded", "status", o.Status)
		return nil
	}
	// recipient is masked by the logger (observability.MaskRecipient).
	log.Info("delivery attempt finished", "decision", dec.Action.String(), "status", o.Status,
		"recipient", n.Recipient, "http_status", res.StatusCode,
		"latency_ms", res.Latency.Milliseconds(), "retry_in", dec.Delay.String())
	w.emit(n, o.Status)
	return nil
}

// deadLetter parks a notification that won't be retried in the DLQ, with
// the reason in headers, so it can be inspected and requeued later.
func (w *Worker) deadLetter(d amqp.Delivery, n domain.Notification, why Action, lastErr string) error {
	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers["x-failure-reason"] = why.String()
	headers["x-attempts"] = int32(n.Attempts) //nolint:gosec // bounded by MaxAttempts
	headers["x-last-error"] = lastErr

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return w.pub.Publish(ctx, broker.Message{
		RoutingKey: broker.QueueDLQ, Body: d.Body, MessageID: d.MessageId,
		CorrelationID: d.CorrelationId, Priority: d.Priority, Headers: headers,
	})[0]
}

func attemptRecord(attemptNo int, res provider.Result, sendErr error) domain.Attempt {
	a := domain.Attempt{AttemptNo: attemptNo, LatencyMS: int(res.Latency.Milliseconds())}
	if res.StatusCode != 0 {
		code := res.StatusCode
		a.HTTPStatus = &code
	}
	if sendErr != nil {
		msg := sendErr.Error()
		a.Error = &msg
	}
	return a
}

func describe(res provider.Result, sendErr error) string {
	if sendErr != nil {
		return sendErr.Error()
	}
	return fmt.Sprintf("provider returned %d", res.StatusCode)
}

// dbContext bounds a database call. It is deliberately not derived from the
// consumer's context: on shutdown we still want to record an outcome for a
// call that already reached the provider.
func dbContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// republish copies the delivery to the retry queue for delay. It returns
// only once the broker has confirmed the copy.
func (w *Worker) republish(d amqp.Delivery, channel domain.Channel, delay time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return w.pub.Publish(ctx, broker.Message{
		Exchange:      broker.ExchangeRetry,
		RoutingKey:    broker.RetryRoutingKey(string(channel), delay),
		Body:          d.Body,
		Priority:      d.Priority,
		MessageID:     d.MessageId,
		CorrelationID: d.CorrelationId,
		Headers:       d.Headers,
	})[0]
}

// settle acks once the message's next step is safely in the broker, and
// otherwise nacks it back to its queue so nothing is lost.
func (w *Worker) settle(d amqp.Delivery, log *slog.Logger, publishErr error) {
	if publishErr != nil {
		log.Error("republish failed; returning message to queue", "err", publishErr)
		_ = d.Nack(false, true)
		return
	}
	_ = d.Ack(false)
}

// poison moves a message the worker cannot even parse to the DLQ for a
// human to look at; retrying it would never help.
func (w *Worker) poison(d amqp.Delivery, err error) {
	w.log.Error("undecodable message; moving to DLQ", "message_id", d.MessageId, "err", err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	perr := w.pub.Publish(ctx, broker.Message{
		RoutingKey: broker.QueueDLQ, Body: d.Body, MessageID: d.MessageId,
		Headers: amqp.Table{"x-failure-reason": "undecodable"},
	})[0]
	w.settle(d, w.log, perr)
}

// emit publishes a status change for live listeners (WebSocket). It is best
// effort: the database already has the truth, and a missed event only
// delays a UI update. Fanout exchanges may have no queue bound, so the
// message is not mandatory.
func (w *Worker) emit(n domain.Notification, st domain.Status) {
	body, _ := json.Marshal(domain.StatusEvent{
		NotificationID: n.ID, BatchID: n.BatchID, Status: st, Attempts: n.Attempts, At: time.Now().UTC(),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := w.pub.Publish(ctx, broker.Message{
		Exchange: broker.ExchangeEvents, Body: body, MessageID: n.ID.String(),
		CorrelationID: n.CorrelationID, NotMandatory: true,
	})[0]; err != nil {
		w.log.Warn("status event not published", "notification_id", n.ID, "status", st, "err", err)
	}
}
