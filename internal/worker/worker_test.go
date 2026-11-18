package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/hkln4/go-notification-service/internal/broker"
	"github.com/hkln4/go-notification-service/internal/domain"
	"github.com/hkln4/go-notification-service/internal/provider"
	"github.com/hkln4/go-notification-service/internal/store/postgres"
)

var delays = []time.Duration{10 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond}

// --- fakes ---

type fakeStore struct {
	mu        sync.Mutex
	n         domain.Notification
	claimable bool
	state     domain.Status
	leased    bool
	notFound  bool
	claimErr  error
	finishErr error
	notApply  bool
	outcomes  []postgres.Outcome
}

func (f *fakeStore) Claim(_ context.Context, id, _ uuid.UUID, _ time.Duration) (domain.Notification, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimErr != nil {
		return domain.Notification{}, false, f.claimErr
	}
	if !f.claimable {
		return domain.Notification{}, false, nil
	}
	f.n.ID = id
	f.n.Attempts++
	return f.n, true, nil
}

func (f *fakeStore) LeaseState(context.Context, uuid.UUID) (domain.Status, bool, error) {
	if f.notFound {
		return "", false, domain.ErrNotFound
	}
	return f.state, f.leased, nil
}

func (f *fakeStore) CompleteAttempt(_ context.Context, o postgres.Outcome) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.finishErr != nil {
		return false, f.finishErr
	}
	f.outcomes = append(f.outcomes, o)
	return !f.notApply, nil
}

type fakePub struct {
	mu   sync.Mutex
	err  error
	msgs []broker.Message
}

func (f *fakePub) Publish(_ context.Context, msgs ...broker.Message) []error {
	f.mu.Lock()
	defer f.mu.Unlock()
	errs := make([]error, len(msgs))
	for i, m := range msgs {
		if f.err != nil && m.Exchange != broker.ExchangeEvents {
			errs[i] = f.err
			continue
		}
		f.msgs = append(f.msgs, m)
	}
	return errs
}

// sent returns published messages except status events.
func (f *fakePub) sent() []broker.Message {
	var out []broker.Message
	for _, m := range f.msgs {
		if m.Exchange != broker.ExchangeEvents {
			out = append(out, m)
		}
	}
	return out
}

func (f *fakePub) events() []domain.Status {
	var out []domain.Status
	for _, m := range f.msgs {
		if m.Exchange == broker.ExchangeEvents {
			var e domain.StatusEvent
			_ = json.Unmarshal(m.Body, &e)
			out = append(out, e.Status)
		}
	}
	return out
}

type fakeSender struct {
	calls []provider.Request
	res   provider.Result
	err   error
}

func (f *fakeSender) Send(_ context.Context, _ uuid.UUID, _ string, req provider.Request) (provider.Result, error) {
	f.calls = append(f.calls, req)
	return f.res, f.err
}

type fakeLimiter struct{ err error }

func (f fakeLimiter) Wait(context.Context, string) (time.Time, error) { return time.Now(), f.err }

type fakeAck struct{ acked, nacked, requeued bool }

func (a *fakeAck) Ack(uint64, bool) error { a.acked = true; return nil }
func (a *fakeAck) Nack(_ uint64, _ bool, requeue bool) error {
	a.nacked, a.requeued = true, requeue
	return nil
}
func (a *fakeAck) Reject(_ uint64, requeue bool) error { return a.Nack(0, false, requeue) }

// --- harness ---

type harness struct {
	store  *fakeStore
	pub    *fakePub
	sender *fakeSender
	lim    fakeLimiter
}

func newHarness() *harness {
	return &harness{
		store: &fakeStore{claimable: true, n: domain.Notification{
			Channel: domain.ChannelSMS, Recipient: "+905551234567", Content: "hi", CorrelationID: "c1",
		}},
		pub:    &fakePub{},
		sender: &fakeSender{res: provider.Result{StatusCode: 202, MessageID: "pm-1"}},
	}
}

func (h *harness) run(t *testing.T, body []byte) *fakeAck {
	t.Helper()
	w := New(h.store, h.pub, h.sender, h.lim,
		Config{Lease: 60 * time.Millisecond, TokenWait: time.Second, RetryDelays: delays, MaxAttempts: 5},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	ack := &fakeAck{}
	w.Handle(context.Background(), amqp.Delivery{
		Acknowledger: ack, DeliveryTag: 1, Body: body, MessageId: "m1", Priority: 2, CorrelationId: "c1",
	})
	require.True(t, ack.acked != ack.nacked, "delivery must be settled exactly once")
	return ack
}

func body(t *testing.T) []byte {
	b, err := json.Marshal(domain.DeliveryMessage{NotificationID: uuid.New(), Channel: domain.ChannelSMS})
	require.NoError(t, err)
	return b
}

// --- tests ---

func TestHandleDelivers(t *testing.T) {
	h := newHarness()
	ack := h.run(t, body(t))

	require.True(t, ack.acked)
	require.Equal(t, []provider.Request{{To: "+905551234567", Channel: "sms", Content: "hi"}}, h.sender.calls)
	require.Len(t, h.store.outcomes, 1)
	o := h.store.outcomes[0]
	require.Equal(t, domain.StatusSent, o.Status)
	require.Equal(t, "pm-1", o.ProviderMessageID)
	require.Equal(t, 1, o.Attempt.AttemptNo)
	require.Equal(t, 202, *o.Attempt.HTTPStatus)
	require.Equal(t, []domain.Status{domain.StatusProcessing, domain.StatusSent}, h.pub.events())
}

func TestHandleWhenNotClaimable(t *testing.T) {
	tests := []struct {
		name      string
		state     domain.Status
		leased    bool
		notFound  bool
		wantDelay time.Duration // 0: acked without republish
	}{
		{"already sent: duplicate dropped", domain.StatusSent, false, false, 0},
		{"cancelled: leftover dropped", domain.StatusCancelled, false, false, 0},
		{"failed: dropped", domain.StatusFailed, false, false, 0},
		{"unknown notification", "", false, true, 0},
		// Lease is 60ms: the shortest queue that waits at least that long.
		{"leased by another worker: parked", domain.StatusProcessing, true, false, 100 * time.Millisecond},
		{"lease just expired: retried soon", domain.StatusProcessing, false, false, delays[0]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness()
			h.store.claimable, h.store.state, h.store.leased, h.store.notFound = false, tt.state, tt.leased, tt.notFound
			ack := h.run(t, body(t))

			require.True(t, ack.acked)
			require.Empty(t, h.sender.calls, "provider must not be called without a lease")
			sent := h.pub.sent()
			if tt.wantDelay == 0 {
				require.Empty(t, sent)
				return
			}
			require.Len(t, sent, 1)
			require.Equal(t, broker.ExchangeRetry, sent[0].Exchange)
			require.Equal(t, broker.RetryRoutingKey("sms", tt.wantDelay), sent[0].RoutingKey)
			require.Equal(t, uint8(2), sent[0].Priority, "priority survives the retry queue")
		})
	}
}

func TestHandleNoRateLimitSlot(t *testing.T) {
	h := newHarness()
	h.lim.err = errors.New("redis down")
	ack := h.run(t, body(t))

	require.True(t, ack.acked)
	require.Empty(t, h.sender.calls)
	require.Empty(t, h.store.outcomes, "no attempt is used")
	require.Equal(t, broker.RetryRoutingKey("sms", delays[0]), h.pub.sent()[0].RoutingKey)
}

func TestHandleInfrastructureFailures(t *testing.T) {
	t.Run("claim fails: requeued via retry queue", func(t *testing.T) {
		h := newHarness()
		h.store.claimErr = errors.New("db down")
		ack := h.run(t, body(t))
		require.True(t, ack.acked)
		require.Len(t, h.pub.sent(), 1)
		require.Empty(t, h.sender.calls)
	})

	t.Run("republish fails: nacked back to queue", func(t *testing.T) {
		h := newHarness()
		h.store.claimErr = errors.New("db down")
		h.pub.err = errors.New("broker down")
		ack := h.run(t, body(t))
		require.True(t, ack.nacked)
		require.True(t, ack.requeued)
	})

	t.Run("outcome not recorded: nacked for redelivery", func(t *testing.T) {
		h := newHarness()
		h.store.finishErr = errors.New("db down")
		ack := h.run(t, body(t))
		require.True(t, ack.nacked)
		require.True(t, ack.requeued)
	})
}

func TestHandleLostLease(t *testing.T) {
	h := newHarness()
	h.store.notApply = true
	ack := h.run(t, body(t))
	require.True(t, ack.acked, "another worker owns the outcome now")
	require.Equal(t, []domain.Status{domain.StatusProcessing}, h.pub.events(), "no event for a discarded result")
}

func TestHandleUndecodableGoesToDLQ(t *testing.T) {
	for _, b := range [][]byte{[]byte("not json"), []byte(`{"channel":"sms"}`)} {
		h := newHarness()
		ack := h.run(t, b)
		require.True(t, ack.acked)
		sent := h.pub.sent()
		require.Len(t, sent, 1)
		require.Equal(t, broker.QueueDLQ, sent[0].RoutingKey)
		require.Empty(t, h.sender.calls)
	}
}

func TestHandleProviderAcceptedWithBadBody(t *testing.T) {
	h := newHarness()
	h.sender.res = provider.Result{StatusCode: 202, BodyErr: errors.New("bad json")}
	h.run(t, body(t))
	require.Equal(t, domain.StatusSent, h.store.outcomes[0].Status)
}

func TestHandleFailureOutcomes(t *testing.T) {
	tests := []struct {
		name        string
		res         provider.Result
		err         error
		attempt     int // attempt number of this delivery
		wantStatus  domain.Status
		wantRouting string
		wantReason  string
	}{
		{"500 retries with the ladder", provider.Result{StatusCode: 500}, nil, 2, domain.StatusRetrying, broker.RetryRoutingKey("sms", delays[1]), ""},
		{"timeout retries", provider.Result{}, context.DeadlineExceeded, 1, domain.StatusRetrying, broker.RetryRoutingKey("sms", delays[0]), ""},
		{"429 honours Retry-After", provider.Result{StatusCode: 429, RetryAfter: 80 * time.Millisecond}, nil, 1, domain.StatusRetrying, broker.RetryRoutingKey("sms", delays[2]), ""},
		{"5th failure goes to DLQ", provider.Result{StatusCode: 503}, nil, 5, domain.StatusFailed, broker.QueueDLQ, "dead_letter"},
		{"400 goes straight to DLQ", provider.Result{StatusCode: 400}, nil, 1, domain.StatusFailed, broker.QueueDLQ, "permanent_fail"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness()
			h.store.n.Attempts = tt.attempt - 1 // Claim increments
			h.sender.res, h.sender.err = tt.res, tt.err
			ack := h.run(t, body(t))

			require.True(t, ack.acked)
			require.Equal(t, tt.wantStatus, h.store.outcomes[0].Status)
			require.NotEmpty(t, h.store.outcomes[0].LastError)
			sent := h.pub.sent()
			require.Len(t, sent, 1)
			require.Equal(t, tt.wantRouting, sent[0].RoutingKey)
			if tt.wantReason != "" {
				require.Equal(t, tt.wantReason, sent[0].Headers["x-failure-reason"])
				require.EqualValues(t, tt.attempt, sent[0].Headers["x-attempts"])
			}
		})
	}
}

// Publish → database → ack: if the retry message can't be published, the
// database is not touched and the delivery goes back to the queue.
func TestHandleRetryPublishFailureLeavesStateAlone(t *testing.T) {
	h := newHarness()
	h.sender.res = provider.Result{StatusCode: 500}
	h.pub.err = errors.New("broker down")
	ack := h.run(t, body(t))

	require.True(t, ack.nacked)
	require.True(t, ack.requeued)
	require.Empty(t, h.store.outcomes, "no outcome recorded before the retry message is safe")
}
