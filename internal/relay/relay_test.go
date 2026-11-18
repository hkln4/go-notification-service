package relay

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
	"github.com/stretchr/testify/require"

	"github.com/hkln4/go-notification-service/internal/broker"
	"github.com/hkln4/go-notification-service/internal/domain"
	"github.com/hkln4/go-notification-service/internal/store/postgres"
)

// fakeStore is safe for concurrent use: Run calls it from its own goroutine
// while the test reads the results.
type fakeStore struct {
	mu      sync.Mutex
	rows    []postgres.OutboxRow
	claims  int
	deleted []int64
}

func (f *fakeStore) ClaimOutbox(_ context.Context, limit int, _ time.Duration) ([]postgres.OutboxRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims++
	n := min(limit, len(f.rows))
	out := f.rows[:n]
	f.rows = f.rows[n:]
	return out, nil
}

func (f *fakeStore) DeleteOutbox(_ context.Context, ids []int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, ids...)
	return nil
}

func (f *fakeStore) deletedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.deleted)
}

type fakePublisher struct {
	disconnected bool
	failIDs      map[string]bool
	published    []broker.Message
}

func (f *fakePublisher) Connected() bool { return !f.disconnected }

func (f *fakePublisher) Publish(_ context.Context, msgs ...broker.Message) []error {
	errs := make([]error, len(msgs))
	for i, m := range msgs {
		if f.failIDs[m.MessageID] {
			errs[i] = broker.ErrNacked
			continue
		}
		f.published = append(f.published, m)
	}
	return errs
}

func row(id int64, channel string, prio int16) postgres.OutboxRow {
	return postgres.OutboxRow{
		ID: id, NotificationID: uuid.New(), RoutingKey: channel, Priority: prio,
		Headers: map[string]string{"x-correlation-id": "corr-" + channel},
	}
}

func newRelay(store *fakeStore, pub *fakePublisher, batch int) *Relay {
	return New(store, pub, Config{BatchSize: batch, PollInterval: time.Millisecond, Lease: time.Second},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestTickPublishesAndDeletesConfirmedRows(t *testing.T) {
	ok1, failed, ok2 := row(1, "sms", 2), row(2, "email", 0), row(3, "push", 1)
	store := &fakeStore{rows: []postgres.OutboxRow{ok1, failed, ok2}}
	pub := &fakePublisher{failIDs: map[string]bool{failed.NotificationID.String(): true}}

	n, err := newRelay(store, pub, 10).Tick(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, []int64{1, 3}, store.deleted, "only confirmed rows are deleted")

	m := pub.published[0]
	require.Equal(t, broker.ExchangeNotifications, m.Exchange)
	require.Equal(t, "sms", m.RoutingKey)
	require.Equal(t, uint8(2), m.Priority)
	require.Equal(t, ok1.NotificationID.String(), m.MessageID)
	require.Equal(t, "corr-sms", m.CorrelationID)
	require.Equal(t, "corr-sms", m.Headers["x-correlation-id"])

	var body domain.DeliveryMessage
	require.NoError(t, json.Unmarshal(m.Body, &body))
	require.Equal(t, domain.DeliveryMessage{NotificationID: ok1.NotificationID, Channel: domain.ChannelSMS}, body)
}

func TestTickSkipsClaimWhileBrokerIsDown(t *testing.T) {
	store := &fakeStore{rows: []postgres.OutboxRow{row(1, "sms", 1)}}
	n, err := newRelay(store, &fakePublisher{disconnected: true}, 10).Tick(context.Background())
	require.NoError(t, err)
	require.Zero(t, n)
	require.Zero(t, store.claims, "rows must not be leased while they can't be published")
}

// A full batch means more rows are probably waiting: Run claims again right
// away instead of sleeping, so 25 rows with batch size 10 take 3 claims plus
// one empty claim before the first sleep.
func TestRunDrainsFullBatchesWithoutWaiting(t *testing.T) {
	store := &fakeStore{}
	for i := range 25 {
		store.rows = append(store.rows, row(int64(i), "sms", 1))
	}
	pub := &fakePublisher{}
	r := newRelay(store, pub, 10)
	r.cfg.PollInterval = time.Hour // a sleep would hang the test

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	require.Eventually(t, func() bool { return store.deletedCount() == 25 }, 5*time.Second, time.Millisecond)
	cancel()
	<-done
	require.Equal(t, 3, store.claims, "two full batches and one partial, then sleep")
}

var errBoom = errors.New("boom")

type failingStore struct{ fakeStore }

func (f *failingStore) ClaimOutbox(context.Context, int, time.Duration) ([]postgres.OutboxRow, error) {
	return nil, errBoom
}

func TestTickReturnsClaimError(t *testing.T) {
	_, err := New(&failingStore{}, &fakePublisher{}, Config{BatchSize: 1}, slog.New(slog.NewTextHandler(io.Discard, nil))).
		Tick(context.Background())
	require.ErrorIs(t, err, errBoom)
}
