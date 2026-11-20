//go:build integration

package worker_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/hkln4/go-notification-service/internal/broker"
	"github.com/hkln4/go-notification-service/internal/domain"
	"github.com/hkln4/go-notification-service/internal/provider"
	"github.com/hkln4/go-notification-service/internal/ratelimit"
	"github.com/hkln4/go-notification-service/internal/relay"
	"github.com/hkln4/go-notification-service/internal/service"
	"github.com/hkln4/go-notification-service/internal/store/postgres"
	"github.com/hkln4/go-notification-service/internal/testutil/mqtest"
	"github.com/hkln4/go-notification-service/internal/testutil/pgtest"
	"github.com/hkln4/go-notification-service/internal/worker"
)

var (
	db *pgtest.DB
	mq *mqtest.MQ
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	if db, err = pgtest.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if mq, err = mqtest.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		db.Close(ctx)
		os.Exit(1)
	}
	code := m.Run()
	mq.Close(ctx)
	db.Close(ctx)
	os.Exit(code)
}

// Every duration is scaled down from production (5s,30s,2m,10m and a 30s
// lease) so the tests run in seconds.
var (
	retryDelays = []time.Duration{100 * time.Millisecond, 300 * time.Millisecond, time.Second, 2 * time.Second}
	lease       = time.Second
	quiet       = slog.New(slog.NewTextHandler(io.Discard, nil))
)

// env is the delivery pipeline minus the API: store, relay, broker, worker
// and a provider that counts requests per notification.
type env struct {
	store   *postgres.Store
	svc     *service.Service
	mq      *broker.Client
	relay   *relay.Relay
	worker  *worker.Worker
	latency time.Duration

	// posts counts provider requests per notification. The provider handler
	// runs on the server's goroutines, hence the mutex.
	mu    sync.Mutex
	posts map[string]*atomic.Int32
	// status, if set, is what the provider answers instead of 202.
	status int
}

func (e *env) postCount(id uuid.UUID) int32 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.posts[id.String()].Load()
}

func newEnv(t *testing.T, providerLatency time.Duration) *env {
	t.Helper()
	db.Reset(t)
	e := &env{store: postgres.New(db.Pool), posts: map[string]*atomic.Int32{}, latency: providerLatency}
	e.svc = service.New(e.store)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.mu.Lock()
		if c, ok := e.posts[r.Header.Get("Idempotency-Key")]; ok {
			c.Add(1)
		}
		status := e.status
		e.mu.Unlock()
		time.Sleep(e.latency)
		if status != 0 {
			w.WriteHeader(status)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"pm","status":"accepted"}`))
	}))
	t.Cleanup(srv.Close)

	e.mq = broker.New(mq.URL, broker.NewTopology(retryDelays), quiet)
	t.Cleanup(e.mq.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, e.mq.WaitReady(ctx))

	var queues []string
	for _, ch := range domain.Channels {
		queues = append(queues, broker.WorkQueue(string(ch)))
		for _, d := range retryDelays {
			queues = append(queues, broker.RetryQueue(string(ch), d))
		}
	}
	require.NoError(t, mq.Purge(append(queues, broker.QueueDLQ)...))

	e.relay = relay.New(e.store, e.mq, relay.Config{BatchSize: 100, PollInterval: 20 * time.Millisecond, Lease: time.Minute}, quiet)
	e.worker = worker.New(e.store, e.mq, provider.NewClient(srv.URL, 5*time.Second), ratelimit.NewMemory(1000),
		worker.Config{Lease: lease, TokenWait: time.Second, RetryDelays: retryDelays, MaxAttempts: 5}, quiet)
	return e
}

// start runs the relay and one worker consuming sms until the test ends.
func (e *env) start(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{}, 2)
	go func() { e.relay.Run(ctx); done <- struct{}{} }()
	go func() {
		e.mq.Consume(ctx, broker.WorkQueue("sms"), 10, func(d broker.Delivery) { e.worker.Handle(ctx, d) })
		done <- struct{}{}
	}()
	t.Cleanup(func() { cancel(); <-done; <-done })
}

func (e *env) create(t *testing.T) uuid.UUID {
	t.Helper()
	res, err := e.svc.Create(context.Background(), service.CreateRequest{
		Input: domain.NotificationInput{Channel: "sms", Recipient: "+905551234567", Content: "hi"},
	})
	require.NoError(t, err)
	e.mu.Lock()
	e.posts[res.Notification.ID.String()] = &atomic.Int32{}
	e.mu.Unlock()
	return res.Notification.ID
}

func (e *env) waitStatus(t *testing.T, id uuid.UUID, want domain.Status) domain.Notification {
	t.Helper()
	var n domain.Notification
	require.Eventually(t, func() bool {
		var err error
		n, err = e.store.GetNotification(context.Background(), id)
		return err == nil && n.Status == want
	}, 15*time.Second, 20*time.Millisecond, "notification never reached %s (last: %s)", want, n.Status)
	return n
}

// The same work message arrives twice while the first copy is still being
// delivered (a relay that republished after a crash, a redelivery after a
// lost ack). The claim lets only one copy through; the other is parked,
// comes back, finds the notification sent and is dropped. The provider sees
// one POST.
func TestDuplicateDeliveryReachesProviderOnce(t *testing.T) {
	e := newEnv(t, 300*time.Millisecond) // slow provider: the copies overlap
	id := e.create(t)

	// Publish the outbox row, then a second copy of the same message.
	_, err := e.relay.Tick(context.Background())
	require.NoError(t, err)
	dup := broker.Message{
		Exchange: broker.ExchangeNotifications, RoutingKey: "sms", MessageID: id.String(),
		Body: []byte(fmt.Sprintf(`{"notificationId":%q,"channel":"sms"}`, id)),
	}
	require.NoError(t, e.mq.Publish(context.Background(), dup)[0])

	e.start(t)
	n := e.waitStatus(t, id, domain.StatusSent)

	// Give the parked copy time to come back and be dropped.
	time.Sleep(lease + 500*time.Millisecond)
	require.EqualValues(t, 1, e.postCount(id), "provider must see exactly one POST")
	require.Equal(t, 1, n.Attempts)
	depth, err := e.mq.QueueDepth(broker.WorkQueue("sms"))
	require.NoError(t, err)
	require.Zero(t, depth, "the duplicate was settled, not left behind")
}

// A worker claims a notification and dies without reporting. Its message
// copy is redelivered; the next worker sees a live lease and parks the
// message instead of dropping it. Once the dead worker's lease expires the
// notification is claimed again and delivered.
func TestCrashedLeaseOwnerDoesNotLoseNotification(t *testing.T) {
	e := newEnv(t, 0)
	id := e.create(t)

	// The "crashed" worker: claims, never completes.
	_, ok, err := e.store.Claim(context.Background(), id, uuid.New(), lease)
	require.NoError(t, err)
	require.True(t, ok)

	e.start(t)
	n := e.waitStatus(t, id, domain.StatusSent)
	require.Equal(t, 2, n.Attempts, "the crashed attempt counts")
	require.EqualValues(t, 1, e.postCount(id))
}

// Cancelled before the worker gets to it: the queued message is dropped and
// the provider is never called.
func TestCancelledNotificationIsNotSent(t *testing.T) {
	e := newEnv(t, 0)
	id := e.create(t)
	_, err := e.svc.Cancel(context.Background(), id)
	require.NoError(t, err)

	e.start(t)
	require.Eventually(t, func() bool {
		depth, err := e.mq.QueueDepth(broker.WorkQueue("sms"))
		n, _ := e.store.GetNotification(context.Background(), id)
		return err == nil && depth == 0 && n.Status == domain.StatusCancelled
	}, 10*time.Second, 50*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	require.Zero(t, e.postCount(id))
}

// Happy path through every component except HTTP: create → outbox → relay →
// queue → worker → provider → sent, with the attempt recorded.
func TestHappyPath(t *testing.T) {
	e := newEnv(t, 0)
	e.start(t)
	id := e.create(t)

	n := e.waitStatus(t, id, domain.StatusSent)
	require.Equal(t, "pm", *n.ProviderMessageID)
	attempts, err := e.store.ListAttempts(context.Background(), id)
	require.NoError(t, err)
	require.Len(t, attempts, 1)
	require.Equal(t, 202, *attempts[0].HTTPStatus)
}

// The provider keeps failing with 500. The notification walks the whole
// retry ladder (scaled down here) and after the 5th attempt lands in the
// DLQ, marked failed, with every attempt recorded.
func TestProvider500EndsInDLQAfterFiveAttempts(t *testing.T) {
	e := newEnv(t, 0)
	e.status = http.StatusInternalServerError
	e.start(t)
	id := e.create(t)

	n := e.waitStatus(t, id, domain.StatusFailed)
	require.Equal(t, 5, n.Attempts)
	require.Equal(t, "provider returned 500", *n.LastError)
	require.EqualValues(t, 5, e.postCount(id))

	attempts, err := e.store.ListAttempts(context.Background(), id)
	require.NoError(t, err)
	require.Len(t, attempts, 5)

	require.Eventually(t, func() bool {
		depth, err := e.mq.QueueDepth(broker.QueueDLQ)
		return err == nil && depth == 1
	}, 5*time.Second, 50*time.Millisecond, "message must be in the DLQ")
}

// A notification that ended in the DLQ is retried through the API path once
// the provider has recovered: fresh attempts, new outbox row, delivered.
func TestRequeueFromDLQIsDelivered(t *testing.T) {
	e := newEnv(t, 0)
	e.status = http.StatusInternalServerError
	e.start(t)
	id := e.create(t)
	e.waitStatus(t, id, domain.StatusFailed)

	e.mu.Lock()
	e.status = 0 // provider recovered
	e.mu.Unlock()
	n, err := e.svc.Retry(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, domain.StatusPending, n.Status)
	require.Zero(t, n.Attempts)

	n = e.waitStatus(t, id, domain.StatusSent)
	require.Equal(t, 1, n.Attempts, "attempts start over")
	attempts, err := e.store.ListAttempts(context.Background(), id)
	require.NoError(t, err)
	require.Len(t, attempts, 6, "history is kept: 5 failed + 1 sent")
	for i, a := range attempts {
		require.Equal(t, i+1, a.AttemptNo, "attemptNo keeps counting across the requeue")
	}
	require.Equal(t, 202, *attempts[5].HTTPStatus)

	_, err = e.svc.Retry(context.Background(), id)
	require.ErrorIs(t, err, domain.ErrInvalidState, "only failed notifications can be retried")
}
