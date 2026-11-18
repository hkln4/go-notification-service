//go:build integration

// Package integration runs the whole system in-process against real
// PostgreSQL, RabbitMQ and Redis containers: HTTP API → outbox → relay →
// queue → worker (Redis rate limiter) → provider.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/hkln4/go-notification-service/internal/broker"
	"github.com/hkln4/go-notification-service/internal/domain"
	httpapi "github.com/hkln4/go-notification-service/internal/http"
	"github.com/hkln4/go-notification-service/internal/provider"
	"github.com/hkln4/go-notification-service/internal/ratelimit"
	"github.com/hkln4/go-notification-service/internal/relay"
	"github.com/hkln4/go-notification-service/internal/service"
	"github.com/hkln4/go-notification-service/internal/store/postgres"
	"github.com/hkln4/go-notification-service/internal/testutil/mqtest"
	"github.com/hkln4/go-notification-service/internal/testutil/pgtest"
	"github.com/hkln4/go-notification-service/internal/testutil/rdtest"
	"github.com/hkln4/go-notification-service/internal/worker"
)

var (
	db *pgtest.DB
	mq *mqtest.MQ
	rd *rdtest.Redis

	api      *httptest.Server
	received = &providerLog{}
)

// providerLog records what the fake provider was sent.
type providerLog struct {
	mu   sync.Mutex
	reqs []provider.Request
}

func (p *providerLog) add(r provider.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reqs = append(p.reqs, r)
}

func (p *providerLog) count(content string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, r := range p.reqs {
		if r.Content == content {
			n++
		}
	}
	return n
}

func TestMain(m *testing.M) {
	code, err := run(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(code)
}

// run starts the containers and every component once for all tests.
func run(m *testing.M) (int, error) {
	ctx := context.Background()
	var err error
	if db, err = pgtest.Start(ctx); err != nil {
		return 0, err
	}
	defer db.Close(ctx)
	if mq, err = mqtest.Start(ctx); err != nil {
		return 0, err
	}
	defer mq.Close(ctx)
	if rd, err = rdtest.Start(ctx); err != nil {
		return 0, err
	}
	defer rd.Close(ctx)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	delays := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond}

	prov := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req provider.Request
		_ = json.NewDecoder(r.Body).Decode(&req)
		received.add(req)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"pm-e2e","status":"accepted","timestamp":"2026-11-18T12:00:00Z"}`))
	}))
	defer prov.Close()

	mqc := broker.New(mq.URL, broker.NewTopology(delays), log)
	defer mqc.Close()
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := mqc.WaitReady(waitCtx); err != nil {
		return 0, err
	}

	rdb := redis.NewClient(&redis.Options{Addr: rd.Addr})
	defer rdb.Close()

	store := postgres.New(db.Pool)
	api = httptest.NewServer(httpapi.New(service.New(store), log))
	defer api.Close()

	runCtx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	r := relay.New(store, mqc, relay.Config{BatchSize: 100, PollInterval: 20 * time.Millisecond, Lease: 10 * time.Second}, log)
	wg.Go(func() { r.Run(runCtx) })
	w := worker.New(store, mqc, provider.NewClient(prov.URL, 2*time.Second), ratelimit.NewRedis(rdb, 100),
		worker.Config{Lease: 3 * time.Second, TokenWait: time.Second, RetryDelays: delays, MaxAttempts: 5}, log)
	wg.Go(func() { w.Run(runCtx, mqc, 10, time.Second) })

	code := m.Run()
	stop()
	wg.Wait()
	return code, nil
}

func call(t *testing.T, method, path string, body any, out any) int {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}
	req, err := http.NewRequest(method, api.URL+path, &buf)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	if out != nil {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(out))
	}
	return resp.StatusCode
}

type notification struct {
	ID               string     `json:"id"`
	Status           string     `json:"status"`
	Attempts         int        `json:"attempts"`
	ScheduledAt      *time.Time `json:"scheduledAt"`
	DeliveryAttempts []struct {
		HTTPStatus int `json:"httpStatus"`
	} `json:"deliveryAttempts"`
}

func waitFor(t *testing.T, id, status string) notification {
	t.Helper()
	var n notification
	require.Eventually(t, func() bool {
		call(t, http.MethodGet, "/api/v1/notifications/"+id, nil, &n)
		return n.Status == status
	}, 15*time.Second, 25*time.Millisecond, "notification %s never became %s (last %s)", id, status, n.Status)
	return n
}

func TestCreateIsDelivered(t *testing.T) {
	content := "e2e " + t.Name()
	var created notification
	status := call(t, http.MethodPost, "/api/v1/notifications", map[string]any{
		"channel": "sms", "recipient": "+905551234567", "content": content, "priority": "high",
	}, &created)
	require.Equal(t, http.StatusAccepted, status)
	require.Equal(t, "pending", created.Status)

	n := waitFor(t, created.ID, "sent")
	require.Equal(t, 1, n.Attempts)
	require.Len(t, n.DeliveryAttempts, 1)
	require.Equal(t, 202, n.DeliveryAttempts[0].HTTPStatus)
	require.Equal(t, 1, received.count(content))
}

// A scheduled notification is invisible to the relay until scheduledAt and
// is delivered shortly after.
func TestScheduledIsDeliveredWhenDue(t *testing.T) {
	content := "e2e " + t.Name()
	at := time.Now().Add(1500 * time.Millisecond).UTC()
	var created notification
	status := call(t, http.MethodPost, "/api/v1/notifications", map[string]any{
		"channel": "email", "recipient": "user@example.com", "content": content, "scheduledAt": at,
	}, &created)
	require.Equal(t, http.StatusAccepted, status)
	require.Equal(t, "scheduled", created.Status)

	time.Sleep(time.Until(at) - 500*time.Millisecond)
	var early notification
	call(t, http.MethodGet, "/api/v1/notifications/"+created.ID, nil, &early)
	require.Equal(t, "scheduled", early.Status, "must not be sent before scheduledAt")
	require.Zero(t, received.count(content))

	waitFor(t, created.ID, "sent")
	require.False(t, time.Now().Before(at), "sent before scheduledAt")
	require.Equal(t, 1, received.count(content))
}

func TestBatchIsDelivered(t *testing.T) {
	content := "e2e " + t.Name()
	items := make([]map[string]any, 30)
	for i := range items {
		ch := domain.Channels[i%3]
		recipient := map[domain.Channel]string{
			domain.ChannelSMS: "+905551234567", domain.ChannelEmail: "a@b.co", domain.ChannelPush: "tok",
		}[ch]
		items[i] = map[string]any{"channel": ch, "recipient": recipient, "content": content}
	}
	var created struct {
		BatchID string `json:"batchId"`
	}
	require.Equal(t, http.StatusAccepted, call(t, http.MethodPost, "/api/v1/notifications/batch",
		map[string]any{"notifications": items}, &created))

	var batch struct {
		StatusCounts map[string]int `json:"statusCounts"`
	}
	require.Eventually(t, func() bool {
		call(t, http.MethodGet, "/api/v1/batches/"+created.BatchID, nil, &batch)
		return batch.StatusCounts["sent"] == 30
	}, 15*time.Second, 50*time.Millisecond, "batch counts: %v", batch.StatusCounts)
	require.Equal(t, 30, received.count(content))
}

// Cancelling through the API before delivery: the notification never
// reaches the provider. scheduledAt keeps it out of the relay's way long
// enough to cancel deterministically.
func TestCancelledIsNeverSent(t *testing.T) {
	content := "e2e " + t.Name()
	var created notification
	call(t, http.MethodPost, "/api/v1/notifications", map[string]any{
		"channel": "push", "recipient": "tok", "content": content,
		"scheduledAt": time.Now().Add(500 * time.Millisecond).UTC(),
	}, &created)

	var cancelled notification
	require.Equal(t, http.StatusOK, call(t, http.MethodPost, "/api/v1/notifications/"+created.ID+"/cancel", nil, &cancelled))
	require.Equal(t, "cancelled", cancelled.Status)

	time.Sleep(time.Second) // past scheduledAt: the relay publishes, the worker drops it
	var n notification
	call(t, http.MethodGet, "/api/v1/notifications/"+created.ID, nil, &n)
	require.Equal(t, "cancelled", n.Status)
	require.Zero(t, received.count(content))
}
