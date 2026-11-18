//go:build integration

package relay_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/hkln4/go-notification-service/internal/broker"
	"github.com/hkln4/go-notification-service/internal/domain"
	"github.com/hkln4/go-notification-service/internal/relay"
	"github.com/hkln4/go-notification-service/internal/service"
	"github.com/hkln4/go-notification-service/internal/store/postgres"
	"github.com/hkln4/go-notification-service/internal/testutil/mqtest"
	"github.com/hkln4/go-notification-service/internal/testutil/pgtest"
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

// From API write to queued message: the service stores notifications with
// their outbox rows, the relay publishes them with the right routing and
// priority and empties the outbox.
func TestRelayMovesOutboxToQueues(t *testing.T) {
	db.Reset(t)
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	mqc := broker.New(mq.URL, broker.NewTopology([]time.Duration{time.Second}), log)
	defer mqc.Close()
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	require.NoError(t, mqc.WaitReady(waitCtx))
	require.NoError(t, mq.Purge("q.sms", "q.email", "q.push"))

	store := postgres.New(db.Pool)
	svc := service.New(store)
	res, err := svc.CreateBatch(ctx, service.CreateBatchRequest{CorrelationID: "corr-relay", Items: []service.BatchItem{
		{Input: domain.NotificationInput{Channel: "sms", Recipient: "+905551234567", Content: "a", Priority: "high"}},
		{Input: domain.NotificationInput{Channel: "email", Recipient: "a@b.co", Content: "b"}},
		{Input: domain.NotificationInput{Channel: "push", Recipient: "tok", Content: "c", Priority: "low"}},
	}})
	require.NoError(t, err)

	r := relay.New(store, mqc, relay.Config{BatchSize: 100, PollInterval: 10 * time.Millisecond, Lease: time.Minute}, log)
	n, err := r.Tick(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, n)

	var outbox int
	require.NoError(t, db.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&outbox))
	require.Zero(t, outbox, "confirmed rows are deleted")

	conn, err := amqp.Dial(mq.URL)
	require.NoError(t, err)
	defer conn.Close()
	ch, err := conn.Channel()
	require.NoError(t, err)

	for i, want := range []struct {
		queue    string
		priority uint8
	}{{"q.sms", 2}, {"q.email", 1}, {"q.push", 0}} {
		d, ok, err := ch.Get(want.queue, true)
		require.NoError(t, err)
		require.True(t, ok, "no message in %s", want.queue)
		require.Equal(t, want.priority, d.Priority)
		require.Equal(t, res.NotificationIDs[i].String(), d.MessageId)
		require.Equal(t, "corr-relay", d.CorrelationId)
		require.Equal(t, amqp.Persistent, d.DeliveryMode)

		var body domain.DeliveryMessage
		require.NoError(t, json.Unmarshal(d.Body, &body))
		require.Equal(t, res.NotificationIDs[i], body.NotificationID)
	}
}
