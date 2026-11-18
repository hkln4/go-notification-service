//go:build integration

package service_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hkln4/go-notification-service/internal/domain"
	"github.com/hkln4/go-notification-service/internal/service"
	"github.com/hkln4/go-notification-service/internal/store/postgres"
	"github.com/hkln4/go-notification-service/internal/testutil/pgtest"
)

var db *pgtest.DB

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	db, err = pgtest.Start(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	db.Close(ctx)
	os.Exit(code)
}

var input = domain.NotificationInput{Channel: "sms", Recipient: "+905551234567", Content: "hi"}

// Many clients retry the same request at once. The unique index serialises
// them: exactly one creates the notification, the rest get it back.
func TestConcurrentCreateSameKey(t *testing.T) {
	db.Reset(t)
	svc := service.New(postgres.New(db.Pool))

	const n = 20
	var created, replayed atomic.Int32
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			res, err := svc.Create(context.Background(), service.CreateRequest{Input: input, IdempotencyKey: "same"})
			// assert, not require: FailNow must not be called off the test goroutine.
			if !assert.NoError(t, err) {
				return
			}
			if res.Replayed {
				replayed.Add(1)
			} else {
				created.Add(1)
			}
		})
	}
	wg.Wait()

	var rows, outbox int
	require.NoError(t, db.Pool.QueryRow(context.Background(),
		`SELECT (SELECT count(*) FROM notifications), (SELECT count(*) FROM outbox)`).Scan(&rows, &outbox))
	require.EqualValues(t, 1, created.Load())
	require.EqualValues(t, n-1, replayed.Load())
	require.Equal(t, 1, rows)
	require.Equal(t, 1, outbox)
}

func TestBatchItemConflictWritesNothing(t *testing.T) {
	db.Reset(t)
	svc := service.New(postgres.New(db.Pool))
	ctx := context.Background()

	_, err := svc.Create(ctx, service.CreateRequest{Input: input, IdempotencyKey: "taken"})
	require.NoError(t, err)
	other := input
	other.Content = "different"
	_, err = svc.CreateBatch(ctx, service.CreateBatchRequest{IdempotencyKey: "batch", Items: []service.BatchItem{
		{Input: input}, {Input: other, IdempotencyKey: "taken"},
	}})
	require.ErrorIs(t, err, domain.ErrIdempotencyMismatch)

	var rows, batches int
	require.NoError(t, db.Pool.QueryRow(ctx,
		`SELECT (SELECT count(*) FROM notifications), (SELECT count(*) FROM batches)`).Scan(&rows, &batches))
	require.Equal(t, 1, rows, "the rejected batch must not leave anything behind")
	require.Zero(t, batches)
}

func TestBatchReplayReturnsSameIDsInOrder(t *testing.T) {
	db.Reset(t)
	svc := service.New(postgres.New(db.Pool))
	ctx := context.Background()

	items := make([]service.BatchItem, 50)
	for i := range items {
		items[i] = service.BatchItem{Input: input}
	}
	req := service.CreateBatchRequest{IdempotencyKey: "b1", Items: items}
	first, err := svc.CreateBatch(ctx, req)
	require.NoError(t, err)
	again, err := svc.CreateBatch(ctx, req)
	require.NoError(t, err)
	require.True(t, again.Replayed)
	require.Equal(t, first.NotificationIDs, again.NotificationIDs, "UUIDv7 order matches request order")
}
