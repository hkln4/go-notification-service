//go:build integration

package service_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"

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
			if err != nil {
				t.Error(err)
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
	if err := db.Pool.QueryRow(context.Background(),
		`SELECT (SELECT count(*) FROM notifications), (SELECT count(*) FROM outbox)`).Scan(&rows, &outbox); err != nil {
		t.Fatal(err)
	}
	if created.Load() != 1 || replayed.Load() != n-1 || rows != 1 || outbox != 1 {
		t.Fatalf("created=%d replayed=%d rows=%d outbox=%d", created.Load(), replayed.Load(), rows, outbox)
	}
}

func TestBatchItemConflictWritesNothing(t *testing.T) {
	db.Reset(t)
	svc := service.New(postgres.New(db.Pool))
	ctx := context.Background()

	if _, err := svc.Create(ctx, service.CreateRequest{Input: input, IdempotencyKey: "taken"}); err != nil {
		t.Fatal(err)
	}
	other := input
	other.Content = "different"
	_, err := svc.CreateBatch(ctx, service.CreateBatchRequest{IdempotencyKey: "batch", Items: []service.BatchItem{
		{Input: input}, {Input: other, IdempotencyKey: "taken"},
	}})
	if !errors.Is(err, domain.ErrIdempotencyMismatch) {
		t.Fatalf("err = %v", err)
	}

	var rows, batches int
	if err := db.Pool.QueryRow(ctx,
		`SELECT (SELECT count(*) FROM notifications), (SELECT count(*) FROM batches)`).Scan(&rows, &batches); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || batches != 0 {
		t.Fatalf("rows=%d batches=%d; the rejected batch must not leave anything behind", rows, batches)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	again, err := svc.CreateBatch(ctx, req)
	if err != nil || !again.Replayed {
		t.Fatalf("replay: %+v, %v", again, err)
	}
	for i := range first.NotificationIDs {
		if first.NotificationIDs[i] != again.NotificationIDs[i] {
			t.Fatalf("id %d differs on replay; UUIDv7 order should match request order", i)
		}
	}
}
