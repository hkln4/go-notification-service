package service

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/hkln4/go-notification-service/internal/domain"
)

var smsInput = domain.NotificationInput{Channel: "sms", Recipient: "+905551234567", Content: "hi", Priority: "high"}

func TestCreateIdempotency(t *testing.T) {
	other := smsInput
	other.Content = "different"
	// Same meaning, different spelling: empty priority defaults to normal.
	normal := smsInput
	normal.Priority = "normal"
	implicitNormal := smsInput
	implicitNormal.Priority = ""

	tests := []struct {
		name         string
		first        domain.NotificationInput
		second       domain.NotificationInput
		key2         string
		wantReplayed bool
		wantErr      error
	}{
		{"same key same request", smsInput, smsInput, "k", true, nil},
		{"same key different request", smsInput, other, "k", false, domain.ErrIdempotencyMismatch},
		{"hash covers validated values", normal, implicitNormal, "k", true, nil},
		{"different key", smsInput, smsInput, "k2", false, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeStore()
			svc := newTestService(store)
			ctx := context.Background()

			first, err := svc.Create(ctx, CreateRequest{Input: tt.first, IdempotencyKey: "k"})
			if err != nil || first.Replayed {
				t.Fatalf("first create: %+v, %v", first, err)
			}
			second, err := svc.Create(ctx, CreateRequest{Input: tt.second, IdempotencyKey: tt.key2})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if second.Replayed != tt.wantReplayed {
				t.Fatalf("replayed = %v, want %v", second.Replayed, tt.wantReplayed)
			}
			if tt.wantReplayed && second.Notification.ID != first.Notification.ID {
				t.Fatal("replay returned a different notification")
			}
			if want := map[bool]int{true: 1, false: 2}[tt.wantReplayed]; store.inserts != want {
				t.Fatalf("inserts = %d, want %d", store.inserts, want)
			}
		})
	}
}

func TestCreateWithoutKeyIsNotDeduplicated(t *testing.T) {
	store := newFakeStore()
	svc := newTestService(store)
	for range 2 {
		if _, err := svc.Create(context.Background(), CreateRequest{Input: smsInput}); err != nil {
			t.Fatal(err)
		}
	}
	if len(store.notifications) != 2 {
		t.Fatalf("got %d notifications, want 2", len(store.notifications))
	}
}

func TestCreateRejectsBadKey(t *testing.T) {
	_, err := newTestService(newFakeStore()).Create(context.Background(), CreateRequest{Input: smsInput, IdempotencyKey: "bad key"})
	var verr *domain.ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("got %v", err)
	}
}

// A concurrent request with the same key commits between our read and our
// insert. The insert reports ErrDuplicateKey; the retry reads the winner.
func TestCreateLosesRaceThenReplays(t *testing.T) {
	store := newFakeStore()
	svc := newTestService(store)
	ctx := context.Background()

	var winner CreateResult
	store.beforeInsert = func() {
		var err error
		winner, err = svc.Create(ctx, CreateRequest{Input: smsInput, IdempotencyKey: "k"})
		if err != nil {
			t.Fatal(err)
		}
	}
	got, err := svc.Create(ctx, CreateRequest{Input: smsInput, IdempotencyKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Replayed || got.Notification.ID != winner.Notification.ID || len(store.notifications) != 1 {
		t.Fatalf("got %+v, want replay of %s", got, winner.Notification.ID)
	}
}

func TestBatchIdempotency(t *testing.T) {
	ctx := context.Background()
	item := func(key, content string) BatchItem {
		in := smsInput
		in.Content = content
		return BatchItem{Input: in, IdempotencyKey: key}
	}

	t.Run("batch key replays", func(t *testing.T) {
		store := newFakeStore()
		svc := newTestService(store)
		req := CreateBatchRequest{IdempotencyKey: "b", Items: []BatchItem{item("", "a"), item("", "b")}}
		first, err := svc.CreateBatch(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		again, err := svc.CreateBatch(ctx, req)
		if err != nil || !again.Replayed || again.Batch.ID != first.Batch.ID || store.inserts != 1 {
			t.Fatalf("replay = %+v, %v (inserts %d)", again, err, store.inserts)
		}

		req.Items[1] = item("", "changed")
		if _, err := svc.CreateBatch(ctx, req); !errors.Is(err, domain.ErrIdempotencyMismatch) {
			t.Fatalf("changed batch with same key: %v", err)
		}
	})

	t.Run("item keys", func(t *testing.T) {
		store := newFakeStore()
		svc := newTestService(store)
		first, err := svc.CreateBatch(ctx, CreateBatchRequest{Items: []BatchItem{item("i1", "a"), item("i2", "b")}})
		if err != nil {
			t.Fatal(err)
		}

		// i1 repeated unchanged is skipped; i3 is new.
		res, err := svc.CreateBatch(ctx, CreateBatchRequest{Items: []BatchItem{item("i3", "c"), item("i1", "a")}})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(res.Duplicates, []int{1}) || res.NotificationIDs[1] != first.NotificationIDs[0] || res.Batch.TotalCount != 1 {
			t.Fatalf("result = %+v", res)
		}

		// i2 reused with different content fails the whole batch.
		_, err = svc.CreateBatch(ctx, CreateBatchRequest{Items: []BatchItem{item("i4", "d"), item("i2", "changed")}})
		var conflict *domain.IdempotencyConflictError
		if !errors.As(err, &conflict) || len(conflict.Fields) != 1 || *conflict.Fields[0].Index != 1 {
			t.Fatalf("got %v, want conflict on index 1", err)
		}
		if found, _ := store.FindByIdempotencyKeys(ctx, []string{"i4"}); len(found) != 0 {
			t.Fatal("i4 was stored although the batch was rejected")
		}
	})
}
