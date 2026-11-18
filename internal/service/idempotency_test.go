package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

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
			require.NoError(t, err)
			require.False(t, first.Replayed)

			second, err := svc.Create(ctx, CreateRequest{Input: tt.second, IdempotencyKey: tt.key2})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantReplayed, second.Replayed)
			if tt.wantReplayed {
				require.Equal(t, first.Notification.ID, second.Notification.ID)
				require.Equal(t, 1, store.inserts)
			} else {
				require.Equal(t, 2, store.inserts)
			}
		})
	}
}

func TestCreateWithoutKeyIsNotDeduplicated(t *testing.T) {
	store := newFakeStore()
	svc := newTestService(store)
	for range 2 {
		_, err := svc.Create(context.Background(), CreateRequest{Input: smsInput})
		require.NoError(t, err)
	}
	require.Len(t, store.notifications, 2)
}

func TestCreateRejectsBadKey(t *testing.T) {
	_, err := newTestService(newFakeStore()).Create(context.Background(), CreateRequest{Input: smsInput, IdempotencyKey: "bad key"})
	var verr *domain.ValidationError
	require.ErrorAs(t, err, &verr)
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
		require.NoError(t, err)
	}
	got, err := svc.Create(ctx, CreateRequest{Input: smsInput, IdempotencyKey: "k"})
	require.NoError(t, err)
	require.True(t, got.Replayed)
	require.Equal(t, winner.Notification.ID, got.Notification.ID)
	require.Len(t, store.notifications, 1)
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
		require.NoError(t, err)
		again, err := svc.CreateBatch(ctx, req)
		require.NoError(t, err)
		require.True(t, again.Replayed)
		require.Equal(t, first.Batch.ID, again.Batch.ID)
		require.Equal(t, 1, store.inserts)

		req.Items[1] = item("", "changed")
		_, err = svc.CreateBatch(ctx, req)
		require.ErrorIs(t, err, domain.ErrIdempotencyMismatch)
	})

	t.Run("item keys", func(t *testing.T) {
		store := newFakeStore()
		svc := newTestService(store)
		first, err := svc.CreateBatch(ctx, CreateBatchRequest{Items: []BatchItem{item("i1", "a"), item("i2", "b")}})
		require.NoError(t, err)

		// i1 repeated unchanged is skipped; i3 is new.
		res, err := svc.CreateBatch(ctx, CreateBatchRequest{Items: []BatchItem{item("i3", "c"), item("i1", "a")}})
		require.NoError(t, err)
		require.Equal(t, []int{1}, res.Duplicates)
		require.Equal(t, first.NotificationIDs[0], res.NotificationIDs[1])
		require.Equal(t, 1, res.Batch.TotalCount)

		// i2 reused with different content fails the whole batch.
		_, err = svc.CreateBatch(ctx, CreateBatchRequest{Items: []BatchItem{item("i4", "d"), item("i2", "changed")}})
		var conflict *domain.IdempotencyConflictError
		require.ErrorAs(t, err, &conflict)
		require.Len(t, conflict.Fields, 1)
		require.Equal(t, 1, *conflict.Fields[0].Index)

		found, err := store.FindByIdempotencyKeys(ctx, []string{"i4"})
		require.NoError(t, err)
		require.Empty(t, found, "i4 was stored although the batch was rejected")
	})
}
