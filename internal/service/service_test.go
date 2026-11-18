package service

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/hkln4/go-notification-service/internal/domain"
)

// fakeStore keeps notifications in memory. Only what the tests need.
type fakeStore struct {
	notifications map[uuid.UUID]domain.Notification
	batches       map[uuid.UUID]domain.Batch
	inserts       int
	insertErr     error
}

func newFakeStore() *fakeStore {
	return &fakeStore{notifications: map[uuid.UUID]domain.Notification{}, batches: map[uuid.UUID]domain.Batch{}}
}

func (f *fakeStore) InsertNotifications(_ context.Context, b *domain.Batch, ns []domain.Notification) error {
	if f.insertErr != nil {
		return f.insertErr
	}
	f.inserts++
	if b != nil {
		f.batches[b.ID] = *b
	}
	for _, n := range ns {
		f.notifications[n.ID] = n
	}
	return nil
}

func (f *fakeStore) GetNotification(_ context.Context, id uuid.UUID) (domain.Notification, error) {
	n, ok := f.notifications[id]
	if !ok {
		return domain.Notification{}, domain.ErrNotFound
	}
	return n, nil
}

func (f *fakeStore) ListAttempts(context.Context, uuid.UUID) ([]domain.Attempt, error) {
	return nil, nil
}

func (f *fakeStore) Cancel(_ context.Context, id uuid.UUID) (domain.Notification, error) {
	n, ok := f.notifications[id]
	if !ok {
		return domain.Notification{}, domain.ErrNotFound
	}
	if !n.Status.IsCancellable() {
		return n, domain.ErrInvalidState
	}
	n.Status = domain.StatusCancelled
	f.notifications[id] = n
	return n, nil
}

func (f *fakeStore) GetBatch(_ context.Context, id uuid.UUID) (domain.Batch, map[domain.Status]int, error) {
	b, ok := f.batches[id]
	if !ok {
		return domain.Batch{}, nil, domain.ErrNotFound
	}
	counts := map[domain.Status]int{}
	for _, n := range f.notifications {
		if n.BatchID != nil && *n.BatchID == id {
			counts[n.Status]++
		}
	}
	return b, counts, nil
}

var fixedNow = time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

func newTestService(store Store) *Service {
	return New(store, WithClock(func() time.Time { return fixedNow }))
}

func TestCreate(t *testing.T) {
	future := fixedNow.Add(time.Hour)
	tests := []struct {
		name       string
		input      domain.NotificationInput
		wantStatus domain.Status
		wantErr    bool
	}{
		{"immediate is pending", domain.NotificationInput{Channel: "sms", Recipient: "+905551234567", Content: "hi"}, domain.StatusPending, false},
		{"future is scheduled", domain.NotificationInput{Channel: "sms", Recipient: "+905551234567", Content: "hi", ScheduledAt: &future}, domain.StatusScheduled, false},
		{"invalid is rejected before storing", domain.NotificationInput{Channel: "sms", Recipient: "bad", Content: "hi"}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeStore()
			n, err := newTestService(store).Create(context.Background(), CreateRequest{Input: tt.input, CorrelationID: "c1"})
			if tt.wantErr {
				var verr *domain.ValidationError
				if !errors.As(err, &verr) {
					t.Fatalf("want validation error, got %v", err)
				}
				if store.inserts != 0 {
					t.Fatal("invalid request reached the store")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if n.Status != tt.wantStatus || n.CorrelationID != "c1" || n.ID.Version() != 7 {
				t.Fatalf("got status=%s corr=%q version=%d", n.Status, n.CorrelationID, n.ID.Version())
			}
			if _, ok := store.notifications[n.ID]; !ok {
				t.Fatal("notification not stored")
			}
		})
	}
}

func TestCreatePropagatesStoreError(t *testing.T) {
	store := newFakeStore()
	store.insertErr = errors.New("db down")
	_, err := newTestService(store).Create(context.Background(), CreateRequest{
		Input: domain.NotificationInput{Channel: "push", Recipient: "tok", Content: "hi"},
	})
	if !errors.Is(err, store.insertErr) {
		t.Fatalf("got %v", err)
	}
}

func TestGetAndCancel(t *testing.T) {
	store := newFakeStore()
	svc := newTestService(store)
	ctx := context.Background()

	n, err := svc.Create(ctx, CreateRequest{Input: domain.NotificationInput{Channel: "push", Recipient: "tok", Content: "hi"}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := svc.Get(ctx, n.ID)
	if err != nil || got.ID != n.ID {
		t.Fatalf("Get = %+v, %v", got, err)
	}
	if c, err := svc.Cancel(ctx, n.ID); err != nil || c.Status != domain.StatusCancelled {
		t.Fatalf("Cancel = %+v, %v", c, err)
	}
	if _, err := svc.Get(ctx, uuid.New()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("Get missing = %v", err)
	}
}

func TestCreateBatch(t *testing.T) {
	valid := domain.NotificationInput{Channel: "sms", Recipient: "+905551234567", Content: "hi"}
	invalid := domain.NotificationInput{Channel: "sms", Recipient: "bad", Content: ""}
	many := make([]domain.NotificationInput, MaxBatchSize+1)
	for i := range many {
		many[i] = valid
	}

	tests := []struct {
		name        string
		items       []domain.NotificationInput
		wantIndexes []int // indexes reported invalid; nil means accepted
	}{
		{"accepted", []domain.NotificationInput{valid, valid, valid}, nil},
		{"max size accepted", many[:MaxBatchSize], nil},
		{"empty", nil, []int{-1}},
		{"too many", many, []int{-1}},
		{"one bad item rejects all", []domain.NotificationInput{valid, invalid, valid, invalid}, []int{1, 1, 3, 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeStore()
			b, ns, err := newTestService(store).CreateBatch(context.Background(), CreateBatchRequest{Items: tt.items, CorrelationID: "c"})

			if tt.wantIndexes == nil {
				if err != nil {
					t.Fatal(err)
				}
				if b.TotalCount != len(tt.items) || len(ns) != len(tt.items) || store.inserts != 1 {
					t.Fatalf("total=%d stored=%d inserts=%d", b.TotalCount, len(ns), store.inserts)
				}
				for _, n := range ns {
					if n.BatchID == nil || *n.BatchID != b.ID || n.CorrelationID != "c" {
						t.Fatalf("notification not linked to batch: %+v", n)
					}
				}
				return
			}

			var verr *domain.ValidationError
			if !errors.As(err, &verr) {
				t.Fatalf("want validation error, got %v", err)
			}
			if store.inserts != 0 {
				t.Fatal("rejected batch reached the store")
			}
			var got []int
			for _, f := range verr.Fields {
				if f.Index == nil {
					got = append(got, -1)
				} else {
					got = append(got, *f.Index)
				}
			}
			if !slices.Equal(got, tt.wantIndexes) {
				t.Fatalf("indexes = %v, want %v (%v)", got, tt.wantIndexes, err)
			}
		})
	}
}

func TestGetBatch(t *testing.T) {
	store := newFakeStore()
	svc := newTestService(store)
	ctx := context.Background()
	in := domain.NotificationInput{Channel: "push", Recipient: "tok", Content: "hi"}

	b, ns, err := svc.CreateBatch(ctx, CreateBatchRequest{Items: []domain.NotificationInput{in, in}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Cancel(ctx, ns[0].ID); err != nil {
		t.Fatal(err)
	}
	got, err := svc.GetBatch(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.StatusCounts[domain.StatusPending] != 1 || got.StatusCounts[domain.StatusCancelled] != 1 {
		t.Fatalf("counts = %v", got.StatusCounts)
	}
}
