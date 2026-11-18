package service

import (
	"context"
	"errors"
	"slices"
	"strings"
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
	lastFilter    domain.ListFilter
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

// ListNotifications only honours Limit and the cursor; filter SQL is covered
// by the store's integration tests.
func (f *fakeStore) ListNotifications(_ context.Context, lf domain.ListFilter) ([]domain.Notification, error) {
	f.lastFilter = lf
	all := make([]domain.Notification, 0, len(f.notifications))
	for _, n := range f.notifications {
		all = append(all, n)
	}
	slices.SortFunc(all, func(a, b domain.Notification) int {
		if c := b.CreatedAt.Compare(a.CreatedAt); c != 0 {
			return c
		}
		return strings.Compare(b.ID.String(), a.ID.String())
	})
	var out []domain.Notification
	for _, n := range all {
		if lf.AfterCreatedAt != nil {
			after := n.CreatedAt.Before(*lf.AfterCreatedAt) ||
				(n.CreatedAt.Equal(*lf.AfterCreatedAt) && n.ID.String() < lf.AfterID.String())
			if !after {
				continue
			}
		}
		if len(out) == lf.Limit {
			break
		}
		out = append(out, n)
	}
	return out, nil
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

func TestListPaginates(t *testing.T) {
	store := newFakeStore()
	svc := newTestService(store)
	base := fixedNow.Add(-time.Hour)
	// 5 rows, two sharing a timestamp to exercise the id tie-break.
	for i, off := range []int{0, 1, 1, 2, 3} {
		id := uuid.Must(uuid.NewV7())
		store.notifications[id] = domain.Notification{ID: id, CreatedAt: base.Add(time.Duration(off) * time.Second), Content: string(rune('a' + i))}
	}

	var seen []uuid.UUID
	q := ListQuery{Limit: 2}
	for pages := 0; ; pages++ {
		if pages > 5 {
			t.Fatal("pagination did not terminate")
		}
		p, err := svc.List(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range p.Items {
			seen = append(seen, n.ID)
		}
		if p.NextCursor == "" {
			break
		}
		q.Cursor = p.NextCursor
	}
	if len(seen) != 5 {
		t.Fatalf("saw %d rows across pages, want 5", len(seen))
	}
	slices.SortFunc(seen, func(a, b uuid.UUID) int { return strings.Compare(a.String(), b.String()) })
	if len(slices.Compact(seen)) != 5 {
		t.Fatal("a row appeared on two pages")
	}
}

func TestListQueryValidation(t *testing.T) {
	from := fixedNow
	before := fixedNow.Add(-time.Hour)
	tests := []struct {
		name      string
		q         ListQuery
		wantField string
		wantLimit int
	}{
		{"defaults", ListQuery{}, "", DefaultPageSize + 1},
		{"max limit", ListQuery{Limit: MaxPageSize}, "", MaxPageSize + 1},
		{"limit too big", ListQuery{Limit: MaxPageSize + 1}, "limit", 0},
		{"negative limit", ListQuery{Limit: -1}, "limit", 0},
		{"bad status", ListQuery{Status: "done"}, "status", 0},
		{"bad channel", ListQuery{Channel: "fax"}, "channel", 0},
		{"to before from", ListQuery{From: &from, To: &before}, "to", 0},
		{"garbage cursor", ListQuery{Cursor: "!!!"}, "cursor", 0},
		{"base64 but not a cursor", ListQuery{Cursor: "e30"}, "cursor", 0}, // {}
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeStore()
			_, err := newTestService(store).List(context.Background(), tt.q)
			if tt.wantField == "" {
				if err != nil {
					t.Fatal(err)
				}
				if store.lastFilter.Limit != tt.wantLimit {
					t.Fatalf("store limit = %d, want %d (page size + 1)", store.lastFilter.Limit, tt.wantLimit)
				}
				return
			}
			var verr *domain.ValidationError
			if !errors.As(err, &verr) || verr.Fields[0].Field != tt.wantField {
				t.Fatalf("got %v, want error on %s", err, tt.wantField)
			}
		})
	}
}
