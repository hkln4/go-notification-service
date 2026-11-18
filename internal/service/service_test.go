package service

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/hkln4/go-notification-service/internal/domain"
)

// fakeStore keeps notifications in memory. Only what the tests need.
type fakeStore struct {
	templates     map[uuid.UUID]domain.Template
	notifications map[uuid.UUID]domain.Notification
	batches       map[uuid.UUID]domain.Batch
	inserts       int
	insertErr     error
	lastFilter    domain.ListFilter
	// beforeInsert runs once, just before the next insert; used to simulate
	// a concurrent request committing first.
	beforeInsert func()
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		notifications: map[uuid.UUID]domain.Notification{},
		batches:       map[uuid.UUID]domain.Batch{},
		templates:     map[uuid.UUID]domain.Template{},
	}
}

// InsertNotifications mimics the store's ON CONFLICT behaviour: a taken key
// rejects the whole insert with ErrDuplicateKey.
func (f *fakeStore) InsertNotifications(_ context.Context, b *domain.Batch, ns []domain.Notification) error {
	if f.insertErr != nil {
		return f.insertErr
	}
	if f.beforeInsert != nil {
		hook := f.beforeInsert
		f.beforeInsert = nil
		hook()
	}
	if b != nil && b.IdempotencyKey != nil {
		for _, prev := range f.batches {
			if prev.IdempotencyKey != nil && *prev.IdempotencyKey == *b.IdempotencyKey {
				return domain.ErrDuplicateKey
			}
		}
	}
	for _, n := range ns {
		if n.IdempotencyKey == nil {
			continue
		}
		for _, prev := range f.notifications {
			if prev.IdempotencyKey != nil && *prev.IdempotencyKey == *n.IdempotencyKey {
				return domain.ErrDuplicateKey
			}
		}
	}
	f.inserts++
	if b != nil {
		f.batches[b.ID] = *b
	}
	for i := range ns {
		ns[i].CreatedAt = fixedNow
		f.notifications[ns[i].ID] = ns[i]
	}
	return nil
}

func (f *fakeStore) FindByIdempotencyKeys(_ context.Context, keys []string) (map[string]domain.Notification, error) {
	found := map[string]domain.Notification{}
	for _, n := range f.notifications {
		if n.IdempotencyKey != nil && slices.Contains(keys, *n.IdempotencyKey) {
			found[*n.IdempotencyKey] = n
		}
	}
	return found, nil
}

func (f *fakeStore) FindBatchByIdempotencyKey(_ context.Context, key string) (domain.Batch, []uuid.UUID, error) {
	for _, b := range f.batches {
		if b.IdempotencyKey != nil && *b.IdempotencyKey == key {
			var ids []uuid.UUID
			for _, n := range f.notifications {
				if n.BatchID != nil && *n.BatchID == b.ID {
					ids = append(ids, n.ID)
				}
			}
			return b, ids, nil
		}
	}
	return domain.Batch{}, nil, domain.ErrNotFound
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

func (f *fakeStore) Requeue(_ context.Context, id uuid.UUID) (domain.Notification, error) {
	n, ok := f.notifications[id]
	if !ok {
		return domain.Notification{}, domain.ErrNotFound
	}
	if n.Status != domain.StatusFailed {
		return n, domain.ErrInvalidState
	}
	n.Status, n.Attempts = domain.StatusPending, 0
	f.notifications[id] = n
	return n, nil
}

func (f *fakeStore) CreateTemplate(_ context.Context, t *domain.Template) error {
	for _, prev := range f.templates {
		if prev.Name == t.Name {
			return domain.ErrNameTaken
		}
	}
	f.templates[t.ID] = *t
	return nil
}

func (f *fakeStore) GetTemplate(_ context.Context, id uuid.UUID) (domain.Template, error) {
	t, ok := f.templates[id]
	if !ok {
		return domain.Template{}, domain.ErrNotFound
	}
	return t, nil
}

func (f *fakeStore) ListTemplates(context.Context) ([]domain.Template, error) {
	var ts []domain.Template
	for _, t := range f.templates {
		ts = append(ts, t)
	}
	return ts, nil
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
			res, err := newTestService(store).Create(context.Background(), CreateRequest{Input: tt.input, CorrelationID: "c1"})
			if tt.wantErr {
				var verr *domain.ValidationError
				require.ErrorAs(t, err, &verr)
				require.Zero(t, store.inserts, "invalid request reached the store")
				return
			}
			require.NoError(t, err)
			n := res.Notification
			require.Equal(t, tt.wantStatus, n.Status)
			require.Equal(t, "c1", n.CorrelationID)
			require.Equal(t, uuid.Version(7), n.ID.Version())
			require.False(t, n.CreatedAt.IsZero(), "CreatedAt not filled")
			require.Contains(t, store.notifications, n.ID)
		})
	}
}

func TestCreatePropagatesStoreError(t *testing.T) {
	store := newFakeStore()
	store.insertErr = errors.New("db down")
	_, err := newTestService(store).Create(context.Background(), CreateRequest{
		Input: domain.NotificationInput{Channel: "push", Recipient: "tok", Content: "hi"},
	})
	require.ErrorIs(t, err, store.insertErr)
}

func TestGetAndCancel(t *testing.T) {
	store := newFakeStore()
	svc := newTestService(store)
	ctx := context.Background()

	res, err := svc.Create(ctx, CreateRequest{Input: domain.NotificationInput{Channel: "push", Recipient: "tok", Content: "hi"}})
	require.NoError(t, err)
	id := res.Notification.ID

	got, err := svc.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, id, got.ID)

	cancelled, err := svc.Cancel(ctx, id)
	require.NoError(t, err)
	require.Equal(t, domain.StatusCancelled, cancelled.Status)

	_, err = svc.Get(ctx, uuid.New())
	require.ErrorIs(t, err, domain.ErrNotFound)
}

func TestCreateBatch(t *testing.T) {
	valid := BatchItem{Input: domain.NotificationInput{Channel: "sms", Recipient: "+905551234567", Content: "hi"}}
	invalid := BatchItem{Input: domain.NotificationInput{Channel: "sms", Recipient: "bad", Content: ""}}
	many := make([]BatchItem, MaxBatchSize+1)
	for i := range many {
		many[i] = valid
	}

	tests := []struct {
		name        string
		items       []BatchItem
		wantIndexes []int // indexes reported invalid (-1: batch level); nil means accepted
	}{
		{"accepted", []BatchItem{valid, valid, valid}, nil},
		{"max size accepted", many[:MaxBatchSize], nil},
		{"empty", nil, []int{-1}},
		{"too many", many, []int{-1}},
		{"one bad item rejects all", []BatchItem{valid, invalid, valid, invalid}, []int{1, 1, 3, 3}},
		{"bad item key", []BatchItem{valid, {Input: valid.Input, IdempotencyKey: "has space"}}, []int{1}},
		{"key repeated within batch", []BatchItem{{Input: valid.Input, IdempotencyKey: "k"}, valid, {Input: valid.Input, IdempotencyKey: "k"}}, []int{2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeStore()
			res, err := newTestService(store).CreateBatch(context.Background(), CreateBatchRequest{Items: tt.items, CorrelationID: "c"})

			if tt.wantIndexes == nil {
				require.NoError(t, err)
				require.Equal(t, len(tt.items), res.Batch.TotalCount)
				require.Len(t, res.NotificationIDs, len(tt.items))
				require.Equal(t, 1, store.inserts, "one transaction per batch")
				for _, id := range res.NotificationIDs {
					n := store.notifications[id]
					require.NotNil(t, n.BatchID)
					require.Equal(t, res.Batch.ID, *n.BatchID)
					require.Equal(t, "c", n.CorrelationID)
				}
				return
			}

			var verr *domain.ValidationError
			require.ErrorAs(t, err, &verr)
			require.Zero(t, store.inserts, "rejected batch reached the store")
			var got []int
			for _, f := range verr.Fields {
				if f.Index == nil {
					got = append(got, -1)
				} else {
					got = append(got, *f.Index)
				}
			}
			require.Equal(t, tt.wantIndexes, got)
		})
	}
}

func TestGetBatch(t *testing.T) {
	store := newFakeStore()
	svc := newTestService(store)
	ctx := context.Background()
	in := domain.NotificationInput{Channel: "push", Recipient: "tok", Content: "hi"}

	res, err := svc.CreateBatch(ctx, CreateBatchRequest{Items: []BatchItem{{Input: in}, {Input: in}}})
	require.NoError(t, err)
	_, err = svc.Cancel(ctx, res.NotificationIDs[0])
	require.NoError(t, err)

	got, err := svc.GetBatch(ctx, res.Batch.ID)
	require.NoError(t, err)
	require.Equal(t, map[domain.Status]int{domain.StatusPending: 1, domain.StatusCancelled: 1}, got.StatusCounts)
}

func TestListPaginates(t *testing.T) {
	store := newFakeStore()
	svc := newTestService(store)
	base := fixedNow.Add(-time.Hour)
	// 5 rows, two sharing a timestamp to exercise the id tie-break.
	for _, off := range []int{0, 1, 1, 2, 3} {
		id := uuid.Must(uuid.NewV7())
		store.notifications[id] = domain.Notification{ID: id, CreatedAt: base.Add(time.Duration(off) * time.Second)}
	}

	var seen []uuid.UUID
	q := ListQuery{Limit: 2}
	for pages := 0; ; pages++ {
		require.LessOrEqual(t, pages, 5, "pagination did not terminate")
		p, err := svc.List(context.Background(), q)
		require.NoError(t, err)
		for _, n := range p.Items {
			seen = append(seen, n.ID)
		}
		if p.NextCursor == "" {
			break
		}
		q.Cursor = p.NextCursor
	}
	require.Len(t, seen, 5)
	slices.SortFunc(seen, func(a, b uuid.UUID) int { return strings.Compare(a.String(), b.String()) })
	require.Len(t, slices.Compact(seen), 5, "a row appeared on two pages")
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
				require.NoError(t, err)
				require.Equal(t, tt.wantLimit, store.lastFilter.Limit, "page size + 1")
				return
			}
			var verr *domain.ValidationError
			require.ErrorAs(t, err, &verr)
			require.Equal(t, tt.wantField, verr.Fields[0].Field)
		})
	}
}
