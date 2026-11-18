//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/hkln4/go-notification-service/internal/domain"
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

func newStore(t *testing.T) *postgres.Store {
	t.Helper()
	db.Reset(t)
	return postgres.New(db.Pool)
}

func newNotification(mod func(*domain.Notification)) domain.Notification {
	n := domain.Notification{
		ID:            uuid.Must(uuid.NewV7()),
		Channel:       domain.ChannelSMS,
		Recipient:     "+905551234567",
		Content:       "hello",
		Priority:      domain.PriorityHigh,
		Status:        domain.StatusPending,
		CorrelationID: "corr-1",
	}
	if mod != nil {
		mod(&n)
	}
	return n
}

func insert(t *testing.T, s *postgres.Store, ns ...domain.Notification) {
	t.Helper()
	require.NoError(t, s.InsertNotifications(context.Background(), nil, ns))
}

func count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, db.Pool.QueryRow(context.Background(), query, args...).Scan(&n))
	return n
}

func TestInsertAndGet(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	ns := []domain.Notification{newNotification(nil)}
	require.NoError(t, s.InsertNotifications(ctx, nil, ns))
	require.False(t, ns[0].CreatedAt.IsZero(), "CreatedAt is filled from RETURNING")

	got, err := s.GetNotification(ctx, ns[0].ID)
	require.NoError(t, err)
	require.Equal(t, domain.ChannelSMS, got.Channel)
	require.Equal(t, domain.PriorityHigh, got.Priority)
	require.Equal(t, domain.StatusPending, got.Status)
	require.Equal(t, "corr-1", got.CorrelationID)
	require.Nil(t, got.BatchID)

	_, err = s.GetNotification(ctx, uuid.New())
	require.ErrorIs(t, err, domain.ErrNotFound)
}

func TestInsertWritesOutboxInSameTransaction(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	at := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)

	now := newNotification(nil)
	later := newNotification(func(n *domain.Notification) {
		n.Channel = domain.ChannelEmail
		n.Recipient = "a@b.co"
		n.Priority = domain.PriorityLow
		n.Status = domain.StatusScheduled
		n.ScheduledAt = &at
	})
	insert(t, s, now, later)

	type outboxRow struct {
		routingKey  string
		priority    int16
		corr        *string
		availableAt time.Time
	}
	read := func(id uuid.UUID) outboxRow {
		var r outboxRow
		require.NoError(t, db.Pool.QueryRow(ctx,
			`SELECT routing_key, priority, headers->>'x-correlation-id', available_at
			 FROM outbox WHERE notification_id = $1`, id).
			Scan(&r.routingKey, &r.priority, &r.corr, &r.availableAt))
		return r
	}

	r := read(now.ID)
	require.Equal(t, "sms", r.routingKey)
	require.Equal(t, int16(2), r.priority)
	require.Equal(t, "corr-1", *r.corr)
	require.WithinDuration(t, time.Now(), r.availableAt, time.Minute, "immediate rows are available now")

	r = read(later.ID)
	require.Equal(t, "email", r.routingKey)
	require.Equal(t, int16(0), r.priority)
	require.True(t, r.availableAt.Equal(at), "scheduled rows become available at scheduled_at")
}

func TestInsertIsAtomic(t *testing.T) {
	s := newStore(t)
	dup := newNotification(nil)

	err := s.InsertNotifications(context.Background(), nil, []domain.Notification{newNotification(nil), dup, dup})
	require.Error(t, err, "want primary key violation")
	require.Zero(t, count(t, `SELECT (SELECT count(*) FROM notifications) + (SELECT count(*) FROM outbox)`),
		"rows survived a failed insert")
}

func TestInsertDuplicateKey(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	key, hash := "k", "h"
	withKey := func(n *domain.Notification) { n.IdempotencyKey, n.RequestHash = &key, &hash }

	insert(t, s, newNotification(withKey))
	err := s.InsertNotifications(ctx, nil, []domain.Notification{newNotification(nil), newNotification(withKey)})
	require.ErrorIs(t, err, domain.ErrDuplicateKey)
	require.Equal(t, 1, count(t, `SELECT count(*) FROM notifications`), "the other item was rolled back")

	found, err := s.FindByIdempotencyKeys(ctx, []string{"k", "missing"})
	require.NoError(t, err)
	require.Len(t, found, 1)
	require.Equal(t, "h", *found["k"].RequestHash)
}

func TestBatchStatusCounts(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	b := &domain.Batch{ID: uuid.Must(uuid.NewV7()), TotalCount: 3}
	ns := make([]domain.Notification, 3)
	for i := range ns {
		ns[i] = newNotification(func(n *domain.Notification) { n.BatchID = &b.ID })
	}
	require.NoError(t, s.InsertNotifications(ctx, b, ns))
	_, err := s.Cancel(ctx, ns[0].ID)
	require.NoError(t, err)

	got, counts, err := s.GetBatch(ctx, b.ID)
	require.NoError(t, err)
	require.Equal(t, 3, got.TotalCount)
	require.False(t, got.CreatedAt.IsZero())
	require.Equal(t, map[domain.Status]int{domain.StatusPending: 2, domain.StatusCancelled: 1}, counts)

	_, _, err = s.GetBatch(ctx, uuid.New())
	require.ErrorIs(t, err, domain.ErrNotFound)
}

func TestCancel(t *testing.T) {
	tests := []struct {
		from    domain.Status
		wantErr error
	}{
		{domain.StatusPending, nil},
		{domain.StatusScheduled, nil},
		{domain.StatusRetrying, nil},
		{domain.StatusCancelled, nil}, // idempotent
		{domain.StatusProcessing, domain.ErrInvalidState},
		{domain.StatusSent, domain.ErrInvalidState},
		{domain.StatusFailed, domain.ErrInvalidState},
	}
	for _, tt := range tests {
		t.Run(string(tt.from), func(t *testing.T) {
			s := newStore(t)
			n := newNotification(func(n *domain.Notification) { n.Status = tt.from })
			insert(t, s, n)

			got, err := s.Cancel(context.Background(), n.ID)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.Equal(t, tt.from, got.Status, "status is unchanged")
				return
			}
			require.NoError(t, err)
			require.Equal(t, domain.StatusCancelled, got.Status)
		})
	}

	t.Run("not found", func(t *testing.T) {
		_, err := newStore(t).Cancel(context.Background(), uuid.New())
		require.ErrorIs(t, err, domain.ErrNotFound)
	})
}

// Many concurrent cancels of the same row: the conditional UPDATE lets
// exactly one of them perform the transition, and all of them succeed.
func TestCancelConcurrent(t *testing.T) {
	s := newStore(t)
	n := newNotification(nil)
	insert(t, s, n)

	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for range 20 {
		wg.Go(func() {
			_, err := s.Cancel(context.Background(), n.ID)
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestListAttemptsEmpty(t *testing.T) {
	got, err := newStore(t).ListAttempts(context.Background(), uuid.New())
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestListNotifications(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	// 30 rows: channels rotate, every third is sent. created_at is set
	// explicitly so ordering and time filters are deterministic, and pairs
	// of rows share a timestamp to exercise the id tie-break.
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	var ns []domain.Notification
	for i := range 30 {
		ns = append(ns, newNotification(func(n *domain.Notification) {
			n.Channel = domain.Channels[i%3]
			if i%3 == 0 {
				n.Status = domain.StatusSent
			}
		}))
	}
	insert(t, s, ns...)
	for i, n := range ns {
		_, err := db.Pool.Exec(ctx, `UPDATE notifications SET created_at = $2 WHERE id = $1`,
			n.ID, base.Add(time.Duration(i/2)*time.Minute))
		require.NoError(t, err)
	}

	sent, sms := domain.StatusSent, domain.ChannelSMS
	from, to := base.Add(5*time.Minute), base.Add(10*time.Minute)

	// pageAll walks every page and checks ordering across page boundaries.
	pageAll := func(t *testing.T, f domain.ListFilter) []domain.Notification {
		var all []domain.Notification
		for {
			page, err := s.ListNotifications(ctx, f)
			require.NoError(t, err)
			all = append(all, page...)
			if len(page) < f.Limit {
				break
			}
			last := page[len(page)-1]
			f.AfterCreatedAt, f.AfterID = &last.CreatedAt, last.ID
		}
		for i := 1; i < len(all); i++ {
			a, b := all[i-1], all[i]
			inOrder := a.CreatedAt.After(b.CreatedAt) || (a.CreatedAt.Equal(b.CreatedAt) && a.ID.String() > b.ID.String())
			require.True(t, inOrder, "rows %d and %d out of order", i-1, i)
		}
		return all
	}

	tests := []struct {
		name string
		f    domain.ListFilter
		want int
	}{
		{"all", domain.ListFilter{Limit: 7}, 30},
		{"status", domain.ListFilter{Status: &sent, Limit: 4}, 10},
		{"channel", domain.ListFilter{Channel: &sms, Limit: 4}, 10},
		{"status and channel", domain.ListFilter{Status: &sent, Channel: &sms, Limit: 3}, 10},
		{"time range, to exclusive", domain.ListFilter{From: &from, To: &to, Limit: 3}, 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pageAll(t, tt.f)
			require.Len(t, got, tt.want)
			seen := map[uuid.UUID]bool{}
			for _, n := range got {
				require.False(t, seen[n.ID], "%s returned twice", n.ID)
				seen[n.ID] = true
				if tt.f.Status != nil {
					require.Equal(t, *tt.f.Status, n.Status)
				}
				if tt.f.Channel != nil {
					require.Equal(t, *tt.f.Channel, n.Channel)
				}
			}
		})
	}
}
