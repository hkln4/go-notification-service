//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

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

func TestInsertAndGet(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	ns := []domain.Notification{newNotification(nil)}
	if err := s.InsertNotifications(ctx, nil, ns); err != nil {
		t.Fatal(err)
	}
	if ns[0].CreatedAt.IsZero() {
		t.Fatal("CreatedAt was not filled from RETURNING")
	}

	got, err := s.GetNotification(ctx, ns[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Channel != domain.ChannelSMS || got.Priority != domain.PriorityHigh ||
		got.Status != domain.StatusPending || got.CorrelationID != "corr-1" || got.BatchID != nil {
		t.Fatalf("unexpected notification: %+v", got)
	}

	if _, err := s.GetNotification(ctx, uuid.New()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing id: got %v, want ErrNotFound", err)
	}
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
	if err := s.InsertNotifications(ctx, nil, []domain.Notification{now, later}); err != nil {
		t.Fatal(err)
	}

	type outboxRow struct {
		routingKey  string
		priority    int16
		corr        string
		availableAt time.Time
	}
	read := func(id uuid.UUID) outboxRow {
		var r outboxRow
		err := db.Pool.QueryRow(ctx,
			`SELECT routing_key, priority, headers->>'x-correlation-id', available_at
			 FROM outbox WHERE notification_id = $1`, id).
			Scan(&r.routingKey, &r.priority, &r.corr, &r.availableAt)
		if err != nil {
			t.Fatalf("outbox row for %s: %v", id, err)
		}
		return r
	}

	if r := read(now.ID); r.routingKey != "sms" || r.priority != 2 || r.corr != "corr-1" ||
		time.Since(r.availableAt) > time.Minute {
		t.Errorf("immediate outbox row = %+v", r)
	}
	if r := read(later.ID); r.routingKey != "email" || r.priority != 0 || !r.availableAt.Equal(at) {
		t.Errorf("scheduled outbox row = %+v, want available_at %v", r, at)
	}
}

func TestInsertIsAtomic(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	dup := newNotification(nil)
	err := s.InsertNotifications(ctx, nil, []domain.Notification{newNotification(nil), dup, dup})
	if err == nil {
		t.Fatal("want primary key violation")
	}

	var n int
	if err := db.Pool.QueryRow(ctx,
		`SELECT (SELECT count(*) FROM notifications) + (SELECT count(*) FROM outbox)`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("%d rows survived a failed insert; want 0", n)
	}
}

func TestBatchStatusCounts(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	b := &domain.Batch{ID: uuid.Must(uuid.NewV7()), TotalCount: 3}
	ns := make([]domain.Notification, 3)
	for i := range ns {
		ns[i] = newNotification(func(n *domain.Notification) { n.BatchID = &b.ID })
	}
	if err := s.InsertNotifications(ctx, b, ns); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Cancel(ctx, ns[0].ID); err != nil {
		t.Fatal(err)
	}

	got, counts, err := s.GetBatch(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalCount != 3 || got.CreatedAt.IsZero() {
		t.Errorf("batch = %+v", got)
	}
	if counts[domain.StatusPending] != 2 || counts[domain.StatusCancelled] != 1 || len(counts) != 2 {
		t.Errorf("counts = %v", counts)
	}

	if _, _, err := s.GetBatch(ctx, uuid.New()); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("missing batch: got %v", err)
	}
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
			ctx := context.Background()
			n := newNotification(func(n *domain.Notification) { n.Status = tt.from })
			if err := s.InsertNotifications(ctx, nil, []domain.Notification{n}); err != nil {
				t.Fatal(err)
			}

			got, err := s.Cancel(ctx, n.ID)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Cancel from %s: err = %v, want %v", tt.from, err, tt.wantErr)
			}
			want := domain.StatusCancelled
			if tt.wantErr != nil {
				want = tt.from
			}
			if got.Status != want {
				t.Fatalf("status = %s, want %s", got.Status, want)
			}
		})
	}

	t.Run("not found", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Cancel(context.Background(), uuid.New()); !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})
}

// Many concurrent cancels of the same row: the conditional UPDATE lets
// exactly one of them perform the transition, and all of them succeed.
func TestCancelConcurrent(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	n := newNotification(nil)
	if err := s.InsertNotifications(ctx, nil, []domain.Notification{n}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for range 20 {
		wg.Go(func() {
			_, err := s.Cancel(ctx, n.ID)
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestListAttemptsEmpty(t *testing.T) {
	s := newStore(t)
	got, err := s.ListAttempts(context.Background(), uuid.New())
	if err != nil || len(got) != 0 {
		t.Fatalf("got %v, %v", got, err)
	}
}
