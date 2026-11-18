//go:build integration

package postgres_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hkln4/go-notification-service/internal/domain"
)

func TestClaimOutboxOnlyDueRows(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	future := time.Now().Add(time.Hour)
	due := newNotification(nil)
	insert(t, s, due, newNotification(func(n *domain.Notification) {
		n.Status, n.ScheduledAt = domain.StatusScheduled, &future
	}))

	rows, err := s.ClaimOutbox(ctx, 10, time.Minute)
	require.NoError(t, err)
	require.Len(t, rows, 1, "scheduled row is not due yet")
	require.Equal(t, due.ID, rows[0].NotificationID)
	require.Equal(t, "sms", rows[0].RoutingKey)
	require.Equal(t, "corr-1", rows[0].Headers["x-correlation-id"])
}

func TestClaimOutboxLease(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	insert(t, s, newNotification(nil))

	first, err := s.ClaimOutbox(ctx, 10, 300*time.Millisecond)
	require.NoError(t, err)
	require.Len(t, first, 1)

	again, err := s.ClaimOutbox(ctx, 10, time.Minute)
	require.NoError(t, err)
	require.Empty(t, again, "leased row must not be claimed twice")

	// The relay that claimed it "died"; once the lease runs out another
	// relay picks the row up.
	require.Eventually(t, func() bool {
		rows, err := s.ClaimOutbox(ctx, 10, time.Minute)
		return err == nil && len(rows) == 1
	}, 5*time.Second, 50*time.Millisecond)
}

// Concurrent relays split the backlog between them: SKIP LOCKED makes each
// one skip rows another is claiming instead of waiting or taking them too.
func TestClaimOutboxConcurrentRelaysNeverShareRows(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	ns := make([]domain.Notification, 500)
	for i := range ns {
		ns[i] = newNotification(nil)
	}
	insert(t, s, ns...)

	var mu sync.Mutex
	seen := map[int64]int{}
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() {
			for {
				rows, err := s.ClaimOutbox(ctx, 37, time.Minute)
				if err != nil || len(rows) == 0 {
					return
				}
				mu.Lock()
				for _, r := range rows {
					seen[r.ID]++
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	require.Len(t, seen, 500, "every row claimed")
	for id, n := range seen {
		require.Equal(t, 1, n, "row %d claimed %d times", id, n)
	}
}

func TestDeleteOutbox(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	insert(t, s, newNotification(nil), newNotification(nil))
	rows, err := s.ClaimOutbox(ctx, 10, time.Minute)
	require.NoError(t, err)

	require.NoError(t, s.DeleteOutbox(ctx, []int64{rows[0].ID}))
	require.NoError(t, s.DeleteOutbox(ctx, nil))
	require.Equal(t, 1, count(t, `SELECT count(*) FROM outbox`))
}
