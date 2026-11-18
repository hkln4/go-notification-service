//go:build integration

package postgres_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hkln4/go-notification-service/internal/domain"
	"github.com/hkln4/go-notification-service/internal/store/postgres"
)

func TestClaimByStatus(t *testing.T) {
	tests := []struct {
		from       domain.Status
		leaseValid bool // only for processing
		wantClaim  bool
	}{
		{domain.StatusPending, false, true},
		{domain.StatusScheduled, false, true},
		{domain.StatusRetrying, false, true},
		{domain.StatusProcessing, true, false},
		{domain.StatusProcessing, false, true}, // lease expired: worker presumed dead
		{domain.StatusSent, false, false},
		{domain.StatusFailed, false, false},
		{domain.StatusCancelled, false, false},
	}
	for _, tt := range tests {
		name := string(tt.from)
		if tt.from == domain.StatusProcessing {
			name += map[bool]string{true: " lease valid", false: " lease expired"}[tt.leaseValid]
		}
		t.Run(name, func(t *testing.T) {
			s := newStore(t)
			ctx := context.Background()
			n := newNotification(func(n *domain.Notification) { n.Status = tt.from })
			insert(t, s, n)
			if tt.from == domain.StatusProcessing {
				until := time.Now().Add(-time.Minute)
				if tt.leaseValid {
					until = time.Now().Add(time.Minute)
				}
				_, err := db.Pool.Exec(ctx, `UPDATE notifications SET lease_token = $2, locked_until = $3, attempts = 1 WHERE id = $1`,
					n.ID, uuid.New(), until)
				require.NoError(t, err)
			}

			got, ok, err := s.Claim(ctx, n.ID, uuid.New(), time.Minute)
			require.NoError(t, err)
			require.Equal(t, tt.wantClaim, ok)
			if ok {
				require.Equal(t, domain.StatusProcessing, got.Status)
				require.Positive(t, got.Attempts)
				require.Equal(t, "+905551234567", got.Recipient)
				return
			}
			st, leased, err := s.LeaseState(ctx, n.ID)
			require.NoError(t, err)
			require.Equal(t, tt.from, st)
			require.Equal(t, tt.leaseValid, leased)
		})
	}
}

func TestClaimIncrementsAttempts(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	n := newNotification(nil)
	insert(t, s, n)

	for want := 1; want <= 3; want++ {
		lease := uuid.New()
		got, ok, err := s.Claim(ctx, n.ID, lease, time.Minute)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, want, got.Attempts)
		applied, err := s.CompleteAttempt(ctx, postgres.Outcome{ID: n.ID, Lease: lease, Status: domain.StatusRetrying,
			LastError: "500", Attempt: domain.Attempt{AttemptNo: want, HTTPStatus: ptr(500)}})
		require.NoError(t, err)
		require.True(t, applied)
	}
	attempts, err := s.ListAttempts(ctx, n.ID)
	require.NoError(t, err)
	require.Len(t, attempts, 3)
	require.Equal(t, 500, *attempts[2].HTTPStatus)
}

// Worker A's lease expires mid-call; worker B claims and sends. When A
// finally reports, its fenced update must not overwrite B's result.
func TestCompleteAttemptIsFencedByLease(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	n := newNotification(nil)
	insert(t, s, n)

	leaseA, leaseB := uuid.New(), uuid.New()
	_, ok, err := s.Claim(ctx, n.ID, leaseA, 50*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

	require.Eventually(t, func() bool {
		_, ok, err := s.Claim(ctx, n.ID, leaseB, time.Minute)
		return err == nil && ok
	}, 5*time.Second, 20*time.Millisecond, "B could not claim after A's lease expired")

	applied, err := s.CompleteAttempt(ctx, postgres.Outcome{ID: n.ID, Lease: leaseB, Status: domain.StatusSent,
		ProviderMessageID: "pm-b", Attempt: domain.Attempt{AttemptNo: 2, HTTPStatus: ptr(202)}})
	require.NoError(t, err)
	require.True(t, applied)

	applied, err = s.CompleteAttempt(ctx, postgres.Outcome{ID: n.ID, Lease: leaseA, Status: domain.StatusFailed,
		LastError: "late", Attempt: domain.Attempt{AttemptNo: 1, Error: ptr("timeout")}})
	require.NoError(t, err)
	require.False(t, applied, "stale lease must not apply")

	got, err := s.GetNotification(ctx, n.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusSent, got.Status)
	require.Equal(t, "pm-b", *got.ProviderMessageID)
	attempts, err := s.ListAttempts(ctx, n.ID)
	require.NoError(t, err)
	require.Len(t, attempts, 2, "both calls happened and are recorded")
}

// Cancel and claim race on the same pending row, many times over. Each
// round exactly one of them wins; never both, never neither.
func TestCancelRacesClaim(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	var claimed, cancelled atomic.Int32
	const rounds = 50
	for range rounds {
		n := newNotification(nil)
		insert(t, s, n)

		var wg sync.WaitGroup
		start := make(chan struct{})
		var claimOK, cancelOK bool
		wg.Go(func() {
			<-start
			_, ok, err := s.Claim(ctx, n.ID, uuid.New(), time.Minute)
			assert.NoError(t, err) // not require: off the test goroutine
			claimOK = ok
		})
		wg.Go(func() {
			<-start
			_, err := s.Cancel(ctx, n.ID)
			cancelOK = err == nil
		})
		close(start)
		wg.Wait()

		require.NotEqual(t, claimOK, cancelOK, "exactly one of claim and cancel must win")
		if claimOK {
			claimed.Add(1)
		} else {
			cancelled.Add(1)
		}
	}
	t.Logf("claim won %d, cancel won %d of %d rounds", claimed.Load(), cancelled.Load(), rounds)
}

func ptr[T any](v T) *T { return &v }
