package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/hkln4/go-notification-service/internal/domain"
)

type fakePublisher struct {
	events []domain.StatusEvent
	err    error
}

func (f *fakePublisher) PublishStatus(_ context.Context, e domain.StatusEvent) error {
	f.events = append(f.events, e)
	return f.err
}

func TestStatusEventsFromAPIChanges(t *testing.T) {
	batch := uuid.New()
	updated := fixedNow.Add(-time.Minute)
	seed := func(store *fakeStore, st domain.Status, attempts int) uuid.UUID {
		id := uuid.New()
		store.notifications[id] = domain.Notification{ID: id, BatchID: &batch, Status: st, Attempts: attempts, UpdatedAt: updated}
		return id
	}

	tests := []struct {
		name       string
		from       domain.Status
		call       func(*Service, context.Context, uuid.UUID) (domain.Notification, error)
		pubErr     error
		wantErr    error
		wantStatus domain.Status // "" means no event
	}{
		{"cancel publishes cancelled", domain.StatusPending, (*Service).Cancel, nil, nil, domain.StatusCancelled},
		{"cancel in invalid state publishes nothing", domain.StatusProcessing, (*Service).Cancel, nil, domain.ErrInvalidState, ""},
		{"retry publishes pending", domain.StatusFailed, (*Service).Retry, nil, nil, domain.StatusPending},
		{"retry in invalid state publishes nothing", domain.StatusSent, (*Service).Retry, nil, domain.ErrInvalidState, ""},
		{"publish failure does not fail cancel", domain.StatusScheduled, (*Service).Cancel, errors.New("broker down"), nil, domain.StatusCancelled},
		{"publish failure does not fail retry", domain.StatusFailed, (*Service).Retry, errors.New("broker down"), nil, domain.StatusPending},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeStore()
			pub := &fakePublisher{err: tt.pubErr}
			svc := New(store, WithClock(func() time.Time { return fixedNow }), WithEvents(pub))
			id := seed(store, tt.from, 5)

			n, err := tt.call(svc, context.Background(), id)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.Empty(t, pub.events)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantStatus, n.Status)
			require.Len(t, pub.events, 1)
			e := pub.events[0]
			require.Equal(t, id, e.NotificationID)
			require.Equal(t, tt.wantStatus, e.Status)
			require.Equal(t, &batch, e.BatchID, "batch subscribers see it too")
			require.Equal(t, n.Attempts, e.Attempts)
			require.Equal(t, n.UpdatedAt, e.At)
		})
	}
}

func TestNoEventsWithoutPublisher(t *testing.T) {
	store := newFakeStore()
	id := uuid.New()
	store.notifications[id] = domain.Notification{ID: id, Status: domain.StatusPending}
	_, err := newTestService(store).Cancel(context.Background(), id)
	require.NoError(t, err)
}
