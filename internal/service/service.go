// Package service holds the API use cases. It validates input, decides the
// initial status and delegates persistence to a Store.
package service

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/hkln4/go-notification-service/internal/domain"
)

// Store is the persistence the service needs. It is defined here, where it is
// used, and implemented by store/postgres.
type Store interface {
	InsertNotifications(ctx context.Context, batch *domain.Batch, ns []domain.Notification) error
	GetNotification(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	ListAttempts(ctx context.Context, id uuid.UUID) ([]domain.Attempt, error)
	Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	GetBatch(ctx context.Context, id uuid.UUID) (domain.Batch, map[domain.Status]int, error)
	ListNotifications(ctx context.Context, f domain.ListFilter) ([]domain.Notification, error)
	FindByIdempotencyKeys(ctx context.Context, keys []string) (map[string]domain.Notification, error)
	FindBatchByIdempotencyKey(ctx context.Context, key string) (domain.Batch, []uuid.UUID, error)
	Requeue(ctx context.Context, id uuid.UUID) (domain.Notification, error)
}

type Service struct {
	store Store
	now   func() time.Time
	newID func() uuid.UUID
}

type Option func(*Service)

// WithClock injects the clock used for scheduling checks.
func WithClock(now func() time.Time) Option { return func(s *Service) { s.now = now } }

// WithIDs injects the ID generator.
func WithIDs(newID func() uuid.UUID) Option { return func(s *Service) { s.newID = newID } }

func New(store Store, opts ...Option) *Service {
	s := &Service{store: store, now: time.Now, newID: newUUIDv7}
	for _, o := range opts {
		o(s)
	}
	return s
}

// newUUIDv7 returns a time-ordered UUID. Consecutive IDs land next to each
// other in the primary key B-tree instead of on random pages (as UUIDv4
// would), which keeps inserts cheap under bursts.
func newUUIDv7() uuid.UUID {
	return uuid.Must(uuid.NewV7())
}

type CreateRequest struct {
	Input         domain.NotificationInput
	CorrelationID string
	// IdempotencyKey is optional. Without it a retried request creates a
	// second notification.
	IdempotencyKey string
}

// CreateResult says whether the notification was created by this call or
// returned from an earlier call with the same idempotency key.
type CreateResult struct {
	Notification domain.Notification
	Replayed     bool
}

// Create validates the request and stores the notification together with its
// outbox row. It never talks to the broker; the relay does that.
//
// With an idempotency key: same key and same request returns the stored
// notification (Replayed); same key and a different request returns
// domain.ErrIdempotencyMismatch.
func (s *Service) Create(ctx context.Context, req CreateRequest) (CreateResult, error) {
	nn, err := req.Input.Validate(s.now())
	if err != nil {
		return CreateResult{}, err
	}
	n := s.build(nn, req.CorrelationID, nil)
	if req.IdempotencyKey == "" {
		ns := []domain.Notification{n}
		err := s.store.InsertNotifications(ctx, nil, ns)
		return CreateResult{Notification: ns[0]}, err
	}
	if !validKey(req.IdempotencyKey) {
		return CreateResult{}, keyError("Idempotency-Key", nil)
	}
	n.IdempotencyKey, n.RequestHash = &req.IdempotencyKey, ptr(requestHash(nn))

	// Read first; if the insert then loses a race to a concurrent request
	// with the same key, read again: the winner has committed by then.
	for range 2 {
		found, err := s.store.FindByIdempotencyKeys(ctx, []string{req.IdempotencyKey})
		if err != nil {
			return CreateResult{}, err
		}
		if prev, ok := found[req.IdempotencyKey]; ok {
			if *prev.RequestHash != *n.RequestHash {
				return CreateResult{}, domain.ErrIdempotencyMismatch
			}
			return CreateResult{Notification: prev, Replayed: true}, nil
		}
		ns := []domain.Notification{n}
		err = s.store.InsertNotifications(ctx, nil, ns)
		if errors.Is(err, domain.ErrDuplicateKey) {
			continue
		}
		return CreateResult{Notification: ns[0]}, err
	}
	return CreateResult{}, domain.ErrDuplicateKey
}

func (s *Service) build(nn domain.NewNotification, correlationID string, batchID *uuid.UUID) domain.Notification {
	status := domain.StatusPending
	if nn.ScheduledAt != nil {
		status = domain.StatusScheduled
	}
	return domain.Notification{
		ID:            s.newID(),
		BatchID:       batchID,
		Channel:       nn.Channel,
		Recipient:     nn.Recipient,
		Content:       nn.Content,
		Priority:      nn.Priority,
		Status:        status,
		ScheduledAt:   nn.ScheduledAt,
		CorrelationID: correlationID,
	}
}

type NotificationDetail struct {
	domain.Notification
	Attempts []domain.Attempt
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (NotificationDetail, error) {
	n, err := s.store.GetNotification(ctx, id)
	if err != nil {
		return NotificationDetail{}, err
	}
	attempts, err := s.store.ListAttempts(ctx, id)
	if err != nil {
		return NotificationDetail{}, err
	}
	return NotificationDetail{Notification: n, Attempts: attempts}, nil
}

func (s *Service) Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	return s.store.Cancel(ctx, id)
}

// Retry sends a failed (dead-lettered) notification again with a fresh set
// of attempts.
func (s *Service) Retry(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	return s.store.Requeue(ctx, id)
}
