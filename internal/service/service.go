// Package service holds the API use cases. It validates input, decides the
// initial status and delegates persistence to a Store.
package service

import (
	"context"
	"errors"
	"log/slog"
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
	CreateTemplate(ctx context.Context, t *domain.Template) error
	GetTemplate(ctx context.Context, id uuid.UUID) (domain.Template, error)
	ListTemplates(ctx context.Context) ([]domain.Template, error)
}

// EventPublisher announces status changes made by the API (cancel, retry)
// to live listeners. Workers announce their own changes.
type EventPublisher interface {
	PublishStatus(ctx context.Context, e domain.StatusEvent) error
}

type Service struct {
	store  Store
	events EventPublisher // nil: no events
	now    func() time.Time
	newID  func() uuid.UUID
}

type Option func(*Service)

// WithClock injects the clock used for scheduling checks.
func WithClock(now func() time.Time) Option { return func(s *Service) { s.now = now } }

// WithIDs injects the ID generator.
func WithIDs(newID func() uuid.UUID) Option { return func(s *Service) { s.newID = newID } }

// WithEvents publishes the status changes the API makes, so WebSocket
// clients see a cancel or a retry, not only what the workers do.
func WithEvents(p EventPublisher) Option { return func(s *Service) { s.events = p } }

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
	nn, err := s.prepare(ctx, req.Input, templateCache{})
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
		TemplateID:    nn.TemplateID,
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

// Cancel stops a notification that has not been picked up yet. Cancelling an
// already cancelled notification succeeds and announces "cancelled" again;
// listeners get a duplicate event, which is harmless.
func (s *Service) Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	n, err := s.store.Cancel(ctx, id)
	if err != nil {
		return n, err
	}
	s.announce(ctx, n)
	return n, nil
}

// Retry sends a failed (dead-lettered) notification again with a fresh set
// of attempts.
func (s *Service) Retry(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	n, err := s.store.Requeue(ctx, id)
	if err != nil {
		return n, err
	}
	s.announce(ctx, n)
	return n, nil
}

// announce publishes n's new status after the change is committed. It is
// best effort, like the worker's events: the database already holds the
// truth, so a failure is logged and never fails the request. The publish
// gets its own short deadline and survives the client hanging up.
func (s *Service) announce(ctx context.Context, n domain.Notification) {
	if s.events == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	err := s.events.PublishStatus(ctx, domain.StatusEvent{
		NotificationID: n.ID,
		BatchID:        n.BatchID,
		Status:         n.Status,
		Attempts:       n.Attempts,
		// The database's time of the change, as in the WebSocket snapshot. A
		// repeated cancel thus carries the original cancel time.
		At: n.UpdatedAt,
	})
	if err != nil {
		slog.WarnContext(ctx, "status event not published", "notification_id", n.ID, "status", n.Status, "err", err)
	}
}
