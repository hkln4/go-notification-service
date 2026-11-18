package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/hkln4/go-notification-service/internal/domain"
	"github.com/hkln4/go-notification-service/internal/observability"
)

const notificationColumns = `id, batch_id, idempotency_key, request_hash, channel, recipient, content,
	priority, status, attempts, scheduled_at, provider_message_id, last_error,
	correlation_id, created_at, updated_at`

func scanNotification(row pgx.Row) (domain.Notification, error) {
	var n domain.Notification
	var correlationID *string
	err := row.Scan(&n.ID, &n.BatchID, &n.IdempotencyKey, &n.RequestHash, &n.Channel, &n.Recipient,
		&n.Content, &n.Priority, &n.Status, &n.Attempts, &n.ScheduledAt, &n.ProviderMessageID,
		&n.LastError, &correlationID, &n.CreatedAt, &n.UpdatedAt)
	if correlationID != nil {
		n.CorrelationID = *correlationID
	}
	return n, err
}

// InsertNotifications writes the notifications, one outbox row per
// notification and, if batch is not nil, the batch row, all in one
// transaction. This is the transactional outbox: a notification is never
// stored without the event that will deliver it, and vice versa.
//
// If any idempotency key is already taken, nothing is written and
// domain.ErrDuplicateKey is returned. A concurrent insert of the same key
// blocks on the unique index until the other transaction finishes, so this
// also catches a race that the caller's read-before-insert missed.
//
// CreatedAt and UpdatedAt are filled from the database clock.
func (s *Store) InsertNotifications(ctx context.Context, batch *domain.Batch, ns []domain.Notification) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if batch != nil {
			err := tx.QueryRow(ctx,
				`INSERT INTO batches (id, idempotency_key, request_hash, total_count)
				 VALUES ($1, $2, $3, $4)
				 ON CONFLICT (idempotency_key) DO NOTHING
				 RETURNING created_at`,
				batch.ID, batch.IdempotencyKey, batch.RequestHash, batch.TotalCount,
			).Scan(&batch.CreatedAt)
			if isNoRows(err) {
				return domain.ErrDuplicateKey
			}
			if err != nil {
				return fmt.Errorf("insert batch: %w", err)
			}
		}
		return insertNotifications(ctx, tx, ns)
	})
}

// insertNotifications queues every INSERT into a single pgx.Batch, so a
// 1000-item request costs one network round trip instead of 2000.
func insertNotifications(ctx context.Context, tx pgx.Tx, ns []domain.Notification) error {
	b := &pgx.Batch{}
	for i := range ns {
		n := &ns[i]
		b.Queue(`INSERT INTO notifications
			(id, batch_id, idempotency_key, request_hash, channel, recipient, content,
			 priority, status, scheduled_at, correlation_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			ON CONFLICT (idempotency_key) DO NOTHING
			RETURNING created_at, updated_at`,
			n.ID, n.BatchID, n.IdempotencyKey, n.RequestHash, n.Channel, n.Recipient, n.Content,
			n.Priority, n.Status, n.ScheduledAt, nullIfEmpty(n.CorrelationID),
		).QueryRow(func(row pgx.Row) error {
			err := row.Scan(&n.CreatedAt, &n.UpdatedAt)
			if isNoRows(err) {
				return domain.ErrDuplicateKey
			}
			return err
		})

		// Scheduled notifications become visible to the relay at scheduled_at;
		// everything else is available immediately.
		b.Queue(`INSERT INTO outbox (notification_id, routing_key, priority, headers, available_at)
			VALUES ($1, $2, $3, $4, COALESCE($5, now()))`,
			n.ID, string(n.Channel), n.Priority, outboxHeaders(n), n.ScheduledAt)
	}
	if err := tx.SendBatch(ctx, b).Close(); err != nil {
		return fmt.Errorf("insert notifications: %w", err)
	}
	return nil
}

// outboxHeaders become AMQP message headers in the relay, carrying the
// correlation ID across the async boundary.
func outboxHeaders(n *domain.Notification) map[string]string {
	h := map[string]string{}
	if n.CorrelationID != "" {
		h[observability.CorrelationHeaderKey] = n.CorrelationID
	}
	return h
}

func (s *Store) GetNotification(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	n, err := scanNotification(s.pool.QueryRow(ctx,
		`SELECT `+notificationColumns+` FROM notifications WHERE id = $1`, id))
	if isNoRows(err) {
		return domain.Notification{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Notification{}, fmt.Errorf("get notification: %w", err)
	}
	return n, nil
}

func (s *Store) ListAttempts(ctx context.Context, id uuid.UUID) ([]domain.Attempt, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT attempt_no, http_status, error, latency_ms, created_at
		 FROM delivery_attempts WHERE notification_id = $1 ORDER BY id`, id)
	if err != nil {
		return nil, fmt.Errorf("list attempts: %w", err)
	}
	attempts, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Attempt, error) {
		var a domain.Attempt
		var latency *int
		err := row.Scan(&a.AttemptNo, &a.HTTPStatus, &a.Error, &latency, &a.CreatedAt)
		if latency != nil {
			a.LatencyMS = *latency
		}
		return a, err
	})
	if err != nil {
		return nil, fmt.Errorf("list attempts: %w", err)
	}
	return attempts, nil
}

// Cancel moves a notification that has not been picked up yet to cancelled
// with a single conditional UPDATE, so it cannot race with a worker's claim:
// whichever statement commits first wins, the other matches zero rows.
//
// The outbox row and any queued message are left alone; the worker's claim
// skips cancelled notifications. Cancelling an already cancelled
// notification succeeds, so clients can safely retry.
func (s *Store) Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	n, err := scanNotification(s.pool.QueryRow(ctx,
		`UPDATE notifications SET status = 'cancelled', updated_at = now()
		 WHERE id = $1 AND status IN ('pending', 'scheduled', 'retrying')
		 RETURNING `+notificationColumns, id))
	if err == nil {
		return n, nil
	}
	if !isNoRows(err) {
		return domain.Notification{}, fmt.Errorf("cancel notification: %w", err)
	}

	cur, err := s.GetNotification(ctx, id)
	if err != nil {
		return domain.Notification{}, err
	}
	if cur.Status == domain.StatusCancelled {
		return cur, nil
	}
	return cur, fmt.Errorf("cannot cancel a %s notification: %w", cur.Status, domain.ErrInvalidState)
}

// FindByIdempotencyKeys returns the notifications holding any of the keys,
// indexed by key.
func (s *Store) FindByIdempotencyKeys(ctx context.Context, keys []string) (map[string]domain.Notification, error) {
	found := map[string]domain.Notification{}
	if len(keys) == 0 {
		return found, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT `+notificationColumns+` FROM notifications WHERE idempotency_key = ANY($1)`, keys)
	if err != nil {
		return nil, fmt.Errorf("find by idempotency keys: %w", err)
	}
	ns, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Notification, error) {
		return scanNotification(row)
	})
	if err != nil {
		return nil, fmt.Errorf("find by idempotency keys: %w", err)
	}
	for _, n := range ns {
		found[*n.IdempotencyKey] = n
	}
	return found, nil
}

func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// Requeue gives a failed notification a fresh set of attempts: status back
// to pending, attempts reset and a new outbox row, in one transaction. The
// copy left in the DLQ stays there as a record; if it is ever replayed, the
// claim simply treats it as one more duplicate.
func (s *Store) Requeue(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	var n domain.Notification
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		n, err = scanNotification(tx.QueryRow(ctx,
			`UPDATE notifications
			 SET status = 'pending', attempts = 0, last_error = NULL, updated_at = now()
			 WHERE id = $1 AND status = 'failed'
			 RETURNING `+notificationColumns, id))
		if isNoRows(err) {
			return errNotRequeueable
		}
		if err != nil {
			return fmt.Errorf("requeue notification: %w", err)
		}
		_, err = tx.Exec(ctx,
			`INSERT INTO outbox (notification_id, routing_key, priority, headers) VALUES ($1, $2, $3, $4)`,
			n.ID, string(n.Channel), n.Priority, outboxHeaders(&n))
		if err != nil {
			return fmt.Errorf("requeue outbox: %w", err)
		}
		return nil
	})
	if errors.Is(err, errNotRequeueable) {
		cur, err := s.GetNotification(ctx, id)
		if err != nil {
			return domain.Notification{}, err
		}
		return cur, fmt.Errorf("only failed notifications can be retried, this one is %s: %w", cur.Status, domain.ErrInvalidState)
	}
	return n, err
}

var errNotRequeueable = errors.New("not requeueable")
