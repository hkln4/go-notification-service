package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/hkln4/go-notification-service/internal/domain"
)

// Claim takes the delivery lease on a notification for one worker. It is a
// single conditional UPDATE, so of two workers holding the same message (a
// redelivery or a duplicate publish) exactly one gets the row; the other
// matches zero rows. The same goes for a claim racing a cancel.
//
// A processing row whose lease has expired can be claimed again: its worker
// is presumed dead, and without this the notification would be stuck.
//
// ok is false when the row was not claimable; LeaseState says why.
func (s *Store) Claim(ctx context.Context, id, lease uuid.UUID, leaseFor time.Duration) (n domain.Notification, ok bool, err error) {
	n, err = scanNotification(s.pool.QueryRow(ctx, `
		UPDATE notifications
		SET status = 'processing', attempts = attempts + 1,
		    lease_token = $2, locked_until = now() + make_interval(secs => $3), updated_at = now()
		WHERE id = $1
		  AND ( status IN ('pending', 'scheduled', 'retrying')
		        OR (status = 'processing' AND locked_until < now()) )
		RETURNING `+notificationColumns,
		id, lease, leaseFor.Seconds()))
	if isNoRows(err) {
		return domain.Notification{}, false, nil
	}
	if err != nil {
		return domain.Notification{}, false, fmt.Errorf("claim notification: %w", err)
	}
	return n, true, nil
}

// LeaseState explains a failed claim: the current status and, for a
// processing notification, whether another worker's lease is still valid.
func (s *Store) LeaseState(ctx context.Context, id uuid.UUID) (domain.Status, bool, error) {
	var st domain.Status
	var leased bool
	err := s.pool.QueryRow(ctx,
		`SELECT status, COALESCE(locked_until > now(), false) FROM notifications WHERE id = $1`, id,
	).Scan(&st, &leased)
	if isNoRows(err) {
		return "", false, domain.ErrNotFound
	}
	if err != nil {
		return "", false, fmt.Errorf("lease state: %w", err)
	}
	return st, leased, nil
}

// Outcome is the result of one delivery attempt.
type Outcome struct {
	ID    uuid.UUID
	Lease uuid.UUID
	// Status is sent, retrying or failed.
	Status            domain.Status
	ProviderMessageID string
	LastError         string
	Attempt           AttemptResult
}

// AttemptResult is what one provider call returned. Its number is assigned
// by CompleteAttempt.
type AttemptResult struct {
	HTTPStatus *int
	Error      *string
	LatencyMS  int
}

// CompleteAttempt records the attempt and moves the notification out of
// processing, in one transaction.
//
// The status update is fenced by the lease token: it only applies if this
// worker still holds the lease. If the lease expired and another worker
// claimed the notification meanwhile, the update matches zero rows and
// applied is false, so a slow worker can't overwrite a newer outcome. The
// attempt row is written either way; the call did happen.
//
// attempt_no numbers every call over the notification's whole history. It
// is not notifications.attempts, which counts the current round and starts
// over when a failed notification is requeued: after five failures and a
// requeue the next call is attempt_no 6, attempts 1.
func (s *Store) CompleteAttempt(ctx context.Context, o Outcome) (applied bool, err error) {
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		// Lock the notification first so two completions for it (the rare
		// case of a worker reporting after losing its lease) are numbered
		// one after the other. The fenced UPDATE below would not do: for the
		// loser it matches no row, so it never waits for the lock. Under
		// READ COMMITTED the INSERT after the wait sees the other's row.
		if _, err := tx.Exec(ctx, `SELECT 1 FROM notifications WHERE id = $1 FOR UPDATE`, o.ID); err != nil {
			return fmt.Errorf("lock notification: %w", err)
		}
		a := o.Attempt
		if _, err := tx.Exec(ctx,
			`INSERT INTO delivery_attempts (notification_id, attempt_no, http_status, error, latency_ms)
			 VALUES ($1,
			         (SELECT COALESCE(max(attempt_no), 0) + 1 FROM delivery_attempts WHERE notification_id = $1),
			         $2, $3, $4)`,
			o.ID, a.HTTPStatus, a.Error, a.LatencyMS); err != nil {
			return fmt.Errorf("insert attempt: %w", err)
		}
		tag, err := tx.Exec(ctx, `
			UPDATE notifications
			SET status = $3,
			    provider_message_id = COALESCE($4, provider_message_id),
			    last_error = $5,
			    lease_token = NULL, locked_until = NULL, updated_at = now()
			WHERE id = $1 AND lease_token = $2`,
			o.ID, o.Lease, o.Status, nullIfEmpty(o.ProviderMessageID), nullIfEmpty(o.LastError))
		if err != nil {
			return fmt.Errorf("complete notification: %w", err)
		}
		applied = tag.RowsAffected() == 1
		return nil
	})
	return applied, err
}
