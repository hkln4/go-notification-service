package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type OutboxRow struct {
	ID             int64
	NotificationID uuid.UUID
	RoutingKey     string
	Priority       int16
	Headers        map[string]string
}

// ClaimOutbox leases up to limit rows that are due and not leased by another
// relay, and returns them. The statement commits on its own, so the lease is
// visible to other relays before any network I/O starts; no transaction is
// held open while publishing.
//
// FOR UPDATE SKIP LOCKED lets several relays claim concurrently without
// blocking on or double-claiming the same rows. The lease (locked_until)
// covers a relay that dies after claiming: its rows become claimable again
// when the lease runs out, so they are published late rather than never.
func (s *Store) ClaimOutbox(ctx context.Context, limit int, lease time.Duration) ([]OutboxRow, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE outbox SET locked_until = now() + make_interval(secs => $2)
		WHERE id IN (
			SELECT id FROM outbox
			WHERE available_at <= now()
			  AND (locked_until IS NULL OR locked_until < now())
			ORDER BY id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, notification_id, routing_key, priority, headers`,
		limit, lease.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim outbox: %w", err)
	}
	claimed, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (OutboxRow, error) {
		var r OutboxRow
		err := row.Scan(&r.ID, &r.NotificationID, &r.RoutingKey, &r.Priority, &r.Headers)
		return r, err
	})
	if err != nil {
		return nil, fmt.Errorf("claim outbox: %w", err)
	}
	return claimed, nil
}

// DeleteOutbox removes rows whose messages the broker has confirmed.
func (s *Store) DeleteOutbox(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM outbox WHERE id = ANY($1)`, ids); err != nil {
		return fmt.Errorf("delete outbox: %w", err)
	}
	return nil
}
