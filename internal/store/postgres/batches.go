package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/hkln4/go-notification-service/internal/domain"
)

// GetBatch returns the batch and how many of its notifications are in each
// status. Statuses with no notifications are absent from the map.
func (s *Store) GetBatch(ctx context.Context, id uuid.UUID) (domain.Batch, map[domain.Status]int, error) {
	var b domain.Batch
	err := s.pool.QueryRow(ctx,
		`SELECT id, idempotency_key, request_hash, total_count, created_at FROM batches WHERE id = $1`, id,
	).Scan(&b.ID, &b.IdempotencyKey, &b.RequestHash, &b.TotalCount, &b.CreatedAt)
	if isNoRows(err) {
		return domain.Batch{}, nil, domain.ErrNotFound
	}
	if err != nil {
		return domain.Batch{}, nil, fmt.Errorf("get batch: %w", err)
	}

	rows, err := s.pool.Query(ctx,
		`SELECT status, count(*) FROM notifications WHERE batch_id = $1 GROUP BY status`, id)
	if err != nil {
		return domain.Batch{}, nil, fmt.Errorf("count batch statuses: %w", err)
	}
	defer rows.Close()

	counts := map[domain.Status]int{}
	for rows.Next() {
		var st domain.Status
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return domain.Batch{}, nil, fmt.Errorf("count batch statuses: %w", err)
		}
		counts[st] = n
	}
	if err := rows.Err(); err != nil {
		return domain.Batch{}, nil, fmt.Errorf("count batch statuses: %w", err)
	}
	return b, counts, nil
}
