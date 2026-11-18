package postgres

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/hkln4/go-notification-service/internal/domain"
)

// ListNotifications uses keyset pagination instead of OFFSET: page N costs
// the same as page 1, and rows inserted while a client pages through the
// list don't shift or duplicate entries. id breaks ties between rows created
// in the same microsecond.
func (s *Store) ListNotifications(ctx context.Context, f domain.ListFilter) ([]domain.Notification, error) {
	var where []string
	var args []any
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}

	if f.Status != nil {
		where = append(where, "status = "+arg(*f.Status))
	}
	if f.Channel != nil {
		where = append(where, "channel = "+arg(*f.Channel))
	}
	if f.From != nil {
		where = append(where, "created_at >= "+arg(*f.From))
	}
	if f.To != nil {
		where = append(where, "created_at < "+arg(*f.To))
	}
	if f.AfterCreatedAt != nil {
		// Row comparison matches the (created_at, id) index order, so the
		// planner turns it into an index range scan.
		where = append(where, "(created_at, id) < ("+arg(*f.AfterCreatedAt)+", "+arg(f.AfterID)+")")
	}

	q := `SELECT ` + notificationColumns + ` FROM notifications`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY created_at DESC, id DESC LIMIT " + arg(f.Limit)

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list notifications: %w", err)
	}
	ns, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Notification, error) {
		return scanNotification(row)
	})
	if err != nil {
		return nil, fmt.Errorf("list notifications: %w", err)
	}
	return ns, nil
}
