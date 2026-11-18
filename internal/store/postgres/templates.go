package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/hkln4/go-notification-service/internal/domain"
)

// CreateTemplate stores t; a taken name returns domain.ErrNameTaken.
func (s *Store) CreateTemplate(ctx context.Context, t *domain.Template) error {
	err := s.pool.QueryRow(ctx,
		`INSERT INTO templates (id, name, channel, body) VALUES ($1, $2, $3, $4) RETURNING created_at`,
		t.ID, t.Name, t.Channel, t.Body).Scan(&t.CreatedAt)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
		return domain.ErrNameTaken
	}
	if err != nil {
		return fmt.Errorf("create template: %w", err)
	}
	return nil
}

func (s *Store) GetTemplate(ctx context.Context, id uuid.UUID) (domain.Template, error) {
	var t domain.Template
	err := s.pool.QueryRow(ctx,
		`SELECT id, name, channel, body, created_at FROM templates WHERE id = $1`, id,
	).Scan(&t.ID, &t.Name, &t.Channel, &t.Body, &t.CreatedAt)
	if isNoRows(err) {
		return domain.Template{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Template{}, fmt.Errorf("get template: %w", err)
	}
	return t, nil
}

func (s *Store) ListTemplates(ctx context.Context) ([]domain.Template, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, name, channel, body, created_at FROM templates ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list templates: %w", err)
	}
	ts, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Template, error) {
		var t domain.Template
		err := row.Scan(&t.ID, &t.Name, &t.Channel, &t.Body, &t.CreatedAt)
		return t, err
	})
	if err != nil {
		return nil, fmt.Errorf("list templates: %w", err)
	}
	return ts, nil
}
