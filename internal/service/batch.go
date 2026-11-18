package service

import (
	"context"
	"errors"
	"strconv"

	"github.com/google/uuid"

	"github.com/hkln4/go-notification-service/internal/domain"
)

const MaxBatchSize = 1000

type CreateBatchRequest struct {
	Items         []domain.NotificationInput
	CorrelationID string
}

// CreateBatch validates every item first and rejects the whole batch if any
// item is invalid, reporting each bad item by index. Valid batches are
// stored in a single transaction: all items or none.
func (s *Service) CreateBatch(ctx context.Context, req CreateBatchRequest) (domain.Batch, []domain.Notification, error) {
	if len(req.Items) == 0 || len(req.Items) > MaxBatchSize {
		return domain.Batch{}, nil, &domain.ValidationError{Fields: []domain.FieldError{{
			Field:   "notifications",
			Message: "must contain between 1 and " + strconv.Itoa(MaxBatchSize) + " items",
		}}}
	}

	now := s.now()
	valid := make([]domain.NewNotification, len(req.Items))
	var verr domain.ValidationError
	for i, in := range req.Items {
		nn, err := in.Validate(now)
		var itemErr *domain.ValidationError
		if errors.As(err, &itemErr) {
			verr.Fields = append(verr.Fields, withIndex(i, itemErr.Fields)...)
			continue
		}
		if err != nil {
			return domain.Batch{}, nil, err
		}
		valid[i] = nn
	}
	if len(verr.Fields) > 0 {
		return domain.Batch{}, nil, &verr
	}

	batch := domain.Batch{ID: s.newID(), TotalCount: len(valid)}
	ns := make([]domain.Notification, len(valid))
	for i, nn := range valid {
		ns[i] = s.build(nn, req.CorrelationID, &batch.ID)
	}
	if err := s.store.InsertNotifications(ctx, &batch, ns); err != nil {
		return domain.Batch{}, nil, err
	}
	return batch, ns, nil
}

func withIndex(i int, fields []domain.FieldError) []domain.FieldError {
	for j := range fields {
		idx := i
		fields[j].Index = &idx
	}
	return fields
}

type BatchSummary struct {
	domain.Batch
	StatusCounts map[domain.Status]int
}

func (s *Service) GetBatch(ctx context.Context, id uuid.UUID) (BatchSummary, error) {
	b, counts, err := s.store.GetBatch(ctx, id)
	if err != nil {
		return BatchSummary{}, err
	}
	return BatchSummary{Batch: b, StatusCounts: counts}, nil
}
