package service

import (
	"context"
	"errors"
	"strconv"

	"github.com/google/uuid"

	"github.com/hkln4/go-notification-service/internal/domain"
)

const MaxBatchSize = 1000

type BatchItem struct {
	Input domain.NotificationInput
	// IdempotencyKey is optional and independent of the batch's own key.
	IdempotencyKey string
}

type CreateBatchRequest struct {
	Items         []BatchItem
	CorrelationID string
	// IdempotencyKey is optional; it identifies the whole batch request.
	IdempotencyKey string
}

type BatchResult struct {
	Batch domain.Batch
	// NotificationIDs follows the request order. For an item whose key
	// already existed with the same request it is the existing ID.
	NotificationIDs []uuid.UUID
	// Duplicates are the indexes of items that already existed and were
	// not created again.
	Duplicates []int
	// Replayed is set when the batch key matched an earlier identical batch;
	// NotificationIDs then lists that batch's own notifications.
	Replayed bool
	// CreatedChannels has the resolved channel of every notification this
	// call created, for metrics. A template item may leave its channel empty
	// in the request; this has the template's. Duplicates and replays add
	// nothing: they were counted when first created.
	CreatedChannels []domain.Channel
}

// hashedItem is what the batch request hash covers: every validated item and
// its key, in order.
type hashedItem struct {
	Key          string                 `json:"key,omitempty"`
	Notification domain.NewNotification `json:"notification"`
}

// CreateBatch validates every item first and rejects the whole batch if any
// item is invalid, reporting each bad item by index. Valid batches are
// stored in a single transaction: all items or none.
//
// Item keys: an item whose key exists with the same request is skipped and
// reported in Duplicates; one whose key exists with a different request
// fails the whole batch with *domain.IdempotencyConflictError.
func (s *Service) CreateBatch(ctx context.Context, req CreateBatchRequest) (BatchResult, error) {
	if len(req.Items) == 0 || len(req.Items) > MaxBatchSize {
		return BatchResult{}, &domain.ValidationError{Fields: []domain.FieldError{{
			Field:   "notifications",
			Message: "must contain between 1 and " + strconv.Itoa(MaxBatchSize) + " items",
		}}}
	}
	if req.IdempotencyKey != "" && !validKey(req.IdempotencyKey) {
		return BatchResult{}, keyError("Idempotency-Key", nil)
	}

	hashed, err := s.validateItems(ctx, req.Items)
	if err != nil {
		return BatchResult{}, err
	}
	batchHash := requestHash(hashed)

	for range 2 {
		res, err := s.createBatchOnce(ctx, req, hashed, batchHash)
		if errors.Is(err, domain.ErrDuplicateKey) {
			continue // lost a race on some key; the re-read will see it
		}
		return res, err
	}
	return BatchResult{}, domain.ErrDuplicateKey
}

func (s *Service) validateItems(ctx context.Context, items []BatchItem) ([]hashedItem, error) {
	hashed := make([]hashedItem, len(items))
	seenKeys := map[string]int{}
	cache := templateCache{} // a batch usually uses one template many times
	var verr domain.ValidationError
	for i, item := range items {
		nn, err := s.prepare(ctx, item.Input, cache)
		var itemErr *domain.ValidationError
		if errors.As(err, &itemErr) {
			verr.Fields = append(verr.Fields, withIndex(i, itemErr.Fields)...)
		} else if err != nil {
			return nil, err
		}

		if k := item.IdempotencyKey; k != "" {
			switch first, dup := seenKeys[k]; {
			case !validKey(k):
				verr.Fields = append(verr.Fields, keyError("idempotencyKey", ptr(i)).Fields...)
			case dup:
				verr.Fields = append(verr.Fields, domain.FieldError{
					Index: ptr(i), Field: "idempotencyKey",
					Message: "duplicates the key of item " + strconv.Itoa(first),
				})
			default:
				seenKeys[k] = i
			}
		}
		hashed[i] = hashedItem{Key: item.IdempotencyKey, Notification: nn}
	}
	if len(verr.Fields) > 0 {
		return nil, &verr
	}
	return hashed, nil
}

func (s *Service) createBatchOnce(ctx context.Context, req CreateBatchRequest, hashed []hashedItem, batchHash string) (BatchResult, error) {
	if req.IdempotencyKey != "" {
		prev, ids, err := s.store.FindBatchByIdempotencyKey(ctx, req.IdempotencyKey)
		switch {
		case err == nil:
			if *prev.RequestHash != batchHash {
				return BatchResult{}, domain.ErrIdempotencyMismatch
			}
			return BatchResult{Batch: prev, NotificationIDs: ids, Replayed: true}, nil
		case !errors.Is(err, domain.ErrNotFound):
			return BatchResult{}, err
		}
	}

	var keys []string
	for _, h := range hashed {
		if h.Key != "" {
			keys = append(keys, h.Key)
		}
	}
	existing, err := s.store.FindByIdempotencyKeys(ctx, keys)
	if err != nil {
		return BatchResult{}, err
	}

	batch := domain.Batch{ID: s.newID()}
	if req.IdempotencyKey != "" {
		batch.IdempotencyKey, batch.RequestHash = &req.IdempotencyKey, &batchHash
	}
	res := BatchResult{NotificationIDs: make([]uuid.UUID, len(hashed))}
	var conflicts []domain.FieldError
	var ns []domain.Notification
	for i, h := range hashed {
		itemHash := requestHash(h.Notification)
		if prev, ok := existing[h.Key]; ok {
			if *prev.RequestHash != itemHash {
				conflicts = append(conflicts, domain.FieldError{
					Index: ptr(i), Field: "idempotencyKey", Message: "already used with a different request",
				})
				continue
			}
			res.NotificationIDs[i] = prev.ID
			res.Duplicates = append(res.Duplicates, i)
			continue
		}
		n := s.build(h.Notification, req.CorrelationID, &batch.ID)
		if h.Key != "" {
			n.IdempotencyKey, n.RequestHash = ptr(h.Key), ptr(itemHash)
		}
		res.NotificationIDs[i] = n.ID
		ns = append(ns, n)
	}
	if len(conflicts) > 0 {
		return BatchResult{}, &domain.IdempotencyConflictError{Fields: conflicts}
	}

	batch.TotalCount = len(ns)
	if err := s.store.InsertNotifications(ctx, &batch, ns); err != nil {
		return BatchResult{}, err
	}
	res.Batch = batch
	for _, n := range ns {
		res.CreatedChannels = append(res.CreatedChannels, n.Channel)
	}
	return res, nil
}

func withIndex(i int, fields []domain.FieldError) []domain.FieldError {
	for j := range fields {
		fields[j].Index = ptr(i)
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
