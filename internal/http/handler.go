// Package httpapi is the REST API under /api/v1.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/hkln4/go-notification-service/internal/domain"
	"github.com/hkln4/go-notification-service/internal/observability"
	"github.com/hkln4/go-notification-service/internal/service"
)

// NotificationService is what the handlers need from the service layer.
type NotificationService interface {
	Create(ctx context.Context, req service.CreateRequest) (service.CreateResult, error)
	Get(ctx context.Context, id uuid.UUID) (service.NotificationDetail, error)
	Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	CreateBatch(ctx context.Context, req service.CreateBatchRequest) (service.BatchResult, error)
	GetBatch(ctx context.Context, id uuid.UUID) (service.BatchSummary, error)
	List(ctx context.Context, q service.ListQuery) (service.Page, error)
}

type Handler struct {
	svc NotificationService
	log *slog.Logger
}

// New returns the full HTTP handler with middleware applied. Routing uses the
// standard library mux (method and wildcard patterns since Go 1.22).
func New(svc NotificationService, log *slog.Logger) http.Handler {
	h := &Handler{svc: svc, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/notifications", h.createNotification)
	mux.HandleFunc("GET /api/v1/notifications", h.listNotifications)
	mux.HandleFunc("GET /api/v1/notifications/{id}", h.getNotification)
	mux.HandleFunc("POST /api/v1/notifications/{id}/cancel", h.cancelNotification)
	mux.HandleFunc("POST /api/v1/notifications/batch", h.createBatch)
	mux.HandleFunc("GET /api/v1/batches/{id}", h.getBatch)

	// Outermost first: correlation ID must exist before anything logs.
	var handler http.Handler = mux
	handler = limitBody(handler)
	handler = recoverPanics(log, handler)
	handler = accessLog(log, handler)
	handler = correlation(handler)
	return handler
}

// IdempotencyKeyHeader is optional on both create endpoints.
const IdempotencyKeyHeader = "Idempotency-Key"

// createNotification godoc
//
//	@Summary		Create a notification
//	@Description	With an Idempotency-Key, repeating the same request returns the stored notification with 200; reusing the key with a different body returns 409.
//	@Tags			notifications
//	@Accept			json
//	@Produce		json
//	@Param			Idempotency-Key	header		string						false	"Deduplicates retries of this request"
//	@Param			body			body		createNotificationRequest	true	"Notification"
//	@Success		202				{object}	notificationResponse		"Created"
//	@Success		200				{object}	notificationResponse		"Replayed: same key and same request"
//	@Failure		400				{object}	errorBody
//	@Failure		409				{object}	errorBody	"Key reused with a different request"
//	@Router			/notifications [post]
func (h *Handler) createNotification(w http.ResponseWriter, r *http.Request) {
	var req createNotificationRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	res, err := h.svc.Create(r.Context(), service.CreateRequest{
		Input:          req.input(),
		CorrelationID:  observability.CorrelationID(r.Context()),
		IdempotencyKey: r.Header.Get(IdempotencyKeyHeader),
	})
	if err != nil {
		writeServiceError(r.Context(), w, h.log, err)
		return
	}
	status := http.StatusAccepted
	if res.Replayed {
		status = http.StatusOK
	}
	w.Header().Set("Location", "/api/v1/notifications/"+res.Notification.ID.String())
	writeJSON(w, status, toNotificationResponse(res.Notification))
}

// getNotification godoc
//
//	@Summary	Get a notification with its delivery attempts
//	@Tags		notifications
//	@Produce	json
//	@Param		id	path		string	true	"Notification ID"
//	@Success	200	{object}	notificationDetailResponse
//	@Failure	404	{object}	errorBody
//	@Router		/notifications/{id} [get]
func (h *Handler) getNotification(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	d, err := h.svc.Get(r.Context(), id)
	if err != nil {
		writeServiceError(r.Context(), w, h.log, err)
		return
	}
	writeJSON(w, http.StatusOK, toDetailResponse(d))
}

// cancelNotification godoc
//
//	@Summary		Cancel a notification
//	@Description	Only pending, scheduled or retrying notifications can be cancelled. Cancelling twice succeeds.
//	@Tags			notifications
//	@Produce		json
//	@Param			id	path		string	true	"Notification ID"
//	@Success		200	{object}	notificationResponse
//	@Failure		404	{object}	errorBody
//	@Failure		409	{object}	errorBody
//	@Router			/notifications/{id}/cancel [post]
func (h *Handler) cancelNotification(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	n, err := h.svc.Cancel(r.Context(), id)
	if err != nil {
		writeServiceError(r.Context(), w, h.log, err)
		return
	}
	writeJSON(w, http.StatusOK, toNotificationResponse(n))
}

// createBatch godoc
//
//	@Summary		Create up to 1000 notifications at once
//	@Description	All items are validated first; if any is invalid the whole batch is rejected and details lists each bad item by index. Valid batches are stored in one transaction. Items may carry their own idempotencyKey: an item that already exists with the same request is skipped and listed in duplicates; one that exists with a different request fails the batch with 409.
//	@Tags			batches
//	@Accept			json
//	@Produce		json
//	@Param			Idempotency-Key	header		string				false	"Deduplicates retries of the whole batch"
//	@Param			body			body		createBatchRequest	true	"Batch"
//	@Success		202				{object}	createBatchResponse	"Created"
//	@Success		200				{object}	createBatchResponse	"Replayed: same key and same batch"
//	@Failure		400				{object}	errorBody
//	@Failure		409				{object}	errorBody
//	@Router			/notifications/batch [post]
func (h *Handler) createBatch(w http.ResponseWriter, r *http.Request) {
	var req createBatchRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	items := make([]service.BatchItem, len(req.Notifications))
	for i, n := range req.Notifications {
		items[i] = service.BatchItem{Input: n.input(), IdempotencyKey: n.IdempotencyKey}
	}
	res, err := h.svc.CreateBatch(r.Context(), service.CreateBatchRequest{
		Items:          items,
		CorrelationID:  observability.CorrelationID(r.Context()),
		IdempotencyKey: r.Header.Get(IdempotencyKeyHeader),
	})
	if err != nil {
		writeServiceError(r.Context(), w, h.log, err)
		return
	}
	status := http.StatusAccepted
	if res.Replayed {
		status = http.StatusOK
	}
	w.Header().Set("Location", "/api/v1/batches/"+res.Batch.ID.String())
	writeJSON(w, status, toCreateBatchResponse(res))
}

// getBatch godoc
//
//	@Summary	Get a batch with notification counts per status
//	@Tags		batches
//	@Produce	json
//	@Param		id	path		string	true	"Batch ID"
//	@Success	200	{object}	batchResponse
//	@Failure	404	{object}	errorBody
//	@Router		/batches/{id} [get]
func (h *Handler) getBatch(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	b, err := h.svc.GetBatch(r.Context(), id)
	if err != nil {
		writeServiceError(r.Context(), w, h.log, err)
		return
	}
	writeJSON(w, http.StatusOK, toBatchResponse(b))
}

// listNotifications godoc
//
//	@Summary		List notifications, newest first
//	@Description	Keyset pagination: pass nextCursor from the previous page as cursor. from is inclusive, to is exclusive.
//	@Tags			notifications
//	@Produce		json
//	@Param			status	query		string	false	"Filter by status"	Enums(pending, scheduled, processing, retrying, sent, failed, cancelled)
//	@Param			channel	query		string	false	"Filter by channel"	Enums(sms, email, push)
//	@Param			from	query		string	false	"Created at or after (RFC 3339)"
//	@Param			to		query		string	false	"Created before (RFC 3339)"
//	@Param			cursor	query		string	false	"Cursor from the previous page"
//	@Param			limit	query		int		false	"Page size, 1-200 (default 50)"
//	@Success		200		{object}	listResponse
//	@Failure		400		{object}	errorBody
//	@Router			/notifications [get]
func (h *Handler) listNotifications(w http.ResponseWriter, r *http.Request) {
	q, ferr := parseListQuery(r.URL.Query())
	if ferr != nil {
		writeServiceError(r.Context(), w, h.log, ferr)
		return
	}
	page, err := h.svc.List(r.Context(), q)
	if err != nil {
		writeServiceError(r.Context(), w, h.log, err)
		return
	}
	writeJSON(w, http.StatusOK, toListResponse(page))
}

// parseListQuery only handles syntax (numbers, timestamps); the service
// validates meaning.
func parseListQuery(v url.Values) (service.ListQuery, error) {
	q := service.ListQuery{Status: v.Get("status"), Channel: v.Get("channel"), Cursor: v.Get("cursor")}
	var verr domain.ValidationError
	for _, p := range []struct {
		name string
		dst  **time.Time
	}{{"from", &q.From}, {"to", &q.To}} {
		if s := v.Get(p.name); s != "" {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				verr.Fields = append(verr.Fields, domain.FieldError{Field: p.name, Message: "must be an RFC 3339 timestamp"})
				continue
			}
			*p.dst = &t
		}
	}
	if s := v.Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			verr.Fields = append(verr.Fields, domain.FieldError{Field: "limit", Message: "must be an integer"})
		}
		q.Limit = n
	}
	if len(verr.Fields) > 0 {
		return service.ListQuery{}, &verr
	}
	return q, nil
}

func pathID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidID, "id must be a UUID", nil)
		return uuid.UUID{}, false
	}
	return id, true
}
