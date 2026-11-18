// Package httpapi is the REST API under /api/v1.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/hkln4/go-notification-service/internal/domain"
	"github.com/hkln4/go-notification-service/internal/observability"
	"github.com/hkln4/go-notification-service/internal/service"
)

// NotificationService is what the handlers need from the service layer.
type NotificationService interface {
	Create(ctx context.Context, req service.CreateRequest) (domain.Notification, error)
	Get(ctx context.Context, id uuid.UUID) (service.NotificationDetail, error)
	Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	CreateBatch(ctx context.Context, req service.CreateBatchRequest) (domain.Batch, []domain.Notification, error)
	GetBatch(ctx context.Context, id uuid.UUID) (service.BatchSummary, error)
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

// createNotification godoc
//
//	@Summary	Create a notification
//	@Tags		notifications
//	@Accept		json
//	@Produce	json
//	@Param		body	body		createNotificationRequest	true	"Notification"
//	@Success	202		{object}	notificationResponse
//	@Failure	400		{object}	errorBody
//	@Router		/notifications [post]
func (h *Handler) createNotification(w http.ResponseWriter, r *http.Request) {
	var req createNotificationRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	n, err := h.svc.Create(r.Context(), service.CreateRequest{
		Input:         req.input(),
		CorrelationID: observability.CorrelationID(r.Context()),
	})
	if err != nil {
		writeServiceError(r.Context(), w, h.log, err)
		return
	}
	w.Header().Set("Location", "/api/v1/notifications/"+n.ID.String())
	writeJSON(w, http.StatusAccepted, toNotificationResponse(n))
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
//	@Description	All items are validated first; if any is invalid the whole batch is rejected and details lists each bad item by index. Valid batches are stored in one transaction.
//	@Tags			batches
//	@Accept			json
//	@Produce		json
//	@Param			body	body		createBatchRequest	true	"Batch"
//	@Success		202		{object}	createBatchResponse
//	@Failure		400		{object}	errorBody
//	@Router			/notifications/batch [post]
func (h *Handler) createBatch(w http.ResponseWriter, r *http.Request) {
	var req createBatchRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	items := make([]domain.NotificationInput, len(req.Notifications))
	for i, n := range req.Notifications {
		items[i] = n.input()
	}
	b, ns, err := h.svc.CreateBatch(r.Context(), service.CreateBatchRequest{
		Items:         items,
		CorrelationID: observability.CorrelationID(r.Context()),
	})
	if err != nil {
		writeServiceError(r.Context(), w, h.log, err)
		return
	}
	w.Header().Set("Location", "/api/v1/batches/"+b.ID.String())
	writeJSON(w, http.StatusAccepted, toCreateBatchResponse(b, ns))
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

func pathID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidID, "id must be a UUID", nil)
		return uuid.UUID{}, false
	}
	return id, true
}
