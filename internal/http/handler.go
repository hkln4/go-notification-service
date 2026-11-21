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
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

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
	Retry(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	CreateTemplate(ctx context.Context, in service.TemplateInput) (domain.Template, error)
	GetTemplate(ctx context.Context, id uuid.UUID) (domain.Template, error)
	ListTemplates(ctx context.Context) ([]domain.Template, error)
}

type Handler struct {
	svc     NotificationService
	log     *slog.Logger
	metrics *observability.Metrics
}

// Options are optional collaborators; the zero value serves only the API.
type Options struct {
	Metrics *observability.Metrics
	// Ready answers /ready; nil means always ready.
	Ready http.Handler
	// Backpressure rejects new notifications while the backlog is too
	// long; the zero value disables it.
	Backpressure Backpressure
	// Swagger serves the API docs UI under /swagger/; nil disables it.
	Swagger http.Handler
	// WebSocket serves live status updates at /api/v1/ws; nil disables it.
	WebSocket http.Handler
}

// New returns the full HTTP handler with middleware applied. Routing uses the
// standard library mux (method and wildcard patterns since Go 1.22).
func New(svc NotificationService, log *slog.Logger, opts Options) http.Handler {
	h := &Handler{svc: svc, log: log, metrics: opts.Metrics}
	if opts.Ready == nil {
		opts.Ready = observability.ReadyHandler(time.Second)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", opts.Metrics.Handler())
	mux.Handle("GET /health", observability.HealthHandler())
	mux.Handle("GET /ready", opts.Ready)
	if opts.Swagger != nil {
		mux.Handle("GET /swagger/", opts.Swagger)
	}
	if opts.WebSocket != nil {
		// Documented in the README; swag can't describe a WebSocket.
		mux.Handle("GET /api/v1/ws", opts.WebSocket)
	}
	mux.Handle("POST /api/v1/notifications", opts.Backpressure.wrap(http.HandlerFunc(h.createNotification)))
	mux.HandleFunc("GET /api/v1/notifications", h.listNotifications)
	mux.HandleFunc("GET /api/v1/notifications/{id}", h.getNotification)
	mux.HandleFunc("POST /api/v1/notifications/{id}/cancel", h.cancelNotification)
	mux.HandleFunc("POST /api/v1/notifications/{id}/retry", h.retryNotification)
	mux.Handle("POST "+batchPath, opts.Backpressure.wrap(http.HandlerFunc(h.createBatch)))
	mux.HandleFunc("GET /api/v1/batches/{id}", h.getBatch)
	mux.HandleFunc("POST /api/v1/templates", h.createTemplate)
	mux.HandleFunc("GET /api/v1/templates", h.listTemplates)
	mux.HandleFunc("GET /api/v1/templates/{id}", h.getTemplate)

	// Outermost first: correlation ID must exist before anything logs.
	var handler http.Handler = mux
	handler = limitBody(handler)
	handler = recoverPanics(log, handler)
	handler = accessLog(log, opts.Metrics, handler)
	handler = correlation(handler)
	// Outermost, so every other layer runs inside the request span. Trace
	// context from an incoming traceparent header is continued.
	handler = otelhttp.NewHandler(handler, "http.server")
	return handler
}

// IdempotencyKeyHeader is optional on both create endpoints.
const IdempotencyKeyHeader = "Idempotency-Key"

// createNotification godoc
//
//	@Summary		Create a notification
//	@Description	With an Idempotency-Key, repeating the same request returns the stored notification with 200; reusing the key with a different body returns 409. Swagger UI pre-fills every optional field: to send plain content right away, delete scheduledAt, templateId and variables from the example (templateId and variables replace content).
//	@Tags			notifications
//	@Accept			json
//	@Produce		json
//	@Param			Idempotency-Key	header		string						false	"Deduplicates retries of this request"
//	@Param			body			body		createNotificationRequest	true	"Notification"
//	@Success		202				{object}	notificationResponse		"Created"
//	@Success		200				{object}	notificationResponse		"Replayed: same key and same request"
//	@Failure		400				{object}	errorBody
//	@Failure		409				{object}	errorBody	"Key reused with a different request"
//	@Failure		413				{object}	errorBody	"Body over 1 MB (the batch endpoint allows 16 MB)"
//	@Failure		429				{object}	errorBody	"Backpressure: too many notifications waiting to be sent"
//	@Header			429				{integer}	Retry-After	"Seconds to wait before retrying"
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
	} else {
		h.metrics.Created(string(res.Notification.Channel))
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

// retryNotification godoc
//
//	@Summary		Retry a failed notification
//	@Description	Gives a failed (dead-lettered) notification a fresh set of attempts and queues it again. Only failed notifications can be retried.
//	@Tags			notifications
//	@Produce		json
//	@Param			id	path		string	true	"Notification ID"
//	@Success		202	{object}	notificationResponse
//	@Failure		404	{object}	errorBody
//	@Failure		409	{object}	errorBody
//	@Router			/notifications/{id}/retry [post]
func (h *Handler) retryNotification(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	n, err := h.svc.Retry(r.Context(), id)
	if err != nil {
		writeServiceError(r.Context(), w, h.log, err)
		return
	}
	writeJSON(w, http.StatusAccepted, toNotificationResponse(n))
}

// createBatch godoc
//
//	@Summary		Create up to 1000 notifications at once
//	@Description	All items are validated first; if any is invalid the whole batch is rejected and details lists each bad item by index. Valid batches are stored in one transaction. Items may carry their own idempotencyKey: an item that already exists with the same request is skipped and listed in duplicates; one that exists with a different request fails the batch with 409. Swagger UI pre-fills every optional field: to send plain content right away, delete scheduledAt, templateId and variables from the example (templateId and variables replace content).
//	@Tags			batches
//	@Accept			json
//	@Produce		json
//	@Param			Idempotency-Key	header		string				false	"Deduplicates retries of the whole batch"
//	@Param			body			body		createBatchRequest	true	"Batch"
//	@Success		202				{object}	createBatchResponse	"Created"
//	@Success		200				{object}	createBatchResponse	"Replayed: same key and same batch"
//	@Failure		400				{object}	errorBody
//	@Failure		409				{object}	errorBody
//	@Failure		413				{object}	errorBody	"Body over 16 MB (other endpoints allow 1 MB)"
//	@Failure		429				{object}	errorBody	"Backpressure: too many notifications waiting to be sent"
//	@Header			429				{integer}	Retry-After	"Seconds to wait before retrying"
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
	for _, ch := range res.CreatedChannels {
		h.metrics.Created(string(ch))
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

// createTemplate godoc
//
//	@Summary		Create a message template
//	@Description	Placeholders are written {{name}} and filled from a notification's variables. Values are inserted verbatim; nothing is evaluated.
//	@Tags			templates
//	@Accept			json
//	@Produce		json
//	@Param			body	body		createTemplateRequest	true	"Template"
//	@Success		201		{object}	templateResponse
//	@Failure		400		{object}	errorBody
//	@Failure		409		{object}	errorBody	"Name already taken"
//	@Failure		413		{object}	errorBody	"Body over 1 MB"
//	@Router			/templates [post]
func (h *Handler) createTemplate(w http.ResponseWriter, r *http.Request) {
	var req createTemplateRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	t, err := h.svc.CreateTemplate(r.Context(), service.TemplateInput{Name: req.Name, Channel: req.Channel, Body: req.Body})
	if err != nil {
		writeServiceError(r.Context(), w, h.log, err)
		return
	}
	w.Header().Set("Location", "/api/v1/templates/"+t.ID.String())
	writeJSON(w, http.StatusCreated, toTemplateResponse(t))
}

// listTemplates godoc
//
//	@Summary	List templates
//	@Tags		templates
//	@Produce	json
//	@Success	200	{object}	templateListResponse
//	@Router		/templates [get]
func (h *Handler) listTemplates(w http.ResponseWriter, r *http.Request) {
	ts, err := h.svc.ListTemplates(r.Context())
	if err != nil {
		writeServiceError(r.Context(), w, h.log, err)
		return
	}
	items := make([]templateResponse, len(ts))
	for i, t := range ts {
		items[i] = toTemplateResponse(t)
	}
	writeJSON(w, http.StatusOK, templateListResponse{Items: items})
}

// getTemplate godoc
//
//	@Summary	Get a template
//	@Tags		templates
//	@Produce	json
//	@Param		id	path		string	true	"Template ID"
//	@Success	200	{object}	templateResponse
//	@Failure	404	{object}	errorBody
//	@Router		/templates/{id} [get]
func (h *Handler) getTemplate(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	t, err := h.svc.GetTemplate(r.Context(), id)
	if err != nil {
		writeServiceError(r.Context(), w, h.log, err)
		return
	}
	writeJSON(w, http.StatusOK, toTemplateResponse(t))
}
