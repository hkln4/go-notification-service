package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/hkln4/go-notification-service/internal/domain"
	"github.com/hkln4/go-notification-service/internal/observability"
	"github.com/hkln4/go-notification-service/internal/service"
)

var knownID = uuid.MustParse("01934a5c-0000-7000-8000-000000000001")

// fakeService returns canned results; the service itself has its own tests.
type fakeService struct {
	createErr error
	lastReq   service.CreateRequest
	lastBatch service.CreateBatchRequest
	lastList  service.ListQuery
	panicOn   string
}

func (f *fakeService) Create(_ context.Context, req service.CreateRequest) (service.CreateResult, error) {
	if f.panicOn == "create" {
		panic("boom")
	}
	f.lastReq = req
	if f.createErr != nil {
		return service.CreateResult{}, f.createErr
	}
	n := domain.Notification{ID: knownID, Channel: domain.ChannelSMS, Status: domain.StatusPending, Priority: domain.PriorityHigh}
	return service.CreateResult{Notification: n, Replayed: req.IdempotencyKey == "seen"}, nil
}

func (f *fakeService) Get(_ context.Context, id uuid.UUID) (service.NotificationDetail, error) {
	if id != knownID {
		return service.NotificationDetail{}, domain.ErrNotFound
	}
	return service.NotificationDetail{Notification: domain.Notification{ID: id, Status: domain.StatusSent}}, nil
}

func (f *fakeService) Cancel(_ context.Context, id uuid.UUID) (domain.Notification, error) {
	if id != knownID {
		return domain.Notification{}, domain.ErrNotFound
	}
	return domain.Notification{ID: id, Status: domain.StatusProcessing}, domain.ErrInvalidState
}

func (f *fakeService) CreateBatch(_ context.Context, req service.CreateBatchRequest) (service.BatchResult, error) {
	f.lastBatch = req
	if f.createErr != nil {
		return service.BatchResult{}, f.createErr
	}
	ids := make([]uuid.UUID, len(req.Items))
	for i := range ids {
		ids[i] = uuid.New()
	}
	return service.BatchResult{
		Batch:           domain.Batch{ID: knownID, TotalCount: len(ids)},
		NotificationIDs: ids,
		Replayed:        req.IdempotencyKey == "seen",
	}, nil
}

func (f *fakeService) GetBatch(_ context.Context, id uuid.UUID) (service.BatchSummary, error) {
	if id != knownID {
		return service.BatchSummary{}, domain.ErrNotFound
	}
	return service.BatchSummary{
		Batch:        domain.Batch{ID: id, TotalCount: 3},
		StatusCounts: map[domain.Status]int{domain.StatusSent: 2, domain.StatusFailed: 1},
	}, nil
}

func (f *fakeService) List(_ context.Context, q service.ListQuery) (service.Page, error) {
	f.lastList = q
	return service.Page{Items: []domain.Notification{{ID: knownID}}, NextCursor: "next"}, nil
}

func (f *fakeService) Retry(_ context.Context, id uuid.UUID) (domain.Notification, error) {
	if id != knownID {
		return domain.Notification{}, domain.ErrNotFound
	}
	return domain.Notification{ID: id, Status: domain.StatusPending}, nil
}

func (f *fakeService) CreateTemplate(_ context.Context, in service.TemplateInput) (domain.Template, error) {
	if in.Name == "taken" {
		return domain.Template{}, domain.ErrNameTaken
	}
	return domain.Template{ID: knownID, Name: in.Name, Channel: domain.Channel(in.Channel), Body: in.Body}, nil
}

func (f *fakeService) GetTemplate(_ context.Context, id uuid.UUID) (domain.Template, error) {
	if id != knownID {
		return domain.Template{}, domain.ErrNotFound
	}
	return domain.Template{ID: id, Name: "t", Channel: domain.ChannelSMS, Body: "Hi {{name}} {{code}}"}, nil
}

func (f *fakeService) ListTemplates(context.Context) ([]domain.Template, error) {
	return []domain.Template{{ID: knownID, Name: "t", Body: "x"}}, nil
}

func do(t *testing.T, h http.Handler, method, path, body string, headers ...string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body errorBody
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), "error body is not JSON: %q", rec.Body)
	return body.Error.Code
}

func newTestHandler(svc NotificationService) http.Handler {
	return New(svc, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Metrics: observability.NewMetrics()})
}

func TestHandlerStatusCodes(t *testing.T) {
	validBody := `{"channel":"sms","recipient":"+905551234567","content":"hi","priority":"high"}`
	tests := []struct {
		name     string
		svc      *fakeService
		method   string
		path     string
		body     string
		want     int
		wantCode string
	}{
		{"create", &fakeService{}, "POST", "/api/v1/notifications", validBody, 202, ""},
		{"create malformed json", &fakeService{}, "POST", "/api/v1/notifications", `{"channel":`, 400, codeInvalidJSON},
		{"create unknown field", &fakeService{}, "POST", "/api/v1/notifications", `{"recipent":"x"}`, 400, codeInvalidJSON},
		{"create trailing data", &fakeService{}, "POST", "/api/v1/notifications", validBody + `{}`, 400, codeInvalidJSON},
		{"create too large", &fakeService{}, "POST", "/api/v1/notifications", `{"content":"` + strings.Repeat("a", MaxBodyBytes) + `"}`, 413, codePayloadTooLarge},
		{"create validation", &fakeService{createErr: &domain.ValidationError{Fields: []domain.FieldError{{Field: "recipient", Message: "bad"}}}}, "POST", "/api/v1/notifications", validBody, 400, codeValidation},
		{"create internal error", &fakeService{createErr: errors.New("db password is hunter2")}, "POST", "/api/v1/notifications", validBody, 500, codeInternal},
		{"create panics", &fakeService{panicOn: "create"}, "POST", "/api/v1/notifications", validBody, 500, codeInternal},
		{"get", &fakeService{}, "GET", "/api/v1/notifications/" + knownID.String(), "", 200, ""},
		{"get bad id", &fakeService{}, "GET", "/api/v1/notifications/not-a-uuid", "", 400, codeInvalidID},
		{"get missing", &fakeService{}, "GET", "/api/v1/notifications/" + uuid.NewString(), "", 404, codeNotFound},
		{"cancel processing", &fakeService{}, "POST", "/api/v1/notifications/" + knownID.String() + "/cancel", "", 409, codeInvalidState},
		{"cancel missing", &fakeService{}, "POST", "/api/v1/notifications/" + uuid.NewString() + "/cancel", "", 404, codeNotFound},
		{"batch", &fakeService{}, "POST", "/api/v1/notifications/batch", `{"notifications":[` + validBody + `,` + validBody + `]}`, 202, ""},
		{"batch validation", &fakeService{createErr: &domain.ValidationError{}}, "POST", "/api/v1/notifications/batch", `{"notifications":[]}`, 400, codeValidation},
		{"get batch", &fakeService{}, "GET", "/api/v1/batches/" + knownID.String(), "", 200, ""},
		{"get batch missing", &fakeService{}, "GET", "/api/v1/batches/" + uuid.NewString(), "", 404, codeNotFound},
		{"list", &fakeService{}, "GET", "/api/v1/notifications?status=sent&limit=10", "", 200, ""},
		{"list bad from", &fakeService{}, "GET", "/api/v1/notifications?from=yesterday", "", 400, codeValidation},
		{"list bad limit", &fakeService{}, "GET", "/api/v1/notifications?limit=ten", "", 400, codeValidation},
		{"create replayed", &fakeService{}, "POST", "/api/v1/notifications", validBody, 200, ""},
		{"create key mismatch", &fakeService{createErr: domain.ErrIdempotencyMismatch}, "POST", "/api/v1/notifications", validBody, 409, codeIdempotency},
		{"batch item key conflict", &fakeService{createErr: &domain.IdempotencyConflictError{}}, "POST", "/api/v1/notifications/batch", `{"notifications":[]}`, 409, codeIdempotency},
		{"retry", &fakeService{}, "POST", "/api/v1/notifications/" + knownID.String() + "/retry", "", 202, ""},
		{"retry missing", &fakeService{}, "POST", "/api/v1/notifications/" + uuid.NewString() + "/retry", "", 404, codeNotFound},
		{"create template", &fakeService{}, "POST", "/api/v1/templates", `{"name":"a","channel":"sms","body":"Hi {{n}}"}`, 201, ""},
		{"template name taken", &fakeService{}, "POST", "/api/v1/templates", `{"name":"taken","channel":"sms","body":"x"}`, 409, codeNameTaken},
		{"list templates", &fakeService{}, "GET", "/api/v1/templates", "", 200, ""},
		{"get template", &fakeService{}, "GET", "/api/v1/templates/" + knownID.String(), "", 200, ""},
		{"get template missing", &fakeService{}, "GET", "/api/v1/templates/" + uuid.NewString(), "", 404, codeNotFound},
		{"health", &fakeService{}, "GET", "/health", "", 200, ""},
		{"ready", &fakeService{}, "GET", "/ready", "", 200, ""},
		{"metrics", &fakeService{}, "GET", "/metrics", "", 200, ""},
		{"wrong method", &fakeService{}, "DELETE", "/api/v1/notifications/" + knownID.String(), "", 405, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var headers []string
			if strings.HasSuffix(tt.name, "replayed") {
				headers = []string{"Idempotency-Key", "seen"}
			}
			rec := do(t, newTestHandler(tt.svc), tt.method, tt.path, tt.body, headers...)
			require.Equal(t, tt.want, rec.Code, "body: %s", rec.Body)
			if tt.wantCode != "" {
				require.Equal(t, tt.wantCode, errorCode(t, rec))
			}
			require.NotContains(t, rec.Body.String(), "hunter2", "internal error message leaked to client")
			require.NotEmpty(t, rec.Header().Get("X-Correlation-ID"))
		})
	}
}

func TestCreatePassesInputAndCorrelationID(t *testing.T) {
	svc := &fakeService{}
	at := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	rec := do(t, newTestHandler(svc), "POST", "/api/v1/notifications",
		`{"channel":"email","recipient":"a@b.co","content":"hi","priority":"low","scheduledAt":"2030-01-02T03:04:05Z"}`,
		"X-Correlation-ID", "client-corr")

	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Equal(t, "client-corr", rec.Header().Get("X-Correlation-ID"))
	require.Equal(t, "/api/v1/notifications/"+knownID.String(), rec.Header().Get("Location"))

	in := svc.lastReq.Input
	require.Equal(t, "email", in.Channel)
	require.Equal(t, "a@b.co", in.Recipient)
	require.Equal(t, "low", in.Priority)
	require.True(t, in.ScheduledAt.Equal(at))
	require.Equal(t, "client-corr", svc.lastReq.CorrelationID)
}

func TestValidationErrorDetails(t *testing.T) {
	svc := &fakeService{createErr: &domain.ValidationError{Fields: []domain.FieldError{
		{Field: "recipient", Message: "bad"}, {Field: "content", Message: "too long"},
	}}}
	rec := do(t, newTestHandler(svc), "POST", "/api/v1/notifications", `{}`)

	var body struct {
		Error struct {
			Details []domain.FieldError `json:"details"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Error.Details, 2)
	require.Equal(t, "content", body.Error.Details[1].Field)
}

func TestGetIncludesDeliveryAttemptsArray(t *testing.T) {
	rec := do(t, newTestHandler(&fakeService{}), "GET", "/api/v1/notifications/"+knownID.String(), "")
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.IsType(t, []any{}, body["deliveryAttempts"], "deliveryAttempts should be an array even when empty")
	require.Equal(t, knownID.String(), body["id"], "embedded fields are flattened")
	require.Equal(t, "sent", body["status"])
}

func TestBatchResponses(t *testing.T) {
	svc := &fakeService{}
	h := newTestHandler(svc)
	item := `{"channel":"push","recipient":"tok","content":"hi"}`

	rec := do(t, h, "POST", "/api/v1/notifications/batch", `{"notifications":[`+item+`,`+item+`]}`, "X-Correlation-ID", "b-1")
	var created createBatchResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	require.Equal(t, knownID.String(), created.BatchID)
	require.Equal(t, 2, created.Count)
	require.Len(t, created.NotificationIDs, 2)
	require.Len(t, svc.lastBatch.Items, 2)
	require.Equal(t, "push", svc.lastBatch.Items[0].Input.Channel)
	require.Equal(t, "b-1", svc.lastBatch.CorrelationID)

	rec = do(t, h, "GET", "/api/v1/batches/"+knownID.String(), "")
	var got batchResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Len(t, got.StatusCounts, len(domain.Statuses), "every status is present")
	require.Equal(t, 2, got.StatusCounts["sent"])
	require.Contains(t, got.StatusCounts, "pending")
	require.Zero(t, got.StatusCounts["pending"])
}

func TestListParsesQuery(t *testing.T) {
	svc := &fakeService{}
	rec := do(t, newTestHandler(svc), "GET",
		"/api/v1/notifications?status=failed&channel=email&from=2026-11-01T00:00:00Z&to=2026-11-02T00:00:00%2B03:00&cursor=abc&limit=5", "")
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body)
	q := svc.lastList
	require.Equal(t, service.ListQuery{
		Status: "failed", Channel: "email", Cursor: "abc", Limit: 5, From: q.From, To: q.To,
	}, q)
	require.True(t, q.From.Equal(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)))
	require.True(t, q.To.Equal(time.Date(2026, 11, 1, 21, 0, 0, 0, time.UTC)), "offset is honoured")

	var body listResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Items, 1)
	require.Equal(t, "next", body.NextCursor)
}

func TestIdempotencyKeysReachService(t *testing.T) {
	svc := &fakeService{}
	h := newTestHandler(svc)

	do(t, h, "POST", "/api/v1/notifications", `{"channel":"push","recipient":"t","content":"c"}`, "Idempotency-Key", "k-1")
	require.Equal(t, "k-1", svc.lastReq.IdempotencyKey)

	do(t, h, "POST", "/api/v1/notifications/batch",
		`{"notifications":[{"channel":"push","recipient":"t","content":"c","idempotencyKey":"item-1"}]}`,
		"Idempotency-Key", "batch-1")
	require.Equal(t, "batch-1", svc.lastBatch.IdempotencyKey)
	require.Equal(t, "item-1", svc.lastBatch.Items[0].IdempotencyKey)

	// The single create endpoint takes its key from the header only.
	rec := do(t, h, "POST", "/api/v1/notifications", `{"channel":"push","recipient":"t","content":"c","idempotencyKey":"x"}`)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestMetricsUseRoutePattern(t *testing.T) {
	m := observability.NewMetrics()
	h := New(&fakeService{}, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Metrics: m})
	do(t, h, "GET", "/api/v1/notifications/"+knownID.String(), "")
	do(t, h, "POST", "/api/v1/notifications", `{"channel":"sms","recipient":"+905551234567","content":"x"}`)

	out := do(t, h, "GET", "/metrics", "").Body.String()
	require.Contains(t, out, `route="GET /api/v1/notifications/{id}"`)
	require.NotContains(t, out, knownID.String(), "IDs must not become label values")
	require.Contains(t, out, `notifications_created_total{channel="sms"} 1`)
}

func TestBackpressure(t *testing.T) {
	backlog := 0
	h := New(&fakeService{}, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		Backpressure: Backpressure{Backlog: func() int { return backlog }, Limit: 100, RetryAfter: 5 * time.Second},
	})
	create := `{"channel":"sms","recipient":"+905551234567","content":"x"}`

	backlog = 100 // at the limit: still accepted
	require.Equal(t, http.StatusAccepted, do(t, h, "POST", "/api/v1/notifications", create).Code)

	backlog = 101
	rec := do(t, h, "POST", "/api/v1/notifications", create)
	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	require.Equal(t, "5", rec.Header().Get("Retry-After"))
	require.Equal(t, codeOverloaded, errorCode(t, rec))
	require.Equal(t, http.StatusTooManyRequests,
		do(t, h, "POST", "/api/v1/notifications/batch", `{"notifications":[`+create+`]}`).Code)

	// Reads and cancels keep working while overloaded.
	require.Equal(t, http.StatusOK, do(t, h, "GET", "/api/v1/notifications/"+knownID.String(), "").Code)
	require.Equal(t, http.StatusConflict, do(t, h, "POST", "/api/v1/notifications/"+knownID.String()+"/cancel", "").Code)
}

func TestTemplateFieldsReachService(t *testing.T) {
	svc := &fakeService{}
	h := newTestHandler(svc)
	do(t, h, "POST", "/api/v1/notifications",
		`{"recipient":"+905551234567","templateId":"`+knownID.String()+`","variables":{"name":"Ayşe"}}`)
	require.Equal(t, knownID.String(), svc.lastReq.Input.TemplateID)
	require.Equal(t, map[string]string{"name": "Ayşe"}, svc.lastReq.Input.Variables)

	rec := do(t, h, "GET", "/api/v1/templates/"+knownID.String(), "")
	var got templateResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, []string{"name", "code"}, got.Variables)
}
