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

	"github.com/hkln4/go-notification-service/internal/domain"
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
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body is not JSON: %q", rec.Body.String())
	}
	return body.Error.Code
}

func newTestHandler(svc NotificationService) http.Handler {
	return New(svc, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
		{"wrong method", &fakeService{}, "DELETE", "/api/v1/notifications/" + knownID.String(), "", 405, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var headers []string
			if strings.HasSuffix(tt.name, "replayed") {
				headers = []string{"Idempotency-Key", "seen"}
			}
			rec := do(t, newTestHandler(tt.svc), tt.method, tt.path, tt.body, headers...)
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d; body %s", rec.Code, tt.want, rec.Body)
			}
			if tt.wantCode != "" {
				if got := errorCode(t, rec); got != tt.wantCode {
					t.Fatalf("error code = %q, want %q", got, tt.wantCode)
				}
			}
			if strings.Contains(rec.Body.String(), "hunter2") {
				t.Fatal("internal error message leaked to client")
			}
			if rec.Header().Get("X-Correlation-ID") == "" {
				t.Fatal("missing X-Correlation-ID on response")
			}
		})
	}
}

func TestCreatePassesInputAndCorrelationID(t *testing.T) {
	svc := &fakeService{}
	at := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	rec := do(t, newTestHandler(svc), "POST", "/api/v1/notifications",
		`{"channel":"email","recipient":"a@b.co","content":"hi","priority":"low","scheduledAt":"2030-01-02T03:04:05Z"}`,
		"X-Correlation-ID", "client-corr")

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := rec.Header().Get("X-Correlation-ID"); got != "client-corr" {
		t.Errorf("response correlation = %q", got)
	}
	if got := rec.Header().Get("Location"); got != "/api/v1/notifications/"+knownID.String() {
		t.Errorf("Location = %q", got)
	}
	in := svc.lastReq.Input
	if in.Channel != "email" || in.Recipient != "a@b.co" || in.Priority != "low" || !in.ScheduledAt.Equal(at) {
		t.Errorf("input = %+v", in)
	}
	if svc.lastReq.CorrelationID != "client-corr" {
		t.Errorf("service got correlation %q", svc.lastReq.CorrelationID)
	}
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
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Error.Details) != 2 || body.Error.Details[1].Field != "content" {
		t.Fatalf("details = %+v", body.Error.Details)
	}
}

func TestGetIncludesDeliveryAttemptsArray(t *testing.T) {
	rec := do(t, newTestHandler(&fakeService{}), "GET", "/api/v1/notifications/"+knownID.String(), "")
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body["deliveryAttempts"].([]any); !ok {
		t.Fatalf("deliveryAttempts should be an array even when empty: %s", rec.Body)
	}
	if body["id"] != knownID.String() || body["status"] != "sent" {
		t.Fatalf("embedded fields not flattened: %s", rec.Body)
	}
}

func TestBatchResponses(t *testing.T) {
	svc := &fakeService{}
	h := newTestHandler(svc)
	item := `{"channel":"push","recipient":"tok","content":"hi"}`

	rec := do(t, h, "POST", "/api/v1/notifications/batch", `{"notifications":[`+item+`,`+item+`]}`, "X-Correlation-ID", "b-1")
	var created createBatchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.BatchID != knownID.String() || created.Count != 2 || len(created.NotificationIDs) != 2 {
		t.Errorf("create response = %+v", created)
	}
	if len(svc.lastBatch.Items) != 2 || svc.lastBatch.Items[0].Input.Channel != "push" || svc.lastBatch.CorrelationID != "b-1" {
		t.Errorf("service got %+v", svc.lastBatch)
	}

	rec = do(t, h, "GET", "/api/v1/batches/"+knownID.String(), "")
	var got batchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.StatusCounts) != len(domain.Statuses) || got.StatusCounts["sent"] != 2 || got.StatusCounts["pending"] != 0 {
		t.Errorf("statusCounts = %v; want every status present", got.StatusCounts)
	}
}

func TestListParsesQuery(t *testing.T) {
	svc := &fakeService{}
	rec := do(t, newTestHandler(svc), "GET",
		"/api/v1/notifications?status=failed&channel=email&from=2026-11-01T00:00:00Z&to=2026-11-02T00:00:00%2B03:00&cursor=abc&limit=5", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
	q := svc.lastList
	if q.Status != "failed" || q.Channel != "email" || q.Cursor != "abc" || q.Limit != 5 {
		t.Errorf("query = %+v", q)
	}
	if !q.From.Equal(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)) || !q.To.Equal(time.Date(2026, 11, 1, 21, 0, 0, 0, time.UTC)) {
		t.Errorf("from/to = %v / %v", q.From, q.To)
	}
	var body listResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || len(body.Items) != 1 || body.NextCursor != "next" {
		t.Errorf("body = %s (%v)", rec.Body, err)
	}
}

func TestIdempotencyKeysReachService(t *testing.T) {
	svc := &fakeService{}
	h := newTestHandler(svc)

	do(t, h, "POST", "/api/v1/notifications", `{"channel":"push","recipient":"t","content":"c"}`, "Idempotency-Key", "k-1")
	if svc.lastReq.IdempotencyKey != "k-1" {
		t.Errorf("create key = %q", svc.lastReq.IdempotencyKey)
	}

	do(t, h, "POST", "/api/v1/notifications/batch",
		`{"notifications":[{"channel":"push","recipient":"t","content":"c","idempotencyKey":"item-1"}]}`,
		"Idempotency-Key", "batch-1")
	if svc.lastBatch.IdempotencyKey != "batch-1" || svc.lastBatch.Items[0].IdempotencyKey != "item-1" {
		t.Errorf("batch keys = %q / %q", svc.lastBatch.IdempotencyKey, svc.lastBatch.Items[0].IdempotencyKey)
	}

	// The single create endpoint takes its key from the header only.
	rec := do(t, h, "POST", "/api/v1/notifications", `{"channel":"push","recipient":"t","content":"c","idempotencyKey":"x"}`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("idempotencyKey in a single create body: status %d, want 400", rec.Code)
	}
}
