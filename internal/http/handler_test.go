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
	panicOn   string
}

func (f *fakeService) Create(_ context.Context, req service.CreateRequest) (domain.Notification, error) {
	if f.panicOn == "create" {
		panic("boom")
	}
	f.lastReq = req
	if f.createErr != nil {
		return domain.Notification{}, f.createErr
	}
	return domain.Notification{ID: knownID, Channel: domain.ChannelSMS, Status: domain.StatusPending, Priority: domain.PriorityHigh}, nil
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
		{"wrong method", &fakeService{}, "DELETE", "/api/v1/notifications/" + knownID.String(), "", 405, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, newTestHandler(tt.svc), tt.method, tt.path, tt.body)
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
