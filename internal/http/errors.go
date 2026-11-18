package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/hkln4/go-notification-service/internal/domain"
)

// Error codes are part of the API contract; clients may switch on them.
const (
	codeInvalidJSON     = "invalid_json"
	codeInvalidID       = "invalid_id"
	codeValidation      = "validation_failed"
	codeNotFound        = "not_found"
	codeInvalidState    = "invalid_state"
	codePayloadTooLarge = "payload_too_large"
	codeInternal        = "internal"
	codeIdempotency     = "idempotency_conflict"
	codeOverloaded      = "overloaded"
)

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string, details any) {
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: msg, Details: details}})
}

// writeServiceError maps domain errors to HTTP. Anything unknown is a 500 and
// is logged; its message is not exposed to the client.
func writeServiceError(ctx context.Context, w http.ResponseWriter, log *slog.Logger, err error) {
	var verr *domain.ValidationError
	var conflict *domain.IdempotencyConflictError
	switch {
	case errors.As(err, &verr):
		writeError(w, http.StatusBadRequest, codeValidation, "request validation failed", verr.Fields)
	case errors.As(err, &conflict):
		writeError(w, http.StatusConflict, codeIdempotency, conflict.Error(), conflict.Fields)
	case errors.Is(err, domain.ErrIdempotencyMismatch):
		writeError(w, http.StatusConflict, codeIdempotency, err.Error(), nil)
	case errors.Is(err, domain.ErrDuplicateKey):
		// Only after the retry also lost the race; the client can retry.
		writeError(w, http.StatusConflict, codeIdempotency, "a concurrent request with the same idempotency key is in progress", nil)
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, codeNotFound, "resource not found", nil)
	case errors.Is(err, domain.ErrInvalidState):
		writeError(w, http.StatusConflict, codeInvalidState, err.Error(), nil)
	default:
		log.ErrorContext(ctx, "request failed", "err", err)
		writeError(w, http.StatusInternalServerError, codeInternal, "internal server error", nil)
	}
}

// decodeJSON reads one JSON object, rejecting unknown fields so that typos
// like "recipent" fail loudly instead of being silently ignored.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, codePayloadTooLarge, "request body exceeds 1 MB", nil)
			return false
		}
		writeError(w, http.StatusBadRequest, codeInvalidJSON, err.Error(), nil)
		return false
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, codeInvalidJSON, "request body must contain a single JSON object", nil)
		return false
	}
	return true
}
