package domain

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrNotFound = errors.New("not found")
	// ErrInvalidState means the requested transition is not allowed from the
	// current status, e.g. cancelling a notification that is processing.
	ErrInvalidState = errors.New("invalid state")
	// ErrDuplicateKey means an insert lost a race on an idempotency key that
	// another transaction committed first. Callers re-read and compare.
	ErrDuplicateKey = errors.New("duplicate idempotency key")
	// ErrIdempotencyMismatch means an idempotency key was reused with a
	// different request body.
	ErrIdempotencyMismatch = errors.New("idempotency key reused with a different request")
	// ErrNameTaken means a template with that name already exists.
	ErrNameTaken = errors.New("name already taken")
)

// IdempotencyConflictError lists the batch items whose key already exists
// with a different request hash. It unwraps to ErrIdempotencyMismatch.
type IdempotencyConflictError struct {
	Fields []FieldError
}

func (e *IdempotencyConflictError) Error() string {
	return ErrIdempotencyMismatch.Error()
}

func (e *IdempotencyConflictError) Unwrap() error { return ErrIdempotencyMismatch }

// FieldError describes one invalid field. Index is set for batch items.
type FieldError struct {
	Index   *int   `json:"index,omitempty"`
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidationError collects every invalid field instead of stopping at the
// first one, so a client can fix a request in a single round trip.
type ValidationError struct {
	Fields []FieldError
}

func (e *ValidationError) Error() string {
	parts := make([]string, len(e.Fields))
	for i, f := range e.Fields {
		if f.Index != nil {
			parts[i] = fmt.Sprintf("[%d].%s: %s", *f.Index, f.Field, f.Message)
		} else {
			parts[i] = f.Field + ": " + f.Message
		}
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

func (e *ValidationError) add(field, msg string) {
	e.Fields = append(e.Fields, FieldError{Field: field, Message: msg})
}

func (e *ValidationError) orNil() error {
	if len(e.Fields) == 0 {
		return nil
	}
	return e
}
