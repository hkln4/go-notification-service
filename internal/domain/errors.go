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
)

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
