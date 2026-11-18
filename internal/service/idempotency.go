package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/hkln4/go-notification-service/internal/domain"
)

const maxIdempotencyKeyLen = 255

// requestHash fingerprints a validated request. Hashing the Go struct rather
// than the raw body makes it independent of the client's JSON field order,
// whitespace and time zone. Marshalling these types cannot fail.
func requestHash(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// validKey accepts printable ASCII up to 255 bytes, so keys are safe to log
// and to echo back.
func validKey(k string) bool {
	if k == "" || len(k) > maxIdempotencyKeyLen {
		return false
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x21 || k[i] > 0x7e {
			return false
		}
	}
	return true
}

func keyError(field string, index *int) *domain.ValidationError {
	return &domain.ValidationError{Fields: []domain.FieldError{{
		Index: index, Field: field, Message: "must be 1-255 printable ASCII characters",
	}}}
}

func ptr[T any](v T) *T { return &v }
