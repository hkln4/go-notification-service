package observability

import (
	"context"

	"github.com/google/uuid"
)

// CorrelationHeader is used on HTTP requests/responses and AMQP messages.
const CorrelationHeader = "X-Correlation-ID"

const maxCorrelationIDLen = 128

type correlationKey struct{}

func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey{}).(string)
	return id
}

// SanitizeCorrelationID keeps a client supplied ID if it is short and
// printable ASCII, so it is safe to echo in headers and logs; otherwise it
// returns a fresh one.
func SanitizeCorrelationID(id string) string {
	if id == "" || len(id) > maxCorrelationIDLen {
		return uuid.NewString()
	}
	for i := 0; i < len(id); i++ {
		if id[i] < 0x21 || id[i] > 0x7e {
			return uuid.NewString()
		}
	}
	return id
}
