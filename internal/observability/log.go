// Package observability holds the logging, correlation ID, PII masking,
// metrics, health check and tracing helpers shared by every binary.
package observability

import (
	"context"
	"io"
	"log/slog"
	"strings"
)

// NewLogger returns a JSON logger tagged with the service name. Records
// logged with a context that carries a correlation ID include it, and
// recipient-like attributes are masked (see MaskRecipient).
func NewLogger(w io.Writer, service, level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl, ReplaceAttr: maskPII})
	return slog.New(contextHandler{h}).With("service", service)
}

// contextHandler adds request-scoped attributes from the context, so call
// sites only have to use the *Context logging methods.
type contextHandler struct{ slog.Handler }

func (h contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := CorrelationID(ctx); id != "" {
		r.AddAttrs(slog.String("correlation_id", id))
	}
	return h.Handler.Handle(ctx, r)
}

func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{h.Handler.WithAttrs(attrs)}
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{h.Handler.WithGroup(name)}
}
