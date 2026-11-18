package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestSanitizeCorrelationID(t *testing.T) {
	tests := []struct {
		in   string
		keep bool
	}{
		{"abc-123", true},
		{strings.Repeat("a", 128), true},
		{strings.Repeat("a", 129), false},
		{"", false},
		{"has space", false},
		{"new\nline", false},
		{"ünicode", false},
	}
	for _, tt := range tests {
		got := SanitizeCorrelationID(tt.in)
		if (got == tt.in) != tt.keep {
			t.Errorf("SanitizeCorrelationID(%q) = %q, keep=%v", tt.in, got, tt.keep)
		}
		if got == "" {
			t.Errorf("SanitizeCorrelationID(%q) returned empty", tt.in)
		}
	}
}

func TestLoggerAddsCorrelationID(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, "api", "info")
	ctx := WithCorrelationID(context.Background(), "corr-1")

	log.InfoContext(ctx, "hello", "k", "v")
	log.With("x", 1).DebugContext(ctx, "filtered by level")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("want exactly one JSON record, got %q: %v", buf.String(), err)
	}
	for k, want := range map[string]any{"msg": "hello", "service": "api", "correlation_id": "corr-1", "k": "v"} {
		if rec[k] != want {
			t.Errorf("%s = %v, want %v", k, rec[k], want)
		}
	}
}
