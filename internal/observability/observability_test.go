package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
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
		require.NotEmpty(t, got)
		require.Equal(t, tt.keep, got == tt.in, "SanitizeCorrelationID(%q) = %q", tt.in, got)
	}
}

func TestLoggerAddsCorrelationID(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, "api", "info")
	ctx := WithCorrelationID(context.Background(), "corr-1")

	log.InfoContext(ctx, "hello", "k", "v")
	log.With("x", 1).DebugContext(ctx, "filtered by level")

	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec), "want exactly one JSON record, got %q", buf.String())
	require.Equal(t, "hello", rec["msg"])
	require.Equal(t, "api", rec["service"])
	require.Equal(t, "corr-1", rec["correlation_id"])
	require.Equal(t, "v", rec["k"])
}
