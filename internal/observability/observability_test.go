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

func TestMaskRecipient(t *testing.T) {
	tests := []struct{ in, want string }{
		{"+905551234567", "+90555***4567"},
		{"+12025550123", "+12025***0123"},
		{"+9055512", "+90***"}, // too short to keep both ends
		{"hasan@gmail.com", "h***@gmail.com"},
		{"a@b.co", "a***@b.co"},
		{"device-token-xyz", "devi***"},
		{"abc", "abc***"},
		{"", ""},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, MaskRecipient(tt.in), tt.in)
	}
}

func TestLoggerMasksPIIAttributes(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, "worker", "info")
	log.With("recipient", "+905551234567").Info("sent", "to", "hasan@gmail.com", "content", "not masked")

	out := buf.String()
	require.NotContains(t, out, "+905551234567")
	require.NotContains(t, out, "hasan@gmail.com")
	require.Contains(t, out, `"recipient":"+90555***4567"`)
	require.Contains(t, out, `"to":"h***@gmail.com"`)
	require.Contains(t, out, `"content":"not masked"`)
}
