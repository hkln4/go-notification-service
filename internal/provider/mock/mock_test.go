package mock

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func post(t *testing.T, h http.Handler, body, key string) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

const valid = `{"to":"+905551234567","channel":"sms","content":"hi"}`

func TestFailureRate(t *testing.T) {
	tests := []struct {
		rate float64
		roll float64
		want int
	}{
		{0, 0, http.StatusAccepted},
		{1, 0.999, http.StatusInternalServerError},
		{0.3, 0.29, http.StatusInternalServerError},
		{0.3, 0.30, http.StatusAccepted},
	}
	for _, tt := range tests {
		s := New(Config{FailureRate: tt.rate})
		s.rand = func() float64 { return tt.roll }
		require.Equal(t, tt.want, post(t, s.Handler(), valid, ""), "rate %v roll %v", tt.rate, tt.roll)
	}
}

func TestRejectsInvalidRequest(t *testing.T) {
	require.Equal(t, http.StatusBadRequest, post(t, New(Config{}).Handler(), `{"channel":"sms"}`, ""))
}

func TestStatsCountDuplicatesAndRate(t *testing.T) {
	s := New(Config{})
	h := s.Handler()
	for _, k := range []string{"a", "b", "a", "c", "c", "c"} {
		post(t, h, valid, k)
	}
	st := s.Stats()
	require.Equal(t, 6, st.Channels["sms"].Received)
	require.Equal(t, 2, st.DuplicateKeys, "a and c were seen more than once")
}
