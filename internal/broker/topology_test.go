package broker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNames(t *testing.T) {
	tests := []struct {
		delay     time.Duration
		wantName  string
		wantQueue string
	}{
		{5 * time.Second, "5s", "q.retry.sms.5s"},
		{30 * time.Second, "30s", "q.retry.sms.30s"},
		{2 * time.Minute, "2m", "q.retry.sms.2m"},
		{10 * time.Minute, "10m", "q.retry.sms.10m"},
		{90 * time.Second, "90s", "q.retry.sms.90s"},
		{250 * time.Millisecond, "250ms", "q.retry.sms.250ms"},
	}
	for _, tt := range tests {
		require.Equal(t, tt.wantName, DelayName(tt.delay))
		require.Equal(t, tt.wantQueue, RetryQueue("sms", tt.delay))
		require.Equal(t, "sms."+tt.wantName, RetryRoutingKey("sms", tt.delay))
	}
	require.Equal(t, "q.email", WorkQueue("email"))
}
