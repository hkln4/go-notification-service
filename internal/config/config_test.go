package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReader(t *testing.T) {
	r := NewReaderFrom(map[string]string{
		"S": "value", "EMPTY": "", "I": "42", "BAD_I": "x", "D": "250ms", "BAD_D": "soon", "F": "0.25",
	})

	require.Equal(t, "value", r.String("S", "def"))
	require.Equal(t, "def", r.String("EMPTY", "def"), "empty value falls back to default")
	require.Equal(t, 42, r.Int("I", 0))
	require.Equal(t, 250*time.Millisecond, r.Duration("D", 0))
	require.InDelta(t, 0.25, r.Float("F", 0), 1e-9)
	require.Equal(t, 7, r.Int("MISSING", 7))
	require.NoError(t, r.Err())

	require.Equal(t, []time.Duration{time.Second, 2 * time.Minute}, NewReaderFrom(map[string]string{"L": "1s, 2m"}).Durations("L", nil))
	bad := NewReaderFrom(map[string]string{"L": "1s,-2s"})
	require.Equal(t, DefaultRetryDelays, bad.Durations("L", DefaultRetryDelays))
	require.Error(t, bad.Err())

	r.Int("BAD_I", 0)
	r.Duration("BAD_D", 0)
	err := r.Err()
	require.ErrorContains(t, err, "BAD_I")
	require.ErrorContains(t, err, "BAD_D")
}

func TestLoadWorkerValidates(t *testing.T) {
	_, err := LoadWorker(NewReaderFrom(nil))
	require.NoError(t, err, "defaults are valid")

	_, err = LoadWorker(NewReaderFrom(map[string]string{"WORKER_LEASE": "5s", "PROVIDER_TIMEOUT": "5s"}))
	require.ErrorContains(t, err, "WORKER_LEASE")

	_, err = LoadWorker(NewReaderFrom(map[string]string{"WORKER_PREFETCH": "0"}))
	require.Error(t, err)
}
