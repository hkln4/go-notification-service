package breaker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newBreaker() (*Breaker, *clock, *[]State) {
	clk := &clock{t: time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)}
	var changes []State
	b := New(Config{Threshold: 3, Cooldown: 30 * time.Second, OnChange: func(_ string, s State) { changes = append(changes, s) }})
	b.now = clk.now
	return b, clk, &changes
}

func call(t *testing.T, b *Breaker, o Outcome) {
	t.Helper()
	tk, ok := b.Allow("sms")
	require.True(t, ok, "call should be allowed")
	tk.Report(o)
}

func TestOpensAfterConsecutiveFailures(t *testing.T) {
	b, _, _ := newBreaker()
	call(t, b, Failure)
	call(t, b, Failure)
	call(t, b, Success) // resets the streak
	call(t, b, Failure)
	call(t, b, Failure)
	require.Equal(t, Closed, b.State("sms"))
	call(t, b, Failure)
	require.Equal(t, Open, b.State("sms"))

	_, ok := b.Allow("sms")
	require.False(t, ok, "open circuit rejects calls")
	_, ok = b.Allow("email")
	require.True(t, ok, "channels are independent")
}

func TestHalfOpenTrial(t *testing.T) {
	tests := []struct {
		name  string
		trial Outcome
		want  State
	}{
		{"trial succeeds: closes", Success, Closed},
		{"trial fails: reopens", Failure, Open},
		{"trial aborted: stays half-open", Abort, HalfOpen},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, clk, _ := newBreaker()
			for range 3 {
				call(t, b, Failure)
			}
			clk.advance(29 * time.Second)
			_, ok := b.Allow("sms")
			require.False(t, ok, "still cooling down")

			clk.advance(time.Second)
			trial, ok := b.Allow("sms")
			require.True(t, ok, "cooldown over: one trial")
			_, ok = b.Allow("sms")
			require.False(t, ok, "only one trial at a time")

			trial.Report(tt.trial)
			require.Equal(t, tt.want, b.State("sms"))
			if tt.trial == Abort {
				_, ok := b.Allow("sms")
				require.True(t, ok, "an aborted trial frees the slot for another")
			}
		})
	}
}

func TestReopenRestartsCooldown(t *testing.T) {
	b, clk, _ := newBreaker()
	for range 3 {
		call(t, b, Failure)
	}
	clk.advance(30 * time.Second)
	call(t, b, Failure) // failed trial
	clk.advance(29 * time.Second)
	_, ok := b.Allow("sms")
	require.False(t, ok, "cooldown counts from the failed trial")
}

func TestStaleReportsAfterOpenAreIgnored(t *testing.T) {
	b, clk, _ := newBreaker()
	slow, _ := b.Allow("sms") // in flight while the circuit opens
	for range 3 {
		call(t, b, Failure)
	}
	slow.Report(Success)
	require.Equal(t, Open, b.State("sms"), "an old success must not close an open circuit")

	clk.advance(30 * time.Second)
	trial, _ := b.Allow("sms")
	trial.Report(Success)
	trial.Report(Failure) // second report ignored
	require.Equal(t, Closed, b.State("sms"))
}

func TestOnChange(t *testing.T) {
	b, clk, changes := newBreaker()
	for range 3 {
		call(t, b, Failure)
	}
	clk.advance(30 * time.Second)
	call(t, b, Success)
	require.Equal(t, []State{Open, HalfOpen, Closed}, *changes)
}
