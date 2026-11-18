package worker

import "time"

type Action int

const (
	ActionSent Action = iota + 1
	// ActionRetry: transient failure with attempts left; wait Delay.
	ActionRetry
	// ActionDeadLetter: transient failures used up every attempt.
	ActionDeadLetter
	// ActionPermanentFail: the provider rejected the request itself (4xx);
	// retrying the same request cannot succeed.
	ActionPermanentFail
)

func (a Action) String() string {
	switch a {
	case ActionSent:
		return "sent"
	case ActionRetry:
		return "retry"
	case ActionDeadLetter:
		return "dead_letter"
	case ActionPermanentFail:
		return "permanent_fail"
	}
	return "unknown"
}

type Decision struct {
	Action Action
	// Delay is set for ActionRetry and is always one of the retry queues.
	Delay time.Duration
}

// Policy is the retry ladder. Delays[i] is the wait after attempt i+1
// failed; with the defaults: 5s, 30s, 2m, 10m, then the DLQ after attempt 5.
type Policy struct {
	Delays      []time.Duration
	MaxAttempts int
}

// Decide maps one provider call to what happens next. It is a pure function
// of its inputs, so every rule is covered by a table-driven test.
//
//	2xx                          → sent (a malformed body doesn't matter)
//	429                          → transient; wait for Retry-After, clamped to
//	                               the ladder and rounded up to a queue
//	5xx, timeout, network error  → transient; next step of the ladder
//	other 4xx (and 1xx/3xx)      → permanent failure
//	transient, attempt >= max    → dead letter
//
// attempt is the 1-based number of the attempt that just finished.
func (p Policy) Decide(statusCode int, err error, attempt int, retryAfter time.Duration) Decision {
	var delay time.Duration
	switch {
	case err != nil:
		delay = p.ladder(attempt)
	case statusCode >= 200 && statusCode < 300:
		return Decision{Action: ActionSent}
	case statusCode == 429:
		if retryAfter > 0 {
			delay = p.atLeast(retryAfter)
		} else {
			delay = p.ladder(attempt)
		}
	case statusCode >= 500:
		delay = p.ladder(attempt)
	default:
		return Decision{Action: ActionPermanentFail}
	}

	if attempt >= p.MaxAttempts {
		return Decision{Action: ActionDeadLetter}
	}
	return Decision{Action: ActionRetry, Delay: delay}
}

func (p Policy) ladder(attempt int) time.Duration {
	i := min(max(attempt-1, 0), len(p.Delays)-1)
	return p.Delays[i]
}

// atLeast returns the shortest queue that waits at least d. Queues have
// fixed TTLs, so the wait is rounded up, never down: a provider asking for
// 40s must not hear from us after 30s. Beyond the longest queue it clamps.
func (p Policy) atLeast(d time.Duration) time.Duration {
	for _, delay := range p.Delays {
		if delay >= d {
			return delay
		}
	}
	return p.Delays[len(p.Delays)-1]
}
