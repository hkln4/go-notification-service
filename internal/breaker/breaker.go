// Package breaker is a per-channel circuit breaker. When a provider keeps
// failing, sending more requests only burns attempts and hammers it while it
// recovers. After Threshold consecutive transient failures the circuit opens
// for Cooldown: messages are parked without being claimed, so no attempt is
// used. Then one trial request is let through (half-open); its result
// closes or reopens the circuit.
//
// The breaker lives in each worker process. Workers don't share it: each
// one notices the outage on its own after a few failures, which keeps the
// design simple at the cost of a few extra failed calls per worker.
package breaker

import (
	"sync"
	"time"
)

type State int

const (
	Closed   State = 0
	HalfOpen State = 1
	Open     State = 2
)

func (s State) String() string {
	return [...]string{"closed", "half_open", "open"}[s]
}

type Outcome int

const (
	// Success: the provider answered (2xx, or a 4xx that is the request's
	// fault, not the provider's).
	Success Outcome = iota
	// Failure: 5xx, 429, timeout or network error.
	Failure
	// Abort: the request never reached the provider (claim lost, shutdown).
	Abort
)

type Config struct {
	Threshold int
	Cooldown  time.Duration
	// OnChange, if set, is called with the new state on every transition
	// (for the circuit_breaker_state metric). It runs under the lock and
	// must not call back into the breaker.
	OnChange func(channel string, s State)
}

type Breaker struct {
	cfg Config
	now func() time.Time

	mu       sync.Mutex
	channels map[string]*circuit
}

type circuit struct {
	state       State
	failures    int // consecutive, while closed
	openedAt    time.Time
	trialActive bool // half-open: the single trial is in flight
}

func New(cfg Config) *Breaker {
	return &Breaker{cfg: cfg, now: time.Now, channels: map[string]*circuit{}}
}

// Ticket is a permission to call the provider. Report must be called once
// with the outcome; later calls are ignored, so callers can defer
// Report(Abort) as a safety net.
type Ticket struct {
	b       *Breaker
	channel string
	trial   bool
	once    sync.Once
}

// Allow asks to call the provider for channel. ok is false while the
// circuit is open, or half-open with its trial already in flight.
func (b *Breaker) Allow(channel string) (t *Ticket, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.circuit(channel)

	if c.state == Open && b.now().Sub(c.openedAt) >= b.cfg.Cooldown {
		b.set(channel, c, HalfOpen)
	}
	switch c.state {
	case Open:
		return nil, false
	case HalfOpen:
		if c.trialActive {
			return nil, false
		}
		c.trialActive = true
		return &Ticket{b: b, channel: channel, trial: true}, true
	default:
		return &Ticket{b: b, channel: channel}, true
	}
}

// State reports the current state, for logs and tests.
func (b *Breaker) State(channel string) State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.circuit(channel).state
}

// Cooldown is how long an open circuit stays open.
func (b *Breaker) Cooldown() time.Duration { return b.cfg.Cooldown }

func (t *Ticket) Report(o Outcome) {
	if t == nil {
		return
	}
	t.once.Do(func() { t.b.report(t, o) })
}

func (b *Breaker) report(t *Ticket, o Outcome) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.circuit(t.channel)

	if t.trial {
		c.trialActive = false
		switch o {
		case Success:
			c.failures = 0
			b.set(t.channel, c, Closed)
		case Failure:
			c.openedAt = b.now()
			b.set(t.channel, c, Open)
		case Abort:
			// No verdict; the next Allow may run another trial.
		}
		return
	}

	// A ticket issued while closed. If the circuit opened meanwhile, the
	// result of this older call no longer changes anything.
	if c.state != Closed {
		return
	}
	switch o {
	case Success:
		c.failures = 0
	case Failure:
		c.failures++
		if c.failures >= b.cfg.Threshold {
			c.openedAt = b.now()
			b.set(t.channel, c, Open)
		}
	case Abort:
	}
}

func (b *Breaker) circuit(channel string) *circuit {
	c, ok := b.channels[channel]
	if !ok {
		c = &circuit{}
		b.channels[channel] = c
	}
	return c
}

func (b *Breaker) set(channel string, c *circuit, s State) {
	c.state = s
	if b.cfg.OnChange != nil {
		b.cfg.OnChange(channel, s)
	}
}
