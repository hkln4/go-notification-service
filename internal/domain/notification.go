// Package domain holds the notification model and its validation rules.
// It has no dependencies on storage, transport or the clock.
package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Channel string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// Channels lists every supported channel in a stable order.
var Channels = []Channel{ChannelSMS, ChannelEmail, ChannelPush}

func ParseChannel(s string) (Channel, error) {
	switch c := Channel(s); c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return c, nil
	}
	return "", fmt.Errorf("unknown channel %q", s)
}

// Priority maps to the AMQP message priority (queues use x-max-priority=2),
// so the numeric values matter.
type Priority int16

const (
	PriorityLow    Priority = 0
	PriorityNormal Priority = 1
	PriorityHigh   Priority = 2
)

// ParsePriority accepts high|normal|low. An empty string means normal.
func ParsePriority(s string) (Priority, error) {
	switch s {
	case "high":
		return PriorityHigh, nil
	case "normal", "":
		return PriorityNormal, nil
	case "low":
		return PriorityLow, nil
	}
	return 0, fmt.Errorf("unknown priority %q", s)
}

func (p Priority) String() string {
	switch p {
	case PriorityHigh:
		return "high"
	case PriorityNormal:
		return "normal"
	case PriorityLow:
		return "low"
	}
	return fmt.Sprintf("Priority(%d)", int16(p))
}

func (p Priority) MarshalText() ([]byte, error) { return []byte(p.String()), nil }

func (p *Priority) UnmarshalText(b []byte) error {
	v, err := ParsePriority(string(b))
	if err != nil {
		return err
	}
	*p = v
	return nil
}

type Status string

const (
	StatusPending    Status = "pending"
	StatusScheduled  Status = "scheduled"
	StatusProcessing Status = "processing"
	StatusRetrying   Status = "retrying"
	StatusSent       Status = "sent"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

// Statuses lists every status in lifecycle order.
var Statuses = []Status{
	StatusPending, StatusScheduled, StatusProcessing, StatusRetrying,
	StatusSent, StatusFailed, StatusCancelled,
}

func ParseStatus(s string) (Status, error) {
	for _, st := range Statuses {
		if string(st) == s {
			return st, nil
		}
	}
	return "", fmt.Errorf("unknown status %q", s)
}

// IsTerminal reports whether no further transition is possible.
func (s Status) IsTerminal() bool {
	return s == StatusSent || s == StatusFailed || s == StatusCancelled
}

// IsCancellable reports whether the notification has not been picked up yet.
// A processing notification may already be on its way to the provider.
func (s Status) IsCancellable() bool {
	return s == StatusPending || s == StatusScheduled || s == StatusRetrying
}

type Notification struct {
	ID                uuid.UUID
	BatchID           *uuid.UUID
	IdempotencyKey    *string
	RequestHash       *string
	Channel           Channel
	Recipient         string
	Content           string
	Priority          Priority
	Status            Status
	Attempts          int
	ScheduledAt       *time.Time
	ProviderMessageID *string
	LastError         *string
	CorrelationID     string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// Attempt is one delivery try against the provider.
type Attempt struct {
	AttemptNo  int
	HTTPStatus *int
	Error      *string
	LatencyMS  int
	CreatedAt  time.Time
}

type Batch struct {
	ID             uuid.UUID
	IdempotencyKey *string
	RequestHash    *string
	TotalCount     int
	CreatedAt      time.Time
}

// ListFilter selects notifications newest first. Nil fields are not
// filtered on. From is inclusive, To is exclusive.
type ListFilter struct {
	Status  *Status
	Channel *Channel
	From    *time.Time
	To      *time.Time
	// After is the keyset cursor: the (created_at, id) of the last row of
	// the previous page.
	AfterCreatedAt *time.Time
	AfterID        uuid.UUID
	Limit          int
}
