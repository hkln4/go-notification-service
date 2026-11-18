package domain

import (
	"net/mail"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MaxSMSContent   = 160
	MaxEmailContent = 10_000
	MaxPushContent  = 256
	MaxPushToken    = 256
	// MaxEmailAddress is the RFC 5321 path limit.
	MaxEmailAddress = 254
)

var e164 = regexp.MustCompile(`^\+[1-9]\d{7,14}$`)

// NotificationInput is the raw request before validation.
type NotificationInput struct {
	Channel     string
	Recipient   string
	Content     string
	Priority    string
	ScheduledAt *time.Time
}

// NewNotification is a validated request. Its JSON encoding is the input of
// the idempotency request hash, so field order and tags must stay stable.
type NewNotification struct {
	Channel     Channel    `json:"channel"`
	Recipient   string     `json:"recipient"`
	Content     string     `json:"content"`
	Priority    Priority   `json:"priority"`
	ScheduledAt *time.Time `json:"scheduledAt,omitempty"`
}

// Validate checks every field and returns all problems at once as a
// *ValidationError. now is injected so scheduling checks are deterministic.
func (in NotificationInput) Validate(now time.Time) (NewNotification, error) {
	var verr ValidationError
	out := NewNotification{Recipient: in.Recipient, Content: in.Content}

	ch, err := ParseChannel(in.Channel)
	switch {
	case in.Channel == "":
		verr.add("channel", "is required")
	case err != nil:
		verr.add("channel", "must be one of sms, email, push")
	default:
		out.Channel = ch
	}

	if p, err := ParsePriority(in.Priority); err != nil {
		verr.add("priority", "must be one of high, normal, low")
	} else {
		out.Priority = p
	}

	if strings.TrimSpace(in.Content) == "" {
		verr.add("content", "is required")
	}
	if strings.TrimSpace(in.Recipient) == "" {
		verr.add("recipient", "is required")
	}

	// Channel specific rules only make sense once the channel is known and
	// the fields are present; otherwise the errors above already say enough.
	if out.Channel != "" {
		if in.Recipient != "" {
			if msg := validateRecipient(out.Channel, in.Recipient); msg != "" {
				verr.add("recipient", msg)
			}
		}
		if limit := maxContent(out.Channel); utf8.RuneCountInString(in.Content) > limit {
			verr.add("content", "must be at most "+strconv.Itoa(limit)+" characters for "+string(out.Channel))
		}
	}

	if in.ScheduledAt != nil {
		if !in.ScheduledAt.After(now) {
			verr.add("scheduledAt", "must be in the future")
		} else {
			t := in.ScheduledAt.UTC()
			out.ScheduledAt = &t
		}
	}

	if err := verr.orNil(); err != nil {
		return NewNotification{}, err
	}
	return out, nil
}

func validateRecipient(ch Channel, r string) string {
	switch ch {
	case ChannelSMS:
		if !e164.MatchString(r) {
			return "must be an E.164 phone number, e.g. +905551234567"
		}
	case ChannelEmail:
		// ParseAddress also accepts `Name <a@b.c>`; requiring the parsed
		// address to equal the input keeps the stored recipient canonical.
		addr, err := mail.ParseAddress(r)
		if err != nil || addr.Address != r {
			return "must be a plain email address, e.g. user@example.com"
		}
		if len(r) > MaxEmailAddress {
			return "must be at most " + strconv.Itoa(MaxEmailAddress) + " characters"
		}
	case ChannelPush:
		if utf8.RuneCountInString(r) > MaxPushToken {
			return "push token must be at most " + strconv.Itoa(MaxPushToken) + " characters"
		}
	}
	return ""
}

// maxContent counts characters (runes), not bytes. Real SMS segmentation
// depends on GSM-7 vs UCS-2 encoding; that is the provider's concern.
func maxContent(ch Channel) int {
	switch ch {
	case ChannelSMS:
		return MaxSMSContent
	case ChannelEmail:
		return MaxEmailContent
	default:
		return MaxPushContent
	}
}
