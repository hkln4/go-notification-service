package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var now = time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

func ptr[T any](v T) *T { return &v }

func TestNotificationInputValidate(t *testing.T) {
	valid := func(mod func(*NotificationInput)) NotificationInput {
		in := NotificationInput{Channel: "sms", Recipient: "+905551234567", Content: "hello", Priority: "high"}
		if mod != nil {
			mod(&in)
		}
		return in
	}

	tests := []struct {
		name       string
		in         NotificationInput
		wantFields []string // invalid fields, in order; nil means valid
	}{
		{"valid sms", valid(nil), nil},
		{"sms min length number", valid(func(in *NotificationInput) { in.Recipient = "+90555123" }), nil},
		{"sms max length number", valid(func(in *NotificationInput) { in.Recipient = "+123456789012345" }), nil},
		{"sms number without plus", valid(func(in *NotificationInput) { in.Recipient = "905551234567" }), []string{"recipient"}},
		{"sms number starting with 0", valid(func(in *NotificationInput) { in.Recipient = "+05551234567" }), []string{"recipient"}},
		{"sms number too long", valid(func(in *NotificationInput) { in.Recipient = "+1234567890123456" }), []string{"recipient"}},
		{"sms content at limit", valid(func(in *NotificationInput) { in.Content = strings.Repeat("a", 160) }), nil},
		{"sms content over limit", valid(func(in *NotificationInput) { in.Content = strings.Repeat("a", 161) }), []string{"content"}},
		{"sms limit counts runes not bytes", valid(func(in *NotificationInput) { in.Content = strings.Repeat("ş", 160) }), nil},

		{"valid email", NotificationInput{Channel: "email", Recipient: "user@example.com", Content: "hi"}, nil},
		{"email with display name", NotificationInput{Channel: "email", Recipient: "User <user@example.com>", Content: "hi"}, []string{"recipient"}},
		{"email missing domain", NotificationInput{Channel: "email", Recipient: "user@", Content: "hi"}, []string{"recipient"}},
		{"email too long", NotificationInput{Channel: "email", Recipient: strings.Repeat("a", 250) + "@b.co", Content: "hi"}, []string{"recipient"}},
		{"email content at limit", NotificationInput{Channel: "email", Recipient: "a@b.co", Content: strings.Repeat("a", 10_000)}, nil},
		{"email content over limit", NotificationInput{Channel: "email", Recipient: "a@b.co", Content: strings.Repeat("a", 10_001)}, []string{"content"}},

		{"valid push", NotificationInput{Channel: "push", Recipient: "device-token", Content: "hi"}, nil},
		{"push token at limit", NotificationInput{Channel: "push", Recipient: strings.Repeat("t", 256), Content: "hi"}, nil},
		{"push token over limit", NotificationInput{Channel: "push", Recipient: strings.Repeat("t", 257), Content: "hi"}, []string{"recipient"}},
		{"push content over limit", NotificationInput{Channel: "push", Recipient: "tok", Content: strings.Repeat("a", 257)}, []string{"content"}},

		{"missing channel", valid(func(in *NotificationInput) { in.Channel = "" }), []string{"channel"}},
		{"unknown channel", valid(func(in *NotificationInput) { in.Channel = "fax" }), []string{"channel"}},
		{"missing recipient", valid(func(in *NotificationInput) { in.Recipient = "" }), []string{"recipient"}},
		{"blank content", valid(func(in *NotificationInput) { in.Content = "   " }), []string{"content"}},
		{"unknown priority", valid(func(in *NotificationInput) { in.Priority = "urgent" }), []string{"priority"}},
		{"empty priority defaults", valid(func(in *NotificationInput) { in.Priority = "" }), nil},
		{"all errors reported at once", NotificationInput{Channel: "fax", Priority: "x"}, []string{"channel", "priority", "content", "recipient"}},

		{"scheduled in future", valid(func(in *NotificationInput) { in.ScheduledAt = ptr(now.Add(time.Minute)) }), nil},
		{"scheduled now", valid(func(in *NotificationInput) { in.ScheduledAt = ptr(now) }), []string{"scheduledAt"}},
		{"scheduled in past", valid(func(in *NotificationInput) { in.ScheduledAt = ptr(now.Add(-time.Second)) }), []string{"scheduledAt"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.in.Validate(now)
			if tt.wantFields == nil {
				require.NoError(t, err)
				return
			}
			var verr *ValidationError
			require.ErrorAs(t, err, &verr)
			var got []string
			for _, f := range verr.Fields {
				got = append(got, f.Field)
			}
			require.Equal(t, tt.wantFields, got)
		})
	}
}

func TestValidateNormalizes(t *testing.T) {
	ist := time.FixedZone("IST", 3*60*60)
	at := now.Add(time.Hour).In(ist)

	got, err := NotificationInput{Channel: "sms", Recipient: "+905551234567", Content: "x", ScheduledAt: &at}.Validate(now)
	require.NoError(t, err)
	require.Equal(t, PriorityNormal, got.Priority)
	require.Equal(t, time.UTC, got.ScheduledAt.Location())
	require.True(t, got.ScheduledAt.Equal(at))
}

// The request hash is computed from this encoding, so it must not depend on
// how the client ordered its JSON fields or which time zone it used.
func TestNewNotificationJSONIsStable(t *testing.T) {
	a := NewNotification{Channel: ChannelSMS, Recipient: "+905551234567", Content: "x", Priority: PriorityHigh}
	b, err := json.Marshal(a)
	require.NoError(t, err)
	// Exact string, not JSONEq: key order matters for the hash.
	require.Equal(t, `{"channel":"sms","recipient":"+905551234567","content":"x","priority":"high"}`, string(b))
}

func TestParsePriority(t *testing.T) {
	tests := []struct {
		in      string
		want    Priority
		wantErr bool
	}{
		{"high", PriorityHigh, false},
		{"normal", PriorityNormal, false},
		{"", PriorityNormal, false},
		{"low", PriorityLow, false},
		{"HIGH", 0, true},
	}
	for _, tt := range tests {
		got, err := ParsePriority(tt.in)
		if tt.wantErr {
			require.Error(t, err, tt.in)
			continue
		}
		require.NoError(t, err, tt.in)
		require.Equal(t, tt.want, got, tt.in)
	}
}

func TestStatusPredicates(t *testing.T) {
	tests := []struct {
		s           Status
		terminal    bool
		cancellable bool
	}{
		{StatusPending, false, true},
		{StatusScheduled, false, true},
		{StatusRetrying, false, true},
		{StatusProcessing, false, false},
		{StatusSent, true, false},
		{StatusFailed, true, false},
		{StatusCancelled, true, false},
	}
	for _, tt := range tests {
		require.Equal(t, tt.terminal, tt.s.IsTerminal(), "%s.IsTerminal()", tt.s)
		require.Equal(t, tt.cancellable, tt.s.IsCancellable(), "%s.IsCancellable()", tt.s)
	}
}
