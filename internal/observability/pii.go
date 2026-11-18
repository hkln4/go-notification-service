package observability

import (
	"log/slog"
	"strings"
)

// piiKeys are log attribute keys whose values are always masked, wherever
// they are logged. Masking in the handler rather than at call sites means a
// new log line can't leak a recipient by accident.
var piiKeys = map[string]bool{
	"recipient": true,
	"to":        true,
	"email":     true,
	"phone":     true,
}

// MaskRecipient hides most of a phone number, email address or push token
// while keeping enough to correlate log lines:
//
//	+905551234567    → +90555***4567
//	hasan@gmail.com  → h***@gmail.com
//	device-token-xyz → devi***
func MaskRecipient(s string) string {
	switch {
	case s == "":
		return ""
	case strings.HasPrefix(s, "+"):
		if len(s) <= 10 {
			return s[:min(3, len(s))] + "***"
		}
		return s[:6] + "***" + s[len(s)-4:]
	case strings.Contains(s, "@"):
		at := strings.LastIndex(s, "@")
		if at == 0 {
			return "***" + s[at:]
		}
		return s[:1] + "***" + s[at:]
	default:
		return s[:min(4, len(s))] + "***"
	}
}

func maskPII(_ []string, a slog.Attr) slog.Attr {
	if piiKeys[a.Key] && a.Value.Kind() == slog.KindString {
		return slog.String(a.Key, MaskRecipient(a.Value.String()))
	}
	return a
}
