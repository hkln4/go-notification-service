package config

import (
	"strings"
	"testing"
	"time"
)

func TestReader(t *testing.T) {
	r := NewReaderFrom(map[string]string{
		"S": "value", "EMPTY": "", "I": "42", "BAD_I": "x", "D": "250ms", "BAD_D": "soon", "F": "0.25",
	})

	if got := r.String("S", "def"); got != "value" {
		t.Errorf("String = %q", got)
	}
	if got := r.String("EMPTY", "def"); got != "def" {
		t.Errorf("empty value should fall back to default, got %q", got)
	}
	if got := r.Int("I", 0); got != 42 {
		t.Errorf("Int = %d", got)
	}
	if got := r.Duration("D", 0); got != 250*time.Millisecond {
		t.Errorf("Duration = %v", got)
	}
	if got := r.Float("F", 0); got != 0.25 {
		t.Errorf("Float = %v", got)
	}
	if got := r.Int("MISSING", 7); got != 7 {
		t.Errorf("missing Int = %d", got)
	}
	if r.Err() != nil {
		t.Fatalf("unexpected error: %v", r.Err())
	}

	r.Int("BAD_I", 0)
	r.Duration("BAD_D", 0)
	err := r.Err()
	if err == nil || !strings.Contains(err.Error(), "BAD_I") || !strings.Contains(err.Error(), "BAD_D") {
		t.Fatalf("want both bad keys reported, got %v", err)
	}
}
