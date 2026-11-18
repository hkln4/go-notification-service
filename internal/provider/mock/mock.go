// Package mock is a stand-in for the external provider (webhook.site in the
// assignment), with configurable latency and failure rate. It also records
// what it received, so a load test can check the rate limit and count
// duplicate deliveries from the provider's side.
package mock

import (
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/hkln4/go-notification-service/internal/ratelimit"
)

type Config struct {
	// FailureRate is the share of requests answered with 500, 0..1.
	FailureRate float64
	Latency     time.Duration
}

type Server struct {
	cfg  Config
	rand func() float64
	now  func() time.Time

	mu       sync.Mutex
	received map[string][]time.Time // channel -> arrival times
	keys     map[string]int         // Idempotency-Key -> times seen
}

func New(cfg Config) *Server {
	return &Server{
		cfg:      cfg,
		rand:     rand.Float64,
		now:      time.Now,
		received: map[string][]time.Time{},
		keys:     map[string]int{},
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /", s.send)
	mux.HandleFunc("GET /stats", s.stats)
	mux.HandleFunc("POST /stats/reset", s.reset)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

type sendRequest struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

func (s *Server) send(w http.ResponseWriter, r *http.Request) {
	var req sendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.To == "" || req.Channel == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}

	s.mu.Lock()
	s.received[req.Channel] = append(s.received[req.Channel], s.now())
	if k := r.Header.Get("Idempotency-Key"); k != "" {
		s.keys[k]++
	}
	s.mu.Unlock()

	if s.cfg.Latency > 0 {
		select {
		case <-time.After(s.cfg.Latency):
		case <-r.Context().Done():
			return
		}
	}
	if s.rand() < s.cfg.FailureRate {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "simulated failure"})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{
		"messageId": uuid.NewString(),
		"status":    "accepted",
		"timestamp": s.now().UTC().Format(time.RFC3339),
	})
}

type ChannelStats struct {
	Received int `json:"received"`
	// MaxPerSecond is the most requests seen in any sliding one-second
	// window.
	MaxPerSecond int `json:"maxPerSecond"`
}

type Stats struct {
	Channels map[string]ChannelStats `json:"channels"`
	// DuplicateKeys counts Idempotency-Keys seen more than once: messages
	// delivered more than once (at-least-once) or retried after a failure.
	DuplicateKeys int `json:"duplicateKeys"`
}

func (s *Server) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := Stats{Channels: map[string]ChannelStats{}}
	for ch, times := range s.received {
		st.Channels[ch] = ChannelStats{Received: len(times), MaxPerSecond: ratelimit.MaxInWindow(times, time.Second)}
	}
	for _, n := range s.keys {
		if n > 1 {
			st.DuplicateKeys++
		}
	}
	return st
}

func (s *Server) stats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Stats())
}

func (s *Server) reset(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.received = map[string][]time.Time{}
	s.keys = map[string]int{}
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
