// Package provider sends notifications to the external provider over HTTP.
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/hkln4/go-notification-service/internal/observability"
)

// DefaultTimeout bounds one provider call, including reading the response.
const DefaultTimeout = 5 * time.Second

const maxResponseBody = 64 << 10

// Request is the provider's wire format.
type Request struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type response struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// Result describes a response that arrived. Non-2xx statuses are results,
// not errors; the retry policy decides what they mean.
type Result struct {
	StatusCode int
	MessageID  string
	// RetryAfter is parsed from the Retry-After header (seconds or HTTP
	// date); zero if absent or unparsable.
	RetryAfter time.Duration
	Latency    time.Duration
	// BodyErr is set when a 2xx body could not be parsed. The provider
	// accepted the message, so it still counts as sent.
	BodyErr error
}

type Client struct {
	url     string
	http    *http.Client
	timeout time.Duration
	now     func() time.Time
}

func NewClient(url string, timeout time.Duration) *Client {
	return &Client{
		url: url,
		http: &http.Client{Transport: otelhttp.NewTransport(&http.Transport{
			Proxy: http.ProxyFromEnvironment,
			// The default of 2 idle connections per host would make most
			// requests open a new TCP (and TLS) connection at 100 msg/s.
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		})},
		timeout: timeout,
		now:     time.Now,
	}
}

// Send posts one notification. The notification ID goes out as the
// Idempotency-Key, so a provider that honours it can drop the duplicate that
// at-least-once delivery occasionally produces.
//
// The error is non-nil only when no response arrived (timeout, connection
// refused, ...): those are retried.
func (c *Client) Send(ctx context.Context, notificationID uuid.UUID, correlationID string, req Request) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	body, err := json.Marshal(req)
	if err != nil {
		return Result{}, fmt.Errorf("encode request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return Result{}, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", notificationID.String())
	if correlationID != "" {
		httpReq.Header.Set(observability.CorrelationHeader, correlationID)
	}

	start := c.now()
	resp, err := c.http.Do(httpReq)
	if err != nil {
		return Result{Latency: c.now().Sub(start)}, fmt.Errorf("post to provider: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	res := Result{
		StatusCode: resp.StatusCode,
		RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), c.now()),
		Latency:    c.now().Sub(start),
	}
	if err != nil {
		// Headers arrived but the body didn't; for a 2xx the provider has
		// the message, anything else is decided on the status code.
		res.BodyErr = fmt.Errorf("read response: %w", err)
		return res, nil
	}

	if res.StatusCode >= 200 && res.StatusCode < 300 {
		var r response
		if err := json.Unmarshal(raw, &r); err != nil {
			res.BodyErr = fmt.Errorf("decode response: %w", err)
		} else {
			res.MessageID = r.MessageID
		}
	}
	return res, nil
}

// parseRetryAfter handles both forms allowed by RFC 9110: delay-seconds and
// an HTTP date.
func parseRetryAfter(v string, now time.Time) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}
