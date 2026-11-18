// Package relay moves outbox rows to RabbitMQ: claim → publish → delete.
//
// The API writes a notification and its outbox row in one transaction and
// never touches the broker, so a broker outage cannot lose or half-create a
// notification. The relay is the only thing that turns committed rows into
// messages.
package relay

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/hkln4/go-notification-service/internal/broker"
	"github.com/hkln4/go-notification-service/internal/domain"
	"github.com/hkln4/go-notification-service/internal/observability"
	"github.com/hkln4/go-notification-service/internal/store/postgres"
)

type Store interface {
	ClaimOutbox(ctx context.Context, limit int, lease time.Duration) ([]postgres.OutboxRow, error)
	DeleteOutbox(ctx context.Context, ids []int64) error
}

type Publisher interface {
	Publish(ctx context.Context, msgs ...broker.Message) []error
	Connected() bool
}

type Config struct {
	BatchSize    int
	PollInterval time.Duration
	// Lease is how long claimed rows stay invisible to other relays. If
	// publishing fails, the rows are retried when it runs out.
	Lease time.Duration
}

type Relay struct {
	store Store
	pub   Publisher
	cfg   Config
	log   *slog.Logger
}

func New(store Store, pub Publisher, cfg Config, log *slog.Logger) *Relay {
	return &Relay{store: store, pub: pub, cfg: cfg, log: log.With("component", "relay")}
}

// Run polls until ctx is cancelled. After a full batch it polls again at
// once, since more rows are probably waiting; otherwise it sleeps for
// PollInterval.
func (r *Relay) Run(ctx context.Context) {
	for {
		n, err := r.Tick(ctx)
		if err != nil && ctx.Err() == nil {
			r.log.Error("relay tick failed", "err", err)
		}
		if n == r.cfg.BatchSize && err == nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		select {
		case <-time.After(r.cfg.PollInterval):
		case <-ctx.Done():
			return
		}
	}
}

// Tick runs one claim → publish → delete cycle and returns how many rows it
// claimed.
//
// Ordering matters: rows are deleted only after the broker confirmed them.
// A crash between publish and delete publishes the rows again after the
// lease expires. That duplicate is accepted: the worker's claim lets only
// one delivery of a notification through.
func (r *Relay) Tick(ctx context.Context) (int, error) {
	// Claiming while the broker is down would only lease rows we can't
	// publish, delaying them by a full lease after the broker returns.
	if !r.pub.Connected() {
		return 0, nil
	}
	rows, err := r.store.ClaimOutbox(ctx, r.cfg.BatchSize, r.cfg.Lease)
	if err != nil || len(rows) == 0 {
		return 0, err
	}

	msgs := make([]broker.Message, len(rows))
	for i, row := range rows {
		msgs[i] = toMessage(row)
	}
	errs := r.pub.Publish(ctx, msgs...)

	confirmed := make([]int64, 0, len(rows))
	for i, err := range errs {
		if err != nil {
			r.log.Warn("publish failed; retrying after lease",
				"notification_id", rows[i].NotificationID, "err", err, "lease", r.cfg.Lease.String())
			continue
		}
		confirmed = append(confirmed, rows[i].ID)
	}
	if err := r.store.DeleteOutbox(ctx, confirmed); err != nil {
		// The messages are out; the rows will be published again after the
		// lease. Duplicate, not loss.
		return len(rows), err
	}
	r.log.Debug("relayed", "claimed", len(rows), "published", len(confirmed))
	return len(rows), nil
}

func toMessage(row postgres.OutboxRow) broker.Message {
	body, _ := json.Marshal(domain.DeliveryMessage{
		NotificationID: row.NotificationID,
		Channel:        domain.Channel(row.RoutingKey),
	})
	headers := amqp.Table{}
	for k, v := range row.Headers {
		headers[k] = v
	}
	return broker.Message{
		Exchange:      broker.ExchangeNotifications,
		RoutingKey:    row.RoutingKey,
		Body:          body,
		Priority:      uint8(row.Priority), //nolint:gosec // 0..2, enforced by a CHECK constraint
		MessageID:     row.NotificationID.String(),
		CorrelationID: row.Headers[observability.CorrelationHeaderKey],
		Headers:       headers,
	}
}
