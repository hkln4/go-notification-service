package broker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hkln4/go-notification-service/internal/domain"
)

// StatusPublisher sends status events to the notification.events fanout,
// where every API instance picks them up for its WebSocket clients.
type StatusPublisher struct {
	Client *Client
}

func (p StatusPublisher) PublishStatus(ctx context.Context, e domain.StatusEvent) error {
	body, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("encode status event: %w", err)
	}
	// Not mandatory: a fanout with no API instance listening is fine.
	return p.Client.Publish(ctx, Message{
		Exchange: ExchangeEvents, Body: body, MessageID: e.NotificationID.String(), NotMandatory: true,
	})[0]
}
