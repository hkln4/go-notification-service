package domain

import (
	"time"

	"github.com/google/uuid"
)

// DeliveryMessage is the body of every work, retry and DLQ message. It only
// identifies the notification: PostgreSQL is the source of truth, so the
// worker reads the current state when it claims the notification instead of
// trusting a copy that may be stale (e.g. cancelled since).
type DeliveryMessage struct {
	NotificationID uuid.UUID `json:"notificationId"`
	Channel        Channel   `json:"channel"`
}

// StatusEvent is published to the notification.events fanout exchange on
// every status change the worker makes.
type StatusEvent struct {
	NotificationID uuid.UUID  `json:"notificationId"`
	BatchID        *uuid.UUID `json:"batchId,omitempty"`
	Status         Status     `json:"status"`
	Attempts       int        `json:"attempts"`
	At             time.Time  `json:"at"`
}
