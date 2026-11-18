package httpapi

import (
	"time"

	"github.com/hkln4/go-notification-service/internal/domain"
	"github.com/hkln4/go-notification-service/internal/service"
)

type createNotificationRequest struct {
	Channel     string     `json:"channel" example:"sms"`
	Recipient   string     `json:"recipient" example:"+905551234567"`
	Content     string     `json:"content" example:"Your order has shipped"`
	Priority    string     `json:"priority,omitempty" enums:"high,normal,low" example:"high"`
	ScheduledAt *time.Time `json:"scheduledAt,omitempty"`
}

func (r createNotificationRequest) input() domain.NotificationInput {
	return domain.NotificationInput{
		Channel:     r.Channel,
		Recipient:   r.Recipient,
		Content:     r.Content,
		Priority:    r.Priority,
		ScheduledAt: r.ScheduledAt,
	}
}

type notificationResponse struct {
	ID                string     `json:"id"`
	BatchID           *string    `json:"batchId,omitempty"`
	Channel           string     `json:"channel"`
	Recipient         string     `json:"recipient"`
	Content           string     `json:"content"`
	Priority          string     `json:"priority"`
	Status            string     `json:"status"`
	Attempts          int        `json:"attempts"`
	ScheduledAt       *time.Time `json:"scheduledAt,omitempty"`
	ProviderMessageID *string    `json:"providerMessageId,omitempty"`
	LastError         *string    `json:"lastError,omitempty"`
	CorrelationID     string     `json:"correlationId,omitempty"`
	CreatedAt         time.Time  `json:"createdAt"`
	UpdatedAt         time.Time  `json:"updatedAt"`
}

func toNotificationResponse(n domain.Notification) notificationResponse {
	r := notificationResponse{
		ID:                n.ID.String(),
		Channel:           string(n.Channel),
		Recipient:         n.Recipient,
		Content:           n.Content,
		Priority:          n.Priority.String(),
		Status:            string(n.Status),
		Attempts:          n.Attempts,
		ScheduledAt:       n.ScheduledAt,
		ProviderMessageID: n.ProviderMessageID,
		LastError:         n.LastError,
		CorrelationID:     n.CorrelationID,
		CreatedAt:         n.CreatedAt,
		UpdatedAt:         n.UpdatedAt,
	}
	if n.BatchID != nil {
		s := n.BatchID.String()
		r.BatchID = &s
	}
	return r
}

type attemptResponse struct {
	AttemptNo  int       `json:"attemptNo"`
	HTTPStatus *int      `json:"httpStatus,omitempty"`
	Error      *string   `json:"error,omitempty"`
	LatencyMS  int       `json:"latencyMs"`
	CreatedAt  time.Time `json:"createdAt"`
}

// notificationDetailResponse embeds notificationResponse, so JSON flattens
// its fields and only adds deliveryAttempts.
type notificationDetailResponse struct {
	notificationResponse
	DeliveryAttempts []attemptResponse `json:"deliveryAttempts"`
}

func toDetailResponse(d service.NotificationDetail) notificationDetailResponse {
	attempts := make([]attemptResponse, len(d.Attempts))
	for i, a := range d.Attempts {
		attempts[i] = attemptResponse{
			AttemptNo:  a.AttemptNo,
			HTTPStatus: a.HTTPStatus,
			Error:      a.Error,
			LatencyMS:  a.LatencyMS,
			CreatedAt:  a.CreatedAt,
		}
	}
	return notificationDetailResponse{
		notificationResponse: toNotificationResponse(d.Notification),
		DeliveryAttempts:     attempts,
	}
}

type createBatchRequest struct {
	Notifications []batchItemRequest `json:"notifications"`
}

type batchItemRequest struct {
	createNotificationRequest
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
}

type createBatchResponse struct {
	BatchID string `json:"batchId"`
	// Count is how many notifications this batch created.
	Count           int      `json:"count"`
	NotificationIDs []string `json:"notificationIds"`
	Duplicates      []int    `json:"duplicates,omitempty"`
}

func toCreateBatchResponse(res service.BatchResult) createBatchResponse {
	ids := make([]string, len(res.NotificationIDs))
	for i, id := range res.NotificationIDs {
		ids[i] = id.String()
	}
	return createBatchResponse{
		BatchID:         res.Batch.ID.String(),
		Count:           res.Batch.TotalCount,
		NotificationIDs: ids,
		Duplicates:      res.Duplicates,
	}
}

type batchResponse struct {
	ID           string         `json:"id"`
	TotalCount   int            `json:"totalCount"`
	StatusCounts map[string]int `json:"statusCounts"`
	CreatedAt    time.Time      `json:"createdAt"`
}

// toBatchResponse lists every status, including zeros, so clients don't have
// to treat a missing key as zero.
func toBatchResponse(b service.BatchSummary) batchResponse {
	counts := make(map[string]int, len(domain.Statuses))
	for _, st := range domain.Statuses {
		counts[string(st)] = b.StatusCounts[st]
	}
	return batchResponse{ID: b.ID.String(), TotalCount: b.TotalCount, StatusCounts: counts, CreatedAt: b.CreatedAt}
}

type listResponse struct {
	Items      []notificationResponse `json:"items"`
	NextCursor string                 `json:"nextCursor,omitempty"`
}

func toListResponse(p service.Page) listResponse {
	items := make([]notificationResponse, len(p.Items))
	for i, n := range p.Items {
		items[i] = toNotificationResponse(n)
	}
	return listResponse{Items: items, NextCursor: p.NextCursor}
}
