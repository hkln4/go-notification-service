package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/hkln4/go-notification-service/internal/domain"
)

const (
	DefaultPageSize = 50
	MaxPageSize     = 200
)

// ListQuery holds the raw query parameters; the service validates them.
type ListQuery struct {
	Status  string
	Channel string
	From    *time.Time
	To      *time.Time
	Cursor  string
	Limit   int
}

type Page struct {
	Items []domain.Notification
	// NextCursor is empty on the last page.
	NextCursor string
}

// cursor is opaque to clients (base64 JSON) so its shape can change later.
type cursor struct {
	CreatedAt time.Time `json:"t"`
	ID        uuid.UUID `json:"id"`
}

func encodeCursor(n domain.Notification) string {
	b, _ := json.Marshal(cursor{CreatedAt: n.CreatedAt, ID: n.ID})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (cursor, bool) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return cursor{}, false
	}
	var c cursor
	if err := json.Unmarshal(b, &c); err != nil || c.CreatedAt.IsZero() {
		return cursor{}, false
	}
	return c, true
}

func (s *Service) List(ctx context.Context, q ListQuery) (Page, error) {
	f, err := q.filter()
	if err != nil {
		return Page{}, err
	}
	want := f.Limit
	// Ask for one extra row: if it exists there is a next page, and no
	// separate COUNT query is needed.
	f.Limit++
	ns, err := s.store.ListNotifications(ctx, f)
	if err != nil {
		return Page{}, err
	}
	page := Page{Items: ns}
	if len(ns) > want {
		page.Items = ns[:want]
		page.NextCursor = encodeCursor(page.Items[want-1])
	}
	return page, nil
}

func (q ListQuery) filter() (domain.ListFilter, error) {
	var verr domain.ValidationError
	f := domain.ListFilter{From: q.From, To: q.To, Limit: q.Limit}

	if q.Status != "" {
		if st, err := domain.ParseStatus(q.Status); err != nil {
			verr.Fields = append(verr.Fields, domain.FieldError{Field: "status", Message: "unknown status"})
		} else {
			f.Status = &st
		}
	}
	if q.Channel != "" {
		if ch, err := domain.ParseChannel(q.Channel); err != nil {
			verr.Fields = append(verr.Fields, domain.FieldError{Field: "channel", Message: "must be one of sms, email, push"})
		} else {
			f.Channel = &ch
		}
	}
	if q.From != nil && q.To != nil && !q.From.Before(*q.To) {
		verr.Fields = append(verr.Fields, domain.FieldError{Field: "to", Message: "must be after from"})
	}
	switch {
	case q.Limit == 0:
		f.Limit = DefaultPageSize
	case q.Limit < 0 || q.Limit > MaxPageSize:
		verr.Fields = append(verr.Fields, domain.FieldError{Field: "limit", Message: "must be between 1 and 200"})
	}
	if q.Cursor != "" {
		if c, ok := decodeCursor(q.Cursor); !ok {
			verr.Fields = append(verr.Fields, domain.FieldError{Field: "cursor", Message: "is invalid"})
		} else {
			f.AfterCreatedAt, f.AfterID = &c.CreatedAt, c.ID
		}
	}

	if len(verr.Fields) > 0 {
		return domain.ListFilter{}, &verr
	}
	return f, nil
}
