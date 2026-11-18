package service

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/hkln4/go-notification-service/internal/domain"
	"github.com/hkln4/go-notification-service/internal/template"
)

const maxTemplateBody = domain.MaxEmailContent

type TemplateInput struct {
	Name    string
	Channel string
	Body    string
}

func (s *Service) CreateTemplate(ctx context.Context, in TemplateInput) (domain.Template, error) {
	var verr domain.ValidationError
	if !template.ValidName(in.Name) {
		verr.Fields = append(verr.Fields, domain.FieldError{Field: "name", Message: "must be 1-100 characters of a-z, 0-9, '-' or '_'"})
	}
	ch, err := domain.ParseChannel(in.Channel)
	if err != nil {
		verr.Fields = append(verr.Fields, domain.FieldError{Field: "channel", Message: "must be one of sms, email, push"})
	}
	if strings.TrimSpace(in.Body) == "" || utf8.RuneCountInString(in.Body) > maxTemplateBody {
		verr.Fields = append(verr.Fields, domain.FieldError{Field: "body", Message: "must be 1-10000 characters"})
	}
	if len(verr.Fields) > 0 {
		return domain.Template{}, &verr
	}

	t := domain.Template{ID: s.newID(), Name: in.Name, Channel: ch, Body: in.Body}
	if err := s.store.CreateTemplate(ctx, &t); err != nil {
		return domain.Template{}, err
	}
	return t, nil
}

func (s *Service) GetTemplate(ctx context.Context, id uuid.UUID) (domain.Template, error) {
	return s.store.GetTemplate(ctx, id)
}

func (s *Service) ListTemplates(ctx context.Context) ([]domain.Template, error) {
	return s.store.ListTemplates(ctx)
}

// templateCache avoids loading the same template once per batch item.
type templateCache map[uuid.UUID]domain.Template

// prepare resolves a template, if any, into the input's content and then
// validates the result. Rendering first means the rendered text is held to
// the same channel limits as typed content (160 characters for SMS, ...).
func (s *Service) prepare(ctx context.Context, in domain.NotificationInput, cache templateCache) (domain.NewNotification, error) {
	var templateID *uuid.UUID
	if in.TemplateID != "" {
		t, ferr, err := s.loadTemplate(ctx, in.TemplateID, cache)
		if err != nil {
			return domain.NewNotification{}, err
		}
		if ferr == nil {
			ferr = applyTemplate(&in, t)
		}
		if ferr != nil {
			return domain.NewNotification{}, &domain.ValidationError{Fields: []domain.FieldError{*ferr}}
		}
		templateID = &t.ID
	} else if len(in.Variables) > 0 {
		return domain.NewNotification{}, &domain.ValidationError{Fields: []domain.FieldError{{
			Field: "variables", Message: "only allowed together with templateId",
		}}}
	}

	nn, err := in.Validate(s.now())
	if err != nil {
		return domain.NewNotification{}, err
	}
	nn.TemplateID = templateID
	return nn, nil
}

// loadTemplate returns a field error for a bad or unknown ID and a plain
// error only for infrastructure failures.
func (s *Service) loadTemplate(ctx context.Context, rawID string, cache templateCache) (domain.Template, *domain.FieldError, error) {
	id, err := uuid.Parse(rawID)
	if err != nil {
		return domain.Template{}, &domain.FieldError{Field: "templateId", Message: "must be a UUID"}, nil
	}
	if t, ok := cache[id]; ok {
		return t, nil, nil
	}
	t, err := s.store.GetTemplate(ctx, id)
	if errors.Is(err, domain.ErrNotFound) {
		return domain.Template{}, &domain.FieldError{Field: "templateId", Message: "template not found"}, nil
	}
	if err != nil {
		return domain.Template{}, nil, err
	}
	cache[id] = t
	return t, nil, nil
}

func applyTemplate(in *domain.NotificationInput, t domain.Template) *domain.FieldError {
	if in.Content != "" {
		return &domain.FieldError{Field: "content", Message: "must be empty when templateId is set"}
	}
	switch in.Channel {
	case "":
		in.Channel = string(t.Channel)
	case string(t.Channel):
	default:
		return &domain.FieldError{Field: "channel", Message: "must match the template's channel (" + string(t.Channel) + ")"}
	}
	content, missing := template.Render(t.Body, in.Variables)
	if len(missing) > 0 {
		return &domain.FieldError{Field: "variables", Message: "missing: " + strings.Join(missing, ", ")}
	}
	in.Content = content
	return nil
}
