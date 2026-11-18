//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/hkln4/go-notification-service/internal/domain"
)

func TestTemplates(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	tpl := &domain.Template{ID: uuid.New(), Name: "otp", Channel: domain.ChannelSMS, Body: "Code {{code}}"}
	require.NoError(t, s.CreateTemplate(ctx, tpl))
	require.False(t, tpl.CreatedAt.IsZero())

	err := s.CreateTemplate(ctx, &domain.Template{ID: uuid.New(), Name: "otp", Channel: domain.ChannelEmail, Body: "x"})
	require.ErrorIs(t, err, domain.ErrNameTaken)

	got, err := s.GetTemplate(ctx, tpl.ID)
	require.NoError(t, err)
	require.Equal(t, "Code {{code}}", got.Body)
	_, err = s.GetTemplate(ctx, uuid.New())
	require.ErrorIs(t, err, domain.ErrNotFound)

	all, err := s.ListTemplates(ctx)
	require.NoError(t, err)
	require.Len(t, all, 1)

	n := newNotification(func(n *domain.Notification) { n.TemplateID = &tpl.ID; n.Content = "Code 1234" })
	insert(t, s, n)
	stored, err := s.GetNotification(ctx, n.ID)
	require.NoError(t, err)
	require.Equal(t, tpl.ID, *stored.TemplateID)
	require.Equal(t, "Code 1234", stored.Content, "the rendered content is what is stored")
}
