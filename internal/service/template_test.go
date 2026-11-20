package service

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/hkln4/go-notification-service/internal/domain"
)

func TestCreateTemplate(t *testing.T) {
	tests := []struct {
		name      string
		in        TemplateInput
		wantField string
	}{
		{"valid", TemplateInput{Name: "order-shipped", Channel: "sms", Body: "Hi {{name}}"}, ""},
		{"bad name", TemplateInput{Name: "Order Shipped", Channel: "sms", Body: "x"}, "name"},
		{"bad channel", TemplateInput{Name: "a", Channel: "fax", Body: "x"}, "channel"},
		{"empty body", TemplateInput{Name: "a", Channel: "sms", Body: " "}, "body"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newTestService(newFakeStore()).CreateTemplate(context.Background(), tt.in)
			if tt.wantField == "" {
				require.NoError(t, err)
				return
			}
			var verr *domain.ValidationError
			require.ErrorAs(t, err, &verr)
			require.Equal(t, tt.wantField, verr.Fields[0].Field)
		})
	}

	svc := newTestService(newFakeStore())
	_, err := svc.CreateTemplate(context.Background(), TemplateInput{Name: "dup", Channel: "sms", Body: "x"})
	require.NoError(t, err)
	_, err = svc.CreateTemplate(context.Background(), TemplateInput{Name: "dup", Channel: "email", Body: "y"})
	require.ErrorIs(t, err, domain.ErrNameTaken)
}

func TestCreateFromTemplate(t *testing.T) {
	store := newFakeStore()
	svc := newTestService(store)
	ctx := context.Background()
	tpl, err := svc.CreateTemplate(ctx, TemplateInput{Name: "otp", Channel: "sms", Body: "Hi {{name}}, your code is {{code}}"})
	require.NoError(t, err)
	long, err := svc.CreateTemplate(ctx, TemplateInput{Name: "long", Channel: "sms", Body: strings.Repeat("x", 150) + "{{tail}}"})
	require.NoError(t, err)

	base := domain.NotificationInput{Recipient: "+905551234567", TemplateID: tpl.ID.String(),
		Variables: map[string]string{"name": "Ayşe", "code": "4821"}}
	with := func(mod func(*domain.NotificationInput)) domain.NotificationInput {
		in := base
		mod(&in)
		return in
	}

	tests := []struct {
		name        string
		in          domain.NotificationInput
		wantContent string
		wantField   string
	}{
		{"renders and takes the template's channel", base, "Hi Ayşe, your code is 4821", ""},
		{"explicit matching channel", with(func(in *domain.NotificationInput) { in.Channel = "sms" }), "Hi Ayşe, your code is 4821", ""},
		{"channel mismatch", with(func(in *domain.NotificationInput) { in.Channel = "email" }), "", "channel"},
		{"content and template", with(func(in *domain.NotificationInput) { in.Content = "x" }), "", "content"},
		{"missing variable", with(func(in *domain.NotificationInput) { in.Variables = map[string]string{"name": "A"} }), "", "variables"},
		{"bad template id", with(func(in *domain.NotificationInput) { in.TemplateID = "nope" }), "", "templateId"},
		{"unknown template", with(func(in *domain.NotificationInput) { in.TemplateID = uuid.NewString() }), "", "templateId"},
		{"variables without template", domain.NotificationInput{Channel: "sms", Recipient: "+905551234567", Content: "x",
			Variables: map[string]string{"a": "b"}}, "", "variables"},
		{"rendered text obeys channel limits", domain.NotificationInput{Recipient: "+905551234567", TemplateID: long.ID.String(),
			Variables: map[string]string{"tail": strings.Repeat("y", 11)}}, "", "content"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := svc.Create(ctx, CreateRequest{Input: tt.in})
			if tt.wantField != "" {
				var verr *domain.ValidationError
				require.ErrorAs(t, err, &verr)
				require.Equal(t, tt.wantField, verr.Fields[0].Field)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantContent, res.Notification.Content)
			require.Equal(t, domain.ChannelSMS, res.Notification.Channel)
			require.Equal(t, tpl.ID, *res.Notification.TemplateID)
		})
	}
}

func TestBatchFromTemplateLoadsItOnce(t *testing.T) {
	store := newFakeStore()
	svc := newTestService(store)
	ctx := context.Background()
	tpl, err := svc.CreateTemplate(ctx, TemplateInput{Name: "hi", Channel: "push", Body: "Hi {{n}}"})
	require.NoError(t, err)

	items := make([]BatchItem, 3)
	for i := range items {
		items[i] = BatchItem{Input: domain.NotificationInput{Recipient: "tok", TemplateID: tpl.ID.String(),
			Variables: map[string]string{"n": string(rune('A' + i))}}}
	}
	res, err := svc.CreateBatch(ctx, CreateBatchRequest{Items: items})
	require.NoError(t, err)
	require.Equal(t, "Hi C", store.notifications[res.NotificationIDs[2]].Content)
}

// The created-notifications metric must use the channel the template
// resolved, not the request's (possibly empty) one, and skip duplicates.
func TestBatchCreatedChannels(t *testing.T) {
	store := newFakeStore()
	svc := newTestService(store)
	ctx := context.Background()
	tpl, err := svc.CreateTemplate(ctx, TemplateInput{Name: "promo", Channel: "push", Body: "Hi {{n}}"})
	require.NoError(t, err)

	fromTemplate := BatchItem{Input: domain.NotificationInput{Recipient: "tok", TemplateID: tpl.ID.String(),
		Variables: map[string]string{"n": "A"}}}
	sms := BatchItem{Input: domain.NotificationInput{Channel: "sms", Recipient: "+905551234567", Content: "x"}, IdempotencyKey: "k1"}

	first, err := svc.CreateBatch(ctx, CreateBatchRequest{Items: []BatchItem{fromTemplate, sms}})
	require.NoError(t, err)
	require.Equal(t, []domain.Channel{domain.ChannelPush, domain.ChannelSMS}, first.CreatedChannels)

	again, err := svc.CreateBatch(ctx, CreateBatchRequest{Items: []BatchItem{sms, fromTemplate}})
	require.NoError(t, err)
	require.Equal(t, []int{0}, again.Duplicates)
	require.Equal(t, []domain.Channel{domain.ChannelPush}, again.CreatedChannels, "the duplicate is not counted again")
}
