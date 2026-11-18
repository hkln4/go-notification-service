package ws

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/hkln4/go-notification-service/internal/domain"
)

func TestHubRoutesByNotificationAndBatch(t *testing.T) {
	h := NewHub()
	nID, bID, other := uuid.New(), uuid.New(), uuid.New()
	byNotification := h.subscribe(nID)
	byBatch := h.subscribe(bID)
	unrelated := h.subscribe(other)

	h.Publish(domain.StatusEvent{NotificationID: nID, BatchID: &bID, Status: domain.StatusSent})

	require.Equal(t, domain.StatusSent, (<-byNotification.events).Status)
	require.Equal(t, domain.StatusSent, (<-byBatch.events).Status)
	require.Empty(t, unrelated.events)

	h.unsubscribe(nID, byNotification)
	require.Zero(t, h.Subscribers(nID))
}

func TestHubDropsSlowClient(t *testing.T) {
	h := NewHub()
	id := uuid.New()
	slow := h.subscribe(id)
	for range clientBuffer + 1 {
		h.Publish(domain.StatusEvent{NotificationID: id}) // must never block
	}
	select {
	case <-slow.gone:
	default:
		t.Fatal("a client with a full buffer must be dropped")
	}
}

func newServer(t *testing.T, hub *Hub, snap Snapshot) *httptest.Server {
	srv := httptest.NewServer(NewHandler(hub, snap, nil, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(srv.Close)
	return srv
}

func dial(t *testing.T, srv *httptest.Server, query string) *websocket.Conn {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/?"+query, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

func read(t *testing.T, conn *websocket.Conn) domain.StatusEvent {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, b, err := conn.Read(ctx)
	require.NoError(t, err)
	var e domain.StatusEvent
	require.NoError(t, json.Unmarshal(b, &e))
	return e
}

func TestHandlerSendsSnapshotThenEvents(t *testing.T) {
	hub := NewHub()
	id := uuid.New()
	srv := newServer(t, hub, func(_ context.Context, got uuid.UUID) (domain.Notification, error) {
		return domain.Notification{ID: got, Status: domain.StatusRetrying, Attempts: 2}, nil
	})
	conn := dial(t, srv, "notificationId="+id.String())

	snap := read(t, conn)
	require.Equal(t, domain.StatusRetrying, snap.Status, "current state first")
	require.Equal(t, 2, snap.Attempts)

	require.Eventually(t, func() bool { return hub.Subscribers(id) == 1 }, time.Second, 5*time.Millisecond)
	hub.Publish(domain.StatusEvent{NotificationID: id, Status: domain.StatusSent, Attempts: 3})
	require.Equal(t, domain.StatusSent, read(t, conn).Status)

	require.NoError(t, conn.Close(websocket.StatusNormalClosure, ""))
	require.Eventually(t, func() bool { return hub.Subscribers(id) == 0 }, time.Second, 5*time.Millisecond,
		"closing the socket unsubscribes")
}

func TestHandlerBatchSubscription(t *testing.T) {
	hub := NewHub()
	batch := uuid.New()
	srv := newServer(t, hub, nil)
	conn := dial(t, srv, "batchId="+batch.String())

	require.Eventually(t, func() bool { return hub.Subscribers(batch) == 1 }, time.Second, 5*time.Millisecond)
	for _, st := range []domain.Status{domain.StatusProcessing, domain.StatusSent} {
		hub.Publish(domain.StatusEvent{NotificationID: uuid.New(), BatchID: &batch, Status: st})
	}
	require.Equal(t, domain.StatusProcessing, read(t, conn).Status)
	require.Equal(t, domain.StatusSent, read(t, conn).Status)
}

func TestHandlerRejectsBadQuery(t *testing.T) {
	srv := newServer(t, NewHub(), nil)
	for _, q := range []string{"", "notificationId=x", "notificationId=" + uuid.NewString() + "&batchId=" + uuid.NewString()} {
		resp, err := http.Get(srv.URL + "/?" + q)
		require.NoError(t, err)
		_ = resp.Body.Close()
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, q)
	}
}
