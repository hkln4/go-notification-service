package ws

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/hkln4/go-notification-service/internal/domain"
)

const (
	writeTimeout = 5 * time.Second
	pingInterval = 30 * time.Second
)

// Snapshot loads a notification's current state, sent on connect so a
// client doesn't miss changes that happened before it subscribed.
type Snapshot func(ctx context.Context, id uuid.UUID) (domain.Notification, error)

type Handler struct {
	hub      *Hub
	snapshot Snapshot
	log      *slog.Logger
	origins  []string
}

// NewHandler serves GET /ws?notificationId=… or ?batchId=…. origins are the
// allowed Origin patterns for browser clients (see websocket.AcceptOptions).
func NewHandler(hub *Hub, snapshot Snapshot, origins []string, log *slog.Logger) *Handler {
	return &Handler{hub: hub, snapshot: snapshot, origins: origins, log: log.With("component", "ws")}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key, isBatch, err := subscription(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// The server's read/write timeouts are meant for short requests and
	// would stay on the hijacked connection, killing a long-lived socket.
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Time{})
	_ = rc.SetWriteDeadline(time.Time{})

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: h.origins})
	if err != nil {
		return // Accept already wrote the response
	}
	defer func() { _ = conn.CloseNow() }()

	c := h.hub.subscribe(key)
	defer h.hub.unsubscribe(key, c)

	// The client only listens; CloseRead handles control frames and gives a
	// context that ends when the client goes away.
	ctx := conn.CloseRead(r.Context())

	if !isBatch {
		if n, err := h.snapshot(ctx, key); err == nil {
			_ = write(ctx, conn, domain.StatusEvent{
				NotificationID: n.ID, BatchID: n.BatchID, Status: n.Status, Attempts: n.Attempts, At: n.UpdatedAt,
			})
		}
	}

	ping := time.NewTicker(pingInterval)
	defer ping.Stop()
	for {
		select {
		case e := <-c.events:
			if err := write(ctx, conn, e); err != nil {
				return
			}
		case <-c.gone:
			_ = conn.Close(websocket.StatusPolicyViolation, "client too slow")
			return
		case <-ping.C:
			pctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := conn.Ping(pctx)
			cancel()
			if err != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func subscription(r *http.Request) (uuid.UUID, bool, error) {
	n, b := r.URL.Query().Get("notificationId"), r.URL.Query().Get("batchId")
	if (n == "") == (b == "") {
		return uuid.Nil, false, errors.New("exactly one of notificationId or batchId is required")
	}
	raw, isBatch := n, false
	if b != "" {
		raw, isBatch = b, true
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, false, errors.New("id must be a UUID")
	}
	return id, isBatch, nil
}

func write(ctx context.Context, conn *websocket.Conn, e domain.StatusEvent) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	return conn.Write(ctx, websocket.MessageText, b)
}

// Forward decodes events from the broker and hands them to the hub.
func (h *Hub) Forward(log *slog.Logger) func(body []byte) {
	return func(body []byte) {
		var e domain.StatusEvent
		if err := json.Unmarshal(body, &e); err != nil {
			log.Warn("undecodable status event", "err", err)
			return
		}
		h.Publish(e)
	}
}
