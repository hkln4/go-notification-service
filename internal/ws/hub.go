// Package ws pushes notification status changes to WebSocket clients.
//
// Workers publish a StatusEvent to the notification.events fanout exchange;
// every API instance receives all of them through its own private queue and
// forwards each to the clients subscribed to that notification or batch.
// Delivery to clients is best effort: the REST API stays the source of
// truth, and a client that reconnects should re-read the state.
package ws

import (
	"sync"

	"github.com/google/uuid"

	"github.com/hkln4/go-notification-service/internal/domain"
)

// clientBuffer bounds how far a client may fall behind before it is
// disconnected; a slow reader must not hold up delivery to everyone else.
const clientBuffer = 64

type client struct {
	events chan domain.StatusEvent
	// gone is closed when the hub drops the client for being too slow.
	gone chan struct{}
	once sync.Once
}

func (c *client) drop() { c.once.Do(func() { close(c.gone) }) }

type Hub struct {
	mu   sync.RWMutex
	subs map[uuid.UUID]map[*client]struct{} // notification or batch ID → clients
}

func NewHub() *Hub {
	return &Hub{subs: map[uuid.UUID]map[*client]struct{}{}}
}

func (h *Hub) subscribe(key uuid.UUID) *client {
	c := &client{events: make(chan domain.StatusEvent, clientBuffer), gone: make(chan struct{})}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subs[key] == nil {
		h.subs[key] = map[*client]struct{}{}
	}
	h.subs[key][c] = struct{}{}
	return c
}

func (h *Hub) unsubscribe(key uuid.UUID, c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.subs[key], c)
	if len(h.subs[key]) == 0 {
		delete(h.subs, key)
	}
}

// Publish forwards e to subscribers of its notification and of its batch.
// It never blocks: a client whose buffer is full is dropped.
func (h *Hub) Publish(e domain.StatusEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	h.send(e.NotificationID, e)
	if e.BatchID != nil {
		h.send(*e.BatchID, e)
	}
}

func (h *Hub) send(key uuid.UUID, e domain.StatusEvent) {
	for c := range h.subs[key] {
		select {
		case c.events <- e:
		default:
			c.drop()
		}
	}
}

// Subscribers returns how many clients are subscribed to key.
func (h *Hub) Subscribers(key uuid.UUID) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs[key])
}
