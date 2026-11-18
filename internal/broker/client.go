package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var ErrNotConnected = errors.New("broker: not connected")

const (
	minBackoff = 500 * time.Millisecond
	maxBackoff = 10 * time.Second
)

// Client keeps one AMQP connection alive. amqp091-go does not reconnect on
// its own: when the connection drops every channel on it is dead. Client
// watches NotifyClose, redials with exponential backoff, re-declares the
// topology and opens a fresh publishing channel. Consumers (see Consume)
// re-subscribe on their own once the client is connected again.
type Client struct {
	url  string
	topo Topology
	log  *slog.Logger

	mu    sync.Mutex
	conn  *amqp.Connection
	pub   *publisher
	ready chan struct{} // closed while connected; replaced on disconnect

	// pubMu serialises publishers on the single confirm-mode channel.
	pubMu sync.Mutex

	done    chan struct{}
	stopped chan struct{}
}

// New starts connecting in the background and returns immediately. Use
// WaitReady to block until the first connection is up.
func New(url string, topo Topology, log *slog.Logger) *Client {
	c := &Client{
		url:     url,
		topo:    topo,
		log:     log.With("component", "broker"),
		ready:   make(chan struct{}),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	go c.run()
	return c
}

// WaitReady blocks until connected or ctx is done.
func (c *Client) WaitReady(ctx context.Context) error {
	c.mu.Lock()
	ready := c.ready
	c.mu.Unlock()
	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for broker: %w", ctx.Err())
	}
}

// Connected reports whether a connection is currently up (for /ready).
func (c *Client) Connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn != nil && !c.conn.IsClosed()
}

// Close stops reconnecting and closes the connection.
func (c *Client) Close() {
	select {
	case <-c.done:
	default:
		close(c.done)
	}
	<-c.stopped
}

func (c *Client) run() {
	defer close(c.stopped)
	backoff := minBackoff
	for {
		conn, pub, err := c.connect()
		if err != nil {
			c.log.Warn("broker connect failed", "err", err, "retry_in", backoff.String())
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
				continue
			case <-c.done:
				return
			}
		}
		backoff = minBackoff

		connClosed := conn.NotifyClose(make(chan *amqp.Error, 1))
		pubClosed := pub.ch.NotifyClose(make(chan *amqp.Error, 1))
		c.setConnected(conn, pub)
		c.log.Info("broker connected")

		select {
		case err := <-connClosed:
			c.log.Warn("broker connection closed", "err", err)
		case err := <-pubClosed:
			// A channel-level error (e.g. publishing to a missing exchange)
			// kills only the channel. Reconnecting from scratch is simpler
			// than repairing it and also re-declares the topology.
			c.log.Warn("broker publish channel closed", "err", err)
		case <-c.done:
			c.setDisconnected()
			_ = conn.Close()
			return
		}
		c.setDisconnected()
		_ = conn.Close()
	}
}

func (c *Client) connect() (*amqp.Connection, *publisher, error) {
	conn, err := amqp.DialConfig(c.url, amqp.Config{
		Heartbeat: 10 * time.Second,
		Properties: amqp.Table{
			"connection_name": "notification-service",
		},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("open channel: %w", err)
	}
	if err := c.topo.Declare(ch); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	pub, err := newPublisher(ch)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	return conn, pub, nil
}

func (c *Client) setConnected(conn *amqp.Connection, pub *publisher) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn, c.pub = conn, pub
	close(c.ready)
}

func (c *Client) setDisconnected() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn, c.pub = nil, nil
	c.ready = make(chan struct{})
}

func (c *Client) current() (*amqp.Connection, *publisher) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn, c.pub
}

// QueueDepth returns the number of ready messages in a queue, using a passive
// declare on a short-lived channel instead of the management HTTP API.
func (c *Client) QueueDepth(queue string) (int, error) {
	conn, _ := c.current()
	if conn == nil {
		return 0, ErrNotConnected
	}
	ch, err := conn.Channel()
	if err != nil {
		return 0, err
	}
	defer func() { _ = ch.Close() }()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		return 0, err
	}
	return q.Messages, nil
}
