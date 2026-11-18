//go:build integration

// Package mqtest starts a throwaway RabbitMQ container for integration tests.
package mqtest

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	tcrabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

// Image matches docker-compose.yml.
const Image = "rabbitmq:4.3.8-management-alpine"

type MQ struct {
	URL       string
	container *tcrabbit.RabbitMQContainer
}

func Start(ctx context.Context) (*MQ, error) {
	c, err := tcrabbit.Run(ctx, Image)
	if err != nil {
		return nil, fmt.Errorf("start rabbitmq container: %w", err)
	}
	url, err := c.AmqpURL(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}
	return &MQ{URL: url, container: c}, nil
}

// CloseAllConnections force-closes every client connection on the broker,
// which is what a broker restart looks like to a client, without changing
// the container's mapped port.
func (m *MQ) CloseAllConnections(ctx context.Context) error {
	code, _, err := m.container.Exec(ctx, []string{"rabbitmqctl", "close_all_connections", "test"})
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("rabbitmqctl exited with %d", code)
	}
	return nil
}

// Purge empties the given queues, ignoring ones that don't exist yet, so a
// test doesn't see messages left by an earlier test or run.
func (m *MQ) Purge(queues ...string) error {
	conn, err := amqp.Dial(m.URL)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	for _, q := range queues {
		// A failed purge (404) closes the channel, so use one per queue.
		ch, err := conn.Channel()
		if err != nil {
			return err
		}
		_, _ = ch.QueuePurge(q, false)
		_ = ch.Close()
	}
	return nil
}

func (m *MQ) Close(ctx context.Context) {
	_ = m.container.Terminate(ctx)
}
