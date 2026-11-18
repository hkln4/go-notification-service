//go:build integration

// Package rdtest starts a throwaway Redis container for integration tests.
package rdtest

import (
	"context"
	"fmt"

	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// Image matches docker-compose.yml.
const Image = "redis:7-alpine"

type Redis struct {
	Addr      string
	container *tcredis.RedisContainer
}

func Start(ctx context.Context) (*Redis, error) {
	c, err := tcredis.Run(ctx, Image)
	if err != nil {
		return nil, fmt.Errorf("start redis container: %w", err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}
	port, err := c.MappedPort(ctx, "6379/tcp")
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}
	return &Redis{Addr: fmt.Sprintf("%s:%s", host, port.Port()), container: c}, nil
}

func (r *Redis) Close(ctx context.Context) {
	_ = r.container.Terminate(ctx)
}
