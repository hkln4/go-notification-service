.PHONY: build run test test-integration lint fmt tidy up down migrate-up migrate-down

build:
	go build -o bin/ ./cmd/...

run:
	go run ./cmd/api

test:
	go test -race ./...

# Needs Docker: testcontainers starts Postgres, RabbitMQ and Redis.
test-integration:
	go test -race -count=1 -tags=integration ./...

lint:
	golangci-lint run ./...

fmt:
	gofmt -s -w .

tidy:
	go mod tidy

up:
	docker-compose up -d

down:
	docker-compose down

migrate-up:
	docker-compose run --rm migrate up

migrate-down:
	docker-compose run --rm migrate down 1
