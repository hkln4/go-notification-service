.PHONY: build run test test-integration lint lint-docker fmt tidy up down migrate-up migrate-down swagger loadtest

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

# Same linter version as CI, without installing it locally.
lint-docker:
	docker run --rm -v "$$PWD":/src -w /src -v golangci-cache:/root/.cache golangci/golangci-lint:v2.15.0 golangci-lint run ./...

fmt:
	gofmt -s -w .

tidy:
	go mod tidy

up:
	docker compose up -d

down:
	docker compose down

migrate-up:
	docker compose run --rm migrate up

migrate-down:
	docker compose run --rm migrate down 1

# Regenerates docs/ from the handler annotations. CI checks it is up to date.
SWAG_VERSION := v1.16.6
swagger:
	go run github.com/swaggo/swag/cmd/swag@$(SWAG_VERSION) init -g cmd/api/main.go -o docs --parseInternal --outputTypes go,json,yaml
	gofmt -w docs

# Needs the stack: docker compose up -d --scale worker=3
loadtest:
	./loadtest/run.sh
