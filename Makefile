.PHONY: build run test lint fmt tidy up down

build:
	go build -o bin/ ./cmd/...

run:
	go run ./cmd/api

test:
	go test -race ./...

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
