# syntax=docker/dockerfile:1

# One image for every binary under cmd/. Compose picks the binary via `command`.

FROM golang:1.27.4-alpine AS build
WORKDIR /src

# Download modules in their own layer so code changes don't invalidate it.
# go.* also matches go.sum once the module has dependencies.
COPY go.* ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

# Static binaries only need CA certs and tzdata; distroless ships both
# and runs as a non-root user without a shell.
FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/ /app/
USER nonroot:nonroot
CMD ["/app/api"]
