# Notification Service

An event-driven notification service in Go that delivers SMS, email and push messages through an external provider. It accepts single and batch requests over REST, queues them by priority, delivers them asynchronously at no more than 100 messages per second per channel, retries failures with backoff and parks what can't be delivered in a dead letter queue. Duplicate requests and duplicate deliveries are both handled, and every notification can be followed through metrics, structured logs, a distributed trace and a WebSocket stream.

| Requirement | Where |
|---|---|
| Create, batch (≤ 1000), get by ID or batch, cancel, list with filters and pagination | [API](#api-examples), `internal/http`, `internal/service` |
| Asynchronous processing via queue workers | `internal/relay`, `internal/worker`, RabbitMQ |
| Max 100 msg/s per channel | `internal/ratelimit` (GCRA in Redis), [rate limiting](#rate-limiting-gcra-in-redis) |
| Priority queues (high, normal, low) | RabbitMQ `x-max-priority`, [priority](#priority) |
| Content validation | `internal/domain/validation.go` |
| Idempotency | [two layers](#idempotency-two-layers) |
| Delivery and retry logic | `internal/worker/decide.go`, [retries](#retries-fixed-ttl-queues-and-a-dlq) |
| Metrics, structured logs with correlation IDs, health checks | [Observability](#observability) |
| Bonus: failure handling (circuit breaker, DLQ requeue), scheduled notifications, templates, WebSocket updates, distributed tracing, GitHub Actions CI | see the sections below |

## Quick start

Requirements: Docker with Compose v2.

```sh
docker compose up -d --build          # or: make up
docker compose up -d --scale worker=3 # optional: three workers share the rate limit
```

The standalone `docker-compose` binary works the same way.

Everything has defaults; no `.env` file is needed. To override something, copy `.env.example` to `.env`.

If PostgreSQL, RabbitMQ or Redis already runs on your machine and its port is taken, set `POSTGRES_PORT`, `RABBITMQ_PORT`, `RABBITMQ_UI_PORT` or `REDIS_PORT` in `.env`. Only the host side changes; the services reach each other inside the compose network. (Only if you also run a binary outside Docker, e.g. `make run`, point its `DATABASE_URL`, `AMQP_URL` or `REDIS_ADDR` at the new port.)

| What | URL |
|---|---|
| API | http://localhost:8080/api/v1 |
| Swagger UI | http://localhost:8080/swagger/index.html |
| API metrics (HTTP latency, created, queue depth, outbox backlog), health, readiness | http://localhost:8080/metrics, `/health`, `/ready` |
| Delivery metrics (success/failure rates, provider latency, end-to-end latency), scraped from every service | Prometheus: http://localhost:9091 |
| Jaeger (traces) | http://localhost:16686 |
| RabbitMQ management | http://localhost:15672 (notification / notification) |
| Mock provider stats | http://localhost:9090/stats |

```sh
curl -s -X POST localhost:8080/api/v1/notifications \
  -d '{"channel":"sms","recipient":"+905551234567","content":"Hello","priority":"high"}'
```

In Swagger UI, the create examples send a plain notification as they are; each endpoint's description shows the optional extras (scheduling, templates, idempotency keys). They are left out of the schema itself because Swagger UI copies every schema field into its example, and these extras don't go together with a plain send.

### Using webhook.site as the provider

By default the worker sends to a bundled mock provider (`cmd/mockprovider`) with configurable latency and failure rate, because webhook.site's free URLs stop answering after about 100 requests. To use webhook.site, create a URL there, set its response to status `202` with a body such as `{"messageId":"8f0c6f5e-3a2b-4c1d-9e7f-1a2b3c4d5e6f","status":"accepted","timestamp":"2026-11-18T12:00:00Z"}`, and set it in `.env`:

```sh
PROVIDER_URL=https://webhook.site/<your-uuid>
```

If the stack is already running, run `docker compose up -d` again so the worker picks up the new URL.

## Running tests

```sh
make test              # unit tests, no Docker needed
make test-integration  # unit + integration tests; testcontainers starts PostgreSQL, RabbitMQ and Redis
make lint-docker       # golangci-lint, same version as CI
make loadtest          # k6 traffic spike against the running stack (see Load test)
```

Tests always run with `-race`. Assertions use `testify/require`. Table-driven tests cover validation, the retry decision (`Decide`), the circuit breaker, the rate limiter and the HTTP layer. Integration tests (build tag `integration`) run against real containers; every duration in them (retry delays, lease, poll interval) is scaled down to milliseconds, except the rate limiter test, which runs at the real 100/s because that rate is the requirement being tested.

Integration scenarios include:

- happy path: `POST` → outbox → relay → queue → worker → provider → `sent`, through the real HTTP API
- the same message delivered twice reaches the provider once
- a cancel racing a claim: exactly one wins, every time
- a worker that dies holding the lease: the notification is still delivered
- a scheduled notification is not sent before `scheduledAt` and is sent after it
- the provider keeps answering 500: five attempts, then `failed` and the DLQ; a requeue then delivers it
- three workers × 10 concurrent senders never exceed 100 grants in any sliding second
- broker connection loss: publishing and consuming recover on their own
- one trace spans the API request, the relay, the worker and the provider call

CI (`.github/workflows/ci.yml`) runs lint, a check that the OpenAPI spec is up to date, unit tests with coverage (informational), integration tests and a Docker build on every push.

## Architecture

```mermaid
flowchart LR
    client([Client]) -- REST --> api[api]
    api -- "one transaction:<br/>notification + outbox row" --> pg[(PostgreSQL)]
    relay[relay] -- "claim due rows<br/>(SKIP LOCKED)" --> pg
    relay -- "publish, mandatory + confirms" --> ex{{notifications<br/>exchange}}
    ex --> qs[q.sms / q.email / q.push<br/>x-max-priority=2]
    qs --> worker[worker ×N]
    worker -- "GCRA slot" --> redis[(Redis)]
    worker -- "claim lease, record outcome" --> pg
    worker -- POST --> provider([Provider])
    worker -- "transient failure" --> retry{{retry exchange}}
    retry --> rq["q.retry.{channel}.{5s,30s,2m,10m}<br/>fixed TTL"]
    rq -- "dead-letter on expiry" --> ex
    worker -- "out of attempts" --> dlq[q.dlq]
    worker -- "status event" --> events{{notification.events<br/>fanout}}
    api -- "status event<br/>(cancel, retry)" --> events
    events --> api
    api -- WebSocket --> client
```

Four binaries from one module and one Docker image: `api`, `relay`, `worker` and `mockprovider`.

### The life of a notification

1. **API.** The request is validated and checked against its idempotency key. The notification (`pending`, or `scheduled` if it has a `scheduledAt`) and an outbox row (available now, or at `scheduledAt`) are written in one transaction, and the API answers `202`. The API never publishes to RabbitMQ.
2. **Relay.** It leases due outbox rows with `UPDATE … FOR UPDATE SKIP LOCKED`, commits, publishes them with `mandatory` and publisher confirms, and deletes the rows the broker confirmed. A full batch is followed by another poll right away; otherwise it waits 200 ms.
3. **Worker** (manual ack, prefetch 10 per queue):
   1. **Circuit breaker.** If the channel's circuit is open, the message is parked in a retry queue for the cooldown. No attempt is used.
   2. **Rate limit.** It books a send slot in Redis, waiting at most 10 s. If no slot comes, or Redis is down, the message goes round the 5 s retry queue. No attempt is used.
   3. **Claim.** A conditional `UPDATE` sets `processing`, increments `attempts` and takes a lease token valid for 30 s. If the claim fails, the notification is already final (the message is dropped), or another worker holds the lease (the message is parked, never dropped).
   4. **Provider call.** `POST` with a 5 s timeout and `Idempotency-Key: <notification id>`.
   5. **Decision.** `Decide()` picks sent, retry, dead letter or permanent failure. The order is always: publish the next step (retry queue or DLQ, confirmed) → update the database (fenced by the lease token) → ack.
   6. **Record and broadcast.** Every attempt is recorded in `delivery_attempts`, and the status change is published to `notification.events` for WebSocket clients.

```
pending   ─┐
scheduled ─┼─▶ processing ─▶ sent | failed | retrying
retrying  ─┘        │
                    └─▶ processing   (only after the lease expired)

pending | scheduled | retrying ─▶ cancelled        terminal: sent, failed, cancelled
```

## Design decisions

### RabbitMQ as the message transport

RabbitMQ carries the work; PostgreSQL stays the source of truth. It was chosen because priority queues, per-queue TTL with dead-lettering (which gives delayed retries without a scheduler), publisher confirms and per-consumer prefetch are all built in, and that is exactly the feature set this problem needs. I gave up Kafka (no per-message priority or delayed redelivery, and partition-level ordering we don't need), NATS JetStream (no priorities) and PostgreSQL as the queue itself (simple, but polling at thousands of messages per second puts the hot path on the database the API also writes to).

### Transactional outbox

The API writes the notification and an outbox row in one transaction, and a separate relay publishes the rows. Without the outbox a crash between the commit and the publish loses the message, and publishing before the commit can deliver a notification that was rolled back. The cost is the relay process and up to one poll interval (200 ms) of latency. `LISTEN/NOTIFY` could remove that latency, at the price of one more moving part.

### At-least-once delivery with a lease and fencing

Delivery is at-least-once, and duplicates are stopped by the claim: a conditional `UPDATE` hands one worker a lease token per notification, and every outcome write is fenced with `WHERE lease_token = $token`. This survives redeliveries, a relay that republishes after a crash and a worker that dies mid-call: the next copy of the message waits out the lease and claims again. Exactly-once delivery to an external provider can't be promised. The worker sends `Idempotency-Key: <notification id>` so that a provider that honours it can drop the rare duplicate; webhook.site doesn't.

### Retries: fixed-TTL queues and a DLQ

Transient failures go to a per-channel retry queue with a fixed TTL, on a ladder of 5 s → 30 s → 2 m → 10 m. After the fifth attempt the message goes to `q.dlq` and the notification is `failed`. Classic queues only expire messages at the head, so a single queue with per-message TTLs would make a 5 s retry wait behind a 10 m one; fixed TTL per queue avoids that, and `x-dead-letter-routing-key` sends the expired message back to its own channel. The decision itself is a pure function, table-tested:

| Provider response | Decision |
|---|---|
| 2xx | sent (a malformed response body is logged and ignored) |
| 429 | transient; wait for `Retry-After`, clamped to 5 s–10 m and rounded **up** to the next queue |
| 5xx, timeout, network error | transient; next step of the ladder |
| other 4xx | permanent failure (DLQ, no retries) |
| transient on attempt 5 | dead letter |

What I gave up:

- **Jitter.** Fixed TTLs mean no jitter ([AWS: Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)). The thundering herd it prevents is absorbed by the rate limiter: however many retries come back at once, the provider never sees more than the channel's rate.
- **429 is counted as an attempt.** If the provider throttles for long, messages can reach the DLQ. With our own limit well under its rate, this should be rare.
- **Requeue from the DLQ.** `POST /api/v1/notifications/{id}/retry` gives a failed notification fresh attempts through a new outbox row. The copy left in `q.dlq` stays there as a record. `attempts` counts the current round and starts over on a requeue, while `deliveryAttempts[].attemptNo` numbers every call over the notification's whole history (five failures and a requeue: the next call is `attemptNo` 6, `attempts` 1).

### Rate limiting: GCRA in Redis

Every worker books a send slot per channel in Redis using GCRA (the generic cell rate algorithm) with burst 1, so slots are evenly spaced (about 10 ms apart at 100/s). A token bucket with a capacity of 100 would let ~200 messages into one sliding second (a full bucket at the end of one second, plus a refill at the start of the next); evenly spaced slots can't exceed the rate in any window. The limiter is a ~20-line Lua script:

- It uses Redis `TIME`, so clock skew between worker hosts can't double-book a slot.
- It works in reservation form: each call books the next free slot in one round trip and then sleeps until it, so waiting workers don't poll Redis.
- It never books a slot later than the caller's deadline.

If Redis is unreachable the limiter **fails closed**: nothing is sent, and messages circle the 5 s retry queue without using attempts. The alternative, sending without a limit, would break the one rule the provider gave us.

The default is **95/s, not 100**. The limiter spaces 100/s slots exactly, but send times drift by a few milliseconds on the way out (scheduling, connection reuse). Under load the provider measured up to 104 requests in a second at a 100/s limit, and 96–98 at 95/s. In warm runs the 5% headroom kept the provider's own view under 100. The guarantee is exact at the limiter (tested with three workers and 30 concurrent senders); at the provider it is a measurement, see [Load test](#load-test).

### Priority

Each channel queue is a classic queue with `x-max-priority=2`, and messages carry the notification's priority (0 low, 1 normal, 2 high), including through retry queues. RabbitMQ orders by priority only among messages waiting in the queue, so prefetch is kept small (10) to leave most of a backlog where priority can act on it. The load test shows the effect: in a backlog, high-priority messages were delivered in about 0.5 s while low-priority ones waited about 30 s.

The relay publishes in outbox order (`ORDER BY id`), not by priority. The backlog builds up in RabbitMQ, where priority already works, and sorting the outbox would fight the `available_at` index. Strict priority can starve low-priority messages under a sustained high-priority flood; per-tenant fairness or weighted consumption would be the next step.

### Idempotency: two layers

1. **API level.** An optional `Idempotency-Key` header (Stripe style, see [Stripe: Designing robust and predictable APIs with idempotency](https://stripe.com/blog/idempotency)) is stored with a SHA-256 hash of the validated request. The same key and the same request returns the stored notification with `200`; the same key with a different request returns `409`. The hash is computed over the validated Go struct, so JSON field order and defaults don't matter. Inserts use `ON CONFLICT DO NOTHING`; a request that loses a race on the same key re-reads the winner instead of failing. Batches take their own key, and items can carry per-item keys: an item that already exists unchanged is skipped and reported in `duplicates`, and one that exists with different content fails the batch with `409`. Without a key there is no deduplication.
2. **Delivery level.** At-least-once with the lease described above. The notification ID is sent to the provider as `Idempotency-Key`.

### Circuit breaker

Each worker keeps a breaker per channel: after 10 consecutive transient failures the circuit opens for 30 s, and messages are parked before the claim, so no attempts are burned while the provider is down. After the cooldown one trial request is let through (half-open), and it closes or reopens the circuit. A trial that never reaches the provider (for example its claim finds a duplicate) frees the half-open slot instead of leaving the breaker stuck. Breakers are per process rather than shared in Redis, which costs a few extra failed calls per worker when an outage starts and keeps the design simple.

### Load protection

- Request bodies are capped at 1 MB, and the batch endpoint at 16 MB: 1000 emails at the 10,000-character limit are about 10 MB of ASCII, more with multi-byte characters, and 16 MB fits that while still bounding memory per request. The server has read and write timeouts, and the PostgreSQL pool is capped at 20 connections.
- **Backpressure.** When more than `BACKPRESSURE_LIMIT` outbox rows are due and unpublished, creates get `429` with `Retry-After: 5`. Reads and cancels keep working. The backlog is sampled once a second in the background, so the check costs nothing per request.

### Smaller choices

- **UUIDv7 IDs.** They are time-ordered, so inserts land at the end of the primary key B-tree instead of on random pages, and replaying a batch returns its IDs in request order.
- **Keyset pagination on `(created_at, id)`.** Every page costs the same, and rows inserted while a client pages don't shift or repeat entries. The indexes are ascending on purpose: a backward scan serves `ORDER BY created_at DESC, id DESC` without a sort step (verified with `EXPLAIN`).
- **Templates are plain `{{variable}}` substitution, not `text/template`.** Templates arrive over the API, and a language with functions and loops is more power than a message needs. The rendered text is validated like typed content, and it is what gets stored, so editing a template never changes what was sent.
- **Cancel** is a single conditional `UPDATE` (pending, scheduled or retrying → cancelled). The queued message stays, and the worker drops it at the claim. Cancelling twice succeeds.
- **The DLQ also takes undecodable messages**, with an `x-failure-reason` header, since retrying them can't help.

## Load test

A sudden traffic spike, the kind a flash sale or Black Friday produces: k6 sends 300 single creates per second plus two 100-item batches per second for 30 seconds, about 15,000 notifications across the three channels with a 10/60/30 high/normal/low priority mix. `loadtest/run.sh` then waits for the system to drain and reports what it saw.

Setup: Docker Desktop on one machine (8 vCPU, 8 GB), every service plus k6 on the same VM, 3 workers, mock provider with 50 ms latency, default limits (95/s per channel).

| Measurement | Result |
|---|---|
| Notifications | 15,101 created, 15,101 sent, 0 failed, 0 HTTP errors |
| API latency, single create | p95 1.3 ms |
| API latency, 100-item batch | p95 11 ms |
| Drain | 25 s after the load stopped (56 s in total); throughput ≈ 3 × 95/s |
| End-to-end, high priority | p50 0.45 s, p95 0.54 s |
| End-to-end, normal priority | p50 4.6 s, p95 7.5 s |
| End-to-end, low priority | p50 29.9 s, p95 36.1 s |
| Provider: most requests in any sliding second, per channel | 98 |
| Provider: duplicate deliveries | 0 |

The API answers in about a millisecond because it only writes to PostgreSQL; the work queues up in RabbitMQ, and priority decides who waits. The provider-side rate is a measurement, not a guarantee: one run made right after restarting every container showed 108 in one second, which shows how much a cold, overloaded single machine can bunch up arrivals. A wider margin (e.g. 90/s) would absorb more of this; beyond that, a real provider enforces its own limit with 429, which the worker honours via Retry-After. The limiter's own spacing is exact and covered by the integration test.

## API examples

The outputs below are real responses from the running stack; long ones are shortened with `"...": "..."`.

Base path `/api/v1`. Every response has an `X-Correlation-ID` header, echoed from the request or generated. Errors look like `{"error":{"code","message","details"}}`. The full spec is in [`docs/swagger.yaml`](docs/swagger.yaml) and at `/swagger/index.html`.

| Method | Path | Result |
|---|---|---|
| POST | `/notifications` | `202`; `200` on an idempotent replay; `409` key reused with a different request; `413` body over 1 MB; `429` backpressure (with `Retry-After`) |
| POST | `/notifications/batch` | `202` with `batchId`; `200` on a replay; `400` with the bad item indexes; `409`; `413` body over 16 MB; `429` backpressure (with `Retry-After`) |
| GET | `/notifications/{id}` | `200` with delivery attempts |
| GET | `/notifications?status&channel&from&to&cursor&limit` | `200`; newest first, `nextCursor` until the last page |
| POST | `/notifications/{id}/cancel` | `200` (also when already cancelled); `409` once processing or final |
| POST | `/notifications/{id}/retry` | `202` for a failed notification; `409` otherwise |
| GET | `/batches/{id}` | `200` with a count per status |
| POST, GET | `/templates`, `/templates/{id}` | `201`, `200` |
| GET | `/ws?notificationId=…` or `?batchId=…` | WebSocket: current state, then every status change |

**Create** (with an idempotency key and a correlation ID):

```sh
$ curl -i -X POST localhost:8080/api/v1/notifications \
    -H 'Idempotency-Key: order-1001-shipped' -H 'X-Correlation-ID: demo-42' \
    -d '{"channel":"sms","recipient":"+905551234567","content":"Your order #1001 has shipped","priority":"high"}'
HTTP/1.1 202 Accepted
Location: /api/v1/notifications/01a1eff2-d42f-7932-9983-4e2579f2e240
X-Correlation-Id: demo-42
{"id":"01a1eff2-d42f-7932-9983-4e2579f2e240","channel":"sms","recipient":"+905551234567","content":"Your order #1001 has shipped","priority":"high","status":"pending","attempts":0,"correlationId":"demo-42","createdAt":"2026-11-18T18:37:11.091345Z","updatedAt":"2026-11-18T18:37:11.091345Z"}
```

The same key with the same request (fields in a different order) is a replay; the same key with a different body is a conflict:

```sh
$ curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8080/api/v1/notifications -H 'Idempotency-Key: order-1001-shipped' \
    -d '{"priority":"high","content":"Your order #1001 has shipped","recipient":"+905551234567","channel":"sms"}'
200
$ curl -s -X POST localhost:8080/api/v1/notifications -H 'Idempotency-Key: order-1001-shipped' \
    -d '{"channel":"sms","recipient":"+905551234567","content":"Different text"}'
{"error":{"code":"idempotency_conflict","message":"idempotency key reused with a different request"}}
```

**Get**, a moment later:

```sh
$ curl -s localhost:8080/api/v1/notifications/01a1eff2-d42f-7932-9983-4e2579f2e240
{
  "id": "01a1eff2-d42f-7932-9983-4e2579f2e240",
  "channel": "sms",
  "recipient": "+905551234567",
  "content": "Your order #1001 has shipped",
  "priority": "high",
  "status": "sent",
  "attempts": 1,
  "providerMessageId": "64248400-3508-4cb8-997f-e7154cfbadb1",
  "correlationId": "demo-42",
  "createdAt": "2026-11-18T18:37:11.091345Z",
  "updatedAt": "2026-11-18T18:37:11.325185Z",
  "deliveryAttempts": [
    { "attemptNo": 1, "httpStatus": 202, "latencyMs": 51, "createdAt": "2026-11-18T18:37:11.325185Z" }
  ]
}
```

**Validation** reports every problem at once:

```sh
$ curl -s -X POST localhost:8080/api/v1/notifications -d '{"channel":"sms","recipient":"05551234567","content":"","priority":"urgent"}'
{
  "error": {
    "code": "validation_failed",
    "message": "request validation failed",
    "details": [
      { "field": "priority", "message": "must be one of high, normal, low" },
      { "field": "content", "message": "is required" },
      { "field": "recipient", "message": "must be an E.164 phone number, e.g. +905551234567" }
    ]
  }
}
```

| Channel | Recipient | Content |
|---|---|---|
| sms | E.164 (`^\+[1-9]\d{7,14}$`) | ≤ 160 characters |
| email | a plain address (`net/mail`), ≤ 254 characters | ≤ 10,000 characters |
| push | device token, ≤ 256 characters | ≤ 256 characters |

`priority` is `high`, `normal` (default) or `low`; `scheduledAt`, if present, must be in the future. Lengths count characters, not bytes.

**Batch**, then its status counts:

```sh
$ curl -s -X POST localhost:8080/api/v1/notifications/batch -d '{"notifications":[
    {"channel":"email","recipient":"ayse@example.com","content":"Welcome!"},
    {"channel":"push","recipient":"device-token-123","content":"Welcome!","priority":"low"}]}'
{
  "batchId": "01a1eff2-e0a1-70a9-ab1f-824c40d32a96",
  "count": 2,
  "notificationIds": ["01a1eff2-e0a1-70c4-bf13-58f9cdad0daa", "01a1eff2-e0a1-70de-b327-d9b0a06b010f"]
}
$ curl -s localhost:8080/api/v1/batches/01a1eff2-e0a1-70a9-ab1f-824c40d32a96
{
  "id": "01a1eff2-e0a1-70a9-ab1f-824c40d32a96",
  "totalCount": 2,
  "statusCounts": { "cancelled": 0, "failed": 0, "pending": 0, "processing": 0, "retrying": 0, "scheduled": 0, "sent": 2 },
  "createdAt": "2026-11-18T18:37:14.27324Z"
}
```

One invalid item rejects the whole batch and is reported by index:

```sh
$ curl -s -X POST localhost:8080/api/v1/notifications/batch -d '{"notifications":[
    {"channel":"sms","recipient":"+905551234567","content":"ok"},
    {"channel":"email","recipient":"not-an-email","content":"x"}]}'
{
  "error": {
    "code": "validation_failed",
    "message": "request validation failed",
    "details": [ { "index": 1, "field": "recipient", "message": "must be a plain email address, e.g. user@example.com" } ]
  }
}
```

**List**, newest first; filters are `status`, `channel`, `from` (inclusive) and `to` (exclusive, RFC 3339). Pass `nextCursor` back as `cursor` for the next page:

```sh
$ curl -s 'localhost:8080/api/v1/notifications?status=sent&limit=2'
{
  "items": [
    { "id": "01a1eff2-e0a1-70de-b327-d9b0a06b010f", "batchId": "01a1eff2-e0a1-70a9-ab1f-824c40d32a96", "channel": "push", "status": "sent", "...": "..." },
    { "id": "01a1eff2-e0a1-70c4-bf13-58f9cdad0daa", "batchId": "01a1eff2-e0a1-70a9-ab1f-824c40d32a96", "channel": "email", "status": "sent", "...": "..." }
  ],
  "nextCursor": "eyJ0IjoiMjAyNi0xMS0xOFQxODozNzoxNC4yNzMyNFoiLCJpZCI6IjAxYTFlZmYyLWUwYTEtNzBjNC1iZjEzLTU4ZjljZGFkMGRhYSJ9"
}
```

**Cancel** a scheduled notification, and try to cancel one that was already sent:

```sh
$ curl -s -X POST localhost:8080/api/v1/notifications/01a1eff2-f5db-70d5-8dee-377adfc36f73/cancel
{ "id": "01a1eff2-f5db-70d5-8dee-377adfc36f73", "status": "cancelled", "scheduledAt": "2026-11-18T19:37:19Z", "...": "..." }
$ curl -s -X POST localhost:8080/api/v1/notifications/01a1eff2-d42f-7932-9983-4e2579f2e240/cancel
{"error":{"code":"invalid_state","message":"cannot cancel a sent notification"}}
```

**Templates:**

```sh
$ curl -s -X POST localhost:8080/api/v1/templates \
    -d '{"name":"order-shipped","channel":"sms","body":"Hi {{name}}, order {{orderId}} is on its way."}'
{
  "id": "01a1eff2-f609-79dc-8bed-7e497a628857",
  "name": "order-shipped",
  "channel": "sms",
  "body": "Hi {{name}}, order {{orderId}} is on its way.",
  "variables": ["name", "orderId"],
  "createdAt": "2026-11-18T18:37:19.754055Z"
}
$ curl -s -X POST localhost:8080/api/v1/notifications \
    -d '{"recipient":"+905551234567","templateId":"01a1eff2-f609-79dc-8bed-7e497a628857","variables":{"name":"Ayşe","orderId":"A-42"}}'
{ "id": "01a1eff3-02d9-7c64-b838-092fde648621", "channel": "sms", "content": "Hi Ayşe, order A-42 is on its way.",
  "status": "pending", "templateId": "01a1eff2-f609-79dc-8bed-7e497a628857", "...": "..." }
```

With `templateId`, `content` must be empty and `channel` may be omitted. A missing variable is a validation error (`"variables": "missing: orderId"`).

**WebSocket.** Connect to `ws://localhost:8080/api/v1/ws?notificationId=<id>` or `?batchId=<id>`. The current state arrives first, then every change: workers announce processing, sent, retrying and failed, and the API announces the changes it makes itself (`cancelled` after a cancel, `pending` after a retry):

```json
{"notificationId":"01a1efd9-67c2-7cc8-8520-0db2e0b384b7","status":"scheduled","attempts":0,"at":"2026-11-18T18:09:24.931Z"}
{"notificationId":"01a1efd9-67c2-7cc8-8520-0db2e0b384b7","status":"processing","attempts":1,"at":"2026-11-18T18:09:42.029Z"}
{"notificationId":"01a1efd9-67c2-7cc8-8520-0db2e0b384b7","status":"sent","attempts":1,"at":"2026-11-18T18:09:42.100Z"}
```

Every API instance reads the `notification.events` fanout through its own exclusive, auto-deleted queue, so it doesn't matter which instance a client is connected to. A client that falls behind is disconnected rather than allowed to slow everyone down; the REST API remains the source of truth.

## Observability

**Metrics** (Prometheus format at `/metrics` on the API, and on `:9100` for the relay and each worker; the bundled Prometheus finds worker replicas through DNS). Delivery metrics are recorded by the workers, whose `:9100` is not published to the host (several replicas would collide), so success/failure rates and latencies are read in Prometheus at http://localhost:9091; `:8080/metrics` has the API's own metrics and the queue depths:

| Metric | Meaning |
|---|---|
| `http_request_duration_seconds{method,route,status}` | API latency, labelled by route pattern, not path |
| `notifications_created_total{channel}` | accepted notifications (replays excluded) |
| `notifications_delivered_total{channel,result}` | attempts by outcome: sent, retry, dead_letter, permanent_fail |
| `notification_delivery_duration_seconds{channel}` | provider call latency |
| `notification_end_to_end_seconds{channel}` | creation (or `scheduledAt`) to delivery |
| `notification_queue_depth{queue}` | ready messages per queue (passive declare, no management API) |
| `notification_outbox_backlog` | due outbox rows not yet published |
| `circuit_breaker_state{channel}` | 0 closed, 1 half-open, 2 open |

Example Prometheus queries: success rate `sum(rate(notifications_delivered_total{result="sent"}[1m])) / sum(rate(notifications_delivered_total[1m]))`, p95 delivery latency `histogram_quantile(0.95, sum by (le) (rate(notification_delivery_duration_seconds_bucket[5m])))`.

**Logs** are JSON (`log/slog`). The correlation ID follows a notification from the API request through the database, the outbox headers and the message headers into the worker's logs and the request to the provider. Recipients are masked by the logger itself, whatever the call site: `+90555***4567`, `h***@gmail.com`.

**Tracing** uses OpenTelemetry, exported over OTLP to Jaeger. The `traceparent` is stored with the outbox row and copied into the message headers, so one trace covers the API request, the relay publish, the worker's delivery (rate limit wait, claim, provider call, outcome) and the provider's HTTP request.

**Health.** `/health` means the process is up. `/ready` pings PostgreSQL, RabbitMQ and Redis and returns `503` with the failing check if any is down.

## Configuration

Environment variables, with defaults that work inside docker-compose. Durations use Go syntax (`5s`, `2m`).

| Variable | Default | Used by | Meaning |
|---|---|---|---|
| `DATABASE_URL` | `postgres://notification:notification@localhost:5432/notification?sslmode=disable` | api, relay, worker | PostgreSQL |
| `AMQP_URL` | `amqp://notification:notification@localhost:5672/` | api, relay, worker | RabbitMQ |
| `REDIS_ADDR` | `localhost:6379` | api, worker | Redis (rate limiter; readiness) |
| `PROVIDER_URL` | `http://localhost:9090/` (compose: the mock) | worker | provider endpoint |
| `RETRY_DELAYS` | `5s,30s,2m,10m` | all | retry ladder; must match across services (queue names and TTLs) |
| `MAX_ATTEMPTS` | `5` | worker | attempts before the DLQ |
| `RATE_LIMIT_PER_SECOND` | `95` | worker | per channel, shared by all workers |
| `WORKER_PREFETCH` | `10` | worker | unacked messages per queue per worker |
| `WORKER_LEASE` | `30s` | worker | claim lease; must exceed `PROVIDER_TIMEOUT` |
| `TOKEN_WAIT` | `10s` | worker | longest wait for a rate limit slot |
| `PROVIDER_TIMEOUT` | `5s` | worker | per provider call |
| `BREAKER_THRESHOLD`, `BREAKER_COOLDOWN` | `10`, `30s` | worker | circuit breaker; threshold `0` disables it |
| `RELAY_BATCH_SIZE`, `RELAY_POLL_INTERVAL`, `RELAY_LEASE` | `100`, `200ms`, `30s` | relay | outbox polling |
| `BACKPRESSURE_LIMIT` | `50000` | api | outbox backlog above which creates get `429`; `0` disables it |
| `WS_ALLOWED_ORIGINS` | `*` | api | allowed WebSocket origins (restrict in production) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | empty (compose: `http://jaeger:4318`) | api, relay, worker | trace export; empty disables it |
| `HTTP_ADDR`, `METRICS_ADDR` | `:8080`, `:9100` | api; relay, worker | listen addresses |
| `SHUTDOWN_TIMEOUT` | `10s` | api, worker | graceful shutdown; the worker stops taking messages and finishes in-flight ones |
| `LOG_LEVEL` | `info` | all | `debug`, `info`, `warn`, `error` |
| `FAILURE_RATE`, `LATENCY_MS` | `0`, `50` | mockprovider | share of 500 answers; added latency |

A database password with characters such as `@`, `:` or `/` must be URL-encoded in `DATABASE_URL`.

## Project layout

```
cmd/                api, relay, worker, mockprovider
internal/
  domain/           model, statuses, validation (no dependencies)
  service/          use cases: create, batch, idempotency, list, templates
  http/             REST handlers, middleware, error format
  store/postgres/   repositories, outbox, claim and lease
  relay/            outbox → RabbitMQ
  broker/           AMQP connection with reconnect, topology, confirmed publish, consume
  worker/           delivery pipeline, Decide(), graceful shutdown
  ratelimit/        GCRA (Redis Lua script, in-memory twin for tests)
  breaker/          per-channel circuit breaker
  provider/         HTTP client; mock/ is the stand-in provider
  template/         {{variable}} rendering
  ws/               WebSocket hub
  observability/    logging, correlation IDs, PII masking, metrics, health, tracing
migrations/         golang-migrate, applied by the one-shot migrate service
test/integration/   end-to-end tests
loadtest/           k6 scenario and runner
docs/               generated OpenAPI spec (make swagger)
```

Dependencies point inward: handlers depend on the service, the service on interfaces it defines itself, and the store, broker and limiter implement them. Time, IDs and randomness are injected where tests need them to be deterministic.

## Trade-offs and what I would do with more time

- **Fairness between tenants.** One large sender can fill a channel's 100/s. Per-tenant limits or weighted fair queuing would stop that.
- **Jitter, at least for 429s.** It would spread retries out even without the shared limiter.
- **Quorum queues** for broker high availability. Recent RabbitMQ versions give them priorities (32 levels as of 4.3).
- **`LISTEN/NOTIFY`** to wake the relay instead of polling, and partitioning the outbox if it grows large.
- **Authentication and authorisation**, and per-client rate limits on the API itself.
- **A shared circuit breaker** in Redis, so all workers react to an outage at the same moment.
- **Retention.** Archive or delete old notifications and attempts, and purge DLQ messages that have been requeued.
- **Adding a channel** means one more value in `domain.Channels`, its validation rules and a migration that adds it to the `channel` enum; queues, retry queues, limits and metrics are all derived from that list.
