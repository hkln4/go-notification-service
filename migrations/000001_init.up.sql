BEGIN;

CREATE TYPE notification_status AS ENUM
  ('pending', 'scheduled', 'processing', 'retrying', 'sent', 'failed', 'cancelled');
CREATE TYPE channel AS ENUM ('sms', 'email', 'push');

CREATE TABLE batches (
  id              UUID PRIMARY KEY,
  idempotency_key TEXT UNIQUE,
  request_hash    TEXT,
  total_count     INT NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE notifications (
  id                  UUID PRIMARY KEY,
  batch_id            UUID REFERENCES batches (id),
  idempotency_key     TEXT UNIQUE,
  request_hash        TEXT,
  channel             channel NOT NULL,
  recipient           TEXT NOT NULL,
  content             TEXT NOT NULL,
  priority            SMALLINT NOT NULL DEFAULT 1  -- 0=low 1=normal 2=high
                      CHECK (priority BETWEEN 0 AND 2),
  status              notification_status NOT NULL DEFAULT 'pending',
  attempts            INT NOT NULL DEFAULT 0,
  scheduled_at        TIMESTAMPTZ,
  lease_token         UUID,
  locked_until        TIMESTAMPTZ,
  provider_message_id TEXT,
  last_error          TEXT,
  correlation_id      TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_notif_batch   ON notifications (batch_id);
-- Ascending on purpose: list pages use ORDER BY created_at DESC, id DESC with a
-- (created_at, id) < ($1, $2) keyset cursor, which a backward scan serves
-- without a sort step. A (created_at DESC, id) index would need one.
CREATE INDEX idx_notif_list    ON notifications (status, channel, created_at, id);
CREATE INDEX idx_notif_created ON notifications (created_at, id);

CREATE TABLE delivery_attempts (
  id              BIGSERIAL PRIMARY KEY,
  notification_id UUID NOT NULL REFERENCES notifications (id),
  attempt_no      INT NOT NULL,
  http_status     INT,
  error           TEXT,
  latency_ms      INT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_attempts_notif ON delivery_attempts (notification_id);

CREATE TABLE outbox (
  id              BIGSERIAL PRIMARY KEY,
  notification_id UUID NOT NULL REFERENCES notifications (id),
  routing_key     TEXT NOT NULL,                -- sms | email | push
  priority        SMALLINT NOT NULL,
  headers         JSONB NOT NULL DEFAULT '{}',  -- correlation id, traceparent
  available_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  locked_until    TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_outbox_ready ON outbox (available_at);

COMMIT;
