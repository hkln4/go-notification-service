BEGIN;

CREATE TABLE templates (
  id         UUID PRIMARY KEY,
  name       TEXT NOT NULL UNIQUE,
  channel    channel NOT NULL,
  body       TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Which template produced the content, if any. The rendered content is
-- stored on the notification itself, so editing a template later never
-- changes what was sent.
ALTER TABLE notifications ADD COLUMN template_id UUID REFERENCES templates (id);

COMMIT;
