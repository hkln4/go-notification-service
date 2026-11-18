BEGIN;

-- Reverse dependency order: children before the tables they reference.
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS delivery_attempts;
DROP TABLE IF EXISTS notifications;
DROP TABLE IF EXISTS batches;

DROP TYPE IF EXISTS channel;
DROP TYPE IF EXISTS notification_status;

COMMIT;
