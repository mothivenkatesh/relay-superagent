-- Durable per-run trace events (the journey/replay substrate, CM-19).
CREATE TABLE IF NOT EXISTS run_events (
  event_id  bigserial PRIMARY KEY,
  run_id    uuid NOT NULL,
  ts        timestamptz NOT NULL,
  agent     text NOT NULL,
  kind      text NOT NULL,
  detail    text NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS run_events_run ON run_events (run_id, event_id);
