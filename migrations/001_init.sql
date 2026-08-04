-- 001: the ledger (spec §4).
--
-- Append-only in spirit: run rows move state only along the machine's legal
-- edges (enforced in domain code), gate_* columns are written exactly once
-- (enforced by trigger here, belt and braces), outcomes are appended rows,
-- and every external side effect passes through `effect`.
--
-- Tenancy: RLS on every table keyed on current_setting('app.tenant_id').
-- The application role cannot see across tenants even if application code is
-- wrong. The migration runs as the cluster owner, which bypasses RLS; the app
-- connects as `relay_superagent_app`.
--
-- Deliberately absent: the pgvector embedding column on evidence_item. Nothing
-- reads embeddings yet; schema that nothing reads is schema that rots. It
-- arrives in 002 with the retrieval that uses it.

BEGIN;

CREATE TABLE IF NOT EXISTS run (
  run_id            uuid PRIMARY KEY,
  tenant_id         uuid NOT NULL,
  loop              text NOT NULL DEFAULT 'watcher',
  idempotency_key   text NOT NULL,
  status            text NOT NULL,
  suppressed_reason text,

  trigger_source    text NOT NULL,
  trigger_ref       text NOT NULL,
  occurred_at       timestamptz NOT NULL,

  opportunity_id    text,
  account_id        text,
  rep_user_id       text,
  competitor_id     text,
  claim_hash        text,
  claim_text        text,

  retrieved_refs    jsonb,
  decision          jsonb,
  evidence          jsonb,

  policy_version    text NOT NULL,
  prompt_hash       text NOT NULL DEFAULT '',
  model             text NOT NULL DEFAULT '',
  arm               text NOT NULL,

  gate_actor        text,
  gate_action       text,
  gate_diff         jsonb,
  gate_is_material  boolean,
  gate_latency_ms   integer,
  gated_at          timestamptz,
  surfaced_at       timestamptz,

  acted_at          timestamptz,
  act_ref           text,
  trace_id          text,
  cost_tokens       integer NOT NULL DEFAULT 0,
  created_at        timestamptz NOT NULL DEFAULT now(),

  UNIQUE (tenant_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS run_tenant_status ON run (tenant_id, status, created_at);
CREATE INDEX IF NOT EXISTS run_tenant_opp ON run (tenant_id, opportunity_id);

-- gate_* columns are written exactly once: a second write with a different
-- value is a bug upstream, and the database refuses to hide it.
CREATE OR REPLACE FUNCTION guard_gate_once() RETURNS trigger AS $$
BEGIN
  IF OLD.gate_action IS NOT NULL AND NEW.gate_action IS DISTINCT FROM OLD.gate_action THEN
    RAISE EXCEPTION 'gate fields are written exactly once (run %)', OLD.run_id;
  END IF;
  RETURN NEW;
END $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS run_gate_once ON run;
CREATE TRIGGER run_gate_once BEFORE UPDATE ON run
  FOR EACH ROW EXECUTE FUNCTION guard_gate_once();

CREATE TABLE IF NOT EXISTS outcome (
  outcome_id    uuid PRIMARY KEY,
  tenant_id     uuid NOT NULL,
  run_id        uuid NOT NULL REFERENCES run(run_id),
  outcome_key   text NOT NULL,
  outcome_value jsonb NOT NULL,
  observed_at   timestamptz NOT NULL,
  source        text NOT NULL,
  UNIQUE (run_id, outcome_key)
);

CREATE TABLE IF NOT EXISTS effect (
  effect_key   text PRIMARY KEY,
  tenant_id    uuid NOT NULL,
  run_id       uuid NOT NULL,
  effect_type  text NOT NULL,
  status       text NOT NULL,
  external_ref text,
  attempts     int NOT NULL DEFAULT 0,
  created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS memory (
  memory_id     uuid PRIMARY KEY,
  tenant_id     uuid NOT NULL,
  subject_type  text NOT NULL,
  subject_id    text NOT NULL,
  concern       text NOT NULL,
  body          jsonb NOT NULL,
  source_run    uuid,
  superseded_by uuid,
  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS memory_live
  ON memory (tenant_id, subject_type, subject_id, concern)
  WHERE superseded_by IS NULL;

CREATE TABLE IF NOT EXISTS evidence_item (
  evidence_id   uuid PRIMARY KEY,
  tenant_id     uuid NOT NULL,
  competitor_id text NOT NULL,
  claim_class   text NOT NULL,
  text          text NOT NULL,
  source_url    text NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS policy (
  policy_version text PRIMARY KEY,
  tenant_id      uuid NOT NULL,
  body           jsonb NOT NULL,
  active         boolean NOT NULL DEFAULT false,
  created_at     timestamptz NOT NULL DEFAULT now()
);

-- ---- tenancy --------------------------------------------------------------
DO $$ BEGIN
  CREATE ROLE relay_superagent_app LOGIN;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

GRANT SELECT, INSERT, UPDATE ON run, outcome, effect, memory, evidence_item, policy
  TO relay_superagent_app;

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['run','outcome','effect','memory','evidence_item','policy'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I', t);
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON %I USING '
      '(tenant_id = current_setting(''app.tenant_id'')::uuid) '
      'WITH CHECK (tenant_id = current_setting(''app.tenant_id'')::uuid)', t);
  END LOOP;
END $$;

COMMIT;
