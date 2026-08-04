"""The Postgres ledger. Same interface as the in-memory `Ledger`, same
guarantees, now held by the database instead of a dict:

- the idempotency collision is a unique constraint, not an if-statement
- gate fields writing exactly once is a trigger as well as domain code
- cross-tenant reads are impossible at the row level (RLS), not just unqueried
- `effect()` serialises concurrent workers with a row lock, so two processes
  racing the same side effect fire it once

The contract tests in tests/test_ledger_contract.py run the same assertions
against both implementations; the fakes remain the CI default so the suite
stays fast and credential-free.

Tenant ids are uuids in Postgres. Tests and fixtures use human strings
("t1"), so ids are mapped through a stable uuid5 both ways.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Callable

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from relay_superagent.domain.models import AgentType, Arm, GateAction, MemoryNote, Outcome, Run, RunState, sha
from relay_superagent.ledger import DuplicateRun

_NS = uuid.UUID("6b1e5a52-0000-4000-8000-c0a2a23e0000")


def _tid(tenant_id: str) -> uuid.UUID:
    """Stable uuid for a human-readable tenant id."""
    try:
        return uuid.UUID(tenant_id)
    except ValueError:
        return uuid.uuid5(_NS, tenant_id)


_RUN_COLS = """run_id, tenant_id, loop, agent_type, idempotency_key, status, suppressed_reason,
trigger_source, trigger_ref, occurred_at, order_id, merchant_id, dispute_id,
reason_code, deadline_at, claim_hash, claim_text, retrieved_refs, decision, evidence,
policy_version, arm, gate_actor, gate_action, gate_diff, gate_is_material,
gate_latency_ms, gated_at, surfaced_at, acted_at, act_ref, cost_tokens"""


class PgLedger:
    def __init__(self, dsn: str, tenant_id: str):
        """One ledger handle per tenant, matching how the app runs: the tenant
        GUC is pinned at connection level and RLS does the rest."""
        self.tenant_id = tenant_id
        self.conn = psycopg.connect(dsn, row_factory=dict_row, autocommit=True)
        self.conn.execute("SELECT set_config('app.tenant_id', %s, false)",
                          [str(_tid(tenant_id))])

    # -- runs -----------------------------------------------------------------
    def insert(self, run: Run) -> Run:
        try:
            self.conn.execute(
                f"""INSERT INTO run ({_RUN_COLS}) VALUES
                    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                     %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                [uuid.UUID(run.run_id), _tid(run.tenant_id), run.loop, run.agent_type.value,
                 run.idempotency_key, run.state.value, run.suppressed_reason,
                 run.trigger_source, run.trigger_ref, run.occurred_at,
                 run.order_id, run.merchant_id, run.dispute_id,
                 run.reason_code, run.deadline_at, run.claim_hash, run.claim_text,
                 Jsonb(run.retrieved_refs), Jsonb(run.decision), Jsonb(run.evidence),
                 run.policy_version, run.arm.value,
                 run.gate_actor,
                 run.gate_action.value if run.gate_action else None,
                 Jsonb(run.gate_diff) if run.gate_diff is not None else None,
                 run.gate_is_material, run.gate_latency_ms, run.gated_at,
                 run.surfaced_at, run.acted_at, run.act_ref, run.cost_tokens])
        except psycopg.errors.UniqueViolation:
            existing = self.conn.execute(
                "SELECT * FROM run WHERE tenant_id=%s AND idempotency_key=%s",
                [_tid(run.tenant_id), run.idempotency_key]).fetchone()
            raise DuplicateRun(self._to_run(existing)) from None
        return run

    def trace(self, run_id, ts, agent, kind, detail=""):
        if not hasattr(self, "_events"):
            self._events = {}
        self._events.setdefault(run_id, []).append(
            {"ts": ts, "agent": agent, "kind": kind, "detail": detail})

    def trace_for(self, run_id):
        return getattr(self, "_events", {}).get(run_id, [])

    def save(self, run: Run) -> None:
        """Persist mutated fields. The gate-once trigger enforces write-once on
        gate_action even if this is called with a bug upstream."""
        self.conn.execute(
            """UPDATE run SET status=%s, suppressed_reason=%s, claim_hash=%s,
               claim_text=%s, retrieved_refs=%s, decision=%s, evidence=%s,
               gate_actor=%s, gate_action=%s, gate_diff=%s, gate_is_material=%s,
               gate_latency_ms=%s, gated_at=%s, surfaced_at=%s, acted_at=%s,
               act_ref=%s, cost_tokens=%s
               WHERE run_id=%s""",
            [run.state.value, run.suppressed_reason, run.claim_hash,
             run.claim_text, Jsonb(run.retrieved_refs), Jsonb(run.decision),
             Jsonb(run.evidence), run.gate_actor,
             run.gate_action.value if run.gate_action else None,
             Jsonb(run.gate_diff) if run.gate_diff is not None else None,
             run.gate_is_material, run.gate_latency_ms, run.gated_at,
             run.surfaced_at, run.acted_at, run.act_ref, run.cost_tokens,
             uuid.UUID(run.run_id)])

    def runs_for_tenant(self, tenant_id: str) -> list[Run]:
        rows = self.conn.execute(
            "SELECT * FROM run WHERE tenant_id=%s ORDER BY created_at",
            [_tid(tenant_id)]).fetchall()
        return [self._to_run(r) for r in rows]

    def _to_run(self, row: dict[str, Any]) -> Run:
        run = Run(
            run_id=str(row["run_id"]), tenant_id=self.tenant_id,
            idempotency_key=row["idempotency_key"],
            trigger_source=row["trigger_source"], trigger_ref=row["trigger_ref"],
            occurred_at=row["occurred_at"], policy_version=row["policy_version"],
            arm=Arm(row["arm"]), loop=row["loop"],
            agent_type=AgentType(row["agent_type"]),
            suppressed_reason=row["suppressed_reason"],
            order_id=row["order_id"], merchant_id=row["merchant_id"],
            dispute_id=row["dispute_id"], reason_code=row["reason_code"],
            deadline_at=row["deadline_at"],
            claim_hash=row["claim_hash"], claim_text=row["claim_text"],
            retrieved_refs=row["retrieved_refs"] or {},
            decision=row["decision"], evidence=row["evidence"] or [],
            gate_actor=row["gate_actor"],
            gate_diff=row["gate_diff"], gate_is_material=row["gate_is_material"],
            gate_latency_ms=row["gate_latency_ms"], gated_at=row["gated_at"],
            surfaced_at=row["surfaced_at"], acted_at=row["acted_at"],
            act_ref=row["act_ref"], cost_tokens=row["cost_tokens"])
        run.state = RunState(row["status"])
        if row["gate_action"]:
            run.gate_action = GateAction(row["gate_action"])
        return run

    # -- outcomes -------------------------------------------------------------
    def append_outcome(self, tenant_id: str, run_id: str, key: str,
                       value: dict[str, Any], at: datetime, source: str) -> Outcome:
        row = self.conn.execute(
            """INSERT INTO outcome (outcome_id, tenant_id, run_id, outcome_key,
                                    outcome_value, observed_at, source)
               VALUES (%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (run_id, outcome_key) DO NOTHING
               RETURNING outcome_id""",
            [uuid.uuid4(), _tid(tenant_id), uuid.UUID(run_id), key,
             Jsonb(value), at, source]).fetchone()
        got = self.conn.execute(
            "SELECT * FROM outcome WHERE run_id=%s AND outcome_key=%s",
            [uuid.UUID(run_id), key]).fetchone()
        return Outcome(outcome_id=str(got["outcome_id"]), tenant_id=tenant_id,
                       run_id=run_id, outcome_key=got["outcome_key"],
                       outcome_value=got["outcome_value"],
                       observed_at=got["observed_at"], source=got["source"])

    def outcome_for(self, run_id: str) -> Outcome | None:
        got = self.conn.execute("SELECT * FROM outcome WHERE run_id=%s",
                                [uuid.UUID(run_id)]).fetchone()
        if not got:
            return None
        return Outcome(outcome_id=str(got["outcome_id"]), tenant_id=self.tenant_id,
                       run_id=run_id, outcome_key=got["outcome_key"],
                       outcome_value=got["outcome_value"],
                       observed_at=got["observed_at"], source=got["source"])

    # -- effects --------------------------------------------------------------
    def effect(self, run_id: str, effect_type: str, target_ref: str,
               do: Callable[[], str]) -> str:
        """Exactly-once across concurrent workers. The row is the lock: claim it
        with an upsert, take FOR UPDATE, and only the holder of a non-done row
        executes `do`. A crash after `do` but before the update leaves a
        'pending' row whose retry re-executes — so callbacks must themselves be
        idempotent against the external system, which Slack posts and CRM notes
        are, keyed by our refs."""
        key = sha(run_id, effect_type, target_ref)
        with self.conn.transaction():
            self.conn.execute(
                """INSERT INTO effect (effect_key, tenant_id, run_id, effect_type, status)
                   VALUES (%s,%s,%s,%s,'pending')
                   ON CONFLICT (effect_key) DO NOTHING""",
                [key, _tid(self.tenant_id), uuid.UUID(run_id), effect_type])
            row = self.conn.execute(
                "SELECT status, external_ref FROM effect WHERE effect_key=%s FOR UPDATE",
                [key]).fetchone()
            if row["status"] == "done":
                return row["external_ref"]
            external_ref = do()
            self.conn.execute(
                """UPDATE effect SET status='done', external_ref=%s,
                   attempts=attempts+1 WHERE effect_key=%s""",
                [external_ref, key])
            return external_ref

    # -- memory ---------------------------------------------------------------
    def append_memory(self, note: MemoryNote) -> None:
        self.conn.execute(
            """INSERT INTO memory (memory_id, tenant_id, subject_type, subject_id,
                                   concern, body, source_run, superseded_by)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            [uuid.UUID(note.memory_id), _tid(note.tenant_id), note.subject_type,
             note.subject_id, note.concern, Jsonb(note.body),
             uuid.UUID(note.source_run) if note.source_run else None,
             uuid.UUID(note.superseded_by) if note.superseded_by else None])

    def memory_for(self, tenant_id: str, subject_id: str, concern: str) -> list[MemoryNote]:
        rows = self.conn.execute(
            """SELECT * FROM memory WHERE tenant_id=%s AND subject_id=%s
               AND concern=%s AND superseded_by IS NULL ORDER BY created_at""",
            [_tid(tenant_id), subject_id, concern]).fetchall()
        return [MemoryNote(memory_id=str(r["memory_id"]), tenant_id=tenant_id,
                           subject_type=r["subject_type"], subject_id=r["subject_id"],
                           concern=r["concern"], body=r["body"],
                           source_run=str(r["source_run"]) if r["source_run"] else None)
                for r in rows]
