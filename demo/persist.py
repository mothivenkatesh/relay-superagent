"""The serving layer's durable ledger: in-memory view, Postgres truth.

Reads stay dict-fast (the whole demo renders off `.runs`); every write goes
through the PgLedger SQL as well, and boot hydrates memory from the
database — so runs, decisions, outcomes and memory survive restarts for
every tenant, sample data included.

The connection is the table OWNER, which bypasses RLS on purpose: this is
the single trusted serving process reading across tenants to hydrate.
Worker processes (supervisor at scale, future queue consumers) still get
per-tenant `relay_superagent_app` connections where RLS enforces isolation.

Not yet durable (known, logged): chat conversations.
"""

from __future__ import annotations

from relay_superagent.domain.models import MemoryNote, Run
from relay_superagent.ledger import Ledger
from relay_superagent.ledger_pg import PgLedger, _tid

DSN = "host=localhost port=5435 dbname=relay_superagent user=relay_superagent"


def _naive(dt):
    return dt.replace(tzinfo=None) if dt is not None and dt.tzinfo else dt


class PersistentLedger(Ledger):
    """The in-memory Ledger, write-through to Postgres."""

    def __init__(self, dsn: str = DSN):
        super().__init__()
        self.pg = PgLedger(dsn, "t1")          # one owner connection; the
        self._labels: set[str] = set()          # tenant GUC is irrelevant here
        self._hydrate()

    # -- boot -----------------------------------------------------------------
    def _hydrate(self) -> None:
        labels = [r["label"] for r in
                  self.pg.conn.execute("SELECT label FROM tenant_label")]
        self._labels = set(labels)
        for label in labels:
            for run in self.pg.runs_for_tenant(label):
                run.tenant_id = label
                for f in ("occurred_at", "gated_at", "surfaced_at", "acted_at"):
                    setattr(run, f, _naive(getattr(run, f)))
                self.runs[run.run_id] = run
            self.pg.tenant_id = label           # scope helper reads per label
            rows = self.pg.conn.execute(
                """SELECT m.* FROM memory m WHERE m.tenant_id=%s
                   AND m.superseded_by IS NULL""", [_tid(label)]).fetchall()
            for r in rows:
                self.memory.append(MemoryNote(
                    memory_id=str(r["memory_id"]), tenant_id=label,
                    subject_type=r["subject_type"], subject_id=r["subject_id"],
                    concern=r["concern"], body=r["body"],
                    source_run=str(r["source_run"]) if r["source_run"] else None))
        self.pg.tenant_id = "t1"
        for row in self.pg.conn.execute(
                "SELECT run_id, ts, agent, kind, detail FROM run_events "
                "ORDER BY event_id"):
            self.events.setdefault(str(row["run_id"]), []).append(
                {"ts": _naive(row["ts"]), "agent": row["agent"],
                 "kind": row["kind"], "detail": row["detail"]})
        # outcomes: hydrate per run
        for run in list(self.runs.values()):
            out = self.pg.outcome_for(run.run_id)
            if out is not None:
                out.tenant_id = run.tenant_id
                out.observed_at = _naive(out.observed_at)
                self.outcomes.append(out)

    def trace(self, run_id, ts, agent, kind, detail=""):
        super().trace(run_id, ts, agent, kind, detail)
        try:
            self.pg.conn.execute(
                """INSERT INTO run_events (run_id, ts, agent, kind, detail)
                   VALUES (%s,%s,%s,%s,%s)""",
                [run_id, ts, agent, kind, detail])
        except Exception as e:                              # noqa: BLE001
            print(f"persist WARNING: trace insert failed: {str(e)[:100]}")

    def _label(self, tenant_id: str) -> None:
        if tenant_id in self._labels:
            return
        self.pg.conn.execute(
            """INSERT INTO tenant_label (tenant_id, label) VALUES (%s, %s)
               ON CONFLICT DO NOTHING""", [_tid(tenant_id), tenant_id])
        self._labels.add(tenant_id)

    # -- write-through --------------------------------------------------------
    def insert(self, run: Run) -> Run:
        got = super().insert(run)               # raises DuplicateRun first
        self._label(run.tenant_id)
        try:
            self.pg.insert(run)
        except Exception as e:                  # duplicate row is fine; anything
            if "duplicate" not in str(e).lower() and "unique" not in str(e).lower():
                print(f"persist WARNING: pg insert failed for {run.run_id}: "
                      f"{str(e)[:120]}")
        return got

    def save(self, run: Run) -> None:
        super().save(run)
        self.pg.save(run)

    def append_outcome(self, tenant_id, run_id, key, value, at, source):
        out = super().append_outcome(tenant_id, run_id, key, value, at, source)
        self._label(tenant_id)
        self.pg.append_outcome(tenant_id, run_id, key, value, at, source)
        return out

    def append_memory(self, note: MemoryNote) -> None:
        super().append_memory(note)
        self._label(note.tenant_id)
        self.pg.append_memory(note)

    def effect(self, run_id, effect_type, target_ref, do):
        """Durable exactly-once: the Postgres effect row is the authority, so
        a side effect fired before a restart is never fired again after it."""
        run = self.runs.get(run_id)
        self.pg.tenant_id = run.tenant_id if run else "t1"
        return self.pg.effect(run_id, effect_type, str(target_ref), do)
