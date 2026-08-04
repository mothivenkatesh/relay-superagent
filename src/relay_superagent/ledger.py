"""The ledger. In production this is Postgres with RLS; here it is the same
shape in memory, because the guarantees live in the shape, not the engine:

- one run per idempotency key, enforced at insert
- outcomes are appended rows referencing a run, never mutations of one
- every external side effect passes through `effect()` exactly once
- memory is derived and rebuildable; the ledger is the only source of truth
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

from relay_superagent.domain.models import MemoryNote, Outcome, Run, sha


class DuplicateRun(Exception):
    """Raised on an idempotency-key collision. Callers treat it as 'already
    handled' and fetch the existing run."""

    def __init__(self, existing: Run):
        self.existing = existing


@dataclass
class Ledger:
    runs: dict[str, Run] = field(default_factory=dict)
    outcomes: list[Outcome] = field(default_factory=list)
    memory: list[MemoryNote] = field(default_factory=list)
    events: dict[str, list] = field(default_factory=dict)
    effects: dict[str, dict[str, Any]] = field(default_factory=dict)
    _by_idem: dict[tuple[str, str], str] = field(default_factory=dict)

    # -- runs -----------------------------------------------------------------
    def insert(self, run: Run) -> Run:
        key = (run.tenant_id, run.idempotency_key)
        if key in self._by_idem:
            raise DuplicateRun(self.runs[self._by_idem[key]])
        self.runs[run.run_id] = run
        self._by_idem[key] = run.run_id
        return run

    def save(self, run: Run) -> None:
        """No-op here: callers hold the same object the dict holds. The Postgres
        ledger overrides this with an UPDATE, so the pipeline calls it at every
        mutation point and stays storage-agnostic."""

    def runs_for_tenant(self, tenant_id: str) -> list[Run]:
        return [r for r in self.runs.values() if r.tenant_id == tenant_id]

    # -- outcomes -------------------------------------------------------------
    def append_outcome(self, tenant_id: str, run_id: str, key: str,
                       value: dict[str, Any], at: datetime, source: str) -> Outcome:
        if any(o.run_id == run_id and o.outcome_key == key for o in self.outcomes):
            return next(o for o in self.outcomes if o.run_id == run_id and o.outcome_key == key)
        out = Outcome(outcome_id=str(uuid.uuid4()), tenant_id=tenant_id, run_id=run_id,
                      outcome_key=key, outcome_value=value, observed_at=at, source=source)
        self.outcomes.append(out)
        return out

    def outcome_for(self, run_id: str) -> Outcome | None:
        return next((o for o in self.outcomes if o.run_id == run_id), None)

    # -- effects: exactly-once side effects -----------------------------------
    def effect(self, run_id: str, effect_type: str, target_ref: str,
               do: Callable[[], str]) -> str:
        """Execute `do` at most once per (run, type, target). Replays and retries
        return the recorded external ref instead of re-firing."""
        key = sha(run_id, effect_type, target_ref)
        if key in self.effects and self.effects[key]["status"] == "done":
            return self.effects[key]["external_ref"]
        external_ref = do()
        self.effects[key] = {"run_id": run_id, "effect_type": effect_type,
                             "status": "done", "external_ref": external_ref}
        return external_ref

    # -- traces ---------------------------------------------------------------
    def trace(self, run_id: str, ts, agent: str, kind: str, detail: str = "") -> None:
        """Append one trace event: which agent did what, when. The granular
        record the run-replay view renders."""
        self.events.setdefault(run_id, []).append(
            {"ts": ts, "agent": agent, "kind": kind, "detail": detail})

    def trace_for(self, run_id: str) -> list:
        return self.events.get(run_id, [])

    # -- memory ---------------------------------------------------------------
    def append_memory(self, note: MemoryNote) -> None:
        self.memory.append(note)

    def memory_for(self, tenant_id: str, subject_id: str, concern: str) -> list[MemoryNote]:
        return [m for m in self.memory
                if m.tenant_id == tenant_id and m.subject_id == subject_id
                and m.concern == concern and m.superseded_by is None]
