"""The domain. Pure data and legal state transitions, zero I/O.

A run is a row, not a chat that runs. Every conversation with the model happens
inside exactly one run, every run ends in a terminal state, and an illegal
transition raises rather than silently corrupting the ledger. The spec's state
machine (§5) is written here once and enforced everywhere.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class RunState(StrEnum):
    RECEIVED = "received"
    SUPPRESSED = "suppressed"      # terminal; still written, still counted
    RUNNING = "running"
    DRAFTED = "drafted"
    CHECKED = "checked"
    AWAITING_GATE = "awaiting_gate"
    APPROVED = "approved"
    EDITED = "edited"
    REJECTED = "rejected"          # terminal
    TIMED_OUT = "timed_out"        # terminal; escalated, never sent
    ACTING = "acting"
    ACTED = "acted"
    RESOLVED = "resolved"          # terminal; an outcome exists
    FAILED = "failed"              # terminal; retries exhausted

    @property
    def terminal(self) -> bool:
        return self in _TERMINAL


_TERMINAL = {
    RunState.SUPPRESSED, RunState.REJECTED, RunState.TIMED_OUT,
    RunState.RESOLVED, RunState.FAILED,
}

# The only edges that exist. Everything else is a bug, and raises.
LEGAL: dict[RunState, set[RunState]] = {
    RunState.RECEIVED: {RunState.SUPPRESSED, RunState.RUNNING, RunState.FAILED},
    RunState.RUNNING: {RunState.DRAFTED, RunState.FAILED},
    RunState.DRAFTED: {RunState.CHECKED, RunState.FAILED},
    RunState.CHECKED: {RunState.AWAITING_GATE, RunState.FAILED},
    RunState.AWAITING_GATE: {
        RunState.APPROVED, RunState.EDITED, RunState.REJECTED,
        RunState.TIMED_OUT, RunState.FAILED,
    },
    RunState.APPROVED: {RunState.ACTING, RunState.FAILED},
    RunState.EDITED: {RunState.ACTING, RunState.FAILED},
    RunState.ACTING: {RunState.ACTED, RunState.FAILED},
    RunState.ACTED: {RunState.RESOLVED},
    # terminal states have no exits
    RunState.SUPPRESSED: set(),
    RunState.REJECTED: set(),
    RunState.TIMED_OUT: set(),
    RunState.RESOLVED: set(),
    RunState.FAILED: set(),
}


class IllegalTransition(Exception):
    pass


class GateAction(StrEnum):
    APPROVE = "approve"
    EDIT = "edit"
    REJECT = "reject"
    TIMEOUT = "timeout"


class Arm(StrEnum):
    TREATED = "treated"
    HOLDOUT = "holdout"


def sha(*parts: str) -> str:
    return hashlib.sha256("‖".join(parts).encode()).hexdigest()


@dataclass
class Competitor:
    id: str
    names: list[str]
    domains: list[str] = field(default_factory=list)


@dataclass
class Policy:
    """Versioned tenant config (§4). Changing this never needs a deploy, which is
    why it is data on the run and not constants in this file."""

    policy_version: str
    tenant_id: str
    competitors: list[Competitor]
    banned_terms: list[str]
    per_rep_per_day: int = 5
    tenant_tokens_per_day: int = 2_000_000
    gate_timeout_hours: int = 24
    judge_threshold: int = 4
    holdout_pct: int = 20
    suppress_window_days: int = 7
    counter_len_bounds: tuple[int, int] = (120, 1200)

    def competitor_by_id(self, cid: str) -> Competitor | None:
        return next((c for c in self.competitors if c.id == cid), None)


@dataclass
class EvidenceItem:
    evidence_id: str
    tenant_id: str
    competitor_id: str
    claim_class: str
    text: str
    source_url: str


@dataclass
class Draft:
    counter_text: str
    cited_evidence_ids: list[str]
    confidence: float
    escalate: bool = False


@dataclass
class TriggerEvent:
    """A normalised inbound event (§6.1). The payload lives in blob storage; only
    the ref travels."""

    tenant_id: str
    source: str            # 'gong' | 'gmail'
    source_ref: str        # external id, never the payload
    occurred_at: datetime
    opportunity_id: str | None
    account_id: str | None
    rep_user_id: str | None
    text: str              # transcript/email text, already fetched for detection


@dataclass
class Run:
    """One row in the ledger. Append-only in spirit: state moves only along LEGAL
    edges, gate_* fields are written exactly once, outcomes are separate rows."""

    run_id: str
    tenant_id: str
    idempotency_key: str
    trigger_source: str
    trigger_ref: str
    occurred_at: datetime
    policy_version: str
    arm: Arm
    state: RunState = RunState.RECEIVED
    loop: str = "watcher"

    suppressed_reason: str | None = None
    opportunity_id: str | None = None
    account_id: str | None = None
    rep_user_id: str | None = None
    competitor_id: str | None = None
    claim_hash: str | None = None
    claim_text: str | None = None

    retrieved_refs: dict[str, Any] = field(default_factory=dict)
    decision: dict[str, Any] | None = None
    evidence: list[dict[str, Any]] = field(default_factory=list)

    gate_actor: str | None = None
    gate_action: GateAction | None = None
    gate_diff: dict[str, Any] | None = None
    gate_is_material: bool | None = None
    gate_latency_ms: int | None = None
    gated_at: datetime | None = None
    surfaced_at: datetime | None = None

    acted_at: datetime | None = None
    act_ref: str | None = None
    cost_tokens: int = 0

    def transition(self, to: RunState) -> None:
        if to not in LEGAL[self.state]:
            raise IllegalTransition(f"{self.state} -> {to}")
        self.state = to

    def record_gate(self, actor: str, action: GateAction, at: datetime) -> None:
        if self.gate_action is not None:
            raise IllegalTransition("gate fields are written exactly once")
        self.gate_actor = actor
        self.gate_action = action
        self.gated_at = at
        if self.surfaced_at is not None:
            self.gate_latency_ms = int((at - self.surfaced_at).total_seconds() * 1000)


@dataclass
class Outcome:
    outcome_id: str
    tenant_id: str
    run_id: str
    outcome_key: str          # 'opportunity_closed'
    outcome_value: dict[str, Any]
    observed_at: datetime
    source: str


@dataclass
class MemoryNote:
    """Derived from an edit (§6.8). Rebuildable by replaying runs; never a source
    of truth."""

    memory_id: str
    tenant_id: str
    subject_type: str          # 'rep' | 'tenant'
    subject_id: str
    concern: str               # 'counter_style'
    body: dict[str, Any]       # {changed, implies, example}
    source_run: str
    superseded_by: str | None = None


def assign_arm(account_id: str, holdout_pct: int) -> Arm:
    """Stable per account (§6.4): two reps on one deal must land in the same arm,
    or they contaminate each other."""
    bucket = int(sha(account_id)[:8], 16) % 100
    return Arm.HOLDOUT if bucket < holdout_pct else Arm.TREATED
