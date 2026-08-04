"""Metrics over the ledger (§10). The ledger is the online eval: these are
computed from rows, so the customer can recompute every one of them from their
own Slack audit log without trusting us. `correction_rate` is the commercial
metric and the autonomy trigger.
"""

from __future__ import annotations

from relay_superagent.domain.models import GateAction, RunState
from relay_superagent.ledger import Ledger

_GATED = (RunState.APPROVED, RunState.EDITED, RunState.REJECTED, RunState.TIMED_OUT,
          RunState.ACTING, RunState.ACTED, RunState.RESOLVED)


def _surfaced(ledger: Ledger, tenant_id: str):
    return [r for r in ledger.runs_for_tenant(tenant_id)
            if r.surfaced_at is not None]


def correction_rate(ledger: Ledger, tenant_id: str) -> float | None:
    surfaced = [r for r in _surfaced(ledger, tenant_id) if r.gate_action is not None]
    if not surfaced:
        return None
    corrected = sum(1 for r in surfaced
                    if r.gate_action is GateAction.REJECT
                    or (r.gate_action is GateAction.EDIT and r.gate_is_material))
    return corrected / len(surfaced)


def counter_usage_rate(ledger: Ledger, tenant_id: str) -> float | None:
    surfaced = [r for r in _surfaced(ledger, tenant_id) if r.gate_action is not None]
    if not surfaced:
        return None
    used = sum(1 for r in surfaced
               if r.gate_action is GateAction.APPROVE
               or (r.gate_action is GateAction.EDIT and not r.gate_is_material))
    return used / len(surfaced)


def trigger_precision(ledger: Ledger, tenant_id: str) -> float | None:
    """Share of triggered runs not suppressed as irrelevant. Suppressions for
    caps, holdout and dedupe are policy, not noise, so they don't count against
    precision."""
    noise_reasons = {"not_competitive"}
    triggered = ledger.runs_for_tenant(tenant_id)
    if not triggered:
        return None
    noise = sum(1 for r in triggered if r.suppressed_reason in noise_reasons)
    return 1 - noise / len(triggered)


def gate_latency_p95_ms(ledger: Ledger, tenant_id: str) -> int | None:
    lat = sorted(r.gate_latency_ms for r in ledger.runs_for_tenant(tenant_id)
                 if r.gate_latency_ms is not None)
    if not lat:
        return None
    return lat[min(len(lat) - 1, int(round(0.95 * len(lat))) )]
