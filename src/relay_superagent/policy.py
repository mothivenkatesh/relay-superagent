"""The two deterministic gates, in code, with no model in either.

`safety` (§6.3) runs before any retrieval or generation: cheap lookups, any
failure suppresses the run with zero LLM spend. `layer1` (§6.6) runs on every
drafted dispute response before the judge: any failure blocks the draft from
ever reaching a merchant.

A prompt-level guardrail does not survive a chargeback deadline. These do.
"""

from __future__ import annotations

import re
from datetime import timedelta

from relay_superagent.domain.models import Draft, Policy, Run, RunState
from relay_superagent.ledger import Ledger
from relay_superagent.ports.base import CrmPort, UrlChecker

CLOSED_STAGES = {"closed_won", "closed_lost"}
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
_PHONE = re.compile(r"(?:\+?\d[\d\s\-()]{8,}\d)")


def safety(run: Run, policy: Policy, ledger: Ledger, crm: CrmPort,
           enrolled_merchants: set[str], tokens_spent_today: int) -> str | None:
    """Returns a suppression reason, or None to proceed. Order is cheapest first;
    every check must hold, and none of them needs a model."""

    order = crm.opportunity(run.order_id) if run.order_id else None
    if order is None:
        return "no_order"
    if order.get("stage") in CLOSED_STAGES:
        return "order_closed"

    window = timedelta(days=policy.suppress_window_days)
    for other in ledger.runs_for_tenant(run.tenant_id):
        if other.run_id == run.run_id:
            continue
        if (other.order_id == run.order_id
                and other.reason_code == run.reason_code
                and other.gate_action is not None
                and other.gate_action.value in ("approve", "edit")
                and other.gated_at is not None
                and run.occurred_at - other.gated_at <= window):
            return "recently_countered"
        if (other.merchant_id == run.merchant_id and run.claim_hash
                and other.claim_hash == run.claim_hash
                and other.state in (RunState.ACTED, RunState.RESOLVED)):
            return "claim_already_handled"

    if run.merchant_id not in enrolled_merchants:
        return "merchant_not_enrolled"

    today = [r for r in ledger.runs_for_tenant(run.tenant_id)
             if r.merchant_id == run.merchant_id
             and r.occurred_at.date() == run.occurred_at.date()
             and r.state not in (RunState.SUPPRESSED,)
             and r.run_id != run.run_id]
    if len(today) >= policy.per_merchant_per_day:
        return "merchant_daily_cap"

    if tokens_spent_today >= policy.tenant_tokens_per_day:
        return "tenant_token_ceiling"

    return None


def layer1(draft: Draft, run: Run, policy: Policy,
           known_evidence_ids: set[str], urls: dict[str, str],
           url_checker: UrlChecker) -> list[str]:
    """Deterministic assertions on a draft (§6.6). Returns failures; empty means
    pass. `urls` maps evidence_id -> source_url for the cited items."""

    failures: list[str] = []

    for eid in draft.cited_evidence_ids:
        if eid not in known_evidence_ids:
            failures.append(f"evidence_unresolved:{eid}")
        elif not url_checker.alive(urls[eid]):
            failures.append(f"source_dead:{eid}")

    lowered = draft.counter_text.lower()
    for term in policy.banned_terms:
        if re.search(rf"\b{re.escape(term.lower())}\b", lowered):
            failures.append(f"banned_term:{term}")

    lo, hi = policy.response_len_bounds
    if not (lo <= len(draft.counter_text) <= hi):
        failures.append("length_out_of_bounds")

    if run.reason_code and policy.reason_by_code(run.reason_code) is None:
        failures.append("unknown_reason_code")

    if _EMAIL.search(draft.counter_text) or _PHONE.search(draft.counter_text):
        failures.append("contact_info_in_counter")

    return failures


def judge_passes(verdict: dict[str, int], threshold: int) -> bool:
    return all(v >= threshold for v in verdict.values())
