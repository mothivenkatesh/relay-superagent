"""Dispute classification (§6.2). Disputes arrive as structured webhooks from
the bank or payment processor with a reason_code already attached — there is
no free-text detection step the way a sales-call transcript needed regex
matching for a competitor name. "Detection" here is just a deterministic
lookup: is this reason_code one the merchant's policy has evidence coverage
for at all? An unknown or uncovered code is pre-trigger noise, not a run.
"""

from __future__ import annotations

from relay_superagent.domain.models import DisputeReason, Policy


def classify_dispute(reason_code: str | None, policy: Policy) -> DisputeReason | None:
    return policy.reason_by_code(reason_code) if reason_code else None
