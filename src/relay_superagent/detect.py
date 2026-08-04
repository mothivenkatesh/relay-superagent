"""Competitor detection (§6.2). Deterministic match first, model second, never
the model without a match. The word-boundary match is the cost control that
keeps the LLM off 99% of transcripts; the model's only job on the remainder is
to reject false positives ("Acme Street") and pull out the claim.
"""

from __future__ import annotations

import re

from relay_superagent.domain.models import Competitor, Policy


def match_competitors(text: str, policy: Policy) -> list[Competitor]:
    hits: list[Competitor] = []
    lowered = text.lower()
    for comp in policy.competitors:
        names = comp.names + comp.domains
        if any(re.search(rf"\b{re.escape(n.lower())}\b", lowered) for n in names):
            hits.append(comp)
    return hits
