"""In-memory fakes with a controllable clock. The simulation harness: no model,
no network, no money, whole-pipeline tests in milliseconds. Same pattern as
ServiceWorker's ports/fakes.py, which is where this design is proven.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from relay_superagent.ports.base import LlmUnavailable


@dataclass
class FakeClock:
    current: datetime

    def now(self) -> datetime:
        return self.current

    def advance(self, **kw) -> None:
        self.current += timedelta(**kw)


@dataclass
class ScriptedLlm:
    """Returns fixtures. Tests assert on tool calls and decisions, never prose,
    so scripted responses are all the model a test needs."""

    mention: dict[str, Any] = field(default_factory=lambda: {
        "is_competitive": True, "claim_text": "Acme is cheaper", "confidence": 0.9})
    claim: dict[str, Any] = field(default_factory=lambda: {
        "claim_text": "Acme is cheaper", "speaker_role": "buyer", "confidence": 0.9})
    draft: dict[str, Any] = field(default_factory=lambda: {
        "counter_text": "On a three year basis Acme's total cost runs higher once "
                        "implementation and per-seat overage are included; see the "
                        "TCO comparison and the Forrester note." + " " * 40,
        "cited_evidence_ids": ["ev_tco", "ev_forrester"],
        "confidence": 0.8, "escalate": False})
    verdict: dict[str, int] = field(default_factory=lambda: {
        "addresses_claim": 5, "matches_register": 5, "evidence_grounded": 5})
    diff: dict[str, Any] = field(default_factory=lambda: {
        "changed": ["tightened second sentence"], "implies": "prefers shorter counters",
        "example": "…", "is_material": False})
    calls: list[tuple[str, dict]] = field(default_factory=list)

    def confirm_mention(self, text, competitor_names):
        self.calls.append(("confirm_mention", {"text": text}))
        return self.mention

    def extract_claim(self, text, competitor_id):
        self.calls.append(("extract_claim", {"competitor_id": competitor_id}))
        return {"competitor_id": competitor_id, **self.claim}

    def draft_counter(self, claim, deal, evidence, memory):
        self.calls.append(("draft_counter", {"claim": claim, "memory": list(memory)}))
        return self.draft

    def judge(self, claim, counter, register_ref):
        self.calls.append(("judge", {}))
        return self.verdict

    def semantic_diff(self, original, edited):
        self.calls.append(("semantic_diff", {"original": original, "edited": edited}))
        return self.diff


    def narrate(self, question, tool, facts):
        self.calls.append(("narrate", question, tool))
        return {"narration": self.narration if getattr(self, "narration", None) else ""}

class TimingOutLlm:
    def __getattr__(self, name):
        def _raise(*a, **kw):
            raise LlmUnavailable(name)
        return _raise


@dataclass
class FakeCrm:
    opportunities: dict[str, dict] = field(default_factory=dict)
    notes: list[tuple[str, str]] = field(default_factory=list)
    deals_by_account: dict[str, str] = field(default_factory=dict)

    def opportunity(self, opportunity_id):
        return self.opportunities.get(opportunity_id)

    def open_deal_for_account(self, account_id):
        return self.deals_by_account.get(account_id)

    def deal_context(self, opportunity_id):
        opp = self.opportunities.get(opportunity_id) or {}
        return {"stage": opp.get("stage"), "amount_band": opp.get("amount_band", "unknown"),
                "competitor_history": opp.get("competitor_history", []), "prior_losses": []}

    def write_note(self, opportunity_id, text):
        self.notes.append((opportunity_id, text))
        return f"note_{len(self.notes)}"

    def close(self, opportunity_id, won: bool):
        self.opportunities[opportunity_id]["stage"] = "closed_won" if won else "closed_lost"


@dataclass
class FakeSlack:
    dms: list[tuple[str, dict]] = field(default_factory=list)
    channel_posts: list[tuple[str, dict]] = field(default_factory=list)

    def dm(self, user_id, blocks):
        self.dms.append((user_id, blocks))
        return f"dm_{len(self.dms)}"

    def channel_post(self, channel, blocks):
        self.channel_posts.append((channel, blocks))
        return f"ch_{len(self.channel_posts)}"


@dataclass
class FakeUrlChecker:
    dead: set[str] = field(default_factory=set)

    def alive(self, url):
        return url not in self.dead
