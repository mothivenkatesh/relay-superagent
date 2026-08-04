"""Ports. Every external rail behind an interface, so the whole pipeline runs in
CI against fakes in milliseconds (spec §9.1). Real adapters — Gong, Salesforce,
Slack, an actual model — land later, one rail at a time, behind these seams.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol


class Clock(Protocol):
    def now(self) -> datetime: ...


class LlmUnavailable(Exception):
    """Raised by an LlmPort when the model cannot answer. Never propagates to a
    rep: the pipeline converts it into escalation, because silence and stack
    traces are both unreachable states."""


class LlmPort(Protocol):
    """One narrow method per seam (§9.3), so each can be scripted, evaluated and
    swapped independently. A single free-form `complete()` would smear the five
    seams back into one untestable blob. Method names are the stable interface;
    what flows through them is Dispute Defender data — a dispute narrative and
    an evidence-backed response, not a competitive counter."""

    def confirm_mention(self, text: str, reason_labels: list[str]) -> dict[str, Any]:
        """Confirms this is a genuine, actionable dispute — not spam or a
        duplicate webhook redelivery. → {is_competitive: bool, claim_text:
        str|None, confidence: float}"""
        ...

    def extract_claim(self, text: str, reason_code: str) -> dict[str, Any]:
        """→ {reason_code, claim_text, speaker_role, confidence}"""
        ...

    def draft_counter(self, claim: str, deal: dict, evidence: list[dict],
                      memory: list[dict]) -> dict[str, Any]:
        """Drafts the evidence-backed dispute response. → {counter_text,
        cited_evidence_ids, confidence, escalate}"""
        ...

    def judge(self, claim: str, counter: str, register_ref: str) -> dict[str, int]:
        """→ {addresses_claim, matches_register, evidence_grounded} each 1..5"""
        ...

    def semantic_diff(self, original: str, edited: str) -> dict[str, Any]:
        """→ {changed: [...], implies: str, example: str, is_material: bool}"""
        ...


    def narrate(self, question: str, tool: str, facts: str) -> dict[str, Any]:
        """Seam 6: rewrite a deterministic result into conversational prose.
        Facts come ONLY from the ledger-derived text passed in; the model
        paraphrases, never adds. The chat stays safe to trust."""
        ...

class CrmPort(Protocol):
    def opportunity(self, order_id: str) -> dict[str, Any] | None: ...
    def deal_context(self, order_id: str) -> dict[str, Any]: ...
    def write_note(self, order_id: str, text: str) -> str: ...

    def open_deal_for_account(self, merchant_id: str) -> str | None:
        """Some trigger sources arrive with no order ref attached; this
        resolves the merchant to its matching open order, or None — and
        None still suppresses, exactly as before."""
        ...


class SlackPort(Protocol):
    def dm(self, user_id: str, blocks: dict[str, Any]) -> str: ...
    def channel_post(self, channel: str, blocks: dict[str, Any]) -> str: ...


class UrlChecker(Protocol):
    def alive(self, url: str) -> bool:
        """2xx within the last 7 days, cached HEAD (§6.6)."""
        ...
