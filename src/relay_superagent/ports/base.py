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
    seams back into one untestable blob."""

    def confirm_mention(self, text: str, competitor_names: list[str]) -> dict[str, Any]:
        """→ {is_competitive: bool, claim_text: str|None, confidence: float}"""
        ...

    def extract_claim(self, text: str, competitor_id: str) -> dict[str, Any]:
        """→ {competitor_id, claim_text, speaker_role, confidence}"""
        ...

    def draft_counter(self, claim: str, deal: dict, evidence: list[dict],
                      memory: list[dict]) -> dict[str, Any]:
        """→ {counter_text, cited_evidence_ids, confidence, escalate}"""
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
    def opportunity(self, opportunity_id: str) -> dict[str, Any] | None: ...
    def deal_context(self, opportunity_id: str) -> dict[str, Any]: ...
    def write_note(self, opportunity_id: str, text: str) -> str: ...

    def open_deal_for_account(self, account_id: str) -> str | None:
        """Email triggers arrive with no deal attached; this resolves the
        account (a domain, a company record) to its open deal, or None —
        and None still suppresses, exactly as before."""
        ...


class SlackPort(Protocol):
    def dm(self, user_id: str, blocks: dict[str, Any]) -> str: ...
    def channel_post(self, channel: str, blocks: dict[str, Any]) -> str: ...


class UrlChecker(Protocol):
    def alive(self, url: str) -> bool:
        """2xx within the last 7 days, cached HEAD (§6.6)."""
        ...
