"""The real model behind LlmPort — Lane A step 1.

Five seams, two models (decision D3): Haiku 4.5 on confirm/extract because they
run on every trigger, Sonnet 5 on draft/judge/diff because quality matters and
they run less. Every call uses structured outputs (`output_config.format`), so
responses are schema-valid JSON and there is no parsing heuristics layer.

Failure mapping is the contract that matters: connection errors, rate-limit
exhaustion, 5xx and refusals all become `LlmUnavailable`, which the pipeline
converts into PMM escalation. A merchant never sees a stack trace; a 400 is a
bug in this file and is allowed to propagate loudly.
"""

from __future__ import annotations

import json
from typing import Any

import anthropic

from relay_superagent.ports.base import LlmUnavailable
from relay_superagent.secrets import get_secret

FAST_MODEL = "claude-haiku-4-5"      # confirm, extract — runs on every trigger
QUALITY_MODEL = "claude-sonnet-5"    # draft, judge, diff — quality-sensitive


def _schema(properties: dict[str, Any], required: list[str] | None = None) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": required or list(properties),
        "additionalProperties": False,
    }

CONFIRM_SCHEMA = _schema({
    "is_competitive": {"type": "boolean"},
    "claim_text": {"type": ["string", "null"],
                   "description": "The dispute narrative, paraphrased tightly; null if none"},
    "confidence": {"type": "number"},
})

CLAIM_SCHEMA = _schema({
    "reason_code": {"type": "string"},
    "claim_text": {"type": "string"},
    "speaker_role": {"type": "string", "enum": ["buyer", "rep", "other"]},
    "confidence": {"type": "number"},
})

DRAFT_SCHEMA = _schema({
    "counter_text": {"type": "string"},
    "cited_evidence_ids": {"type": "array", "items": {"type": "string"}},
    "confidence": {"type": "number"},
    "escalate": {"type": "boolean",
                 "description": "True if a human should review before anyone at the business sees this"},
})

JUDGE_SCHEMA = _schema({
    "addresses_claim": {"type": "integer", "enum": [1, 2, 3, 4, 5]},
    "matches_register": {"type": "integer", "enum": [1, 2, 3, 4, 5]},
    "evidence_grounded": {"type": "integer", "enum": [1, 2, 3, 4, 5]},
})

DIFF_SCHEMA = _schema({
    "changed": {"type": "array", "items": {"type": "string"}},
    "implies": {"type": "string"},
    "example": {"type": "string"},
    "is_material": {"type": "boolean"},
})

# The five seam prompts, canonical and inspectable. These are code: versioned
# in git, exercised by evals/, rendered read-only in the agent console.
# Merchant customization enters as DATA (evidence library, response-style
# memory, policy) injected into these fixed templates — never by editing them
# at runtime.
SEAM_PROMPTS = {
    "confirm_mention": (
        "You screen inbound dispute/chargeback webhooks for a small "
        "business. "
        "Decide whether this is a genuine, actionable dispute (a real "
        "chargeback filed against a real order) as opposed to noise (a "
        "duplicate webhook redelivery, a test payload, or a notification "
        "with no dispute actually attached)."),
    "extract_claim": (
        "Extract the single dispute claim being made in this narrative. "
        "reason_code must be echoed exactly as given. claim_text is the "
        "buyer's claim in one tight sentence (e.g. 'buyer says the order "
        "never arrived')."),
    "draft_counter": (
        "You draft evidence-backed responses to payment disputes on behalf "
        "of a small business. Rules: cite ONLY evidence ids from the list provided, "
        "never invent a fact or a source, never use superlatives like best "
        "or leading, no contact details, 2-4 sentences, confident and "
        "specific, and reference the concrete proof (delivery record, "
        "invoice, communication log) that backs the response. If the "
        "evidence cannot support a response to this claim, set "
        "escalate=true and say why in counter_text."),
    "judge": (
        "Score this drafted dispute response 1-5 on each axis. "
        "addresses_claim: does it answer the specific claim made, not a "
        "nearby one. matches_register: professional, factual, no hype, no "
        "disparagement of the buyer. evidence_grounded: every assertion is "
        "supported by the cited evidence. Score strictly; 5 means you would "
        "file it with the bank yourself."),
}

SEAM_PROMPTS["narrate"] = (
    "You rewrite a structured Relay result into one to three conversational "
    "sentences for a business owner. Use ONLY facts present in FACTS — every "
    "number, name and claim in your reply must appear there verbatim. Never "
    "add analysis, advice or data that is not present. Plain, warm, direct.")

NARRATE_SCHEMA = {
    "type": "object",
    "properties": {"narration": {"type": "string"}},
    "required": ["narration"],
    "additionalProperties": False,
}

SEAM_PROMPTS["semantic_diff"] = """You compare an AI-drafted dispute response with the version a human \
operator at the business actually sent, and classify the edit.

is_material is true ONLY if the meaning changed: a different claim, a different \
piece of evidence, an argument added or removed, or a factual correction. It is \
false for typo, grammar, tone, length or formatting changes. This value drives \
billing, so when genuinely uncertain, choose true (the customer-favourable answer).

changed: the specific differences. implies: what the edit suggests about how this \
business likes to respond (one sentence). example: a short quote from the edited \
text that shows the preference."""


class ClaudeLlm:
    """LlmPort implementation on the Anthropic API."""

    def __init__(self, client: anthropic.Anthropic | None = None,
                 fast_model: str = FAST_MODEL, quality_model: str = QUALITY_MODEL):
        if client is None:
            client = anthropic.Anthropic(api_key=get_secret("anthropic"))
        self.client = client
        self.fast_model = fast_model
        self.quality_model = quality_model

    # -- seam 1 ---------------------------------------------------------------
    def confirm_mention(self, text: str, reason_labels: list[str]) -> dict[str, Any]:
        return self._call(
            self.fast_model,
            system=SEAM_PROMPTS["confirm_mention"],
            user=f"Dispute reason(s): {', '.join(reason_labels)}\n\nNarrative:\n{text}",
            schema=CONFIRM_SCHEMA,
        )

    # -- seam 2 ---------------------------------------------------------------
    def extract_claim(self, text: str, reason_code: str) -> dict[str, Any]:
        return self._call(
            self.fast_model,
            system=SEAM_PROMPTS["extract_claim"],
            user=f"reason_code: {reason_code}\n\nNarrative:\n{text}",
            schema=CLAIM_SCHEMA,
        )

    # -- seam 3 ---------------------------------------------------------------
    def draft_counter(self, claim: str, deal: dict, evidence: list[dict],
                      memory: list[dict]) -> dict[str, Any]:
        ev = "\n".join(f"- [{e['evidence_id']}] {e['text']} ({e['source_url']})"
                       for e in evidence) or "(none available)"
        style = "\n".join(f"- {m.get('implies', '')}" for m in memory if m.get("implies"))
        return self._call(
            self.quality_model,
            system=SEAM_PROMPTS["draft_counter"],
            user=f"Claim to respond to: {claim}\n\nOrder context: {json.dumps(deal)}\n\n"
                 f"Available evidence:\n{ev}\n\n"
                 + (f"This business's style, learned from their edits:\n{style}" if style else ""),
            schema=DRAFT_SCHEMA,
        )

    # -- seam 4 ---------------------------------------------------------------
    def judge(self, claim: str, counter: str, register_ref: str) -> dict[str, int]:
        return self._call(
            self.quality_model,
            system=SEAM_PROMPTS["judge"],
            user=f"Claim: {claim}\n\nDrafted response: {counter}",
            schema=JUDGE_SCHEMA,
        )

    # -- seam 5 ---------------------------------------------------------------
    def semantic_diff(self, original: str, edited: str) -> dict[str, Any]:
        return self._call(
            self.quality_model,
            system=SEAM_PROMPTS["semantic_diff"],
            user=f"ORIGINAL:\n{original}\n\nEDITED:\n{edited}",
            schema=DIFF_SCHEMA,
        )

    # -- seam 6 ---------------------------------------------------------------
    def narrate(self, question: str, tool: str, facts: str) -> dict[str, Any]:
        return self._call(
            self.fast_model,
            system=SEAM_PROMPTS["narrate"],
            user=f"QUESTION: {question}\nTOOL: {tool}\nFACTS:\n{facts}",
            schema=NARRATE_SCHEMA,
        )

    # -- transport ------------------------------------------------------------
    def _call(self, model: str, system: str, user: str,
              schema: dict[str, Any]) -> dict[str, Any]:
        try:
            response = self.client.messages.create(
                model=model,
                max_tokens=1024,
                system=system,
                messages=[{"role": "user", "content": user}],
                output_config={"format": {"type": "json_schema", "schema": schema}},
            )
        except anthropic.BadRequestError:
            raise                                   # a bug here, not an outage
        except (anthropic.APIConnectionError, anthropic.RateLimitError,
                anthropic.InternalServerError, anthropic.APIStatusError) as e:
            raise LlmUnavailable(str(e)) from e

        if response.stop_reason == "refusal":
            raise LlmUnavailable("model refusal")
        if response.stop_reason == "max_tokens":
            raise LlmUnavailable("output truncated")

        text = next((b.text for b in response.content if b.type == "text"), None)
        if text is None:
            raise LlmUnavailable("no text block in response")
        return json.loads(text)
