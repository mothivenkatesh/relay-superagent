"""The Claude adapter, on a stub transport: model routing per seam, JSON
handling, and — most importantly — the failure mapping the pipeline's
escalation path depends on. No network, no key."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import anthropic
import httpx
import pytest

from relay_superagent.llm.claude import FAST_MODEL, QUALITY_MODEL, ClaudeLlm
from relay_superagent.ports.base import LlmUnavailable


@dataclass
class Block:
    type: str
    text: str = ""


@dataclass
class Response:
    content: list
    stop_reason: str = "end_turn"


@dataclass
class StubMessages:
    payload: dict[str, Any] = field(default_factory=lambda: {"ok": True})
    stop_reason: str = "end_turn"
    error: Exception | None = None
    calls: list[dict] = field(default_factory=list)

    def create(self, **kw):
        self.calls.append(kw)
        if self.error:
            raise self.error
        return Response(content=[Block("text", json.dumps(self.payload))],
                        stop_reason=self.stop_reason)


@dataclass
class StubClient:
    messages: StubMessages = field(default_factory=StubMessages)


def make(payload=None, stop_reason="end_turn", error=None) -> ClaudeLlm:
    stub = StubMessages(payload=payload or {"ok": True},
                       stop_reason=stop_reason, error=error)
    return ClaudeLlm(client=StubClient(messages=stub))


def test_cheap_seams_use_fast_model_quality_seams_use_quality_model():
    llm = make({"is_competitive": True, "claim_text": "x", "confidence": 0.9})
    llm.confirm_mention("text", ["Acme"])
    llm.extract_claim("text", "acme")
    assert [c["model"] for c in llm.client.messages.calls] == [FAST_MODEL, FAST_MODEL]

    llm2 = make({"changed": [], "implies": "", "example": "", "is_material": False})
    llm2.semantic_diff("a", "b")
    llm2.judge("claim", "counter", "ref")
    assert [c["model"] for c in llm2.client.messages.calls] == [QUALITY_MODEL, QUALITY_MODEL]


def test_every_call_uses_structured_outputs_with_closed_schema():
    llm = make({"is_competitive": False, "claim_text": None, "confidence": 0.2})
    llm.confirm_mention("text", ["Acme"])
    fmt = llm.client.messages.calls[0]["output_config"]["format"]
    assert fmt["type"] == "json_schema"
    assert fmt["schema"]["additionalProperties"] is False
    assert set(fmt["schema"]["required"]) == set(fmt["schema"]["properties"])


def test_no_sampling_params_are_ever_sent():
    llm = make({"changed": [], "implies": "", "example": "", "is_material": True})
    llm.semantic_diff("a", "b")
    call = llm.client.messages.calls[0]
    assert "temperature" not in call and "top_p" not in call and "top_k" not in call


def test_draft_prompt_carries_evidence_ids_and_memory():
    llm = make({"counter_text": "c", "cited_evidence_ids": ["ev_1"],
                "confidence": 0.8, "escalate": False})
    llm.draft_counter(
        "Acme is cheaper", {"stage": "evaluation"},
        [{"evidence_id": "ev_1", "text": "TCO model", "source_url": "https://x/y"}],
        [{"implies": "prefers shorter counters"}])
    user = llm.client.messages.calls[0]["messages"][0]["content"]
    assert "[ev_1]" in user and "prefers shorter counters" in user


def test_connection_error_maps_to_llm_unavailable():
    err = anthropic.APIConnectionError(request=httpx.Request("POST", "https://api"))
    with pytest.raises(LlmUnavailable):
        make(error=err).confirm_mention("t", ["Acme"])


def test_refusal_maps_to_llm_unavailable():
    with pytest.raises(LlmUnavailable, match="refusal"):
        make(stop_reason="refusal").judge("c", "x", "r")


def test_truncation_maps_to_llm_unavailable():
    with pytest.raises(LlmUnavailable, match="truncated"):
        make(stop_reason="max_tokens").draft_counter("c", {}, [], [])


def test_bad_request_propagates_as_a_bug_not_an_outage():
    err = anthropic.BadRequestError(
        "bad", response=httpx.Response(400, request=httpx.Request("POST", "https://api")),
        body=None)
    with pytest.raises(anthropic.BadRequestError):
        make(error=err).confirm_mention("t", ["Acme"])
