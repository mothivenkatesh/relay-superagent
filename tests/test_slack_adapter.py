"""The Slack rail on a stub transport: Block Kit rendering, the ok:false
failure mode, signature verification with replay protection, and button
payload parsing. No network, no token."""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass, field
from typing import Any

import pytest

from relay_superagent.adapters.slack import (
    RealSlack, SlackError, gate_card, parse_interaction, verify_signature,
)

GATE = {"run_id": "r-42", "claim": "Acme is cheaper",
        "counter": "Three-year TCO says otherwise.",
        "evidence": ["https://ours.example/tco"],
        "reasoning": "Acme was named on a call for an open deal.",
        "actions": ["send", "edit", "dismiss"]}


@dataclass
class StubResponse:
    payload: dict[str, Any]
    def json(self):
        return self.payload


@dataclass
class StubHttp:
    payload: dict[str, Any] = field(default_factory=lambda: {"ok": True, "ts": "171.001"})
    calls: list = field(default_factory=list)
    def post(self, url, json=None, headers=None):
        self.calls.append({"url": url, "json": json, "headers": headers})
        return StubResponse(self.payload)


def make(payload=None) -> RealSlack:
    return RealSlack(bot_token="xoxb-test",
                     client=StubHttp(payload or {"ok": True, "ts": "171.001"}))


# -- rendering ----------------------------------------------------------------

def test_gate_card_carries_claim_counter_evidence_reasoning_and_buttons():
    blocks = gate_card(GATE)
    text = json.dumps(blocks)
    assert "> Acme is cheaper" in text                     # claim, quoted
    assert "Three-year TCO says otherwise." in text
    assert "ours.example/tco" in text
    assert "Why you're seeing this" in text                # §6.7: required
    actions = blocks[-1]
    ids = [e["action_id"] for e in actions["elements"]]
    assert ids == ["approve", "reject", "open_workspace"]
    assert all(e.get("value") == "r-42" for e in actions["elements"][:2])


# -- outbound ------------------------------------------------------------------

def test_dm_posts_to_chat_postmessage_with_bearer_and_returns_ts():
    slack = make()
    ts = slack.dm("U123", GATE)
    call, = slack.client.calls
    assert ts == "171.001"
    assert call["url"] == "/chat.postMessage"
    assert call["headers"]["Authorization"] == "Bearer xoxb-test"
    assert call["json"]["channel"] == "U123"
    assert call["json"]["text"]                            # notification fallback

def test_ok_false_raises_slack_error_despite_http_200():
    with pytest.raises(SlackError, match="channel_not_found"):
        make({"ok": False, "error": "channel_not_found"}).dm("U404", GATE)

def test_missing_token_fails_loud_with_the_fix_in_the_message():
    import relay_superagent.adapters.slack as mod
    orig = mod.get_secret
    mod.get_secret = lambda name: None
    try:
        with pytest.raises(SlackError, match="add-generic-password"):
            RealSlack()
    finally:
        mod.get_secret = orig


# -- signature -----------------------------------------------------------------

SECRET = "8f742231b10e8888abcd99yyyzzz85a5"

def sign(body: bytes, ts: str) -> str:
    base = b"v0:" + ts.encode() + b":" + body
    return "v0=" + hmac.new(SECRET.encode(), base, hashlib.sha256).hexdigest()

def test_valid_signature_passes():
    body = b"payload=%7B%7D"
    assert verify_signature(SECRET, "1000", body, sign(body, "1000"), now=lambda: 1010)

def test_wrong_signature_fails():
    assert not verify_signature(SECRET, "1000", b"x", "v0=deadbeef", now=lambda: 1010)

def test_stale_timestamp_fails_even_with_valid_mac():
    body = b"x"
    assert not verify_signature(SECRET, "1000", body, sign(body, "1000"),
                                now=lambda: 1000 + 301)

def test_garbage_timestamp_fails_closed():
    assert not verify_signature(SECRET, "not-a-number", b"x", "v0=aa")


# -- interaction parsing -------------------------------------------------------

def payload_form(action_id: str, value: str = "r-42") -> str:
    from urllib.parse import urlencode
    return urlencode({"payload": json.dumps({
        "type": "block_actions",
        "user": {"id": "U777", "username": "dan"},
        "actions": [{"action_id": action_id, "value": value}],
    })})

def test_approve_button_maps_to_action_run_actor():
    got = parse_interaction(payload_form("approve"))
    assert got == {"action": "approve", "run_id": "r-42", "actor": "dan"}

def test_reject_button_maps_too():
    assert parse_interaction(payload_form("reject"))["action"] == "reject"

def test_link_button_and_non_block_actions_are_ignored():
    assert parse_interaction(payload_form("open_workspace")) is None
    from urllib.parse import urlencode
    other = urlencode({"payload": json.dumps({"type": "view_submission"})})
    assert parse_interaction(other) is None
    assert parse_interaction("not-a-form") is None
