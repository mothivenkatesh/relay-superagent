"""The Fathom rail on pure functions: Svix-style signature verification,
payload → TriggerEvent mapping against the shape from Fathom's own OpenAPI
example, and the payload driven end-to-end through the pipeline on fakes.

Fathom call recordings are inherited reference material — real disputes
arrive as structured bank/processor webhooks with reason_code already
attached, not a transcript to scan. `to_trigger_event` here takes an
explicit `reason_code` the way a real caller would supply one from call
notes; when none is given the mapping still succeeds but the pipeline
produces no row, exactly as `classify_dispute` intends."""

from __future__ import annotations

import base64
import copy
import hashlib
import hmac

from relay_superagent.adapters.fathom import to_trigger_event, verify_signature
from relay_superagent.domain.models import RunState

from .conftest import make_policy

# -- signature ----------------------------------------------------------------

RAW_KEY = b"relay_superagent-test-signing-key-0001"
SECRET = "whsec_" + base64.b64encode(RAW_KEY).decode()


def sign(msg_id: str, ts: str, body: bytes) -> str:
    digest = hmac.new(RAW_KEY, f"{msg_id}.{ts}.".encode() + body,
                      hashlib.sha256).digest()
    return "v1," + base64.b64encode(digest).decode()


def test_valid_signature_passes():
    body = b'{"title": "QBR"}'
    sig = sign("msg_1", "1000", body)
    assert verify_signature(SECRET, "msg_1", "1000", body, sig, now=lambda: 1010)


def test_any_of_multiple_space_delimited_signatures_may_match():
    body = b"x"
    header = "v2,QVBJQVBJ " + sign("msg_1", "1000", body)
    assert verify_signature(SECRET, "msg_1", "1000", body, header, now=lambda: 1010)


def test_wrong_signature_fails():
    assert not verify_signature(SECRET, "msg_1", "1000", b"x",
                                "v1,ZGVhZGJlZWY=", now=lambda: 1010)


def test_body_tamper_fails():
    sig = sign("msg_1", "1000", b"original")
    assert not verify_signature(SECRET, "msg_1", "1000", b"tampered", sig,
                                now=lambda: 1010)


def test_stale_timestamp_fails_even_with_valid_mac():
    body = b"x"
    sig = sign("msg_1", "1000", body)
    assert not verify_signature(SECRET, "msg_1", "1000", body, sig,
                                now=lambda: 1000 + 301)


def test_garbage_timestamp_and_garbage_secret_fail_closed():
    assert not verify_signature(SECRET, "m", "not-a-number", b"x", "v1,aa")
    assert not verify_signature("whsec_!!not-base64!!", "m", "1000", b"x", "v1,aa",
                                now=lambda: 1000)


def test_secret_without_whsec_prefix_is_treated_as_raw_base64():
    body = b"x"
    sig = sign("msg_1", "1000", body)
    bare = base64.b64encode(RAW_KEY).decode()
    assert verify_signature(bare, "msg_1", "1000", body, sig, now=lambda: 1010)


# -- mapping ------------------------------------------------------------------
# The shape from Fathom's OpenAPI `newMeeting` example, dispute narrative injected.

PAYLOAD = {
    "title": "Quarterly Business Review",
    "url": "https://fathom.video/xyz123",
    "share_url": "https://fathom.video/share/xyz123",
    "created_at": "2026-08-03T09:01:30Z",
    "recording_end_time": "2026-08-03T10:00:55Z",
    "transcript": [
        {"speaker": {"display_name": "Jane Doe",
                     "matched_calendar_invitee_email": "jane@client.com"},
         "text": "The order never arrived, I want to dispute the charge.",
         "timestamp": "00:05:32"},
        {"speaker": {"display_name": "Alice Johnson",
                     "matched_calendar_invitee_email": "alice@ours.com"},
         "text": "Happy to pull the delivery record for you.",
         "timestamp": "00:05:40"},
    ],
    "calendar_invitees": [
        {"name": "Jane Doe", "email": "jane@client.com",
         "is_external": True, "email_domain": "client.com"},
        {"name": "Alice Johnson", "email": "alice@ours.com",
         "is_external": False, "email_domain": "ours.com"},
    ],
    "recorded_by": {"name": "Alice Johnson", "email": "alice@ours.com",
                    "team": "Sales"},
    "crm_matches": {
        "contacts": [{"name": "Jane Doe", "email": "jane@client.com",
                      "record_url": "https://app.hubspot.com/contacts/123"}],
        "companies": [{"name": "Client Corp",
                       "record_url": "https://app.hubspot.com/companies/456"}],
        "deals": [{"name": "Order 789", "amount": 50000,
                   "record_url": "https://app.hubspot.com/deals/789"}],
    },
}


def test_maps_the_openapi_example_shape():
    ev = to_trigger_event(PAYLOAD, "t1", reason_code="RG")
    assert ev.source == "fathom"
    assert ev.source_ref == "https://fathom.video/xyz123"
    assert ev.order_id == "https://app.hubspot.com/deals/789"
    assert ev.merchant_id == "https://app.hubspot.com/companies/456"
    assert ev.reason_code == "RG"
    assert ev.occurred_at.isoformat() == "2026-08-03T10:00:55+00:00"
    assert "Jane Doe: The order never arrived" in ev.text
    assert "Alice Johnson: Happy to pull the delivery record" in ev.text


def test_reason_code_defaults_to_none_when_not_supplied():
    ev = to_trigger_event(PAYLOAD, "t1")
    assert ev.reason_code is None


def test_merchant_falls_back_to_external_invitee_domain():
    p = copy.deepcopy(PAYLOAD)
    p["crm_matches"] = {}
    ev = to_trigger_event(p, "t1")
    assert ev.merchant_id == "client.com"
    assert ev.order_id is None


def test_no_transcript_and_no_ref_are_noise_not_errors():
    p = copy.deepcopy(PAYLOAD)
    p["transcript"] = []
    assert to_trigger_event(p, "t1") is None
    p = copy.deepcopy(PAYLOAD)
    del p["url"], p["share_url"]
    assert to_trigger_event(p, "t1") is None


# -- end to end on fakes ------------------------------------------------------

def _wire(world):
    """Point the world's fixtures at what the payload carries."""
    world.d.policy = make_policy()
    world.d.enrolled_merchants = {"https://app.hubspot.com/companies/456"}
    world.d.crm.opportunities["https://app.hubspot.com/deals/789"] = {
        "stage": "evaluation", "amount_band": "50-100k",
        "prior_dispute_history": ["RG"]}


def test_fathom_payload_reaches_the_gate(world):
    _wire(world)
    ev = to_trigger_event(PAYLOAD, "t1", reason_code="RG")
    run = world.handle_event(ev)
    assert run.state is RunState.AWAITING_GATE
    assert run.trigger_source == "fathom"
    assert run.reason_code == "RG"


def test_redelivery_of_the_same_meeting_is_one_run(world):
    _wire(world)
    ev = to_trigger_event(PAYLOAD, "t1", reason_code="RG")
    first = world.handle_event(ev)
    again = world.handle_event(to_trigger_event(PAYLOAD, "t1", reason_code="RG"))
    assert again.run_id == first.run_id
    assert len(world.d.ledger.runs) == 1
