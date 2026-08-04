"""The Fathom rail on pure functions: Svix-style signature verification,
payload → TriggerEvent mapping against the shape from Fathom's own OpenAPI
example, and the payload driven end-to-end through the pipeline on fakes."""

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
# The shape from Fathom's OpenAPI `newMeeting` example, competitor injected.

PAYLOAD = {
    "title": "Quarterly Business Review",
    "url": "https://fathom.video/xyz123",
    "share_url": "https://fathom.video/share/xyz123",
    "created_at": "2026-08-03T09:01:30Z",
    "recording_end_time": "2026-08-03T10:00:55Z",
    "transcript": [
        {"speaker": {"display_name": "Jane Doe",
                     "matched_calendar_invitee_email": "jane@client.com"},
         "text": "We're also looking at Acme, and honestly they are cheaper.",
         "timestamp": "00:05:32"},
        {"speaker": {"display_name": "Alice Johnson",
                     "matched_calendar_invitee_email": "alice@ours.com"},
         "text": "Happy to walk through the numbers.",
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
        "deals": [{"name": "Q3 Renewal", "amount": 50000,
                   "record_url": "https://app.hubspot.com/deals/789"}],
    },
}


def test_maps_the_openapi_example_shape():
    ev = to_trigger_event(PAYLOAD, "t1", rep_directory={"alice@ours.com": "rep_7"})
    assert ev.source == "fathom"
    assert ev.source_ref == "https://fathom.video/xyz123"
    assert ev.opportunity_id == "https://app.hubspot.com/deals/789"
    assert ev.account_id == "https://app.hubspot.com/companies/456"
    assert ev.rep_user_id == "rep_7"
    assert ev.occurred_at.isoformat() == "2026-08-03T10:00:55+00:00"
    assert "Jane Doe: We're also looking at Acme" in ev.text
    assert "Alice Johnson: Happy to walk through" in ev.text


def test_unknown_rep_email_passes_through_for_enrollment_check_to_decide():
    ev = to_trigger_event(PAYLOAD, "t1", rep_directory={})
    assert ev.rep_user_id == "alice@ours.com"


def test_account_falls_back_to_external_invitee_domain():
    p = copy.deepcopy(PAYLOAD)
    p["crm_matches"] = {}
    ev = to_trigger_event(p, "t1")
    assert ev.account_id == "client.com"
    assert ev.opportunity_id is None


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
    world.d.enrolled_reps = {"rep_7"}
    world.d.crm.opportunities["https://app.hubspot.com/deals/789"] = {
        "stage": "evaluation", "amount_band": "50-100k",
        "competitor_history": ["acme"]}


def test_fathom_payload_reaches_the_gate(world):
    _wire(world)
    ev = to_trigger_event(PAYLOAD, "t1", rep_directory={"alice@ours.com": "rep_7"})
    run = world.handle_event(ev)
    assert run.state is RunState.AWAITING_GATE
    assert run.trigger_source == "fathom"
    assert run.competitor_id == "acme"


def test_redelivery_of_the_same_meeting_is_one_run(world):
    _wire(world)
    ev = to_trigger_event(PAYLOAD, "t1", rep_directory={"alice@ours.com": "rep_7"})
    first = world.handle_event(ev)
    again = world.handle_event(
        to_trigger_event(PAYLOAD, "t1", rep_directory={"alice@ours.com": "rep_7"}))
    assert again.run_id == first.run_id
    assert len(world.d.ledger.runs) == 1
