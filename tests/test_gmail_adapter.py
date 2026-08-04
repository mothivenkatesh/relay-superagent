"""The Gmail rail: message-resource mapping (multipart base64url bodies,
reply-chain trimming, internal-mail rejection) and the payload driven through
the pipeline on fakes. Polling client on a stub transport.

Inherited reference material: a real dispute trigger is a bank/processor
webhook, not an inbox poll — see the module docstring in gmail.py."""

from __future__ import annotations

import base64

from relay_superagent.adapters.gmail import GmailPoller, to_trigger_event
from relay_superagent.domain.models import RunState


def b64(s: str) -> str:
    return base64.urlsafe_b64encode(s.encode()).decode().rstrip("=")


def message(body: str, sender="Jane Doe <jane@client.com>",
            subject="Re: chargeback", mid="m_1") -> dict:
    return {
        "id": mid, "threadId": "t_1", "internalDate": "1754211600000",
        "payload": {
            "mimeType": "multipart/alternative",
            "headers": [{"name": "From", "value": sender},
                        {"name": "To", "value": "alice@ours.com"},
                        {"name": "Subject", "value": subject}],
            "parts": [
                {"mimeType": "text/html", "body": {"data": b64("<p>html</p>")}},
                {"mimeType": "text/plain", "body": {"data": b64(body)}},
            ],
        },
    }


def test_maps_multipart_message_and_keeps_subject_and_sender():
    ev = to_trigger_event(message("The order never arrived, I'm disputing it."),
                          "t1", "alice@ours.com", reason_code="RG")
    assert ev.source == "gmail"
    assert ev.source_ref == "m_1"
    assert ev.reason_code == "RG"
    assert ev.merchant_id == "client.com"
    assert ev.order_id is None
    assert "Subject: Re: chargeback" in ev.text
    assert "jane@client.com: The order never arrived" in ev.text


def test_quoted_reply_history_is_trimmed():
    body = ("The order never arrived, disputing the charge.\n\n"
            "On Mon, Aug 3, 2026 Alice wrote:\n"
            "> our previous mail mentioning the refund policy\n> and more")
    ev = to_trigger_event(message(body), "t1", "alice@ours.com")
    assert "The order never arrived, disputing the charge." in ev.text
    assert "refund policy" not in ev.text       # history must not re-trigger


def test_internal_and_own_mail_is_noise():
    internal = message("The order never arrived", sender="Bob <bob@ours.com>")
    assert to_trigger_event(internal, "t1", "alice@ours.com") is None


def test_no_text_part_is_noise():
    msg = message("x")
    msg["payload"]["parts"] = [{"mimeType": "text/html",
                                "body": {"data": b64("<p>only html</p>")}}]
    assert to_trigger_event(msg, "t1", "alice@ours.com") is None


def test_gmail_event_without_order_ref_is_suppressed_not_resolved(world):
    """Disputes carry their order ref on the webhook; the pipeline no longer
    resolves merchant → open order. A gmail-sourced event with no order id is
    suppressed as no_order rather than guessed at."""
    world.d.enrolled_merchants.add("client.com")
    ev = to_trigger_event(
        message("Honestly, the order never arrived."), "t1", "alice@ours.com",
        reason_code="RG")
    run = world.handle_event(ev)
    assert run.state is RunState.SUPPRESSED
    assert run.suppressed_reason == "no_order"


def test_gmail_event_with_order_ref_reaches_the_gate_and_dedupes(world):
    world.d.enrolled_merchants.add("client.com")
    ev = to_trigger_event(
        message("Honestly, the order never arrived."), "t1", "alice@ours.com",
        reason_code="RG")
    ev.order_id = "order_1"                 # upstream enrichment attaches the order
    run = world.handle_event(ev)
    assert run.state is RunState.AWAITING_GATE
    assert run.trigger_source == "gmail"
    again = to_trigger_event(
        message("Honestly, the order never arrived."), "t1", "alice@ours.com",
        reason_code="RG")
    again.order_id = "order_1"
    assert world.handle_event(again).run_id == run.run_id


# -- poller on a stub transport ------------------------------------------------

class StubResp:
    def __init__(self, payload, status=200):
        self.status_code, self._p = status, payload
        self.text = str(payload)
    def json(self):
        return self._p


class StubHttp:
    def __init__(self):
        self.calls = []
    def get(self, path, params=None):
        self.calls.append((path, params))
        if path == "/messages":
            return StubResp({"messages": [{"id": "m_new"}, {"id": "m_old"}]})
        return StubResp(message("hello", mid=path.rsplit("/", 1)[-1]))


def test_poller_lists_then_fetches_full_messages_oldest_first():
    p = GmailPoller(token="tok", client=StubHttp())
    got = p.recent_inbound()
    assert [m["id"] for m in got] == ["m_old", "m_new"]
    assert p.client.calls[0][0] == "/messages"
    assert p.client.calls[0][1]["q"].startswith("in:inbox")
    assert p.client.calls[1][1] == {"format": "full"}
