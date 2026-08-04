"""The Gmail rail: message-resource mapping (multipart base64url bodies,
reply-chain trimming, internal-mail rejection) and the payload driven through
the pipeline on fakes. Polling client on a stub transport."""

from __future__ import annotations

import base64

from relay_superagent.adapters.gmail import GmailPoller, to_trigger_event
from relay_superagent.domain.models import RunState


def b64(s: str) -> str:
    return base64.urlsafe_b64encode(s.encode()).decode().rstrip("=")


def message(body: str, sender="Jane Doe <jane@client.com>",
            subject="Re: pricing", mid="m_1") -> dict:
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
    ev = to_trigger_event(message("We're also looking at Acme."),
                          "t1", "alice@ours.com",
                          rep_directory={"alice@ours.com": "rep_7"})
    assert ev.source == "gmail"
    assert ev.source_ref == "m_1"
    assert ev.rep_user_id == "rep_7"
    assert ev.account_id == "client.com"
    assert ev.opportunity_id is None
    assert "Subject: Re: pricing" in ev.text
    assert "jane@client.com: We're also looking at Acme." in ev.text


def test_quoted_reply_history_is_trimmed():
    body = ("Acme quoted us 40% less.\n\nOn Mon, Aug 3, 2026 Alice wrote:\n"
            "> our previous mail mentioning RivalSoft\n> and more")
    ev = to_trigger_event(message(body), "t1", "alice@ours.com")
    assert "Acme quoted us 40% less." in ev.text
    assert "RivalSoft" not in ev.text          # history must not re-trigger


def test_internal_and_own_mail_is_noise():
    internal = message("Acme came up again", sender="Bob <bob@ours.com>")
    assert to_trigger_event(internal, "t1", "alice@ours.com") is None


def test_no_text_part_is_noise():
    msg = message("x")
    msg["payload"]["parts"] = [{"mimeType": "text/html",
                                "body": {"data": b64("<p>only html</p>")}}]
    assert to_trigger_event(msg, "t1", "alice@ours.com") is None


def test_gmail_event_reaches_the_gate_and_dedupes(world):
    world.d.enrolled_reps.add("rep_7")
    world.d.crm.deals_by_account["client.com"] = "opp_1"    # domain → open deal
    ev = to_trigger_event(
        message("Honestly, Acme is cheaper for us."), "t1", "alice@ours.com",
        rep_directory={"alice@ours.com": "rep_7"})
    run = world.handle_event(ev)
    assert run.state is RunState.AWAITING_GATE
    assert run.trigger_source == "gmail"
    again = world.handle_event(to_trigger_event(
        message("Honestly, Acme is cheaper for us."), "t1", "alice@ours.com",
        rep_directory={"alice@ours.com": "rep_7"}))
    assert again.run_id == run.run_id


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
