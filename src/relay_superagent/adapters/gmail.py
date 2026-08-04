"""The Gmail rail — the second trigger source.

Spec Appendix A originally excluded the inbound-email trigger (the LangChain
reference's noisiest idea). The captain overrode that on 2026-07-31; what
makes it safe here is that email enters the SAME pipeline as calls: the
enrolled-rep check, claim-hash dedupe, suppression window and the gate all
apply, and nothing is ever auto-sent. Noise dies as a suppressed row, not in
a rep's face.

Two pieces, same split as fathom.py:

- `to_trigger_event` is a pure mapper from the Gmail API message resource
  (users.messages.get, format=full) onto TriggerEvent. The message id is the
  idempotency ref. The rep is the connected inbox's owner; the account is the
  external correspondent's domain. Prospect email bodies quote whole threads,
  so only the newest text is kept (reply markers cut).
- `GmailPoller` pulls recent inbound mail over REST with an injectable
  client. Polling, not push: Gmail push needs a GCP Pub/Sub topic — wrong
  cost for one inbox pre-gate. The supervisor's cadence is enough.
"""

from __future__ import annotations

import base64
from datetime import datetime, timezone
from typing import Any

import httpx

from relay_superagent.domain.models import TriggerEvent
from relay_superagent.secrets import get_secret

API = "https://gmail.googleapis.com/gmail/v1/users/me"

# Anything below the first of these is quoted history, not the new message.
REPLY_MARKERS = ("\nOn ", "\n> ", "\n-----Original Message-----")


class GmailError(Exception):
    pass


def _header(msg: dict[str, Any], name: str) -> str:
    for h in (msg.get("payload") or {}).get("headers", []):
        if h.get("name", "").lower() == name.lower():
            return h.get("value", "")
    return ""


def _text_part(part: dict[str, Any]) -> str | None:
    if part.get("mimeType") == "text/plain":
        data = (part.get("body") or {}).get("data")
        if data:
            return base64.urlsafe_b64decode(data + "===").decode(errors="replace")
    for sub in part.get("parts") or []:
        found = _text_part(sub)
        if found:
            return found
    return None


def _address(raw: str) -> str:
    """'Jane Doe <jane@client.com>' -> 'jane@client.com'."""
    if "<" in raw:
        raw = raw.rsplit("<", 1)[1].rstrip(">")
    return raw.strip().lower()


def to_trigger_event(msg: dict[str, Any], tenant_id: str, inbox_email: str,
                     rep_directory: dict[str, str] | None = None,
                     ) -> TriggerEvent | None:
    """None = nothing to detect on: no text, or not from an external party
    (internal mail and own sent mail are pre-trigger noise)."""
    text = _text_part(msg.get("payload") or {})
    if not text or not msg.get("id"):
        return None
    for marker in REPLY_MARKERS:
        cut = text.find(marker)
        if cut > 0:
            text = text[:cut]
    text = text.strip()
    if not text:
        return None

    sender = _address(_header(msg, "From"))
    inbox_domain = inbox_email.rsplit("@", 1)[-1].lower()
    if not sender or sender.rsplit("@", 1)[-1] == inbox_domain:
        return None

    try:
        occurred = datetime.fromtimestamp(
            int(msg.get("internalDate", 0)) / 1000, tz=timezone.utc)
    except (TypeError, ValueError):
        occurred = datetime.now(timezone.utc)

    subject = _header(msg, "Subject")
    return TriggerEvent(
        tenant_id=tenant_id,
        source="gmail",
        source_ref=msg["id"],
        occurred_at=occurred,
        opportunity_id=None,               # CRM linkage happens at safety/draft
        account_id=sender.rsplit("@", 1)[-1],
        rep_user_id=(rep_directory or {}).get(inbox_email, inbox_email),
        text=f"Subject: {subject}\n{sender}: {text}" if subject
             else f"{sender}: {text}",
    )


class GmailPoller:
    """Recent inbound mail over REST. Token from the keychain at point of
    use; client injectable so tests never touch the network."""

    def __init__(self, token: str | None = None,
                 client: httpx.Client | None = None):
        self.token = token or get_secret("gmail-token")
        if not self.token:
            raise GmailError("no token — security add-generic-password -U "
                             "-s relay_superagent -a gmail-token -w '<oauth-access-token>'")
        self.client = client or httpx.Client(
            base_url=API, timeout=15,
            headers={"Authorization": f"Bearer {self.token}"})

    def _get(self, path: str, **params) -> dict[str, Any]:
        try:
            resp = self.client.get(path, params=params)
        except httpx.HTTPError as e:
            raise GmailError(f"transport: {e}") from e
        if resp.status_code != 200:
            raise GmailError(f"{resp.status_code}: {resp.text[:200]}")
        return resp.json()

    def recent_inbound(self, newer_than: str = "1d") -> list[dict[str, Any]]:
        """Full message resources for recent inbox mail, oldest first."""
        listing = self._get("/messages", q=f"in:inbox newer_than:{newer_than}",
                            maxResults=50)
        ids = [m["id"] for m in listing.get("messages", [])]
        return [self._get(f"/messages/{mid}", format="full")
                for mid in reversed(ids)]
