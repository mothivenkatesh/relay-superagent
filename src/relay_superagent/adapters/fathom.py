"""The Fathom rail — Lane A step 3, the trigger end of the gate sentence.

Two pure pieces; the HTTP endpoint that uses them lives in the serving layer:

- `verify_signature` implements Fathom's Svix-style scheme, which is NOT
  Slack's v0: three headers (webhook-id / webhook-timestamp /
  webhook-signature), HMAC-SHA256 over ``{id}.{timestamp}.{body}``, base64
  digests, the secret base64-decoded from after its ``whsec_`` prefix, and
  possibly several space-delimited ``v1,<sig>`` entries of which any one may
  match. Replay window 5 minutes, checked before any crypto.
- `to_trigger_event` maps the meeting payload onto TriggerEvent. The webhook
  already carries ``crm_matches`` (deals/companies with record URLs), so deal
  linkage costs no CRM round-trip at ingest. The fathom.video URL is the
  stable meeting ref — webhook-id changes only per message, the URL names the
  recording — so idempotency dedupes redelivery AND re-registration.

Reps arrive as emails (``recorded_by.email``); Slack wants member ids. The
mapping is tenant config, passed in as ``rep_directory`` — unknown emails fall
through unchanged so the pipeline's enrolled-rep check suppresses them
instead of a KeyError deciding policy.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import time
from datetime import datetime, timezone
from typing import Any, Callable

from relay_superagent.domain.models import TriggerEvent


def verify_signature(secret: str, msg_id: str, timestamp: str, body: bytes,
                     signature_header: str, now: Callable[[], float] = time.time,
                     tolerance_s: int = 300) -> bool:
    try:
        if abs(now() - int(timestamp)) > tolerance_s:
            return False
        key = base64.b64decode(secret.split("_", 1)[1]
                               if secret.startswith("whsec_") else secret)
    except (TypeError, ValueError):
        return False
    signed = f"{msg_id}.{timestamp}.".encode() + body
    expected = base64.b64encode(
        hmac.new(key, signed, hashlib.sha256).digest()).decode()
    for entry in (signature_header or "").split():
        _, _, sig = entry.partition(",")
        if hmac.compare_digest(expected, sig):
            return True
    return False


def _iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def to_trigger_event(payload: dict[str, Any], tenant_id: str,
                     rep_directory: dict[str, str] | None = None,
                     ) -> TriggerEvent | None:
    """None means "nothing to detect on" — no transcript or no stable ref —
    which is pre-trigger noise, not an error and not a run."""
    transcript = payload.get("transcript") or []
    source_ref = payload.get("url") or payload.get("share_url")
    if not transcript or not source_ref:
        return None

    lines = []
    for entry in transcript:
        speaker = (entry.get("speaker") or {}).get("display_name") or "Unknown"
        text = entry.get("text") or ""
        if text:
            lines.append(f"{speaker}: {text}")
    if not lines:
        return None

    matches = payload.get("crm_matches") or {}
    deals = matches.get("deals") or []
    companies = matches.get("companies") or []
    account = (companies[0].get("record_url") or companies[0].get("name")
               if companies else None)
    if not account:
        external = [i for i in payload.get("calendar_invitees") or []
                    if i.get("is_external") and i.get("email_domain")]
        account = external[0]["email_domain"] if external else None

    rep_email = (payload.get("recorded_by") or {}).get("email")
    rep = (rep_directory or {}).get(rep_email, rep_email)

    occurred = (_iso(payload.get("recording_end_time"))
                or _iso(payload.get("created_at"))
                or datetime.now(timezone.utc))

    return TriggerEvent(
        tenant_id=tenant_id,
        source="fathom",
        source_ref=source_ref,
        occurred_at=occurred,
        opportunity_id=deals[0].get("record_url") if deals else None,
        account_id=account,
        rep_user_id=rep,
        text="\n".join(lines),
    )
