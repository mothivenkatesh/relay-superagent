"""The real Slack rail — Lane A step 2. First real adapter behind SlackPort.

Three pieces, all small on purpose:

- `gate_card` / `escalation_card` render the pipeline's block payloads into
  Block Kit. Rendering is pure and unit-tested; the pipeline never learns
  Slack exists.
- `RealSlack` implements SlackPort over `chat.postMessage`. Slack signals
  failure as HTTP 200 with ``{"ok": false}``, so success is checked on the
  body, never the status code. The returned ``ts`` is the external ref the
  effect table stores.
- `verify_signature` / `parse_interaction` are the inbound half: Slack's v0
  HMAC scheme with replay protection, and the button payload mapped to
  (action, run_id, actor). The HTTP endpoint that uses them stays in the
  serving layer; this module has no opinions about frameworks.

Editing from Slack is deliberately a link out to the workspace, not a modal:
a modal needs trigger_id round-trips and view state, and the workspace
already has the edit box. One decision path, two transports (spec §7).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any, Callable
from urllib.parse import parse_qs

import httpx

from relay_superagent.secrets import get_secret

WORKSPACE_URL = "http://localhost:8787"


class SlackError(Exception):
    """Slack said no — ok:false, auth failure, or transport error."""


# -- rendering ---------------------------------------------------------------

def _mrkdwn(text: str) -> dict[str, Any]:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def gate_card(b: dict[str, Any]) -> list[dict[str, Any]]:
    """The §6.7 anatomy: claim quoted, counter, evidence links, visible
    reasoning, then the three actions."""
    run_id = b.get("run_id", "")
    evidence = " · ".join(f"<{u}|{u.rsplit('/', 1)[-1]}>" for u in b.get("evidence", []))
    blocks: list[dict[str, Any]] = [
        _mrkdwn(f"> {b.get('claim', '')}"),
        _mrkdwn(b.get("counter", "")),
    ]
    if evidence:
        blocks.append({"type": "context",
                       "elements": [{"type": "mrkdwn", "text": f"Evidence: {evidence}"}]})
    blocks.append({"type": "context",
                   "elements": [{"type": "mrkdwn",
                                 "text": f"*Why you're seeing this* — {b.get('reasoning', '')}"}]})
    blocks.append({
        "type": "actions",
        "block_id": "gate",
        "elements": [
            {"type": "button", "style": "primary", "action_id": "approve",
             "value": run_id, "text": {"type": "plain_text", "text": "Approve & send"}},
            {"type": "button", "action_id": "reject", "value": run_id,
             "text": {"type": "plain_text", "text": "Dismiss"}},
            {"type": "button", "action_id": "open_workspace", "url": WORKSPACE_URL,
             "text": {"type": "plain_text", "text": "Edit in Relay"}},
        ],
    })
    return blocks


def escalation_card(b: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        _mrkdwn(f":warning: *Escalation* — `{b.get('reason', 'unknown')}`"),
        {"type": "context",
         "elements": [{"type": "mrkdwn",
                       "text": f"Claim: “{b.get('claim') or '—'}” · run {b.get('run', '—')}"}]},
    ]


# -- outbound ----------------------------------------------------------------

class RealSlack:
    """SlackPort over the Web API. Tokens come from the keychain at point of
    use; a client is injectable so tests never touch the network."""

    def __init__(self, bot_token: str | None = None, client: httpx.Client | None = None):
        self.token = bot_token or get_secret("slack-bot")
        if not self.token:
            raise SlackError("no bot token — "
                             "security add-generic-password -U -s relay_superagent -a slack-bot -w 'xoxb-…'")
        self.client = client or httpx.Client(
            base_url="https://slack.com/api", timeout=10)

    def _post(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            resp = self.client.post(
                f"/{method}", json=payload,
                headers={"Authorization": f"Bearer {self.token}"})
            data = resp.json()
        except (httpx.HTTPError, json.JSONDecodeError) as e:
            raise SlackError(f"transport: {e}") from e
        if not data.get("ok"):
            raise SlackError(data.get("error", "unknown"))
        return data

    def dm(self, user_id: str, blocks: dict[str, Any]) -> str:
        data = self._post("chat.postMessage", {
            "channel": user_id,
            "text": f"Counter ready: {blocks.get('claim', '')}",   # notification fallback
            "blocks": gate_card(blocks),
        })
        return data["ts"]

    def channel_post(self, channel: str, blocks: dict[str, Any]) -> str:
        data = self._post("chat.postMessage", {
            "channel": channel,
            "text": f"Escalation: {blocks.get('reason', '')}",
            "blocks": escalation_card(blocks),
        })
        return data["ts"]


# -- inbound -----------------------------------------------------------------

def verify_signature(signing_secret: str, timestamp: str, body: bytes,
                     signature: str, now: Callable[[], float] = time.time,
                     tolerance_s: int = 300) -> bool:
    """Slack's v0 scheme with replay protection. Constant-time compare;
    stale timestamps rejected before any crypto."""
    try:
        if abs(now() - int(timestamp)) > tolerance_s:
            return False
    except (TypeError, ValueError):
        return False
    base = b"v0:" + timestamp.encode() + b":" + body
    expected = "v0=" + hmac.new(signing_secret.encode(), base, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature or "")


def parse_interaction(raw_form_body: str) -> dict[str, str] | None:
    """Slack posts ``payload=<json>`` form-encoded. Returns
    {action, run_id, actor} for gate buttons, None for anything else."""
    payload = parse_qs(raw_form_body).get("payload", [None])[0]
    if not payload:
        return None
    data = json.loads(payload)
    if data.get("type") != "block_actions":
        return None
    for act in data.get("actions", []):
        if act.get("action_id") in ("approve", "reject"):
            return {"action": act["action_id"],
                    "run_id": act.get("value", ""),
                    "actor": data.get("user", {}).get("username")
                             or data.get("user", {}).get("id", "slack")}
    return None
