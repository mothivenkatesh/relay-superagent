"""Wiring smoke for the Fathom endpoint: signs the OpenAPI example payload
with the keychain secret and POSTs it at the running local server, exactly as
Fathom would. Proves signature verification + mapping + pipeline wiring
without a Fathom account or a tunnel.

Needs: server running (uv run python demo/server.py) and a keychain entry
  security add-generic-password -U -s relay_superagent -a fathom-webhook -w 'whsec_…'
(any base64 value works for the smoke; use Fathom's real secret once created).

Usage: uv run python scripts/smoke_fathom.py
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import sys
import time

import httpx

sys.path.insert(0, "tests")
sys.path.insert(0, ".")

from relay_superagent.secrets import get_secret                     # noqa: E402
from tests.test_fathom_adapter import PAYLOAD                 # noqa: E402

secret = get_secret("fathom-webhook")
if not secret:
    sys.exit("no secret — security add-generic-password -U -s relay_superagent "
             "-a fathom-webhook -w 'whsec_<base64>'")

key = base64.b64decode(secret.split("_", 1)[1]
                       if secret.startswith("whsec_") else secret)
body = json.dumps(PAYLOAD).encode()
msg_id, ts = "msg_smoke_1", str(int(time.time()))
digest = hmac.new(key, f"{msg_id}.{ts}.".encode() + body, hashlib.sha256).digest()

resp = httpx.post(
    "http://localhost:8787/webhooks/fathom", content=body,
    headers={"webhook-id": msg_id, "webhook-timestamp": ts,
             "webhook-signature": "v1," + base64.b64encode(digest).decode(),
             "Content-Type": "application/json"})
print(resp.status_code, resp.text)

# tampered body must bounce
resp = httpx.post(
    "http://localhost:8787/webhooks/fathom", content=body + b" ",
    headers={"webhook-id": msg_id, "webhook-timestamp": ts,
             "webhook-signature": "v1," + base64.b64encode(digest).decode(),
             "Content-Type": "application/json"})
print("tampered →", resp.status_code, "(want 401)")
