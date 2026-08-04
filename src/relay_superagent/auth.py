"""Session cookies: HMAC-signed, expiring, tenant-scoped. WorkOS proves who
the user is; this only remembers it across requests. Constant-time compare,
fail closed on anything malformed."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets as pysecrets
import time
from typing import Any, Callable

from relay_superagent.secrets import get_secret

SESSION_TTL_S = 7 * 24 * 3600
_boot_secret: str | None = None


def session_secret() -> str:
    """Keychain-backed. Created once on first use so sessions survive server
    restarts — a per-boot secret silently logs everyone out on every restart.
    Falls back to per-process only if the keychain write fails."""
    global _boot_secret
    stored = get_secret("session-secret")
    if stored:
        return stored
    fresh = pysecrets.token_hex(32)
    try:
        import subprocess
        subprocess.run(
            ["security", "add-generic-password", "-U", "-s", "relay_superagent",
             "-a", "session-secret", "-w", fresh],
            capture_output=True, timeout=5, check=True)
        return get_secret("session-secret") or fresh
    except Exception:
        if _boot_secret is None:
            _boot_secret = fresh
        return _boot_secret


def sign_session(data: dict[str, Any], secret: str,
                 now: Callable[[], float] = time.time) -> str:
    body = base64.urlsafe_b64encode(json.dumps(
        {**data, "exp": int(now()) + SESSION_TTL_S}).encode()).decode()
    sig = hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest()
    return f"{body}.{sig}"


def verify_session(cookie: str, secret: str,
                   now: Callable[[], float] = time.time) -> dict[str, Any] | None:
    try:
        body, sig = cookie.split(".", 1)
        expected = hmac.new(secret.encode(), body.encode(),
                            hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, sig):
            return None
        data = json.loads(base64.urlsafe_b64decode(body))
        if data.get("exp", 0) < now():
            return None
        return data
    except (ValueError, TypeError, json.JSONDecodeError):
        return None
