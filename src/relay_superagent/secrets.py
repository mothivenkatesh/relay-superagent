"""Secrets via the macOS keychain, never .env files (decisions.md).

Store once:  security add-generic-password -U -s relay_superagent -a anthropic -w '<key>'
The value is fetched at point of use and never logged.
"""

from __future__ import annotations

import os
import subprocess


def get_secret(name: str) -> str | None:
    env = os.environ.get(name.upper().replace("-", "_") + "_API_KEY")
    if env:
        return env
    try:
        out = subprocess.run(
            ["security", "find-generic-password", "-s", "relay_superagent", "-a", name, "-w"],
            capture_output=True, text=True, timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    return out.stdout.strip() or None
