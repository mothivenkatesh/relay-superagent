"""Demo state: one versioned store for everything a merchant changes.

Three guarantees, in order of importance:
- Backward compatible: every file carries a schema version. Old versions
  are migrated on load; unknown versions and corrupt files fail open to
  an empty state, never a crash. Unknown keys inside a known version are
  ignored, so a newer server reads an older file and vice versa.
- Durable: writes are atomic (tempfile + os.replace), so a crash mid-save
  leaves the previous state intact, never a half-written file.
- Thread-safe: LOCK serialises every request handler; save() also takes
  it, so a persist can never interleave with a mutation.
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path

SCHEMA_VERSION = 2  # v1 was the unversioned .demo_cfg.json ({"cfg","on"})
PATH = Path(__file__).parent / ".demo_state.json"
LEGACY_PATH = Path(__file__).parent / ".demo_cfg.json"

# One coarse lock around every request. The demo trades parallel writes
# for correctness; reads of a fully rendered page hold it for ~ms.
LOCK = threading.RLock()

# The keys a state file may carry. Anything else is dropped on load, so
# a file written by a newer build never injects state this build doesn't
# understand.
KNOWN_KEYS = {"agent_cfg", "demo_on", "autonomy", "routines",
              "kfiles", "assign", "folders"}


def save(**stores) -> None:
    """Atomic write of the given stores plus the schema stamp."""
    with LOCK:
        payload: dict = {"schema": SCHEMA_VERSION}
        payload.update({k: v for k, v in stores.items() if k in KNOWN_KEYS})
        fd, tmp = tempfile.mkstemp(dir=str(PATH.parent),
                                   prefix=".demo_state.")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(payload, f)
            os.replace(tmp, PATH)
        except OSError:
            try:
                os.unlink(tmp)
            except OSError:
                pass


def load() -> dict:
    """Read state, migrating older schemas. Fails open to {}."""
    raw = None
    for path in (PATH, LEGACY_PATH):
        try:
            raw = json.loads(path.read_text())
            break
        except (OSError, ValueError):
            continue
    if not isinstance(raw, dict):
        return {}
    version = raw.get("schema")
    if version == SCHEMA_VERSION:
        return {k: v for k, v in raw.items() if k in KNOWN_KEYS}
    return _migrate(raw, version)


def _migrate(d: dict, version) -> dict:
    """v1 (and the unversioned .demo_cfg.json) → v2 key names."""
    if version in (None, 1):
        out: dict = {}
        if isinstance(d.get("cfg"), dict):
            out["agent_cfg"] = d["cfg"]
        if isinstance(d.get("on"), dict):
            out["demo_on"] = d["on"]
        return out
    # Unknown (likely newer) schema: fail open rather than misread it.
    return {}
