"""Live smoke test for the Claude adapter — the only test that spends money.

Needs the API key in the keychain first (one-time, human-only):

    security add-generic-password -U -s relay_superagent -a anthropic -w 'sk-ant-...'

Then:  uv run python scripts/smoke_llm.py
Runs one pass through all five seams on a canned transcript, ~1 cent total.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from relay_superagent.llm.claude import ClaudeLlm  # noqa: E402
from relay_superagent.secrets import get_secret  # noqa: E402

TRANSCRIPT = ("Honestly we like what we've seen, but we're also talking to Acme "
              "and their quote came in about forty percent lower than yours.")
EVIDENCE = [{"evidence_id": "ev_tco",
             "text": "Three-year TCO model: per-seat overage adds 31% by year two",
             "source_url": "https://ours.example/tco"}]

if not get_secret("anthropic"):
    print(__doc__)
    sys.exit(1)

llm = ClaudeLlm()

mention = llm.confirm_mention(TRANSCRIPT, ["Acme", "Acme Corp"])
print("1 confirm_mention :", json.dumps(mention))
assert mention["is_competitive"] is True

claim = llm.extract_claim(TRANSCRIPT, "acme")
print("2 extract_claim   :", json.dumps(claim))
assert claim["competitor_id"] == "acme"

draft = llm.draft_counter(claim["claim_text"], {"stage": "evaluation"}, EVIDENCE, [])
print("3 draft_counter   :", json.dumps(draft))
assert set(draft["cited_evidence_ids"]) <= {"ev_tco"}, "invented a citation"

verdict = llm.judge(claim["claim_text"], draft["counter_text"], "narrative_map")
print("4 judge           :", json.dumps(verdict))

diff = llm.semantic_diff(draft["counter_text"],
                         draft["counter_text"] + " Happy to walk through the model.")
print("5 semantic_diff   :", json.dumps(diff))
assert diff["is_material"] is False, "an appended pleasantry should not be material"

print("\nAll five seams answered. The pipeline can go live on real models.")
