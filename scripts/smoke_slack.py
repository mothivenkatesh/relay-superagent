"""Live Slack smoke — posts one real gate card. Needs in the keychain:

    security add-generic-password -U -s relay_superagent -a slack-bot -w 'xoxb-…'

Usage:  uv run python scripts/smoke_slack.py <channel-or-user-id>
(e.g. your member ID from your Slack profile, or a channel the bot is in)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from relay_superagent.adapters.slack import RealSlack  # noqa: E402

if len(sys.argv) != 2:
    print(__doc__)
    sys.exit(1)

slack = RealSlack()
ts = slack.dm(sys.argv[1], {
    "run_id": "smoke-run",
    "claim": "Acme is cheaper",
    "counter": "On a three-year basis Acme's total cost runs higher once "
               "implementation and per-seat overage are included; the TCO model "
               "puts the crossover at month nine.",
    "evidence": ["https://ours.example/tco"],
    "reasoning": "This is the Relay SuperAgent smoke test — buttons won't work until "
                 "the interactions tunnel is up, and that's expected.",
})
print(f"Gate card posted, ts={ts}. Check Slack — claim, counter, evidence, "
      f"reasoning and three buttons should all be visible.")
