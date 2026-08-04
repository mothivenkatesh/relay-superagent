"""Live smoke for the HubSpot rail. Read-only by default; pass a deal ref
and --note to exercise the act.

Needs: security add-generic-password -U -s relay_superagent -a hubspot-token -w 'pat-…'
Usage:
  uv run python scripts/smoke_hubspot.py <deal-id-or-url>            # read
  uv run python scripts/smoke_hubspot.py <deal-id-or-url> --note     # + write test note
  uv run python scripts/smoke_hubspot.py --domain client.com         # account -> open deal
"""

from __future__ import annotations

import sys

sys.path.insert(0, "src")

from relay_superagent.adapters.hubspot import HubSpot  # noqa: E402

args = sys.argv[1:]
hs = HubSpot()

if args and args[0] == "--domain":
    print("open deal for", args[1], "->", hs.open_deal_for_account(args[1]))
    sys.exit(0)

if not args:
    sys.exit("usage: smoke_hubspot.py <deal-id-or-url> [--note] | --domain <domain>")

opp = hs.opportunity(args[0])
print("deal:", opp)
if opp and "--note" in args:
    ref = hs.write_note(args[0], "Relay smoke test note — safe to delete.")
    print("note written, id:", ref)
