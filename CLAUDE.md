# Relay SuperAgent — project memory

## What this is
An agentic platform for SMB teams (5–10 people): a SuperAgent that scaffolds
and runs a fleet of sub-agents to handle a merchant's operations, driven
entirely by prompting (ask for insights, ask what's in flight, delegate a
task). Not a workflow builder — workflows are value-added services bolted on
top, never the core product. See [`README.md`](README.md) for the full
positioning (the four pillars + the Lassie/no-BYOK constraint).

## Origin
Forked from [CoMarketer](https://github.com/mothivenkatesh/comarketer)
2026-08-04, keeping the harness (pipeline/state-machine, ports+fakes,
Postgres ledger, multi-tenancy, supervisor, orchestrator-hires-agents-by-
prompting pattern) and discarding the domain (GTM deal-defense). Package
renamed `comarketer` → `relay_superagent` throughout; 98 tests passed
post-rename (2 skip without local Postgres). CoMarketer's own docs/specs
are left in `docs/` and `spec/` as reference implementations of the pattern
— they describe the old domain, not this one.

## Operating mode: L8, always
Captain (Mothi) does three things: dumps requirements, injects judgment at
decision points, demands evidence. First mate (the agent) does everything else.
Never pull the captain into the middle (diffs, tool-call narration,
coordination). Don't ask unless something only a human can do. Report by
exception; show evidence, not transcripts. Parallelize streams; keep the
captain thinking about the next decision, not waiting.

## Open decisions (blocking real build — captain's call)
- **Domain**: which SMB operations surface does this target first? (payments
  recon, support, inventory, something Cashfree-adjacent — undecided.)
- **Sub-agents**: what do "insights / work status / delegate work" resolve to
  as concrete agent capabilities and tool access?
- **MVP gate**: CoMarketer's gate (a Fathom call → Slack DM → HubSpot note)
  does not carry over. No replacement gate defined yet — nothing should be
  declared "done" until one exists.
- **Integrations**: since the merchant can't bring their own keys, every
  connector this product uses has to be held and provisioned by the platform.
  Which rails, and who provisions/pays for the upstream API keys, is open.

## Conventions inherited from CoMarketer (still apply — violate = bug)
- Ports and fakes: every external rail behind `ports/base.py`; tests run on
  fakes in ms; real adapters in `src/relay_superagent/adapters/`.
- Secrets: macOS keychain only, never .env. `security add-generic-password
  -U -s relay_superagent -a <name> -w '…'`; fetched via `secrets.py` at point of use.
- Decisions: `decisions.md` is the append-only ledger; write at the moment of
  decision, never at session end. CoMarketer's D1–D21 are inherited history —
  new decisions for this product append after them, don't renumber.
- The run ledger is append-only; gate_* fields write exactly once; timeout
  escalates to a human and NEVER auto-sends. This "no ungated sending"
  principle is the same one Lassie leans on (95%+ auto, human handles the
  tail) — keep it as-is even though the domain is changing.

## How to run
- `uv run pytest` — all tests, fakes only, no credentials.
- `uv run python demo/server.py` — inherited CoMarketer demo workspace on
  :8787. Useful for seeing the harness work end-to-end; not the SMB product.
- `make pg` — repo-local Postgres 17 on :5434 (no sudo, Postgres.app binaries).

## Current state (2026-08-04)
Fresh fork of CoMarketer, rebranded, package renamed, tests green. No
domain-specific code written yet — the inherited agents (Prospector,
deal-defense Watcher) and adapters (Fathom, HubSpot, Slack, Gmail) are
GTM-sales-specific and are reference material only, not the product.
Next real step is a captain decision on the open items above before any
SMB-specific agent gets built.

## Lessons (append when the captain corrects course)
- Product face is Relay SuperAgent. Naming collision risk: "Relay" is
  already Cashfree's product name across Mothi's other repos/docs — this
  project lives at `relay-superagent` specifically to avoid that clash. Don't
  drop the "-superagent" suffix in any filename, repo, or doc title.
- (Inherited from CoMarketer, still true) Design ports come from the live
  reference, not memory — screenshot first, restyle after.
- (Inherited) Paired control rows (input + button) must share exact height:
  bars use align-items:stretch and buttons drop vertical padding.
- (Inherited) Headless-Chrome --virtual-time-budget screenshots freeze CSS
  animations at arbitrary states — verify logic from those shots, never
  animation end-states; real browsers finish fill:both.
