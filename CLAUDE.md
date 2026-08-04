# Relay — project memory

## What this is
Relay: an agent orchestrator for payments and compliance operations, built
for SMB teams (5–10 people). Eight pre-built agents (Dispute Defender, COD
Guard, Payment Rescue, Cart Rescue, Settlement Clarity, Refund Shield, Loan
Recovery, Due Diligence) plus build-your-own. The merchant prompts one
conversation (insights, work status, delegation); specialist agents run the
work behind it; every money- or customer-touching action waits at a human
gate. Not a workflow builder — workflows are value-added services on top,
never the core. Canonical agent lineup + copy source: Mothi's Relay Intro
deck (~/Downloads/Relay Intro (4).html). Trust line, used verbatim in
product copy: "You approve before anything sends."

## Origin + rebuild history
Forked from [CoMarketer](https://github.com/mothivenkatesh/comarketer)
2026-08-04 for the domain-agnostic harness (run state-machine, ports+fakes,
append-only Postgres ledger with RLS, multi-tenancy, supervisor,
orchestrator-hires-agents-by-prompting pattern). Same day, deep domain
rebuild: the GTM deal-defense domain was replaced end-to-end by the
**Dispute Defender** domain — models (DisputeReason/merchant_id/order_id/
dispute_id/reason_code/deadline_at/AgentType), detect (structured
reason-code classify, no regex), pipeline, policy, SQL schema, seam
prompts, all tests, and the demo workspace content. 108 tests green
(incl. live Postgres contract tests). Fathom/HubSpot/Gmail/Slack adapters
remain as reference implementations of the adapter pattern only.

## Operating mode: L8, always
Captain (Mothi) does three things: dumps requirements, injects judgment at
decision points, demands evidence. First mate (the agent) does everything else.
Never pull the captain into the middle (diffs, tool-call narration,
coordination). Don't ask unless something only a human can do. Report by
exception; show evidence, not transcripts. Parallelize streams; keep the
captain thinking about the next decision, not waiting.

## Domain rules (Dispute Defender — violate = bug)
- Disputes arrive as structured webhooks with a reason_code; "detection" is
  a deterministic policy lookup (detect.classify_dispute), never regex/LLM.
- Dispute runs are NEVER held out (arm=TREATED always) — missing a
  chargeback deadline is real merchant money, not a valid experiment.
- Drafts cite only the evidence vault, matched by reason_code.
- LlmPort seam names (confirm_mention/extract_claim/draft_counter/judge/
  semantic_diff) and CrmPort names (deal_context/write_note) are kept from
  the fork for interface stability; the dispute semantics live in what
  flows through them and in SEAM_PROMPTS. Don't rename seams casually.
- Trace agent labels: detection-agent, eligibility-agent, response-agent,
  compliance-agent, filing-agent, gate, escalation-agent, reporting-agent.

## Open decisions (captain's call)
- **MVP gate**: no real-webhook gate defined yet. Everything runs on fakes;
  nothing is "done" until a real dispute webhook → gate → filed response
  path exists against a live rail.
- **Integrations**: merchant can't bring their own keys (no-BYOK
  constraint, Lassie precedent) — the platform holds every credential.
  Which rails and who provisions upstream keys is open.
- **Agent #2**: which of the seven roadmap agents gets wired next.

## Conventions inherited from CoMarketer (still apply — violate = bug)
- Ports and fakes: every external rail behind `ports/base.py`; tests run on
  fakes in ms; real adapters in `src/relay_superagent/adapters/`.
- Secrets: macOS keychain only, never .env. `security add-generic-password
  -U -s relay_superagent -a <name> -w '…'`; fetched via `secrets.py` at point of use.
- Decisions: `decisions.md` is the append-only ledger; write at the moment of
  decision, never at session end. D1–D21 inherited; Relay's start at D22.
- The run ledger is append-only; gate_* fields write exactly once; timeout
  escalates to a human and NEVER auto-sends.

## How to run
- `uv run pytest` — 108 tests, ~1s (Postgres contract tests use :5435).
- `uv run python demo/server.py` — Relay demo workspace on :8790
  (dispute content, 8-agent console; Dispute Defender wired, 7 roadmap).
- `make pg` — repo-local Postgres 17 on :5435 (no sudo, Postgres.app
  binaries). Ports are 5435/8790 because comarketer still owns 5434/8787
  on this machine.

## Current state (2026-08-04, evening)
Dispute Defender domain-complete on the inherited engine; demo workspace
fully re-seeded (fictional Indian SMB merchants: Loomcraft Textiles, Verve
Wellness, Bumblebee Mobility, Kavali Kitchens, Northgate Fresh; dispute
reason codes; evidence packs). Browser-verified: Home brief, Approvals
queue, Agents console (8 agents, live vs Coming-next badges) all render
dispute content with zero GTM leftovers. Connectors still on fakes.

## Lessons (append when the captain corrects course)
- Product face is **Relay** (bare name — captain's explicit call, over the
  earlier "Relay SuperAgent"). The repo/folder stays `relay-superagent`
  purely to avoid filesystem/GitHub collision with Cashfree Relay repos.
- Demo merchants must be INVENTED names. Never seed real Cashfree
  customers/pilots (Country Chicken Co, Cure.fit, Ohsou) into fake data.
- Agent lineup and copy come from the canonical intro deck, not memory —
  an earlier pass invented a 5-agent lineup with wrong metrics; the deck
  has 8 agents with locked personas and one-liners. Extract, don't recall.
- (Inherited) Design ports come from the live reference, not memory —
  screenshot first, restyle after.
- (Inherited) Paired control rows (input + button) must share exact height:
  bars use align-items:stretch and buttons drop vertical padding.
- (Inherited) Headless-Chrome --virtual-time-budget screenshots freeze CSS
  animations at arbitrary states — verify logic from those shots, never
  animation end-states; real browsers finish fill:both.
