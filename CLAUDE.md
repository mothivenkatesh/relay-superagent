# Relay — project memory

## Mission (north star, captain's words 2026-08-05)
**Every small business should run itself.** The busy work is done by agents.

## What this is
Relay: the super agent that makes a small business AI-first. Built for
millions of small merchants on teams of 5–10 — non-technical, no API keys
to bring, no integrations project, nobody whose job is "owning the tool."
They prompt a few lines and the agents handle the rest. Relay holds every
credential and connection itself; there is no setup project (the Lassie
no-BYOK constraint). Not a workflow builder and not a canvas — workflows
are value-added services on top, never the core.

**Positioning stack (settled 2026-08-05, captain-directed):**
- Mission: every small business should run itself.
- Category: Super Agent for Commerce / AI for small business.
- Mechanism: **labor arbitrage.** Ops headcount scales with orders, software
  doesn't; at a small merchant the labor line is ~10x the software line.
  Price against the labor budget, per unit of work — never per seat.
- Problem sentence: every unit of growth buys a proportional unit of ops
  labor, so growth never compounds margin.
- Surface (captain-directed 2026-08-05): **a virtual back office / agent
  swarm**, organised as the people a founder would otherwise put on
  payroll. 14 agents, 6 hires:
  **Your accounts manager** (3-Way Reconciliation, Settlement Insights,
  Cashflow Forecast, Payouts Desk) · **Your inventory manager** (Stock
  Watch) · **Your risk & compliance manager** (Refund Shield, GST &
  Compliance, KYC Desk) · **Your support manager** (Dispute Defender —
  the only wired one — and Returns Desk) · **Your telecaller** (Cart
  Rescue, Payment Rescue, COD Guard) · **Your MIS analyst** (Daily MIS).
  Desks are JOB TITLES, never functional taxonomies — a founder thinks
  "I need an accounts person", never "I need a finance desk". Use common
  Indian commerce job titles (telecaller, MIS executive, accounts
  executive), not American corporate ones (CFO, VP Ops).
- **Every agent holds a named role** (captain, 2026-08-05), so the roster
  reads as staff, not features: Reconciliation Officer, Settlement
  Analyst, Cashflow Planner, Payouts Clerk, Inventory Controller, Refund
  Risk Officer, Compliance Officer, KYC Verifier, Disputes Officer,
  Returns Coordinator, Cart Recovery Caller, Payment Recovery Caller,
  COD Confirmation Caller, MIS Analyst. Cards lead with the ROLE; the
  product name (Dispute Defender etc.) is the secondary chip. The pitch
  is "a team of agents running the business".
- **Autonomy wording, handle with care:** "running the business
  autonomously" contradicts the standing trust line. Settled phrasing:
  they "work unattended and never stop, but nothing that touches money
  or a customer goes out until you say yes." Never drop the gate to make
  the autonomy claim louder.
- **The rule every agent must pass (captain, 2026-08-05):** an agent
  earns a place ONLY if it replaces a real job and beats that job on
  cost. Not "assists", not "speeds up" — replaces. Every agent therefore
  carries a `replaces` field naming the job and an indicative monthly
  salary band, rendered on the card. **The salary bands are indicative
  Indian SMB ranges, NOT sourced data** — flag this before any external
  use. If an agent cannot name the job it displaces, it should not ship.
- Voice: **Khatabook-plain**. Big numbers, few words, zero jargon. No
  B2B role-speak — a five-person business has no risk lead and no ops
  lead, so no personas anywhere in product copy. Banned in user-visible
  copy: run/pipeline/gate/ledger/escalation/evidence pack/reason code/
  merchant/tenant/workspace/adoption. Say: job, needs your yes, history,
  needs a person, proof, reply to the bank, the business's name or "you".
  Internal identifiers, DB columns and test names keep the old words.
- Long horizon is a product claim, so it must be visible: the switched-on
  agent shows "Working since <date> · N disputes checked · never sleeps";
  everything else says "Not switched on", never "roadmap".
- Moat: **one order graph.** All six key off the same order, so each agent
  added makes the others better. This is the concrete version of "memory is
  the moat" — a shared join key, not abstract memory.
- Trust line, verbatim in product copy: "You approve before anything sends."

Agent copy source: Mothi's Relay Intro deck (~/Downloads/Relay Intro (4).html);
the `today:` labor line on each agent card is the arbitrage frame and is the
new product-visible expression of the thesis.

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
- **Agent #2**: which of the six not-switched-on agents gets wired next.

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
- `uv run python demo/server.py` — Relay demo on :8790. Nav is Home /
  Needs you / History / Your team / Settings (routes unchanged:
  / /approvals /journeys /agents /settings). Home is the AI-CFO screen:
  one big rupee number off the record, "N things need your yes", "What
  your team did today", prompt box.
- `make pg` — repo-local Postgres 17 on :5435 (no sudo, Postgres.app
  binaries). Ports are 5435/8790 because comarketer still owns 5434/8787
  on this machine.

## Current state (2026-08-05)
Virtual-back-office pass landed: `AgentType` is 7 values (THREE_WAY_RECON,
SETTLEMENT_INSIGHTS, DISPUTE_DEFENDER, REFUND_SHIELD, CART_RESCUE,
PAYMENT_RESCUE, COD_GUARD); `RELAY_AGENTS` carries `desk`, no `persona`,
and `today` renders as "Without Relay:". Plain-language sweep done across
Home, Needs you, History, Your team, Settings, agent detail, chat replies
and seeded conversations. 108 tests green; all routes 200.

## Previous state (2026-08-04, evening)
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
