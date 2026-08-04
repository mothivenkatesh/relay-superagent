# Decisions ledger

Append-only. Every entry: date, decision, why, who. Open items at top.

## Open — needs the captain

- **WorkOS keys** (blocks live signup/login): create a WorkOS app, enable
  email+password in User Management, then
  `security add-generic-password -U -s relay_superagent -a workos-api-key -w 'sk_…'`
  and `-a workos-client-id -w 'client_…'`. Demo mode works without them.
- **Gmail OAuth token** (blocks live polling): an OAuth access token with
  gmail.readonly for the connected inbox → keychain `-a gmail-token`.
  (Proper refresh-token flow is backlog; a Playground token proves the rail.)

## Decided

- **2026-08-02 — D18: the Concierge is a first-class planned surface.** A
  buyer-facing AI concierge delivering trusted, personalized experiences
  across the entire customer lifecycle (first visit → evaluation →
  purchase → onboarding → renewal), grounded exclusively in battlecards/
  graph facts (no invented claims), playbook-governed, autonomy-tiered
  (answers the routine, escalates pricing/legal/competitive-sensitive to
  humans), fully traced, and lift-measured like everything else. It is the
  Decagon-positioning (D17) made literal: their concierge for support,
  ours for the GTM lifecycle. Sequenced after Prospector (it reuses the
  same team + graph). Backlog: CM-27.

- **2026-08-02 — D17: positioning is “Decagon for GTM operations.”** The
  identity is conversational-operations-grade AI agents for GTM work:
  playbooks authored in natural language and compiled to constrained
  behavior (AOP pattern), an Autopilot loop where corrections become
  validated, human-approved behavior updates (CM-20), and tiered autonomy
  — handle the routine fully, escalate the exceptional. The Takeoff-style
  Jobs API (D15) remains the scale surface, but the category anchor is
  Decagon's, applied to GTM operations instead of customer support.

- **2026-08-02 — Value-alignment triad (captain approved all three; source:
  Takeoff's 'Value Alignment in Enterprise AI' essay).**
  D14 · PRICING DESTINATION IS MEASURED LIFT: the ladder is work-accepted
  (correction-rate, launch) → lift-priced (destination) — the holdout arm
  is the billing instrument, not just measurement: we charge on win-rate
  delta of treated vs holdout accounts, the only honest outcome currency
  in GTM. Named failure mode this fixes: acceptance-priced systems
  eventually optimize for pleasing the approver (sycophancy as revenue).
  D15 · END-STATE IS THE JOBS API: POST /jobs/defend-deal, /work-signal,
  /rescue-loss — outcome via webhook. The workspace (Approvals, Journeys)
  is the SUPERVISION surface; the API is the scale surface. The
  Orchestrator's role reframes: it defines new job types; customers post
  jobs, never configure agents. The gate is a probation period with an
  earnable graduation path (Conductor unlocks + Autopilot), not a
  copilot destiny.
  D16 · THE WEDGE NARROWS TO ONE JOB: 'competitive deal defense for B2B
  SaaS, priced on measured lift.' Not 'AI for GTM'. Prospector and the
  rest expand from a beachhead that pays for itself.

- **2026-08-01 — Chat: transparent routing + narration as seam 6.** Every
  chat reply carries a visible meta line (routed by Haiku|keywords ·
  narrated|deterministic · facts from the ledger) — silent fallback stays
  for resilience but is never invisible. `narrate` is the sixth LlmPort
  seam: Haiku paraphrases the deterministic result into prose; facts come
  only from ledger-derived text; eval fixtures enforce numbers-subset (no
  invented figures) incl. an adversarial instruction-injection case. The
  model still never chooses actions or writes cards.

- **2026-08-01 — Agent Orchestrator spec'd: spec/relay_superagent-orchestrator-v1-spec.md.**
  D13: agents are built by prompting — but the prompt is the INTERFACE,
  never the substrate. The Orchestrator compiles a job description into a
  closed AgentSpec over existing primitives (triggers, playbook rule nodes,
  access grants, caps, gates); no raw system prompts, no workflow canvas,
  no ungated birth. Employment metaphor is the product language: hire →
  interview → trial (shadow/manual-first) → performance review (evals,
  generated at hire and required for activation) → earned autonomy
  (Conductor unlock only). Unsupported asks surface honestly as capability
  requests. Deck line: "Don't build agents. Hire them." Build after
  Watcher gate + Context Graph.
- **2026-07-31 — Context Graph + Conductor spec'd: spec/relay_superagent-context-graph-conductor-spec.md.**
  D10: A2A = blackboard + gates, never agent chatter — agents read/write the
  shared ledger+graph; a deterministic, model-free Conductor in the existing
  sweep enforces learned sequencing rules (precondition/unlock/throttle/cap).
  D11: the graph is three Postgres tables with 12 closed node kinds,
  append-only via supersession; drafts cite node ids. No graph DB, no
  embeddings in v1. D12: rules and priors ship seeded from the ONEGTMLAB
  GTM Field Notes (16 failure rules, 18 signal priors, benchmarks;
  distilled tables in arivu `2026-07-31-gtm-field-notes-distilled.md`) and
  are replaced by the tenant's own ledger data — "your own data beats data
  you buy." Build after the Watcher gate; nothing in it blocks v1.
- **2026-07-31 — Prospector (outbound agent) spec'd: spec/relay_superagent-prospector-v1-spec.md.**
  D5: run-per-message grouped by signal — multi-threading without touching
  the state machine; gate/edit/correction-rate stay per message. D6: signals
  behind SignalPort — Apollo reference impl + CSV zero-dependency path (no
  enrichment waterfall). D7: sends from the rep's own Gmail, ≤20/day/rep and
  ≤3/week/domain HARD; no warmup infra, no sequences engine (follow-up = one
  `no_reply` signal, max_touches 2). D8: LinkedIn actions are human-only
  gated task cards, durable position (ToS/ban risk kills the trust story).
  D9: open-deal accounts are Watcher's — hard suppression keeps the loops
  disjoint; arm assignment shared across loops via the same account hash.
  Build order unchanged: Watcher gate closes first.
- **2026-07-31 — Tunnel: captain approved the direct cloudflared download.**
  Binary at ~/bin/cloudflared (v2026.7.3, official GitHub releases,
  darwin-arm64). `make tunnel` prepends ~/bin to PATH.
- **2026-07-31 — Gmail trigger added on the captain's explicit override** of
  spec Appendix A (which excluded the LangChain-style inbound-email trigger).
  What makes it safe: email enters the SAME pipeline — enrolled-rep check,
  claim-hash dedupe, suppress window, gate, never auto-send — so noise dies
  as suppressed rows. Reply-chain history is trimmed before detection so
  quoted text can't re-trigger. Polling (not Gmail push): push needs a GCP
  Pub/Sub topic — wrong cost pre-gate.
- **2026-07-31 — Account→deal resolution seam added to CrmPort**
  (`open_deal_for_account`): email triggers carry no deal id, so the pipeline
  resolves sender domain → open deal before the safety gate. HubSpot will
  implement it as company-domain → associated open deal.
- **2026-07-31 — Multi-tenancy is data, not architecture.** Rows were
  tenant-scoped since day one (RLS); added the other half: TenantContext
  (policy + rep directory + auth linkage), a registry, one Pipeline per
  tenant over the SHARED ledger. Fresh tenants get an empty-competitor
  default policy — onboarding is policy edits, not deploys. Registry is
  in-memory: WorkOS is the identity source of truth and login re-creates
  contexts after a restart; durable per-tenant policy in Pg is backlog.
- **2026-07-31 — WorkOS with email & password for signup/login** (captain's
  pick). Ground truth from their API reference: create user / create org /
  membership under Bearer sk_; password grant on /user_management/authenticate
  with client_secret = the sk_ key. Passwords pass form → WorkOS over TLS,
  never stored or logged here; sessions are our own HMAC-signed expiring
  cookies (tenant_id + email), keychain-backed secret with per-boot random
  fallback. WorkOS org id IS the tenant id.
- **2026-07-31 — Ask chat is demo-tenant-only for now** — the workspace and
  gate actions are tenant-scoped; the chat tools still read the demo world.
  Backlog before any second real tenant uses chat.

- **2026-07-31 — Fathom signature scheme is Svix-style, verified against their
  OpenAPI spec — NOT Slack's v0.** Headers webhook-id/-timestamp/-signature;
  HMAC-SHA256 over `{id}.{timestamp}.{body}`; base64 both ways; secret is
  base64 after the `whsec_` prefix; multiple space-delimited `v1,<sig>`
  entries, any one may match; 5-min replay window. Own verifier in
  `adapters/fathom.py`, never shared with Slack's.
- **2026-07-31 — Meeting URL (fathom.video/…) is the idempotency ref, not
  webhook-id.** webhook-id is per message (stable only across redelivery of
  one message); the URL names the recording, so re-registration of the
  webhook also dedupes.
- **2026-07-31 — Stall detection is two-sweep same-state comparison in the
  supervisor, not an updated_at column.** A run seen in the same pre-gate
  working state on two consecutive sweeps → FAILED + PMM escalation (never
  silent, never auto-sent). In-memory on purpose: after a crash+restart the
  first sweep re-marks, the second escalates — the exact case it exists for.
  No DDL change; closes the CHECKED-parked gap.
- **2026-07-31 — Rep identity maps email→Slack-id via tenant config
  (REP_DIRECTORY); unknown emails pass through** so the enrolled-rep policy
  check decides, not a KeyError.

- **2026-07-31 — Direct adapters, not Composio, for the gate rails.** Slack,
  Fathom and HubSpot are ~100 lines each behind existing ports; Composio would
  put a hosted middleman in the gate's critical path, move credentials out of
  the keychain into their vault, and make effect-table idempotency unprovable.
  Revisit trigger: when Ask needs breadth across a customer's arbitrary stack
  (long-tail CRMs, managed OAuth refresh) — then it slots in behind the same
  ports.
- **2026-07-31 — Slack edit is a link-out, not a modal.** The gate card's third
  button opens the workspace, where the edit box already exists. Modals need
  trigger_id round-trips and view state for zero extra safety.

- **2026-07-31 — D1: Fathom** for gate ingest. **D2: HubSpot** free private-app for CRM.
  **D3: split models by seam** — Haiku 4.5 on confirm/extract (runs on every trigger),
  Sonnet 5 on draft/judge/diff (quality-sensitive, runs less). **D4: gate-first.**
- **2026-07-31 — Structured outputs everywhere** (`output_config.format`, closed
  schemas) so there is no JSON-parsing heuristics layer; refusals, truncation and
  transport failures all map to `LlmUnavailable` → PMM escalation.

- **2026-07-31 — MVP gate sentence:** *A real Fathom call in which someone names a
  competitor produces, within ten minutes, a Slack DM to me with the counter; my
  Approve writes a note onto the real HubSpot deal; and the run row with my
  decision sits in Postgres.* All work sequences backward from this.
- **2026-07-31 — Prototype-phase rules in force:** direct commits, no PR gate, no
  adversarial review pipeline until the gate sentence passes. (l8 §4)
- **2026-07-31 — Fathom over Gong for v1 ingest.** Gong = enterprise seats, no dev
  self-serve. Spec §3 updated in spirit; Gong stays a listed adapter for real
  customers later. Verified via Fathom developer docs (developers.fathom.ai).
- **2026-07-31 — HubSpot over Salesforce first.** Free tier, private-app token,
  notes API with deal associations. Salesforce lands later behind the same CrmPort.
- **2026-07-31 — LangGraph adoption deferred until after the gate.** Deliberate
  deviation from spec §2, flagged for veto: the pipeline is plain code with
  Postgres state and passes 46 tests; at one tenant and four LLM calls per run,
  in-process calls are simpler to debug. Adopt LangGraph+deepagents when drafting
  goes multi-source/parallel or when runs must survive process death mid-draft.
- **2026-07-31 — AG-UI streaming deferred.** Page-reload workspace is enough to
  review design and run the gate. Streaming when the workspace is the product.
- **2026-07-31 — Postgres ledger + repo-local cluster** (`make pg`, :5434), RLS
  enforced via non-owner app role, gate-once trigger. Contract tests cover both
  ledgers; CI needs no database.
- **2026-07-31 — Secrets via macOS keychain fetch, never .env** (l8 §8):
  `security add-generic-password -s relay_superagent -a <name> -w` once, adapters read
  through a small fetch helper at point of use.

## Kill list — do not build before the gate (and probably not after)

Multi-env tiers · auth/SSO · billing · multi-tenant onboarding UI · email ingest
(Gmail OAuth review is weeks of pain for zero gate value) · sending on the
customer's behalf · Salesforce · Gong · mobile workspace · config UI beyond
competitors + banned terms.

## D19 — Capability map locked from the long-running-agents research (2026-08-02)
Studied Sierra Horizon, Takeoff's runtime, Google ADK, Cloudflare Agents, Anthropic's
harness repo, and the O'Reilly synthesis, word for word including every diagram. Where
all of them converged, we adopt: a durable task scheduler (every wait becomes a row with
a due_at — CM-28, prerequisite for follow-ups/campaigns), kill switch + steering (CM-31),
budgets/circuit breakers (CM-32), approval routing (CM-33). Ledger-native compounders:
win/loss intelligence → gated battlecard updates (CM-29), campaigns with goals as the
pricing surface (CM-30), more signal intake (CM-34), journey-replay tests (CM-35),
weekly lift brief (CM-36), Gong/Zoom/Salesforce connectors (CM-37). Explicitly refused
even though feasible: auto-send tiers, LinkedIn automation, voice/SMS surfaces, generic
chat-with-CRM. Sierra's own architecture diagram draws guardrails as an input and human
approval as an execution surface — our gate is the frontier's picture, not a deviation.
All ten written into PRD v1.4 §6 + Appendix B in plain words. Nothing starts before the
Watcher gate passes.

## D20 — Identity: Relay is the Track Record for GTM work (2026-08-02)
Ratified after the Dixon-canon read (36 pieces), the value-chain research, and the
53-competitor scan (Sortment, OpenGTM, Kami, Next 50). Everything upstream commoditizes
(agents bundled free, the approval gate now exists as an open-source student project,
drafting free, signals crowded); the layer that can't commoditize is proof of what
caused revenue. Identity: we sell the scoreboard, not the agent. Relay = the
customer's GTM Track Record — every decision, the action it produced, and the outcome
it caused — with the Scoreboard (randomized sit-out deals → measured lift) as the
causal instrument. Watcher is workload #1 writing into it, not the company.
Can't-be-evil commitments, day one: the customer OWNS their track record; export is
a right, not a feature; the Scoreboard ships its number even when the number is bad.
Vocabulary rule: "Track Record" (product asset), "Scoreboard" (lift instrument),
"receipts" (casual register). "Ledger" is an engine word only — code and tables —
per the existing UI-speaks-marketer convention. Every future vendor billing on
outcomes self-reports attribution; the neutral adjudicator is the durable position.

## D21 — Distribution and statistics: the agency channel + pooled lift (2026-08-02)
Three critique wounds close with one actor. GTM agencies and fractional PMMs
(a) feel the attribution pain most acutely — they must prove retainers cause revenue
(ONEGTMLAB hand-wrote exactly this record for 7 months; that is demand, proven),
(b) pool deals across many clients, which is the only honest fix for the lift
power-calculation problem at mid-market volume, and (c) are the outside-in door —
a proof-of-value scoreboard for one client engagement, adoptable in an afternoon,
no enterprise sale. Attribution routes credit to the under-compensated value
creators (the PMM whose battlecard closed, the rep whose edit won, the agency whose
play lifted) — participants feed a network that makes them legible. End-state: the
Jobs API inverts from "we do work" to accepting any work source (human, agency,
third-party agent) for attribution and adjudication. The gate reframes from eternal
doctrine to earned-autonomy scaffolding (per the Orchestrator ladder): the Track
Record doesn't care who did the work, only that it's provable. Sequence unchanged
and non-negotiable: Watcher gate → first customer → first agency → open the write
surface. A settlement layer with zero settled outcomes is a whitepaper.

## D22 — Dispute Defender is the wired domain (2026-08-04)
The fork's first real domain: a payment dispute/chargeback filed against a
merchant's order, with a deadline. Engine (state machine, ledger, gates,
ports/fakes) untouched; domain data replaced end-to-end (DisputeReason,
reason_code/merchant_id/order_id/dispute_id/deadline_at, evidence types).
Notable calls: (1) disputes are NEVER held out — runs are built arm=TREATED,
a missed chargeback deadline is real financial harm, not an A/B cell;
(2) LlmPort/CrmPort method names kept verbatim (confirm_mention/draft_counter/
deal_context/…) so the reference adapters keep importing — what flows through
them changed, the seams did not; (3) the account→open-deal resolution step is
gone: order_id arrives on the webhook, a missing order suppresses as no_order;
(4) TriggerEvent.deadline_at added with a None default at the end of the
dataclass so every existing kwargs construction site stays valid;
(5) escalation channel renamed #relay-dispute-review (Deps.escalation_channel);
(6) demo world: 6 invented Indian SMB merchants, all 8 real Relay agents on
the Agents console with verbatim intro-deck one-liners — Dispute Defender
live, the other seven visibly badged roadmap. 108 tests green (Postgres
contract tests included, no skips).
