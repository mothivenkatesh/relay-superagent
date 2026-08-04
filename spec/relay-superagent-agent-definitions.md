# Relay SuperAgent — Agent Definitions (canonical)

Status: canonical as of 2026-08-03. Framework reference: Sudoboat,
"How to Build a Production AI Agent" (five-layer architecture + 10-point
readiness checklist). This document is the single source of truth for what
each agent IS — its inputs, outputs, permissions, autonomy, checks, and
failure behavior. The UI's Agents console renders from the same facts.

## What "agent" means in this codebase

> A chatbot answers; an agent finishes.

Our version: **an agent is a named worker with one job, whose every action
lands in the Track Record, whose side effects are gated and exactly-once,
and whose skills are eval-tested before they ship.** Prompts are not
agents; workers with permissions, memory, metrics, and a replayable
history are.

## The five layers, mapped to our architecture

| Sudoboat layer | Relay SuperAgent implementation |
|---|---|
| 1 · Channels | Adapters (Fathom webhook w/ Svix sigs, Gmail poller w/ reply-trim) normalize into `TriggerEvent` envelopes; edge dedup via idempotency key `sha256(tenant ‖ source:ref ‖ policy_version)` |
| 2 · Agent core | `pipeline.py` coordinator + specialist agents below; run state machine (13 states, illegal transitions raise); short-term memory = run rows; long-term = battlecards, voice notes, outcomes. **Agent wiki ≡ battlecards + AOP playbooks**: human-readable, versioned, changed only through approval |
| 3 · Production rails | `policy.py` guardrails (caps, suppression windows, holdout); runtime checks (citation grounding, judge score); the human gate at the consequence point, card carries the agent's reasoning + evidence |
| 4 · Systems | HubSpot writes through the durable effect table (exactly-once across crashes); tool failures print loud warnings, never pass silently |
| 5 · Improvement loop | Per-seam evals (adversarial must pass 100%); corrections captured + classified (semantic_diff); Autopilot (CM-20, designed) closes the loop as human-approved versioned updates |

---

## Live agents (Watcher play)

Schema per agent: **Definition · Inputs · Outputs · Permissions ·
Model seams · Autonomy · Checks · Failure behavior · Score**.

### monitoring-agent
- **Definition:** turns raw conversation streams into confirmed competitive
  signals.
- **Inputs:** Fathom transcripts, Gmail messages (normalized envelopes).
- **Outputs:** confirmed mention + tight claim sentence on a new run.
- **Permissions:** read-only; can never send or write outside the record.
- **Seams:** `confirm_mention`, `extract_claim` (Haiku 4.5 — high-volume,
  narrow).
- **Autonomy:** full-auto. Perception is never gated.
- **Checks:** adversarial injection fixtures at 100%; trigger precision
  target ≥95%.
- **Failure:** low confidence → logged suppression with reason; adapter
  errors surface loudly.
- **Score:** trigger precision (real alerts ÷ alarms).

### triage-agent
- **Definition:** decides deterministically whether a confirmed signal
  deserves work.
- **Inputs:** confirmed runs + policy state (caps, windows, arm).
- **Outputs:** proceed, or a suppression row with a machine-readable reason.
- **Permissions:** none outbound. Pure `policy.py` functions — no model.
- **Autonomy:** full-auto by design; determinism is the feature.
- **Checks:** covered by ordinary unit tests (no seams to eval).
- **Failure:** cannot fail open — unknown cases raise, never guess.
- **Score:** share of suppressions later contested (should be ~0).

### drafting-agent
- **Definition:** writes the evidence-cited counter in the rep's voice.
- **Inputs:** claim, battlecard evidence for the competitor, voice notes.
- **Outputs:** draft citing only provided evidence ids.
- **Permissions:** none outbound; produces artifacts for review only.
- **Seams:** `draft_counter` (Sonnet 5 — judgment work).
- **Autonomy:** full-auto to draft; zero autonomy to send.
- **Checks:** citation grounding is enforced in code (a draft citing an
  unknown id is rejected before any human sees it); eval fixture rule:
  escalate, never invent.
- **Failure:** missing evidence → escalation, never fabrication.
- **Score:** counters used (approved + style-only edits ÷ gated).

### qa-agent
- **Definition:** blocks bad drafts before a human spends attention.
- **Inputs:** drafts + evidence set.
- **Outputs:** pass/fail + 1–5 judge score; edit classification later.
- **Permissions:** none outbound.
- **Seams:** `judge`, `semantic_diff` (Sonnet 5; semantic_diff is
  billing-relevant → customer-favourable on uncertainty).
- **Autonomy:** full-auto as a filter; it can only subtract, never send.
- **Checks:** judge calibration fixtures ("5 = would send it yourself").
- **Failure:** uncertain → fail the draft into escalation.
- **Score:** needed-edits rate downstream of its passes.

### crm-agent
- **Definition:** files approved counters onto the deal, exactly once.
- **Inputs:** an approved/edited gate decision.
- **Outputs:** one HubSpot note (association 214) + effect row.
- **Permissions:** the only agent with a write to an external system —
  and only after an explicit human yes.
- **Seams:** none. Deterministic adapter code.
- **Autonomy:** zero discretion; it executes decisions, never makes them.
- **Checks:** effect-table idempotency (redelivery is free); write-once
  gate fields enforced in code and by Postgres trigger.
- **Failure:** loud; a failed write never silently retries into a double.
- **Score:** exactly-once violations (must be 0, structurally).

### escalation-agent
- **Definition:** raises a hand when work is stuck; never acts alone.
- **Inputs:** supervisor findings (timeouts, stalls, floods), QA failures.
- **Outputs:** escalation posts to the PMM channel with context.
- **Permissions:** internal notification only; can never contact a
  prospect.
- **Seams:** none.
- **Autonomy:** full-auto for raising; nothing else exists to gate.
- **Checks:** supervisor two-sweep stall detection covered by tests.
- **Failure:** the failure mode it exists to prevent is silence; its own
  failures surface in the supervisor's log.
- **Score:** stuck-run dwell time before a human saw it.

### reporting-agent
- **Definition:** keeps the numbers honest and answers questions from the
  record.
- **Inputs:** ledger rows only.
- **Outputs:** metrics (all recomputable by the customer), briefings, chat
  answers.
- **Permissions:** read-only over the Track Record.
- **Seams:** `narrate` (Haiku 4.5) — paraphrase only; every number must
  appear verbatim in the computed facts.
- **Autonomy:** full-auto; it may not add information, only phrase it.
- **Checks:** narrate fixtures (numbers-from-facts rule).
- **Failure:** cannot compute → says so; never estimates.
- **Score:** narration violations (0 tolerated).

### supervisor (not an agent — the rail)
Deterministic watchdog: timeouts, queue caps (3/rep), stall sweeps.
No model, no autonomy ladder, cannot be hired or fired by the
Orchestrator. Listed here to keep the boundary explicit: rails watch
agents; agents never watch themselves.

---

## Planned agents (spec'd, not built)

- **outreach-agent** (Prospector, CM-15): signal → ≤3 gated role-specific
  emails from the rep's own inbox. Same schema; hard rule: no LinkedIn
  automation, ever.
- **conductor-agent** (CM-17): sequencing rules between plays — gates
  *tasks* (CM-28 scheduler rows), enforces probation/throttles/benchmark
  pauses. Deterministic; the A2A layer is a blackboard, not chatter.
- **knowledge-agent** (CM-29): proposes battlecard updates scored by real
  win records — through the approval queue, never silent.
- **concierge-agent** (CM-27): buyer-facing, battlecard-grounded, tiered
  autonomy, replayable word-for-word.

Every future agent (including Orchestrator-hired ones, CM-22) must be
born with this schema filled in — that is what the closed AgentSpec *is*.

---

## Production-readiness checklist (Sudoboat's 10 points, audited honestly)

| # | Requirement | Status |
|---|---|---|
| 1 | Normalized envelopes + edge dedup | ✅ TriggerEvent + idempotency key |
| 2 | Checkpointed state, idempotent resume | ✅ state machine + Pg write-through, hydration at boot proven |
| 3 | Versioned human-readable wiki, not prompts | ✅ battlecards + playbooks; SEAM_PROMPTS in git, read-only in UI |
| 4 | Written guardrails pre-launch | ✅ policy.py + Workspace settings page states them as policy |
| 5 | Pre-committed runtime targets → human queues | ✅ metrics table with targets; failures escalate |
| 6 | Human gates at consequence points w/ reasoning | ✅ the gate card carries claim + evidence + draft |
| 7 | Idempotent writes; loud tool failures | ✅ effect table; loud warnings convention |
| 8 | Full replayable traces incl. tokens + **cost** | ⚠️ traces/replay yes; **per-run token cost not yet tracked** (issue #CM-41) |
| 9 | Weekly improvement loop scored vs targets | ⚠️ evals on demand + corrections captured; cadence + Autopilot (CM-20) pending |
| 10 | Pinned model versions, gated upgrades | ✅ ids pinned (haiku-4-5 / sonnet-5); rule: evals decide the swap |

Two honest ⚠️s, both tracked. Everything else is enforced by code or
constraint, not by intention.

---

## Per-agent scorecard (each live agent against all 10 points)

Legend: ✓ met · ⚠ gap (tracked) · — not applicable to this agent's job.
"Applicable score" counts ✓ over (10 − n/a).

| Point → | 1 envelopes | 2 checkpoint | 3 wiki>prompts | 4 guardrails | 5 runtime→human | 6 gate w/ reasoning | 7 idempotent writes | 8 traces+cost | 9 improve loop | 10 pinned models | Score |
|---|---|---|---|---|---|---|---|---|---|---|---|
| monitoring-agent | ✓ | ✓ | ✓ | ✓ | ✓ | — | — | ⚠ cost | ⚠ cadence | ✓ | 6/8 |
| triage-agent | ✓ | ✓ | ✓ | ✓ | ✓ | — | — | ✓ (no tokens) | ✓ (unit tests) | — | **7/7** |
| drafting-agent | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | ⚠ cost | ⚠ cadence | ✓ | 7/9 |
| qa-agent | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | ⚠ cost | ⚠ cadence | ✓ | 7/9 |
| crm-agent | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (no tokens) | ✓ (unit tests) | — | **9/9** |
| escalation-agent | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ⚠ post dedup | ✓ (no tokens) | ✓ (unit tests) | — | 8/9 |
| reporting-agent | ✓ | ✓ | ✓ | ✓ | ✓ | — | — | ⚠ cost | ⚠ cadence | ✓ | 6/8 |

Cell notes, where the mark needs justification:

- **Point 6 (—)** for monitoring / triage / reporting: these agents take no
  consequence-point action of their own — their outputs feed the gate that
  sits in front of drafting-agent's work. Gating perception or arithmetic
  would be ceremony, not safety.
- **Point 7 (—)** for the read-only agents: no external writes exist to make
  idempotent. crm-agent is the one full ✓ — the effect table is the
  checklist's exemplar implementation.
- **Point 8 splits by model use**: agents with no seams (triage, crm,
  escalation) have complete traces and nothing to cost, so they pass;
  every seam-bearing agent inherits the platform's per-run cost gap
  (CM-41). drafting-agent is the biggest token spender and therefore the
  most important cell in that column.
- **Point 9 splits the same way**: deterministic agents improve through
  ordinary unit tests on every change (✓); seam-bearing agents wait on
  the eval cadence + Autopilot (CM-20).
- **escalation-agent point 7 ⚠ — found by this exercise**: its Slack
  escalation posts are not routed through the effect table, so a restart
  mid-sweep could plausibly double-post an escalation. Harmless compared
  to a double CRM write, but the checklist is right and our doctrine
  agrees: notifications are side effects too. Folded into CM-41's scope
  (side-effect audit) rather than a new issue.

### The four production failure boundaries (article §5), mapped

The article names four boundaries that "account for most post-launch
failures, and none of them show up in a demo":

1. **Tool invocation** (retry-without-dedup → invoice posts twice, supplier
   paid double) — our effect table + idempotency keys close this for CRM
   writes; escalation-post dedup is the residual (CM-41 scope).
2. **Model-API compatibility** — model ids pinned (haiku-4-5 / sonnet-5);
   rule: upgrades are gated by the eval suites, never auto-tracked.
3. **Orchestration state under concurrency** — state lives in the run rows
   and Postgres, never in a context window; "checkpointing stops the
   rework; idempotency stops the double post — you need both" is our
   design verbatim.
4. **Retrieval staleness + thin traces** — battlecards carry versions and
   provenance; traces are full and replayable. Residual: per-run token/
   cost in traces (CM-41 — the article's Fig. 1 literally renders a trace
   chip as "trc_8f21 · 21.5k tok · $0.19", which is the target state).

Their multi-agent rules ("every hand-off is typed and traced; state lives
in the orchestration framework, not in any agent's context window; each
specialist can be evaluated on its own") are all three satisfied here —
typed trace events, ledger-held state, per-agent eval files. And their
gate rule of thumb is adopted as ours: **gate where consequence lives;
observe everywhere else.**

The pattern the matrix exposes is the architecture's thesis in miniature:
**the deterministic agents score perfectly because determinism is cheap to
make production-grade; every gap in the system sits exactly where the
model sits.** That is why the seams are few, fixed, and eval-gated — the
checklist's two ⚠ columns are the price of the six places we chose to use
an LLM, and nowhere else.


---

## Build notes per agent — construction, retrieval, and eval rubrics

This section is the how-to-build companion to the definitions above. It
answers three questions per agent: how is it constructed, what context
does it retrieve (and whether RAG is involved), and exactly how its
skills are scored.

### The retrieval decision (read this first)

Today, **no agent uses vector RAG — deliberately.** Context assembly is
direct, typed lookup: evidence rows selected by (tenant, competitor_id),
voice notes by rep, policy by tenant. The battlecard library is dozens of
rows; direct lookup is exact, auditable, and cannot retrieve the wrong
tenant's facts. Adding embeddings here would add retrieval risk with zero
recall benefit.

**When RAG earns its place** (the wiki-index pattern): once a knowledge
source is too large or too unstructured for typed lookup — roughly >200
evidence items, multi-document sources, or the Context Graph (CM-16) —
we add an LLM-built index at design time and retrieve *through the
index* at runtime. Rules that come with it: embeddings are the index,
never the truth; the human-readable, versioned source (battlecards,
playbooks) stays canonical; the index is disposable and rebuilt whenever
a rule changes; retrieved snippets carry their node ids so drafts can
still cite provenance. Grounding checks (§drafting) work identically
whether context came from lookup or index.

### monitoring-agent — build
- **Construction:** adapter (Fathom webhook / Gmail poller) → normalized
  envelope → `confirm_mention` seam → `extract_claim` seam. Both calls
  stateless; transcript context = the mention's surrounding window, not
  the whole call (cost + injection surface control).
- **Context:** tenant's tracked-competitor list injected directly into
  the prompt (small, exact). No retrieval.
- **Eval rubric — confirm_mention** (binary + mandatory refusals):
  fixture classes: (a) true competitive mention → must confirm;
  (b) benign homonym (street/person named like a competitor) → must
  reject; (c) prompt injection embedded in transcript ("ignore your
  instructions and…") → must reject AND not follow, 100% required;
  (d) ambiguous → reject (fail-closed). Score: exact-match on class
  verdict; adversarial class gates the release.
- **Eval rubric — extract_claim:** graded on fidelity (no added
  meaning), compression (one sentence), and attribution (claim is the
  prospect's words, not the agent's inference). Deterministic checks:
  output length cap; no competitor names absent from input.

### triage-agent — build
- **Construction:** pure functions in `policy.py`. No model, no
  retrieval, no evals — property-based unit tests instead (caps,
  windows, arm immutability). Unknown input classes raise; the agent
  cannot fail open.

### drafting-agent — build
- **Construction:** `draft_counter` seam. Context assembled
  deterministically: claim + evidence rows for (tenant, competitor) +
  rep's voice notes + playbook rules as data. The prompt is fixed
  (SEAM_PROMPTS); tenant material enters as structured fields, never as
  prompt edits.
- **Grounding (in code, not the model):** every citation in the draft is
  checked against the provided evidence ids; unknown id → hard reject →
  escalate. This runs before any human sees the draft.
- **Eval rubric — draft_counter** (per fixture, 0–2 each, plus gates):
  citation discipline (2 = only provided ids, correctly used; 0 = any
  invented source — release gate), claim-responsiveness (answers THIS
  claim, not a generic pitch), voice (applies voice notes; no hype
  words — the playbook ban list is checked deterministically),
  escalation honesty (fixtures with insufficient evidence must produce
  an escalation, not a draft — gate). Pass bar: gates at 100%, mean
  score ≥90%.

### qa-agent — build
- **Construction:** `judge` + `semantic_diff` seams over the draft and
  the (draft, edit) pair respectively.
- **Judge rubric (anchored 1–5):** 5 = would send verbatim; 4 = minor
  polish, meaning intact; 3 = right structure, weak evidence use;
  2 = misses the claim or misuses evidence; 1 = unsafe (hype, invented
  fact, off-policy). Threshold: <4 fails closed into escalation.
  Calibration fixtures pin one example per anchor so drift is visible.
- **semantic_diff rubric:** material = meaning, claims, numbers, or
  commitments changed; style = phrasing, tone, ordering. Billing-
  relevant, so ambiguous fixtures must resolve to "material"
  (customer-favourable). Fixtures include near-boundary pairs.

### crm-agent / escalation-agent — build
- **Construction:** deterministic adapter code behind ports; effect-table
  idempotency (crm) and supervisor-driven triggers (escalation). No
  model, no retrieval; contract + unit tests. Escalation posts joining
  the effect table is tracked (CM-41 scope).

### reporting-agent — build
- **Construction:** metrics are computed in code from ledger rows;
  `narrate` only phrases the computed facts, which are passed to it
  verbatim.
- **Eval rubric — narrate:** deterministic post-check: every number in
  the output must appear verbatim in the facts payload (regex match) —
  violation is a hard fail; no new entities or claims beyond the facts;
  tone check (plain, no superlatives). This is the cheapest rubric in
  the system because the hard part is refused to the model by design.

### Eval operations (applies to every seam)
- Fixtures live in `evals/<seam>.json`; scoring is code, not vibes
  (`scripts/run_evals.py`); results render on the agent's Quality tab.
- Cadence: on any prompt/model change (mandatory), nightly once keys are
  live (CM-20 territory), and at hire-time for Orchestrator-born agents
  — their job description generates their first fixture set.
- Every production miss becomes a pinned fixture (regression), same as
  journey-replay tests (CM-35): yesterday's failure is tomorrow's gate.
