# Relay SuperAgent Watcher v1 — Engineering Spec

For implementation. No prior context needed.

---

## 1. What you're building

A competitor gets named in a live sales deal. The system drafts a rebuttal ("counter") with cited evidence, posts it to the rep in Slack for approval, writes the approved version to the CRM, and weeks later records whether that deal was won.

**Flow:** event → retrieve → draft → check → human approves → write back → outcome lands later.

Every run writes one row to an append-only ledger. **That ledger is the product.** Data loss there is a P0.

> **Read Appendix A first.** It links a working reference implementation, and names four things in it you must not copy.

### Do not build
- Autonomous sending. Nothing leaves without a human approving it.
- RL, fine-tuning, or any model training.
- Agent-authored UI. Components are fixed at build time.
- Shared models or vector indexes across tenants.
- Third-party intent data or enrichment vendors.
- Any loop other than Watcher.

---

## 2. Stack — decided, don't relitigate

| Concern | Choice |
|---|---|
| Orchestration | LangGraph + `deepagents` (planning tool, subagents, virtual filesystem) |
| Durability | LangGraph Postgres checkpointer. Interrupt at the approval node. |
| Store | Postgres 15+, `pgvector` for evidence search |
| Queue | Postgres job table, or Temporal if you already run it. No Kafka. |
| Tracing | LangSmith. `trace_id` on every run. |
| Blob | S3-compatible. Transcripts never enter the context window. |
| UI transport | **AG-UI** over SSE, CopilotKit React bindings. Not AP2, see §7. |

---

## 3. Integrations — all connected at onboarding

| System | Access | Purpose |
|---|---|---|
| Salesforce **or** HubSpot | read + write | opportunity state; write counter as note/task |
| Gong **or** Fathom **or** Chorus | read | transcripts, webhook on transcript-ready |
| Gmail **or** Outlook | read | email threads on an opportunity |
| Slack | read + write | approval surface, bot DMs the rep |
| Google / LinkedIn Ads | read | connect only, not consumed in v1 |

OAuth per tenant, tokens KMS-encrypted. Store granted scopes and refuse to start a run if one is missing, rather than failing halfway.

---

## 4. Data model

Postgres. RLS on every table. No query may span tenants.

```sql
-- Append-only. One row per run. Only columns marked mutable are ever updated.
CREATE TABLE run (
  run_id            uuid PRIMARY KEY,
  tenant_id         uuid NOT NULL,
  loop              text NOT NULL DEFAULT 'watcher',
  idempotency_key   text NOT NULL,
  status            text NOT NULL,
  suppressed_reason text,                 -- set iff status='suppressed'

  trigger_source    text NOT NULL,        -- 'gong' | 'gmail' | ...
  trigger_ref       text NOT NULL,        -- external id, never the payload
  occurred_at       timestamptz NOT NULL,

  opportunity_id    text,
  account_id        text,
  rep_user_id       text,
  competitor_id     text,
  claim_hash        text,                 -- sha256(normalised claim)

  retrieved_refs    jsonb,                -- handles only
  decision          jsonb,                -- {counter_text, cited_evidence_ids[], confidence}
  evidence          jsonb,

  policy_version    text NOT NULL,
  prompt_hash       text NOT NULL,
  model             text NOT NULL,
  arm               text NOT NULL,        -- 'treated' | 'holdout'

  gate_actor        text,                 -- mutable once
  gate_action       text,                 -- mutable once: approve|edit|reject|timeout
  gate_diff         jsonb,                -- mutable once
  gate_is_material  boolean,              -- mutable once
  gate_latency_ms   integer,              -- mutable once
  gated_at          timestamptz,

  acted_at          timestamptz,
  act_ref           text,
  trace_id          text,
  cost_tokens       integer,
  created_at        timestamptz NOT NULL DEFAULT now(),

  UNIQUE (tenant_id, idempotency_key)
);
CREATE INDEX ON run (tenant_id, status, created_at);
CREATE INDEX ON run (tenant_id, opportunity_id);

-- Outcomes arrive days to months later. Append here, never update run.
CREATE TABLE outcome (
  outcome_id    uuid PRIMARY KEY,
  tenant_id     uuid NOT NULL,
  run_id        uuid NOT NULL REFERENCES run(run_id),
  outcome_key   text NOT NULL,            -- 'opportunity_closed'
  outcome_value jsonb NOT NULL,           -- {won, amount, competitor_id}
  observed_at   timestamptz NOT NULL,
  source        text NOT NULL,
  UNIQUE (run_id, outcome_key)
);

-- Every external side effect. This is what makes retries safe.
CREATE TABLE effect (
  effect_key   text PRIMARY KEY,          -- sha256(run_id | effect_type | target_ref)
  tenant_id    uuid NOT NULL,
  run_id       uuid NOT NULL,
  effect_type  text NOT NULL,             -- 'slack_post' | 'crm_note' | 'crm_task'
  status       text NOT NULL,             -- 'pending' | 'done' | 'failed'
  external_ref text,
  attempts     int NOT NULL DEFAULT 0,
  created_at   timestamptz NOT NULL DEFAULT now()
);

-- Derived. Rebuildable by replaying run + outcome. Never a source of truth.
CREATE TABLE memory (
  memory_id     uuid PRIMARY KEY,
  tenant_id     uuid NOT NULL,
  subject_type  text NOT NULL,            -- 'rep' | 'tenant'
  subject_id    text NOT NULL,
  concern       text NOT NULL,            -- 'counter_style' | 'objection_handling'
  body          jsonb NOT NULL,           -- {changed, implies, example}
  source_run    uuid,
  superseded_by uuid,
  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON memory (tenant_id, subject_type, subject_id, concern) WHERE superseded_by IS NULL;

-- Evidence, per tenant. Ranked by observed win rate.
CREATE TABLE evidence_item (
  evidence_id   uuid PRIMARY KEY,
  tenant_id     uuid NOT NULL,
  competitor_id text NOT NULL,
  claim_class   text NOT NULL,
  text          text NOT NULL,
  source_url    text NOT NULL,
  embedding     vector(1536),
  created_at    timestamptz NOT NULL DEFAULT now()
);

-- Versioned config. Changing this must never need a deploy.
CREATE TABLE policy (
  policy_version text PRIMARY KEY,
  tenant_id      uuid NOT NULL,
  body           jsonb NOT NULL,
  active         boolean NOT NULL DEFAULT false,
  created_at     timestamptz NOT NULL DEFAULT now()
);
```

`policy.body`:
```json
{
  "competitors": [{"id":"acme","names":["Acme","Acme Corp"],"domains":["acme.com"]}],
  "banned_terms": ["best","leading","number one"],
  "narrative_map_ref": "s3://.../positioning.md",
  "autonomy_stage": 1,
  "caps": {"per_rep_per_day": 5, "tenant_tokens_per_day": 2000000},
  "gate_timeout_hours": 24,
  "judge_threshold": 4,
  "holdout_pct": 20,
  "suppress_window_days": 7
}
```

---

## 5. Run states

```
received → suppressed                    (safety check failed, zero LLM spend)
received → running → drafted → checked → awaiting_gate
awaiting_gate → approved | edited | rejected | timed_out
approved | edited → acting → acted → resolved   (resolved once an outcome exists)
any → failed                             (retries exhausted)
```

- Suppressed runs are still written. They are the denominator for trigger precision.
- `timed_out` posts to the PMM channel. **It never sends.**

---

## 6. Pipeline

**6.1 Ingest.** Webhook or poll per source. Payload to blob, keep the ref. Dedupe on `(tenant_id, source, source_ref)`.

**6.2 Detect competitor, cheap first.**
1. Deterministic match against `policy.competitors[].names` and `domains`. No LLM.
2. Only on a hit, a small model confirms it is a real competitive mention and extracts the claim.

Never run step 2 without step 1. This is the main cost control.

**6.3 Safety checks, before any retrieval or generation.** All cheap lookups. Any failure writes `status='suppressed'` with a reason and stops.
1. Opportunity exists and is not Closed Won or Closed Lost.
2. No approved or edited run for the same `(opportunity_id, competitor_id)` inside `suppress_window_days`.
3. `claim_hash` not already acted on for this `account_id`.
4. Rep is enrolled and under `caps.per_rep_per_day`.
5. Tenant under `caps.tenant_tokens_per_day`.

**6.4 Assign arm.** Hash on `account_id`, so assignment is stable per account, not per run. Holdout runs stop here as `suppressed`, reason `holdout`.

**6.5 Retrieve, four subagents in parallel.**

| Subagent | Tools | Output |
|---|---|---|
| `claim_extractor` | `transcript_fetch` | `{competitor_id, claim_text, speaker_role, ts_ms, confidence}` |
| `deal_context` | `crm_read` | `{stage, amount_band, close_date, competitor_history[], prior_losses[]}` |
| `evidence_retriever` | `evidence_search`, `docs_search` | `{candidates:[{evidence_id, text, source_url, observed_win_rate, n}]}` |
| `counter_drafter` | none | `{counter_text, cited_evidence_ids[], confidence, escalate}` |

No subagent gets a tool it does not need. The drafter gets none. Nothing here has a send tool.

`observed_win_rate` comes from a materialised view over `run ⋈ outcome`, grouped by `(competitor_id, claim_class, evidence_id)`. Return `n` with it. Do not rank on a rate where `n < 5`.

**6.6 Check, both layers must pass.**

*Deterministic, failure blocks:*
- every `cited_evidence_id` resolves
- every `source_url` returned 2xx in the last 7 days (cached HEAD)
- zero banned terms, case-insensitive, word boundary
- `counter_text` within length bounds
- the draft's `competitor_id` matches the extracted one
- no email or phone number in `counter_text`

*LLM judge, score 1-5, all must hit `judge_threshold`:*
- addresses the specific claim
- matches the narrative map's register
- asserts nothing the cited evidence does not support

Either failure routes to the PMM, never to the rep. `escalate:true` does the same.

**6.7 Gate, Slack DM to the rep.** Block Kit containing:
- the claim quoted, with a deep link to the call moment
- the drafted counter
- evidence as clickable links
- a "why you're seeing this" line. **Required.** Adoption depends on it.
- buttons: **Send / Edit / Dismiss**. Edit opens a prefilled modal.

Record `gate_actor`, `gate_action`, `gate_latency_ms`, `gated_at`. On timeout: `timed_out`, post to the PMM channel, send nothing.

**6.8 Semantic diff, this computes the billing metric.** On `gate_action='edit'`, send original and edited text to an LLM:

```json
{"changed": ["..."], "implies": "...", "example": "...", "is_material": true}
```

`is_material` is **true** only if meaning changed: a different claim, a different competitor, an argument added or removed, a factual correction. **False** for typo, grammar, tone, length, formatting.

Because it drives billing:
- store the full diff, the classifier prompt hash and its model on the run
- expose a per-run customer override, logged as an append, never a mutation
- version the classifier and stamp the version on the run

Write the result to `memory`, concern `counter_style`, subject the rep.

**6.9 Act.** On approve or edit, write the counter to the CRM opportunity via the `effect` table. Stage 1 stops here. Email send is **not in v1**. Leave the seam, do not build it.

**6.10 Outcomes.** Poll or subscribe to CRM stage changes. On close, append one `outcome` row. Keep a watermark per `outcome_key`. **Report every outcome metric as of the watermark, never wall clock.**

**6.11 Compaction.** Weekly per `(tenant, subject, concern)`: summarise unsuperseded `memory` rows into one, insert it, mark the inputs superseded. Cap memory at a fixed token budget at draft time.

---

## 7. UI layer

Two surfaces, one run. **Slack** for the AE mid-deal. The **Relay SuperAgent workspace** for the operator and PMM doing batch review. The workspace is where you watch the agent work rather than wait for output.

**Protocol: AG-UI** ([docs.ag-ui.com](https://docs.ag-ui.com)), over SSE, with CopilotKit React bindings. LangGraph is a first-party AG-UI integration, so this is wiring, not a custom transport.

> **Not AP2.** AP2 is Google's Agent Payments Protocol, for proving a human authorised an agent's purchase. Unrelated to UI streaming.

**Events, and what renders**

| Event | Renders as |
|---|---|
| `RunStarted` / `RunFinished` / `RunError` | status chip on the queue row |
| `StepStarted` / `StepFinished` | pipeline stepper: safety → retrieve → draft → check → gate |
| `ToolCall*` | live activity line, "reading transcript", "searching evidence" |
| `TextMessage*` | the counter streaming in token by token |
| `ReasoningMessage*` | the "why you're seeing this" panel, live |
| `StateSnapshot` / `StateDelta` | shared run object via JSON Patch |
| `Custom` | our extensions, namespaced `relay_superagent.*` |

**Components:** `CounterCard` (claim, counter, evidence, Approve/Edit/Dismiss), `EvidenceCard` (source, snippet, win rate with `n`), `DiffView` (original against edited, with the override from §6.8), `RunTimeline`, `QueueTable`.

**The gate.** The LangGraph interrupt maps to AG-UI's interrupt. Slack and the workspace resolve the same one. Whichever answers first wins, and the `effect` table makes that race safe.

**Rules**
- The UI is a view over the ledger, never a second source of truth.
- Reasoning streams live. It is not summarised afterwards.
- A run never blocks on a client being connected. Disconnect, reconnect and replay must not fail or duplicate it.
- UI actions write the same `gate_*` columns as Slack. One decision path, two transports.
- Event payloads carry the same tenant isolation as queries.

*(If agent-chosen components are ever needed, A2UI is the payload spec that rides on AG-UI. Not in v1.)*

---

## 8. Non-functional

| Property | Requirement |
|---|---|
| **Idempotency** | `idempotency_key = sha256(tenant_id ‖ trigger_ref ‖ policy_version)`, unique index. Every side effect keyed in `effect`. Replay, retry and crash recovery must never produce a second Slack post or CRM note. |
| **Durability** | LangGraph Postgres checkpointer. Restart resumes from the last checkpoint and repeats no completed side effect. The graph can sit interrupted at §6.7 for days. |
| **Retries** | Exponential backoff with jitter, max 5, then `failed` plus dead letter. Never retry a node that already produced a side effect. |
| **Backpressure** | Per-tenant concurrency semaphore, per-rep daily cap. If a rep's `awaiting_gate` queue passes 3, stop generating for them. A flooded reviewer stops reviewing, which kills the data supply. |
| **Cost** | Per-tenant daily token ceiling. On breach, pause and alert. Degrade to nothing, never to worse output. |
| **Tenancy** | RLS everywhere, separate vector namespaces. No code path reads across tenants, including admin tools and evals. |
| **Secrets** | OAuth tokens KMS-encrypted, never logged, never in traces. |
| **Replay** | Re-score historical runs against a new `policy_version` offline with side effects disabled. Required before any prompt or policy change ships. |

---

## 9. Tests and evals

**Principle (proven in ServiceWorker):** you do not evaluate the agent, you evaluate the harness. Deterministic code gets tests that run on every commit with no model call. Models get evals, and only at the seams where a model actually touches the run.

### 9.1 Ports and fakes — build first

Every external rail behind a port, each with a real adapter and an in-memory fake: `TranscriptPort` (Gong/Fathom), `CrmPort`, `SlackPort`, `LlmPort`, plus a controllable clock. Ship a `ScriptedLlm` (returns fixture responses) and a `TimingOutLlm`. The full pipeline — trigger to gate to effect — must run in CI in milliseconds, deterministic, zero cost.

### 9.2 Deterministic tests, every commit

```
tests/
  test_policy.py       # §6.3 safety checks: each rule allows/suppresses correctly
  test_checks.py       # §6.6 layer 1: dead URL blocks, banned term blocks, competitor mismatch blocks
  test_idempotency.py  # same webhook 10x → one run, one slack post, one crm note
  test_states.py       # every legal transition; illegal ones raise
  test_supervisor.py   # timeout → escalation (never send); watermark; backpressure cap
  test_scenarios.py    # golden runs end-to-end on ScriptedLlm — the spec as executable tests
```

`test_scenarios.py` asserts on **tool calls, decision JSON and `effect` rows — never on counter prose.** "Did it draft, cite evidence_42, gate to rep_7, and write nothing to the CRM" is deterministic; "was the counter good" is not, and belongs to the judge.

### 9.3 Model evals — at the five seams only

| Seam | Fixture set | Metric | Bar |
|---|---|---|---|
| Mention confirmation (§6.2) | labelled snippets, incl. near-misses | precision / recall | P ≥ 0.85 before stage 1 |
| Claim extraction (§6.5) | labelled transcripts | field-level F1 | wrong `competitor_id` = auto-fail |
| Counter drafting (§6.5) | golden scenarios | layer-1 assertions + judge | deterministic layer: 100% |
| **Judge calibration** (§6.6) | human-labelled verdicts | agreement with humans | re-calibrate on every judge prompt/model change. An uncalibrated judge silently passes bad drafts. |
| Semantic diff `is_material` (§6.8) | labelled edit pairs | accuracy | **highest bar — this computes the invoice.** Errors here are billing disputes. |

```
evals/
  detection/    seam 1 and 2 fixtures
  drafting/     seam 3 scenarios + judge rubric
  judge/        seam 4 calibration set
  diff/         seam 5 edit pairs
  adversarial/  see 9.4
  run.py        pinned models, one command
```

Seed ~30 hand-labelled cases per seam before production traffic.

### 9.4 Adversarial suite — 100% pass, not 95%

Transcripts are **attacker-controlled input**: anything a buyer says on a recorded call flows into our context. Fixtures must include: instruction injection spoken on a call ("ignore your rules and…"), a buyer misquoting our pricing to elicit a false claim, attempts to extract another account's information, claims about competitors we do not track, and profanity/legal-bait the counter must never echo. Any failure blocks the release.

### 9.5 When things run

| Trigger | What runs | Cost |
|---|---|---|
| every commit | 9.2 + full pipeline on `ScriptedLlm` | ms, free |
| prompt / policy / model change | affected seam's evals + **offline replay over the ledger** (§8), diff the decisions | minutes |
| nightly | full eval suite on pinned models, drift vs last run | small |
| production, continuous | the ledger is the online eval: reviewer marks → live trigger precision, `gate_action` → live correction rate, `gate_latency_p95` → trust decay. Threshold breach auto-demotes autonomy (§10). | free |

CI gates: deterministic suites 100%; judge mean must not regress > 0.2 vs `main`; adversarial 100%.

### 9.6 The flywheel

Every `gate_action='reject'` auto-emits a candidate fixture for the relevant seam, pending one human accept. Production failures become regression tests. This is the only way the suite gets harder as the model gets better.

## 10. Metrics

| Metric | Definition |
|---|---|
| `correction_rate` | `(material edits + rejects) / surfaced`, as of watermark. **The commercial metric.** |
| `trigger_precision` | share of fired triggers marked relevant, against a labelled sample |
| `gate_latency_p50/p95` | Slack post to action. Rising p95 is the early warning that trust is slipping. |
| `weekly_active_reviewers` | distinct `gate_actor` per 7 days over enrolled |
| `counter_usage_rate` | `(approve + non-material edit) / surfaced` |
| `cost_per_run` | tokens and dollars |
| `win_rate_by_arm` | treated against holdout, **with confidence intervals, never a point estimate** |

Alert on: correction rate over 10% (auto-demote the autonomy stage), gate p95 doubling week on week, cost ceiling hit, dead letter queue non-empty.

---

## 11. Done when

1. A Gong webhook naming a configured competitor produces a Slack DM to the right rep, **p95 under 10 minutes**.
2. Replaying that webhook 10 times gives exactly one run, one Slack post, one CRM note.
3. Killing the service mid-run and restarting resumes with no duplicate side effects.
4. A run sits in `awaiting_gate` for 7 days and still completes correctly.
5. Timeout escalates to the PMM and sends nothing.
6. A draft citing a dead URL never reaches a rep.
7. An edit produces a `memory` row, a `gate_is_material` value, and a retrievable diff.
8. An opportunity closing 60 days later attaches an `outcome` to the right run.
9. `correction_rate` is queryable by the customer and reconciles with their own Slack audit log.
10. Adding a competitor, banned term or cap takes effect **with no deploy**.
11. `evals/run.py` passes on a clean checkout.
12. No query can return rows across two tenants.
13. The workspace streams steps, tool activity, reasoning and the counter live, not on completion.
14. Killing the browser mid-run replays correctly and does not affect the run.
15. Approving in Slack resolves the workspace card and the reverse, with no duplicate effect.

---

## 12. Your call

- Temporal or a Postgres job table. Use what you already run.
- Salesforce or HubSpot first. Not both at once.
- Judge model and threshold, calibrated against the seed scenarios.
- `claim_class` as embeddings or a fixed taxonomy. Fixed is fine for v1 and faster.

---

## Appendix A — Prior art, and what not to copy

This architecture is not speculative. LangChain built and published a GTM agent on the same shape in December 2025.

**Read:** [How we built LangChain's GTM agent](https://www.langchain.com/blog/how-we-built-langchains-gtm-agent) and the [`deepagents`](https://github.com/langchain-ai/deepagents) repo. Worth an hour.

**Five things it does that this spec adopts:**

1. Safety checks before retrieval, not after. §6.3
2. Edit, then structured extraction, then an append-only per-person log, then cron compaction. §6.8, §6.11
3. Subagents with constrained tools and typed output, in parallel. §6.5
4. Two eval layers, scenario library written before production code. §9
5. Virtual filesystem for large tool results, so transcripts stay out of the context window. Use the `deepagents` primitive rather than writing truncation logic.

**Four things not to port:**

| They do | We do | Why |
|---|---|---|
| 48h SLA auto-sends unapproved items | Timeout escalates to the PMM, sends nothing | Theirs is outbound to a lead. Ours is a factual claim about a named competitor. Unreviewed, that is a legal and trust problem. |
| Trigger is a new inbound lead | Trigger is a claim inside an open opportunity | Much lower volume, much higher stakes per run. Tune for precision, not recall. |
| Reviewers are their own staff | Reviewers are a customer's AEs | Ours have no stake in teaching the system. That is why visible reasoning (§6.7) and the queue cap (§8) are requirements, not polish. |
| Edits carry no commercial meaning | `is_material` drives billing | They never had to classify an edit. Ours must be auditable and customer-overridable. §6.8 |

**One caution.** They report 250% lift in lead-to-opportunity conversion and 86% weekly active use over four months. That is their result, on their funnel, with their own staff. Not our target, and not something to repeat to a customer. What it tells you is that the shape works, and that adoption is achievable when the reasoning is visible.
