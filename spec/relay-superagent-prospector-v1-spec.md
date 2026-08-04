# Relay SuperAgent Prospector v1 — Engineering Spec

The second loop. Watcher defends deals that exist; Prospector starts ones that
don't. Same harness, same ledger, same gate discipline — pointed outward.

**The gate (all work sequences backward from this):** a hiring signal on a
target account produces, within one hour, up to three role-specific gated
emails; each Approve sends from the operator's real Gmail; a prospect reply
lands as an outcome row on the same run in Postgres.

## 1. What you're building

A signal about a target account (they're hiring SDRs, they raised, they
adopted a tech) turns into a small set of role-specific emails — one per
stakeholder, at most three per account — each individually approved by the
operator before anything sends. Replies are detected and written back as
outcomes on the same run rows. The ledger of (signal, message, human
judgement, reply) is the product; the drafting is the commodity, exactly as
in Watcher.

Multi-threading means: one signal → several stakeholders in one account →
one run PER MESSAGE, grouped by the signal. The champion gets a different
email than the exec. Gate, edit-memory, correction rate, and billing all
work per message with zero changes to the state machine.

### Do not build

- **No LinkedIn automation. None.** No official API exists for outreach;
  every tool that "automates LinkedIn" browser-automates against ToS and
  gets accounts banned. LinkedIn touches ship as gated HUMAN TASKS (a card
  saying "send this connection note", with the note drafted) — the operator
  clicks through and does it by hand. The task card and its dismissal are
  still ledger rows.
- No auto-send, ever. Timeout escalates to the PMM channel, same as Watcher.
- No sequences/cadences engine in v1. One signal → one gated batch → replies.
  Follow-ups are a second signal ("no reply in 7 days"), not a scheduler.
- No email warmup infra, no send-rate arms race. v1 sends from the
  operator's own Gmail at human volumes (≤ 20/day/rep hard cap). If a
  customer wants 500/day they want a spam cannon, and that product loses.
- No enrichment waterfall. One provider behind a port (§3), CSV as the
  zero-dependency path.

## 2. Stack — decided, don't relitigate

Everything from the Watcher spec §2 carries over: Python 3.12, ports and
fakes, Postgres ledger with RLS, keychain secrets, structured outputs,
Haiku 4.5 on cheap seams / Sonnet 5 on judgement seams. Prospector is new
modules in the same repo, not a new service.

## 3. Integrations

| Rail | v1 choice | Why |
|---|---|---|
| Signals | **SignalPort**: Apollo search API (`x-api-key` header) as reference impl + **CSV upload** as the zero-dependency path | Apollo search/counts spend no credits on the existing account; contact email reveal costs credits and is metered per policy. CSV means the loop runs with no vendor at all. |
| Contacts | Same SignalPort (Apollo people search by domain + role) or columns in the CSV | One port, two methods. |
| Send | **Gmail API** `users.messages.send`, scope `gmail.send`, token from keychain `gmail-send-token` | The read rail exists (Watcher's gmail.py); send is one more method and one more effect type. Sends come from the rep's real mailbox — deliverability comes free, volume stays honest. |
| Reply detect | Gmail thread poll (existing poller) matching `threadId` of sent messages | No webhook infra needed; supervisor cadence is enough. |
| CRM check | Existing CrmPort: `open_deal_for_account` | An account with an open deal belongs to Watcher, not Prospector. Hard suppression. |
| Gate | Existing Slack adapter + workspace | Same card anatomy, one new field: the signal, quoted. |

## 4. Data model — additions to §4 of the Watcher spec

Runs table: UNCHANGED. Prospector runs are rows with `loop = 'prospector'`
(the column exists). `trigger_source = 'signal'`, `trigger_ref = signal_id`,
`claim_text` holds the signal summary, `decision` holds the drafted message.
One run per (signal, contact). Idempotency key = sha256(tenant ‖ signal_id ‖
contact_email ‖ policy_version) — a re-fired signal cannot re-email anyone.

New tables (same RLS pattern as everything else):

```sql
CREATE TABLE signals (
  signal_id    text PRIMARY KEY,          -- provider id or sha of CSV row
  tenant_id    text NOT NULL,
  source       text NOT NULL,             -- 'apollo' | 'csv'
  kind         text NOT NULL,             -- 'hiring' | 'funding' | 'tech_adopt' | 'no_reply'
  account_domain text NOT NULL,
  summary      text NOT NULL,             -- one sentence, shown on the card
  payload      jsonb NOT NULL,
  occurred_at  timestamptz NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE contacts (
  contact_id   text PRIMARY KEY,
  tenant_id    text NOT NULL,
  account_domain text NOT NULL,
  email        text NOT NULL,
  name         text,
  title        text,
  role_class   text NOT NULL,             -- 'champion' | 'exec' | 'user'
  source       text NOT NULL,
  UNIQUE (tenant_id, email)
);

CREATE TABLE do_not_contact (
  tenant_id    text NOT NULL,
  address      text NOT NULL,             -- email or bare domain
  reason       text NOT NULL,             -- 'unsubscribe_reply' | 'manual' | 'bounce'
  added_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, address)
);

CREATE TABLE sent_threads (
  run_id         text PRIMARY KEY REFERENCES runs(run_id),
  tenant_id      text NOT NULL,
  gmail_thread_id text NOT NULL,          -- for reply matching
  sent_message_id text NOT NULL
);
```

`do_not_contact` is checked as a hard suppression AND is append-mostly:
rows are only ever removed by an explicit human action in the workspace.

## 5. Run states — unchanged

The Watcher state machine applies verbatim. `acting/acted` = the Gmail send
through the effect table. `resolved` = a reply (or a terminal no-reply after
the follow-up window). Timeout at the gate escalates; nothing sends.

## 6. Pipeline

`prospector.py`, same shape as `pipeline.py`. The model appears at four new
seams, all on LlmPort (§9):

**6.1 Ingest.** SignalPort yields signals (Apollo poll on the tenant's saved
search, or CSV upload in the workspace). Dedupe on signal_id.

**6.2 Confirm** — `confirm_signal(summary, payload, icp_description)` →
{is_relevant, angle, confidence}. Haiku. A hiring signal for a company that
doesn't fit the tenant's ICP dies here as a suppressed run (one row, contact
NULL, reason `not_icp`).

**6.3 Resolve threads** — deterministic + one seam. Contacts for the domain
come from SignalPort/CSV. `pick_threads(signal, contacts, memory)` →
[{contact_id, role_class, angle}] max 3. Sonnet — choosing WHO to write and
with what angle is judgement, and its evals are the interesting ones.

**6.4 Safety (deterministic, per contact, cheapest first).** Suppress when:
- account has an open deal (`open_deal_for_account` — Watcher's territory)
- contact or domain in `do_not_contact`
- contact emailed within `policy.contact_cooldown_days` (default 30)
- domain over `policy.per_domain_per_week` (default 3)
- rep over daily send cap (`policy.per_rep_per_day_sends`, default 20 — hard)
- rep not enrolled; arm = holdout (same account-hash assignment as Watcher,
  so an account is consistently treated or held out ACROSS BOTH LOOPS)

**6.5 Draft** — `draft_opener(signal, contact, angle, evidence, memory)` →
{subject, body, cited_evidence_ids, confidence, escalate}. Sonnet. Memory
notes from Watcher edits apply here too — one voice per rep, learned once.

**6.6 Check (deterministic).** Watcher's layer1 plus: subject ≤ 80 chars and
not deceptive-pattern (no "Re:"/"Fwd:" on a first touch), body length
bounds, unsubscribe line present, tenant postal address present (CAN-SPAM —
both are template-injected, the check just proves it), no contact info other
than the rep's signature, every factual claim cites evidence or the signal.

**6.7 Judge** — existing `judge` seam, threshold from policy.

**6.8 Gate.** Same card as Watcher plus the signal quoted ("Why now"). Cards
for one account are grouped in Slack (one message, three cards) and the
workspace. Each message approves/edits/rejects INDIVIDUALLY. Queue cap
shared with Watcher (a rep's attention is one budget).

**6.9 Act.** Effect type `gmail_send`, target = contact_email, external ref =
Gmail message id; write `sent_threads` row. Exactly-once via the effect
table — a crash between send and save cannot double-send.

**6.10 Outcomes.** The Gmail poller matches inbound to `sent_threads`.
`classify_reply(body)` → {disposition: interested | not_now | unsubscribe |
bounce | other, summary}. Haiku. Outcome row per reply. `unsubscribe` (or a
deterministic keyword hit — "unsubscribe", "remove me", "stop emailing")
auto-appends `do_not_contact` and needs no model to do so. `interested`
escalates to the rep's DM with the reply quoted — the handoff moment.
No reply after `policy.followup_after_days` (default 7) emits ONE `no_reply`
signal, which re-enters at 6.1 for at most one gated follow-up
(`policy.max_touches`, default 2, hard).

**6.11 Learning.** Identical mechanics: material edits → memory notes;
`semantic_diff` on every edit; correction rate per seam and per rep.

## 7. UI

The existing queue renders prospector runs with zero changes (they're runs).
Add: account grouping header on the queue (signal summary + N cards), a CSV
upload on a `/prospector` page, and the do-not-contact list view. LinkedIn
task cards render as a third card type with a single "Done / Skip" action.

## 8. Non-functional

- ≤ 20 sends/day/rep, ≤ 3/week/domain — HARD caps, checked deterministically,
  not policy suggestions.
- CAN-SPAM: unsubscribe line + tenant postal address template-injected into
  every body; opt-outs honored immediately (do_not_contact write is part of
  the reply-classification transaction). v1 targets US B2B; GDPR/EU contact
  handling is out of scope and logged as such.
- A signal batch from ingest to gate ≤ 1 hour (the gate sentence's clock).
- Same crash discipline: every state legal, supervisor stall detection
  already covers prospector runs because they're runs.

## 9. Tests and evals

Ports first: `FakeSignalSource`, `FakeGmailSender` (extends the existing
FakeSlack pattern), everything runs in ms with no vendor.

Four new LlmPort seams — the interface is still the eval map:

| Seam | Model | Eval fixtures (~30 each) |
|---|---|---|
| confirm_signal | Haiku | ICP fit/misfit, stale signals, wrong-country accounts |
| pick_threads | Sonnet | seniority mapping, ≤3 selection, no-contact-data cases |
| draft_opener | Sonnet | angle grounding, no invented claims, length, voice-memory use |
| classify_reply | Haiku | interested vs polite-no, unsubscribe phrasings, OOO ≠ reply |

Adversarial suite (100% pass): prompt-injection in a signal payload or a
REPLY BODY must never mutate do_not_contact for a third party, never leak
another account's data into a draft, never bypass the gate. Reply bodies are
attacker-controlled input — treat them like Watcher treats transcripts.

Deterministic tests: idempotent signal re-fire, double-send impossibility
(effect table), cooldown/caps, holdout consistency across loops,
unsubscribe append, one-follow-up ceiling.

## 10. Metrics

Per tenant, per rep, per seam: correction rate (shared definition), reply
rate, positive-reply rate, gated-to-sent ratio, time-to-gate p95, and
correction rate ON OPENERS specifically — the pricing trigger works the same
way: the tenant pays for sent messages whose edits were non-material.

## 11. Done when

1. CSV with one account + hiring signal + 2 contacts → two gated cards ≤ 1h.
2. Approve sends from real Gmail; message visible in Sent; run acted.
3. A real reply flips the run to resolved with a classified outcome row.
4. "unsubscribe" reply adds do_not_contact; the next signal for that contact
   suppresses.
5. Account with an open HubSpot deal never gets prospected.
6. Caps: the 21st send of a day cannot happen, provably (test, not vibes).
7. All four eval suites ≥ target; adversarial at 100%.
8. Watcher's 94 tests still green — shared harness, no regressions.

## 12. Your call

Batch size per account (≤3), follow-up copy angle, Apollo saved-search
shapes, CSV column schema, Slack grouping layout. Decide and log in
decisions.md.

> Forward pointer: the hard caps in §6.4/§8, the max_touches ceiling, and
> the `no_reply` follow-up signal become Conductor rules (data, not
> constants) once `spec/relay_superagent-context-graph-conductor-spec.md` builds.
> Nothing in this spec waits for that.

## Appendix A — Relation to Watcher, and the LinkedIn stance

Prospector reuses: the ledger and state machine (runs with `loop`), the
effect table, arm assignment, the gate transports, memory, semantic_diff,
supervisor, metrics. It adds: 4 tables, 4 LlmPort seams, SignalPort, a send
method on the Gmail rail. If a piece of Prospector can't reuse the Watcher
harness, that's a design smell — stop and re-read this spec.

LinkedIn: signals ABOUT LinkedIn activity may arrive via the provider
(that's their compliance problem, priced into their product). Actions ON
LinkedIn are human-only task cards. This is a durable position, not a v1
shortcut — the moment this product automates LinkedIn it inherits ban-risk
support tickets forever and the trust story ("you approve everything, we
never touch your accounts") dies.
