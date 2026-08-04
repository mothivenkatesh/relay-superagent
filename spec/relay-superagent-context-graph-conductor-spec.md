# Relay Context Graph + Conductor — Engineering Spec

The third piece of the system, and the one that makes the loops compound.
Watcher defends deals, Prospector starts them; the Graph is what both know,
and the Conductor is what neither is allowed to do yet.

**Build after the Watcher gate passes.** Nothing here blocks v1.

**The gate:** with two loops live, disabling a tenant's signed positioning
node provably prevents any new Prospector run from reaching a rep; a play
whose last 50 sends replied under its prior's floor pauses itself with a
PMM escalation stating what it cost; and every drafted sentence in a gated
card can be traced to a graph node id.

## 1. What you're building

Two things, both deterministic at the core:

**The Context Graph** — per-tenant typed nodes with provenance and expiry:
what the tenant knows (positioning, verbatim buyer quotes, proof bank,
objection library), what the world signals (signals with decay windows),
who exists (accounts, contacts, buying committees), and what experience has
taught (rules, priors). Every agent reads it through one port; every draft
cites node ids the way Watcher drafts cite evidence_ids today.

**The Conductor** — agent-to-agent communication as a blackboard plus
gates, NOT agent chatter. Agents never message each other; they read/write
the ledger and the graph, and the Conductor — plain code inside the
supervisor sweep, no model — enforces sequencing rules between loops:
preconditions (no outbound without signed positioning), unlocks (automation
only after N approved manual reps), throttles (below the prior's floor →
pause), caps (the existing hard limits, now data).

The rules and priors ship SEEDED from a real agency's 7-month field log
(16 failure-derived rules, ~18 signal priors, ~16 stage benchmarks — see
`seed/` and Appendix A) and are replaced by the tenant's own ledger data as
it accumulates. Their principle, our architecture: "your own data beats
data you buy."

### Do not build

- No graph database. It's three Postgres tables. Nodes, edges, done.
- No LLM in the Conductor. A rule either holds or it doesn't.
- No inter-agent messages, queues, or "agent protocols". The blackboard IS
  the protocol. If two loops need to coordinate, the coordination is a rule.
- No ontology project. Twelve node kinds, listed below, closed set for v1.
- No embedding search in v1. Retrieval is by kind + ref + recency; the
  graph is small per tenant.

## 2. Data model — additions to the existing DDL, same RLS pattern

```sql
CREATE TABLE graph_nodes (
  node_id      text PRIMARY KEY,
  tenant_id    text NOT NULL,
  kind         text NOT NULL CHECK (kind IN (
    'account','contact','signal','claim','evidence','quote','positioning',
    'objection','proof','playbook','rule','prior')),
  ref          text NOT NULL,          -- natural key: domain, email, signal id…
  body         jsonb NOT NULL,
  provenance   jsonb NOT NULL,         -- {origin: run_id | doc | 'seed', row: …}
  signed_by    text,                   -- humans sign positioning/playbook nodes
  valid_from   timestamptz NOT NULL DEFAULT now(),
  expires_at   timestamptz,            -- signal decay, playbook stage expiry
  superseded_by text,
  UNIQUE (tenant_id, kind, ref, valid_from)
);

CREATE TABLE graph_edges (
  tenant_id text NOT NULL,
  src text NOT NULL REFERENCES graph_nodes(node_id),
  dst text NOT NULL REFERENCES graph_nodes(node_id),
  rel text NOT NULL,                   -- 'belongs_to','cites','contradicts','about'
  body jsonb NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, src, dst, rel)
);

CREATE TABLE conductor_rules (
  rule_id    text PRIMARY KEY,
  tenant_id  text NOT NULL,            -- 'global' allowed for seeds
  kind       text NOT NULL CHECK (kind IN ('precondition','unlock','throttle','cap')),
  scope      jsonb NOT NULL,           -- {loop} | {play} | {signal_kind}
  spec       jsonb NOT NULL,           -- see §4 shapes
  source     text NOT NULL,            -- 'seed:failure#2' | run_id | 'manual'
  enabled    boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);
```

Priors are nodes (`kind='prior'`), not a fourth table: body carries
{metric, segment, floor, ceiling, decay_days, observed_n}; provenance
carries the seed row or the ledger query that re-estimated it.

Nodes are append-only via supersession (same doctrine as memory notes):
an edited positioning doc is a new node with `superseded_by` set on the
old one. History is never lost — "what did the agent know when it drafted
this" stays answerable forever, which is what makes correction rate
auditable.

## 3. GraphPort

```python
class GraphPort(Protocol):
    def get(self, tenant_id, kind, ref) -> Node | None: ...          # live node
    def about(self, tenant_id, node_id, rel=None) -> list[Node]: ... # neighbors
    def put(self, node: Node, edges: list[Edge]) -> str: ...         # supersedes
    def sign(self, tenant_id, node_id, actor) -> None: ...           # write-once
    def live_rules(self, tenant_id, scope) -> list[Rule]: ...
    def prior(self, tenant_id, metric, segment) -> Prior | None: ... # tenant, else global seed
```

One implementation over Postgres, one fake over dicts. `sign` is
write-once like gate fields — a signature cannot be edited, only a new
node signed.

## 4. The Conductor

Runs inside the existing supervisor sweep. Four rule kinds, exact shapes:

- **precondition** `{requires: {kind, signed: true}}` scoped to a loop.
  Checked at ingest: a Prospector event for a tenant with no live signed
  positioning node becomes a suppressed run, reason `conductor:<rule_id>`.
  Suppressed, not dropped — the ledger still counts what the rule cost.
- **unlock** `{requires_approved: N, of: 'manual'}` scoped to a play.
  The gate already counts approvals; until the count of approved-or-
  non-materially-edited runs for the play reaches N, drafts route to the
  PMM channel instead of reps (the play runs "manual-first" through the
  same pipeline). Seed value N=20, from the field log's most-enforced rule.
- **throttle** `{metric, floor, window_n, action: 'pause'}` scoped to a
  signal kind or play. Sweep compares the ledger metric over the last
  window_n against the live prior; below floor → the play's runs suppress
  and ONE escalation posts with the numbers ("reply 2.1% over 50 sends,
  prior floor 4% — pausing; spent: N sends, M edits"). Resume is a human
  action in the workspace, logged as a rule change.
- **cap** — the existing hard limits (20 sends/day/rep, 3/week/domain,
  queue cap 3) move from constants to rows, so tenants see them and the
  ledger records which rule suppressed what. The VALUES stay hard floors:
  a tenant rule may tighten a global cap, never loosen it.

Rule evaluation order: caps → preconditions → unlocks → throttles.
First hit wins and is written to `suppressed_reason`. All deterministic;
the sweep stays model-free.

## 5. How the loops change

- Pipelines read policy-adjacent context through GraphPort instead of ad-hoc
  Deps fields: evidence, memory notes, positioning, objections become node
  fetches. Deps keeps the same shape; the evidence list just comes from the
  graph. (Fakes unchanged: FakeGraph returns the same fixtures.)
- `draft_counter` / `draft_opener` prompts include node ids; the returned
  `cited_evidence_ids` generalizes to `cited_node_ids`. Layer-1 check:
  every cited id resolves to a live, unexpired node. An expired signal node
  citing is a check failure, not a stale email.
- Signals (Prospector §4) also write `signal` nodes with `expires_at` =
  occurred_at + decay_days from the signal's prior. Suppression "signal
  expired" becomes a graph property, not pipeline arithmetic.
- Outcomes append to the graph too: a reply classified `interested` edges
  contact→claim; the diagnostic agent (later wedge, own spec) walks
  exactly this graph.

## 6. Tests and evals

No new model seams — the Conductor is deterministic, so this is all fast
tests: precondition suppression (and un-suppression when a node is signed),
unlock counting against real gate rows, throttle math at the floor boundary,
cap tightening-only, rule provenance in suppressed_reason, node supersession
(old drafts still resolve their cited ids), expiry (a decayed signal fails
layer-1 citation), RLS on all three tables, seed idempotency (loading seeds
twice changes nothing).

One eval consequence: draft evals gain fixtures where the graph contains a
CONTRADICTION (two live claim nodes that conflict) — the draft must escalate,
not pick one.

## 7. Metrics

Per tenant: rules fired by rule_id (what the system refused, and what that
cost/saved), unlock progress per play, prior vs observed per signal kind,
graph coverage (accounts with signed positioning path, contacts with
committee edges), citation density per draft. The first three are the
customer-facing "why didn't it send" answer — the trust surface.

## 8. Done when

1. Seeds loaded: 16 rules + ~18 signal priors + ~16 benchmark priors as
   nodes/rules with `source: seed:*`, traceable to the field-notes row.
2. Unsigning positioning stops new Prospector runs (test + live demo path).
3. A play with <20 approvals routes to PMM; the 20th approval unlocks reps.
4. A sub-floor reply rate pauses its play with the costed escalation.
5. Every card in the workspace shows "cited: n nodes" resolving to live ids.
6. All existing tests green; fakes still run the whole suite in ms.

## 9. Your call

Node body schemas per kind, edge rel vocabulary beyond the four named,
seed-file format (suggest: one JSON per sheet in `seed/`), sweep cadence.

## Appendix A — Provenance and the smell test

Seed data comes from ONEGTMLAB's "GTM Field Notes" (2026): 16 failure-log
rules, the signal library's decay windows and observed hit-rates, and the
stage benchmarks. Distilled tables live in the arivu source doc
`2026-07-31-gtm-field-notes-distilled.md`; each seed row keeps its sheet
and row reference in provenance. Their own principles are the design brief:
"context quality determines output quality" (the graph), "judgment
compounds, execution commoditizes" (the ledger), "manual mastery before
automation" (the unlock rule), "your own data beats data you buy" (priors
re-estimation).

Reuse smell test, same as Prospector's: this spec adds three tables, one
port, and rule evaluation inside the EXISTING sweep. If an implementation
grows a message bus, a scheduler, or a second supervisor, stop and re-read
§1's "Do not build".
