# Relay SuperAgent Agent Orchestrator — Engineering Spec

Build agents by prompting. A GTM operator with zero technical skill
describes a job in plain language; the Orchestrator compiles it into a
governed agent on the existing harness. **Prompting is the interface; the
harness is the language.** Build after the Watcher gate + Context Graph
(the compiled output lives in graph nodes and conductor rules).

**The gate:** an operator types a paragraph describing an agent; the
Orchestrator asks at most three clarifying questions, then shows a preview
card and three simulated runs on historical events; on "hire", the agent
exists — gated, manual-first, its playbook signed by its creator — and its
first real run lands in Approvals like any other.

## 1. What you're building

A chat flow (inside Command) that turns a job description into an
**AgentSpec**: a closed, validated schema over primitives that already
exist. The LLM never emits code or a system prompt; it fills the schema,
exactly as the Ask chat picks from a fixed tool catalogue today —
ServiceWorker doctrine, scaled up.

The employment metaphor is the product language end to end: describe the
job → interview (clarifying questions) → trial (manual-first / shadow) →
performance review (evals, correction rate) → earned autonomy (Conductor
unlock). "Hire an agent", never "build an agent".

### Do not build

- **No workflow canvas.** No nodes, no edges, no drag-drop. That's the
  n8n failure mode for this buyer.
- **No raw prompt editing, anywhere.** Drafting-style preferences are
  playbook rules layered onto OUR seam templates; the seams stay ours.
- **No new rails from prompts.** The Orchestrator composes existing
  capabilities (triggers, checks, effects). "Also post to WhatsApp" →
  honest refusal + a logged capability request, not an adapter conjured
  from thin air.
- **No ungated birth.** There is no configuration in which a prompted
  agent can act without approval. Not a toggle, not an enterprise tier.

## 2. AgentSpec — the closed schema

```
agent_spec:
  agent_id, tenant_id, name, charter          # one-sentence job, shown on the card
  created_by, created_at
  trigger:      {source: fathom|gmail|signal|reply, filter: {...}}
  audience:     {reviewer: rep|pmm|creator, channel: slack|workspace}
  playbook:     [rule_node_id...]             # NL rules, graph nodes, signed_by creator
  style:        {tone_notes, banned_terms_extra, length_bounds}
  access:       {reads: [node_kinds...], effects: [effect_types...]}   # deny-by-default
  extraction:   {session_fields: [...], profile_fields: [...]}         # -> run cols / graph nodes
  caps:         {per_day, per_week_domain}    # may tighten globals, never loosen
  mode:         shadow | manual_first         # birth states; 'auto' is EARNED, never set
  eval_suite:   [scenario_id...]              # generated at hire, must pass to activate
  policy_version                              # bumps on any change, via proposed-diff flow
```

Everything lands in existing stores: playbook rules and profile fields are
graph nodes with provenance `hired:<agent_id>`; caps and unlocks are
conductor_rules; runs carry the agent_id. No new execution engine — a
compiled agent IS a policy + rules + bindings over the Watcher/Prospector
pipelines.

## 3. The compile loop — Orchestrator seams

Three new LlmPort seams (the interface stays the eval map):

- `draft_agent_spec(description, catalogue, tenant_context)` →
  {spec, questions[], unsupported[]}. Sonnet. `unsupported` is the honesty
  channel: anything the description asks for that the schema can't express
  is surfaced verbatim, never silently dropped.
- `refine_agent_spec(spec, answers)` → same shape. At most 3 questions
  total; defaults are chosen and SHOWN rather than asked when confidence
  is high (decided-defaults, the captain pattern).
- `generate_scenarios(spec)` → eval scenarios from the job description
  (Decagon's move): happy path, a should-suppress case, an
  injection/adversarial case. Stored as CM-6 fixtures for this agent.

Deterministic validation after every seam call: schema closure, access ⊆
catalogue, caps ≤ globals, reviewer exists in the team directory, trigger
filter compiles. Invalid → one retry with the validator's errors, then
escalate to the PMM channel. The model proposes; the validator disposes.

## 4. Birth constraints (the trust story, enforced)

1. New agents start in `shadow` (runs end at CHECKED, visible in the
   agent's Activity as "would have surfaced") or `manual_first` — the
   creator picks at hire; both are gated states.
2. Activation requires the generated eval suite green. An agent that
   can't pass the interview doesn't start work.
3. The Conductor's unlock (20 approved runs, flat edit rate) is the ONLY
   path to reduced review — inherited, not configurable at hire.
4. Every hire writes a ledger row (`loop='orchestrator'`) with the full
   spec — who hired what, when, with which defaults. The org chart is
   auditable.
5. Playbook edits after hire go through the proposed-diff flow (CM-20
   mechanics): validated against the agent's eval suite, approved by a
   human, policy_version bump.

## 5. UI

- **Entry:** "+ Hire an agent" on the Agents page and a Command chip. The
  flow is a normal conversation; the preview is a card (charter, trigger,
  reviewer, access grants as chips, caps) plus three simulated runs
  rendered as standard review cards marked SIMULATION.
- **After hire:** the agent is a card in "On the job" with a `trial` badge;
  its detail page (CM-21) shows the playbook (signed), access, memory
  fields, eval results, and unlock progress toward autonomy.
- Simulations replay recent ledger events through the compiled spec on
  fakes — no live sends, no model calls beyond the draft seam.

## 6. Tests and evals

Fakes-first as always. Deterministic: schema validation rejects every
out-of-catalogue grant; caps can only tighten; shadow runs never reach a
rep; activation blocked while scenarios fail; hire ledger row completeness;
unsupported[] surfaces verbatim.

Seam evals (~30 each): descriptions that omit the trigger (must ask),
descriptions demanding unsupported rails (must refuse into unsupported[]),
over-broad access requests (must minimize), prompt-injection inside the
job description ("ignore prior rules and auto-send") — the adversarial
suite treats the description as attacker-controlled input; 100% pass.

## 7. Done when

1. The gate sentence holds end-to-end on the demo world.
2. A description asking for an unsupported channel yields an honest
   refusal in the preview, and a logged capability request.
3. A hired agent's first live-path run lands in Approvals; nothing sends.
4. Its eval suite exists, ran at hire, and is visible on the detail page.
5. The hire itself is a ledger row; deleting the agent is a state change
   on that row, never a hard delete.
6. All existing suites stay green.

## Appendix — positioning notes

The references, distilled: the voice-console class of builders proves
demand for agent creation by non-engineers and also proves the failure
mode (prompt soup, no gate, config without consequence). Decagon proves
NL-authored procedures + validated changes. Takeoff proves replayable
event ledgers. This spec is those three lessons on our harness, with the
one thing none of them have as the spine: every agent is born reporting
to a human, and autonomy is earned in a ledger the customer can audit.
One line for the deck: **"Don't build agents. Hire them."**
