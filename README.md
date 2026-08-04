# Relay SuperAgent

**An agentic platform built for SMB teams — where the next customer isn't a person clicking through a workflow, it's an agent doing the work.**

Small business teams (5–10 people) don't want a tool. They're not technical, they can't bring their own API keys, and they don't have anyone whose job it is to configure an integration. What they want is someone — something — that just runs the operations: pull the insights, report what's in flight, take the delegated task and go do it. Relay SuperAgent is that someone: a SuperAgent that scaffolds and runs a fleet of sub-agents on the merchant's behalf, driven entirely by prompting.

## The four things this is (and the one thing it isn't)

1. **Persona: SMB teams of 5–10.** Not enterprise, not solo freelancers. Small enough that nobody owns "the tool," big enough to have real operational surface area (payments, support, ops, insights) that's currently nobody's job.
2. **Built for agents, not for people clicking buttons.** The platform's primary interface isn't a dashboard a human operates — it's a surface an agent (the SuperAgent, or eventually the merchant's own agents) can act through. Your next customer is an AI agent, not a user persona.
3. **A SuperAgent, not a single bot.** One entry point scaffolds multiple sub-agents automatically to run operations end-to-end. The merchant's job is to ask — "how are we doing," "what's stuck," "go handle X" — not to configure a pipeline.
4. **Not a workflow builder.** Workflows are a trap: they turn the product into professional-services-in-disguise, where value is delivered by an implementation partner configuring steps for a fee. This platform runs operations itself. If a workflow surface ever exists, it's a value-added layer on top — never the core product.

**The hard constraint from how this class of product actually wins (Lassie/dental-ops precedent):** the customer cannot bring their own keys, cannot configure connectors, and cannot tolerate a setup project. Relay SuperAgent has to hold and manage every credential and integration itself, ship a self-serve onboarding as simple as a Stripe checkout, and get to ~95%+ hands-off automation with a human-escalation path for the tail — not 100% before anything ships. The product is judged on hours of labor it took off the team's plate, not on features it exposes.

## What's inherited, what's not

This repo was scaffolded from [CoMarketer](https://github.com/mothivenkatesh/comarketer), a working GTM-defense agent system — kept because the **harness** is domain-agnostic and genuinely reusable:

- **Built and tested, and applies as-is:** the pipeline/run state-machine, ports-and-fakes adapter pattern (every external rail is swappable and tested on fakes in ms), an append-only Postgres ledger with row-level security, multi-tenancy, a supervisor for stall detection, and an orchestrator pattern for "hire an agent by prompting." 98 tests pass, 2 skip without a local Postgres.
- **Not yet built, and specific to the old product:** the actual agents (Prospector, the deal-defense Watcher), the adapters (Fathom, HubSpot, Slack, Gmail) — these are GTM-sales-specific and don't map onto SMB merchant operations. They're left in place as **reference implementations of the pattern**, not code to keep.
- **Undecided — needs the captain's call before real build starts:** which SMB operations domain this targets first (payments recon? support? inventory? something Cashfree-adjacent?), what the actual sub-agents are, what "insights / work status / delegate work" resolve to as concrete agent capabilities, and what the MVP gate is. The old MVP gate (a Fathom call → Slack DM → HubSpot note) is CoMarketer's and does not carry over.

## Try it (15 minutes)

```bash
git clone https://github.com/mothivenkatesh/relay-superagent && cd relay-superagent
uv run pytest                    # tests, ~10s, no credentials needed
make pg                          # local Postgres 17 on :5434
uv run python demo/server.py     # workspace on http://localhost:8787
```

The demo still runs the inherited GTM-defense scenario end to end (Home → Approvals → Journeys → Agents) — useful for seeing the harness work, not representative of the SMB product yet.

## Read more

- [`CLAUDE.md`](CLAUDE.md) — project memory. Read this first.
- [`decisions.md`](decisions.md) — every non-obvious choice with its reason, inherited from CoMarketer (D1–D21); append new decisions here going forward.
- [`docs/`](docs/) — inherited PRDs (CoMarketer's — stale for this product; kept for reference).
- [`spec/`](spec/) — inherited locked specs (CoMarketer's agents/orchestrator) — read for the pattern, not the domain.

Secrets live in the macOS keychain only, never `.env`:
```bash
security add-generic-password -U -s relay_superagent -a <name> -w '<value>'
```
