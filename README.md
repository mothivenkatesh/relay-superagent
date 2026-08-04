# Relay

**Pre-built agents for payment and compliance ops. Or build your own. You approve before anything sends.**

Relay is an agent orchestrator for payments and compliance operations, built for SMB teams (5–10 people). The merchant doesn't configure workflows — they talk to Relay: ask for insights, ask what's in flight, delegate work. A crew of specialist agents runs the operations behind that one conversation, and every action that touches money or a customer waits at a human gate first.

## The eight agents

| Agent | For | What it does | Status here |
|---|---|---|---|
| **Dispute Defender** | support lead, online merchant | Gathers the evidence, builds the case, files before the deadline | **Wired end-to-end in this repo** |
| COD Guard | ops lead, COD-heavy D2C | Confirms COD orders before dispatch via WhatsApp/voice, blocks repeat bad addresses | Roadmap |
| Payment Rescue | growth lead, UPI-heavy store | Reads the decline reason, calls the buyer, sends a retry link | Roadmap |
| Cart Rescue | growth lead, ad-spending D2C | Calls the buyer in their language on a cart drop, sends a payment link | Roadmap |
| Settlement Clarity | finance lead, multi-channel merchant | Matches payout → order → bank, flags gaps, posts to books | Roadmap |
| Refund Shield | risk lead, high-refund D2C | Scores refund claims against cross-merchant fraud signals, holds the risky | Roadmap |
| Loan Recovery | collections lead, NBFC/lender | On an EMI bounce: verifies, states the EMI, captures consent, acts | Roadmap |
| Due Diligence | compliance lead, LSP/lender | Verifies on Secure ID, risk-tiers, auto-clears low-risk, escalates EDD | Roadmap |

## What's actually built (honest status)

- **The engine, fully tested:** trigger → classify → draft → check → gate → act pipeline as a strict run state-machine; append-only Postgres ledger with row-level security and write-exactly-once gate fields; ports-and-fakes adapters (108 tests run in ~1s with no credentials, including live Postgres contract tests); multi-tenancy; supervisor stall detection; an orchestrator spec for "hire an agent by prompting."
- **Dispute Defender, domain-complete:** disputes arrive as structured webhooks with a reason code; the agent structures the claim, assembles an evidence pack (delivery proof, invoice, comms log) matched to the reason code, drafts the response, and files it after the merchant approves. Dispute responses are never held out — missing a chargeback deadline is real money, so every dispute is treated.
- **The demo workspace** (`demo/server.py`): a full merchant workspace — Home (chat), Approvals, Pipeline, Journeys, Evidence, Agents console with all eight agents — seeded with fictional Indian SMB merchants and realistic dispute rows.
- **Not live:** the connectors run on fakes; no real bank/PG webhook is attached yet.

## The rules (enforced by code, not policy)

- Nothing is filed without a human yes. Approval happens where the merchant already is; small-and-safe can be auto-approved as trust builds.
- Drafts cite only the evidence vault. The agent cannot invent facts.
- Every skip is logged with a reason. Side effects happen exactly once, even across crashes.
- Every number can be recomputed from the merchant's own record.

## Try it (15 minutes)

```bash
git clone https://github.com/mothivenkatesh/relay-superagent && cd relay-superagent
uv run pytest                    # 108 tests, ~1s
make pg                          # local Postgres 17 on :5435
uv run python demo/server.py     # workspace on http://localhost:8790
```

Log in with **"Continue with the demo workspace"**. Walk: Home (chat) → Approvals (decide a dispute response) → Journeys (replay a dispute like a film) → Agents (all eight, with what's live vs. next).

## Origin

Forked 2026-08-04 from [CoMarketer](https://github.com/mothivenkatesh/comarketer) for its domain-agnostic harness, then domain-rebuilt: the GTM deal-defense domain (competitors, deals, counter-drafts) was replaced by the dispute domain end-to-end — models, pipeline, SQL schema, prompts, tests, and demo content. The Fathom/HubSpot/Gmail adapters remain as reference implementations of the adapter pattern.

## Read more

- [`CLAUDE.md`](CLAUDE.md) — project memory. Read this first.
- [`decisions.md`](decisions.md) — append-only decision ledger (D1–D21 inherited, D22+ are Relay's).
- [`spec/`](spec/) — inherited engine specs (state machine, orchestrator, context graph); read for the pattern.

Secrets live in the macOS keychain only, never `.env`:
```bash
security add-generic-password -U -s relay_superagent -a <name> -w '<value>'
```
