# Relay

> **Every small business should run itself.**

Relay is the super agent that makes a small business AI-first. Prompt a few lines. The agents run your operations. You approve before anything sends.

## The problem

In commerce, every unit of growth buys a proportional unit of ops labor. A brand doubles its orders and doubles the people confirming COD, chasing failed payments, reviewing refunds, filing disputes, and tying out settlements. Revenue scales linearly, headcount scales right alongside it, and growth never compounds margin — it just buys more work.

The gap that makes this solvable: at a small merchant the **labor line is roughly 10x the software line**. Ops headcount scales with orders; software doesn't. Moving work across that boundary is the entire opportunity, and it's why this is priced against the labor budget rather than sold as another SaaS seat.

## Who it's for

Millions of small merchants running on teams of five to ten. Non-technical. No API keys to bring, no integrations project to run, nobody whose job is "owning the tool." They don't want a canvas to configure — they want to describe an outcome in a few lines and have it handled. Relay holds every credential and connection itself; there is no setup project.

## The six agents: one order, six moments

Relay's lineup is the money motion of a single commerce order, in lifecycle order. Each entry lists what the work costs today — that gap is the product.

| Moment | Agent | Today | Status |
|---|---|---|---|
| Cart dropped | **Cart Rescue** | A WhatsApp blast recovers a fraction; nobody can call 400 dropped carts a day | Roadmap |
| Payment failed | **Payment Rescue** | A failed UPI payment is just a lost order; nobody reads a decline code | Roadmap |
| Before dispatch | **COD Guard** | Someone works the COD list every morning; COD is 50–65% of orders, 15–25% come back | Roadmap |
| Refund claimed | **Refund Shield** | Claims get paid before review, because reviewing every claim costs more than the fraud | Roadmap |
| Dispute filed | **Dispute Defender** | Evidence sits across store, courier and inbox; by the time it's gathered the window closed | **Wired end-to-end** |
| Money settles | **Reconciliation** | A finance exec ties out lines by hand daily; tools reach ~80%, failures surface weeks late | Roadmap |

## Why one super agent, not six tools

Every one of those moments keys off the same object: **the order**. A dropped cart, a declined payment, a COD confirmation, a refund claim, a chargeback, and a settlement break are six views of one order. Point tools each hold one view and are blind to the rest.

Relay holds the whole graph — order → payment → shipment → conversation → refund → dispute → settlement — and that's where compounding comes from. Dispute Defender wins because COD Guard logged the delivery confirmation. Refund Shield holds a claim because Reconciliation knows the payment never settled. COD Guard blocks an address because it failed twice before. **Each agent added makes the others better**, which is a property no point tool can copy.

## The rules (enforced by code, not policy)

- Nothing is filed or sent without a human yes. Small-and-safe can be auto-approved as trust builds; the line is the merchant's to set.
- Drafts cite only the evidence vault. The agent cannot invent facts.
- Every skip is logged with a reason. Side effects happen exactly once, even across crashes.
- Every number can be recomputed from the merchant's own record.

## What's actually built (honest status)

- **The engine, fully tested:** trigger → classify → draft → check → gate → act as a strict run state machine; append-only Postgres ledger with row-level security and write-once gate fields; ports-and-fakes adapters; multi-tenancy; supervisor stall detection. 108 tests, ~1s, no credentials needed.
- **Dispute Defender, domain-complete:** disputes arrive as structured webhooks with a reason code; the agent structures the claim, assembles an evidence pack matched to that code, drafts the response, and files once the merchant approves. Disputes are never held out of treatment — a missed deadline is real money.
- **The demo workspace:** Home (prompt), Approvals, Pipeline, Journeys, Evidence, and an Agents console showing all six.
- **Not live:** every connector runs on fakes. No real bank or PG webhook is attached yet, and the self-serve onboarding that the "no setup project" promise requires is not built.

## Try it

```bash
git clone https://github.com/mothivenkatesh/relay-superagent && cd relay-superagent
uv run pytest                    # 108 tests, ~1s
make pg                          # local Postgres 17 on :5435
uv run python demo/server.py     # workspace on http://localhost:8790
```

Log in with **"Continue with the demo workspace"**, then walk Home → Approvals → Journeys → Agents.

## Origin

Forked 2026-08-04 from [CoMarketer](https://github.com/mothivenkatesh/comarketer) for its domain-agnostic harness, then rebuilt end-to-end onto the dispute domain — models, pipeline, SQL schema, prompts, tests, and demo content. The inherited Fathom/HubSpot/Gmail adapters remain only as reference implementations of the adapter pattern.

## Read more

- [`CLAUDE.md`](CLAUDE.md) — project memory. Read this first.
- [`decisions.md`](decisions.md) — append-only decision ledger (D1–D21 inherited, D22+ are Relay's).
- [`spec/`](spec/) — inherited engine specs; read for the pattern, not the domain.

Secrets live in the macOS keychain only, never `.env`:
```bash
security add-generic-password -U -s relay_superagent -a <name> -w '<value>'
```
