# Relay

> **Every small business should run itself.**

Relay is the super agent that makes a small business AI-first. Prompt a few lines. The agents run your operations. You approve before anything sends.

## The problem

In commerce, every unit of growth buys a proportional unit of ops labor. A brand doubles its orders and doubles the people confirming COD, chasing failed payments, reviewing refunds, filing disputes, and tying out settlements. Revenue scales linearly, headcount scales right alongside it, and growth never compounds margin — it just buys more work.

The gap that makes this solvable: at a small merchant the **labor line is roughly 10x the software line**. Ops headcount scales with orders; software doesn't. Moving work across that boundary is the entire opportunity, and it's why this is priced against the labor budget rather than sold as another SaaS seat.

## Who it's for

Millions of small merchants running on teams of five to ten. Non-technical. No API keys to bring, no integrations project to run, nobody whose job is "owning the tool." They don't want a canvas to configure — they want to describe an outcome in a few lines and have it handled. Relay holds every credential and connection itself; there is no setup project.

## The rule every agent must pass

**An agent only earns a place if it replaces a real job and costs less than that job does.** Not "assists with," not "speeds up" — replaces. Every card in the product names the job it takes over and roughly what that job costs, because if it can't beat the labour it is displacing, it shouldn't ship.

## The team: 14 agents, each with a named role

Every agent holds a named role, so what you are looking at is a staff roster, not a feature list: a team running the business day to day. They work unattended and never stop — but nothing touching money or a customer goes out until you say yes.

A founder thinks "I need an accounts person," never "I need a finance desk," so they report to the manager you would otherwise put on payroll.

**Your accounts manager**

| Role | Does | Replaces |
|---|---|---|
| **Reconciliation Officer** <br>_3-Way Reconciliation_ | Matches every order to the payout to the bank credit | An accounts executive tying out the bank, ₹20–30k/mo |
| **Settlement Analyst** <br>_Settlement Insights_ | What's landing, when, what was deducted, what's stuck | The part of the accounts job spent reading statements |
| **Cashflow Planner** <br>_Cashflow Forecast_ | What cash lands this week, what's committed, when it gets tight | The finance person keeping the cash sheet, ₹25–40k/mo |
| **Payouts Clerk** <br>_Payouts Desk_ | Pays vendors, staff and refunds on time, each their way | The accounts payable clerk, ₹20–28k/mo |

**Your inventory manager**

| Role | Does | Replaces |
|---|---|---|
| **Inventory Controller** <br>_Stock Watch_ | Watches stock across channels, stops you selling what you don't have | An inventory executive, ₹18–25k/mo |

**Your risk & compliance manager**

| Role | Does | Replaces |
|---|---|---|
| **Refund Risk Officer** <br>_Refund Shield_ | Scores every refund claim for fraud before you pay | A fraud reviewer you almost certainly never hired |
| **Compliance Officer** <br>_GST & Compliance_ | Ties GST, TDS and e-invoices to real orders before filing | The monthly compliance scramble your CA bills for |
| **KYC Verifier** <br>_KYC Desk_ | Verifies PAN, Aadhaar, GST, RC or DL in seconds, keeps the proof | A KYC executive checking documents by hand, ₹18–25k/mo |

**Your support manager**

| Role | Does | Replaces |
|---|---|---|
| **Disputes Officer** <br>_Dispute Defender_ — **wired end-to-end** | Gathers proof, writes the reply, files before the deadline | The support executive chasing proof, ₹18–25k/mo |
| **Returns Coordinator** <br>_Returns Desk_ | Follows a return from pickup to restock, then releases the refund | The returns coordinator between courier, warehouse and refund |

**Your telecaller**

| Role | Does | Replaces |
|---|---|---|
| **Cart Recovery Caller** <br>_Cart Rescue_ | Calls buyers who left without paying, sends a payment link | A telecaller, ₹15–22k/mo |
| **Payment Recovery Caller** <br>_Payment Rescue_ | Reads the decline reason, waits, calls, sends a fresh link | A telecaller, ₹15–22k/mo |
| **COD Confirmation Caller** <br>_COD Guard_ | Confirms COD before dispatch, blocks addresses that keep failing | The morning COD calling shift, ₹15–22k/mo |

**Your MIS analyst**

| Role | Does | Replaces |
|---|---|---|
| **MIS Analyst** <br>_Daily MIS_ | The numbers that matter each morning, and what changed and why | An MIS executive, ₹25–35k/mo |

Salary bands are indicative Indian small-business ranges for framing the arbitrage, not sourced figures — replace them with real data before any external use.

The surface is deliberately plain: the reader is a shop owner, not a risk lead. A five-person business has no ops lead and no finance lead; one person wears every hat, and every screen is written for that person.

## Why one back office, not fourteen tools

Every one of those jobs keys off the same object: **the order**. A dropped cart, a declined payment, a COD confirmation, a stock line, a refund claim, a chargeback, a GST line and a settlement break are all views of one order. Point tools each hold one view and are blind to the rest.

Relay holds the whole graph — order → payment → shipment → conversation → refund → dispute → settlement — and that's where compounding comes from. Dispute Defender wins because COD Guard logged the delivery confirmation. Refund Shield holds a claim because 3-Way Reconciliation knows the payment never settled. COD Guard blocks an address because it failed twice before. **Each agent added makes the others better**, which is a property no point tool can copy.

## The rules (enforced by code, not policy)

- Nothing is filed or sent without a human yes. Small-and-safe can be auto-approved as trust builds; the line is the merchant's to set.
- Drafts cite only the evidence vault. The agent cannot invent facts.
- Every skip is logged with a reason. Side effects happen exactly once, even across crashes.
- Every number can be recomputed from the merchant's own record.

## What's actually built (honest status)

- **The engine, fully tested:** trigger → classify → draft → check → gate → act as a strict run state machine; append-only Postgres ledger with row-level security and write-once gate fields; ports-and-fakes adapters; multi-tenancy; supervisor stall detection. 108 tests, ~1s, no credentials needed.
- **Dispute Defender, domain-complete:** disputes arrive as structured webhooks with a reason code; the agent structures the claim, assembles an evidence pack matched to that code, drafts the response, and files once the merchant approves. Disputes are never held out of treatment — a missed deadline is real money.
- **The demo:** Home (one money number, what needs your yes, what the team did today, and a prompt box), Needs you, History, Your team (all 14 agents grouped by the hire they replace), and Settings.
- **Not live:** every connector runs on fakes. No real bank or PG webhook is attached yet, and the self-serve onboarding that the "no setup project" promise requires is not built.

## Try it

```bash
git clone https://github.com/mothivenkatesh/relay-superagent && cd relay-superagent
uv run pytest                    # 108 tests, ~1s
make pg                          # local Postgres 17 on :5435
uv run python demo/server.py     # workspace on http://localhost:8790
```

Log in with **"Continue with the demo workspace"**, then walk Home → Needs you → History → Your team.

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
