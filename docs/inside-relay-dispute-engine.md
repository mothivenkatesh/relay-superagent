# Inside Relay: one agent, a context engine, and a gate

Cashfree launched Relay this week: the AI Super Agent that runs payment
operations for SMBs. The launch note covers what it does. This post
covers how the hardest part of it works, using the dispute engine as the
walkthrough, because disputes are where every design decision gets
tested by real consequences.

## The problem, in one merchant's week

7 in 10 Indian SMBs run payment operations entirely by hand. Carts need
calling, failed payments need retrying, COD orders need confirming,
disputes need filing before a bank's deadline. On Cashfree's data an
average SMB spends 60 hours a week on this. A 5 person team does not
have a person to spare, so the work competes with everything else and
loses. A missed dispute deadline is not a task slipped. It is a
chargeback debited.

## What Relay is

Relay is an AI team member that completes payment operations end to
end: it reads the merchant's own transaction records and acts, rather
than flagging work for someone else to do. Merchants describe the
outcome they want in plain language. Relay runs a turnkey agent or
builds one. Approval is required at exactly 2 points: before a payment
moves and before a customer is contacted.

## One agent, all the way down

Relay ships several agents. This post goes deep on 1, Dispute
Defender, because it is the one we have proven end to end: webhook to
drafted reply to human approval to filed response, with a test suite
across the whole path. The others follow the same architecture. Where
they are earlier in that journey, we say roadmap and mean it.

A dispute arrives as a structured webhook with a reason code. There is
no model call in detection: a reason code is a fact, and facts get a
lookup, not an inference. From there the pipeline runs perceive, draft,
check, gate, file, and the model appears in exactly 4 calls: confirm
the claim, extract it, draft the reply, judge the draft. Everything
around those calls is deterministic code.

## Inside the context engine

The interesting part is not the model. It is what the model reads.
3 tiers, all Postgres, every table tenant-isolated by row-level
security.

**1. Policy.** Per-merchant configuration: which reason codes qualify,
caps, thresholds, when to ask. Changing a merchant's behaviour is a row
update, not a redeploy.

**2. Evidence.** The facts a reply may cite: delivery scans, courier
agreements, refund policies. Retrieval is exact match by reason code.
No RAG, no embeddings, deliberately. A dispute reply cites the invoice
for this order, and there is no version of "semantically similar
invoice" that a bank should ever receive. The draft may only cite
evidence ids that exist in the vault. A citation outside it fails the
check and never reaches the merchant.

**3. Learned preference memory.** The closed loop. When a merchant
edits a drafted reply before approving it, the engine diffs what
changed, writes the lesson as a memory note, and the next dispute for
that merchant reads it back before drafting. Reword "refund will be
processed" into the bank's escalation format once, and the next draft
arrives already in that format. This is not a demo behaviour. It is a
single test in the suite:
`test_edit_runs_the_diff_writes_memory_and_next_draft_reads_it`.
That test is the "gets smarter from your edits" claim, executable.

## The trust model, mechanically

"You approve before anything sends" is the tagline. Here is the
machine underneath it.

1. 2 deterministic checks run first, with no model in either: banned
   phrases and promises, citation validity, link liveness, caps.
2. An LLM judge then scores the draft against the claim. Below
   threshold, it never surfaces.
3. What passes lands in front of a human on Slack. 3 buttons: approve,
   edit, dismiss. Nothing sends without the first 2.
4. Anything that fails, times out, or looks unusual escalates to an
   internal review channel. Escalations go to our team, never to the
   merchant's customer.
5. The ledger is append-only and every action carries an idempotency
   key scoped to the tenant. A retry cannot double-file a dispute. A
   gate decision is written exactly once.

The gate is not a compliance apology bolted onto an autonomous system.
It is where the memory tier gets its training data. Every edit teaches
the engine. The human in the loop is the learning loop.

## What this build has proven, and what it has not

The dispute engine described here is proven against simulated rails:
the full path runs end to end in the test suite, including the memory
loop, on the same Postgres schema that production uses. What it does
not have yet: a live bank webhook feeding it production disputes, and
self-serve connection of this specific agent. Merchants on the Relay
beta since May have been running the turnkey agents; the dispute
engine's numbers in this post come from its test suite, not from
projections. We would rather you know exactly which is which.

## Why Cashfree can build this

The context engine is only as good as the records under it. Relay runs
on Cashfree's own payments infrastructure and is grounded in the
merchant's actual transaction data: orders, settlements, disputes,
refunds. Merchant transaction data is not sent to external AI
providers. The agents arrive pre-trained on payment operations
patterns, which is why a turnkey agent is useful on day 1 instead of
after a month of configuration.

## Where this goes

Relay is free for all Cashfree merchants at launch. The aim is blunt:
take a merchant's payment operations from 60 hours a week to under 45
minutes of approvals. If you run an SMB on Cashfree, Relay is on your
dashboard today. Describe the job you would hire for. Approve what it
drafts. Keep the hours.
