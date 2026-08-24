# Relay, explained through one dispute

*How Relay runs payment operations for SMBs, told from the inside of
the hardest workflow it owns.*

The webhook lands on a Tuesday at 4:47 pm. Reason code RD. A buyer has
told their bank they were charged twice for an order of pet food, and
the bank has opened a dispute. From this moment a clock is running.
Answer with evidence before the deadline and the payment stays yours.
Miss it and the amount is debited, no appeal, case closed by default.

For the merchant, this email is item 41 on a day that already has 40.
The proof it needs is scattered: the delivery scan sits with the
courier, the invoice in the billing tool, the buyer's own "where is my
order" message in a WhatsApp thread. Gathering it takes an evening the
merchant does not have. So the reply goes out late, or thin, or not at
all. Banks do not grade effort.

Now multiply by everything else the payments day holds. Carts dropped
at checkout that a phone call would recover. Failed payments where the
decline code says "try UPI tomorrow morning" to anyone who can read
decline codes. COD orders shipping to pincodes that bounce 2 parcels
in 5. On Cashfree's data, 7 in 10 Indian SMBs do all of this by hand,
and the average SMB spends 60 hours a week doing it. A 5 person
company does not have a 6th person. The work happens badly, or the
founder does it at midnight, which is the same thing with worse sleep.

This is the job Relay was built to take. Not to flag. To finish.

## What Relay is

Relay is an AI teammate that runs payment operations for SMBs.
It reads
the merchant's own transaction records and completes the work: retries
the failed payment, calls the dropped cart, confirms the COD order,
files the dispute. A merchant sets it up by describing the outcome in
plain language. There is no workflow canvas and no node editor,
because the merchant we built this for will never open one.

It asks permission at exactly 2 moments: before a payment moves and
before a customer is contacted. Everything else it does quietly and
writes down.

One more thing it is not: a catalog. Every Relay agent is first-party
and runs on the same engine, the same ledger, the same gate. There is
no marketplace of rebadged vendors underneath, because agents that
share nothing can teach each other nothing.

Relay launched this week for all Cashfree merchants, free at launch,
after running in beta since May. That is the announcement. What
follows is the part a press release cannot hold: how the thing
actually works, shown through the 1 agent we have proven end to end.

## One agent, all the way down

Relay is a superagent: one teammate at the surface, specialist
agents underneath. We could describe all of them at the same shallow
depth. More useful to take Dispute Defender, the one that runs
webhook to filed response with a test suite across the entire path,
and cut it open. The rest share this architecture and are earlier on
the same road. Where a thing is roadmap, we will call it roadmap.

Back to Tuesday, 4:47 pm.

Detection is not AI. A dispute arrives as structured data with a
reason code, and a reason code is a fact. Facts get a lookup. The
engine classifies the dispute in deterministic code, checks the
merchant qualifies, and opens a run in an append-only ledger.

Then the pipeline: perceive, draft, check, gate, file. Across that
whole path the model is called exactly 4 times: confirm the claim,
extract it, draft the reply, judge the draft. Everything between those
4 calls is ordinary code that runs the same way every time. If the
phrase "agentic workflow" conjures a model freestyling through your
compliance process, this is the opposite. The model writes prose. Code
decides what happens.

Here is what that looks like in the engine's own trace, the same one
every run keeps forever:

    16:47:12  detection-agent    signal     bank webhook · dispute RD filed
    16:47:12  detection-agent    confirmed  charged twice for the same order
    16:47:13  eligibility-agent  qualified  all safety checks passed
    16:47:19  response-agent     drafted    2 citations
    16:47:21  compliance-agent   passed     checks + judge clean
    16:47:22  gate               surfaced   card sent to the merchant

10 seconds from webhook to a drafted reply citing the delivery scan
and the buyer's own message thread, sitting in front of a human with
3 buttons. Nothing has been sent. Nothing will be, until a person
presses 1 of the first 2.

## The context engine

The model is not the interesting part. What the model reads is.

Underneath every agent sit 3 tiers of context, all in Postgres, every
table isolated per merchant by row-level security.

**Policy.** What this merchant allows: which reason codes qualify,
caps, thresholds, when to ask a human. It is configuration in rows,
so changing a merchant's behaviour is an update, not a deploy.

**Evidence.** The facts a reply may cite: delivery proof, courier
agreements, the refund policy. Retrieval is exact match by reason
code. There is no RAG here, no embeddings, and that is a decision,
not a gap. RAG is the right tool when an agent answers from a help
centre, where the nearest article is usually good enough. Evidence
for a bank is not that kind of knowledge. A dispute reply cites the
invoice for this order. There is no useful notion of a semantically
similar invoice, and a bank should never receive one. The draft may only cite evidence ids that exist in
the vault; a citation from outside it fails a check and dies before
any human sees it.

**Learned preference memory.** The tier we are proudest of, because
it closes a loop most AI products leave open. When a merchant edits a
drafted reply before approving it, the engine diffs the draft against
the edit, writes what it learned as a memory note, and the next
dispute for that merchant reads the note back before drafting.

Concretely: a merchant once rewrote our generic settlement line to
quote the buyer's own bank reference number instead. One edit. Every
draft since arrives with the reference number already quoted. The
behaviour is pinned by a single test whose name is the whole story:

    test_edit_runs_the_diff_writes_memory_and_next_draft_reads_it

"It gets smarter from your edits" is a sentence every AI product says.
Ours compiles.

The memory is scoped per merchant and per concern. One brand's hard
won phrasing never leaks into another brand's drafts; the same
row-level security that isolates the ledger isolates the lessons.

## The trust machine

"Autonomous" is the favourite adjective in agent launches this year.
It is also the fastest way to lose the trust of anyone who runs a
regulated book. Relay's tagline is "you approve before anything
sends". Taglines are cheap too. Here is the machinery.

First, 2 deterministic checks with no model in either: banned phrases
and promises, citation validity, link liveness, caps. Then an LLM
judge scores the draft against the claim; below threshold it never
surfaces. What passes lands on Slack with approve, edit, dismiss.
Anything that fails, stalls past 24 hours, or looks unusual escalates
to an internal review channel, staffed by us, invisible to the
merchant's customer. And every action carries an idempotency key
scoped to the merchant, so a retry cannot double-file and a gate
decision writes exactly once. The ledger only appends. Nobody edits
history, including us.

Notice what the gate really is. Every edit a merchant makes there
feeds the memory tier. Most products treat approval as friction to be
engineered away; Relay treats it as the training signal. The human in
the loop is the learning loop, which is why the gate gets better at
staying out of your way the more you use it.

## The honest ledger

Beta since May means merchants have been running Relay's turnkey
agents on real operations. The dispute engine in this post is proven
against simulated rails: the full path, including the memory loop,
runs green in the suite on the same Postgres schema production uses.
What it does not have yet is a live bank webhook feeding it production
disputes, or self-serve connection of this specific agent. The numbers
here come from a test suite, not a projection. We would rather tell
you which is which than let a paragraph imply more than exists.

## Why this needed a payments company

An agent is only as good as the records under it. Relay runs on
Cashfree's own infrastructure and is grounded in the merchant's actual
transaction data: orders, settlements, refunds, disputes, the decline
codes and the delivery scans. Its agents arrive pre-trained on payment
operations patterns, which is why a turnkey agent is useful on its
first day instead of after a month of tuning. Merchant transaction
data is not sent to external AI providers.

A horizontal AI tool can draft you a very polite dispute reply. It has
never seen your order, cannot attach your proof, and will not file
anything. The draft was never the hard part.

## Keep the hours

The aim is blunt: from 60 hours a week of payment operations to under
45 minutes of approvals. If you run on Cashfree, Relay is on your
dashboard today. Describe the job you would have hired for. Approve
what it drafts. Edit it once, and watch the next draft arrive already
knowing.

Tuesday, 4:47 pm will keep happening. It just stops being yours.
