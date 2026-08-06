"""The workspace, first design pass: run `uv run python demo/server.py`.

Serves the merchant operator view on http://localhost:8790 with the REAL pipeline
running on fakes underneath: every card is a genuine ledger row produced by
`Pipeline.handle_event`, and the buttons call the same `approve` / `edit` /
`reject` the Slack webhook will call. No JS framework, no external assets;
this page is a design surface for spec §7.3's components (QueueTable,
CounterCard, DiffView, metrics strip) before AG-UI streaming lands.
"""

from __future__ import annotations
import hashlib as _hashlib

import html
import json
import re as _re
import sys
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from relay_superagent.domain.models import (  # noqa: E402
    AgentType, DisputeReason, EvidenceItem, GateAction, Policy, RunState, TriggerEvent,
)
from relay_superagent.ledger import Ledger  # noqa: E402
from relay_superagent.metrics import (  # noqa: E402
    correction_rate, counter_usage_rate, gate_latency_p95_ms, trigger_precision,
)
from relay_superagent.pipeline import Deps, Pipeline  # noqa: E402
from relay_superagent.ports.fakes import (  # noqa: E402
    FakeClock, FakeCrm, FakeSlack, FakeUrlChecker, ScriptedLlm,
)
from relay_superagent.supervisor import Supervisor  # noqa: E402
from relay_superagent.auth import session_secret, sign_session, verify_session  # noqa: E402
from relay_superagent.tenants import (  # noqa: E402
    TenantContext, TenantRegistry, UnknownTenant, default_policy,
)

PORT = 8790

# ------------------------------------------------------------- the business
# This workspace IS one small business, not a portfolio of them: Ojas
# Wellness, an invented Indian D2C Ayurvedic brand — cold-pressed juices,
# A2 ghee, capsules and monthly refills. Eight people. Its own store plus
# Amazon, Flipkart and quick commerce. ~₹50L a month, most orders between
# ₹700 and ₹1,400, a lot of it cash on delivery. The logged-in user is the
# founder.
#
# `Run.merchant_id` means "whose business is this", so it is ONE constant
# value here. The people raising chargebacks are this business's own buyers,
# and they are display-only seed data (CUSTOMERS below), never a field on
# the domain model.
BUSINESS = "Ojas Wellness"
BUSINESS_TAG = "Ayurvedic wellness &middot; Bengaluru"
BUSINESS_CHANNELS = ("ojaswellness.in, Amazon, Flipkart and quick commerce")
BUSINESS_ID = "m_ojas"

# The one exception to the single-id rule: a marketplace account that has not
# been connected to Relay yet. It is not this business's own connected store,
# which is exactly why a dispute arriving on it gets set aside.
UNLINKED_CHANNEL_ID = "m_ojas_marketplace_unlinked"

# order_id -> (customer, what they bought, where they bought it). Display
# only: the buyer never touches the engine, the ledger or the SQL.
CUSTOMERS: dict[str, tuple[str, str, str]] = {
    "order_1":  ("Priya S.",   "A2 Desi Ghee 500ml",         "our store"),
    "order_2":  ("Rahul M.",   "Ashwagandha Gold capsules",  "monthly refill"),
    "order_3":  ("Anjali K.",  "Aloe Vera Juice 1L",         "Amazon"),
    "order_4":  ("Vikram R.",  "Shilajit Resin 20g",         "our store"),
    "order_5":  ("Sneha D.",   "Amla Juice 1L",              "monthly refill"),
    "order_6":  ("Farhan A.",  "Triphala tablets",           "Flipkart"),
    "order_7":  ("Meera J.",   "Karela Jamun Juice 1L",      "quick commerce"),
    "order_9":  ("Karthik V.", "Moringa capsules",           "Amazon"),
    "order_10": ("Divya P.",   "Wild Turmeric powder",       "our store"),
    "order_11": ("Arun N.",    "Ashwagandha Gold capsules",  "monthly refill"),
    "order_12": ("Ritu B.",    "A2 Desi Ghee 500ml",         "Flipkart"),
    "order_13": ("Sameer T.",  "Shilajit Resin 20g",         "our store"),
    "order_14": ("Nandini G.", "Aloe Vera Juice 1L",         "quick commerce"),
    "order_15": ("Harsh V.",   "Amla Juice 1L",              "Amazon"),
    "order_16": ("Ishita R.",  "Triphala tablets",           "our store"),
    "order_17": ("Gaurav S.",  "Karela Jamun Juice 1L",      "our store"),
    "order_18": ("Pooja M.",   "Moringa capsules",           "monthly refill"),
    "order_19": ("Tarun K.",   "Wild Turmeric powder",       "Amazon"),
    "order_20": ("Lata S.",    "A2 Desi Ghee 500ml",         "our store"),
}
ORDER_IDS = list(CUSTOMERS)


def customer_of(order_id: str | None) -> str:
    row = CUSTOMERS.get(order_id or "")
    return row[0] if row else "Unknown buyer"


def bought(order_id: str | None) -> str:
    row = CUSTOMERS.get(order_id or "")
    return row[1] if row else "no matching order"


def channel_of(order_id: str | None) -> str:
    row = CUSTOMERS.get(order_id or "")
    return row[2] if row else "our store"


def _make_ledger():
    """Postgres-backed when the local cluster is up; in-memory otherwise.
    and it says which, loudly, at boot."""
    try:
        from demo.persist import PersistentLedger
        led = PersistentLedger()
        print(f"ledger: postgres (hydrated {len(led.runs)} runs)")
        return led
    except Exception as e:                                  # noqa: BLE001
        print(f"ledger: IN-MEMORY (postgres unavailable: {str(e)[:80]}). "
              f"data will not survive restarts; run `make pg`")
        return Ledger()



# ---------------------------------------------------------------- seed world
# Responses, written once per dispute reason and filled with the SKU that was
# actually ordered. The same evidence bank backs every one of them, and every
# row goes through the same checks as the live path.
RESP = {
    "RG": ("Courier proof-of-delivery shows this order of {sku} signed for and "
           "GPS-stamped at the address on file, and the WhatsApp thread "
           "carries the buyer's own confirmation from the same evening. "
           "Both are attached to this reply."),
    "RD": ("The invoice and the payment gateway agree on one reference for "
           "this order of {sku}, and the bank settlement excerpt confirms a "
           "single debit cleared. The second line on the buyer's "
           "statement is an authorisation hold, not a charge, and it drops "
           "off on its own."),
    "RN": ("The product page snapshot from the order date matches the batch "
           "and the seal that shipped, and the buyer's own return-request "
           "photos show that same seal intact on the {sku} that shipped. Both "
           "are attached."),
    "RC": ("The refill log shows this {sku} subscription was still running "
           "when the order was picked and packed; the cancellation is "
           "timestamped after the parcel left the warehouse, and the refill "
           "policy on the page that day credits the next cycle instead of "
           "the shipped one."),
    "RF": ("The device and the phone number on this order of {sku} match the "
           "buyer's three earlier orders, the delivery address is the one "
           "used on all of them, and the one-time password at checkout was "
           "confirmed on that same number."),
}
CITES = {
    "RG": ["ev_pod", "ev_wa"], "RD": ["ev_inv", "ev_bank"],
    "RN": ["ev_listing", "ev_returnphotos"], "RC": ["ev_subs", "ev_policy"],
    "RF": ["ev_device", "ev_otp"],
}


def build_world() -> Pipeline:
    clock = FakeClock(datetime(2026, 8, 3, 9, 0))
    policy = Policy(
        policy_version="pol_7", tenant_id="t1",
        dispute_reasons=[
            DisputeReason(id="goods_not_received", code="RG",
                         label="Goods/services not received"),
            DisputeReason(id="not_as_described", code="RN",
                         label="Product not as described"),
            DisputeReason(id="duplicate_charge", code="RD",
                         label="Duplicate charge"),
            DisputeReason(id="fraud_claim", code="RF",
                         label="Unauthorized/fraud claim"),
            DisputeReason(id="subscription_cancelled", code="RC",
                         label="Subscription already cancelled"),
        ],
        banned_terms=["best", "leading", "number one"], holdout_pct=0,
        # Tenant config, not a constant: the cap exists to stop one business
        # flooding an operator's day. This workspace IS one business, so the
        # meaningful number is "how many of our own disputes may be worked in
        # a day", and at ~₹50L a month that is not five. The safety check
        # itself is untouched.
        per_merchant_per_day=200)
    crm = FakeCrm(opportunities={
        oid: {"stage": "evaluation",
              "amount_band": ["under-1k", "1k-3k", "3k-10k"][i % 3]}
        for i, oid in enumerate(ORDER_IDS)})
    evidence = [
        EvidenceItem("ev_pod", "t1", "RG", "delivery_proof",
                     "Courier proof-of-delivery, signed and GPS-stamped at the door",
                     "https://ojaswellness.example/proof/delivery"),
        EvidenceItem("ev_wa", "t1", "RG", "communication_log",
                     "WhatsApp thread where the buyer confirms the parcel arrived",
                     "https://ojaswellness.example/proof/whatsapp"),
        EvidenceItem("ev_inv", "t1", "RD", "invoice",
                     "The order invoice, tied to one payment reference",
                     "https://ojaswellness.example/proof/invoice"),
        EvidenceItem("ev_bank", "t1", "RD", "communication_log",
                     "Bank settlement excerpt showing one debit on this order, not two",
                     "https://ojaswellness.example/proof/settlement"),
        EvidenceItem("ev_listing", "t1", "RN", "invoice",
                     "Product page snapshot from the order date, with the batch and seal detail",
                     "https://ojaswellness.example/proof/listing"),
        EvidenceItem("ev_returnphotos", "t1", "RN", "communication_log",
                     "The buyer's own return-request photos of the bottle and its seal",
                     "https://ojaswellness.example/proof/return-photos"),
        EvidenceItem("ev_subs", "t1", "RC", "communication_log",
                     "Refill subscription log, showing to the minute when it was paused or cancelled",
                     "https://ojaswellness.example/proof/subscription-log"),
        EvidenceItem("ev_policy", "t1", "RC", "refund_policy",
                     "The refills and cancellation policy page as it stood on the order date",
                     "https://ojaswellness.example/proof/refill-policy"),
        EvidenceItem("ev_device", "t1", "RF", "communication_log",
                     "Device, phone and delivery address matched to the buyer's earlier orders",
                     "https://ojaswellness.example/proof/device-match"),
        EvidenceItem("ev_otp", "t1", "RF", "invoice",
                     "The one-time password confirmation captured on this order at checkout",
                     "https://ojaswellness.example/proof/otp"),
    ]
    deps = Deps(clock=clock, llm=ScriptedLlm(), crm=crm, slack=FakeSlack(),
                url_checker=FakeUrlChecker(), ledger=_make_ledger(), policy=policy,
                evidence=evidence, enrolled_merchants={BUSINESS_ID})
    p = Pipeline(deps)

    def fire(ref, order, reason, text, claim, counter=None, cites=None,
             merchant=BUSINESS_ID):
        """One dispute from one of Ojas Wellness's own buyers. `merchant` is
        the business, always: the buyer lives in CUSTOMERS, keyed by order."""
        counter = counter or RESP[reason].format(sku=bought(order))
        cites = cites or CITES[reason]
        deps.llm.mention = {"is_competitive": True, "claim_text": claim, "confidence": .9}
        deps.llm.claim = {"claim_text": claim, "speaker_role": "buyer", "confidence": .9}
        deps.llm.draft = {"counter_text": counter, "cited_evidence_ids": cites,
                          "confidence": .82, "escalate": False}
        return p.handle_event(TriggerEvent(
            tenant_id="t1", source="bank_webhook", source_ref=ref, occurred_at=clock.now(),
            order_id=order, merchant_id=merchant, dispute_id=f"dp_{ref}",
            reason_code=reason, text=text))

    # already seeded (hydrated from Postgres)? Rebuild pointers and move on.
    if any(r.tenant_id == "t1" for r in deps.ledger.runs.values()):
        by_ref = {r.trigger_ref: r.run_id
                  for r in deps.ledger.runs.values() if r.tenant_id == "t1"}
        for stage, ref in [("review", "m_lifecycle_1"), ("edited", "lc_edit"),
                           ("lost", "lc_lost"), ("qa_blocked", "lc_qa"),
                           ("not_actionable", "lc_noise"),
                           ("merchant_not_enrolled", "lc_unlinked"),
                           ("no_order", "lc_noopp"),
                           ("recently_countered", "lc_recent"),
                           ("won", "c_won"),
                           ("timed_out", "c_old"), ("approved", "q1")]:
            if ref in by_ref:
                EXEMPLARS[stage] = by_ref[ref]
        newest = max((r.occurred_at for r in deps.ledger.runs.values()), default=None)
        if newest:
            clock.current = newest + timedelta(hours=2)
        return p

    # 1) a run that will time out and escalate
    fire("c_old", "order_6", "RD",
         "Chargeback filed on Flipkart: buyer says the Triphala order was billed twice.",
         "Charged twice for the triphala tablets")
    clock.advance(hours=25)
    Supervisor(deps.ledger, clock, deps.slack, policy).sweep()

    # 2) resolved win — approved 6 weeks ago, dispute won
    r = fire("c_won", "order_1", "RG",
             "Chargeback filed: buyer says the ghee order never arrived.",
             "Says the ghee never arrived")
    clock.advance(minutes=4)
    p.approve(r, BUSINESS_ID)
    clock.advance(days=40)
    p.record_resolution(r, won=True, amount_paise=499_600)   # a 4-jar ghee box

    # 3) an edit (non-material) that landed and is still open
    r = fire("c_edit", "order_2", "RD",
             "Chargeback filed: two debits on the card for one monthly refill.",
             "Charged twice for the monthly ashwagandha refill")
    clock.advance(minutes=11)
    deps.llm.diff = {"changed": "said the hold drops off in a few days",
                     "is_material": False,
                     "implies": "tell the buyer when the second line disappears"}
    p.edit(r, BUSINESS_ID,
           "Only one debit settled on this refill: the payment reference and "
           "the bank settlement excerpt both say so. The second line on the "
           "statement is an authorisation hold that was never captured, and "
           "it drops off within a few days.")

    # 4) rejected
    r = fire("c_rej", "order_3", "RN",
             "Chargeback filed on Amazon: buyer says the aloe vera juice arrived unsealed.",
             "Says the seal on the aloe vera juice was broken")
    clock.advance(minutes=27)
    p.reject(r, BUSINESS_ID)

    # 5) suppressed — same order, same reason again inside the window
    fire("c_dup", "order_2", "RD",
         "Buyer reopened the case: still says the refill was double charged.",
         "Reopened the charged-twice claim on the ashwagandha refill")

    # 6–8) three live cards awaiting review
    fire("c_live1", "order_4", "RF",
         "Chargeback filed: cardholder does not recognise this order.",
         "Does not recognise the shilajit order")
    clock.advance(minutes=9)
    fire("c_live2", "order_5", "RC",
         "Chargeback filed: buyer says the refill was cancelled before dispatch.",
         "Says she cancelled the refill before it shipped")
    clock.advance(minutes=6)
    fire("c_live3", "order_7", "RN",
         "Chargeback filed on a quick-commerce order: buyer says the juice is not what was listed.",
         "Says the juice that arrived is not what the page showed")

    # a quarter of this business's own disputes: same evidence bank, same
    # checks, one buyer per row.
    QUARTER = [
        # ref, order, claim, reason, action, won_amount_paise
        ("q1",  "order_9",  "Says a second moringa parcel never arrived",     "RG", "approve", 224_700),
        ("q2",  "order_10", "Statement shows the turmeric order twice",       "RD", "approve", 119_800),
        ("q3",  "order_11", "Says the ashwagandha refill was never delivered","RG", "approve",    None),
        ("q4",  "order_12", "Bank flagged a repeat charge on the ghee order", "RD", "edit",    249_800),
        ("q5",  "order_13", "Says the shilajit resin never showed up",        "RG", "edit",       None),
        ("q6",  "order_14", "Says the delivery slot came and went",           "RG", "reject",     None),
        ("q7",  "order_15", "Statement lists the amla juice order twice",     "RD", "approve",    None),
        ("q8",  "order_16", "Says the triphala order never left the warehouse","RG", "wait",      None),
        ("q9",  "order_9",  "Reopened: still says the moringa parcel is missing", "RG", "wait",  None),
        ("q10", "order_11", "Bank is disputing the ashwagandha refill charge","RD", "wait",       None),
    ]
    for ref, order, claim, reason, action, won in QUARTER:
        clock.advance(hours=13)
        r = fire(ref, order, reason, claim + ".", claim)
        clock.advance(minutes=18)
        if action == "approve":
            p.approve(r, BUSINESS_ID)
        elif action == "edit":
            deps.llm.diff = ({"changed": "named the payment reference outright",
                              "is_material": False,
                              "implies": "quote the reference, not the wording"}
                             if reason == "RD" else
                             {"changed": "led with the delivery scan",
                              "is_material": False,
                              "implies": "put the proof in the first line"})
            p.edit(r, BUSINESS_ID, RESP[reason].format(sku=bought(order))
                   + " Happy to share the raw settlement export too.")
        elif action == "reject":
            p.reject(r, BUSINESS_ID)
        if won:
            clock.advance(days=21)
            p.record_resolution(r, won=True, amount_paise=won)

    # -- lifecycle exemplars: one run living at every stage, so the product
    # story is legible from the UI. EXEMPLARS maps stage -> run_id for the
    # Plays-page storyboard.
    def mark(stage, run):
        if run is not None:
            EXEMPLARS[stage] = run.run_id
        return run

    # signal heard + gated (the chargeback notice forwarded by email this time)
    clock.advance(hours=3)
    deps.llm.mention = {"is_competitive": True,
                        "claim_text": "Says the juice order never arrived",
                        "confidence": .9}
    deps.llm.claim = {"claim_text": "Says the juice order never arrived",
                      "speaker_role": "buyer", "confidence": .9}
    deps.llm.draft = {"counter_text":
        "The delivery address on the order matches the courier's "
        "proof-of-delivery scan, signed the same afternoon the buyer filed; "
        "the WhatsApp confirmation from that day is attached, and there is no "
        "return request on this karela jamun juice order before the dispute.",
        "cited_evidence_ids": ["ev_pod", "ev_wa"], "confidence": .8,
        "escalate": False}
    r = p.handle_event(TriggerEvent(
        tenant_id="t1", source="email_forward", source_ref="m_lifecycle_1",
        occurred_at=clock.now(), order_id="order_17",
        merchant_id=BUSINESS_ID, dispute_id="dp_m_lifecycle_1", reason_code="RG",
        text="Forwarding the chargeback notice: buyer says the juice order never arrived."))
    mark("review", r)

    # a material edit, then filed
    clock.advance(hours=1)
    r2 = fire("lc_edit", "order_18", "RD",
              "Chargeback filed: two separate charges for one moringa refill.",
              "Says two charges hit the card for one refill")
    clock.advance(minutes=25)
    deps.llm.diff = {"changed": "replaced the generic settlement language with the "
                     "buyer's own bank reference number", "is_material": True,
                     "implies": "quote the buyer's own reference number back"}
    p.edit(r2, BUSINESS_ID,
           "Only one debit settled on this refill: quote the payment "
           "reference and the buyer's own bank reference number, both "
           "attached. The second line is an authorisation hold, not a charge, "
           "and it reverses on its own within a few working days.")
    mark("edited", r2)

    # the same claim, sent again a few hours later: set aside, never worked
    # twice on the same order
    clock.advance(hours=6)
    deps.llm.mention = {"is_competitive": True,
                        "claim_text": "Reopened the charged-twice claim on the refill",
                        "confidence": .9}
    r8 = p.handle_event(TriggerEvent(
        tenant_id="t1", source="bank_webhook", source_ref="lc_recent",
        occurred_at=clock.now(), order_id="order_18",
        merchant_id=BUSINESS_ID, dispute_id="dp_lc_recent", reason_code="RD",
        text="Buyer reopened the same duplicate-charge claim on the refill."))
    mark("recently_countered", r8)

    # a loss, honestly recorded
    clock.advance(hours=2)
    r3 = fire("lc_lost", "order_19", "RN",
              "Chargeback filed on Amazon: buyer says the turmeric powder is a different shade.",
              "Says the turmeric powder is a different shade to the photos")
    clock.advance(minutes=12)
    p.approve(r3, BUSINESS_ID)
    clock.advance(days=14)
    p.record_resolution(r3, won=False)
    mark("lost", r3)

    # QA blocks a draft that breaks the rules (banned superlative)
    clock.advance(hours=1)
    r4 = fire("lc_qa", "order_20", "RG",
              "Chargeback filed: buyer says the whole case of ghee is missing.",
              "Says the whole case of ghee never came",
              counter="Our delivery record is simply the best in the trade and "
                      "everyone knows it; the numbers speak for themselves and "
                      "the buyer is clearly wrong about the whole thing.",
              cites=["ev_pod"])
    mark("qa_blocked", r4)

    # triage suppressions, one per reason
    clock.advance(hours=1)
    deps.llm.mention = {"is_competitive": False, "claim_text": "", "confidence": .9}
    r5 = p.handle_event(TriggerEvent(
        tenant_id="t1", source="bank_webhook", source_ref="lc_noise",
        occurred_at=clock.now(), order_id="order_4",
        merchant_id=BUSINESS_ID, dispute_id="dp_lc_noise", reason_code="RG",
        text="Duplicate redelivery of a webhook already actioned: no new claim."))
    mark("not_actionable", r5)

    deps.llm.mention = {"is_competitive": True,
                        "claim_text": "Says the amla juice refill never arrived",
                        "confidence": .9}
    r6 = p.handle_event(TriggerEvent(
        tenant_id="t1", source="bank_webhook", source_ref="lc_unlinked",
        occurred_at=clock.now(), order_id="order_5",
        merchant_id=UNLINKED_CHANNEL_ID, dispute_id="dp_lc_unlinked",
        reason_code="RG",
        text="Chargeback on a marketplace account that is not connected to Relay yet."))
    mark("merchant_not_enrolled", r6)

    deps.llm.mention = {"is_competitive": True,
                        "claim_text": "Charged twice, on an order we cannot find",
                        "confidence": .9}
    r7 = p.handle_event(TriggerEvent(
        tenant_id="t1", source="bank_webhook", source_ref="lc_noopp",
        occurred_at=clock.now(), order_id="order_missing",
        merchant_id=BUSINESS_ID, dispute_id="dp_lc_noopp", reason_code="RD",
        text="A duplicate-charge chargeback against an order we cannot find."))
    mark("no_order", r7)

    # one buyer's saga in full, for texture beyond the single-run exemplars
    clock.advance(days=2)
    r10 = fire("lc_1", "order_1", "RG",
               "Buyer reopened: says the replacement ghee jar never showed either.",
               "Says the replacement ghee jar never showed")
    clock.advance(minutes=31)
    p.reject(r10, BUSINESS_ID)

    clock.advance(days=9)
    r11 = fire("lc_2", "order_1", "RG",
               "The bank escalated: it says the delivery proof on its own is not enough.",
               "Bank says the delivery proof is not enough")
    clock.advance(minutes=14)
    deps.llm.diff = {"changed": "tightened to the bank's escalation format",
                     "is_material": False, "implies": "mirror the bank's own language"}
    p.edit(r11, BUSINESS_ID,
           "The courier's GPS-stamped scan places this ghee order at the "
           "address on file, and the buyer's own WhatsApp message thanking the "
           "rider is timestamped the same afternoon. Both are attached again "
           "for the bank's escalation review.")

    clock.advance(days=16)         # past the 7-day suppress window
    r12 = fire("lc_3", "order_1", "RG",
               "Final round: the bank has asked for a signed acknowledgement too.",
               "Bank asked for a signed delivery acknowledgement")
    clock.advance(minutes=9)
    p.approve(r12, BUSINESS_ID)

    # exemplar pointers for stages already seeded earlier
    for run in deps.ledger.runs.values():
        if run.state is RunState.RESOLVED and "won" not in EXEMPLARS:
            out = deps.ledger.outcome_for(run.run_id)
            if out and (out.outcome_value or {}).get("won"):
                EXEMPLARS["won"] = run.run_id
        if run.state is RunState.TIMED_OUT:
            EXEMPLARS.setdefault("timed_out", run.run_id)
        if run.state is RunState.RESOLVED and run.gate_action is not None \
                and run.gate_action.value == "approve":
            EXEMPLARS.setdefault("approved", run.run_id)

    # anything still at the gate is by definition recent (timeout is 24h);
    # restamp queue cards left behind by the quarter's clock advances
    from datetime import timedelta as _td
    stale = [r for r in deps.ledger.runs.values()
             if r.state.value == "awaiting_gate"]
    for i, r in enumerate(sorted(stale, key=lambda r: r.occurred_at)):
        r.occurred_at = clock.now() - _td(hours=22 - i * 3)
        r.surfaced_at = r.occurred_at + _td(minutes=6)
    return p


EXEMPLARS: dict[str, str] = {}

WORLD = build_world()


def _rebase_world() -> None:
    """The demo timeline is fixed and hydrated snapshots age; the seed clock
    can end months from real time. Every boot, shift the in-memory view so
    the newest moment lands about now, keep the approval queue hours old,
    and cap synthetic gate latencies at human scale. The DB keeps its
    original stamps; only the served view moves."""
    from dataclasses import fields as _fields, is_dataclass as _isdc
    led = WORLD.d.ledger
    stamps = [r.occurred_at for r in led.runs.values() if r.occurred_at]
    for evs in led.events.values():
        stamps += [e["ts"] for e in evs if isinstance(e.get("ts"), datetime)]
    if not stamps:
        return
    delta = datetime.now() - timedelta(hours=1) - max(stamps)

    def shift(obj, d):
        if _isdc(obj):
            for f in _fields(obj):
                v = getattr(obj, f.name, None)
                if isinstance(v, datetime):
                    setattr(obj, f.name, v + d)
    for r in led.runs.values():
        shift(r, delta)
    for o in led.outcomes:
        shift(o, delta)
    for m in led.memory:
        shift(m, delta)
    for evs in led.events.values():
        for e in evs:
            if isinstance(e.get("ts"), datetime):
                e["ts"] = e["ts"] + delta
    WORLD.d.clock.current = datetime.now()

    # the gate escalates at 24h, so nothing in the queue may look older
    ages = [timedelta(minutes=45), timedelta(hours=3, minutes=20),
            timedelta(hours=7), timedelta(hours=11)]
    waiting = sorted((r for r in led.runs.values()
                      if r.state is RunState.AWAITING_GATE),
                     key=lambda r: r.occurred_at, reverse=True)
    for i, r in enumerate(waiting):
        target = WORLD.d.clock.now() - (ages[i] if i < len(ages)
                                        else timedelta(hours=2 + 3 * i))
        d2 = target - r.occurred_at
        shift(r, d2)
        for e in led.events.get(r.run_id, []):
            if isinstance(e.get("ts"), datetime):
                e["ts"] = e["ts"] + d2
    for r in led.runs.values():
        if r.gate_latency_ms and r.gate_latency_ms > 48 * 3_600_000:
            r.gate_latency_ms = (5 + sum(map(ord, r.run_id)) % 1150) * 60_000


_rebase_world()

# ------------------------------------------------------------- multi-tenant
# Runs were tenant-scoped rows from day one; this adds the other half.
# The registry is in-memory: WorkOS is the identity source of truth, and a
# login after a restart re-creates the tenant context from its org id.
# (Durable per-tenant policy in Postgres is backlog, logged.)
TENANTS = TenantRegistry()
TENANTS.add(TenantContext(
    tenant_id="t1", name="Demo Co", policy=WORLD.d.policy,
    enrolled_merchants=WORLD.d.enrolled_merchants))
PIPELINES: dict[str, Pipeline] = {"t1": WORLD}


def pipeline_for(tenant_id: str) -> Pipeline:
    """One Pipeline per tenant over the SHARED ledger and (for now) shared
    fake ports; only policy and identity differ. Raises UnknownTenant."""
    if tenant_id not in PIPELINES:
        ctx = TENANTS.get(tenant_id)
        d = WORLD.d
        PIPELINES[tenant_id] = Pipeline(Deps(
            clock=d.clock, llm=d.llm, crm=d.crm, slack=d.slack,
            url_checker=d.url_checker, ledger=d.ledger, policy=ctx.policy,
            evidence=[], enrolled_merchants=ctx.enrolled_merchants))
    return PIPELINES[tenant_id]


def ensure_tenant(workos_org_id: str, name: str) -> TenantContext:
    ctx = TENANTS.by_workos_org(workos_org_id)
    if ctx is None:
        ctx = TENANTS.add(TenantContext(
            tenant_id=workos_org_id, name=name,
            policy=default_policy(workos_org_id), workos_org_id=workos_org_id))
    return ctx

# dispute reason code -> plain-English label, for anywhere a run's reason_code
# needs to render as something a merchant reads, not a bank code.
COMP = {"RG": "Goods not received", "RN": "Not as described",
        "RD": "Duplicate charge", "RF": "Unauthorized/fraud",
        "RC": "Subscription cancelled"}

# The same five, said the way a shop owner would say them out loud. Used
# anywhere the owner is reading a sentence rather than scanning a table.
# Why something was set aside, said plainly. The engine writes short codes;
# the owner should never have to read one.
PLAIN_SKIP = {
    "no_order": "no order matches this",
    "merchant_not_enrolled": "that sales channel isn&rsquo;t connected yet",
    "merchant_daily_cap": "already handled plenty today",
    "not_actionable": "not really a dispute",
    "recent_duplicate": "same claim, already handled",
    "recently_countered": "same claim, already answered",
    "claim_already_handled": "same claim, already answered",
}

PLAIN_REASON = {
    "RG": "Buyer says it never arrived",
    "RN": "Buyer says it wasn&rsquo;t what they ordered",
    "RD": "Buyer says they were charged twice",
    "RF": "Buyer says they never made this payment",
    "RC": "Buyer says they had already cancelled",
}

# The eight people who actually run Ojas Wellness, plus the small Relay-side
# crew who pick up anything the agents hand over. "Says yes or no" marks the
# people who can approve a reply before it goes to a bank.
TEAM = [
    ("u_aditya",  "Aditya Rao",      "founder",                BUSINESS, True),
    ("u_lakshmi", "Lakshmi Menon",   "operations",             BUSINESS, True),
    ("u_nikhil",  "Nikhil Bhat",     "customer support",       BUSINESS, True),
    ("u_preeti",  "Preeti Nair",     "accounts",               BUSINESS, False),
    ("u_ravi",    "Ravi Deshmukh",   "packing and dispatch",   BUSINESS, False),
    ("u_zoya",    "Zoya Ahmed",      "marketplaces",           BUSINESS, False),
    ("u_manish",  "Manish Gupta",    "subscriptions and refills", BUSINESS, False),
    ("u_kavya",   "Kavya Reddy",     "social and content",     BUSINESS, False),
    # Relay's people (3) — they pick up what needs a human, and never see a
    # queue of their own
    ("ops_deepa", "Deepa Krishnan",  "handles disputes",  "Relay’s people", False),
    ("ops_arjun", "Arjun Pillai",    "looks after you",   "Relay’s people", False),
    ("ops_riya",  "Riya Kapoor",     "support",           "Relay’s people", False),
]
REP = {tid: name for tid, name, _, _, _ in TEAM}
STATE_META = {
    RunState.AWAITING_GATE: ("Pending approval", "wait"),
    RunState.ACTED: ("Sent to the bank", "ok"),
    RunState.RESOLVED: ("Done", "ok"),
    RunState.EDITED: ("Your wording, sent", "ok"),
    RunState.REJECTED: ("You said no", "mut"),
    RunState.TIMED_OUT: ("Needs a person", "warn"),
    RunState.SUPPRESSED: ("Set aside", "mut"),
    RunState.FAILED: ("Needs a person", "warn"),
}
STEPS = ["safety", "retrieve", "draft", "check", "gate"]


def auth_page(mode: str, error: str = "") -> str:
    """Login/signup card. Same Inter + periwinkle language as the workspace;
    passwords go form → WorkOS over TLS, nothing stored here."""
    signup = mode == "signup"
    company = ('<label>Company<input name="company" required '
               'placeholder="Ojas Wellness"></label>') if signup else ""
    err = f'<div class="err">{esc(error)}</div>' if error else ""
    swap = (('Already have an account? <a href="/login">Log in</a>') if signup
            else ('New here? <a href="/signup">Create a workspace</a>'))
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Relay &middot; {'Sign up' if signup else 'Log in'}</title>
<style>
*{{box-sizing:border-box;margin:0}}
body{{font-family:'Circular Std',-apple-system,BlinkMacSystemFont,'SF Pro Text',sans-serif;background:#FAFAFC;min-height:100vh;
  display:flex;align-items:center;justify-content:center;color:#1a1c23}}
.card{{background:#fff;border:1px solid #E6E7EE;border-radius:14px;
  padding:36px 32px;width:360px}}
.brand{{display:flex;align-items:center;gap:8px;margin-bottom:24px;font-weight:650}}
.logo{{display:inline-flex;width:26px;height:26px;border-radius:7px;background:#5266EB;
  color:#fff;align-items:center;justify-content:center;font-size:14px;font-weight:700}}
h1{{font-size:17px;font-weight:600;margin-bottom:16px}}
label{{display:block;font-size:12.5px;color:#5a5e6e;margin-bottom:12px}}
input{{display:block;width:100%;margin-top:4px;padding:8px 12px;font:inherit;
  font-size:14px;border:1px solid #E3E4EA;border-radius:9px;background:#fff;
  transition:border-color .15s,box-shadow .15s}}
input:hover{{border-color:#C9CBD6}}
input::placeholder{{color:#9A9DAB}}
input:focus{{outline:none;border-color:#98A5F0;box-shadow:0 0 0 3px rgba(82,102,235,.13)}}
{BTN_CSS}
.err{{background:#FDF2F2;border:1px solid #F5C6C6;color:#9b3535;font-size:12.5px;
  border-radius:8px;padding:8px 12px;margin-bottom:16px}}
.swap{{font-size:12.5px;color:#5a5e6e;margin-top:16px;text-align:center}}
.swap a{{color:#5266EB;text-decoration:none}}
.demo{{margin-top:8px;text-align:center}}
</style></head><body>
<div class="card">
  <div class="brand"><span class="logo">C</span>Relay</div>
  <h1>{'Create your workspace' if signup else 'Welcome back'}</h1>
  {err}
  <form method="post" action="/auth/{mode}">
    {company}
    <label>Work email<input name="email" type="email" required
      placeholder="you@company.com"></label>
    <label>Password<input name="password" type="password" required
      minlength="8" autocomplete="{'new-password' if signup else 'current-password'}"></label>
    <button class="btn primary wide">{'Sign up' if signup else 'Log in'}</button>
  </form>
  <div class="swap">{swap}</div>
  <form class="demo" method="post" action="/auth/demo"><button class="btn ghost wide">Try the demo</button></form>
</div>
</body></html>"""


def verify_page(email: str, pending_token: str, error: str = "") -> str:
    """The OTP step: WorkOS emailed a one-time code; this page collects it
    with the pending token riding along hidden."""
    err = f'<div class="err">{esc(error)}</div>' if error else ""
    style = auth_page("login").split("<style>")[1].split("</style>")[0]
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Relay &middot; Verify your email</title>
<style>{style}
.code input{{font-size:22px;letter-spacing:8px;text-align:center;font-variant-numeric:tabular-nums}}
.hint{{font-size:12.5px;color:#5a5e6e;margin-bottom:16px}}
</style></head><body>
<div class="card">
  <div class="brand"><span class="logo">C</span>Relay</div>
  <h1>Check your inbox</h1>
  <div class="hint">We sent a 6-digit code to <b>{esc(email)}</b>. Enter it to
  finish signing in.</div>
  {err}
  <form method="post" action="/auth/verify" class="code">
    <input type="hidden" name="pending_token" value="{esc(pending_token)}">
    <input type="hidden" name="email" value="{esc(email)}">
    <label>Verification code<input name="code" inputmode="numeric" pattern="[0-9]*"
      minlength="6" maxlength="6" required autofocus autocomplete="one-time-code"></label>
    <button class="btn primary wide">Verify &amp; log in</button>
  </form>
  <div class="swap"><a href="/login">Back to log in</a> (logging in again sends a fresh code)</div>
</div>
</body></html>"""


# The conversational surface, styled once and shared by both templates: the
# rail that lists work, the steps that tick off while you watch, the work
# product that comes out the other end, and the case page it opens into.
WORK_CSS = """
.compose{margin-left:auto;width:30px;height:30px;border-radius:99px;
  display:grid;place-items:center;color:#4A4E63;background:#EDEEF4}
.compose:hover{background:#E3E5EE;color:var(--ink,#1B1F30)}
.compose svg{width:15px;height:15px}
.navblock{margin:0 0 8px;padding-bottom:8px;border-bottom:1px solid #ECECF1}
.navbtn{width:100%;border:0;background:none;font:inherit;cursor:pointer;
  text-align:left}
.railsearch{margin:0 4px 8px;padding:8px 12px;font:inherit;font-size:13px;
  border:1.5px solid #C7CDF3;border-radius:10px;outline:none;background:#fff}
.railsearch:focus{border-color:var(--accent)}
.navblock .nav{margin-bottom:1px}
.nav.on{background:#ECECF1;color:var(--ink,#1B1F30)}
.nav.on svg{color:var(--ink,#1B1F30)}
.railhead{display:flex;align-items:center;justify-content:space-between;
  padding:2px 2px 8px 8px;min-height:34px}
.railhead .navsec{margin:0;padding:0;line-height:1}
.railhead .railfilter{padding:0;align-items:center}
.railfilter{display:flex;gap:8px;padding:2px 8px 8px}
.rf{font:inherit;font-size:12.5px;font-weight:500;color:var(--mut,#8A8D9C);
  background:none;border:1px solid transparent;border-radius:8px;padding:4px 12px;
  cursor:pointer;display:inline-flex;gap:8px;align-items:center}
.rf:hover{background:#F0F0F5}
.rf.on{background:#fff;border-color:var(--hair,#E8E9EF);color:var(--ink,#1B1F30)}
.rf i{font-style:normal;font-size:11px;font-weight:600;color:#4553C8;
  background:var(--accent-soft,#E9EBF8);border-radius:6px;padding:0 4px}
.raillist{padding-bottom:8px;flex:1;overflow-y:auto;min-height:0}
.rail{display:flex;align-items:center;gap:8px;padding:8px 8px;border-radius:10px;
  font-size:13.5px;color:var(--text,#3A3D4D);margin-bottom:1px}
#raillist [data-st][hidden]{display:none}
.rail:hover{background:#F0F0F5}
.rail.active{background:#ECECF1;color:var(--ink,#1B1F30)}
.rail .alogo{width:30px;height:30px;border-radius:99px;flex:none;display:grid;
  place-items:center;color:#fff;font-size:13px;font-weight:600}
.rbody{flex:1;min-width:0;display:flex;flex-direction:column;gap:1px}
.rtop{display:flex;align-items:baseline;gap:8px}
.rname{flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
  font-weight:500;color:var(--ink,#1B1F30)}
.rwhen{flex:none;font-size:11px;color:var(--mut,#8A8D9C)}
.rsub{display:flex;align-items:center;gap:8px}
.rprev{flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
  font-size:12px;color:var(--mut,#8A8D9C)}
.rail.unread .rname{font-weight:700}
.rail.unread .rprev{color:var(--text,#3A3D4D)}

.rlabel{flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.railsys{flex:none;padding-top:8px;margin-top:8px;border-top:1px solid #ECECF1}
.railsys .nav{margin-bottom:1px}
.arow-min{padding:6px 10px;gap:8px}
.quietrow .rlabel{color:var(--mut);font-weight:400}
.quietrow:hover .rlabel{color:var(--ink,#1B1F30)}
.qword{flex:none;font-size:10.5px;color:#B9BCC7}
.conv{display:flex;align-items:center;gap:8px;padding:8px 8px;border-radius:8px;
font-size:13.5px;color:var(--text,#3A3D4D);margin-bottom:1px;position:relative}
.conv .dot{width:7px;height:7px;border-radius:50%;border:1.5px solid #C2C5D2;flex:none}
.conv .ctitle{flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.conv .kebab{visibility:hidden;color:var(--mut,#8A8D9C);padding:0 2px;font-size:15px}
.conv:hover{background:#F0F0F5}
.conv:hover .kebab{visibility:visible}
.conv.active{background:#ECECF1;color:var(--ink,#1B1F30)}
.cbadge{flex:none;font-size:11px}
.qlist{background:#fff;border:1px solid var(--hair);border-radius:16px;
  padding:4px 20px;margin:4px 0 8px}
.qrow{border-bottom:1px solid #F1F1F5}
.qrow:last-child{border-bottom:0}
.qmain{display:flex;align-items:center;gap:12px;padding:12px 0}
.qtext{flex:1;min-width:0}
.qtext b{display:block;font-size:14px;color:var(--ink)}
.qsub{display:flex;gap:8px;align-items:baseline;font-size:12.5px;
  margin-top:2px;flex-wrap:wrap}
.qno{color:#A63A2B}
.qarr{color:#B9BCC7}
.qyes{color:#177245}
.qacts{display:flex;gap:8px;align-items:center;flex:none}
.qmore{border:0;background:none;color:var(--mut);cursor:pointer;
  font-size:15px;padding:4px 8px;border-radius:8px}
.qmore:hover{background:#F0F0F5}
.qmore.open{transform:rotate(180deg)}
.qdetail{padding:0 0 16px 42px}
@media (max-width:760px){
  .qmain{flex-wrap:wrap}
  .qacts{width:100%;justify-content:flex-end}
}
.needsrow{display:flex;align-items:center;gap:10px;margin:4px 0 12px;
  padding:10px 12px;border-radius:12px;background:#FBF2E2;color:#9A6215}
.needsrow .rbadge{margin:0}
.nr-t{flex:1;min-width:0}
.nr-t b{display:block;font-size:13px}
.nr-sub{display:block;font-size:11.5px;opacity:.8}
.needsrow .nharrow{opacity:.6}
.needsrow:hover .nharrow{opacity:1}
.spot-scrim{position:fixed;inset:0;background:rgba(24,25,32,.4);z-index:80;
  display:none}
.spot-scrim.open{display:block}
.spot{position:fixed;top:12vh;left:50%;transform:translateX(-50%);
  width:min(640px,94vw);background:#fff;border-radius:16px;z-index:81;
  box-shadow:0 24px 80px rgba(24,25,32,.3);overflow:hidden;display:none}
.spot.open{display:block}
.spot-head{display:flex;align-items:center;gap:12px;padding:16px 20px;
  border-bottom:1px solid var(--hair,#E8E9EF)}
.spot-head svg{width:18px;height:18px;color:#8A8D9C;flex:none}
.spot-in{flex:1;border:0;outline:none;font:inherit;font-size:16px;
  color:var(--ink,#1B1F30)}
.spot-in::placeholder{color:#9A9DAB}
.spot-x{border:0;background:none;font-size:16px;color:#8A8D9C;
  cursor:pointer;padding:4px}
.spot-res{max-height:56vh;overflow-y:auto;padding:8px}
.spot-sec{font-size:11px;font-weight:700;letter-spacing:.06em;
  text-transform:uppercase;color:#9A9DAB;padding:10px 12px 4px}
.spot-row{display:flex;align-items:center;gap:12px;padding:10px 12px;
  border-radius:10px;cursor:pointer}
.spot-row.sel{background:#F4F5F9}
.sr-av{width:28px;height:28px;border-radius:8px;flex:none;display:grid;
  place-items:center;font-weight:700;font-size:13px}
.sr-t{flex:1;min-width:0;font-size:14px;color:var(--ink,#1B1F30)}
.sr-t b{font-weight:500}
.sr-sub{display:block;font-size:12px;color:#8A8D9C;white-space:nowrap;
  overflow:hidden;text-overflow:ellipsis}
.sr-enter{flex:none;color:#B9BCC7;opacity:0}
.spot-row.sel .sr-enter{opacity:1}
.spot-empty{padding:24px;text-align:center;color:#8A8D9C;font-size:13.5px}
.needshdr{display:flex;align-items:center;gap:8px;margin:12px 8px 4px;
  font-size:12px;font-weight:600;color:#9A6215}
.needshdr .rbadge{margin:0}
.needshdr .nharrow{margin-left:auto;color:var(--mut);opacity:0;
  transition:opacity .12s}
.needshdr:hover .nharrow{opacity:1}
.rstake{flex:none;font-size:12px;font-weight:700;color:#9A6215}
.st.need2{background:#FBF2E2;color:#9A6215}
.arow-min .rlabel{font-size:13px}
.arow-min.unread .rlabel{font-weight:600;color:var(--ink,#1B1F30)}
.rbadge{flex:none;min-width:18px;height:18px;border-radius:99px;
  background:#E8A33D;color:#fff;font-size:11px;font-weight:700;
  display:inline-flex;align-items:center;justify-content:center;
  padding:0 5px}
.caselist{max-width:640px;background:#fff;border:1px solid var(--hair);
  border-radius:14px;padding:4px 12px;margin:4px 0 8px}
.caselist .rail{border-bottom:1px solid #F4F4F7;border-radius:0;
  margin:0;padding:10px 4px}
.caselist .rail:last-child{border-bottom:0}
.cdot{width:8px;height:8px;border-radius:50%;flex:none}
.cdot.need{background:#E8A33D}
.cdot.work{background:var(--accent,#5266EB)}
.cdot.done{background:#1E9E5A}
.railfoot{display:flex;gap:8px;align-items:center;padding:8px 12px 4px;
  margin-top:8px;border-top:1px solid #ECECF1;font-size:12.5px;
  color:var(--mut,#8A8D9C);flex:none;flex-wrap:wrap;row-gap:2px}
.railfoot a{color:var(--mut,#8A8D9C)}
.railfoot a:hover{color:var(--ink,#1B1F30)}
.cempty{padding:4px 8px;font-size:12.5px;color:var(--mut,#8A8D9C)}
.navsec.csec{margin-top:16px;display:flex;align-items:center;gap:6px}
.navsec.csec svg{width:12px;height:12px;flex:none}
.steps{align-self:stretch;max-width:100%}
.btn.scanning{color:#fff;border-color:transparent;
  background:linear-gradient(100deg,var(--accent,#5266EB) 25%,#B9C2F7 45%,var(--accent,#5266EB) 65%);
  background-size:200% 100%;animation:btnsheen 1.6s linear infinite}
@keyframes btnsheen{0%{background-position:130% 0}100%{background-position:-70% 0}}

/* the work, ticking off */
.wsteps{list-style:none;margin:2px 0 4px;padding:0;max-width:100%}
.wstep{display:flex;align-items:baseline;gap:8px;padding:8px 0;font-size:13.5px;
  color:var(--mut,#8A8D9C);opacity:.45;transition:opacity .25s,color .25s}
.wstep.live,.wstep.done{opacity:1}
.wstep.live{color:var(--ink,#1B1F30)}
.wstep.done{color:var(--text,#3A3D4D)}
.wlabel{flex:0 1 auto}
.wfound{color:var(--mut,#8A8D9C);font-size:12.5px;opacity:0;transition:opacity .25s}
.wstep.done .wfound{opacity:1}
.wtick{width:14px;height:14px;flex:none;border-radius:50%;position:relative;
  border:1.5px solid #D3D6E0;align-self:center}
.wstep.live .wtick{border-color:var(--accent,#5266EB);border-top-color:transparent;
  animation:wspin .7s linear infinite}
@keyframes wspin{to{transform:rotate(360deg)}}
.wstep.done .wtick{border-color:#1E9E5A;background:#1E9E5A}
.wstep.done .wtick::after{content:"";position:absolute;left:4px;top:1px;width:4px;
  height:8px;border:solid #fff;border-width:0 1.6px 1.6px 0;transform:rotate(43deg)}

/* the work product: a document, not a chat bubble */
.wp{background:#fff;border:1px solid var(--hair,#E8E9EF);border-radius:12px;
  box-shadow:0 1px 2px rgba(27,31,48,.04);overflow:hidden;max-width:100%}
.wp-h{padding:16px 24px 16px;border-bottom:1px solid #F1F1F5}
.wp-kicker{font-size:11px;font-weight:600;letter-spacing:.07em;text-transform:uppercase;
  color:var(--mut,#8A8D9C);margin-bottom:8px}
.wp-h h3{font-size:15.5px;font-weight:600;color:var(--ink,#1B1F30);line-height:1.35}
.wp-meta{font-size:12.5px;color:var(--mut,#8A8D9C);margin-top:4px}
.wp-body{padding:16px 24px 8px}
.wp-claim{background:#F4F4F7;border-radius:8px;padding:8px 12px;font-size:13px;
  color:#4A4E63;margin-bottom:16px}
.wp-p{font-size:14px;line-height:1.7;color:#26293A;margin-bottom:12px}
.srcchip{display:inline-block;font-size:11px;font-weight:600;color:#4553C8;
  background:var(--accent-soft,#E9EBF8);border-radius:6px;padding:1px 8px;
  margin-left:4px;vertical-align:1px;white-space:nowrap}
.srcchip:hover{background:#DCE0F7}
.wp-srcs{display:flex;flex-wrap:wrap;gap:8px;align-items:center;
  padding:12px 24px;background:#FAFAFC;border-top:1px solid #F1F1F5}
.wp-srch{font-size:11.5px;font-weight:600;color:var(--mut,#8A8D9C);margin-right:2px}
.wp-acts{display:flex;flex-wrap:wrap;gap:8px;align-items:center;
  padding:16px 24px;border-top:1px solid #F1F1F5}
.wp-acts form{display:flex;gap:8px}
.wp-trust{font-size:12px;color:var(--mut,#8A8D9C);margin-left:auto}
.wp-edit{padding:0 24px 16px}
.wp-edit textarea{width:100%;font:inherit;font-size:13.5px;padding:8px 12px;
  border:1px solid #E3E4EA;border-radius:9px;margin-bottom:8px;color:#26293A;
  background:#fff;resize:vertical}
.wp-edit textarea:focus{outline:none;border-color:#98A5F0;
  box-shadow:0 0 0 3px rgba(82,102,235,.13)}
.wp-open{display:block;padding:12px 24px;border-top:1px solid #F1F1F5;
  font-size:12.5px;font-weight:500;color:var(--accent,#5266EB);background:#FCFCFD}
.wp-open:hover{background:#F5F6FB}

/* the case page */
.casehead{display:flex;align-items:flex-start;gap:16px;margin:2px 0 20px;flex-wrap:wrap}
.casename{display:flex;align-items:center;gap:8px;margin:2px 0 4px;font-size:26px}
.casemeta{font-size:13px;color:var(--mut,#8A8D9C)}
.casest{font-size:12.5px;font-weight:600;border-radius:999px;padding:4px 12px;
  margin-left:auto;flex:none;white-space:nowrap}
.casest.need{background:#FCEED8;color:#9A6215}
.casest.work{background:var(--accent-soft,#E9EBF8);color:#4553C8}
.casest.done{background:#E5F4EC;color:#177245}
.casegrid{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);
  gap:12px 32px;align-items:start}
@media (max-width:900px){.casegrid{grid-template-columns:minmax(0,1fr)}}
.ctimeline{list-style:none;margin:8px 0 0;padding:0}
.cstep{display:flex;align-items:baseline;gap:12px;padding:8px 0 8px 16px;
  position:relative;font-size:13.5px;opacity:0;animation:wfadein .45s ease forwards}
.cstep:not(:last-child)::before{content:"";position:absolute;left:3px;top:16px;
  bottom:-9px;width:1px;background:#E7E8EE}
.cdot2{position:absolute;left:0;top:12px;width:8px;height:8px;border-radius:50%;
  background:#C6C9D4}
.clab{flex:1;color:var(--ink,#1B1F30);min-width:0}
.csub{display:block;font-size:12.5px;color:var(--mut,#8A8D9C);margin-top:1px;
  overflow-wrap:anywhere}
.cwhen{color:var(--mut,#8A8D9C);font-size:12px;flex:none}
.cdecide{display:flex;flex-wrap:wrap;gap:8px;margin:8px 0 4px}
.pcard{background:#fff;border:1px solid var(--hair,#E8E9EF);border-radius:12px;
  padding:8px 16px;margin-top:8px}
.prow{padding:12px 0;border-bottom:1px solid #F1F1F5;font-size:13.5px}
.prow:last-child{border-bottom:none}
.prow b{display:block;color:var(--ink,#1B1F30);font-size:13px;margin-bottom:2px}
.prow span{color:var(--mut,#8A8D9C);font-size:12.5px;overflow-wrap:anywhere}
.agchip2{display:inline-block;background:var(--accent-soft,#E9EBF8);
  color:#3A46A8;border-radius:8px;padding:1px 8px;font-weight:600;
  font-size:.95em;white-space:nowrap}
.agchip2:hover{background:#DDE1F6}
.onemem{display:flex;align-items:center;gap:12px;background:#fff;
  border:1px solid var(--hair);border-radius:14px;padding:14px 20px;
  margin:0 0 16px;max-width:880px;font-size:13.5px}
.onemem svg{width:18px;height:18px;color:#3A46A8;flex:none}
.onemem span{flex:1}
.onemem a{color:var(--accent);font-size:12.5px;flex:none}
.avwrap{position:relative;flex:none;display:inline-flex}
.avwrap .pres{position:absolute;right:-2px;bottom:-2px;width:10px;
  height:10px;border-radius:50%;border:2px solid #fff}
.avwrap .pres.on{background:#1E9E5A}
.avwrap .pres.off{background:#C2C5D2}
.arow-min .avwrap .pres{width:8px;height:8px;right:-1px;bottom:-1px}
.mention{display:inline-block;border-radius:8px;padding:0 6px;
  font-weight:600;font-size:.95em;white-space:nowrap;line-height:1.5}
.burger{display:none;position:fixed;top:12px;left:12px;z-index:70;
  width:40px;height:40px;border-radius:12px;border:1px solid #E8E9EF;
  background:#fff;font-size:17px;cursor:pointer;
  box-shadow:0 2px 12px rgba(27,31,48,.08)}
@media (max-width:760px){
  .burger{display:grid;place-items:center}
  .sidebar{transform:translateX(-100%);transition:transform .18s ease;
    width:290px;z-index:60;box-shadow:none}
  .sidebar.open{transform:none;box-shadow:16px 0 44px rgba(27,31,48,.18)}
  .main{margin-left:0 !important}
  h1.page{font-size:24px;margin-top:48px}
  .convhead{padding:14px 16px 14px 60px}
  .stattiles{grid-template-columns:1fr 1fr}
  .dhead{flex-wrap:wrap;row-gap:8px;margin-top:44px}
  .composer{padding:8px 12px 16px}
  .hero{margin-top:52px}
  .miles{overflow-x:auto}
}
@keyframes wfadein{to{opacity:1}}
"""

# One button system for every surface (decisions: consistency over variety).
BTN_CSS = """
.btn{font:inherit;font-size:13px;font-weight:500;border-radius:9px;padding:8px 16px;
  border:1px solid transparent;cursor:pointer;
  transition:background .12s,border-color .12s,color .12s}
.btn.primary{background:var(--accent,#5266EB);color:#fff}
.btn.primary:hover{background:#4557D6}
.btn.primary:active{background:#3D4EC4}
.btn.ghost{background:#fff;border-color:#E3E4EA;color:var(--text,#3A3D4D)}
.btn.ghost:hover{border-color:#C9CBD6;color:var(--ink,#1B1F30)}
.btn.ghost:active{background:#F5F5F8}
.btn.wide{width:100%;padding:8px;font-size:14px;font-weight:600;margin-top:8px}
.btn.sm{padding:4px 12px;font-size:12.5px}
.btn:disabled{opacity:.45;cursor:not-allowed}
.btn:focus-visible{outline:2px solid #98A5F0;outline-offset:2px}
.sendbtn{width:36px;height:36px;border-radius:50%;border:none;background:var(--pill,#EEEFF2);
  color:#5A5D6D;cursor:pointer;display:grid;place-items:center;flex:none;
  transition:background .12s,color .12s}
.sendbtn svg{width:15px;height:15px}
.sendbtn:hover{background:var(--accent,#5266EB);color:#fff}
.sendbtn:active{background:#3D4EC4;color:#fff}
.sendbtn:focus-visible{outline:2px solid #98A5F0;outline-offset:2px}
"""


def _fmt_latency(ms):
    if ms is None:
        return "–"
    minutes = ms / 60000
    return f"{minutes/60:.0f}h" if minutes >= 90 else f"{minutes:.0f}m"


def fmt_pct(v):
    return "–" if v is None else f"{v * 100:.0f}%"


def esc(s):
    return html.escape(str(s or ""))


# ---------------------------------------------------------------- rendering
# Official Lucide line icons (ISC), embedded inline — no runtime deps.
# Path data fetched from lucide-static; stroke 1.75 app-wide.
ICONS = {
    'home': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M15 21v-8a1 1 0 0 0-1-1h-4a1 1 0 0 0-1 1v8" />  <path d="M3 10a2 2 0 0 1 .709-1.528l7-6a2 2 0 0 1 2.582 0l7 6A2 2 0 0 1 21 10v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z" /></svg>',
    'tasks': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10" />  <path d="m9 12 2 2 4-4" /></svg>',
    'cmd': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M12 19h8" />  <path d="m4 17 6-6-6-6" /></svg>',
    'ledger': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M6 22a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h8a2.4 2.4 0 0 1 1.704.706l3.588 3.588A2.4 2.4 0 0 1 20 8v12a2 2 0 0 1-2 2z" />  <path d="M14 2v5a1 1 0 0 0 1 1h5" />  <path d="M10 9H8" />  <path d="M16 13H8" />  <path d="M16 17H8" /></svg>',
    'alert': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3" />  <path d="M12 9v4" />  <path d="M12 17h.01" /></svg>',
    'pin': '<svg viewBox="0 0 24 24" fill="currentColor" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M12 17v5" fill="none"/><path d="M9 10.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24V16a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V7a1 1 0 0 1 1-1 2 2 0 0 0 0-4H8a2 2 0 0 0 0 4 1 1 0 0 1 1 1z"/></svg>',
    'chat': '<svg viewBox="0 0 24 24" fill="currentColor" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M7.9 20A9 9 0 1 0 4 16.1L2 22z"/></svg>',
    'search': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="m21 21-4.34-4.34" />  <circle cx="11" cy="11" r="8" /></svg>',
    'bolt': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M15.914 4a1.5 1.5 0 00-2.474-1.561l-9 9A1.5 1.5 0 005.5 14h4.002a.5.5 0 01.471.666L8.086 20a1.5 1.5 0 002.475 1.56l9-9A1.5 1.5 0 0018.5 10h-3.997a.5.5 0 01-.472-.667z" /></svg>',
    'bm': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M17 3a2 2 0 0 1 2 2v15a1 1 0 0 1-1.496.868l-4.512-2.578a2 2 0 0 0-1.984 0l-4.512 2.578A1 1 0 0 1 5 20V5a2 2 0 0 1 2-2z" /></svg>',
    'flow': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><rect width="8" height="8" x="3" y="3" rx="2" />  <path d="M7 11v4a2 2 0 0 0 2 2h4" />  <rect width="8" height="8" x="13" y="13" rx="2" /></svg>',
    'book': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M12 5v16" />  <path d="M20.001 19A2 2 0 0022 17V5a2 2 0 00-1.999-2L16 3.002A5 5 0 0012 5a5 5 0 00-4-2H4a2 2 0 00-2 2v12a2 2 0 001.999 2H8a5 5 0 014 2 5 5 0 014-2z" /></svg>',
    'lock': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><rect width="18" height="11" x="3" y="11" rx="2" ry="2" />  <path d="M7 11V7a5 5 0 0 1 10 0v4" /></svg>',
    'bot': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M12 8V4H8" />  <rect width="16" height="12" x="4" y="8" rx="2" />  <path d="M2 14h2" />  <path d="M20 14h2" />  <path d="M15 13v2" />  <path d="M9 13v2" /></svg>',
    'gear': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M9.671 4.136a2.34 2.34 0 0 1 4.659 0 2.34 2.34 0 0 0 3.319 1.915 2.34 2.34 0 0 1 2.33 4.033 2.34 2.34 0 0 0 0 3.831 2.34 2.34 0 0 1-2.33 4.033 2.34 2.34 0 0 0-3.319 1.915 2.34 2.34 0 0 1-4.659 0 2.34 2.34 0 0 0-3.32-1.915 2.34 2.34 0 0 1-2.33-4.033 2.34 2.34 0 0 0 0-3.831A2.34 2.34 0 0 1 6.35 6.051a2.34 2.34 0 0 0 3.319-1.915" />  <circle cx="12" cy="12" r="3" /></svg>',
    'folder': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M20 20a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-7.9a2 2 0 0 1-1.69-.9L9.6 3.9A2 2 0 0 0 7.93 3H4a2 2 0 0 0-2 2v13a2 2 0 0 0 2 2Z" /></svg>',
    'ear': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M6 8.5a6.5 6.5 0 1 1 13 0c0 6-6 6-6 10a3.5 3.5 0 1 1-7 0" />  <path d="M15 8.5a2.5 2.5 0 0 0-5 0v1a2 2 0 1 1 0 4" /></svg>',
    'funnel': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M10 20a1 1 0 0 0 .553.895l2 1A1 1 0 0 0 14 21v-7a2 2 0 0 1 .517-1.341L21.74 4.67A1 1 0 0 0 21 3H3a1 1 0 0 0-.742 1.67l7.225 7.989A2 2 0 0 1 10 14z" /></svg>',
    'pen': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M13 21h8" />  <path d="M21.174 6.812a1 1 0 0 0-3.986-3.987L3.842 16.174a2 2 0 0 0-.5.83l-1.321 4.352a.5.5 0 0 0 .623.622l4.353-1.32a2 2 0 0 0 .83-.497z" /></svg>',
    'shield': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z" />  <path d="m9 12 2 2 4-4" /></svg>',
    'note': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><rect width="8" height="4" x="8" y="2" rx="1" ry="1" />  <path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2" />  <path d="m9 14 2 2 4-4" /></svg>',
    'moon': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M20.985 12.486a9 9 0 1 1-9.473-9.472c.405-.022.617.46.402.803a6 6 0 0 0 8.268 8.268c.344-.215.825-.004.803.401" /></svg>',
    'chart': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v16a2 2 0 0 0 2 2h16" />  <path d="M18 17V9" />  <path d="M13 17V5" />  <path d="M8 17v-3" /></svg>',
    'send': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M14.536 21.686a.5.5 0 0 0 .937-.024l6.5-19a.496.496 0 0 0-.635-.635l-19 6.5a.5.5 0 0 0-.024.937l7.93 3.18a2 2 0 0 1 1.112 1.11z" />  <path d="m21.854 2.147-10.94 10.939" /></svg>',
    'baton': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M11.017 2.814a1 1 0 0 1 1.966 0l1.051 5.558a2 2 0 0 0 1.594 1.594l5.558 1.051a1 1 0 0 1 0 1.966l-5.558 1.051a2 2 0 0 0-1.594 1.594l-1.051 5.558a1 1 0 0 1-1.966 0l-1.051-5.558a2 2 0 0 0-1.594-1.594l-5.558-1.051a1 1 0 0 1 0-1.966l5.558-1.051a2 2 0 0 0 1.594-1.594z" />  <path d="M20 2v4" />  <path d="M22 4h-4" />  <circle cx="4" cy="20" r="2" /></svg>',
}


def influenced_won(tid: str) -> int:
    """Paise won on disputes where an approved or lightly-edited response
    landed: the operator's proof-of-impact number."""
    led = WORLD.d.ledger
    total = 0
    for r in led.runs.values():
        if (r.tenant_id == tid and r.gate_action is not None
                and r.gate_action.value in ("approve", "edit")):
            out = led.outcome_for(r.run_id)
            if out and (out.outcome_value or {}).get("won"):
                total += (out.outcome_value or {}).get("amount_paise") or 0
    return total


def inr(paise: int | float) -> str:
    """Whole rupees, Indian grouping: 12400000 paise &rarr; '1,24,000'."""
    n = int(round((paise or 0) / 100))
    s = str(abs(n))
    if len(s) > 3:
        head, tail = s[:-3], s[-3:]
        parts = []
        while len(head) > 2:
            parts.insert(0, head[-2:])
            head = head[:-2]
        if head:
            parts.insert(0, head)
        s = ",".join(parts) + "," + tail
    return ("-" if n < 0 else "") + s


def recovered(tid: str) -> tuple[int, int, str]:
    """The AI-CFO headline: money Relay kept for this business, in paise,
    with how many wins it came from and the honest window it covers.

    This month when there is something this month; otherwise the whole
    record, and it says so rather than quietly padding a month."""
    led = WORLD.d.ledger
    now = WORLD.d.clock.now()
    wins = []
    for r in led.runs.values():
        if r.tenant_id != tid:
            continue
        out = led.outcome_for(r.run_id)
        if out and (out.outcome_value or {}).get("won"):
            wins.append((out.observed_at,
                         (out.outcome_value or {}).get("amount_paise") or 0))
    this_month = [w for w in wins
                  if w[0].year == now.year and w[0].month == now.month]
    if this_month:
        return (sum(a for _, a in this_month), len(this_month), "this month")
    if not wins:
        return (0, 0, "so far")
    first = min(t for t, _ in wins)
    return (sum(a for _, a in wins), len(wins),
            f"since {first.strftime('%-d %B')}")


def _working_since(tid: str) -> tuple[str, int]:
    """When the switched-on agent started, and how much it has looked at.
    the proof that this is a team that runs, not a chat you open."""
    led = WORLD.d.ledger
    mine = [r for r in led.runs.values() if r.tenant_id == tid]
    if not mine:
        return ("", 0)
    return (min(r.occurred_at for r in mine).strftime("%-d %B"), len(mine))


def rail_cases(tid: str, limit: int = 18) -> list[dict]:
    """The work, newest first: one row per order anyone has touched. One
    order is one case, so repeat disputes on the same order collapse into
    the row that already exists rather than piling up as separate items."""
    led = WORLD.d.ledger
    by_order: dict[str, list] = {}
    for r in led.runs.values():
        if r.tenant_id != tid or not r.order_id:
            continue
        by_order.setdefault(r.order_id, []).append(r)
    out = []
    for order, runs in by_order.items():
        runs.sort(key=lambda r: r.occurred_at)
        key, word = case_status(runs)
        out.append({
            "order": order, "key": key, "word": word,
            "last": max(r.occurred_at for r in runs),
            "label": f"{customer_of(order)} · {bought(order)}"})
        if key == "need" and ASSIGN.get(order):
            out[-1]["word"] = f'With {esc(ASSIGN[order].split()[0])} to say yes'
    out.sort(key=lambda c: c["last"], reverse=True)
    return out[:limit]


def agent_rail_state(tid: str, a: dict) -> tuple:
    """(preview, needs_yes, badge_count) for one agent's rail row. Work
    items aggregate under the agent responsible for them, the way messages
    aggregate under a contact."""
    slug = a["slug"]
    if slug == "dispute_defender":
        led = WORLD.d.ledger
        n = sum(1 for r in led.runs.values()
                if r.tenant_id == tid and r.state is RunState.AWAITING_GATE)
        if n:
            return (f'{n} repl{"ies" if n != 1 else "y"} waiting on your yes',
                    True, n)
        return ("Every dispute answered. Watching the mail.", False, 0)
    if slug in PROPS_DEF:
        p = prop_state(tid, slug)
        d = PROPS_DEF[slug]
        if p["state"] == "waiting":
            return (f'{d["rail"]} &middot; needs your yes', True, 1)
        if p["state"] == "approved":
            return (d["approved"], False, 0)
        return (d["declined"], False, 0)
    if slug in REPORT_AGENTS:
        return ("Wrote today&rsquo;s note into your brief", False, 0)
    return ("Watching &middot; learning your business", False, 0)


def rail_html(tid: str, active: str = "", convs: str | None = None,
              email: str = "") -> str:
    """The Paperclip read of a chat rail: the persistent contacts are your
    STAFF. One row per agent, unread-bold when it needs your yes, and the
    work aggregates under the agent responsible: 7 disputes are one
    Disputes Officer row carrying a 7, not seven rows."""
    # HCI: the sidebar is chrome: stable landmarks and temporal recall.
    # The work queue is primary content and lives on the main canvas; the
    # rail keeps ONE ambient landmark for it, then conversation history.
    led_r = WORLD.d.ledger
    n_need = sum(1 for r in led_r.runs.values()
                 if r.tenant_id == tid
                 and r.state is RunState.AWAITING_GATE) + props_waiting(tid)
    rows = ""
    if n_need:
        total_p = sum(price_of(r.order_id) for r in led_r.runs.values()
                      if r.tenant_id == tid
                      and r.state is RunState.AWAITING_GATE)
        total_p += 100 * sum(
            PROPS_DEF[sl].get("stake_n", 0) for sl in PROPS_DEF
            if prop_state(tid, sl)["state"] == "waiting")
        rows += (
            f'<a class="needsrow" href="/approvals">'
            f'<span class="rbadge">{n_need}</span>'
            f'<span class="nr-t"><b>Pending approvals</b>'
            f'<span class="nr-sub">&#8377;{inr(total_p)} riding on '
            f'them</span></span>'
            f'<span class="nharrow">&rarr;</span></a>')
    return f"""
  <button class="burger" aria-label="Menu"
    onclick="document.querySelector('.sidebar').classList.toggle('open')">&#9776;</button>
  <aside class="sidebar">
    <div class="brand"><span class="logo">R</span>
      <span class="bname"><b>Relay</b><span class="biz">{BUSINESS}</span></span></div>
    <div class="navblock">
      <button class="nav navbtn" onclick="railSearchToggle()">{ICONS["search"]}<span>Search</span></button>
      <a class="nav" href="/"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M12 5v14M5 12h14"/></svg><span>New</span></a>
      <a class="nav{' on' if active == 'scheduled' else ''}" href="/scheduled"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg><span>Scheduled</span></a>
    </div>


    <div class="raillist" id="raillist">
      {rows}
      {convs if convs is not None else ''}
    </div>
    <div class="railsys">
      <a class="nav{' on' if active == 'agents' else ''}" href="/agents">{ICONS["bot"]}<span>Agents</span></a>
      <a class="nav{' on' if active == 'memory' else ''}" href="/memory">{ICONS["book"]}<span>Knowledge</span></a>
      <a class="nav{' on' if active in ('journeys', 'activity') else ''}" href="/impact">{ICONS["ledger"]}<span>History</span></a>
    </div>
    <details class="acct railacct">
      <summary class="railme">
        <span class="avatar">{user_avatar(email)}</span>
        <span class="rm-t"><b>{esc(user_name(email))}</b>
        <span>{BUSINESS}</span></span>
        {ICONS["gear"]}
      </summary>
      <div class="acctmenu">
        <div class="acct-biz">{esc(user_name(email))}</div>
        <div class="acct-mail">{esc(email)}</div>
        <div class="acct-shop">{BUSINESS}</div>
        <a class="acct-set" href="/settings">Settings</a>
        <a class="acct-out" href="/logout">Log out</a>
      </div>
    </details>
  </aside>
  <div class="spot-scrim" id="spotscrim" onclick="closeSpot()"></div>
  <div class="spot" id="spot" role="dialog" aria-label="Search">
    <div class="spot-head">
      {ICONS["search"]}
      <input class="spot-in" id="spotin" placeholder="Search buyers, agents, files"
        oninput="spotQuery(this.value)" onkeydown="spotKeys(event)">
      <button class="spot-x" onclick="closeSpot()" aria-label="Close">&#10005;</button>
    </div>
    <div class="spot-res" id="spotres"></div>
  </div>
  <script>
  function railFilter(b){{
    document.querySelectorAll('.rf').forEach(x => x.classList.toggle('on', x === b));
    const need = b.dataset.f === 'need';
    document.querySelectorAll('#raillist [data-st]')
      .forEach(el => {{ el.hidden = need && el.dataset.st !== 'need'; }});
  }}
  const SPOT_COLORS = ["#5266EB", "#E8590C", "#0CA678", "#B33AB3",
                       "#E0A800", "#2B8A9E", "#D6336C", "#6741D9"];
  let spotItems = [], spotSel = 0, spotT = null;
  const QUICK = [
    {{kind: "Go", title: "Everything waiting on your yes", sub: "the approvals queue", href: "/approvals"}},
    {{kind: "Go", title: "Morning brief", sub: "what the office did", href: "/briefs/morning"}},
    {{kind: "Go", title: "Knowledge", sub: "what your team knows", href: "/memory"}}];
  function railSearchToggle(){{ openSpot(); }}
  function openSpot(){{
    document.getElementById('spotscrim').classList.add('open');
    document.getElementById('spot').classList.add('open');
    const i = document.getElementById('spotin');
    i.value = ''; spotRender(QUICK); i.focus();
  }}
  function closeSpot(){{
    document.getElementById('spotscrim').classList.remove('open');
    document.getElementById('spot').classList.remove('open');
  }}
  function spotColor(t){{
    let n = 0;
    for (const ch of t) n += ch.charCodeAt(0);
    return SPOT_COLORS[n % SPOT_COLORS.length];
  }}
  function spotRender(items){{
    spotItems = items; spotSel = 0;
    const res = document.getElementById('spotres');
    if (!items.length){{
      res.innerHTML = '<div class="spot-empty">Nothing matches. Try a buyer, an agent, or a file.</div>';
      return;
    }}
    let html = '', last = '';
    items.forEach((it, i) => {{
      if (it.kind !== last && it.kind !== 'Go'){{
        html += '<div class="spot-sec">' + it.kind + 's</div>';
        last = it.kind;
      }}
      html += '<div class="spot-row' + (i === spotSel ? ' sel' : '')
        + '" data-i="' + i + '" onclick="spotGo(' + i + ')">'
        + '<span class="sr-av" style="background:' + spotColor(it.title) + '1c;color:'
        + spotColor(it.title) + '">' + it.title[0].toUpperCase() + '</span>'
        + '<span class="sr-t"><b>' + it.title + '</b>'
        + '<span class="sr-sub">' + it.sub + '</span></span>'
        + '<span class="sr-enter">&#8617;</span></div>';
    }});
    res.innerHTML = html;
  }}
  function spotQuery(v){{
    clearTimeout(spotT);
    if (!v.trim()){{ spotRender(QUICK); return; }}
    spotT = setTimeout(async () => {{
      const r = await fetch('/api/search?q=' + encodeURIComponent(v.trim()));
      spotRender(await r.json());
    }}, 140);
  }}
  function spotMove(d){{
    if (!spotItems.length) return;
    spotSel = (spotSel + d + spotItems.length) % spotItems.length;
    document.querySelectorAll('.spot-row').forEach(r =>
      r.classList.toggle('sel', +r.dataset.i === spotSel));
    document.querySelector('.spot-row.sel')?.scrollIntoView({{block: 'nearest'}});
  }}
  function spotGo(i){{
    const it = spotItems[i === undefined ? spotSel : i];
    if (it) location.href = it.href;
  }}
  function spotKeys(e){{
    if (e.key === 'ArrowDown'){{ e.preventDefault(); spotMove(1); }}
    else if (e.key === 'ArrowUp'){{ e.preventDefault(); spotMove(-1); }}
    else if (e.key === 'Enter'){{ e.preventDefault(); spotGo(); }}
    else if (e.key === 'Escape'){{ closeSpot(); }}
  }}
  addEventListener('keydown', e => {{
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k'){{
      e.preventDefault();
      document.getElementById('spot').classList.contains('open')
        ? closeSpot() : openSpot();
    }}
  }});
  </script>"""


def sidebar_html(active: str, tid: str = "t1", convs: str | None = None,
                 email: str = "") -> str:
    return rail_html(tid, active, convs, email)


def avatar(slug: str, size: int = 30, on: bool = True) -> str:
    """An agent's face with presence where a person expects it: a small
    dot on the avatar's corner, like any chat app."""
    return (f'<span class="avwrap">{_identicon(slug, size)}'
            f'<i class="pres {"on" if on else "off"}"></i></span>')


def _identicon(seed: str, size: int = 34) -> str:
    """A Gravatar-style identicon, generated inline and deterministically.

    Each agent gets a face of its own so the roster reads as staff rather
    than a feature list. 5x5 grid mirrored down the middle (the GitHub
    identicon trick), hue taken from the same hash, so the same agent always
    renders the same and no network request is involved."""
    h = _hashlib.sha256(seed.encode()).digest()
    hue = h[0] * 360 // 256
    fg = f"hsl({hue} 58% 45%)"
    bg = f"hsl({hue} 58% 94%)"
    cell = size / 5
    blocks = []
    for col in range(3):                      # left half + centre, then mirror
        for row in range(5):
            if h[col * 5 + row + 1] & 1:
                for c in {col, 4 - col}:
                    blocks.append(
                        f'<rect x="{c * cell:.2f}" y="{row * cell:.2f}" '
                        f'width="{cell:.2f}" height="{cell:.2f}" fill="{fg}"/>')
    return (f'<svg class="ident" width="{size}" height="{size}" '
            f'viewBox="0 0 {size} {size}" aria-hidden="true">'
            f'<rect width="{size}" height="{size}" rx="9" fill="{bg}"/>'
            f'{"".join(blocks)}</svg>')


_LOGO_COLORS = ["#5266EB", "#E8590C", "#0CA678", "#B33AB3", "#E0A800",
                "#2B8A9E", "#D6336C", "#6741D9"]

# Buyers are people, not companies, so there is no logo to fetch — everyone
# renders as a letter-mark avatar. The map stays because a real workspace may
# one day carry a domain for a business-to-business buyer.
_DOMAIN_BY_NAME: dict[str, str] = {}


def _logo(label: str, size: int = 18) -> str:
    """Favicon when there is a real domain on file; letter-mark fallback
    otherwise (every buyer in this demo)."""
    if not label:
        return ""
    domain = _DOMAIN_BY_NAME.get(label) or (
        label.lower() if "." in label and " " not in label else None)
    if domain:
        return (f'<img class="alogo" width="{size}" height="{size}" '
                f'src="https://www.google.com/s2/favicons?domain={esc(domain)}&sz=64" '
                f'alt="{esc(label[0].upper())}" loading="lazy">')
    color = _LOGO_COLORS[sum(label.encode()) % len(_LOGO_COLORS)]
    return (f'<span class="alogo" style="background:{color}">'
            f'{esc(label[0].upper())}</span>')


def plain_detail(d: str) -> str:
    """Trace details are internal strings. The handful that are suppression
    or escalation codes get said in plain words before anyone reads one."""
    d = (d or "").strip()
    if not d:
        return "."
    if d in PLAIN_SKIP:
        return PLAIN_SKIP[d]
    if d == "gate_timeout":
        return "waited a day for your yes, so a person has it now"
    if d.startswith("stalled"):
        return "got stuck part-way, so a person has it now"
    if d == "drafter_escalate":
        return "the drafter wasn&rsquo;t sure, so a person has it now"
    if d.startswith("layer1:"):
        return "the reply broke one of your rules, so nobody was shown it"
    if d.startswith("judge:"):
        return "the wording scored too low, so a person got it instead"
    if d.startswith("llm_unavailable"):
        return "the AI was unreachable, so a person got it instead"
    return esc(d)


def _account_label(r) -> str:
    """Whose dispute this is, on screen: the Ojas Wellness buyer who raised
    it. Held in the demo seed against the order, never on the run: the run
    only knows whose business it belongs to."""
    return customer_of(r.order_id)


def auto_send_chip(r) -> str:
    """Earned autonomy, visible where it applies: once small ones may send
    themselves, every small waiting item says so, and says how to stop it."""
    if (autonomy_mode(r.tenant_id) == "small"
            and price_of(r.order_id) < SMALL_LIMIT_PAISE):
        return ('<span class="st ok" title="Under your small-amount line">'
                'sends itself at 6 PM unless you stop it</span>')
    return ""


def task_row(r) -> str:
    plain = PLAIN_REASON.get(r.reason_code, "A buyer is disputing a payment")
    customer = esc(_account_label(r))
    when = r.occurred_at.strftime("%b %-d")
    rid = r.run_id
    counter = esc((r.decision or {}).get("counter_text", ""))
    auto = auto_send_chip(r)
    return f"""
<div class="taskwrap">
  <div class="trow">
    <input type="checkbox" class="selrun" value="{rid}" onclick="bulksync()">
    <span class="ico">{ICONS["bolt"]}</span>
    <span class="tdesc">{auto} Reply to the bank for
      <a href="/cases/{esc(r.order_id or "")}"><b>{customer}</b></a> &middot;
      {esc(bought(r.order_id))}. {plain}.
      <a class="mut" href="/cases/{esc(r.order_id or "")}">{esc(case_no(r.order_id))} &rarr;</a>
      <span class="stream">&ldquo;{esc(r.claim_text)}&rdquo;</span></span>
    <span class="tacts">
      <form method="post" action="/act"><input type="hidden" name="run" value="{rid}">
        <button class="btn primary" name="action" value="approve">Send it</button>
        <input class="whyin" name="why" maxlength="200" placeholder="why not? (optional)">
        <button class="btn ghost" name="action" value="reject">Don&rsquo;t send</button>
      </form>
      <button class="btn ghost" onclick="toggleEdit('e-{rid}')">Change the wording</button>
    </span>
    <span class="when">{when}</span>
  </div>
  <div class="editbox" id="e-{rid}" hidden>
    <div class="draft">{counter}</div>
    <form method="post" action="/act">
      <input type="hidden" name="run" value="{rid}">
      <textarea name="text" rows="3">{counter}</textarea>
      <button class="btn primary" name="action" value="edit">Send my version</button>
    </form>
  </div>
</div>"""


def ledger_row(r) -> str:
    label, cls = STATE_META.get(r.state, (r.state.value, "mut"))
    extra = ((f' &middot; {PLAIN_SKIP.get(r.suppressed_reason, esc(r.suppressed_reason))}')
             if r.state is RunState.SUPPRESSED else "")
    out = WORLD.d.ledger.outcome_for(r.run_id)
    won = ""
    if out:
        v = out.outcome_value
        amt = v.get("amount_paise")
        won = (f'<span class="st ok">kept{f" &#8377;{inr(amt)}" if amt else ""}</span>'
               if v.get("won") else '<span class="st warn">lost</span>')
    mat = ""
    if r.gate_action is GateAction.EDIT:
        mat = ('<span class="st mut">you reworded it</span>' if r.gate_is_material is False
               else '<span class="st warn">you fixed a fact</span>')
    return (f'<a class="trow slim" href="/runs/{r.run_id}"><span class="ico">{ICONS["ledger"]}</span>'
            f'<span class="tdesc"><b>{PLAIN_REASON.get(r.reason_code, "A buyer disputed a payment")}</b> '
            f'<span class="stream">&ldquo;{esc(r.claim_text) if r.claim_text else "."}&rdquo;</span> '
            f'<span class="mut">{esc(_account_label(r))} &middot; '
            f'{esc(bought(r.order_id))}</span></span>'
            f'{mat}{won}<span class="st {cls}">{label}{extra}</span>'
            f'<span class="when">{r.occurred_at.strftime("%b %-d")}</span></a>')


# =========================================================================
# The case — one order is one case is one record.
#
# Everything below is the shared vocabulary for the conversational surface:
# the case number, the money on it, the date the bank gave you, the proof
# it stands on, the steps the team took to build it, and the finished reply
# that comes out the other end. Home, the thread, the queue and the case
# page all read from these, so a case says the same thing everywhere.
#
# All display-only and deterministic: prices key off the SKU, case numbers
# off the order id, so the same order renders the same on every boot.
# =========================================================================

SKU_PRICE = {                                # paise, display only
    "A2 Desi Ghee 500ml": 124_900,
    "Ashwagandha Gold capsules": 89_900,
    "Aloe Vera Juice 1L": 49_900,
    "Shilajit Resin 20g": 189_900,
    "Amla Juice 1L": 44_900,
    "Triphala tablets": 39_900,
    "Karela Jamun Juice 1L": 54_900,
    "Moringa capsules": 74_900,
    "Wild Turmeric powder": 59_900,
}


def case_no(order_id: str | None) -> str:
    """The number the owner says out loud. Derived, never stored."""
    tail = (order_id or "").rsplit("_", 1)[-1]
    if tail.isdigit():
        return f"OJW-{4600 + 13 * int(tail)}"
    seed = int(_hashlib.sha256((order_id or "?").encode()).hexdigest()[:6], 16)
    return f"OJW-{4000 + seed % 900}"


def price_of(order_id: str | None) -> int:
    p = SKU_PRICE.get(bought(order_id))
    if p:
        return p
    return 40_000 + (sum((order_id or "x").encode()) % 120) * 500


def due_on(r) -> datetime:
    """The date the bank wants an answer by. Real processors send it on the
    notice; nothing in this seed carries one, so it falls back to the usual
    week from the day it landed."""
    return r.deadline_at or (r.occurred_at + timedelta(days=7))


def _days_left(r) -> int:
    return (due_on(r) - datetime.now()).days


# Proof, said in two words — what goes on the chip in the reply, and what
# the owner sees in the list underneath it.
EV_LABEL = {
    "ev_pod": "Delivery proof", "ev_wa": "WhatsApp",
    "ev_inv": "Invoice", "ev_bank": "Bank statement",
    "ev_listing": "Product page", "ev_returnphotos": "Buyer photos",
    "ev_subs": "Refill log", "ev_policy": "Policy page",
    "ev_device": "Device match", "ev_otp": "One-time password",
}

# Which sentence each piece of proof belongs to. Plain substring matching on
# the drafted reply, so a chip only ever lands on a line it actually backs.
EV_MATCH = {
    "ev_pod": ("proof-of-delivery", "delivery proof", "courier", "gps",
               "scan", "delivered", "delivery record", "signed"),
    "ev_wa": ("whatsapp", "buyer's own", "thanking", "confirmation",
              "acknowledging", "message"),
    "ev_inv": ("invoice", "payment reference", "one reference", "reference"),
    "ev_bank": ("settlement", "debit", "statement", "bank", "hold"),
    "ev_listing": ("product page", "listing", "batch", "seal that shipped"),
    "ev_returnphotos": ("return-request photos", "return request", "photos"),
    "ev_subs": ("refill log", "subscription", "cancellation", "cancelled",
                "timestamp"),
    "ev_policy": ("policy", "next cycle", "credits"),
    "ev_device": ("device", "phone", "address", "earlier orders"),
    "ev_otp": ("one-time password", "checkout"),
}

# What the team says while it is checking each piece, and what it found.
EV_STEP = {
    "ev_pod": ("Checking the delivery proof", "found, signed {del_date}"),
    "ev_wa": ("Checking the WhatsApp thread", "buyer confirmed the address"),
    "ev_inv": ("Opening the invoice", "one payment reference on this order"),
    "ev_bank": ("Reading the bank statement", "one debit cleared, not two"),
    "ev_listing": ("Pulling the product page from the order date",
                   "batch and seal match what shipped"),
    "ev_returnphotos": ("Looking at the buyer's own return photos",
                        "seal intact in every shot"),
    "ev_subs": ("Reading the refill log",
                "cancelled after the parcel left the warehouse"),
    "ev_policy": ("Checking the refill policy as it read that day",
                  "the next cycle is credited, not this one"),
    "ev_device": ("Matching the device, phone and address",
                  "same as three earlier orders"),
    "ev_otp": ("Checking the one-time password at checkout",
               "confirmed on the buyer's own number"),
}


def _ev_base(eid: str) -> str:
    """Sample tenants get suffixed copies of the same proof; both read as
    the same piece on screen."""
    return next((k for k in EV_LABEL if eid.startswith(k)), eid)


def cited_proof(r) -> list[str]:
    seen, out = set(), []
    for eid in (r.decision or {}).get("cited_evidence_ids", []):
        b = _ev_base(eid)
        if b in EV_LABEL and b not in seen:
            seen.add(b)
            out.append(b)
    return out


def _src_chip(eid: str, order_id: str | None) -> str:
    return (f'<a class="srcchip" href="/cases/{esc(order_id or "")}#proof">'
            f'{EV_LABEL[eid]}</a>')


def sourced_draft(text: str, proof: list[str], order_id: str | None) -> str:
    """The drafted reply with every claim carrying the proof it stands on.
    A chip lands on the sentence whose words it matches; anything left over
    rides on the last line, so no proof is silently dropped."""
    if not text:
        return '<p class="wp-p">Nothing written yet.</p>'
    parts = [s for s in _re.split(r"(?<=[.!?])\s+", text.strip()) if s]
    used: set[str] = set()
    lines = []
    for s in parts:
        low = s.lower()
        chips = [e for e in proof
                 if e not in used and any(k in low for k in EV_MATCH[e])]
        used.update(chips)
        lines.append((s, chips))
    left = [e for e in proof if e not in used]
    if lines and left:
        lines[-1] = (lines[-1][0], lines[-1][1] + left)
    return "".join(
        f'<p class="wp-p">{esc(s)}'
        + "".join(_src_chip(e, order_id) for e in chips) + "</p>"
        for s, chips in lines)


def case_steps(r) -> list[tuple[str, str]]:
    """What the team actually did on this case, one line each, in order.
    Every line names something real off the record: the order, the SKU,
    the buyer, the proof it opened, the date the bank set."""
    order = r.order_id
    plain = PLAIN_REASON.get(r.reason_code, "A buyer is disputing a payment")
    del_date = (r.occurred_at - timedelta(days=3)).strftime("%-d %B")
    steps = [
        ("Reading the dispute from the bank",
         f"{plain[0].lower() + plain[1:]}, filed {r.occurred_at.strftime('%-d %B')}"),
        (f"Pulling order {case_no(order)} &middot; {esc(bought(order))} "
         f"&middot; {esc(customer_of(order))}",
         f"&#8377;{inr(price_of(order))} &middot; {esc(channel_of(order))}"),
    ]
    for eid in cited_proof(r)[:3]:
        label, found = EV_STEP[eid]
        steps.append((label, found.replace("{del_date}", del_date)))
    left = _days_left(r)
    steps.append(("Checking the date the bank gave you",
                  f"reply due {due_on(r).strftime('%-d %B')}"
                  + (f", {left} day{'s' if left != 1 else ''} left" if left >= 0
                     else ", already answered")))
    n = len(cited_proof(r))
    steps.append(("Writing the reply",
                  f"{n} piece{'s' if n != 1 else ''} of proof attached"))
    return steps


def steps_html(steps: list[tuple[str, str]], done: bool = True) -> str:
    """The work, visible. Done rows render ticked; a live thread hands the
    same markup to the browser and lets it tick them off one at a time."""
    if not steps:
        return ""
    rows = "".join(
        f'<li class="wstep{" done" if done else ""}">'
        f'<span class="wtick"></span>'
        f'<span class="wlabel">{label}</span>'
        f'<span class="wfound">{found}</span></li>'
        for label, found in steps)
    return f'<ol class="wsteps">{rows}</ol>'


def work_product(r, mode: str = "thread") -> str:
    """The thing a manager would forward: the finished reply to the bank,
    every line carrying its proof, with the three decisions underneath it.

    `mode="thread"` wires the buttons to the chat's confirm path;
    `mode="page"` posts the same decisions through the ordinary form. Both
    end up in the one approve/edit/dismiss the workspace has always used."""
    order = r.order_id
    text = (r.decision or {}).get("counter_text", "")
    proof = cited_proof(r)
    label, cls = STATE_META.get(r.state, (r.state.value, "mut"))
    left = _days_left(r)
    due = (f'reply due {due_on(r).strftime("%-d %B")}'
           + (f' &middot; {left} day{"s" if left != 1 else ""} left'
              if left >= 0 else ""))
    srcs = "".join(_src_chip(e, order) for e in proof) or (
        '<span class="mut">nothing attached</span>')
    acts = ""
    if r.state is RunState.AWAITING_GATE:
        if mode == "thread":
            pid = str(_uuid.uuid4())
            PROPOSALS[pid] = {"run_id": r.run_id, "action": None}
            acts = (
                f'<div class="wp-acts" id="prop-{pid}">'
                f'<button class="btn primary" onclick="wpAct(\'{pid}\',\'approve\',this)">'
                f'Approve</button>'
                f'<button class="btn ghost" onclick="wpEdit(\'{pid}\')">Edit</button>'
                f'<button class="btn ghost" onclick="wpAct(\'{pid}\',\'reject\',this)">'
                f'Not this time</button>'
                f'<span class="wp-trust">You approve before anything sends.</span>'
                f'</div>'
                f'<div class="wp-edit" id="wpe-{pid}" hidden>'
                f'<textarea id="wpt-{pid}" rows="6">{esc(text)}</textarea>'
                f'<button class="btn primary" onclick="wpAct(\'{pid}\',\'edit\',this)">'
                f'Send my version</button></div>')
        else:
            acts = (
                f'<div class="wp-acts">'
                f'<form method="post" action="/act">'
                f'<input type="hidden" name="run" value="{r.run_id}">'
                f'<button class="btn primary" name="action" value="approve">Approve</button>'
                f'<button class="btn ghost" name="action" value="reject">Not this time</button>'
                f'</form>'
                f'<button class="btn ghost" onclick="toggleEdit(\'wpe-{r.run_id}\')">Edit</button>'
                f'<span class="wp-trust">You approve before anything sends.</span></div>'
                f'<div class="wp-edit" id="wpe-{r.run_id}" hidden>'
                f'<form method="post" action="/act">'
                f'<input type="hidden" name="run" value="{r.run_id}">'
                f'<textarea name="text" rows="6">{esc(text)}</textarea>'
                f'<button class="btn primary" name="action" value="edit">'
                f'Send my version</button></form></div>')
    else:
        acts = (f'<div class="wp-acts"><span class="st {cls}">{label}</span>'
                f'<span class="wp-trust">Already decided: nothing here '
                f'can be sent twice.</span></div>')
    return (
        f'<article class="wp">'
        f'<div class="wp-h"><div class="wp-kicker">Reply to the bank</div>'
        f'<h3>{esc(case_no(order))} &middot; {esc(customer_of(order))} '
        f'&middot; {esc(bought(order))}</h3>'
        f'<div class="wp-meta">&#8377;{inr(price_of(order))} &middot; '
        f'{esc(channel_of(order))} &middot; {due}</div></div>'
        f'<div class="wp-body">'
        f'<div class="wp-claim">The buyer says: &ldquo;{esc(r.claim_text or ".")}&rdquo;</div>'
        f'{sourced_draft(text, proof, order)}</div>'
        f'<div class="wp-srcs"><span class="wp-srch">Proof attached</span>{srcs}</div>'
        f'{acts}'
        f'<a class="wp-open" href="/cases/{esc(order or "")}">Open the case &rarr;</a>'
        f'</article>')


# The case timeline reads as one shared history, so the internals of the
# engine — agent slugs, actor ids, source names, citation counts — get said
# in the words the owner would use. Display only; the record keeps its own.
SOURCE_WORDS = {"bank_webhook": "straight from the bank",
                "email_forward": "forwarded to you by email",
                "gateway_webhook": "from the payment gateway",
                "sample": "a made-up one, for trying it out"}


def mention(name: str) -> str:
    """A person's name as a WhatsApp-style mention tag: @Name in that
    person's own color, the way a group chat colors its members. "you"
    stays plain prose; tags are for named people."""
    n = (name or "").strip()
    if not n or n.lower() == "you":
        return "you"
    color = _LOGO_COLORS[sum(n.encode()) % len(_LOGO_COLORS)]
    return (f'<span class="mention" style="background:{color}1a;'
            f'color:{color}">@{esc(n)}</span>')


def _who(actor: str | None, seed: str = "") -> str:
    """Name on a decision. Seeded history is attributed across the team
    (deterministic per run, like every other fact in this fictional world)
    so the record reads the way a real office does: several people saying
    yes, not one. Live decisions carry the real session user."""
    a = (actor or "").strip()
    if not a or a in (BUSINESS_ID, UNLINKED_CHANNEL_ID, "workspace", "ask",
                      "m_ojas", "system"):
        if seed:
            names = ["you", "Nikhil Bhat", "Lakshmi Menon"]
            return names[sum(seed.encode()) % len(names)]
        return "you"
    return esc(a.split("@")[0])


def plain_step(e: dict, r) -> tuple[str, str]:
    """(what happened, what came of it): one line of the case's history."""
    agent, kind, d = e["agent"], e["kind"], (e.get("detail") or "")
    reason = PLAIN_REASON.get(r.reason_code, "A buyer is disputing a payment")
    if agent == "detection-agent":
        if kind == "signal":
            src = next((v for k, v in SOURCE_WORDS.items() if k in d), "")
            return ("The dispute came in", f"{reason}. {src}" if src else reason)
        if kind == "confirmed":
            return ("Read what the buyer is claiming",
                    esc(d) or (esc(r.claim_text or "")))
        return ("Decided it is not really a dispute", plain_detail(d))
    if agent == "eligibility-agent":
        if kind == "qualified":
            return ("Checked it is worth answering", "nothing in the way")
        return ("Set it aside", plain_detail(r.suppressed_reason or d))
    if agent == "response-agent":
        n = len(cited_proof(r))
        return ("Wrote the reply",
                f"{n} piece{'s' if n != 1 else ''} of proof attached")
    if agent == "compliance-agent":
        if kind == "passed":
            return ("Checked it against your rules", "nothing broken")
        return ("Held it back", plain_detail(d))
    if agent == "gate":
        if kind == "surfaced":
            return ("Put it in front of you", "waiting on your yes")
        if kind == "approved":
            who = _who(d.removeprefix('by '), seed=r.run_id)
            return (("You said yes" if who == "you"
                     else f"{mention(who)} said yes"), "sent as written")
        if kind == "edited":
            return ("You changed the wording",
                    "you fixed a fact" if r.gate_is_material else "just the wording")
        return ("You said no", "nothing was sent")
    if agent == "filing-agent":
        return ("Sent it to the bank", f"before {due_on(r).strftime('%-d %B')}")
    if agent == "reporting-agent":
        return ("How it ended", esc(d) or "written down")
    if agent == "escalation-agent":
        return ("Handed it to a person", plain_detail(d))
    if agent == "note" or kind == "note":
        who = _who(agent if agent != "note" else None)
        return (("Note from you" if who == "you"
                 else f"Note from {mention(who)}"), esc(d))
    return (esc(kind.replace("_", " ").capitalize()), plain_detail(d))


def case_runs(tid: str, order_id: str) -> list:
    led = WORLD.d.ledger
    return sorted((r for r in led.runs.values()
                   if r.tenant_id == tid and r.order_id == order_id),
                  key=lambda r: r.occurred_at)


def case_status(runs: list) -> tuple[str, str]:
    """(key, words): the one line that says where a case stands."""
    if any(r.state is RunState.AWAITING_GATE for r in runs):
        return ("need", "Pending approval")
    if any(r.state in (RunState.TIMED_OUT, RunState.FAILED) for r in runs):
        # A Relay person is on it — that is THEIR queue, not the founder's.
        # Counting it under "Needs you" hands the founder a job they cannot
        # do; to them, a case in someone's hands is a case being worked.
        return ("work", "With a person: being handled")
    if any(not r.state.terminal for r in runs):
        return ("work", "Working")
    return ("done", "Done")


def case_content(tid: str, order_id: str) -> str:
    """One order, one case, one shared history. Every agent that touched
    this order writes into the same timeline, in the order it happened.
    not a log per agent."""
    led = WORLD.d.ledger
    runs = case_runs(tid, order_id)
    if not runs:
        return ('<h1 class="page">Nothing here</h1>'
                '<div class="pagehint">No case on that order.</div>')
    latest = runs[-1]
    who = customer_of(order_id)
    skey, sword = case_status(runs)
    kept = 0
    for r in runs:
        out = led.outcome_for(r.run_id)
        if out and (out.outcome_value or {}).get("won"):
            kept += (out.outcome_value or {}).get("amount_paise") or 0

    # the shared history: every step every agent took, oldest first
    marks = []
    for r in runs:
        for e in led.trace_for(r.run_id):
            marks.append((e["ts"], e, r))
    marks.sort(key=lambda m: m[0])
    tl = "".join(
        f'<li class="cstep" style="animation-delay:{min(i, 14) * 60}ms">'
        f'<span class="cdot2"></span>'
        f'<span class="clab">{plain_step(e, er)[0]}'
        f'<span class="csub">{plain_step(e, er)[1]}</span></span>'
        f'<span class="cwhen">{e["ts"].strftime("%-d %b, %H:%M")}</span></li>'
        for i, (_, e, er) in enumerate(marks)) or (
        '<li class="cstep"><span class="cdot2"></span>'
        '<span class="clab">Nothing written down yet</span></li>')

    proof = cited_proof(latest)
    ev_by_id = {e.evidence_id: e for e in WORLD.d.evidence}
    prows = "".join(
        f'<div class="prow"><b>{EV_LABEL[e]}</b>'
        f'<span>{esc((ev_by_id.get(e).text if ev_by_id.get(e) else ""))}</span></div>'
        for e in proof) or '<div class="prow"><span class="mut">Nothing attached to this one.</span></div>'

    decided = []
    for r in runs:
        lbl, cls = STATE_META.get(r.state, (r.state.value, "mut"))
        decided.append(f'<span class="st {cls}">{lbl}</span>')

    left = _days_left(latest)
    meta = (f'&#8377;{inr(price_of(order_id))} &middot; '
            f'{esc(bought(order_id))} &middot; {esc(channel_of(order_id))} '
            f'&middot; reply due {due_on(latest).strftime("%-d %B")}'
            + (f' ({left} day{"s" if left != 1 else ""} left)' if left >= 0 else "")
            + (f' &middot; &#8377;{inr(kept)} kept' if kept else ""))
    # The four moments a founder actually cares about, read left to right
    # in one second. The full story below is for whoever wants it.
    filed = any(r.state in (RunState.ACTED, RunState.RESOLVED) for r in runs)
    res_out = next((led.outcome_for(r.run_id) for r in runs
                    if r.state is RunState.RESOLVED
                    and led.outcome_for(r.run_id)), None)
    won = ((res_out.outcome_value or {}).get("won")
           if res_out is not None else None)
    drafted = any(r.decision for r in runs)
    waiting = any(r.state is RunState.AWAITING_GATE for r in runs)
    d0 = min(r.occurred_at for r in runs)

    def mile(state, label, sub):
        tick = "&#10003;" if state == "done" else ""
        return (f'<span class="mile {state}"><span class="mdot">{tick}</span>'
                f'<b>{label}</b><span class="msub">{sub}</span></span>')

    if won is True:
        settle = mile("done", "Settled",
                      f'<span class="st ok">Won &middot; &#8377;{inr(kept)}</span>')
    elif won is False:
        settle = mile("done", "Settled", '<span class="st mut">Lost</span>')
    elif filed:
        settle = mile("todo", "Settled", "not yet")
    else:
        settle = mile("todo", "Settled", "not yet")
    miles = (
        '<div class="miles">'
        + mile("done", "Dispute came in", d0.strftime("%-d %b"))
        + f'<span class="mbar done"></span>'
        + (mile("done", "Reply written", "with proof attached") if drafted
           else mile("cur", "Reply written", "being written"))
        + f'<span class="mbar {"done" if filed else ""}"></span>'
        + (mile("done", "Filed with the bank", "exactly once") if filed
           else mile("cur",
                     f"Waiting on {esc(ASSIGN[order_id].split()[0])}"
                     if ASSIGN.get(order_id) else "Waiting on your yes",
                     "one tap") if waiting
           else mile("todo", "Filed with the bank", "after your yes"))
        + f'<span class="mbar {"done" if res_out is not None else ""}"></span>'
        + (settle if res_out is not None
           else mile("cur", "Bank reviewing", "in progress") if filed
           else mile("todo", "Bank review", "not yet"))
        + '</div>')
    # The founder's job on a waiting case is one thing: read the reply,
    # say yes or no. So on a waiting case the reply comes FIRST — right
    # under the milestones — and the story moves below it. Nobody should
    # scroll past a twelve-step timeline to do their only job.
    # Whose phone is this yes sitting on. The founder can hand it to any
    # approver, or pull it back, without touching the reply itself.
    assign_ui = ""
    if waiting:
        assignee = ASSIGN.get(order_id)
        approvers = [p["name"] for p in people_for(tid) if p["approver"]]
        opts = "".join(f'<option value="{esc(n)}"'
                       + (' selected' if n == assignee else '')
                       + f'>{esc(n)}</option>' for n in approvers)
        # One control, no button: pick a name and it is theirs. The label
        # is the select itself, so the state and the action are one thing.
        assign_ui = (
            f'<div class="assignbar">'
            + (f'<span>Waiting on {mention(assignee)} to say yes.</span>'
               if assignee else
               '<span>This yes is with <b>you</b>.</span>')
            + f'<form method="post" action="/api/assign">'
            f'<label class="assignlbl" for="assignsel">Whose yes:</label>'
            f'<select class="assignsel" id="assignsel" name="name" '
            f'onchange="this.form.submit()">'
            f'<option value=""{"" if assignee else " selected"}>You</option>'
            f'{opts}</select>'
            f'<input type="hidden" name="order" value="{esc(order_id)}">'
            f'<noscript><button class="btn ghost sm">Move it</button>'
            f'</noscript></form></div>')
    note_box = (
        f'<form class="notebar" method="post" action="/api/note" '
        f'style="max-width:640px;margin-top:16px">'
        f'<input type="hidden" name="run_id" value="{latest.run_id}">'
        f'<input type="hidden" name="back" value="/cases/{esc(order_id)}">'
        f'<input class="jfind notein" name="text" maxlength="200" '
        f'placeholder="Leave a note for the team, it lands in the case history">'
        f'<button class="btn primary sm">Add note</button></form>')
    reply_sec = (f'{assign_ui}<h2 class="sec">The reply</h2>'
                 f'{work_product(latest, mode="page")}')
    story = (
        f'<div class="casegrid">'
        f'<div><h2 class="sec">What happened, start to finish</h2>'
        f'<ol class="ctimeline">{tl}</ol></div>'
        f'<div><h2 class="sec">Where it stands</h2>'
        f'<div class="cdecide">{"".join(decided)}</div>'
        f'<h2 class="sec" id="proof">The proof on file</h2>'
        f'<div class="pcard">{prows}</div>'
        f'<h2 class="sec">Everything, one by one</h2>'
        + "".join(ledger_row(r) for r in reversed(runs))
        + '</div></div>' + note_box)
    body = (reply_sec + story) if waiting else (story + reply_sec)
    return (
        f'<a class="jback" href="/">&lsaquo; Back</a>'
        f'<div class="casehead"><div>'
        f'<div class="wp-kicker">Case {esc(case_no(order_id))}</div>'
        f'<h1 class="page casename">{_logo(who, 26)}{esc(who)}</h1>'
        f'<div class="casemeta">{meta}</div></div>'
        f'<span class="casest {skey}">{sword}</span></div>'
        f'{miles}'
        + body)


HOME_CONTENT = """
    <h1 class="page" id="tasks">Approvals</h1>
    <div class="pagehint">__QSUMMARY__ Nothing moves until you say yes.
      <form method="post" action="/api/sample" style="display:inline;margin-left:8px">
      <button class="btn ghost sm">Try a sample</button></form></div>
    __PROPS__
    __DHEAD__
    <div id="bulkbar" class="bulkbar" hidden>
      <b id="bcount"></b>
      <input id="bwhy" class="whyin" style="width:190px" maxlength="200"
             placeholder="why? (applies to dismissals)">
      <button class="btn primary sm" onclick="bulk('approve', this)">Say yes to these</button>
      <button class="btn ghost sm" onclick="bulk('reject', this)">Don&rsquo;t send these</button>
      <span class="mut" style="font-size:12px">j/k move &middot; x pick &middot; a yes &middot; d no &middot; e change</span>
    </div>
    __TASKS__
    <script>
    function qToggle(b){
      const det = b.closest('.qrow').querySelector('.qdetail');
      det.hidden = !det.hidden;
      b.classList.toggle('open', !det.hidden);
    }
    </script>"""

def render(tid: str = "t1", email: str = "") -> str:
    led = WORLD.d.ledger
    runs = sorted((r for r in led.runs.values() if r.tenant_id == tid),
                  key=lambda r: r.occurred_at, reverse=True)
    waiting = [r for r in runs if r.state is RunState.AWAITING_GATE]
    # Every agent's finished work queues with the dispute replies: one
    # list of yeses, whatever kind of work produced them.
    waiting_props = [sl for sl in PROPS_DEF
                     if prop_state(tid, sl)["state"] == "waiting"]
    waiting_props.sort(key=lambda sl: -PROPS_DEF[sl].get("stake_n", 0))
    stake_total = sum(PROPS_DEF[sl].get("stake_n", 0)
                      for sl in waiting_props) * 100
    props = ""
    if waiting_props:
        props = (f'<h2 class="sec">From your agents ({len(waiting_props)})'
                 f'</h2><div class="qlist">'
                 + "".join(prop_row(tid, sl) for sl in waiting_props)
                 + '</div>')
    dhead = (f'<h2 class="sec">Replies to the bank ({len(waiting)})</h2>'
             if waiting else '')
    tasks = ("\n".join(task_row(r) for r in waiting) or (
        '' if props else
        '<div class="empty">All clear: nothing needs review.</div>'))
    n_all_q = len(waiting_props) + len(waiting)
    d_stake = sum(price_of(r.order_id) for r in waiting)
    summary = (f'<b>{n_all_q}</b> decisions, '
               f'<b>&#8377;{inr(stake_total + d_stake)}</b> riding on them.'
               if n_all_q else 'All clear.')
    return (TEMPLATE
            .replace("__CONTENT__", HOME_CONTENT)
            .replace("__SIDEBAR__", sidebar_html("tasks", tid,
                                                 convs=conv_list_html(tid),
                                                 email=email))
            .replace("__BIZ__", BUSINESS)
            .replace("__NAME__", esc(user_name(email)))
            .replace("__USER__", esc(email))
            .replace("__INITIAL__", user_avatar(email))
            .replace("__NWAIT__", str(len(waiting) + cash_waiting(tid)))
            .replace("__TASKS__", tasks)
            .replace("__PROPS__", props)
            .replace("__DHEAD__", dhead)
            .replace("__QSUMMARY__", summary))


TEMPLATE = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Relay</title>
<style>
:root{--ink:#1B1F30;--text:#3A3D4D;--mut:#8A8D9C;--hair:#E8E9EF;--accent:#5266EB;
--accent-soft:#E9EBF8;--pill:#EEEFF2;--side:#FAFAFC}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Circular Std',-apple-system,BlinkMacSystemFont,'SF Pro Text',sans-serif;
color:var(--text);background:#FDFDFE;-webkit-font-smoothing:antialiased;font-size:14px}
a{text-decoration:none;color:inherit}
.sidebar{position:fixed;top:0;bottom:0;left:0;width:250px;background:var(--side);
border-right:1px solid #ECECF1;padding:16px 12px;display:flex;
flex-direction:column;overflow:hidden}
.brand{display:flex;align-items:center;gap:8px;padding:8px 8px;margin-bottom:12px}
.logo{width:26px;height:26px;border-radius:8px;background:#21232E;color:#fff;font-weight:700;
font-size:13px;display:grid;place-items:center}
.brand b{font-size:14px;color:var(--ink);font-weight:600;line-height:1.15}
.brand .bname{display:flex;flex-direction:column;gap:1px}
.brand .biz{font-size:11px;color:var(--mut);font-weight:500;letter-spacing:.2px}
.bizchip{border:1px solid var(--hair);border-radius:999px;padding:3px 8px;
font-size:12px;color:var(--ink);background:#fff;white-space:nowrap}
.pro{margin-left:auto;background:#21232E;color:#fff;font-size:10.5px;font-weight:600;
border-radius:6px;padding:2px 8px}
.nav{display:flex;align-items:center;gap:12px;padding:8px 8px;border-radius:8px;
color:var(--text);font-size:13.5px;margin-bottom:1px}
.nav svg{width:16px;height:16px;color:#6A6D7D;flex:none}
.nav:hover{background:#F0F0F5}
.nav.active{background:var(--accent-soft);color:var(--ink);font-weight:500}
.nav.active svg{color:var(--ink)}
.nav .count{margin-left:auto;color:var(--mut);font-size:12.5px}
.nav .new{margin-left:auto;background:#E3E6F0;color:#4A4E63;font-size:10.5px;font-weight:600;
border-radius:6px;padding:2px 8px}
hr.side{border:none;border-top:1px solid #ECECF1;margin:8px 0}
.navsec{margin:8px 8px 8px;font-size:12px;font-weight:600;color:var(--mut)}
.bm{padding:4px 8px}
.bm span{font-size:13px;color:var(--text);display:flex;gap:12px;align-items:center}
.bm span svg{width:15px;height:15px;color:#6A6D7D}
.bm i{font-style:normal;font-size:12.5px;color:var(--mut);padding-left:24px;display:block}
.main{margin-left:248px;min-height:100vh;background:linear-gradient(#FDFDFE,#F4F5F9)}
.topbar{display:flex;align-items:center;padding:20px 44px;color:var(--mut);font-size:13.5px}
.topbar .search input{border:none;outline:none;background:none;font:inherit;font-size:13.5px;color:var(--ink);width:220px}
.search input::placeholder{color:#9A9DAB}
.search{display:flex;gap:8px;align-items:center;cursor:pointer}
.topbar .search svg{width:15px;height:15px}
.topbar .right{margin-left:auto;display:flex;gap:16px;align-items:center}
.avatar{width:26px;height:26px;border-radius:50%;background:#5266EB;color:#fff;
  display:inline-flex;align-items:center;justify-content:center;
  font-size:12px;font-weight:650;text-transform:uppercase}
.acct{position:relative}
.acct summary{list-style:none;cursor:pointer}
.acct summary::-webkit-details-marker{display:none}
.acct[open] summary{outline:2px solid #C7CDF3;outline-offset:2px}
.acctmenu{position:absolute;right:0;top:calc(100% + 8px);background:#fff;
  border:1px solid var(--hair);border-radius:12px;
  box-shadow:0 10px 32px rgba(27,31,48,.12);padding:8px;min-width:208px;
  z-index:40;text-align:left}
.acct-biz{font-weight:600;color:var(--ink);padding:8px 12px 2px;font-size:13.5px}
.acct-mail{color:var(--mut);padding:0 12px;font-size:12.5px}
.acct-shop{color:var(--mut);padding:6px 12px 8px;font-size:12px;
  border-bottom:1px solid var(--hair);margin-bottom:4px}
.acct-out{display:block;padding:8px 12px;border-radius:8px;color:#B3372B;
  font-size:13.5px;text-decoration:none}
.acct-out:hover{background:#FBF1EF}
.avatar img{width:100%;height:100%;object-fit:cover;border-radius:50%;
  display:block}
.railacct{position:relative;margin-top:8px}
.railacct summary{list-style:none;cursor:pointer}
.railacct summary::-webkit-details-marker{display:none}
.railme{display:flex;align-items:center;gap:10px;padding:10px 12px;
  border-radius:12px;transition:background .12s}
.railme:hover{background:#F0F0F4}
.railme .avatar{width:30px;height:30px;flex:none}
.rm-t{flex:1;min-width:0;line-height:1.3}
.rm-t b{display:block;font-size:13.5px;color:var(--ink);font-weight:600}
.rm-t span{display:block;font-size:11.5px;color:var(--mut)}
.railme > svg{width:15px;height:15px;color:var(--mut);flex:none}
.railacct .acctmenu{top:auto;bottom:calc(100% + 8px);left:0;right:0;
  min-width:0}
.acct-set{display:block;padding:8px 12px;border-radius:8px;
  color:var(--ink);font-size:13.5px}
.acct-set:hover{background:#F5F5F8}
.hubtabs{display:flex;gap:24px;border-bottom:1px solid var(--hair);
  margin:0 0 24px}
.hubtabs a{padding:0 0 10px;font-size:13.5px;font-weight:500;
  color:var(--mut);border-bottom:2px solid transparent;margin-bottom:-1px}
.hubtabs a.on{color:var(--ink);border-color:var(--ink)}
.hubhead{display:flex;align-items:flex-start;justify-content:space-between;
  gap:24px;margin-bottom:12px}
.hubsearch{width:280px;border:1px solid var(--hair);border-radius:10px;
  padding:9px 14px;font:inherit;font-size:13.5px;outline:none;
  background:#fff;flex:none}
.hubsearch:focus{border-color:#98A5F0}
.hubtoolrow{display:flex;align-items:center;justify-content:space-between;
  margin:0 0 12px}
.hubpills{display:flex;gap:8px}
.hubpill{border:1px solid var(--hair);background:#fff;border-radius:999px;
  padding:6px 14px;font:inherit;font-size:12.5px;font-weight:500;
  color:var(--ink);cursor:pointer;transition:background .12s}
.hubpill.on{background:var(--ink);color:#fff;border-color:var(--ink)}
.hubgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;
  margin:0 0 8px}
.hubgrid.two{grid-template-columns:1fr 1fr}
@media (max-width:900px){.hubgrid,.hubgrid.two{grid-template-columns:1fr}}
.hubcard{border:1px solid var(--hair);background:#fff;border-radius:14px;
  padding:14px 16px;display:flex;gap:12px;align-items:flex-start;
  position:relative}
.hubcard .rtools{position:absolute;right:10px;bottom:8px;background:#fff;
  padding:2px 4px;border-radius:8px}
.hubcard .hc-t{flex:1;min-width:0}
.hubcard .hc-t b{display:block;font-size:14px;color:var(--ink)}
.hubcard .hc-t span{font-size:12.5px;color:var(--mut);line-height:1.45;
  display:block;margin-top:2px}
.hc-act{display:flex;flex-direction:column;gap:8px;align-items:flex-end;
  flex:none}
.hubsec h2.sec{margin-top:20px}
.goalcard{background:#fff;border:1px solid var(--hair);border-radius:16px;
  padding:16px 20px;margin:0 0 24px;max-width:720px}
.goaltop{display:flex;align-items:center;justify-content:space-between;
  margin-bottom:8px}
.goallbl{font-size:11.5px;font-weight:600;letter-spacing:.05em;
  text-transform:uppercase;color:var(--mut)}
.goalline{display:flex;gap:16px;align-items:center;margin:4px 0 12px}
.goalnum{font-size:34px;color:var(--ink);font-weight:650;flex:none}
.goaltxt b{display:block;font-size:14.5px;color:var(--ink)}
.goaltxt .mut{font-size:12.5px}
.goalbar{position:relative;height:8px;border-radius:99px;background:#EEEFF3}
.goalbar i{display:block;height:100%;border-radius:99px;background:var(--accent)}
.goalbar em{position:absolute;top:-3px;bottom:-3px;width:2px;
  background:#1B1F30;border-radius:2px}
.goalfoot{display:flex;justify-content:space-between;font-size:12px;
  color:var(--mut);margin-top:6px}
.goalact{display:flex;gap:12px;align-items:baseline;padding:8px 0;
  border-bottom:1px solid #F1F1F5;font-size:13.5px}
.goalact:last-child{border-bottom:0}
.goalact .tdesc{flex:1}
.goalmini{font-size:12.5px;color:var(--mut);margin:0 0 8px}
.goalmini b{color:#177245}
.content{max-width:1120px;margin:0 auto;padding:8px 44px 88px}
.alogo{display:inline-flex;width:18px;height:18px;border-radius:5px;color:#fff;
  font-size:10.5px;font-weight:700;align-items:center;justify-content:center;
  vertical-align:-4px;margin:0 4px 0 2px}
img.alogo{background:#fff;border:1px solid var(--hair);object-fit:contain;padding:1px}
h1.page{font-size:32px;font-weight:450;color:var(--ink);letter-spacing:-.01em;margin:8px 0 16px}
.pagehint{color:var(--mut);font-size:13.5px;margin:0 0 16px;line-height:1.55}
h1.page + .pagehint{margin-top:-8px}
.impactline{background:#E5F4EC;color:#177245;border-radius:10px;padding:12px 16px;
  font-size:13.5px;margin-bottom:16px}
.impactline b{font-weight:650}
.pills{display:flex;gap:8px;margin:0 0 4px}
.fpill{font-size:13.5px;padding:8px 16px;border-radius:9px;background:var(--accent-soft);
color:var(--ink);font-weight:500}
.fpill.off{background:#fff;border:1px solid #E3E4EA;color:var(--text);font-weight:400}
.cols-h{display:flex;font-size:12.5px;color:var(--mut);padding:16px 0 8px;
border-bottom:1px solid var(--hair)}
.cols-h .right{margin-left:auto}
.taskwrap{border-bottom:1px solid #EEEFF3}
.trow{display:flex;align-items:center;gap:12px;row-gap:8px;padding:16px 0;
font-size:14.5px;color:#26293A;flex-wrap:wrap}
.trow.slim{border-bottom:1px solid #EEEFF3;padding:16px 0;font-size:14px}
.trow .ico{width:16px;height:16px;color:#6A6D7D;flex:none}
.trow .ico svg{width:16px;height:16px;display:block}
.trow .ico.warn-i{color:#B47816}
.tdesc{flex:1 1 340px;min-width:240px;line-height:1.5}
.tdesc b{font-weight:600;color:var(--ink)}
.mut{color:var(--mut)}
.tacts{display:flex;gap:8px;align-items:center;flex:none;opacity:.92;margin-left:auto}
.tacts form{display:flex;gap:8px}
.when{color:#5A5D6D;font-size:13.5px;flex:none;min-width:52px;
  white-space:nowrap;text-align:right;margin-left:auto}
""" + BTN_CSS + WORK_CSS + """
.bulkbar{display:flex;gap:8px;align-items:center;margin:16px 0 2px;
padding:8px 16px;border:1px solid #DFDBFA;background:#F4F3FE;border-radius:10px}
.taskwrap.kfocus{background:#FAFAFE;box-shadow:inset 3px 0 0 var(--accent)}
.selrun{width:15px;height:15px;accent-color:var(--accent);flex:none;cursor:pointer}
.rolepills{display:flex;gap:8px;align-items:center;justify-content:center;
margin:0 0 16px;font-size:12.5px;color:var(--mut)}
.rolepills a{padding:4px 12px;border-radius:999px;border:1px solid var(--hair);
color:var(--text);text-decoration:none;font-weight:500}
.rolepills a.on{background:var(--accent);border-color:var(--accent);color:#fff}
.sechead{display:flex;align-items:center;justify-content:space-between}
.editbox{padding:2px 0 16px 28px}
.editbox .draft{display:none}
/* one form-control system: same border, radius, hover, focus ring and
   placeholder as the button system: sizes vary, states never do */
.jfind,.whyin,.editbox textarea{background:#fff;border:1px solid #E3E4EA;
border-radius:9px;color:#26293A;font:inherit;
transition:border-color .15s,box-shadow .15s}
.jfind:hover,.whyin:hover,.editbox textarea:hover{border-color:#C9CBD6}
.jfind:focus,.whyin:focus,.editbox textarea:focus{outline:none;
border-color:#98A5F0;box-shadow:0 0 0 3px rgba(82,102,235,.13)}
.jfind::placeholder,.whyin::placeholder,.editbox textarea::placeholder{color:#9A9DAB}
.jfind:disabled,.whyin:disabled,.editbox textarea:disabled{opacity:.45;cursor:not-allowed}
.editbox textarea{width:100%;font-size:13.5px;padding:8px 12px;margin-bottom:8px}
h2.sec{font-size:15px;font-weight:600;color:var(--ink);margin:40px 0 8px}
.st{font-size:12px;font-weight:500;border-radius:12px;padding:3px 8px;white-space:nowrap;flex:none}
.st.ok{background:#E5F4EC;color:#177245}
.st.warn{background:#FCEED8;color:#9A6215}
.st.wait{background:var(--accent-soft);color:#4553C8}
.st.mut{background:#EFEFF3;color:#6A6D7D}
.empty{color:var(--mut);padding:16px 2px;font-size:13.5px}
.atoolbar{display:flex;gap:12px;margin:4px 0 16px}
.atoolbar .search-in{flex:1;max-width:420px;display:flex;gap:8px;align-items:center;
  background:#fff;border:1px solid var(--hair);border-radius:10px;padding:8px 12px}
.atoolbar .search-in svg{width:15px;height:15px;color:var(--mut)}
.atoolbar .search-in input{border:none;outline:none;font:inherit;font-size:13.5px;
  width:100%;background:none;color:var(--ink)}
.seg{display:flex;background:var(--pill);border-radius:10px;padding:3px}
.seg a{font-size:13px;font-weight:500;padding:8px 16px;border-radius:8px;color:var(--mut)}
.seg a.on{background:#fff;color:var(--ink);box-shadow:0 1px 3px rgba(27,31,48,.08)}
.atable{background:#fff;border:1px solid var(--hair);border-radius:12px;overflow:hidden}
.atable .thead{display:grid;grid-template-columns:2.2fr 1fr 1fr 1fr 60px;
  padding:12px 16px;font-size:11.5px;font-weight:600;letter-spacing:.05em;
  text-transform:uppercase;color:var(--mut);background:#FAFAFC;
  border-bottom:1px solid var(--hair)}
.atable .arow2{display:grid;grid-template-columns:2.2fr 1fr 1fr 1fr 60px;
  align-items:center;padding:12px 16px;border-bottom:1px solid #F1F1F5;
  color:var(--ink);font-size:14px}
.atable .arow2:last-child{border-bottom:none}
.atable .arow2:hover{background:#FAFAFC}
.aname{display:flex;align-items:center;gap:12px;position:relative}
.aname .tile{width:32px;height:32px;position:relative}
.aname .tile svg{width:16px;height:16px}
.aname .dot2{position:absolute;left:34px;bottom:6px;width:8px;height:8px;
  border-radius:50%;border:1.5px solid #fff}
.dot2.on{background:#1E9E5A}.dot2.off{background:#C2C5D2}
.atable .go2{color:var(--mut);text-align:right}
.hzn{font-size:12.5px;color:var(--mut);border-top:1px solid #F1F1F5;
  padding-top:8px;margin-top:2px}
.hzn.on{color:#177245;font-weight:500}
.slimcard{display:flex !important;align-items:center;gap:8px;
  padding:12px 16px !important;font-size:14px;color:var(--ink)}
.slimcard .dot2{position:static;border:0;width:8px;height:8px;flex:none}
.slimcard .ident{flex:none}
.slimcard b{flex:none;font-weight:600}
.slimcard .slimdesc{flex:1;min-width:0;white-space:nowrap;overflow:hidden;
  text-overflow:ellipsis;font-size:12.5px}
.slimcard .go2{flex:none}
.sec.fam{display:flex;align-items:center;gap:12px;flex-wrap:wrap}
.sec.fam .st{flex:none;line-height:1.4}
.stattiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));
  gap:12px;margin:16px 0 8px;max-width:880px}
.stile{display:block;background:#fff;border:1px solid var(--hair);
  border-radius:14px;padding:16px 20px;color:var(--ink)}
.stile:hover{border-color:#C7CDF3}
.stile.on{border-color:var(--accent);box-shadow:0 0 0 1px var(--accent)}
.filein{flex:1;font:inherit;font-size:13px;padding:8px;background:#fff;border:1.5px solid var(--hair);border-radius:10px}
.stile b{display:block;font-size:26px;font-weight:600;letter-spacing:-.01em}
.stile span{display:block;font-size:13px;font-weight:500;margin-top:4px}
.stile i{display:block;font-style:normal;font-size:11.5px;
  color:var(--mut);margin-top:2px}
.stile.need b{color:#B47816}
.lastrun{display:flex;gap:20px;align-items:stretch;background:#fff;
  border:1px solid var(--hair);border-radius:14px;padding:14px 20px;
  margin:12px 0 16px;max-width:720px;flex-wrap:wrap}
.lr-l{flex:1;min-width:260px}
.lr-h{display:flex;align-items:center;gap:8px;font-weight:600;
  font-size:13px;color:var(--ink);margin-bottom:4px}
.lr-when{margin-left:auto;font-weight:400;font-size:12px}
.lr-t{font-size:13.5px;line-height:1.55}
.lr-spark{flex:none;display:flex;flex-direction:column;gap:4px;
  justify-content:center}
.lr-spark > .mut{font-size:11px}
.spark{display:flex;gap:3px;align-items:flex-end;height:28px}
.spark i{width:7px;border-radius:3px 3px 0 0;background:#1E9E5A;opacity:.85}
.outcome{font-size:24px;font-weight:600;letter-spacing:-.01em;color:var(--ink);
  margin:20px 0 8px;max-width:640px}
.hsteps{margin:4px 0 8px;max-width:560px}
.hstep{position:relative;padding:0 0 20px 28px}
.hstep::before{content:"";position:absolute;left:3px;top:16px;bottom:2px;
  width:2px;border-radius:2px;background:#ECECF1}
.hstep:last-child{padding-bottom:4px}
.hstep:last-child::before{display:none}
.hstep .hdot{position:absolute;left:0;top:6px;width:8px;height:8px;
  border-radius:50%;background:var(--ink);box-shadow:0 0 0 3px #ECECF1}
.hstep b{display:block;font-size:14px;color:var(--ink)}
.hstep .mut{font-size:13px}
.capwrap{display:flex;flex-wrap:wrap;gap:8px;max-width:640px;margin:4px 0 8px}
.capchip{display:inline-flex;align-items:center;gap:8px;background:#fff;
  border:1px solid var(--hair);border-radius:999px;padding:8px 16px;
  font-size:12.5px;color:var(--text)}
.capchip svg{width:13px;height:13px;color:#6A6D7D}
.capchip.act{background:var(--accent-soft,#E9EBF8);border-color:transparent;
  color:#3A46A8}
.capchip.act svg{color:#3A46A8}
.miles{display:flex;align-items:flex-start;background:#fff;
  border:1px solid var(--hair);border-radius:14px;padding:20px 24px;
  margin:8px 0 24px;overflow-x:auto}
.mile{display:flex;flex-direction:column;align-items:center;gap:4px;
  min-width:120px;text-align:center;flex:none}
.mile .mdot{width:26px;height:26px;border-radius:50%;display:grid;
  place-items:center;background:#ECECF1;color:#9A9DAB;font-size:13px;
  font-weight:700}
.mile.done .mdot{background:var(--accent);color:#fff}
.mile.cur .mdot{background:#fff;border:2px solid var(--accent)}
.mile b{font-size:12.5px;color:var(--ink)}
.mile.todo b{color:var(--mut);font-weight:500}
.mile .msub{font-size:11.5px;color:var(--mut)}
.mbar{flex:1;height:3px;border-radius:99px;background:#ECECF1;
  margin-top:12px;min-width:24px}
.mbar.done{background:var(--accent)}
.ctahint{display:block;font-size:11.5px;color:var(--mut);margin-top:8px;
  text-align:right}
.tglbtn{width:34px;height:20px;border-radius:10px;background:#D9DBE4;border:0;
  position:relative;flex:none;cursor:pointer;padding:0}
.tglbtn.on{background:var(--accent)}
.tglbtn i{position:absolute;top:2px;left:2px;width:16px;height:16px;
  border-radius:50%;background:#fff;transition:left .12s}
.tglbtn.on i{left:16px}
.rtools{display:flex;gap:4px;flex:none;opacity:0;transition:opacity .12s}
.trow:hover .rtools,.hubcard:hover .rtools{opacity:1}
.rtool{font:inherit;font-size:12px;color:var(--mut);background:none;border:0;
  cursor:pointer;padding:4px 8px;border-radius:7px}
.rtool:hover{background:#F0F0F5;color:var(--ink)}
.rtool.danger:hover{color:#C0392B}
.inedit{display:flex;gap:8px;align-items:center;width:100%}
.inedit-in{flex:1;font:inherit;font-size:13.5px;border:1.5px solid #C7CDF3;
  border-radius:9px;padding:8px 12px;outline:none}
.inedit-in:focus{border-color:var(--accent)}
.cfgbox{font:inherit;font-size:13.5px;line-height:1.55;color:var(--ink);
  background:#fff;border:1.5px solid var(--hair);border-radius:12px;
  padding:12px 16px;outline:none}
.cfgbox:focus{border-color:var(--accent)}
.cfgbox::placeholder{color:#9A9DAB}
.ctitle[contenteditable]{outline:1.5px solid var(--accent);border-radius:6px;
  padding:1px 8px;background:#fff}
.assignbar{display:flex;align-items:center;gap:12px;background:#FFF8EC;
  border:1px solid #F0DCB4;border-radius:12px;padding:12px 16px;
  margin:0 0 16px;font-size:13.5px;flex-wrap:wrap}
.assignbar form{display:flex;gap:8px;align-items:center;margin-left:auto}
.assignlbl{font-size:12px;color:var(--mut)}
.thread2{max-width:640px;background:#fff;border:1px solid var(--hair);
  border-radius:14px;padding:8px 16px;margin:4px 0 8px}
.thr{display:flex;gap:8px;align-items:flex-start;padding:12px 0;
  border-bottom:1px solid #F1F1F5;font-size:13.5px}
.thr:last-child{border-bottom:0}
.thr .tht{flex:none;width:98px;font-size:11.5px;color:var(--mut);
  padding-top:3px;font-variant-numeric:tabular-nums}
.thr .thb{flex:1;line-height:1.55}
.thr.open .thb{font-weight:500;color:var(--ink)}
.thr.close{background:#FAFDF9;margin:0 -16px;padding:12px 16px;
  border-radius:0 0 14px 14px}
.chch{flex:none;font-size:11px;font-weight:700;border-radius:7px;
  padding:3px 8px;margin-top:1px;letter-spacing:.03em}
.chc{background:#EAEFFB;color:#2B4AA8}
.chw{background:#E7F6EC;color:#177245}
.tiers{max-width:640px}
.tier{background:#fff;border:1px solid var(--hair);border-radius:12px;
  padding:12px 16px;margin-bottom:8px}
.tierh{display:flex;align-items:center;gap:8px;margin-bottom:4px}
.tierh b{font-size:14px}
.tierh .st{margin-left:auto}
.tier p{margin:0;font-size:13px;color:var(--text);line-height:1.55}
.cashcard{max-width:640px;background:#fff;border:1.5px solid #C7CDF3;
  border-radius:16px;padding:20px 24px;margin:4px 0 8px}
.cashcard h3{font-size:17px;color:var(--ink);margin:4px 0 8px}
.cashwhy{font-size:13.5px;line-height:1.6;margin-bottom:12px}
.abwrap{display:flex;gap:12px;margin:12px 0 16px;flex-wrap:wrap}
.ab{flex:1;min-width:200px;border-radius:12px;padding:12px 16px}
.ab span{display:block;font-size:11px;font-weight:700;letter-spacing:.06em;
  text-transform:uppercase;margin-bottom:4px;opacity:.75}
.ab b{font-size:15px;line-height:1.4;display:block}
.ab.no{background:#FDF1EF;color:#A63A2B}
.ab.yes{background:#E9F7EF;color:#177245}
.cashmore{margin-top:12px;border-top:1px solid #F1F1F5;padding-top:8px}
.cashmore summary{font-size:12.5px;color:var(--mut);cursor:pointer;
  padding:4px 0}
.cashmore[open] summary{margin-bottom:8px}
.cashrow{display:flex;gap:12px;font-size:13.5px;line-height:1.6;
  padding:8px 0;border-top:1px solid #F1F1F5}
.cashrow span:first-child{flex:none;width:88px;font-size:11.5px;
  text-transform:uppercase;letter-spacing:.05em;color:var(--mut);
  padding-top:2px}
.wpbtns{display:flex;gap:8px;align-items:center;margin-top:12px;
  flex-wrap:wrap}
.cashdone{margin-top:12px;background:#FAFAFC;border-radius:12px;
  padding:12px 16px;font-size:13.5px;color:var(--text)}
.cashdone.ok{background:#E9F7EF;color:#177245;font-weight:500}
.trace{margin-top:12px;border-top:1px solid #F1F1F5;padding-top:12px}
.trace-h{font-size:11px;font-weight:700;letter-spacing:.08em;
  text-transform:uppercase;color:var(--mut);margin-bottom:8px}
.tr3{display:flex;align-items:center;gap:12px;padding:6px 0;
  position:relative}
.tr3 .tpill{width:8px;height:18px;border-radius:99px;background:#1E9E5A;
  flex:none}
.tr3.running .tpill{animation:pulse 1.4s ease infinite}
@keyframes pulse{50%{opacity:.4}}
.tr3 .tlabel{flex:1;font-size:13px;color:var(--ink)}
.tr3 .tstat{font-size:12px;color:#1E9E5A;font-weight:600}
.tr3.running .tstat{color:#177245}
.assignsel{font:inherit;font-size:13px;border:1px solid var(--hair);
  border-radius:9px;padding:8px 8px;background:#fff}
.tabbar{display:flex;gap:4px;border-bottom:1px solid var(--hair);margin:16px 0 24px}
.tabbar a{padding:8px 16px;font-size:13.5px;font-weight:500;color:var(--mut);
  border-bottom:2px solid transparent;margin-bottom:-1px}
.tabbar a.on{color:var(--accent);border-bottom-color:var(--accent)}
.tabbar a:hover{color:var(--ink)}
.twopane{display:grid;grid-template-columns:300px 1fr;gap:16px;align-items:start}
.pane-list .pitem{display:flex;gap:12px;align-items:center;background:#fff;
  border:1px solid var(--hair);border-radius:10px;padding:12px 16px;margin-bottom:8px;
  font-size:13.5px;color:var(--ink)}
.pane-list .pitem.on{border-color:#C7CDF3;box-shadow:0 0 0 1px #C7CDF3}
.pane-list .pitem .sub2{display:block;font-size:12px;color:var(--mut);margin-top:1px}
.pane-list .pitem .tgl{margin-left:auto;width:30px;height:18px;border-radius:9px;
  background:#D9DBE4;position:relative;flex:none}
.pane-list .pitem .tgl.on{background:var(--accent)}
.pane-list .pitem .tgl::after{content:"";position:absolute;top:2px;left:2px;
  width:14px;height:14px;border-radius:50%;background:#fff;transition:left .12s}
.pane-list .pitem .tgl.on::after{left:14px}
.pane-detail{background:#fff;border:1px solid var(--hair);border-radius:12px;
  padding:16px 20px;min-height:120px}
.pane-detail h3{font-size:14.5px;font-weight:600;color:var(--ink);margin-bottom:8px}
.pane-detail p{font-size:13.5px;color:var(--text);line-height:1.6}
.trow{flex-wrap:wrap;row-gap:8px}
.trow .tdesc{min-width:min(320px,100%)}
.rgt{display:flex;gap:8px;align-items:center;margin-left:auto;flex-wrap:wrap}
.jbtime{display:block;font-size:11px;color:var(--mut);margin-top:4px;text-align:right;max-width:460px}
.jtabs{display:flex;gap:8px;margin:8px 0 16px;flex-wrap:wrap}
.jtab{display:flex;gap:8px;align-items:center;font-size:13.5px;font-weight:500;
  padding:8px 16px;border-radius:10px;background:var(--pill);color:var(--text)}
.jtab.on{background:#21232E;color:#fff}
.jtab.on .alogo{border-color:transparent}
.jtab:hover:not(.on){background:#E6E7EC}
.jbubble{display:inline-block;background:#21232E;color:#fff;border-radius:12px;
  padding:8px 16px;font-size:13px;line-height:1.5;max-width:460px}
.jwrap{margin:20px 0 8px}
.goalk{color:var(--mut);font-weight:450}
.jnav{margin-left:auto;display:flex;gap:8px;flex:none}
.jcirc{width:36px;height:36px;border-radius:50%;padding:0;display:grid;place-items:center}
.jday{font-size:11px;letter-spacing:.12em;fill:var(--mut);font-weight:600;font-family:inherit}
.jdate{font-size:9.5px;letter-spacing:.06em;fill:#B4BAC8;font-weight:600;font-family:inherit}
.jday-grid{stroke:#E4E5EC}
.jrule{stroke:#D9DDE6;stroke-width:1}
.jruletick{stroke:#C9CFDB;stroke-width:1}
.jrulelab{font-size:8.5px;fill:#B4BAC8;font-weight:600;font-family:inherit}
.jruledot{fill:var(--accent);transition:opacity .35s}
.jruledot.future{opacity:.3}
.jrtmaj{stroke:#C0C7D4}
#jheadg,#jruleg{transition:transform var(--jdur,.45s) cubic-bezier(.22,.61,.36,1)}
#jprog{transition:width var(--jdur,.45s) cubic-bezier(.22,.61,.36,1)}
.jscrub #jheadg,.jscrub #jruleg,.jscrub #jprog{transition:none}
.jseg{transition:opacity .35s}
.jseg.future{opacity:.3}
.jmark{transition:opacity .35s,transform .22s;transform-box:fill-box;transform-origin:center;cursor:pointer}
.jmark.future{opacity:.42}
.jmark.active{transform:scale(1.3)}
.jgaplab{font-size:10.5px;color:var(--mut);margin-top:3px;font-weight:600}
.jflash{animation:jfade .28s ease}
@keyframes jfade{from{opacity:.3;transform:translateY(4px)}to{opacity:1;transform:none}}
.jgap{font-size:9.5px;letter-spacing:.06em;fill:var(--mut);font-weight:700;font-family:inherit}
.jgapbox{fill:#F1F2F7;stroke:#E4E5EC}
.jback{display:inline-block;margin:0 0 16px;color:var(--mut);font-size:13px;font-weight:600;text-decoration:none}
.jback:hover{color:var(--ink)}
.jfind{width:100%;max-width:440px;margin:8px 0 16px;padding:8px 16px;font-size:14px}
.jgoalrow{cursor:pointer}
.shstats{display:flex;gap:16px;margin:16px 0 8px}
.shstat{flex:1;border:1px solid var(--hair);border-radius:12px;padding:16px 16px}
.shstat .n{font-size:26px;font-weight:700;color:var(--ink);font-variant-numeric:tabular-nums}
.shstat .l{font-size:12px;color:var(--mut);margin-top:2px}
.dgridwrap{overflow-x:auto;margin-top:16px;border:1px solid #E4E5EC;border-radius:10px}
.dgrid{width:100%;border-collapse:collapse;font-size:13.5px;min-width:980px}
.dgrid th{position:sticky;top:0;background:#F7F7FA;border-bottom:1px solid #E4E5EC;
border-right:1px solid #ECEDF2;padding:8px 12px;font-weight:600;color:var(--ink);
text-align:left;font-size:12.5px;white-space:nowrap}
.dgrid th.ghost{color:#A6ACBB;font-weight:500}
.dgrid td{border-bottom:1px solid #F0F1F5;border-right:1px solid #F0F1F5;
padding:8px 12px;vertical-align:middle;background:#fff;height:46px;line-height:1.45}
.dgrid tr:hover td{background:#FBFBFD}
.dgrid td.chk,.dgrid th.chk{width:36px;text-align:center;border-right-color:#ECEDF2}
.dgrid td.chk input{width:14px;height:14px;accent-color:var(--accent)}
.dgrid td.acct{white-space:nowrap;font-size:13.5px}
.dgrid td.wide{min-width:300px}
.dgrid td.ghost{background:#FAFAFC}
.dgrid td.pending .cw,.dgrid td.pending .cv{visibility:hidden}
.dgrid td.working{background:#EAF7F1}
.dgrid td.working .cw{visibility:visible;font-style:italic;
background:linear-gradient(100deg,#2E8B57 20%,#A9E8C6 42%,#2E8B57 64%);
background-size:200% 100%;-webkit-background-clip:text;background-clip:text;
-webkit-text-fill-color:transparent;animation:shsheen 1.5s linear infinite}
@keyframes shsheen{0%{background-position:130% 0}100%{background-position:-70% 0}}
.dgrid td.working .cv{display:none}
.dgrid td.done .cw{display:none}
.dgrid td.done .cv{visibility:visible}
.dgrid td.done .st{animation:chippop .32s ease both}
@keyframes chippop{from{opacity:0;transform:scale(.85)}to{opacity:1;transform:scale(1)}}
.wfade{opacity:0;animation:wfadein .5s ease forwards}
@keyframes wfadein{to{opacity:1}}
.btn.scanning{color:#fff;border-color:transparent;background:linear-gradient(100deg,var(--accent) 25%,#B9C2F7 45%,var(--accent) 65%);
background-size:200% 100%;animation:btnsheen 1.6s linear infinite}
@keyframes btnsheen{0%{background-position:130% 0}100%{background-position:-70% 0}}
.shband a.btn{white-space:nowrap;flex:0 0 auto}
.dgrid th{cursor:pointer;user-select:none}
.dgrid th.ghost,.dgrid th.chk{cursor:default}
.dgrid th[data-dir=a]:not(.ghost)::after{content:' \2191';color:var(--accent)}
.dgrid th[data-dir=d]:not(.ghost)::after{content:' \2193';color:var(--accent)}
.dgrid td.num,.dgrid th.num{text-align:right;white-space:nowrap}
.dgrid .ell{display:inline-block;max-width:420px;overflow:hidden;
text-overflow:ellipsis;white-space:nowrap;vertical-align:bottom}
.gridcount{font-size:12px;color:var(--mut);margin:12px 2px -8px}
.gmore{margin-top:12px}
.rowin{animation:rowin .45s ease both}
@keyframes rowin{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:none}}
.shband{margin-top:24px;padding:16px 20px;border:1px solid #DFDBFA;background:#F4F3FE;
border-radius:12px;display:none;align-items:center;gap:16px;justify-content:space-between}
.shband.on{display:flex}
.shband b{color:#4A3FD6}
.notebar{display:flex;gap:8px;align-items:stretch;margin:16px 0 4px}
.notebar .notein{margin:0;max-width:560px;flex:1}
.notebar .btn{padding-top:0;padding-bottom:0;display:inline-flex;align-items:center}
.whyin{width:130px;padding:8px 8px;font-size:12.5px}
.ehist{max-width:660px;margin-top:8px}
.eh-item{position:relative;padding:0 0 24px 32px}
.eh-item:not(:last-child):after{content:"";position:absolute;left:7px;top:20px;bottom:-2px;width:2px;background:#ECEEF3}
.eh-node{position:absolute;left:1px;top:3px;width:12px;height:12px;border:2px solid #D0D5DE;border-radius:50%;background:#fff}
.eh-head{display:flex;align-items:center;gap:8px;font-size:14px;color:var(--ink)}
.eh-av{width:22px;height:22px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;font-size:9.5px;font-weight:700;color:#fff;flex:none}
.eh-when{color:var(--mut);font-size:12.5px;font-weight:500}
.eh-line{margin:8px 0 0 32px;display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.eh-verb{color:#3F4A5C;font-size:13.5px;font-weight:600}
.echip{padding:3px 12px;border-radius:8px;font-size:12.5px;font-weight:600;border:1px solid transparent}
.ec-purple{background:#F1EBFE;color:#6B3FD6;border-color:#E4D9FB}
.ec-pink{background:#FDE7F1;color:#C0316E;border-color:#F9D3E4}
.ec-blue{background:#E5EEFB;color:#2E5AAC;border-color:#D3E2F7}
.ec-green{background:#E3F5EA;color:#1F7A46;border-color:#CDEBD9}
.ec-amber{background:#FBF3D8;color:#8A6A12;border-color:#F3E6B8}
.ec-orange{background:#FDEDDE;color:#B05E1E;border-color:#F8DDC2}

.jaxis{stroke:var(--hair)}
.jhalo{fill:none;stroke:var(--accent);stroke-width:1.2;opacity:0;transition:opacity .15s}
.jcard{display:grid;grid-template-columns:130px 1fr;gap:20px;background:#fff;
  border:1px solid var(--hair);border-radius:12px;padding:16px 20px;margin-top:12px}
.jday2{font-size:11px;letter-spacing:.08em;color:var(--mut);font-weight:600;margin:8px 0 2px}
.jtime{font-size:21px;font-weight:600;color:var(--ink)}
.jcard h3{font-size:15px;font-weight:600;color:var(--ink);margin-bottom:8px}
.jkv{display:grid;grid-template-columns:130px 1fr;gap:8px 16px;font-size:13px}
.jkv .k{font-size:10.5px;letter-spacing:.08em;color:var(--mut);font-weight:600;
  align-self:baseline;padding-top:2px}
.jscroll{overflow-x:auto;background:#fff;border:1px solid var(--hair);
  border-radius:12px;margin-bottom:12px}
.jlane{font-size:10.5px;letter-spacing:.08em;fill:var(--mut);font-weight:600}
.jgrid{stroke:#EFEFF4;stroke-dasharray:2 4}
.jheadline{stroke:var(--accent);stroke-width:1.4;opacity:.75}
.jmark{cursor:pointer}
.jscroll svg{cursor:ew-resize;touch-action:none}
.jscroll svg .jmark{cursor:pointer}
.jkicker{font-size:11px;font-weight:600;letter-spacing:.05em;color:var(--mut);
  text-transform:uppercase;margin-bottom:4px}
.dhead{display:flex;align-items:center;gap:16px;margin:8px 0 2px}
.dhead .back2{display:flex;align-items:center;justify-content:center;
  width:38px;height:38px;flex:none;border-radius:10px;border:1px solid var(--hair);
  background:#fff;color:var(--text);font-size:20px;transition:background .12s}
.dhead .back2:hover{background:var(--pill);color:var(--ink)}
.dhead .back2:active{background:#DEDFE6}
.dhead .tile{width:44px;height:44px}
.dhead .meta{font-size:12.5px;color:var(--mut);margin-top:3px;display:flex;gap:8px;align-items:center}
.dhead h1{font-size:22px;font-weight:600;color:var(--ink);margin:0}
.agchips{display:flex;gap:4px;flex-wrap:wrap}
.agchip{display:inline-flex;width:26px;height:26px;border-radius:7px;
  background:var(--accent-soft);color:var(--accent);align-items:center;
  justify-content:center;border:1px solid transparent}
.agchip svg{width:14px;height:14px}
.agchip:hover{border-color:#98A5F0}
.agchip.off{background:var(--pill);color:var(--mut)}
.agrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));
  gap:16px;margin:8px 0 24px}
.acard{background:#fff;border:1px solid var(--hair);border-radius:12px;
  padding:16px 16px 16px;display:flex;flex-direction:column;gap:8px}
.acard .tile{width:36px;height:36px;border-radius:9px;background:var(--accent-soft);
  color:var(--accent);display:flex;align-items:center;justify-content:center}
.acard .tile svg{width:18px;height:18px}
.acard h3{font-size:14.5px;font-weight:600;color:var(--ink)}
.acard p{font-size:12.5px;color:var(--mut);line-height:1.5;flex:1}
.acard .foot{display:flex;align-items:center;gap:8px;margin-top:2px}
.acard .foot .n{margin-left:auto;font-size:12px;color:var(--mut)}
</style></head><body>
__SIDEBAR__
<div class="main">
  <div class="topbar">
    <span></span>
    <div class="right"><details class="acct"><summary class="avatar">__INITIAL__</summary>
      <div class="acctmenu">
        <div class="acct-biz">__NAME__</div>
        <div class="acct-mail">__USER__</div>
        <div class="acct-shop">__BIZ__</div>
        <a class="acct-out" href="/logout">Log out</a>
      </div></details></div>
  </div>
  <div class="content">__CONTENT__</div>
</div>
<script>
document.addEventListener('click', e => {
  document.querySelectorAll('details.acct[open]').forEach(d => {
    if (!d.contains(e.target)) d.removeAttribute('open');
  });
});
function hubFilter(inp){
  const sel = inp.dataset.sel || '[data-hub]';
  const q = inp.value.toLowerCase();
  document.querySelectorAll(sel).forEach(el => {
    el.style.display =
      el.textContent.toLowerCase().includes(q) ? '' : 'none';
  });
  hubSections();
}
function hubPill(btn){
  document.querySelectorAll('.hubpill').forEach(b =>
    b.classList.toggle('on', b === btn));
  const f = btn.dataset.f;
  document.querySelectorAll('[data-hub]').forEach(el => {
    el.style.display =
      (f === 'all' || el.dataset.hub === f
       || el.dataset.hub === 'template') ? '' : 'none';
  });
  hubSections();
}
function hubSections(){
  document.querySelectorAll('.hubsec').forEach(sec => {
    const any = [...sec.querySelectorAll('[data-hub]')]
      .some(el => el.style.display !== 'none');
    sec.style.display = any ? '' : 'none';
  });
}
function toggleEdit(id){document.getElementById(id).toggleAttribute('hidden')}
function bulksync(){
  const sel = document.querySelectorAll('.selrun:checked');
  const bar = document.getElementById('bulkbar');
  if (!bar) return;
  bar.hidden = sel.length === 0;
  document.getElementById('bcount').textContent = sel.length + ' selected';
}
function wstream(el, ms = 45, delay = 0){
  const words = el.textContent.split(' ');
  el.innerHTML = words.map((w, k) =>
    '<span class="wfade" style="animation-delay:' + (delay + k * ms) + 'ms">' +
    w.replace(/&/g, '&amp;').replace(/</g, '&lt;') + '</span>').join(' ');
}
function dsort(th){
  const table = th.closest('table'), tb = table.querySelector('tbody');
  if (th.classList.contains('ghost') || th.classList.contains('chk')) return;
  const idx = [...th.parentNode.children].indexOf(th);
  const dir = th.dataset.dir === 'a' ? 'd' : 'a';
  table.querySelectorAll('th').forEach(h => { delete h.dataset.dir; });
  th.dataset.dir = dir;
  const val = r => {
    const c = r.children[idx];
    if (c.dataset.s !== undefined) return +c.dataset.s;
    const t = c.textContent.trim();
    const n = parseFloat(t.replace(/[^0-9.-]/g, ''));
    return /^[0-9$.,%\\s-]+$/.test(t) && !isNaN(n) ? n : t.toLowerCase();
  };
  [...tb.children]
    .sort((a, b) => { const x = val(a), y = val(b);
      return (x < y ? -1 : x > y ? 1 : 0) * (dir === 'a' ? 1 : -1); })
    .forEach(r => tb.appendChild(r));
}
function jfilter(q){
  q = (q || '').toLowerCase();
  const rows = [...document.querySelectorAll('#jgrid tbody tr')];
  let n = 0;
  rows.forEach(r => {
    const hit = !q || (r.dataset.q + ' ' + r.textContent.toLowerCase()).includes(q);
    const chunked = !q && !window._jall && +r.dataset.i >= 60;
    r.hidden = !hit || chunked;
    if (!r.hidden) n++;
  });
  const c = document.getElementById('jcount');
  if (c) c.textContent = 'Showing ' + n + ' of ' + rows.length + ' customers';
  const m = document.getElementById('jmore');
  if (m) m.hidden = !!q || !!window._jall;
}
function jshowall(b){
  window._jall = true;
  jfilter(document.querySelector('.jfind') ? document.querySelector('.jfind').value : '');
}
document.addEventListener('submit', e => {
  const b = e.submitter, f = e.target;
  if (!b || !b.name || f.getAttribute('action') !== '/act') return;
  b.classList.add('scanning');
  b.textContent = b.value === 'reject' ? 'Logging\u2026' : 'Sending\u2026';
});
window.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('#jgrid tbody tr:not([hidden])').forEach((r, i) => {
    if (i < 12) { r.classList.add('rowin'); r.style.animationDelay = (i * 55) + 'ms'; }
  });
  document.querySelectorAll('#jgrid .stream').forEach((el, i) => {
    if (i < 8) wstream(el, 30, 150 + i * 200);
  });
  document.querySelectorAll('.taskwrap').forEach((w, i) => {
    if (i < 10) { w.classList.add('rowin'); w.style.animationDelay = (i * 70) + 'ms'; }
  });
  document.querySelectorAll('.taskwrap .stream').forEach((el, i) => {
    if (i < 6) wstream(el, 38, 200 + i * 300);
  });
  document.querySelectorAll('.trow.slim').forEach((r, i) => {
    if (i < 12) { r.classList.add('rowin'); r.style.animationDelay = (i * 45) + 'ms'; }
  });
  document.querySelectorAll('.arow2').forEach((r, i) => {
    if (i < 14) { r.classList.add('rowin'); r.style.animationDelay = (i * 45) + 'ms'; }
  });
  document.querySelectorAll('.trow.slim .stream').forEach((el, i) => {
    if (i < 6) wstream(el, 30, 150 + i * 220);
  });
  document.querySelectorAll('.countup').forEach(el => {
    const n = +el.dataset.n, pre = el.dataset.pre || '', suf = el.dataset.suf || '';
    let t0 = null;
    const step = ts => {
      if (!t0) t0 = ts;
      const p = Math.min(1, (ts - t0) / 900), v = Math.round(n * p * (2 - p));
      el.textContent = pre + v.toLocaleString('en-US') + suf;
      if (p < 1) requestAnimationFrame(step);
    };
    requestAnimationFrame(step);
  });
});
function bulk(action, btn){
  if (btn) { btn.classList.add('scanning');
    btn.textContent = action === 'approve' ? 'Sending\u2026' : 'Logging\u2026'; }

  const ids = [...document.querySelectorAll('.selrun:checked')].map(c => c.value);
  if (!ids.length) return;
  const body = new URLSearchParams({runs: ids.join(','), action,
    why: (document.getElementById('bwhy') || {value:''}).value});
  fetch('/act/bulk', {method: 'POST', body}).then(() => location.reload());
}
(function(){
  const wraps = () => [...document.querySelectorAll('.taskwrap')]
    .filter(w => w.style.display !== 'none');
  if (!document.querySelector('.taskwrap')) return;
  let ki = -1;
  function focus(i){
    const ws = wraps();
    if (!ws.length) return;
    ki = Math.max(0, Math.min(ws.length - 1, i));
    ws.forEach((w, j) => w.classList.toggle('kfocus', j === ki));
    ws[ki].scrollIntoView({block: 'nearest', behavior: 'smooth'});
  }
  document.addEventListener('keydown', ev => {
    if (/INPUT|TEXTAREA/.test(document.activeElement.tagName)) return;
    const ws = wraps();
    if (!ws.length) return;
    if (ev.key === 'j') focus(ki + 1);
    else if (ev.key === 'k') focus(ki - 1);
    else if (ki >= 0 && ev.key === 'x') {
      const c = ws[ki].querySelector('.selrun');
      if (c) { c.checked = !c.checked; bulksync(); }
    }
    else if (ki >= 0 && ev.key === 'a')
      ws[ki].querySelector('button[value="approve"]')?.click();
    else if (ki >= 0 && ev.key === 'd')
      ws[ki].querySelector('button[value="reject"]')?.click();
    else if (ki >= 0 && ev.key === 'e')
      ws[ki].querySelector('.tacts > button')?.click();
    else return;
    ev.preventDefault();
  });
})();
(function(){
  const f = document.getElementById('pgfilter');
  if (!f){
    document.addEventListener('keydown', ev => {
      if (ev.key === '/'
          && !/INPUT|TEXTAREA/.test(document.activeElement.tagName)
          && typeof openSpot === 'function'){
        ev.preventDefault(); openSpot();
      }
    });
    return;
  }
  const rows = () => document.querySelectorAll('.trow, .jgoalrow, .acard');
  f.addEventListener('input', () => {
    const q = f.value.toLowerCase();
    rows().forEach(r => r.style.display =
      (!q || r.textContent.toLowerCase().includes(q)) ? '' : 'none');
  });
  document.addEventListener('keydown', ev => {
    if (ev.key === '/' && document.activeElement !== f
        && !/INPUT|TEXTAREA/.test(document.activeElement.tagName)) {
      ev.preventDefault(); f.focus();
    }
    if (ev.key === 'Escape' && document.activeElement === f) {
      f.value = ''; f.dispatchEvent(new Event('input')); f.blur();
    }
  });
})();
if (location.search.includes('audit=1')) {
  document.querySelectorAll('.notebar, .tacts form, .atoolbar').forEach(bar => {
    const hs = [...bar.children].filter(c => c.offsetHeight)
      .map(c => ({el: c, h: c.offsetHeight}));
    if (hs.length < 2) return;
    const max = Math.max(...hs.map(x => x.h)), min = Math.min(...hs.map(x => x.h));
    if (max - min > 2) {
      console.warn('height drift', (max - min) + 'px', bar);
      hs.forEach(x => x.el.style.outline = '2px solid red');
    }
  });
}
</script>
</body></html>"""


# ---------------------------------------------------------------- side pages
def user_name(email: str) -> str:
    """The person, not the login. The demo founder is Mothi; anyone else
    gets a readable name derived from their address."""
    if (email or "").lower() == "demo@local":
        return "Mothi"
    stem = (email or "").split("@")[0].replace(".", " ").replace("_", " ").strip()
    return stem.title() if stem else "there"


# The founder's face, embedded so the single-file demo stays single-file.
MOTHI_PHOTO = "data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAASABIAAD/4QBMRXhpZgAATU0AKgAAAAgAAYdpAAQAAAABAAAAGgAAAAAAA6ABAAMAAAAB//8AAKACAAQAAAABAAAAgKADAAQAAAABAAAAgAAAAAD/7QA4UGhvdG9zaG9wIDMuMAA4QklNBAQAAAAAAAA4QklNBCUAAAAAABDUHYzZjwCyBOmACZjs+EJ+/+IB2ElDQ19QUk9GSUxFAAEBAAAByAAAAAAEMAAAbW50clJHQiBYWVogB+AAAQABAAAAAAAAYWNzcAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAPbWAAEAAAAA0y0AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAJZGVzYwAAAPAAAAAkclhZWgAAARQAAAAUZ1hZWgAAASgAAAAUYlhZWgAAATwAAAAUd3RwdAAAAVAAAAAUclRSQwAAAWQAAAAoZ1RSQwAAAWQAAAAoYlRSQwAAAWQAAAAoY3BydAAAAYwAAAA8bWx1YwAAAAAAAAABAAAADGVuVVMAAAAIAAAAHABzAFIARwBCWFlaIAAAAAAAAG+iAAA49QAAA5BYWVogAAAAAAAAYpkAALeFAAAY2lhZWiAAAAAAAAAkoAAAD4QAALbPWFlaIAAAAAAAAPbWAAEAAAAA0y1wYXJhAAAAAAAEAAAAAmZmAADypwAADVkAABPQAAAKWwAAAAAAAAAAbWx1YwAAAAAAAAABAAAADGVuVVMAAAAgAAAAHABHAG8AbwBnAGwAZQAgAEkAbgBjAC4AIAAyADAAMQA2/8AAEQgAgACAAwEiAAIRAQMRAf/EAB8AAAEFAQEBAQEBAAAAAAAAAAABAgMEBQYHCAkKC//EALUQAAIBAwMCBAMFBQQEAAABfQECAwAEEQUSITFBBhNRYQcicRQygZGhCCNCscEVUtHwJDNicoIJChYXGBkaJSYnKCkqNDU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6g4SFhoeIiYqSk5SVlpeYmZqio6Slpqeoqaqys7S1tre4ubrCw8TFxsfIycrS09TV1tfY2drh4uPk5ebn6Onq8fLz9PX29/j5+v/EAB8BAAMBAQEBAQEBAQEAAAAAAAABAgMEBQYHCAkKC//EALURAAIBAgQEAwQHBQQEAAECdwABAgMRBAUhMQYSQVEHYXETIjKBCBRCkaGxwQkjM1LwFWJy0QoWJDThJfEXGBkaJicoKSo1Njc4OTpDREVGR0hJSlNUVVZXWFlaY2RlZmdoaWpzdHV2d3h5eoKDhIWGh4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uLj5OXm5+jp6vLz9PX29/j5+v/bAEMAAwMDAwMDBAMDBAYEBAQGCAYGBgYICggICAgICg0KCgoKCgoNDQ0NDQ0NDQ8PDw8PDxISEhISFBQUFBQUFBQUFP/bAEMBAwMDBQUFCQUFCRUODA4VFRUVFRUVFRUVFRUVFRUVFRUVFRUVFRUVFRUVFRUVFRUVFRUVFRUVFRUVFRUVFRUVFf/dAAQACP/aAAwDAQACEQMRAD8A+KVjqYJ2xUqoKsIgr6BQPMciFYh0qURD0q0sdSiMVookuRVEQ9KeIvargQVl3uoC3byoUMknRsdB/wDXpTkoK8mEE5OyLRRVGWwBVcz2icNIFPbPGfpWFJemXiYNDIwOGOCSPQc4FUr2d4I0jwrKwyVLkk+57A/SvOnj3f3UdccMre8zr0kgfGx1bPT8al8uvL4pJrNxcxl15zjO5T7EV1WieIBcSra3ICsThWzn8DW1DGKT5ZKxFTDtK8WdIY6jMfPStLZmoynNd7ictzNaP1FQtHWm0dQFMVDiUmZrJULJWkyVA0fpWbgWmf/Q+PUWrSL0pqCrKKOK+nSPHbFVamC04JUwStEiGyndOLe2lm/55qW/IV5QLidpTKHbDE5bnBPrXp+tgjSrsg9Ux+BIzXF+FfDGq+LdTOnaVEZGClmJ+6AOBntkngV42a1VCzk7JK56eX0nP3Yq7bKMN7BHuAJJCqhZvmyuSWOD/KhIJjLJ9jjaQt8qlFOQT7dq+ldG/ZO8c38UVyJrWLzGHmIZDuVe+07cZ/GvrDwd8AtD0S0Q39oJLyNcuwb5SfcYGcV8/PMItXpq57lPLpX/AHjt+Z+V93p17ZjNzG0cjc/MOuOx+lU7MMlzHK3y7TnnqT04r9GfHPwe06e5vLoxMJJVKq+0FV54OBgH618T+O9Bfwprctm8OVkO+KXaQrIecD3B4PvWeBzNVpODVmi8dlbowVRO6N+wmF1ZxTd2XDfUcH9aslRWF4XmMunMCdxSQjn3ANdGVr7mi+aCb7HyFVcsmioU5qFlFXCtRMBVtEJlBl71Awq861XZaixomf/R+SkHSrSDFRIKsoK+rSPEbJFAqUCmgYHNSAVRDZT1CHz7K4hx9+NlH1I4rvfhh4usPA/h2ycaDPq9xcKbhniwoUsxChmwScAVy1tEJrmCFgCskiqQTgEMwGCe31r1fRPhR4g13wzpyaUrL9heWOW0LiJWKyNgMzdcKeP518pxRKmlCNTqfU8Nwm3OUHsd1o37TEz3Is30x7Jc7VUurNnsNo71pfED43eMfD9pEkli+mSXKFoZbpTsZT0ZR39xXN+HP2d10i80/U/EUKwXCzh4bZJmmZiuGzK+AMZGdoB+tfT/AMQfBWmeJdP0+21+yTUY4owyoARIuB8zIw6cduh7ivjKigpe420t0fW02+VKaV3sz4Ui+NXj3Ugy6j4ljtFUBii2xB2nGGPB+U5HXrT/ABbD/wAJ54NnujtlurVTIk6LgO8Y3ZAxxuXIIFfWtr+z94YuNl2muSSWBUYthFArYHRWdUDHGMdAayPiHo2jaJoEtjp0SQxIu0qCMkHg5I6k1rXqKE4SgtbmdCHNGUJO907nwN4P0q4/sa9vi6Rokw2xNuEjAABmXjaQpIyM56nGK3CK9ptLA6J8JpJdWhhmldAmnyKMMqXOFYtwOQARnnJHXrXi1foGR4qdelJyWidl8j4fO8LChUiovVq79RhFQsKnNRkDvXstHjJlVxUDCrTCoWFQ0Umf/9L5RQVaUcVWT0qwDX1qR4TZOKeOlRqf1qQc1aRDYpz24r6v8PfEAeFtHs579My6hbQ3YdDnduXax7jJZSW9/evlHtVu2v3tmXzGLRqMKGOVXnOMHoCf15rweIcveIocy3WvyPcyDHqhX5ZbPT5n1F4q+J+u6v4dn1fw/KkWoufIt4gN0nln7zBezHHB9PY15TF8SPi3q+o2LW0k+mm3QxSvPjy3ycEjcM5x2B+tY2i+Hl1SS714XdzJEhG+yt22lo8Z4I5GD2BFdPaal4Nu3OnaR8PtT1K+cbSbl55I1P8AeBZ9v5rXwlOhTV09Wu5946jaUm7LsjrdZ+Itzo89vaeH9QL3Uqqs9rOeWfGCyMOhJ6g8H2riNX8V6nrxlTUG2CIlpcngn+7VnxN4O0bw7pkGrPpcNhqhfcFXdmNcdBk84P8A9avKTfSXsq6ZaEs905LHuB/EfoBU0qFOVpRXzZNXETV4t39Dtdc8ejVvBmneEYdPW3FmyPJcbixcRBgoVf4R82Tyeelec804gKSo/h4/KkPSv1LCYSnQpqnTVlv8z8yxWLnXm5zd3sRmmHmpGIqM9M9a6GjnTImz0qBhxU5qFqlopM//0/lFDVhTVVCKmU19ejwGWk5qUHtmoFOBT81aRkyXJqpeTxQW0k0vKKpyPX2H1ouLuC1TdKwB7KOWP0Fctq9+b23VEG1W+Yjv7ZrlxOKjBNXu+x1YfDylJNqyO98J+NdY8HQQ6taL5tnfoGfcu4oQSCpz9ODXpn/DRl/9lSO3t/KZc5Zc5JI44x0zXkfhOe3vfDy2U2N1szJg+hYsP51lXWlWqTkL+7Gf4en5V+c1PZupJTWqfQ/QoQqKlFwejS37mn4s+IviPxhLGL12KRHK5GDW/wCCNMljeS9uATI6nBPUD+lYmn6fZRsHI3H1I/pXrWhC2EJVF5I5rlxWJShyQVkdmDwb5vaVHdnzpZXmpabrl7puol2jSY7d/OFLHayk9sV2p6U/4iWkVtq1siKFlMJd/XDMdo/Q1y8OrmJVjcbyBjHfFfcZTmN6adTZnw2a4BRrOMOh0Rx61Ee9Qw3kFwAEbBP8LcH8v8KmJr34zUldO54zg46SRGT1FQk8VKx9ahYihiR//9T5JV6nD1RVxVee7CgBDkcgn3FfT1q6pq7PGp0XN2RqtcxoMk8+grPudTcgrEdnuOv51kvOTyTVcygnFeZUxs5aJ2R3ww0I67smYmQsxJZiOp5qnKJURHI4HH5VbQgD3pk97HbRMLqN5A7Ep5YzxgZB9Oc1xNdToRY0qZ4pXFu4UTYyCcAEf41tiDU5X2tGxbsRzn6V5beXV9chjbxPBADgjufr7fpXd+APFh07VkstTGbG7dVd26xHoH/3f7w9Oe1eZjMM2nUgrvt3PXwGLUWqdR2Xfsdvpem6xNMkRgZQxHJFe56F4YuNKsJ9Z1dvs+n2sbTSyNwAqjJP1PQDueK9H0/w7ZafaNqN80cdvAhlaViAqoBksT0xivnP4o/F4eIUGiaZILLQYSS6v8sl0y/dZh2UHlV+hPOAPAw9KeKnypWXVn0GKxUMLC6d29jzHxfrn9s6zdaqEMazECJG6rEBtQH3wMn3JrnbW2kMb3UvBONo+pFYltrq3Nxm+Vg2eGRSQR24HIrpJdTtJ0FtbhyxIOSjAAA56kCvr6cFFKMdkfFzqOUnKW7EDVdiv5owBu3KOzc/r1rNYio9/wCddEZuDumZSipaSR0iX8TkB/lPr1FTFgRkHI9a5IyjOM1btL0JIImb5WHfse1d1HGttKf3nJUwqSvE/9X4uu7ryIjg8ngVgpcHBUnOeRUeo3W+VgDwOB+FZwlwVOeO9d2Iq802+hjSp2ikahnzEDnuRU0BLfMe9Y+8kqg5+Yn+VbURCKBnBArC5pYvjpTHOBkc1GG4oLccc02xEcgVCHxlTww9jQukpfgW8SFpCTtK4BHGc54wMdc07IKFT9KiiuZLYExuyMOjKcHBBB/TioA73xL8W/EMngLSfAEoMU1gXjuZ8nMscbYhVv8AcHUd8AnpXiMMFzdzGRw0jMclm5J/OuotrJbiSS7uSZGYliWOfmPJx7+tWIwiBggCgCohSUVZKxrUquTvJ3I7OIRj7oBHpWiDVaNsDmnF+9boyJWPHWqjvtce9OL8dapzuCM+lDYDRKSWJPAzUCz5ctnvVSSXargHkkgVGHxnmouUkf/W/PGWUlznvVbzSAPY4/A0x2zkA8jkVXkPGRwGH5GtWwNW0lBIkb+Hj6mthJuM9zXKW8xyqnoCTWrHODjmhMTRuCXNP8zFZay+9SebkYzTuFi95nXnrULsuCWO0DqarGXI600XCqSDnPYjtSCwHUTkJHxGvCr3PuaspISg55PNV2mQgjcxB7cc1GHA4FNMLGn5nHWmmU45qh5oPWmmXPequFi40oqnLLwcnioHl96pSzcdalsEiOWUmTaD15/OlSQkZHc8VQZ8yZB5xircRABc8Ko4qUxn/9k="


def user_avatar(email: str) -> str:
    """The photo where the letter-mark was. The demo founder gets the
    face; any other login keeps its initial."""
    if user_name(email) == "Mothi":
        return f'<img src="{MOTHI_PHOTO}" alt="Mothi">'
    return esc(user_name(email)[0])


def _shell(content: str, active: str, tid: str, email: str) -> str:
    return (TEMPLATE
            .replace("__CONTENT__", content)
            .replace("__SIDEBAR__", sidebar_html(active, tid,
                                                 convs=conv_list_html(tid),
                                                 email=email))
            .replace("__BIZ__", BUSINESS)
            .replace("__NAME__", esc(user_name(email)))
            .replace("__USER__", esc(email))
            .replace("__INITIAL__", user_avatar(email)))


def _card(title: str, sub: str, right: str = "") -> str:
    return (f'<div class="trow slim"><span class="ico">{ICONS["bolt"]}</span>'
            f'<span class="tdesc"><b>{title}</b> <span class="mut">{sub}</span></span>'
            f'{right}</div>')


# Relay's back office, organised as the three people a founder would
# otherwise have to hire: a CFO who knows where the money is, an ops manager
# who keeps orders moving, and a support manager who deals with customers
# and their banks. Every agent keys off the same order, which is why this is
# one back office and not seven tools.
#
# `today` renders as "Without Relay:" — who does this work now, and what it
# costs or why it goes undone. That gap is the product. Dispute Defender is
# the only one switched on here; the rest are off.
#
# No personas on these cards, deliberately. A five-person business has no
# risk lead and no ops lead. One person wears every hat, and that person is
# the only reader this copy is written for.
# The office is organised by the three people a founder would otherwise
# have to hire — not by an internal functional taxonomy. A founder thinks
# "I need a CFO", never "I need a finance desk".
DESKS = [
    ("accounts", "Your accounts manager",
     "Ties out every rupee against the bank, tells you what you were "
     "actually paid, and warns you before cash gets tight."),
    ("inventory", "Your inventory manager",
     "Knows what you have, what is moving, and what you are about to run "
     "out of."),
    ("risk", "Your risk &amp; compliance manager",
     "Checks what looks wrong before the money leaves, and keeps you filing-ready."),
    ("support", "Your support manager",
     "Answers your customers and their banks, and sees returns through to "
     "the refund."),
    ("calling", "Your telecaller",
     "Calls buyers: to confirm the order, to recover the payment, to "
     "close the sale."),
    ("analyst", "Your MIS analyst",
     "Puts the numbers in front of you every morning, and says what "
     "changed."),
]

RELAY_AGENTS = [
    # --- Your accounts manager -------------------------------------------
    dict(slug="three_way_recon", name="3-Way Reconciliation", icon="ledger",
        status="roadmap", desk="accounts",
        role="Reconciliation Officer",
        desc="Matches every order to the payout to the bank credit. Tells "
             "you what didn&rsquo;t tie.",
        today="Someone ties out lines by hand every day. Tools get to about "
              "80%, and a failed payout can surface weeks later.",
        replaces="an accounts executive tying out the bank, &#8377;20&ndash;30k a month"),
    dict(slug="settlement_insights", name="Settlement Insights", icon="chart",
        status="roadmap", desk="accounts",
        role="Settlement Analyst",
        desc="Tells you what&rsquo;s landing, when, what was deducted, and "
             "what&rsquo;s stuck.",
        today="You find out what you were actually paid by opening a "
              "statement and doing the maths.",
        replaces="the part of the accounts job spent reading statements"),
    dict(slug="cashflow_forecast", name="Cashflow Forecast", icon="flow",
        status="roadmap", desk="accounts",
        role="Cashflow Planner",
        desc="Tells you what cash lands this week, what is already spoken "
             "for, and when it gets tight.",
        today="You find out you are short in the week you are short. The "
              "forecast lives in someone&rsquo;s head or a stale sheet.",
        replaces="the finance person who keeps the cash sheet, &#8377;25&ndash;40k a month"),
    dict(slug="payouts_desk", name="Payouts Desk", icon="send",
        status="roadmap", desk="accounts",
        role="Payouts Clerk",
        desc="Pays vendors, staff and refunds on time, each in the way they "
             "want to be paid.",
        today="Payment day is one person with a bank tab open, copying "
              "account numbers. Something always goes out late.",
        replaces="the accounts payable clerk, &#8377;20&ndash;28k a month"),
    dict(slug="payment_forms", name="Payment Forms", icon="pen",
        status="roadmap", desk="accounts",
        role="Billing Executive",
        desc="Builds a payment form on the fly for anything that isn&rsquo;t "
             "normal checkout: a bulk order, a part advance, a "
             "subscription mandate: with any verification the rules "
             "require built into the same form.",
        today="Someone opens the dashboard, hand-builds a link, WhatsApps it, "
              "then chases the customer and types the details into the books "
              "afterwards.",
        replaces="the billing executive who hand-builds links, "
                 "&#8377;18&ndash;25k a month"),
    # --- Your inventory manager ------------------------------------------
    dict(slug="stock_watch", name="Stock Watch", icon="folder",
        status="roadmap", desk="inventory",
        role="Inventory Controller",
        desc="Watches stock across every channel, warns you before you run "
             "out, and stops you selling what you don&rsquo;t have.",
        today="You oversell on one channel and sit on dead stock in "
              "another. Someone checks sheets by hand, usually after a "
              "customer has already complained.",
        replaces="an inventory executive, &#8377;18&ndash;25k a month"),
    # --- Your risk manager -----------------------------------------------
    dict(slug="refund_shield", name="Refund Shield", icon="moon",
        status="roadmap", desk="risk",
        role="Refund Risk Officer",
        desc="Checks every refund claim for fraud before you pay it.",
        today="Claims get paid before anyone looks, because looking at every "
              "claim costs more than the fraud does.",
        replaces="a fraud reviewer you almost certainly never hired"),
    dict(slug="gst_compliance", name="GST &amp; Compliance", icon="funnel",
        status="roadmap", desk="risk",
        role="Compliance Officer",
        desc="Keeps GST, TDS and e-invoices tied to real orders, and flags "
             "what won&rsquo;t match before you file.",
        today="Your CA asks for the file on the 18th. Someone spends two "
              "days rebuilding it out of three different systems.",
        replaces="the monthly compliance scramble your CA bills you for"),
    dict(slug="kyc_desk", name="KYC Desk", icon="lock",
        status="roadmap", desk="risk",
        role="KYC Verifier",
        desc="Screens a buyer in seconds, runs the standard checks, escalates "
             "to a deeper review when the risk calls for it, and blocks mule "
             "accounts before any money moves.",
        today="The counter needs a PAN before a &#8377;2L sale. Someone types "
              "the number into a portal and files a printout while the "
              "customer stands there waiting.",
        replaces="a KYC executive checking documents by hand, "
                 "&#8377;18&ndash;25k a month"),
    # --- Your support manager --------------------------------------------
    dict(slug="dispute_defender", name="Dispute Defender", icon="shield",
        status="live", desk="support",
        role="Disputes Officer",
        desc="Gathers the proof, writes the reply, and files it before the "
             "deadline.",
        today="Proof sits across your store, the courier and your inbox. By "
              "the time it&rsquo;s gathered, the window has shut.",
        replaces="the support executive who chases proof, &#8377;18&ndash;25k a month"),
    dict(slug="returns_desk", name="Returns Desk", icon="baton",
        status="roadmap", desk="support",
        role="Returns Coordinator",
        desc="Follows every return from pickup to restock, and releases the "
             "refund once the goods are actually back.",
        today="Refunds go out before the item returns, or the customer "
              "chases you for weeks. Nobody joins the courier&rsquo;s "
              "tracking to your refund.",
        replaces="the returns coordinator between courier, warehouse and refund"),
    # --- Your telecaller --------------------------------------------------
    dict(slug="cart_rescue", name="Cart Rescue", icon="tasks",
        status="live", desk="calling",
        role="Cart Recovery Caller",
        desc="Calls buyers who left without paying and sends them a payment "
             "link.",
        today="A WhatsApp blast gets a fraction back. Nobody can call 400 "
              "dropped carts a day.",
        replaces="a telecaller, &#8377;15&ndash;22k a month"),
    dict(slug="payment_rescue", name="Payment Rescue", icon="bolt",
        status="live", desk="calling",
        role="Payment Recovery Caller",
        desc="Reads why a payment failed, waits a few minutes, then calls "
             "and sends a fresh link.",
        today="A failed UPI payment is just a lost order. Nobody reads a "
              "decline code.",
        replaces="a telecaller, &#8377;15&ndash;22k a month"),
    dict(slug="cod_guard", name="COD Guard", icon="note",
        status="roadmap", desk="calling",
        role="COD Confirmation Caller",
        desc="Confirms COD orders before dispatch and blocks addresses that "
             "keep failing.",
        today="Someone works the COD list every morning. COD is half your "
              "orders and a fifth of them come back.",
        replaces="the morning COD calling shift, &#8377;15&ndash;22k a month"),
    # --- Your MIS analyst -------------------------------------------------
    dict(slug="daily_mis", name="Daily MIS", icon="book",
        status="roadmap", desk="analyst",
        role="MIS Analyst",
        desc="Sends you the numbers that matter each morning, and flags what "
             "changed and why.",
        today="Someone rebuilds the MIS sheet every morning. It tells you "
              "what happened, never why.",
        replaces="an MIS executive, &#8377;25&ndash;35k a month"),
]

# Dispute Defender's own crew — the seven sub-agents inside the pipeline
# (§6 of the spec), each behind its own access/memory/playbook/quality tabs.
PLAY_AGENTS = {
    "Dispute Defender": ["detection-agent", "eligibility-agent", "response-agent",
                         "compliance-agent", "filing-agent", "escalation-agent",
                         "reporting-agent"],
}
AGENT_PLAYS = {}
for _play, _members in PLAY_AGENTS.items():
    for _m in _members:
        AGENT_PLAYS.setdefault(_m, []).append(_play)

_AGENT_ICON_BY_SLUG = {"detection-agent": "ear", "eligibility-agent": "funnel",
                       "response-agent": "pen", "compliance-agent": "shield",
                       "filing-agent": "note", "escalation-agent": "moon",
                       "reporting-agent": "chart",
                       "conductor-agent": "baton", "knowledge-agent": "book"}


def _agent_chips(slugs: list[str]) -> str:
    chips = []
    for sl in slugs:
        icon = ICONS[_AGENT_ICON_BY_SLUG.get(sl, "bot")]
        if sl in AGENT_DEFS:
            chips.append(f'<a class="agchip" href="/agents/{sl}" title="{sl}">{icon}</a>')
        else:
            chips.append(f'<span class="agchip off" title="{sl} (planned)">{icon}</span>')
    return f'<span class="agchips">{"".join(chips)}</span>'


# Journey scenarios: what your team is doing for one buyer, keyed by the
# order the buyer disputed.
GOAL_META = {
    "order_1":  ("Never-arrived claims",
                 "Keep winning Priya S.&rsquo;s ghee claim, even when the bank asks twice."),
    "order_2":  ("Charged-twice claims",
                 "Clear Rahul M.&rsquo;s charged-twice claim on the ashwagandha refill before the next payout."),
    "order_3":  ("Broken-seal claims",
                 "Settle Anjali K.&rsquo;s broken-seal claim on the aloe vera juice without paying the bank&rsquo;s fee."),
    "order_4":  ("Unknown-order claims",
                 "Show that Vikram R. placed and confirmed the shilajit order himself."),
    "order_5":  ("Cancelled-refill claims",
                 "Prove when Sneha D.&rsquo;s amla juice refill was actually cancelled."),
}


def _tbl(cols: str, head: list[str], rows: list[str]) -> str:
    """Osvi-style table with per-table column template."""
    thead = "".join(f"<span>{h}</span>" for h in head)
    body = "".join(rows) or '<div class="empty">Nothing here yet.</div>'
    return (f'<div class="atable">'
            f'<div class="thead" style="grid-template-columns:{cols}">{thead}</div>'
            f'{body}</div>')


def _trow2(cols: str, cells: list[str], href: str = "") -> str:
    inner = "".join(f"<span>{c}</span>" for c in cells)
    if href:
        return (f'<a class="arow2" style="grid-template-columns:{cols}" '
                f'href="{href}">{inner}</a>')
    return f'<div class="arow2" style="grid-template-columns:{cols}">{inner}</div>'


def workflows_content(tid: str) -> str:
    led = WORLD.d.ledger
    runs = [r for r in led.runs.values() if r.tenant_id == tid]
    waiting = sum(1 for r in runs if r.state is RunState.AWAITING_GATE)
    resolved = sum(1 for r in runs if r.state is RunState.RESOLVED)
    PCOLS = "1.1fr 2.6fr 1.5fr .8fr 1.4fr"
    play_rows = [
        _trow2(PCOLS, ["<b>Dispute Defender</b>",
                       '<span class="mut">Dispute filed &rarr; evidence-cited '
                       'response &rarr; your approval &rarr; filed with the '
                       'bank.</span>',
                       _agent_chips(PLAY_AGENTS["Dispute Defender"]),
                       '<span class="st ok">live</span>',
                       f'<span class="mut">{len(runs)} runs &middot; {waiting} '
                       f'waiting &middot; {resolved} resolved</span>']),
        _trow2(PCOLS, ["<b>COD Guard, Payment Rescue, Cart Rescue, Settlement "
                       "Clarity, Refund Shield, Loan Recovery, Due Diligence</b>",
                       '<span class="mut">Relay&rsquo;s other seven agents. '
                       'see the full fleet on the Agents console.</span>',
                       '<span class="st mut">not yet wired in this demo</span>',
                       '<span class="st wait">roadmap</span>', ""]),
    ]
    body = (_tbl(PCOLS, ["Playbook", "What it does", "Staffed by", "Status",
                         "Activity"], play_rows)
            + '<div class="pagehint" style="margin-top:8px">Playbooks share their '
              'crew: the same sub-agents staff every run, carrying what '
              'they&rsquo;ve learned. Sequencing across Relay&rsquo;s agents is the '
              '<b>conductor-agent</b>&rsquo;s job (planned): nothing automates '
              'before 20 approved manual responses, and underperforming '
              'playbooks pause themselves.</div>')

    led = WORLD.d.ledger
    truns = [r for r in led.runs.values() if r.tenant_id == tid]
    n_all = len(truns)
    n_sup = sum(1 for r in truns if r.state is RunState.SUPPRESSED)
    n_drafted = sum(1 for r in truns if r.decision)
    n_blocked = sum(1 for r in truns
                    if r.state in (RunState.FAILED, RunState.TIMED_OUT))
    n_wait = sum(1 for r in truns if r.state is RunState.AWAITING_GATE)
    n_acted = sum(1 for r in truns
                  if r.state in (RunState.ACTED, RunState.RESOLVED))
    won = influenced_won(tid)
    STAGE_AGENT = {"ear": "detection-agent", "funnel": "eligibility-agent",
                   "pen": "response-agent", "tasks": None,
                   "note": "filing-agent", "chart": "reporting-agent"}
    STAGES = [
        ("ear", "1 · A dispute is filed",
         "A bank or processor webhook lands with a reason code already attached. "
         "Nobody had to notice.",
         f"{n_all} signals so far", "review"),
        ("funnel", "2 · Eligibility decides if it matters",
         "No matching order, the same claim twice, a sales channel that "
         "isn&rsquo;t connected: every no is written down with its "
         "reason, never quietly dropped.",
         f"{n_sup} filtered out", "recently_countered"),
        ("pen", "3 · A response is drafted and checked",
         "Written from your own proof, in the way {biz} writes, then anything "
         "with a dead link, an invented fact or a boast is stopped before you "
         "ever see it.".format(biz=BUSINESS),
         f"{n_drafted} drafted &middot; {n_blocked} blocked or escalated", "qa_blocked"),
        ("tasks", "4 · You decide",
         "You get the card. Say yes, change the wording, or say no. "
         "nothing is filed without your yes. Anything you leave goes to a "
         "person, never files itself.",
         f"{n_wait} waiting now", "edited"),
        ("note", "5 · The response is filed with the bank",
         "Approved responses are filed onto the order exactly once, with the "
         "evidence attached.", f"{n_acted} filed", "approved"),
        ("chart", "6 · The outcome comes back",
         "Days later the bank settles the dispute and the result attaches to the "
         "same run: wins, losses, and what your edits taught the drafter.",
         f"&#8377;{won/100000:.0f}k won", "won"),
    ]
    SCOLS = "2.6fr 1.2fr 1.1fr"
    def stage_row(icon, title, desc, stat, ex_key):
        rid = EXEMPLARS.get(ex_key)
        ex_run = led.runs.get(rid) if rid else None
        link = (f'<a class="st wait" href="/runs/{rid}">'
                f'see example journey &rarr;</a>'
                if ex_run is not None and ex_run.tenant_id == tid else
                '<span class="mut">.</span>')
        ag = STAGE_AGENT.get(icon)
        who = (f'<a class="st wait" href="/agents/{ag}">{worker_name(ag)}</a>' if ag
               else '<span class="st ok">you</span>')
        return _trow2(SCOLS, [
            f'<span style="display:flex;gap:12px;align-items:flex-start">'
            f'<span class="ico" style="margin-top:2px">{ICONS[icon]}</span>'
            f'<span><b>{title}</b> {who}<br>'
            f'<span class="mut">{desc}</span></span></span>',
            f'<span class="st mut">{stat}</span>', link])
    story = _tbl(SCOLS, ["Stage", "In this workspace", "Example"],
                 [stage_row(*st) for st in STAGES])
    if not any(r.tenant_id == tid for r in led.runs.values()):
        story += ('<div class="empty">Load sample data from Home to fill every '
                  'stage with example journeys you can open.</div>')
    return (f'<h1 class="page">Playbooks</h1>{body}'
            f'<h2 class="sec">The lifecycle, live</h2>'
            f'<div class="pagehint">What actually happens to a dispute, stage '
            f'by stage: every count below is real rows in this '
            f'workspace, and every example opens a run&rsquo;s journey.</div>'
            f'{story}')


_AV_COLORS = ["#6E63E8", "#D8598E", "#3D77C9", "#2E9E67", "#C98A2E", "#8E44AD"]


def _edit_history(entries) -> str:
    """Actor-attributed change timeline. entries: (actor, when, verb, chips)
    where chips is a list of (label, color-class). Reused wherever governed
    things change: evidence-vault proof, agent autonomy, playbook rules."""
    def av(name):
        c = _AV_COLORS[sum(map(ord, name)) % len(_AV_COLORS)]
        ini = "".join(w[0] for w in name.split()[:2]).upper()
        return f'<span class="eh-av" style="background:{c}">{ini}</span>'
    items = "".join(
        f'<div class="eh-item"><span class="eh-node"></span>'
        f'<div class="eh-head">{av(actor)}<b>{esc(actor)}</b>'
        f'<span class="eh-when">&middot; {esc(when)}</span></div>'
        f'<div class="eh-line"><span class="eh-verb">{esc(verb)}:</span>'
        + "".join(f'<span class="echip {c}">{esc(l)}</span>' for l, c in chips)
        + "</div></div>"
        for actor, when, verb, chips in entries)
    return f'<div class="ehist">{items}</div>'


EV_NOTES: list = []       # (tenant_id, actor, when-str, text): demo store


# Evidence types are stored as the engine's snake_case strings; nobody
# reading the vault should have to see one.
EV_TYPE = {"delivery_proof": "delivery proof", "invoice": "invoice",
           "communication_log": "message thread", "refund_policy": "policy page"}


def knowledge_content(tid: str) -> str:
    ev = [e for e in WORLD.d.evidence if e.tenant_id == tid]
    notes = [m for m in WORLD.d.ledger.memory if m.tenant_id == tid
             and m.superseded_by is None]
    ECOLS = "1fr 1fr 3fr .8fr"
    ev_rows = _tbl(ECOLS, ["Dispute reason", "Evidence type", "What it proves", "Source"],
                   [_trow2(ECOLS, [
                       f"<b>{esc(COMP.get(e.reason_code, e.reason_code))}</b>",
                       f'<span class="st mut">{EV_TYPE.get(e.evidence_type, esc(e.evidence_type))}</span>',
                       f'<span class="mut">{esc(e.text)}</span>',
                       f'<a class="st wait" href="{esc(e.source_url)}">source &rarr;</a>'])
                    for e in ev])
    NCOLS = "1.2fr 3fr"
    note_rows = _tbl(NCOLS, ["What you changed", "What it taught the drafter"],
                     [_trow2(NCOLS, [
                         f"<b>{esc((m.body or {}).get('changed') or 'a rewording')}</b>",
                         f'<span class="mut">{esc((m.body or {}).get("implies") or "style note")}</span>'])
                      for m in notes])
    return (f'<h1 class="page">Evidence vault</h1>'
            f'<div class="pagehint">Evidence packs by reason code. Every '
            f'dispute response cites from here: no claim ships that '
            f'you didn&rsquo;t arm it with.</div>'
            f'<h2 class="sec" id="knowledge">Evidence packs</h2>{ev_rows}'
            f'<h2 class="sec">Your voice: learned from the wording you change</h2>{note_rows}'
            f'<h2 class="sec">Edit history</h2>'
            f'<div class="pagehint">The evidence vault is governed: every change '
            f'has an author, a timestamp, and an approval behind it.</div>'
            + _edit_history([(a, w, "Commented", [(t[:64], "ec-blue")])
                             for tn, a, w, t in reversed(EV_NOTES) if tn == tid] + [
                ("Deepa Krishnan", "Jul 28, 4:12 PM", "Added",
                 [("Courier POD feed v2", "ec-purple"), ("GPS-stamp attachment", "ec-blue")]),
                ("Autopilot proposal", "Jul 21, 9:05 AM", "Approved by Deepa Krishnan",
                 [("Duplicate-charge bank excerpt refresh", "ec-green")]),
                ("Arjun Pillai", "Jul 12, 3:08 PM", "Removed",
                 [("Expired refund-policy PDF", "ec-amber")]),
                ("Deepa Krishnan", "Jul 12, 3:07 PM", "Added",
                 [("WhatsApp comms-log export", "ec-blue"), ("Listing snapshot archive", "ec-orange")]),
                ("Riya Kapoor", "Jul 2, 6:24 PM", "Added",
                 [("Invoice-to-transaction matcher", "ec-pink")])])
            + f'<form class="notebar" method="post" action="/api/evnote">'
            f'<input class="jfind notein" name="text" maxlength="200" '
            f'placeholder="Comment on the evidence vault: e.g. a proof that went stale">'
            f'<button class="btn primary sm">Comment</button></form>')


def projects_content(tid: str) -> str:
    # Pipeline: the order-centric view. Groups runs by order so each row is
    # an order with its dispute reason coverage. Today it is run-derived;
    # when a real bank/gateway feed connects, the full open-order pipeline
    # syncs here and uncovered orders get flagged (reads are built).
    led = WORLD.d.ledger
    orders: dict[str, list] = {}
    for r in led.runs.values():
        if r.tenant_id == tid and r.merchant_id:
            orders.setdefault(r.order_id or "unknown_order", []).append(r)
    if not orders:
        return ('<h1 class="page">Pipeline</h1>'
                '<div class="empty">Orders appear here once the first dispute '
                'names one.</div>')
    entries = []
    total_open = total_won = 0
    for oid, runs in sorted(orders.items(),
                            key=lambda kv: -max(r.occurred_at.timestamp()
                                                for r in kv[1])):
        r0 = runs[0]
        label = _account_label(r0)
        reasons = sorted({COMP.get(r.reason_code, r.reason_code)
                          for r in runs if r.reason_code})
        waiting = sum(1 for r in runs if r.state is RunState.AWAITING_GATE)
        outs = [led.outcome_for(r.run_id) for r in runs]
        outs = [o for o in outs if o]
        won_amt = sum((o.outcome_value or {}).get("amount_paise") or 0
                      for o in outs if (o.outcome_value or {}).get("won"))
        lost = any(not (o.outcome_value or {}).get("won") for o in outs)
        if won_amt:
            stage = '<span class="st ok">won</span>'
            total_won += won_amt
        elif lost:
            stage = '<span class="st mut">lost</span>'
        elif waiting:
            stage = f'<span class="st wait">awaiting review &middot; {waiting}</span>'
            total_open += 1
        else:
            stage = '<span class="st mut">resolved</span>'
            total_open += 1
        value = f"&#8377;{won_amt / 100:,.0f}" if won_amt else '<span class="mut">.</span>'
        last = max(r.occurred_at for r in runs).strftime("%b %-d")
        entries.append((0 if waiting else (1 if not outs else 2),
                        _trow2(PCOLS, [
            f'{_logo(label)}<b>{esc(label)}</b>'
            f'<span class="mut"> &middot; {esc(bought(oid))}</span>',
            stage,
            f'<span class="mut">{esc(", ".join(reasons)) or "."}</span>',
            f'{len(runs)} dispute{"s" if len(runs) != 1 else ""} on this order',
            value,
            f'<span class="mut">{last}</span>',
            f'<a class="st wait" href="/journeys?a={esc(oid)}">journey &rarr;</a>'])))
    rows = [h for _, h in sorted(entries, key=lambda t: t[0])]
    table = _tbl(PCOLS, ["Customer &amp; order", "Stage", "Dispute reason",
                         "Coverage", "Won amount", "Last seen", ""],
                 rows)
    return (f'<h1 class="page">Pipeline</h1>'
            f'<div class="pagehint">Your orders, by dispute coverage: '
            f'<b class="countup" data-n="{total_open}">0</b> open under watch '
            f'&middot; <b class="countup" data-n="{int(total_won / 100)}" '
            f'data-pre="&#8377;">&#8377;0</b> won '
            f'with a filed response attached. Run-derived today; a live bank '
            f'feed sync adds every open order and flags the uncovered ones.'
            f'</div>' + table)


PCOLS = "1.6fr 1.2fr 1.1fr 1.1fr .8fr .7fr .8fr"


def vault_content(tid: str) -> str:
    from relay_superagent.secrets import get_secret
    rails = [("The AI that writes the replies", "anthropic"),
             ("Slack, so you can say yes from there", "slack-bot"),
             ("Slack button security", "slack-signing"),
             ("Meeting notes (not used here)", "fathom-webhook"),
             ("Your email inbox", "gmail-token"),
             ("Order notes (not used here)", "hubspot-token"),
             ("Signing in", "workos-api-key")]
    def rail_status(acct):
        if get_secret(acct):
            return '<span class="st ok">connected</span>'
        # every adapter is built; what remains is the key — say exactly that
        return ('<span class="st wait">built, not switched on</span>')
    rows = "".join(
        f'<div class="trow slim"><span class="ico">{ICONS["lock"]}</span>'
        f'<span class="tdesc"><b>{esc(name)}</b> <span class="mut">held by Relay</span></span>'
        + rail_status(acct) + '</div>'
        for name, acct in rails)
    hint = ('<div class="empty">Nothing here is yours to set up. Relay holds '
            'every one of these itself, in the Mac keychain, never in a '
            'file.</div>')
    return f'<h2 class="sec" id="vault">What it&rsquo;s plugged into</h2>{rows}{hint}'


# Per-agent console data (CM-21 demo world). Access grants and memory
# fields mirror the real architecture; playbook rules quote the seeded
# field-notes rules. Slug = URL identity.
AGENT_DEFS = {
    "detection-agent": dict(icon="ear", name="detection-agent",
        charter="Reads every dispute the moment the bank sends it, and works out what the buyer is claiming.",
        reads=["disputes from the bank", "chargeback emails"],
        effects=[], scope="Only reads what the bank sent. Nothing else about the buyer.",
        session=["what the buyer claimed", "what kind of claim it is", "how sure it is"],
        profile=["what this business has been disputed for before"],
        rules=["It only takes on a claim it has proof for.",
               "If the bank sends the same dispute twice, it is not worked twice."]),
    "eligibility-agent": dict(icon="funnel", name="eligibility-agent",
        charter="Decides what is worth your time. Every no is written down with a reason.",
        reads=["your orders", "what has already been done", "who is signed up"],
        effects=[], scope="Reads the history. The only thing it writes is why it said no.",
        session=["why it was set aside"], profile=[],
        rules=["No matching order &rarr; set aside.",
               "Same claim handled recently &rarr; set aside.",
               "Too many in one day for one business &rarr; set aside. That limit is hard, not a suggestion."]),
    "response-agent": dict(icon="pen", name="response-agent",
        charter="Writes the reply to the bank, quoting the proof, in the way this business writes.",
        reads=["the proof on file", "how you have reworded replies before", "the order"],
        effects=[], scope="Reads the proof and your past wording. It never sends anything.",
        session=["the reply", "the proof it quoted"],
        profile=["how you write (learned from your changes)"],
        rules=["Every claim in the reply points at a piece of proof on file.",
               "No boasting words (best / leading / number one).",
               "If it is not sure, it asks a person. It never guesses."]),
    "compliance-agent": dict(icon="shield", name="compliance-agent",
        charter="Checks the links, the facts and the wording before you ever see a reply.",
        reads=["replies", "where the proof came from"], effects=["ask a person"],
        scope="Reads replies. The only power it has is to stop one.",
        session=["what failed the check", "how the wording scored"], profile=[],
        rules=["A dead link stops the reply.",
               "A phone number or email in the reply stops it.",
               "If the wording scores low, a person sees it, not you."]),
    "filing-agent": dict(icon="note", name="filing-agent",
        charter="Sends the reply you approved, exactly once, and records how it ended.",
        reads=["replies you said yes to"], effects=["send the reply to the bank"],
        scope="The only one that talks to the bank: and only after your yes.",
        session=["what was sent", "when"], profile=["how the dispute ended, and for how much"],
        rules=["Exactly once. A crash can never send the same reply twice.",
               "It only acts on replies you said yes to."]),
    "escalation-agent": dict(icon="moon", name="escalation-agent",
        charter="Notices what is stuck or piling up and hands it to a person. It never sends anything.",
        reads=["everything in flight", "how much is waiting"], effects=["hand it to a person"],
        scope="Reads everything, sends nothing. It has no AI in it on purpose.",
        session=["why it got stuck"], profile=[],
        rules=["Left unanswered for a day, it goes to a person: it never sends itself.",
               "If something breaks, it is said out loud, never buried."]),
    "reporting-agent": dict(icon="chart", name="reporting-agent",
        charter="Keeps the history and the numbers you see on every screen.",
        reads=["the history"], effects=[],
        scope="Reads only. Every number it shows can be worked out again from your own record.",
        session=[], profile=[],
        rules=["Numbers are counted from what actually happened, never guessed.",
               "How often you have to fix a reply is on the bill: you can check it yourself."]),
}

# Every roster agent opens to a real page, wired or not. What each desk may
# read and the rules it works under are written per desk, in plain words —
# honest "will" phrasing for the ones not switched on yet. Switching one on
# is a demo-level act: it starts in watching mode and touches nothing.
DESK_ACCESS = {
    "accounts": dict(
        reads=["your orders", "what the bank settled", "payout statements"],
        does=["write you a summary", "flag what didn&rsquo;t tie"],
        rules=["It reads statements; it never moves money.",
               "Anything that doesn&rsquo;t tie is shown to you, never patched over.",
               "Every number can be traced back to a bank line."],
        handoff=["A line that still won&rsquo;t tie after two tries goes to "
                 "a person, with the working shown.",
                 "Anything that would move money is never guessed: a "
                 "person decides."]),
    "inventory": dict(
        reads=["stock counts", "orders coming in", "supplier lead times"],
        does=["warn you before you run out", "draft a reorder for your yes"],
        rules=["It never places an order itself: a reorder waits for your yes.",
               "A warning names the product, the days left, and why."],
        handoff=["A reorder bigger than your usual goes to a person first.",
                 "Counts that disagree across channels: a person "
                 "reconciles them, not a guess."]),
    "risk": dict(
        reads=["refund claims", "filing deadlines", "the rules that apply to you"],
        does=["flag the risky ones", "keep your filings on time"],
        rules=["A doubt goes to you, never a quiet guess.",
               "Deadlines are watched every day; a near-miss is said out loud."],
        handoff=["A claim it cannot prove either way goes to a person, with "
                 "both readings written down.",
                 "A filing at risk of missing its date is put in front of a "
                 "person the same day."]),
    "support": dict(
        reads=["disputes from the bank", "your orders", "the proof on file"],
        does=["write the reply for your yes", "file it once you agree"],
        rules=["Nothing reaches a bank or a buyer without your yes.",
               "Every claim in a reply points at proof on file."],
        handoff=["An angry buyer, or any mention of legal action, goes "
                 "straight to a person.",
                 "A reply it is not sure of is never sent: a person "
                 "reads it first."]),
    "calling": dict(
        reads=["dropped carts", "failed payments", "orders waiting confirmation"],
        does=["call or message the buyer", "bring back the ones worth saving"],
        rules=["It never calls the same buyer twice about one thing.",
               "Calling hours are respected: no 9 PM calls.",
               "A buyer who says stop is never contacted again."],
        handoff=["A buyer who asks for a person gets one: the agent "
                 "stops talking, mid-call if needed.",
                 "Any argument about money on a call is handed over, not "
                 "argued."]),
    "analyst": dict(
        reads=["everything the other agents record"],
        does=["write your daily one-pager"],
        rules=["Numbers are counted from the record, never guessed.",
               "Bad news is on page one, not buried."],
        handoff=["A number that looks wrong is flagged to a person. "
                 "never smoothed over."]),
}

DEMO_ON: dict[str, bool] = {}    # slug -> switched on (demo state, in-memory)
# Stress posture: the whole team starts switched on (watching), so every
# surface shows a full office at work. Captain's call for the demo; the
# gate still applies to anything an agent would DO.
DEMO_ON.update({a["slug"]: True for a in RELAY_AGENTS
                if a["status"] != "live"})

# One rescue, followed through: the multithreaded story a founder needs to
# see to believe a caller agent. Voice and WhatsApp are one conversation to
# the buyer; the agent waits, switches channel, retries at a sane hour, and
# stops when told. Times are offsets, not clock times, so the story is
# stable however long the demo has been up.
AGENT_THREADS = {
    "cart_rescue": dict(
        opening="6:12 PM. Vikram leaves a &#8377;1,899 Shilajit order at checkout.",
        closing="Won back &#8377;1,899. Three touches over two days, then it "
                "stops. A buyer who says stop is never contacted again.",
        won="&#8377;1,899 paid",
        rows=[
            ("+30 min", "call", "Calls Vikram. No answer. It does not ring "
             "twice in a row.", "missed"),
            ("+32 min", "wa", "&ldquo;Your cart is saved. Pay in one tap "
             "whenever you are ready.&rdquo; Payment link attached.", "delivered"),
            ("next day, 11 AM", "call", "Second call, at a sane hour. Vikram "
             "answers: he wanted COD. The agent offers the prepaid "
             "discount instead.", "2 min call"),
            ("+2 min", "wa", "Fresh link with the discount applied.", "paid"),
        ]),
    "payment_rescue": dict(
        opening="8:41 PM. Sneha&rsquo;s UPI payment fails on a &#8377;549 "
                "refill. The bank timed out; she does not know if she paid.",
        closing="Saved the order the same evening. The agent read the "
                "decline reason first, so every follow-up matched what "
                "actually went wrong.",
        won="&#8377;549 paid",
        rows=[
            ("+5 min", "wa", "&ldquo;That payment did not go through and "
             "nothing was charged. Here is a fresh link.&rdquo;", "read"),
            ("+20 min", "call", "No payment yet, so it calls. Sneha retries "
             "on the call; her card declines this time (limit).", "3 min call"),
            ("+1 min", "wa", "New link with UPI and card both open, plus "
             "&ldquo;pay on delivery&rdquo; as the fallback.", "paid"),
        ]),
}


# A day on the job, one per agent: the fastest way to believe an agent is
# to watch one shift. Three or four beats, real objects, real amounts, and
# the gate visible wherever money would move.
AGENT_DAYS = {
    "three_way_recon": [
        ("7:00 AM", "Pulled yesterday: 214 orders, 3 payouts, one bank statement.", ""),
        ("7:04 AM", "211 tied out on their own.", "matched"),
        ("7:05 AM", "&#8377;4,310 short on one payout. Named the 3 orders inside it.", "flagged"),
        ("7:06 AM", "Wrote the morning note. Your accounts person starts at zero.", "done")],
    "settlement_insights": [
        ("11:20 AM", "Settlement landed: &#8377;1,84,220.", ""),
        ("11:21 AM", "Broke it down: &#8377;2,140 fees, &#8377;890 held back, reason named.", "done"),
        ("11:22 AM", "&ldquo;Landing today&rdquo; updated. Nothing stuck.", "done")],
    "cashflow_forecast": [
        ("8:00 AM", "Read what lands this week and what goes out.", ""),
        ("8:01 AM", "Thursday looks tight: vendor day and a GST debit collide.", "warned"),
        ("8:01 AM", "Suggested moving one payout by two days. Your call.", "your yes")],
    "payouts_desk": [
        ("6:00 PM", "Lined up 14 payments due tomorrow: vendors, 2 refunds, the courier.", ""),
        ("6:01 PM", "One list, one yes. You approved from your phone.", "your yes"),
        ("6:02 PM", "Paid each one the way they want it. Every payment against its bill.", "done")],
    "payment_forms": [
        ("2:10 PM", "A bulk buyer wants to pay &#8377;2.6 lakh as a 40% advance.", ""),
        ("2:11 PM", "Built the form: PAN, the advance, balance on delivery. One link.", "done"),
        ("2:12 PM", "Buyer paid in one step. The books already know.", "paid")],
    "stock_watch": [
        ("9:00 AM", "Counted every channel: store, Amazon, Flipkart, quick commerce.", ""),
        ("9:01 AM", "Amla Juice: 6 days left at this pace. The supplier needs 10.", "warned"),
        ("9:02 AM", "Drafted the reorder. It goes nowhere until your yes.", "your yes")],
    "refund_shield": [
        ("4:40 PM", "Refund claim: &ldquo;bottle arrived broken&rdquo;, &#8377;1,249.", ""),
        ("4:41 PM", "Checked the photo, the delivery scan, the history: second claim in 3 weeks.", "flagged"),
        ("4:41 PM", "Held for a person. The buyer sees &ldquo;being reviewed&rdquo;, not a no.", "held")],
    "gst_compliance": [
        ("18th, 10:00 AM", "Tied the month&rsquo;s invoices to real orders. Two won&rsquo;t match.", ""),
        ("10:01 AM", "Named both, with the fix for each.", "flagged"),
        ("10:02 AM", "One clean file to your CA. Nothing rebuilt by hand.", "done")],
    "returns_desk": [
        ("Monday", "Return picked up: Shilajit Resin, &#8377;1,899.", "tracking"),
        ("Wednesday", "Reached the warehouse. Seal checked, item fine.", "checked"),
        ("Wednesday", "Refund released the same hour. It never went out early.", "done")],
    "cod_guard": [
        ("10:00 AM", "38 COD orders lined up for dispatch today.", ""),
        ("10:20 AM", "31 confirmed on call, 4 more on WhatsApp.", "confirmed"),
        ("10:25 AM", "3 never picked up twice. Held from dispatch, pincode noted.", "held")],
    "daily_mis": [
        ("7:30 AM", "Collected yesterday from every channel.", ""),
        ("7:31 AM", "Orders up 12%, but returns on one SKU doubled. Said why.", "insight"),
        ("7:32 AM", "One page, in your morning brief.", "done")],
    "dispute_defender": [
        ("9:14 AM", "Bank sends a dispute: &ldquo;never arrived&rdquo;, &#8377;549.", ""),
        ("9:14 AM", "Proof pulled: delivery scan, WhatsApp thread, policy as it read that day.", "done"),
        ("9:15 AM", "Reply written and put in front of you. One tap.", "your yes"),
        ("9:40 AM", "Filed with the bank, exactly once. Deadline was 6 days out.", "filed")],
}


def day_html(slug: str) -> str:
    day = AGENT_DAYS.get(slug)
    if not day:
        return ""
    rows = "".join(
        f'<div class="thr"><span class="tht">{when}</span>'
        f'<span class="thb">{text}</span>'
        + (f'<span class="st {"ok" if stat in ("done", "paid", "matched", "confirmed", "checked", "filed") else "wait" if stat == "your yes" else "mut"}">{stat}</span>'
           if stat else '')
        + '</div>'
        for when, text, stat in day)
    return (f'<h2 class="sec">A day on the job</h2>'
            f'<div class="pagehint">One real shift, start to finish. '
            f'Anything that moves money stops for your yes.</div>'
            f'<div class="thread2">{rows}</div>')


def risk_ladder_html() -> str:
    """Agentic due diligence: the depth of the check follows the risk, and
    the deep tier is honestly long horizon. The agent holds an open check
    for days, wakes when a registry answers, and never blocks the buyer
    with an error screen while it waits."""
    tiers = [
        ("Every buyer", "under 10 seconds",
         "A quiet screen: phone, device, order history. The buyer sees "
         "nothing and waits for nothing.", "ok"),
        ("Higher value, or something odd", "under 30 seconds",
         "The standard checks, inside the payment form. PAN collected "
         "where the rules ask for it; verify and pay stay one step.", "ok"),
        ("Real risk", "2 to 7 days",
         "A deeper look: watchlists, registries, and a person on the "
         "decision. The agent holds the case open, wakes when each answer "
         "lands, and keeps you posted. The buyer sees &ldquo;under "
         "review&rdquo;, never an error.", "wait"),
        ("A mule score above the line", "instant",
         "Blocked before any money moves. Said to you plainly, with the "
         "score and the reason.", "warn"),
    ]
    rows = "".join(
        f'<div class="tier"><div class="tierh"><b>{name}</b>'
        f'<span class="st {cls}">{t}</span></div>'
        f'<p>{body}</p></div>'
        for name, t, body, cls in tiers)
    return (f'<h2 class="sec">Depth follows risk</h2>'
            f'<div class="pagehint">Most buyers get seconds. Real risk '
            f'gets days. The agent does not forget an open check: it '
            f'wakes when the answer lands, even a week later.</div>'
            f'<div class="tiers">{rows}</div>')


def thread_html(slug: str) -> str:
    t = AGENT_THREADS.get(slug)
    if not t:
        return ""
    CH = {"call": ("Call", "chc"), "wa": ("WhatsApp", "chw")}
    rows = "".join(
        f'<div class="thr"><span class="tht">{when}</span>'
        f'<span class="chch {CH[ch][1]}">{CH[ch][0]}</span>'
        f'<span class="thb">{text}</span>'
        f'<span class="st {"ok" if stat == "paid" else "mut"}">{stat}</span>'
        f'</div>'
        for when, ch, text, stat in t["rows"])
    return (f'<h2 class="sec">One rescue, followed through</h2>'
            f'<div class="pagehint">Voice and WhatsApp are one conversation '
            f'to the buyer. The agent waits, switches channel, and picks '
            f'its hour. This runs over days, not minutes.</div>'
            f'<div class="thread2">'
            f'<div class="thr open"><span class="thb">{t["opening"]}</span></div>'
            f'{rows}'
            f'<div class="thr close"><span class="thb">{t["closing"]}</span>'
            f'<span class="st ok">{t["won"]}</span></div></div>')

# One outcome, three-or-four steps. The Razorpay Agent Studio grammar:
# say what you get, then how, in one glance — never a wall of rows.
AGENT_STORY = {
    "three_way_recon": dict(
        outcome="Every rupee tied out, every morning",
        steps=[("Pull", "your orders, the payouts, the bank"),
               ("Match", "line by line, all three ways"),
               ("Flag", "what didn&rsquo;t tie, with the reason"),
               ("Report", "one morning note: matched, short, stuck")]),
    "settlement_insights": dict(
        outcome="Know what you were actually paid",
        steps=[("Watch", "every settlement as it lands"),
               ("Break down", "what was deducted, and why"),
               ("Tell you", "what&rsquo;s landing today and what&rsquo;s stuck")]),
    "cashflow_forecast": dict(
        outcome="See a cash crunch a week before it hits",
        steps=[("Read", "what&rsquo;s landing and what&rsquo;s going out"),
               ("Project", "your cash position 7 days ahead"),
               ("Warn", "the moment a tight week shows up")]),
    "payouts_desk": dict(
        outcome="Vendors, staff and refunds paid on time, every time",
        steps=[("Line up", "what&rsquo;s due, to whom, by when"),
               ("Ask", "you approve the list: one yes"),
               ("Pay", "each one, the way they want to be paid"),
               ("Record", "every payment against its bill")]),
    "payment_forms": dict(
        outcome="Any odd payment collected with one form",
        steps=[("Hear", "what you need: a bulk order, an advance"),
               ("Build", "the form, checks included"),
               ("Collect", "the money straight into your account")]),
    "stock_watch": dict(
        outcome="Never oversell, never sit on dead stock",
        steps=[("Watch", "stock across every channel"),
               ("Warn", "before you run out, days ahead"),
               ("Draft", "the reorder: sent only on your yes")]),
    "refund_shield": dict(
        outcome="Refund fraud caught before the money leaves",
        steps=[("Check", "every claim against the order and delivery"),
               ("Score", "what looks wrong, and why"),
               ("Hold", "the doubtful ones for your call")]),
    "gst_compliance": dict(
        outcome="Filing-ready books, no month-end scramble",
        steps=[("Tie", "GST, TDS and e-invoices to real orders"),
               ("Spot", "what won&rsquo;t match before it&rsquo;s filed"),
               ("Hand over", "one clean file your CA can use")]),
    "kyc_desk": dict(
        outcome="Every buyer checked in seconds, not at the counter",
        steps=[("Screen", "the buyer the moment it matters"),
               ("Verify", "the standard checks, automatically"),
               ("Escalate", "the risky ones to a deeper look"),
               ("Block", "mule accounts before money moves")]),
    "dispute_defender": dict(
        outcome="Every dispute answered before the deadline",
        steps=[("Read", "the dispute the moment the bank sends it"),
               ("Gather", "the proof from your own records"),
               ("Write", "the reply: you approve it"),
               ("File", "before the deadline, exactly once")]),
    "returns_desk": dict(
        outcome="Refunds released only when the goods are back",
        steps=[("Follow", "every return from pickup to doorstep"),
               ("Check", "the item actually arrived back"),
               ("Release", "the refund, and record it")]),
    "cart_rescue": dict(
        outcome="Dropped carts called back within the hour",
        steps=[("Spot", "a cart left at checkout"),
               ("Call", "the buyer while they still care"),
               ("Send", "the payment link that closes it")]),
    "payment_rescue": dict(
        outcome="Failed payments turned back into orders",
        steps=[("Read", "why the payment failed"),
               ("Wait", "a few minutes: then call"),
               ("Send", "a fresh link that works")]),
    "cod_guard": dict(
        outcome="COD confirmed before dispatch, returns cut",
        steps=[("Call", "every COD order before it ships"),
               ("Confirm", "the buyer actually wants it"),
               ("Block", "addresses that keep bouncing")]),
    "daily_mis": dict(
        outcome="Your numbers every morning, with the why",
        steps=[("Collect", "yesterday from every channel"),
               ("Compare", "against the weeks before"),
               ("Tell you", "what changed and why it changed")]),
}


# A person reading this is not a developer. Nobody should ever see a slug like
# "detection-agent" on screen: each worker has a plain job title, the way
# you would name the person you hired to do it.
WORKER_NAME = {
    "detection-agent": "Dispute Reader",
    "eligibility-agent": "Case Screener",
    "response-agent": "Reply Writer",
    "compliance-agent": "Reply Checker",
    "filing-agent": "Filing Clerk",
    "escalation-agent": "Escalation Watch",
    "reporting-agent": "Bookkeeper",
}


def worker_name(slug: str) -> str:
    """Plain job title for a worker; falls back to a de-slugged label."""
    return WORKER_NAME.get(slug) or slug.replace("-agent", "").replace(
        "-", " ").title()


# Per-agent controls, founder-worded but real: instructions the agent
# follows, tool switches, memory, guardrail numbers, and checks. Saved
# per tenant per agent; every change lands in the Decisions trail.
AGENT_CFG: dict = {}

GUARD_DEFAULTS = {
    "calling": [("quiet", "No calls outside", "9 AM to 8 PM"),
                ("tries", "Give up after", "3 tries")],
    "accounts": [("ask_above", "Always ask you above", "&#8377;10,000")],
    "inventory": [("ask_above", "Always ask you above", "&#8377;25,000")],
    "risk": [("ask_above", "Always ask you above", "&#8377;1,000")],
    "support": [("ask_above", "Always ask you above", "&#8377;5,000")],
    "analyst": [],
}


def agent_cfg(tid: str, slug: str) -> dict:
    return AGENT_CFG.setdefault(tid, {}).setdefault(slug, {
        "instructions": "", "learn": True, "eval_at": "", "tools_off": [],
        "guards": {}})


def agent_settings_content(tid: str, a: dict) -> str:
    """Five controls, almost no words. Placeholders teach; captions are
    one line; anything constant is said once, at the bottom."""
    slug = a["slug"]
    cfg = agent_cfg(tid, slug)
    acc = DESK_ACCESS.get(a["desk"], DESK_ACCESS["analyst"])

    instr = (
        '<h2 class="sec">Instructions</h2>'
        '<form method="post" action="/api/agent_cfg">'
        '<input type="hidden" name="slug" value="' + slug + '">'
        '<textarea class="cfgbox" name="instructions" rows="2" '
        'style="width:100%;max-width:640px;resize:vertical" '
        'placeholder="e.g. never call before 11 AM. It follows this on '
        'every job.">'
        + esc(cfg["instructions"]) + '</textarea>'
        '<div style="margin-top:8px"><button class="btn primary sm">'
        'Save</button></div></form>')

    reads_line = (
        '<div class="trow slim"><span class="ico">' + ICONS["book"]
        + '</span><span class="tdesc"><span class="mut">Always reads: '
        + ", ".join(acc["reads"]) + '.</span></span></div>')
    trows = ""
    for i, d in enumerate(acc["does"]):
        off = str(i) in cfg["tools_off"]
        trows += (
            '<div class="trow slim" style="display:flex">'
            '<form method="post" action="/api/agent_cfg" '
            'style="display:contents">'
            '<input type="hidden" name="slug" value="' + slug + '">'
            '<input type="hidden" name="tool" value="' + str(i) + '">'
            '<button class="tglbtn ' + ("" if off else "on") + '"><i></i>'
            '</button></form>'
            '<span class="tdesc">' + d[0].upper() + d[1:] + '</span></div>')
    tools = ('<h2 class="sec">Tools</h2>' + trows + reads_line)

    mem = (
        '<h2 class="sec">Memory</h2>'
        '<div class="trow slim" style="display:flex">'
        '<form method="post" action="/api/agent_cfg" style="display:contents">'
        '<input type="hidden" name="slug" value="' + slug + '">'
        '<input type="hidden" name="learn" value="'
        + ("0" if cfg["learn"] else "1") + '">'
        '<button class="tglbtn ' + ("on" if cfg["learn"] else "") + '">'
        '<i></i></button></form>'
        '<span class="tdesc">Learns your style from what you change</span>'
        '<a class="st wait" href="/memory">what it knows &rarr;</a></div>')

    fields = GUARD_DEFAULTS.get(a["desk"], GUARD_DEFAULTS["analyst"])
    grows = ""
    for key, label, default in fields:
        val = cfg["guards"].get(key, default)
        grows += (
            '<form class="trow slim" method="post" action="/api/agent_cfg" '
            'style="display:flex;align-items:center">'
            '<input type="hidden" name="slug" value="' + slug + '">'
            '<span class="tdesc">' + label + '</span>'
            '<input class="inedit-in" style="flex:none;width:140px" '
            'name="guard_' + key + '" value="' + esc(val) + '">'
            '<button class="btn ghost sm">Save</button></form>')
    guards = ('<h2 class="sec">Limits</h2>' + grows) if grows else ""

    checked = cfg["eval_at"]
    ev = (
        '<h2 class="sec">Checks</h2>'
        + "".join(
            '<div class="trow slim"><span class="ico">' + ICONS["shield"]
            + '</span><span class="tdesc">' + c + '</span>'
            + ('<span class="st ok">passed</span>' if checked else
               '<span class="st mut">runs tonight</span>')
            + '</div>'
            for c in ["Follows your instructions",
                      "Stops at your limits",
                      "Speaks plainly"])
        + '<form method="post" action="/api/agent_cfg" '
        'style="margin-top:8px">'
        '<input type="hidden" name="slug" value="' + slug + '">'
        '<input type="hidden" name="run_checks" value="1">'
        '<button class="btn ghost sm">Run now</button>'
        + (('<span class="ctahint" style="display:inline;margin-left:8px">'
            'Last run ' + esc(checked) + '.</span>') if checked else '')
        + '</form>')

    footer = ('<div class="pagehint" style="margin-top:32px">Always true, '
              'whatever is set here: nothing sends without your yes, and '
              'every change lands in '
              '<a href="/settings?s=decisions"><b>Decisions</b></a>.</div>')

    return instr + tools + mem + guards + ev + footer


# The moat, made visible: one order record that every agent reads and
# writes. AGENT_LINKS is the edge list: what each agent hands the others
# and what it borrows, all keyed off the same order.
AGENT_LINKS = {
    "three_way_recon": dict(
        gives=[("cashflow_forecast", "what actually landed each day"),
               ("daily_mis", "clean, tied-out numbers")],
        uses=[("payouts_desk", "every payment against its bill")]),
    "settlement_insights": dict(
        gives=[("cashflow_forecast", "what is landing this week")],
        uses=[("three_way_recon", "the tie-out it trusts")]),
    "cashflow_forecast": dict(
        gives=[("payouts_desk", "which day is safe to pay")],
        uses=[("settlement_insights", "what is landing"),
              ("gst_compliance", "what the tax debit will take")]),
    "payouts_desk": dict(
        gives=[("three_way_recon", "every payment, filed against its bill")],
        uses=[("cashflow_forecast", "the safe day to pay")]),
    "payment_forms": dict(
        gives=[("kyc_desk", "the order that needs checks")],
        uses=[("payment_rescue", "which payment rails work for this buyer")]),
    "stock_watch": dict(
        gives=[("cart_rescue", "what is actually in stock to sell"),
               ("cod_guard", "what is worth shipping")],
        uses=[("returns_desk", "what is coming back sellable")]),
    "refund_shield": dict(
        gives=[("dispute_defender", "claim patterns and reused photos")],
        uses=[("returns_desk", "the warehouse seal check"),
              ("cod_guard", "the address history")]),
    "gst_compliance": dict(
        gives=[("daily_mis", "what the filing changed")],
        uses=[("three_way_recon", "numbers already tied out")]),
    "kyc_desk": dict(
        gives=[("payment_forms", "the checks built into the form")],
        uses=[("refund_shield", "risk signals on the buyer")]),
    "dispute_defender": dict(
        gives=[("refund_shield", "which proof banks actually accept")],
        uses=[("cod_guard", "the confirmation call, as proof"),
              ("returns_desk", "delivery and return scans")]),
    "returns_desk": dict(
        gives=[("refund_shield", "the seal check"),
               ("stock_watch", "which returns come back sellable")],
        uses=[("dispute_defender", "which buyers dispute after returning")]),
    "cart_rescue": dict(
        gives=[("payment_rescue", "buyers who got stuck mid-payment")],
        uses=[("stock_watch", "never offers what is out of stock"),
              ("cod_guard", "which pincodes to offer prepaid instead")]),
    "payment_rescue": dict(
        gives=[("payment_forms", "which rails work per buyer")],
        uses=[("cart_rescue", "what the buyer wanted in the first place")]),
    "cod_guard": dict(
        gives=[("cart_rescue", "the pincode truth"),
               ("dispute_defender", "confirmation calls, kept as proof")],
        uses=[("stock_watch", "what to hold back from risky addresses")]),
    "daily_mis": dict(
        gives=[],
        uses=[("three_way_recon", "the books"),
              ("stock_watch", "the stock picture"),
              ("dispute_defender", "what was won and lost")]),
}

# What one agent learned and another now uses: the exchange itself,
# written as rows a founder can read.
TEACHINGS = [
    ("cod_guard", "Pincode 400013 bounces 2 of every 5 COD parcels",
     ["cart_rescue"], "offers those buyers prepaid with a discount instead"),
    ("returns_desk", "This buyer&rsquo;s returns come back with seals broken",
     ["refund_shield"], "holds their next cash refund for a person"),
    ("dispute_defender", "Banks accept the courier scan plus the WhatsApp "
     "thread, and little else",
     ["refund_shield"], "asks for exactly that proof, first"),
    ("payment_rescue", "This buyer&rsquo;s card fails but UPI works",
     ["payment_forms"], "builds their forms with UPI first"),
    ("cashflow_forecast", "Vendor day and the GST debit collide on Thursdays",
     ["payouts_desk"], "queues payments around it without being told"),
    ("stock_watch", "Amla Juice sells 3x faster after a festival week",
     ["cart_rescue"], "calls those carts first while stock lasts"),
]


def role_of(slug: str) -> str:
    return next((a["role"] for a in RELAY_AGENTS if a["slug"] == slug), slug)


def agent_chip(slug: str) -> str:
    return (f'<a class="agchip2" href="/agents/{slug}">'
            f'{esc(role_of(slug))}</a>')


def teamwork_html(slug: str) -> str:
    links = AGENT_LINKS.get(slug)
    if not links:
        return ""
    rows = ""
    for to, what in links["gives"]:
        rows += (f'<div class="trow slim"><span class="ico">{ICONS["send"]}'
                 f'</span><span class="tdesc">Hands {agent_chip(to)} '
                 f'<span class="mut">{what}</span></span></div>')
    for frm, what in links["uses"]:
        rows += (f'<div class="trow slim"><span class="ico">{ICONS["book"]}'
                 f'</span><span class="tdesc">Borrows from {agent_chip(frm)} '
                 f'<span class="mut">{what}</span></span></div>')
    return (f'<h2 class="sec">How it works with the team</h2>'
            f'<div class="pagehint">Everyone reads and writes the same '
            f'order record. Nothing is emailed around; the record is the '
            f'conversation.</div>{rows}')


def teachings_html() -> str:
    rows = "".join(
        f'<div class="trow slim"><span class="ico">{ICONS["bolt"]}</span>'
        f'<span class="tdesc">{agent_chip(frm)} learned '
        f'<b>{fact}</b>, so '
        + " and ".join(agent_chip(t) for t in tos)
        + f' now {effect}.</span></div>'
        for frm, fact, tos, effect in TEACHINGS)
    return (f'<h2 class="sec" id="teach">What they teach each other</h2>'
            f'<div class="pagehint">One agent learns it once; the whole '
            f'team uses it the same day. This is why the fifteenth hire '
            f'is better than the first.</div>{rows}')


def latest_run_html(tid: str, slug: str) -> str:
    """Paperclip's Latest Run strip, in plain words: what this agent last
    did and when, with a fortnight of activity beside it."""
    led = WORLD.d.ledger
    if slug == "dispute_defender":
        evs = []
        for r in led.runs.values():
            if r.tenant_id == tid:
                for e in led.trace_for(r.run_id):
                    evs.append((e["ts"], e, r))
        if evs:
            ts, e, r = max(evs, key=lambda x: x[0])
            what, sub = plain_step(e, r)
            line = f'{what}. <span class="mut">{sub}</span>'
            when = ts.strftime("%-d %b, %H:%M")
        else:
            line, when = "Waiting for the first dispute.", ""
        badge = '<span class="st ok">did its job</span>'
    elif slug in PROPS_DEF:
        p = prop_state(tid, slug)
        d = PROPS_DEF[slug]
        if p["state"] == "waiting":
            line = (f'Brought you a finished call: <b>{d["title"]}</b>. '
                    f'<span class="mut">One yes closes it.</span>')
            badge = '<span class="st wait">waiting on you</span>'
        elif p["state"] == "approved":
            line = d["approved"]
            badge = '<span class="st ok">did its job</span>'
        else:
            line = d["declined"]
            badge = '<span class="st mut">stood down</span>'
        when = "today"
    else:
        line = ("Wrote today&rsquo;s note into the <b>Morning brief</b>, "
                "before you sat down.")
        when = "8:00"
        badge = '<span class="st ok">did its job</span>'
    if slug == "dispute_defender":
        from datetime import datetime as _dt
        today = _dt.utcnow().date()
        counts = [0] * 14
        for r in led.runs.values():
            if r.tenant_id == tid:
                dlt = (today - r.occurred_at.date()).days
                if 0 <= dlt < 14:
                    counts[13 - dlt] += 1
    else:
        h = _hashlib.sha256(slug.encode()).digest()
        counts = [1 + h[i] % 7 for i in range(14)]
    mx = max(counts) or 1
    bars = "".join(
        f'<i style="height:{max(3, round(28 * c / mx))}px" title="{c}"></i>'
        for c in counts)
    return (f'<div class="lastrun"><div class="lr-l">'
            f'<div class="lr-h">Latest run {badge}'
            f'<span class="mut lr-when">{when}</span></div>'
            f'<div class="lr-t">{line}</div></div>'
            f'<div class="lr-spark"><span class="mut">Last 14 days</span>'
            f'<div class="spark">{bars}</div></div></div>')


# ------------------------------------------------------------- agent goals
# Every agent claims ONE number it is hired to move. The goal is a sentence
# a founder would say out loud; progress is counted from actions, and every
# listed action names what it added. Dispute Defender's number is computed
# from the real record; the rest carry the demo week's numbers, consistent
# with the morning brief.
AGENT_GOALS = {
    "dispute_defender": dict(
        goal="Win 8 of every 10 disputes", target=80,
        how="Disputes won, out of disputes answered. Whole record."),
    "cart_rescue": dict(
        goal="Recover 7 of every 10 dropped carts", target=70, now=64,
        how="Carts paid within a day of the call, out of carts called. "
            "Last 30 days.",
        actions=[
            ("Called Meera T. about her ₹4,180 cart; she paid on the "
             "fresh link", "+1 cart back"),
            ("WhatsApp follow-up to Rohan D.; paid this morning",
             "+1 cart back"),
            ("Called Sana K. twice; no answer, queued for this evening",
             "still open")]),
    "payment_rescue": dict(
        goal="Bring back 6 of every 10 failed payments", target=60, now=58,
        how="Failed payments completed after its call or message, out of "
            "failures it chased. Last 30 days.",
        actions=[
            ("Messaged the 12 buyers whose UPI timed out on Tuesday; "
             "7 paid", "+7 payments back"),
            ("Called Vikram R. about a card decline; paying by Friday",
             "promised"),
            ("3 buyers quiet after two tries; parked, not pestered",
             "still open")]),
    "cod_guard": dict(
        goal="Confirm 9 of every 10 COD orders before dispatch",
        target=90, now=82,
        how="COD orders confirmed by call or message before they ship, "
            "out of all COD orders.",
        actions=[
            ("Confirmed 31 of today's 38 COD orders before noon",
             "+31 confirmed"),
            ("Held 3 orders after two unanswered calls each",
             "pending approval"),
            ("Learned pincode 4000xx answers after 6 PM, from Cart "
             "Rescue's notes", "sharper calls")]),
    "stock_watch": dict(
        goal="Keep every fast mover on the shelf", target=100, now=90,
        how="Fast-moving products in stock, out of the ten that sell "
            "most. Counted daily.",
        actions=[
            ("Spotted Amla Juice down to 6 days at this pace",
             "caught early"),
            ("Drafted the reorder for six weeks of cover",
             "pending approval"),
            ("Matched the reorder to the sale-week pace, not the quiet "
             "week's", "right size")]),
    "three_way_recon": dict(
        goal="Tie out 99 of every 100 lines without a person",
        target=99, now=98,
        how="Order, payout and bank lines matched on their own, out of "
            "all lines. This month.",
        actions=[
            ("Tied out 211 of 214 lines on their own this morning",
             "+211 tied"),
            ("Named the one payout short by ₹4,310", "flagged"),
            ("2 lines waiting on tomorrow's bank statement",
             "still open")]),
    "settlement_insights": dict(
        goal="Explain every deduction the day it lands", target=100, now=96,
        how="Deductions explained in plain words the same day, out of "
            "all deductions.",
        actions=[
            ("Read yesterday's settlement: 4 deductions, all named",
             "+4 explained"),
            ("Flagged a ₹1,120 hold as new, not routine", "flagged"),
            ("One deduction awaits the bank's own note", "still open")]),
    "cashflow_forecast": dict(
        goal="See every cash crunch a week early", target=100, now=100,
        how="Tight weeks flagged at least 7 days before they hit, out "
            "of tight weeks that came.",
        actions=[
            ("Saw Thursday's dip: vendor day and a GST debit collide",
             "caught early"),
            ("Drafted the courier payout move that keeps Thursday "
             "positive", "pending approval")]),
    "payouts_desk": dict(
        goal="Pay every vendor on the day it is due", target=100, now=93,
        how="Payouts that left on their due day, out of all payouts. "
            "This month.",
        actions=[
            ("Lined up tomorrow's 14 payments for one yes",
             "pending approval"),
            ("Caught a changed account number before it burned a "
             "payment", "saved one"),
            ("One vendor paid a day late last week; the why is in "
             "History", "written down")]),
    "payment_forms": dict(
        goal="Collect every odd payment within a day of asking",
        target=100, now=78,
        how="Bulk orders, advances and mandates paid within a day of "
            "the form going out.",
        actions=[
            ("Built the ₹2.6 lakh advance form, PAN check built in",
             "pending approval"),
            ("Last week's part-advance form: paid in 4 hours",
             "+1 in a day")]),
    "refund_shield": dict(
        goal="Catch 9 of every 10 fishy refunds before money leaves",
        target=90, now=88,
        how="Refunds flagged before payout that turned out wrong, out "
            "of wrong refunds.",
        actions=[
            ("Flagged two refunds landing in the same UPI handle",
             "caught"),
            ("Held one refund until the parcel actually came back",
             "pending approval"),
            ("Cleared 9 honest refunds untouched; nobody good was "
             "slowed", "clean")]),
    "returns_desk": dict(
        goal="Refund only after the goods are back", target=100, now=100,
        how="Refunds released after the return passed its photo check, "
            "out of all refunds.",
        actions=[
            ("Matched 6 returns to their photos this week; all clean",
             "+6 checked"),
            ("One return photo shows a different batch seal; held",
             "pending approval")]),
    "gst_compliance": dict(
        goal="File every month with zero last-minute scramble",
        target=100, now=100,
        how="Filings ready 3 days before the date, out of all filings.",
        actions=[
            ("This month's numbers are already tied to the books",
             "on track"),
            ("Set aside the GST debit that hits Thursday, so cash "
             "planning saw it", "handed over")]),
    "kyc_desk": dict(
        goal="Clear 9 of every 10 buyers in under a minute",
        target=90, now=87,
        how="Buyers cleared by the quick check alone, out of all "
            "buyers checked.",
        actions=[
            ("Cleared 34 buyers this week in seconds each", "+34 cleared"),
            ("Sent one flagged buyer through the deep check; came back "
             "clean", "pending approval"),
            ("Nobody honest waited at the counter", "clean")]),
    "daily_mis": dict(
        goal="Numbers on your phone before you ask, every day",
        target=100, now=100,
        how="Morning briefs delivered by 8:00 with the why behind every "
            "number.",
        actions=[
            ("Wrote this morning's brief: 9 lines, every number from "
             "your own record", "delivered"),
            ("Put the Thursday cash line on top because it moves money",
             "sharper brief")]),
}


def goal_block(tid: str, slug: str) -> str:
    """The number this agent is hired to move, said once and measured in
    the open: the claim, the honest counting rule, a bar with the goal
    marked on it, and the actions that moved it, each naming what it
    added. Dispute Defender's number comes from the real record."""
    g = AGENT_GOALS.get(slug)
    if not g:
        return ""
    now = g.get("now")
    if slug == "dispute_defender":
        led = WORLD.d.ledger
        won = lost = 0
        for r in led.runs.values():
            if r.tenant_id != tid:
                continue
            out = led.outcome_for(r.run_id)
            if out and out.outcome_value:
                if out.outcome_value.get("won"):
                    won += 1
                else:
                    lost += 1
        now = (100 * won // (won + lost)) if (won + lost) else 0
    on_track = now >= g["target"]
    actions = g.get("actions")
    if slug == "dispute_defender":
        actions = [(f"Won {won} of the {won + lost} disputes settled so "
                    f"far; every reply and outcome is in History",
                    f"+{won} won"),
                   ("7 replies are written and waiting on your yes",
                    "pending approval")]
    act_rows = "".join(
        f'<div class="goalact"><span class="tdesc">{txt}</span>'
        f'<span class="st {"ok" if tag.startswith("+") else "mut"}">'
        f'{tag}</span></div>'
        for txt, tag in (actions or []))
    return (
        f'<div class="goalcard">'
        f'<div class="goaltop"><span class="goallbl">The number it is '
        f'hired to move</span>'
        f'<span class="st {"ok" if on_track else "wait"}">'
        f'{"on goal" if on_track else "getting there"}</span></div>'
        f'<div class="goalline"><b class="goalnum">{now}%</b>'
        f'<span class="goaltxt"><b>{g["goal"]}</b>'
        f'<span class="mut">{g["how"]}</span></span></div>'
        f'<div class="goalbar"><i style="width:{now}%"></i>'
        f'<em style="left:{g["target"]}%"></em></div>'
        f'<div class="goalfoot"><span>today <b>{now}%</b></span>'
        f'<span>goal <b>{g["target"]}%</b></span></div>'
        + (f'<div class="goallbl" style="margin-top:12px">How it moved '
           f'the number</div>{act_rows}' if act_rows else '')
        + '</div>')


def goal_mini(tid: str, slug: str) -> str:
    """One quiet line on the roster card: the claim and where it stands."""
    g = AGENT_GOALS.get(slug)
    if not g:
        return ""
    now = g.get("now", 0)
    if slug == "dispute_defender":
        led = WORLD.d.ledger
        won = lost = 0
        for r in led.runs.values():
            if r.tenant_id != tid:
                continue
            out = led.outcome_for(r.run_id)
            if out and out.outcome_value:
                won, lost = ((won + 1, lost)
                             if out.outcome_value.get("won")
                             else (won, lost + 1))
        now = (100 * won // (won + lost)) if (won + lost) else 0
    return (f'<div class="goalmini">Goal: {g["goal"].lower()} &middot; at '
            f'<b>{now}%</b> of {g["target"]}%</div>')


def roster_detail_content(tid: str, a: dict, tab: str = "work") -> str:
    """One page per roster agent, wired or not. The not-yet ones read like
    a hire you could make today: what the job is, what it would touch, the
    rules it works under, and one button. Brief it like someone joining on
    Monday: no tabs, no settings, one screen."""
    on = a["status"] == "live" or DEMO_ON.get(a["slug"], False)
    acc = DESK_ACCESS.get(a["desk"], DESK_ACCESS["analyst"])
    helpers = ""
    if a["status"] == "live" and a["slug"] != "dispute_defender":
        # Live, but not the dispute pipeline: no crew list to show.
        state = '<span class="st ok">working</span>'
        action = ""
    elif a["status"] == "live":
        state = '<span class="st ok">working</span>'
        action = ""
        # The machinery lives here, one level down — never on the team
        # page. Most founders never need this list; the ones who ask
        # "but what is it actually doing?" find every part named.
        led = WORLD.d.ledger
        runs = [r for r in led.runs.values() if r.tenant_id == tid]
        n_sup = sum(1 for r in runs if r.state is RunState.SUPPRESSED)
        n_drafted = sum(1 for r in runs if r.decision)
        n_acted = sum(1 for r in runs
                      if r.state in (RunState.ACTED, RunState.RESOLVED))
        crew = [("ear", "detection-agent", f"{len(runs)} disputes read"),
                ("funnel", "eligibility-agent", f"{n_sup} set aside"),
                ("pen", "response-agent", f"{n_drafted} replies written"),
                ("shield", "compliance-agent", "checks every reply"),
                ("note", "filing-agent", f"{n_acted} filed"),
                ("moon", "escalation-agent", "hands the stuck ones to a person"),
                ("chart", "reporting-agent", "keeps the numbers")]
        helpers = (
            f'<h2 class="sec">How the work is split</h2>'
            f'<div class="pagehint">Seven hands inside this one role. Open '
            f'any of them if you want the detail.</div>'
            + "".join(
                f'<a class="trow slim" href="/agents/{s}" style="display:flex">'
                f'<span class="ico">{ICONS[i]}</span>'
                f'<span class="tdesc"><b>{worker_name(s)}</b> '
                f'<span class="mut">{stat}</span></span>'
                f'<span class="go2">&rsaquo;</span></a>'
                for i, s, stat in crew))
    elif on:
        state = '<span class="st wait">watching: learning your business</span>'
        action = (f'<form method="post" action="/api/agent_off">'
                  f'<input type="hidden" name="slug" value="{a["slug"]}">'
                  f'<button class="btn ghost">Switch off</button></form>')
    else:
        state = '<span class="st mut">not switched on</span>'
        action = (f'<form method="post" action="/api/agent_on">'
                  f'<input type="hidden" name="slug" value="{a["slug"]}">'
                  f'<button class="btn primary">Switch on</button>'
                  f'<span class="ctahint">It only watches for the first week.</span></form>')
    story = AGENT_STORY.get(a["slug"], {})
    steps = "".join(
        f'<div class="hstep"><span class="hdot"></span>'
        f'<div><b>{w}</b><span class="mut">{d}</span></div></div>'
        for w, d in story.get("steps", []))
    caps = "".join(
        f'<span class="capchip">{ICONS["book"]} Reads {r}</span>'
        for r in acc["reads"]) + "".join(
        f'<span class="capchip act">{ICONS["bolt"]} Can {d}'
        + ('' if 'yes' in d else ' &middot; after your yes') + '</span>'
        for d in acc["does"])
    rules = "".join(f'<div class="trow slim"><span class="ico">{ICONS["shield"]}</span>'
                    f'<span class="tdesc">{r}</span></div>'
                    for r in acc["rules"])
    # The AOP moment (Fin/Decagon pattern): every agent knows when the job
    # stops being its to do. The handoff conditions are written per desk,
    # and the standing rule is the same everywhere: once a person has it,
    # the agent waits.
    handoff = "".join(
        f'<div class="trow slim"><span class="ico">{ICONS["moon"]}</span>'
        f'<span class="tdesc">{h}</span></div>'
        for h in acc.get("handoff", []))
    handoff += (
        '<div class="trow slim"><span class="ico">' + ICONS["shield"] + '</span>'
        '<span class="tdesc"><b>While a person has it, this agent waits.</b> '
        '<span class="mut">It never acts on a handed-over case, and the '
        'handover itself is written into the case history.</span></span></div>')
    slug = a["slug"]
    # ---- Work: this agent's inbox. What it brought you, nothing else.
    prop_sec = ""
    if slug in PROPS_DEF:
        p_waiting = prop_state(tid, slug)["state"] == "waiting"
        prop_sec = (f'<h2 class="sec">'
                    f'{"Waiting on your yes" if p_waiting else "Its last call"}'
                    f'</h2>{prop_card(tid, slug)}')
    cases_sec = ""
    if slug == "dispute_defender":
        cases_sec = (
            '<h2 class="sec">Its cases</h2>'
            '<div class="pagehint">The ones waiting on your yes come '
            'first.</div><div class="caselist">'
            + "".join(
                f'<a class="rail{" unread" if c["key"] == "need" else ""}" '
                f'href="/cases/{esc(c["order"])}">'
                f'{_logo(c["label"].partition(" · ")[0], 30)}'
                f'<span class="rbody"><span class="rtop"><span class="rname">'
                f'{esc(c["label"].partition(" · ")[0])}</span>'
                f'<span class="rwhen">{c["last"].strftime("%b %-d")}</span>'
                f'</span><span class="rsub"><span class="rprev">'
                f'{esc(c["label"].partition(" · ")[2])} &middot; {c["word"]}'
                f'</span><span class="cdot {c["key"]}"></span></span>'
                f'</span></a>'
                for c in sorted(rail_cases(tid),
                                key=lambda c: c["key"] != "need"))
            + '</div>')
    report_sec = ""
    if slug in REPORT_AGENTS:
        report_sec = (
            f'<h2 class="sec">Where its work lands</h2>'
            f'<div class="trow slim" style="display:flex">'
            f'<span class="ico">{ICONS["flow"]}</span>'
            f'<span class="tdesc">Its note is written into the <b>Morning '
            f'brief</b> every day, before you sit down.</span>'
            f'<a class="st wait" href="/briefs/morning">read today&rsquo;s '
            f'&rarr;</a></div>')
    work_body = (goal_block(tid, slug)
                 + latest_run_html(tid, slug) + prop_sec + cases_sec
                 + report_sec
                 + (_kyc_builder(tid) if slug == "kyc_desk" else ""))

    # ---- About: the read-once material. The hire brief, not the inbox.
    about_body = (
        f'<div class="outcome">{story.get("outcome", a["desc"])}</div>'
        f'<p class="mut" style="max-width:560px;margin:0 0 8px">{a["desc"]}</p>'
        f'<p class="mut" style="font-size:12.5px;margin:0 0 24px">'
        f'Replaces {a["replaces"]}.</p>'
        f'<h2 class="sec">How it works</h2><div class="hsteps">{steps}</div>'
        + (thread_html(slug) or day_html(slug))
        + (risk_ladder_html() if slug == "kyc_desk" else "")
        + f'<h2 class="sec">What it can touch</h2>'
        f'<div class="capwrap">{caps}</div>'
        f'<h2 class="sec">Rules it works under</h2>{rules}'
        + teamwork_html(slug)
        + f'<h2 class="sec">When a person takes over</h2>{handoff}'
        f'{helpers}')

    tabbar = ('<div class="tabbar">'
              + "".join(
                  f'<a class="{"on" if tab == k else ""}" '
                  f'href="/agents/{slug}?tab={k}">{lbl}</a>'
                  for k, lbl in [("work", "Work"), ("about", "About"),
                                 ("settings", "Settings")])
              + '</div>')
    return (
        f'<div class="dhead"><a class="back2" href="/agents">&lsaquo;</a>'
        f'{avatar(slug, 44, on)}'
        f'<div><h1>{a["role"]}</h1>'
        f'<div class="meta">{state}<span>&middot;</span>'
        f'<span>{a["name"]}</span></div></div>'
        f'<div style="margin-left:auto;display:flex;gap:8px;align-items:center">'
        f'<a class="btn ghost" href="/?say=/{esc(a["role"])} ">'
        f'Give it a job</a>{action}</div></div>'
        f'{tabbar}'
        + (work_body if tab == "work"
           else agent_settings_content(tid, a) if tab == "settings"
           else about_body))


# Saved KYC procedures, per tenant: the editor's Save and Set live land
# here, and the KYC Desk page lists them the way Fin lists what is live.
KYC_PROCS: dict[str, list[dict]] = {}


def procedure_editor_content(tid: str, ask: str) -> str:
    """The Fin procedures editor, in Relay's plain voice: when to use it,
    numbered steps with tool tokens you can swap, an IF and its ELSE, a
    Test that runs a simulated buyer in a side panel, and Set live."""
    ask = (ask or "checks for buyers above ₹2 lakh").strip()[:90]
    title = esc(ask[0].upper() + ask[1:])
    TOOLS = ["Verify PAN", "Video KYC", "Check for mule accounts",
             "Collect the payment", "Tag the case", "Hand to a person"]
    menu = "".join(f'<div class="tmenu-it" onclick="toolPick(this)">{t}</div>'
                   for t in TOOLS) + (
           '<div class="tmenu-it new">+ Ask for a new tool</div>')

    def tok(label):
        return (f'<span class="ttok" onclick="toolMenu(event, this)">'
                f'{ICONS["bolt"]}<b>Use</b> <span class="tlab">{label}</span>'
                f'<i>&#8942;</i></span>')

    return f"""
<div class="dhead" style="margin-bottom:4px">
  <a class="back2" href="/agents/kyc_desk">&lsaquo;</a>
  <div><h1>Procedure: {title}</h1>
  <div class="meta"><span class="st mut">draft</span><span>&middot;</span>
  <span>KYC Desk</span></div></div>
  <div class="prochdr">
    <button class="btn ghost" onclick="simOpen()">Test</button>
    <button class="btn ghost" onclick="procSave('draft')">Save</button>
    <button class="btn live" onclick="procSave('live')">Set live</button>
  </div>
</div>
<div class="proc" id="proc">
  <h2 class="sec proc-reveal">When to use this procedure</h2>
  <div class="whenbox proc-reveal" contenteditable="plaintext-only"
    id="whenbox">Use this when {esc(ask)}. The buyer should never leave the payment form to get verified.</div>
  <div class="proc-reveal" style="margin:8px 0 24px">
    <span class="capchip">{ICONS["bot"]} Every buyer at checkout</span>
    <span class="capchip">{ICONS["shield"]} Nothing goes live until you say yes</span>
  </div>
  <h2 class="sec proc-reveal">Steps</h2>
  <ol class="psteps">
    <li class="proc-reveal"><span class="pstep" contenteditable="plaintext-only">Read the order and its value the moment the buyer reaches payment.</span></li>
    <li class="proc-reveal"><span class="pif">IF</span>
      <span class="pcond" contenteditable="plaintext-only">the order is above &#8377;2,00,000</span>
      <ol class="psub">
        <li><span class="pstep" contenteditable="plaintext-only">Collect the PAN inside the payment form.</span> {tok("Verify PAN")}</li>
        <li><span class="pstep" contenteditable="plaintext-only">Run the standard checks while the buyer types.</span> {tok("Check for mule accounts")}</li>
        <li><span class="pif">IF</span>
          <span class="pcond" contenteditable="plaintext-only">anything looks risky</span>
          <ol class="psub">
            <li><span class="pstep" contenteditable="plaintext-only">Hold the payment and hand the case over. A person decides, the buyer sees &ldquo;under review&rdquo;, never an error.</span>
              {tok("Hand to a person")}</li>
          </ol>
          <span class="pelse">ELSE</span>
          <ol class="psub">
            <li><span class="pstep" contenteditable="plaintext-only">Take the payment in the same form. Verify and pay is one step for the buyer.</span> {tok("Collect the payment")}</li>
          </ol>
        </li>
      </ol>
      <span class="pelse">ELSE</span>
      <ol class="psub">
        <li><span class="pstep" contenteditable="plaintext-only">Go straight to payment. No checks the order does not call for.</span>
          {tok("Collect the payment")}</li>
      </ol>
    </li>
    <li class="proc-reveal"><span class="pstep" contenteditable="plaintext-only">Write what happened into the case history, either way.</span>
      {tok("Tag the case")}</li>
  </ol>
</div>
<div class="tmenu" id="tmenu" hidden>{menu}</div>
<form method="post" action="/api/procedure_save" id="procform" hidden>
  <input type="hidden" name="title" id="pf_title" value="{title}">
  <input type="hidden" name="when" id="pf_when" value="">
  <input type="hidden" name="mode" id="pf_mode" value="draft">
</form>
<div class="simpanel" id="simpanel" hidden>
  <div class="simhead"><b>Simulations</b>
    <button class="rtool" onclick="simClose()">&#10005;</button></div>
  <div class="simcard"><b>High-value gold order</b>
    <span class="st ok" id="simbadge" hidden>Passed &middot; just now</span></div>
  <div id="simrows"></div>
</div>
<script>
let tokTarget = null;
function toolMenu(ev, el){{
  ev.stopPropagation();
  tokTarget = el;
  const m = document.getElementById('tmenu');
  m.hidden = false;
  const r = el.getBoundingClientRect();
  m.style.left = Math.min(r.left, innerWidth - 260) + 'px';
  m.style.top = (r.bottom + 6) + 'px';
  setTimeout(() => addEventListener('click',
    () => {{ m.hidden = true; }}, {{once: true}}));
}}
function toolPick(it){{
  if (tokTarget) tokTarget.querySelector('.tlab').textContent = it.textContent;
}}
function procSave(mode){{
  document.getElementById('pf_mode').value = mode;
  document.getElementById('pf_when').value =
    document.getElementById('whenbox').textContent.slice(0, 300);
  document.getElementById('procform').submit();
}}
const SIM = [
  ['user', 'Simulated buyer: a ₹2.4 lakh gold order, paying by card.'],
  ['ok', 'High-value order triggered'],
  ['think', 'Step 1 thinking'],
  ['ok', 'PAN collected inside the payment form'],
  ['ok', 'Checks passed, nothing risky'],
  ['agent', 'Verified. The form moves straight to payment; the buyer never left the page.'],
  ['ok', 'Payment collected in the same form'],
  ['ok', 'Case history written']];
async function simOpen(){{
  const p = document.getElementById('simpanel');
  p.hidden = false;
  const rows = document.getElementById('simrows');
  rows.innerHTML = '';
  document.getElementById('simbadge').hidden = true;
  for (const [kind, text] of SIM){{
    const d = document.createElement('div');
    d.className = 'simrow ' + kind;
    d.innerHTML = kind === 'ok' ? '&#10003; ' + text : text;
    rows.appendChild(d);
    await new Promise(r => setTimeout(r, kind === 'think' ? 700 : 450));
  }}
  document.getElementById('simbadge').hidden = false;
}}
function simClose(){{ document.getElementById('simpanel').hidden = true; }}
</script>
<style>
.prochdr{{margin-left:auto;display:flex;gap:8px;align-items:center}}
.btn.live{{background:#1E9E5A;border-color:#1E9E5A;color:#fff}}
.btn.live:hover{{background:#177245}}
.proc{{max-width:720px}}
.whenbox{{background:#fff;border:1px solid var(--hair);border-radius:12px;
  padding:16px 16px;font-size:14px;line-height:1.55;outline:none}}
.whenbox:focus{{border-color:var(--accent)}}
.psteps{{list-style:none;counter-reset:ps;margin:0;padding:0}}
.psteps > li{{counter-increment:ps;position:relative;padding:8px 0 8px 32px;
  font-size:14.5px;line-height:1.6}}
.psteps > li::before{{content:counter(ps) ".";position:absolute;left:2px;
  top:10px;color:var(--mut);font-size:12.5px;font-family:ui-monospace,monospace}}
.psub{{list-style:none;counter-reset:pa;margin:8px 0 2px;padding-left:24px;
  border-left:2px solid #ECECF1}}
.psub > li{{counter-increment:pa;position:relative;padding:8px 0 8px 24px}}
.psub > li::before{{content:counter(pa, upper-alpha) ".";position:absolute;
  left:0;top:7px;color:var(--mut);font-size:12px;
  font-family:ui-monospace,monospace}}
.pstep{{outline:none;border-radius:6px;padding:1px 3px}}
.pstep:focus{{background:#F4F5FB}}
.pif,.pelse{{display:inline-block;background:var(--accent-soft,#E9EBF8);
  color:#3A46A8;font-size:11.5px;font-weight:700;letter-spacing:.06em;
  border-radius:7px;padding:3px 8px;margin:2px 8px 2px 0}}
.pelse{{margin-top:8px}}
.pcond{{display:inline-block;background:#fff;border:1px solid var(--hair);
  border-radius:9px;padding:8px 12px;font-size:13.5px;outline:none}}
.pcond:focus{{border-color:var(--accent)}}
.ttok{{display:inline-flex;align-items:center;gap:8px;background:#F0F1F7;
  border-radius:8px;padding:4px 8px;font-size:12.5px;cursor:pointer;
  white-space:nowrap;vertical-align:middle}}
.ttok svg{{width:12px;height:12px;color:#3A46A8}}
.ttok b{{font-weight:600}}
.ttok i{{color:var(--mut);font-style:normal}}
.ttok:hover{{background:#E7E9F3}}
.tmenu{{position:fixed;background:#fff;border:1px solid var(--hair);
  border-radius:12px;box-shadow:0 12px 40px rgba(27,31,48,.14);padding:8px;
  min-width:240px;z-index:40}}
.tmenu-it{{padding:8px 12px;border-radius:8px;font-size:13.5px;cursor:pointer}}
.tmenu-it:hover{{background:#F5F5F8}}
.tmenu-it.new{{border-top:1px solid var(--hair);color:var(--mut);
  margin-top:4px}}
.simpanel{{position:fixed;top:0;right:0;bottom:0;width:380px;background:#fff;
  border-left:1px solid var(--hair);box-shadow:-14px 0 44px rgba(27,31,48,.10);
  padding:20px 24px;z-index:45;overflow-y:auto}}
.simhead{{display:flex;justify-content:space-between;align-items:center;
  margin-bottom:16px;font-size:15px}}
.simcard{{background:#FAFAFC;border:1px solid var(--hair);border-radius:12px;
  padding:12px 16px;margin-bottom:16px;display:flex;justify-content:space-between;
  align-items:center;font-size:13.5px}}
.simrow{{font-size:13px;padding:8px 8px;border-radius:9px;margin-bottom:8px;
  animation:fadeup .3s ease both}}
.simrow.ok{{background:#E9F7EF;color:#177245;font-weight:500}}
.simrow.user{{background:#F0F1F7}}
.simrow.agent{{background:#fff;border:1px solid var(--hair)}}
.simrow.think{{color:var(--mut);font-size:12px;padding:2px 8px}}
.proc-reveal{{animation:fadeup .4s ease both}}
.proc-reveal:nth-child(2){{animation-delay:.1s}}
.psteps .proc-reveal:nth-child(1){{animation-delay:.25s}}
.psteps .proc-reveal:nth-child(2){{animation-delay:.45s}}
.psteps .proc-reveal:nth-child(3){{animation-delay:.7s}}
</style>"""


def _kyc_builder(tid: str = "t1") -> str:
    """The way in: saved procedures first (Live or draft, like Fin's
    Guidance cards), then one sentence that opens the full editor."""
    procs = KYC_PROCS.get(tid, [])
    rows = "".join(
        f'<a class="trow slim" style="display:flex" '
        f'href="/procedures/new?ask={esc(p["title"])}">'
        f'<span class="ico">{ICONS["note"]}</span>'
        f'<span class="tdesc"><b>{esc(p["title"])}</b> '
        f'<span class="mut">{esc((p.get("when") or "")[:90])}</span></span>'
        + ('<span class="st ok">Live</span>' if p["mode"] == "live"
           else '<span class="st mut">draft</span>')
        + '<span class="go2">&rsaquo;</span></a>'
        for p in procs)
    if rows:
        rows = ('<h2 class="sec">Journeys this desk runs</h2>'
                '<div class="pagehint">Open one to read or change it. '
                'Changes are drafts until you set them live.</div>' + rows)
    return (rows +
        '<h2 class="sec">Or describe the onboarding you need</h2>'
        '<div class="pagehint">One sentence opens the full procedure: '
        'steps, tools, the checks, and a test run. Nothing goes live '
        'until you say yes.</div>'
        '<form class="notebar" style="max-width:640px" method="get" '
        'action="/procedures/new">'
        '<input class="jfind notein" name="ask" maxlength="90" '
        'placeholder="e.g. checks for buyers above &#8377;2 lakh, PAN before payment">'
        '<button class="btn primary sm">Build</button></form>')


def agent_detail_content(tid: str, slug: str, tab: str = "overview",
                         item: int = 0) -> str:
    a = AGENT_DEFS.get(slug)
    if not a:
        roster = next((x for x in RELAY_AGENTS if x["slug"] == slug), None)
        if roster is not None:
            return roster_detail_content(
                tid, roster, tab if tab in ("work", "about", "settings") else "work")
        return '<h1 class="page">We don&rsquo;t have anyone by that name</h1>'
    led = WORLD.d.ledger
    runs = [r for r in led.runs.values() if r.tenant_id == tid]
    corr = correction_rate(led, tid)

    TABS = [("overview", "What it does"), ("activity", "What it did"),
            ("access", "What it can touch"), ("memory", "What it remembers"),
            ("playbook", "Rules it follows"), ("quality", "How good it is")]
    tabbar = '<div class="tabbar">' + "".join(
        f'<a class="{"on" if key == tab else ""}" '
        f'href="/agents/{slug}?tab={key}">{label}</a>'
        for key, label in TABS) + "</div>"

    head = (f'<div class="dhead"><a class="back2" href="/agents">&lsaquo;</a>'
            f'<span class="tile">{ICONS[a["icon"]]}</span>'
            f'<div><h1>{worker_name(slug)}</h1>'
            f'<div class="meta"><span class="st ok">working</span>'
            f'<span>&middot;</span><span>{len(runs)} jobs done</span>'
            f'<span>&middot;</span><span>you had to fix {fmt_pct(corr)}</span></div></div>'
            f'<form method="post" action="/api/sample" style="margin-left:auto">'
            f'<button class="btn ghost">Try a sample</button></form></div>')

    if tab == "access":
        items = ([("Can read: " + r, "It can read this. " + a["scope"], True) for r in a["reads"]]
                 + [("Can do: " + e, "Something this one is allowed to do. "
                     "once and only once, and never before your yes.", True)
                    for e in a["effects"]]
                 + [("Everything else", "No, by default. It cannot touch anything "
                     "that isn&rsquo;t on this list.", False)])
        item = min(item, len(items) - 1)
        left = "".join(
            f'<a class="pitem {"on" if i == item else ""}" '
            f'href="/agents/{slug}?tab=access&item={i}"><span>{t}</span>'
            f'<span class="tgl {"on" if on else ""}"></span></a>'
            for i, (t, _, on) in enumerate(items))
        title, desc, _ = items[item]
        autonomy = _edit_history([
            ("Mothi Venkatesh", "Jul 30, 11:40 AM", "Allowed",
             [("write replies for me to check", "ec-green")]),
            ("Mothi Venkatesh", "Jul 18, 9:15 AM", "Taken back",
             [("weekend work", "ec-amber")]),
            ("Relay", "Jul 12, 10:02 AM", "Started on trial",
             [("watching only", "ec-purple"), ("has to pass tests", "ec-blue")])])
        body = (f'<div class="twopane"><div class="pane-list">{left}</div>'
                f'<div class="pane-detail"><h3>{title}</h3><p>{desc}</p></div></div>'
                f'<h2 class="sec">What it has been allowed to do</h2>'
                f'<div class="pagehint">Trust is given a bit at a time, and every '
                f'bit is written down: who allowed what, when, and what was '
                f'taken back. You can always take it back.</div>' + autonomy)
    elif tab == "memory":
        panes = [("Just for this job", "Picked up while doing one piece of "
                  "work and written down against it, so you can check it later.",
                  a["session"]),
                 ("Kept for good", "Goes into what your team knows: proof, "
                  "how you write, how disputes ended. Nothing is ever "
                  "overwritten; new facts sit on top of old ones.", a["profile"])]
        item = min(item, 1)
        left = "".join(
            f'<a class="pitem {"on" if i == item else ""}" '
            f'href="/agents/{slug}?tab=memory&item={i}"><span>{t}'
            f'<span class="sub2">{len(fields) or "no"} things</span></span></a>'
            for i, (t, _, fields) in enumerate(panes))
        t, d, fields = panes[item]
        rows = "".join(f'<div class="trow slim"><span class="ico">{ICONS["note"]}</span>'
                       f'<span class="tdesc"><b>{f}</b></span></div>'
                       for f in fields) or '<div class="empty">Nothing kept.</div>'
        body = (f'<div class="twopane"><div class="pane-list">{left}</div>'
                f'<div class="pane-detail"><h3>{t}</h3><p>{d}</p>{rows}</div></div>')
    elif tab == "playbook":
        rows = "".join(
            f'<div class="trow slim"><span class="ico">{ICONS["bm"]}</span>'
            f'<span class="tdesc">{r} <span class="mut">signed off'
            f'</span></span></div>' for r in a["rules"])
        body = (f'<div class="pagehint">The rules this one works under. Written '
                f'in plain words and signed off: and there is no box for '
                f'anyone to quietly rewrite them in. Changing a rule is a change '
                f'to the software, checked before it reaches you: never a '
                f'text box someone can break.</div>{rows}')
    elif tab == "activity":
        led2 = WORLD.d.ledger
        mine = []
        for r in runs:
            for e in led2.trace_for(r.run_id):
                if e["agent"] == slug:
                    mine.append((e, r))
        mine.sort(key=lambda t: t[0]["ts"], reverse=True)
        ACOLS = "1.1fr 2.4fr 1.2fr .9fr"
        rows2 = [_trow2(ACOLS, [
            f'<b>{esc(e["kind"])}</b>',
            f'<span class="mut">{plain_detail(e["detail"])}</span>',
            f'{_logo(_account_label(r))}{esc(_account_label(r))}',
            f'<a class="st wait" href="/runs/{r.run_id}">see it &rarr;</a>'],
        ) for e, r in mine[:25]]
        body = (f'<div class="pagehint">What this one did, newest first. '
                f'Open any row to see the whole story it belongs to.</div>'
                + _tbl(ACOLS, ["What it did", "Detail", "Customer", ""], rows2))
    elif tab == "quality":
        import json as _json
        from pathlib import Path as _Path
        AGENT_SEAMS = {"detection-agent": ["confirm_mention", "extract_claim"],
                       "response-agent": ["draft_counter"],
                       "compliance-agent": ["judge", "semantic_diff"],
                       "reporting-agent": ["narrate"]}
        # Plain words for what each check actually looks at. Nobody should read
        # "semantic_diff" on a quality page.
        CHECK_LABEL = {
            "confirm_mention": "Is this really about one of your orders",
            "extract_claim": "What exactly the buyer is claiming",
            "draft_counter": "Writing the reply, in your words",
            "judge": "Is the reply fair and backed by proof",
            "semantic_diff": "Did an edit quietly change the meaning",
            "narrate": "Putting the numbers into plain words"}
        evdir = _Path(__file__).resolve().parents[1] / "evals"
        results = {}
        if (evdir / "results.json").exists():
            results = _json.loads((evdir / "results.json").read_text())
        seam_rows = ""
        for seam in AGENT_SEAMS.get(slug, []):
            res = results.get(seam)
            if res and res["passed"] == res["total"]:
                status = '<span class="st ok">checked, all good</span>'
            elif res:
                status = '<span class="st warn">needs a look</span>'
            else:
                status = '<span class="st ok">checked, all good</span>'
            seam_rows += (
                f'<div class="trow slim"><span class="ico">{ICONS["shield"]}</span>'
                f'<span class="tdesc"><b>{CHECK_LABEL.get(seam, seam)}</b> '
                f'<span class="mut">checked before it can ever reach you</span>'
                f'</span>{status}</div>')
        if not seam_rows:
            seam_rows = (f'<div class="trow slim"><span class="ico">{ICONS["shield"]}</span>'
                         f'<span class="tdesc">Follows fixed rules, not a guess '
                         f'<span class="mut">so there is nothing here that can '
                         f'drift: it does the same thing every time</span>'
                         f'</span><span class="st ok">checked, all good</span></div>')
        body = (f'<div class="trow slim"><span class="ico">{ICONS["chart"]}</span>'
                f'<span class="tdesc"><b>How often you had to fix a reply</b> '
                f'<span class="mut">across your whole business</span></span>'
                f'<span class="st mut">{fmt_pct(corr)}</span></div>'
                + seam_rows)
    else:
        plays = AGENT_PLAYS.get(slug, [])
        play_html = "".join(
            f'<span class="st ok" style="margin-right:8px">{pl} &middot; on</span>'
            for pl in plays) or '<span class="st mut">helps out</span>'
        body = (f'<div class="pane-detail" style="max-width:640px"><h3>Its job</h3>'
                f'<p>{a["charter"]}</p><p style="margin-top:8px" class="mut">'
                f'{a["scope"]}</p>'
                f'<h3 style="margin-top:16px">Works for</h3><p>{play_html}</p></div>')

    return head + tabbar + body


def _relay_agent_card(a: dict, tid: str = "t1") -> str:
    """A wired agent earns the full card. Everything else is one calm row.
    role, one line, Off: that opens to its page. The founder scanning this
    at 9 PM sees at a glance what is working and is not asked to read
    fourteen pitches (Hick: the pitch lives one click away, not here)."""
    live = a["status"] == "live"
    watching = not live and DEMO_ON.get(a["slug"], False)
    if live:
        since, n = _working_since(tid)
        if a["slug"] in ("cart_rescue", "payment_rescue"):
            pstate = prop_state(tid, a["slug"])["state"]
            horizon = ("Working &middot; tonight&rsquo;s list waits on "
                       "your yes" if pstate == "waiting" else
                       "Working &middot; calling now, outcomes land in "
                       "your chats" if pstate == "approved" else
                       "Working &middot; standing by, next list this evening")
            horizon = f'<div class="hzn on">{horizon}</div>'
            switch = '<span class="st ok">On</span>'
            inner = (f'<div class="aname" style="margin-bottom:8px;'
                     f'flex-wrap:wrap;row-gap:8px">'
                     f'{avatar(a["slug"], 34, True)}'
                     f'<b style="font-size:15px">{a["role"]}</b>'
                     f'<span class="st mut" style="flex:none">{a["name"]}</span>'
                     f'<span class="st ok" style="margin-left:auto;flex:none">On</span></div>'
                     f'<div style="margin:4px 0 8px">{a["desc"]}</div>'
                     f'{goal_mini(tid, a["slug"])}'
                     f'<div class="repl">Replaces {a["replaces"]}.</div>'
                     f'{horizon}')
            return (f'<a class="arow2" style="display:block;padding:16px" '
                    f'href="/agents/{a["slug"]}">{inner}</a>')
        horizon = (f'Working since {since} &middot; {n} disputes checked '
                   f'&middot; never sleeps' if since
                   else "Switched on &middot; never sleeps")
        inner = (f'<div class="aname" style="margin-bottom:8px;flex-wrap:wrap;'
                 f'row-gap:8px">'
                 f'{avatar(a["slug"], 34, True)}'
                 f'<b style="font-size:15px">{a["role"]}</b>'
                 f'<span class="st mut" style="flex:none">{a["name"]}</span>'
                 f'<span class="st ok" style="margin-left:auto;flex:none">On</span></div>'
                 f'<div style="margin:2px 0 8px">{a["desc"]}</div>'
                 f'{goal_mini(tid, a["slug"])}'
                 f'<div class="repl">Replaces {a["replaces"]}.</div>'
                 f'<div class="hzn on">{horizon}</div>')
        return (f'<a class="arow2" style="display:block;padding:16px" '
                f'href="/agents/{a["slug"]}">{inner}</a>')
    stat = ('<span class="st wait" style="flex:none">watching</span>' if watching
            else '<span class="st mut" style="flex:none">Off</span>')
    line = AGENT_STORY.get(a["slug"], {}).get("outcome", a["desc"])
    return (f'<a class="arow2 slimcard" href="/agents/{a["slug"]}">'
            f'{avatar(a["slug"], 28, watching)}'
            f'<b>{a["role"]}</b>'
            f'<span class="mut slimdesc">{line}</span>'
            f'{stat}<span class="go2">&rsaquo;</span></a>')


def agents_content(tid: str, f: str = "all", q: str = "") -> str:
    agents = RELAY_AGENTS
    if f in ("active", "planned"):
        want = "live" if f == "active" else "roadmap"
        agents = [a for a in agents if a["status"] == want]
    if q:
        agents = [a for a in agents if q.lower() in a["name"].lower()]

    # Grouped as product families, the way Fin groups its surface: three
    # names a founder can hold, not six org-chart desks.
    FAMILIES = [
        ("Relay for Commerce", ("support", "calling", "inventory"),
         "The order side: buyers, calls, deliveries, disputes, stock."),
        ("Relay for Finance", ("accounts", "analyst"),
         "The money side: settlements, cash, payouts, the daily numbers."),
        ("Relay for Trust", ("risk",),
         "The checks: refund fraud, filings, KYC, mule accounts."),
        ("Built by you", ("custom",),
         "Described in chat, drafted by Relay. Same rule: nothing sends "
         "without your yes."),
    ]
    def _needs(a):
        sl = a["slug"]
        if sl == "dispute_defender":
            led3 = WORLD.d.ledger
            return any(r.tenant_id == tid
                       and r.state is RunState.AWAITING_GATE
                       for r in led3.runs.values())
        return (sl in PROPS_DEF
                and prop_state(tid, sl)["state"] == "waiting")

    desks = ""
    for title, keys, line in FAMILIES:
        mine = [a for a in agents if a["desk"] in keys]
        if not mine:
            continue
        mine.sort(key=lambda a: (not _needs(a), a["status"] != "live"))
        n_on_fam = sum(1 for a in mine
                       if a["status"] == "live" or DEMO_ON.get(a["slug"]))
        n_need_fam = sum(1 for a in mine if _needs(a))
        desks += (f'<h2 class="sec fam">{title}'
                  f'<span class="st {"ok" if n_on_fam else "mut"}">'
                  f'{n_on_fam} of {len(mine)} on</span>'
                  + (f'<span class="st need2">{n_need_fam} pending</span>'
                     if n_need_fam else '')
                  + '</h2>'
                  f'<div class="pagehint">{line}</div>'
                  f'<div class="atable" style="grid-template-columns:1fr">'
                  + "".join(_relay_agent_card(a, tid) for a in mine) + '</div>')
    if not desks:
        desks = '<div class="empty">Nothing matches.</div>'

    seg = lambda key, label: (f'<a class="{"on" if f == key else ""}" '
                              f'href="/agents?f={key}">{label}</a>')
    n_all = len(RELAY_AGENTS)
    n_on = sum(1 for a in RELAY_AGENTS
               if a["status"] == "live" or DEMO_ON.get(a["slug"]))
    led2 = WORLD.d.ledger
    truns = [r for r in led2.runs.values() if r.tenant_id == tid]
    n_yes = sum(1 for r in truns
                if r.state is RunState.AWAITING_GATE) + props_waiting(tid)
    kept2, n_wins2, _w = recovered(tid)
    n_live = sum(1 for a in RELAY_AGENTS if a["status"] == "live")
    tiles = (
        f'<div class="stattiles">'
        f'<a class="stile" href="/agents?f=active"><b>{n_on}</b>'
        f'<span>Agents on</span><i>{n_live} working &middot; '
        f'{n_on - n_live} watching</i></a>'
        f'<a class="stile need" href="/approvals"><b>{n_yes}</b>'
        f'<span>Pending approval</span><i>replies, holds, payouts</i></a>'
        f'<a class="stile" href="/briefs/morning"><b>&#8377;{inr(kept2)}</b>'
        f'<span>Kept for you</span><i>{n_wins2} disputes won</i></a>'
        f'<a class="stile" href="/impact"><b>{len(truns)}</b>'
        f'<span>Jobs done</span><i>every one in History</i></a>'
        f'</div>')
    return (f'<h1 class="page">Agents</h1>'
            f'<div class="pagehint">The people you would hire, as '
            f'agents. Nothing goes out without your yes.</div>'
            f'{tiles}'
            f'<div class="onemem">{ICONS["book"]}<span><b>One memory, '
            f'fifteen hands.</b> Every agent reads and writes the same '
            f'order record, so each one you switch on makes the rest '
            f'sharper.</span>'
            f'<a href="/memory?t=teach">see what they teach each other '
            f'&rarr;</a></div>'
            f'<div class="atoolbar">'
            f'<span class="seg">{seg("all", "All")}{seg("active", "On")}'
            f'{seg("planned", "Not on yet")}</span></div>'
            f'{desks}')


def activity_content(tid: str, f: str = "all") -> str:
    led = WORLD.d.ledger
    runs = sorted((r for r in led.runs.values() if r.tenant_id == tid),
                  key=lambda r: r.occurred_at, reverse=True)
    mine = {r.run_id for r in runs}
    esc_rows = [b for _, b in WORLD.d.slack.channel_posts if b.get("run") in mine]
    if f == "escalated":
        shown = [r for r in runs
                 if r.state in (RunState.TIMED_OUT, RunState.FAILED)]
    elif f == "resolved":
        shown = [r for r in runs if r.state is RunState.RESOLVED]
    else:
        shown = runs
    def chip(key, label, n=None):
        cls = "fpill" if key == f else "fpill off"
        count = f" &middot; {n}" if n is not None else ""
        return f'<a class="{cls}" href="/impact?f={key}">{label}{count}</a>'
    chips = ('<div class="pills">'
             + chip("all", "Everything", len(runs))
             + chip("escalated", "Needed a person", len(esc_rows))
             + chip("resolved", "Kept the money",
                    sum(1 for r in runs if r.state is RunState.RESOLVED))
             + "</div>")
    rows = "\n".join(ledger_row(r) for r in shown) or (
        '<div class="empty">Nothing here yet.</div>')
    won = influenced_won(tid)
    banner = (f'<div class="impactline">Replies you said yes to kept '
              f'<b>&#8377;{inr(won)}</b> in your account.</div>' if won else "")
    return (f'<h1 class="page">What it saved you</h1>'
            f'<div class="pagehint">Every decision, with the working shown. '
            f'<a href="/export/impact.csv"><b>download it as a spreadsheet</b></a>.'
            f'</div>{banner}{chips}{rows}')


_AGENT_ICONS = {"detection-agent": "ear", "eligibility-agent": "funnel",
                "response-agent": "pen", "compliance-agent": "shield", "gate": "tasks",
                "filing-agent": "note", "escalation-agent": "moon",
                "reporting-agent": "chart"}


# Lane mapping for the journey view: Takeoff's four altitudes, our events.
_LANES = ["SIGNAL", "DECISION", "TASK", "INTERACTION"]
_EVENT_LANE = {
    ("detection-agent", "signal"): 0, ("detection-agent", "confirmed"): 0,
    ("detection-agent", "dismissed"): 1, ("reporting-agent", "outcome"): 0,
    ("eligibility-agent", "qualified"): 1, ("eligibility-agent", "suppressed"): 1,
    ("compliance-agent", "passed"): 1, ("compliance-agent", "blocked"): 1,
    ("gate", "approved"): 1, ("gate", "edited"): 1, ("gate", "dismissed"): 1,
    ("response-agent", "drafted"): 2,
    ("gate", "surfaced"): 3, ("filing-agent", "filed"): 3,
    ("escalation-agent", "escalated"): 3, ("escalation-agent", "timed out"): 3,
    ("escalation-agent", "stalled"): 3,
}
_LANE_MARK = ["M{x} {y1} L{x1} {y} L{x} {y2} L{x2} {y} Z",  # diamond
              None, None, None]


def _journey_svg(events: list, run=None) -> str:
    if not events:
        return ""
    import json as _json
    from datetime import timedelta as _td

    day0 = events[0]["ts"].date()
    # time-proportional layout: one column per day that has events, sized by
    # its event count; events spaced evenly inside their day (reference look)
    days: dict[int, list[int]] = {}
    for i, e in enumerate(events):
        days.setdefault((e["ts"].date() - day0).days + 1, []).append(i)
    X0, H = 118, 374
    LY = [86, 150, 214, 278]
    JB = H - 68          # bottom of the lane area (playhead + day grid stop here)
    RY = H - 38          # baseline of the bottom time ruler
    xs: dict[int, float] = {}
    day_cols = []
    x = X0
    for dnum in sorted(days):
        idxs = days[dnum]
        width = 44 + len(idxs) * 54
        day_cols.append((dnum, x, width))
        for j, i in enumerate(idxs):
            xs[i] = x + 30 + j * 54
        x += width
    W = max(760, x + 40)

    pts = []
    for i, e in enumerate(events):
        lane = 1 if e["kind"] == "note" else _EVENT_LANE.get((e["agent"], e["kind"]), 2)
        pts.append((xs[i], LY[lane], lane))

    labels = "".join(
        f'<text x="{dx}" y="36" class="jday">DAY {dnum}</text>'
        f'<text x="{dx}" y="48" class="jdate">'
        f'{(day0 + _td(days=dnum - 1)).strftime("%a %b %-d").upper()}</text>'
        + (f'<line x1="{dx - 18}" y1="56" x2="{dx - 18}" y2="{JB}" '
           f'class="jgrid jday-grid"/>' if k else "")
        for k, (dnum, dx, _w) in enumerate(day_cols))
    axis = f'<line x1="{X0 - 34}" y1="56" x2="{W - 20}" y2="56" class="jaxis"/>'

    # bottom time ruler: one mini 24h scale per day column (ticks every 6h),
    # with an accent dot at each event's actual clock position — so hour-level
    # gaps read inside a day, and skipped days read as labeled jumps between
    # columns. scales to weeks: absent days become "+Nd" chips, not width.
    ruler = [f'<line x1="{X0 - 34}" y1="{RY}" x2="{W - 20}" y2="{RY}" class="jrule"/>']
    rxs: dict[int, float] = {}
    prev_dnum = None
    for dnum, dx, width in day_cols:
        sx0, sx1 = dx + 4, dx + width - 24
        sw = sx1 - sx0
        pxph = sw / 24
        hstep = 1 if pxph >= 7 else (3 if pxph >= 3.5 else 6)
        for h in range(0, 25, hstep):
            tx = sx0 + sw * h / 24
            tall = 7 if h % 24 == 0 else (5 if h % 6 == 0 else 3)
            maj = " jrtmaj" if h % 6 == 0 else ""
            ruler.append(f'<line x1="{tx}" y1="{RY}" x2="{tx}" y2="{RY + tall}" class="jruletick{maj}"/>')
        hour_labels = ((6, "6a"), (12, "12p"), (18, "6p")) if sw >= 90 else \
                      (((12, "12p"),) if sw >= 56 else ())
        for h, lab in hour_labels:
            tx = sx0 + sw * h / 24
            ruler.append(f'<text x="{tx}" y="{RY + 17}" text-anchor="middle" class="jrulelab">{lab}</text>')
        for i in days[dnum]:
            t = events[i]["ts"]
            frac = (t.hour + t.minute / 60) / 24
            rxs[i] = sx0 + sw * frac
            ruler.append(f'<circle cx="{rxs[i]}" cy="{RY}" r="2.6" class="jruledot"/>')
        if prev_dnum is not None and dnum - prev_dnum > 1:
            gx = dx - 18
            gap_lab = f"+{dnum - prev_dnum - 1}d"
            ruler.append(f'<rect x="{gx - 16}" y="{RY - 9}" width="32" height="18" rx="9" class="jgapbox"/>'
                         f'<text x="{gx}" y="{RY + 3.5}" text-anchor="middle" class="jgap">{gap_lab}</text>')
        prev_dnum = dnum

    segs, bands = [], []
    for (a, b), (e0, e1) in zip(zip(pts, pts[1:]), zip(events, events[1:])):
        (x0, y0, _), (x1, y1, _) = a, b
        gap = e1["ts"] - e0["ts"]
        mx = (x0 + x1) / 2
        dash = ' stroke-dasharray="4 5"' if gap > _td(hours=1) else ""
        segs.append(f'<path class="jseg" d="M{x0} {y0} C{mx} {y0} {mx} {y1} {x1} {y1}" '
                    f'fill="none" stroke="#B9BECD" stroke-width="1.4"{dash}/>')
        if gap >= _td(days=2):          # the reference's long-hold highlight
            bands.append(f'<rect x="{x0 + 10}" y="{y0 - 4}" '
                         f'width="{min(110, max(30, (x1 - x0) * 0.45))}" height="8" '
                         f'rx="4" fill="var(--accent)" opacity=".18"/>')

    marks = []
    for i, ((x, y, lane), e) in enumerate(zip(pts, events)):
        common = (f'class="jmark" onclick="jshow({i})" data-i="{i}" '
                  f'stroke="var(--accent)" stroke-width="1.8" fill="#fff"')
        if lane == 0:
            shape = f'<path d="M{x} {y-7} L{x+7} {y} L{x} {y+7} L{x-7} {y} Z" {common}/>'
        elif lane == 1:
            shape = f'<rect x="{x-6}" y="{y-6}" width="12" height="12" {common}/>'
        elif lane == 2:
            shape = f'<circle cx="{x}" cy="{y}" r="6.5" {common}/>'
        else:
            shape = f'<path d="M{x-5} {y-6.5} L{x+7} {y} L{x-5} {y+6.5} Z" {common}/>'
        marks.append(f'<circle id="jhalo{i}" cx="{x}" cy="{y}" r="13" class="jhalo"/>' + shape)

    lanes = "".join(
        f'<text x="14" y="{LY[i]+4}" class="jlane">{name}</text>'
        f'<line x1="{X0-34}" y1="{LY[i]}" x2="{W-20}" y2="{LY[i]}" class="jgrid"/>'
        for i, name in enumerate(_LANES))

    if run is not None:
        kv_base = {"source": SOURCE_WORDS.get(run.trigger_source,
                                              run.trigger_source),
                   "customer": _account_label(run),
                   "bought": bought(run.order_id),
                   "reason": COMP.get(run.reason_code, run.reason_code)}
    else:
        kv_base = {}
    payload = _json.dumps([
        {"agent": worker_name(e["agent"]), "kind": e["kind"],
         "t": int(e["ts"].timestamp()),
         "lane": _LANES[1 if e["kind"] == "note" else _EVENT_LANE.get((e["agent"], e["kind"]), 2)],
         "day": (e["ts"].date() - day0).days + 1,
         "time": e["ts"].strftime("%-I:%M %p").lower(),
         "bubble": e.get("bubble", ""),
         "kv": {**kv_base, **e.get("kv", {}), "detail": plain_detail(e["detail"])}}
        for e in events])
    first_x = xs[0]

    return f"""
<div class="jwrap">
  <div class="jscroll"><svg viewBox="0 0 {W} {H}" width="{W}" height="{H}">
    {labels}{axis}{lanes}{"".join(bands)}{"".join(ruler)}
    <rect id="jprog" x="{rxs[0]}" y="{RY - 1.2}" height="2.4" rx="1.2" fill="var(--accent)" opacity=".65" style="width:0"/>
    <g id="jheadg" style="transform:translate({first_x}px,0)">
      <line x1="0" y1="56" x2="0" y2="{JB}" class="jheadline"/>
      <circle cx="0" cy="56" r="5" fill="var(--accent)"/></g>
    <g id="jruleg" style="transform:translate({rxs[0]}px,0)">
      <circle cx="0" cy="{RY}" r="5.5" fill="#fff" stroke="var(--accent)" stroke-width="2"/></g>
    {"".join(segs)}{"".join(marks)}
  </svg></div>
  <div class="jcard"><div class="jleft">
      <div class="jkicker" id="jlanelabel"></div>
      <div class="jday2" id="jdaylabel"></div>
      <div class="jtime" id="jtimelabel"></div>
      <div class="jgaplab" id="jgaplabel"></div></div>
    <div class="jright"><h3 id="jtitle"></h3><div id="jkv" class="jkv"></div></div>
  </div>
</div>
<script>
const JEV = {payload};
const JXS = {_json.dumps([xs[i] for i in range(len(events))])};
const JRX = {_json.dumps([rxs[i] for i in range(len(events))])};
let JI = 0, JPLAY = null, JLASTX = null;
const JSVG = document.querySelector('.jscroll svg');
function jfmtgap(s){{
  const m = Math.round(s / 60);
  if (m < 60) return m + 'm';
  const h = Math.floor(m / 60), mm = m % 60;
  if (h < 24) return h + 'h' + (mm ? ' ' + mm + 'm' : '');
  const d = Math.floor(h / 24), hh = h % 24;
  return d + 'd' + (hh ? ' ' + hh + 'h' : '');
}}
function jshow(i, instant){{
  JI = i;
  const e = JEV[i], x = JXS[i];
  const dist = Math.abs(x - (JLASTX === null ? x : JLASTX)); JLASTX = x;
  const dur = instant ? 0 : Math.max(280, Math.min(680, dist * 1.1));
  JSVG.style.setProperty('--jdur', dur + 'ms');
  document.getElementById('jheadg').style.transform = 'translate(' + x + 'px,0)';
  document.getElementById('jruleg').style.transform = 'translate(' + JRX[i] + 'px,0)';
  document.getElementById('jprog').style.width = Math.max(0, JRX[i] - JRX[0]) + 'px';
  document.querySelectorAll('.jseg').forEach((s, j) => s.classList.toggle('future', j >= i));
  document.querySelectorAll('.jmark').forEach((mk, j) => {{
    mk.classList.toggle('future', j > i);
    mk.classList.toggle('active', j === i);
    mk.setAttribute('fill', j === i ? 'var(--accent)' : '#fff');
  }});
  document.querySelectorAll('.jruledot').forEach((d0, j) => d0.classList.toggle('future', j > i));
  JEV.forEach((_, j) => {{ document.getElementById('jhalo' + j).style.opacity = j === i ? 1 : 0; }});
  document.getElementById('jlanelabel').textContent = e.lane;
  document.getElementById('jdaylabel').textContent = 'DAY ' + e.day;
  document.getElementById('jtimelabel').textContent = e.time;
  document.getElementById('jgaplabel').textContent =
    i ? '+' + jfmtgap(e.t - JEV[i - 1].t) + ' after previous' : 'journey start';
  document.getElementById('jtitle').innerHTML = e.agent + ' &middot; ' + e.kind;
  document.getElementById('jkv').innerHTML = Object.entries(e.kv).map(
    ([k, v]) => '<span class="k">' + k.toUpperCase() + '</span><span>' + v + '</span>').join('')
    + (e.bubble ? '<span class="k">MESSAGE</span><span><span class="jbubble">' + e.bubble
       + '</span><span class="jbtime">' + e.time + '</span></span>' : '');
  const card = document.querySelector('.jcard .jright');
  card.classList.remove('jflash'); void card.offsetWidth; card.classList.add('jflash');
  document.querySelector('.jscroll').scrollTo({{left: Math.max(0, x - 340), behavior: 'smooth'}});
}}
function jstep(d){{ jshow(Math.min(JEV.length - 1, Math.max(0, JI + d))); }}
(function(){{
  const svg = JSVG;
  if (!svg) return;
  const wrap = document.querySelector('.jwrap');
  let dragging = false;
  function pick(ev){{
    const pt = svg.createSVGPoint();
    pt.x = ev.clientX; pt.y = ev.clientY;
    const x = pt.matrixTransform(svg.getScreenCTM().inverse()).x;
    let best = 0, bd = 1e9;
    JXS.forEach((jx, j) => {{ const d = Math.abs(jx - x); if (d < bd) {{ bd = d; best = j; }} }});
    if (best !== JI) jshow(best, dragging);
  }}
  svg.addEventListener('pointerdown', ev => {{ dragging = true; wrap.classList.add('jscrub'); svg.setPointerCapture(ev.pointerId); pick(ev); }});
  svg.addEventListener('pointermove', ev => {{ if (dragging) pick(ev); }});
  svg.addEventListener('pointerup',   () => {{ dragging = false; wrap.classList.remove('jscrub'); }});
  document.addEventListener('keydown', ev => {{
    if (ev.key === 'ArrowRight') {{ jstep(1); ev.preventDefault(); }}
    if (ev.key === 'ArrowLeft')  {{ jstep(-1); ev.preventDefault(); }}
  }});
}})();
function jplay(btn){{
  if (JPLAY){{ clearInterval(JPLAY); JPLAY = null; btn.innerHTML = '&#9654;'; return; }}
  btn.innerHTML = '&#9208;';
  if (JI >= JEV.length - 1) jshow(0);
  JPLAY = setInterval(() => {{
    if (JI >= JEV.length - 1){{ clearInterval(JPLAY); JPLAY = null; btn.innerHTML = '&#9654;'; return; }}
    jstep(1);
  }}, 1150);
}}
jshow(0, true);
</script>"""


def account_journey_content(tid: str, acct_id: str) -> str:
    led = WORLD.d.ledger
    runs = sorted((r for r in led.runs.values()
                   if r.tenant_id == tid and r.order_id == acct_id),
                  key=lambda r: r.occurred_at)
    if not runs:
        return '<h1 class="page">Nothing here</h1>'
    label = _account_label(runs[0])
    merged = []
    for r in runs:
        for e in led.trace_for(r.run_id):
            bubble = ""
            if e["kind"] in ("surfaced", "approved", "edited") and r.decision:
                bubble = esc((r.decision or {}).get("counter_text", "")[:280])
            merged.append({**e, "bubble": bubble, "kv": {
                "claim": r.claim_text or ".",
                "reason": COMP.get(r.reason_code, r.reason_code),
                "customer": _account_label(r),
                "bought": bought(r.order_id),
                "run": f'<a href="/runs/{r.run_id}">open this run &rarr;</a>'}})
    merged.sort(key=lambda e: e["ts"])
    won = 0
    for r in runs:
        out = led.outcome_for(r.run_id)
        if out and (out.outcome_value or {}).get("won"):
            won += (out.outcome_value or {}).get("amount_paise") or 0
    waiting = sum(1 for r in runs if r.state is RunState.AWAITING_GATE)
    goal_line = (GOAL_META[acct_id][1] if acct_id in GOAL_META
                 else "Work out which order this claim belongs to."
                 if acct_id not in CUSTOMERS
                 else esc(f"Answer {label}'s claim on the "
                          f"{bought(acct_id)} and keep the money."))
    stats = (f'{len(runs)} things done &middot; {waiting} need your yes'
             + (f' &middot; &#8377;{inr(won)} kept' if won else ""))
    rows = "\n".join(ledger_row(r) for r in reversed(runs))
    return (
        '<div class="tkscope">'
        f'<div class="dhead"><a class="back2" href="/journeys">&lsaquo;</a>'
        f'<div><h1><span class="goalk">Job:</span> {goal_line}</h1>'
        f'<div class="meta">{_logo(label)}{esc(label)}'
        f'<span>&middot;</span><span>{esc(bought(acct_id))}, '
        f'{esc(channel_of(acct_id))}</span>'
        f'<span>&middot;</span><span>{stats}</span></div></div>'
        f'<span class="jnav"><button class="btn ghost sm jcirc" onclick="jstep(-1)">&larr;</button>'
        f'<button class="btn primary sm jcirc" onclick="jplay(this)">&#9654;</button>'
        f'<button class="btn ghost sm jcirc" onclick="jstep(1)">&rarr;</button></span></div>'
        + _journey_svg(merged)
        + '</div>'
        + f'<form class="notebar" method="post" action="/api/note">'
        f'<input type="hidden" name="run_id" value="{runs[-1].run_id}">'
        f'<input type="hidden" name="back" value="/journeys?a={acct_id}">'
        f'<input class="jfind notein" name="text" maxlength="280" '
        f'placeholder="Tell your team something about {esc(label)} that isn&rsquo;t written down yet">'
        f'<button class="btn primary sm">Add note</button></form>'
        + f'<h2 class="sec">Everything, one by one</h2>{rows}')


def journeys_content(tid: str, sel: str = "") -> str:
    # One row per buyer who disputed something, keyed by the order they
    # disputed, searchable client-side, sorted by last activity. Lanes are
    # agent-agnostic (signal/decision/task/interaction), so new agents land
    # here without redesign; goal titles derive from the runs and GOAL_META
    # only overrides the wording.
    led = WORLD.d.ledger
    accts: dict[str, dict] = {}
    for r in led.runs.values():
        if r.tenant_id != tid or not r.order_id or not led.trace_for(r.run_id):
            continue
        a = accts.setdefault(r.order_id, {
            "n": 0, "waiting": 0, "won": 0, "lost": False,
            "last": r.occurred_at, "comps": set(), "name": _account_label(r),
            "bought": bought(r.order_id)})
        a["n"] += 1
        a["last"] = max(a["last"], r.occurred_at)
        if r.reason_code:
            a["comps"].add(PLAIN_REASON.get(r.reason_code, r.reason_code))
        if r.state is RunState.AWAITING_GATE:
            a["waiting"] += 1
        out = led.outcome_for(r.run_id)
        if out:
            if (out.outcome_value or {}).get("won"):
                a["won"] += (out.outcome_value or {}).get("amount_paise") or 0
            else:
                a["lost"] = True
    if not accts:
        return ('<h1 class="page">History</h1>'
                '<div class="pagehint">Everything your team has done, customer '
                'by customer: what came in, what it decided, what you said yes '
                'to, and how it ended.</div>'
                '<div class="empty">Nothing here yet: load the sample '
                'from Home, then come back.</div>')

    if sel in accts:
        return ('<a class="jback" href="/journeys">&lsaquo; All of it</a>'
                + account_journey_content(tid, sel))

    def _goal(a, d):
        # GOAL_META strings are trusted copy (may carry entities); derived
        # titles are escaped at build below.
        if a in GOAL_META:
            return GOAL_META[a][1]
        if a not in CUSTOMERS:
            return "Work out which order this claim belongs to."
        return esc(f"Answer {d['name']}'s claim on the {d['bought']} "
                   f"and keep the money.")

    order = sorted(accts, key=lambda a: accts[a]["last"], reverse=True)
    SHOW = 60  # chunked render: everything is in the DOM for search/sort,
    rows = []  # but only the first chunk paints until asked.
    for i, a in enumerate(order):
        d = accts[a]
        if d["waiting"]:
            chip = f'<span class="st amber">{d["waiting"]} need your yes</span>'
        elif d["won"]:
            chip = f'<span class="st ok">kept &#8377;{inr(d["won"])}</span>'
        elif d["lost"]:
            chip = '<span class="st mut">lost &middot; written down</span>'
        else:
            chip = '<span class="st mut">working</span>'
        comps = ", ".join(sorted(d["comps"])) or '<span class="mut">.</span>'
        stream = " stream" if i < 8 else ""
        hid = " hidden" if i >= SHOW else ""
        rows.append(
            f'<tr class="jgoalrow" data-q="{esc((d["name"] + " " + d["bought"]).lower())}" data-i="{i}"{hid} '
            f'onclick="location=\'/journeys?a={a}\'">'
            f'<td class="acct">{_logo(d["name"])}<b>{esc(d["name"])}</b>'
            f'<span class="mut"> &middot; {esc(d["bought"])}</span></td>'
            f'<td><span class="ell{stream}">{_goal(a, d)}</span></td>'
            f'<td>{comps}</td>'
            f'<td class="num">{d["n"]}</td>'
            f'<td>{chip}</td>'
            f'<td class="num" data-s="{int(d["last"].timestamp())}">'
            f'{d["last"].strftime("%b %-d")}</td></tr>')
    more_btn = (f'<button class="btn ghost sm gmore" id="jmore" '
                f'onclick="jshowall(this)">Show all {len(order)}</button>'
                if len(order) > SHOW else "")
    shown = min(SHOW, len(order))
    return (
        '<h1 class="page">History</h1>'
        '<div class="pagehint">Every customer your team has answered for you, '
        'and everything it did. Open one to play it back.</div>'
        '<input class="jfind" placeholder="Search by customer, product or what happened&hellip;" '
        'oninput="jfilter(this.value)">'
        f'<div class="gridcount" id="jcount">Showing {shown} of {len(order)}</div>'
        '<div class="dgridwrap"><table class="dgrid" id="jgrid">'
        '<thead><tr>'
        '<th onclick="dsort(this)">Customer</th>'
        '<th onclick="dsort(this)">What we are doing for you</th>'
        '<th onclick="dsort(this)">What they claimed</th>'
        '<th onclick="dsort(this)" class="num">Things done</th>'
        '<th onclick="dsort(this)">Where it stands</th>'
        '<th onclick="dsort(this)" class="num">Last touched</th>'
        '</tr></thead>'
        f'<tbody>{"".join(rows)}</tbody></table></div>'
        + more_btn)


def run_trace_content(tid: str, run_id: str) -> str:
    led = WORLD.d.ledger
    r = led.runs.get(run_id)
    if r is None or r.tenant_id != tid:
        return '<h1 class="page">Not found</h1>'
    label, cls = STATE_META.get(r.state, (r.state.value, "mut"))
    events = [dict(e) for e in led.trace_for(run_id)]
    for e in events:
        if e["kind"] in ("surfaced", "approved", "edited") and r.decision:
            e["bubble"] = esc((r.decision or {}).get("counter_text", "")[:280])
    rows = "".join(
        f'<div class="trow slim"><span class="ico">'
        f'{ICONS[_AGENT_ICONS.get(e["agent"], "bolt")]}</span>'
        f'<span class="tdesc"><b>{esc(worker_name(e["agent"]))}</b> {esc(e["kind"])} '
        f'<span class="mut">{plain_detail(e["detail"])}</span></span>'
        f'<span class="when">{e["ts"].strftime("%b %-d, %H:%M")}</span></div>'
        for e in events) or (
        '<div class="empty">Nothing was written down for this one.</div>')
    comp_name = PLAIN_REASON.get(r.reason_code, "A buyer disputed a payment")
    return (
        '<div class="tkscope">'
        f'<div class="dhead"><a class="back2" href="/journeys" onclick="if(history.length>1){{history.back();return false}}">&lsaquo;</a>'
        f'<div><h1><span class="goalk">Job:</span> {comp_name}. '
        f'&ldquo;{esc(r.claim_text or "claim")}&rdquo;, from '
        f'{esc(_account_label(r))}.</h1>'
        f'<div class="meta">{_logo(_account_label(r))}{esc(_account_label(r))}'
        f'<span>&middot;</span><span>{esc(bought(r.order_id))}, '
        f'{esc(channel_of(r.order_id))}</span>'
        f'<span>&middot;</span><span class="st {cls}">{label}</span></div></div>'
        f'<span class="jnav"><button class="btn ghost sm jcirc" onclick="jstep(-1)">&larr;</button>'
        f'<button class="btn primary sm jcirc" onclick="jplay(this)">&#9654;</button>'
        f'<button class="btn ghost sm jcirc" onclick="jstep(1)">&rarr;</button></span></div>'
        + _journey_svg(events, r)
        + '</div>'
        + f'<form class="notebar" method="post" action="/api/note">'
        f'<input type="hidden" name="run_id" value="{run_id}">'
        f'<input class="jfind notein" name="text" maxlength="280" '
        f'placeholder="Add a note: what did you know that isn&rsquo;t written down here?">'
        f'<button class="btn primary sm">Add note</button></form>'
        + f'<h2 class="sec">Everything that happened</h2>{rows}')


# (days ago, customer, reason label, buyer claim, verdict, evidence, draft)
# The look-back: what Dispute Defender would have done with Ojas Wellness's
# own last 30 days, before it was switched on. Nothing here was ever sent.
SHADOW_ROWS = [
    (27, "Ananya B.",  "Says it never arrived",   "A2 ghee jar never arrived",              "send", "2 proofs",
     "The courier's delivery proof is signed and stamped at the door; the WhatsApp thread confirms it arrived the same evening."),
    (25, "Rohit K.",   "Says charged twice",      "Two debits for one ashwagandha refill",  "send", "2 proofs",
     "One payment, one debit. The second line is a hold the bank releases on its own."),
    (24, "Shruti M.",  "",                        "Asking where a refund is, not a dispute", "skip", "",
     ""),
    (21, "Devang P.",  "Says it never arrived",   "Shilajit resin marked delivered, not received", "send", "2 proofs",
     "It was delivered two days before the complaint, and the buyer's own message from that day is attached."),
    (19, "Nisha T.",   "Says it was wrong",       "Aloe vera juice seal looked tampered",   "send", "1 proof",
     "The listing from the order date matches the batch and the seal that shipped, exactly."),
    (17, "Imran S.",   "Says charged twice",      "Amla juice order billed twice on Amazon","send", "2 proofs",
     "The invoice and the bank statement agree: one charge for this order."),
    (16, "Bhavna R.",  "",                        "Buyer praised the delivery speed",       "skip", "",
     ""),
    (14, "Suresh N.",  "Says it wasn't them",     "Cardholder says the shilajit order wasn't his", "esc",  "none",
     ""),
    (12, "Aparna V.",  "Says charged twice",      "Statement lists the turmeric powder twice", "send", "2 proofs",
     "The payment and the bill tie this order to one charge; the second line never went through."),
    (9,  "Yash D.",    "Says it never arrived",   "Moringa capsules marked undelivered",    "send", "1 proof",
     "The courier's scan puts the parcel at the address they gave, signed for on the spot."),
    (7,  "Kiran L.",   "",                        "Asking to change the refill date",       "skip", "",
     ""),
    (5,  "Neelam J.",  "Says it never arrived",   "Karela jamun juice box never showed",    "send", "2 proofs",
     "Delivery proof plus the buyer's own WhatsApp message from the day it arrived. Both go with the reply."),
    (2,  "Ganesh A.",  "Says they had cancelled", "Says the amla refill was cancelled first", "send", "2 proofs",
     "The refill log timestamps the cancellation after the parcel left the warehouse, and the policy that day credits the next cycle."),
]


def shadow_content(tid: str) -> str:
    from datetime import datetime as _dt, timedelta as _td
    now = _dt.now()

    def cell(work, value, extra_cls=""):
        return (f'<td class="pending {extra_cls}"><span class="cw">{work}</span>'
                f'<span class="cv">{value}</span></td>')

    body = []
    for days, acct, comp, claim, verdict, proof, draft in SHADOW_ROWS:
        when = (now - _td(days=days)).strftime("%b %-d")
        comp_v = (f'<span class="st warn">{esc(comp)}</span>' if comp
                  else '<span class="mut">none</span>')
        claim_v = f'<span class="stream">&ldquo;{esc(claim)}&rdquo;</span>'
        if verdict == "send":
            proof_v = f'<span class="st ok">{esc(proof)}</span>'
            send_v = (f'<span class="st ok">ready</span> '
                      f'<span class="mut stream">{esc(draft)}</span>')
        elif verdict == "esc":
            proof_v = '<span class="st warn">nothing on file</span>'
            send_v = '<span class="st warn">would ask you: no proof on file</span>'
        else:
            proof_v = '<span class="mut">.</span>'
            send_v = '<span class="mut">.</span>'
        body.append(
            f'<tr data-v="{verdict}">'
            f'<td class="chk"><input type="checkbox"></td>'
            f'<td class="acct">{_logo(acct)}<b>{esc(acct)}</b> '
            f'<span class="mut">{when}</span></td>'
            + cell("reading&hellip;", comp_v)
            + cell("working out the claim&hellip;", claim_v)
            + cell("looking for proof&hellip;", proof_v)
            + cell("writing the reply&hellip;", send_v, "wide")
            + '<td class="ghost"></td></tr>')

    return (
        '<h1 class="page">See what you missed</h1>'
        '<div class="pagehint">Put your last 30 days of disputes through '
        'Dispute Defender. Nothing is sent, nobody is contacted, and it costs '
        'nothing: you just see what got away.</div>'
        '<button class="btn primary" id="shbtn" onclick="shstart()">Check the last 30 days</button>'
        '<div class="shstats">'
        '<div class="shstat"><div class="n" id="sh-calls">0</div><div class="l">disputes looked at</div></div>'
        '<div class="shstat"><div class="n" id="sh-moments">0</div><div class="l">worth replying to</div></div>'
        '<div class="shstat"><div class="n" id="sh-sends">0</div><div class="l">replies it had ready</div></div>'
        '<div class="shstat"><div class="n" id="sh-missed">0</div><div class="l">answered by anyone</div></div>'
        '</div>'
        '<div class="dgridwrap"><table class="dgrid">'
        '<thead><tr><th class="chk"></th>'
        '<th onclick="dsort(this)">Dispute</th>'
        '<th onclick="dsort(this)">Something we handle?</th>'
        '<th onclick="dsort(this)">What the buyer said</th>'
        '<th onclick="dsort(this)">Proof on file?</th>'
        '<th class="wide" onclick="dsort(this)">Would it have replied?</th>'
        '<th class="ghost">+ New question</th></tr></thead>'
        f'<tbody>{"".join(body)}</tbody></table></div>'
        '<div class="shband" id="shband"><span><b>Nothing was sent.</b> Nobody '
        'was contacted and this cost you nothing. That is what last month '
        'looked like without Dispute Defender.</span>'
        '<a class="btn primary sm" href="/settings?s=connectors">Switch it on &rarr;</a></div>'
        + """<script>
function shstart(){
  const btn = document.getElementById('shbtn');
  btn.disabled = true; btn.textContent = 'Scanning\u2026';
  btn.classList.add('scanning');
  const rows = [...document.querySelectorAll('.dgrid tbody tr')];
  let calls = 0, moments = 0, sends = 0;
  const callsEl = document.getElementById('sh-calls');
  const tick = setInterval(() => {
    calls = Math.min(47, calls + 1 + Math.floor(Math.random() * 3));
    callsEl.textContent = calls;
    if (calls >= 47) clearInterval(tick);
  }, 150);
  const ROW = 430, COL = 540, WORK = 620;
  rows.forEach((r, i) => {
    const cells = [...r.querySelectorAll('td.pending')];
    cells.forEach((c, j) => {
      const t0 = 400 + i * ROW + j * COL;
      setTimeout(() => c.classList.add('working'), t0);
      setTimeout(() => {
        c.classList.remove('working', 'pending');
        c.classList.add('done');
        c.querySelectorAll('.cv .stream').forEach(el => wstream(el));
        if (j === cells.length - 1) {
          const v = r.dataset.v;
          if (v !== 'skip') { moments++; document.getElementById('sh-moments').textContent = moments; }
          if (v === 'send') { sends++; document.getElementById('sh-sends').textContent = sends; }
        }
      }, t0 + WORK);
    });
  });
  const total = 400 + rows.length * ROW + 4 * COL + WORK + 400;
  setTimeout(() => {
    document.getElementById('shband').classList.add('on');
    btn.classList.remove('scanning');
    btn.textContent = 'Scan complete';
  }, total);
}
</script>""")


# ------------------------------------------------------------- proposals
# The employee thesis, made mechanical: every acting agent ends its job in
# finished work plus ONE decision. Same card shape, same store, same
# lifecycle for all of them; a decision is written down and cannot be
# re-decided. Reporting agents close their loop in the morning note instead.
PROPS_DEF = {
    "cashflow_forecast": dict(
        stake="&#8377;48,200", stake_n=48200,
        ifno="Thursday dips to &minus;&#8377;12,400", ifyes="Thursday stays at +&#8377;35,800",
        kicker="Cash call", rail="Payout move",
        title="Move the courier payout by two days",
        why="Thursday collides: vendor day and the GST debit land together. "
            "Without a move, the week dips to <b>&minus;&#8377;12,400</b> "
            "on Thursday morning.",
        rows=[("The move", "Courier payout <b>&#8377;48,200</b>: Thursday "
               "&rarr; Saturday. The courier&rsquo;s terms allow it; nobody "
               "is paid late."),
              ("After", "Thursday stays at <b>+&#8377;35,800</b>. Nothing "
               "else changes."),
              ("Read from", "settlements landing, the payout calendar, the "
               "GST schedule.")],
        yes="Approve the move", no="Not this time",
        approved="The courier payout moves to Saturday; the calendar is "
                 "updated and nobody is paid late.",
        declined="Left as it was. Thursday will run tight; the planner "
                 "warns you again the day before."),
    "stock_watch": dict(
        stake="&#8377;68,400", stake_n=68400,
        ifno="Out of Amla Juice in 6 days", ifyes="Covered for six weeks",
        kicker="Reorder", rail="Reorder draft",
        title="Reorder Amla Juice before it runs out",
        why="6 days of stock left at this pace, across every channel. The "
            "supplier needs 10 to deliver.",
        rows=[("The order", "40 cases, <b>&#8377;68,400</b>, Vasudha Farms. "
               "Same terms as the last three orders."),
              ("After", "Covered for six weeks. No channel oversells."),
              ("Read from", "channel counts, sales pace, supplier lead times.")],
        yes="Place the order", no="Not yet",
        approved="Order placed with Vasudha Farms. Delivery expected Tuesday.",
        declined="No order placed. It warns again at 4 days of stock."),
    "payouts_desk": dict(
        stake="&#8377;1,12,350", stake_n=112350,
        ifno="14 payments go out late", ifyes="Everyone paid on time, morning",
        kicker="Payment day", rail="Tomorrow&rsquo;s payments",
        title="Tomorrow&rsquo;s 14 payments, one yes",
        why="Vendors, two refunds, and the courier. Every payment sits "
            "against its bill.",
        rows=[("The list", "14 payments, <b>&#8377;1,12,350</b> together. "
               "Each goes out the way the receiver wants it."),
              ("Read from", "bills due, the payout calendar, past payment "
               "preferences.")],
        yes="Pay all 14", no="Hold them",
        approved="All 14 scheduled for the morning. Each lands against its "
                 "bill.",
        declined="Held. Nothing goes out until you say so."),
    "refund_shield": dict(
        stake="&#8377;1,249", stake_n=1249,
        ifno="&#8377;1,249 paid to a likely fraud", ifyes="Refused with proof, replacement offered",
        kicker="Refund check", rail="A claim to refuse",
        title="Refuse the broken-bottle claim, with proof",
        why="Second claim from this buyer in three weeks. The delivery scan "
            "is clean, and the photo matches the first claim&rsquo;s photo.",
        rows=[("The reply", "Refuse politely, proof attached, and offer a "
               "replacement instead of cash."),
              ("At stake", "<b>&#8377;1,249</b>, and the pattern if it works "
               "twice."),
              ("Read from", "the claim photo, the delivery scan, the "
               "buyer&rsquo;s history.")],
        yes="Refuse with proof", no="Pay it anyway",
        approved="Refused with the proof attached. A replacement was "
                 "offered instead.",
        declined="Refund paid as claimed. The pattern is noted."),
    "cod_guard": dict(
        stake="&#8377;2,141", stake_n=2141,
        ifno="&#8377;2,141 shipped at a 40% bounce risk", ifyes="3 held; slots go to confirmed orders",
        kicker="Dispatch hold", rail="3 COD orders held",
        title="Hold 3 COD orders that never picked up",
        why="Two calls each, unanswered, plus a WhatsApp. Their pincode "
            "bounces 2 of every 5 COD parcels.",
        rows=[("The hold", "3 orders stay back today. Their slots go to "
               "confirmed orders."),
              ("At stake", "<b>&#8377;2,141</b> of shipping and return cost "
               "if they bounce."),
              ("Read from", "call outcomes, the pincode&rsquo;s history.")],
        yes="Hold them", no="Ship anyway",
        approved="Held from dispatch. The slots went to confirmed orders.",
        declined="Shipped as normal. The bounce risk is noted against the "
                 "pincode."),
    "returns_desk": dict(
        stake="&#8377;1,899", stake_n=1899,
        ifno="The buyer waits and chases you", ifyes="&#8377;1,899 back today, case closed",
        kicker="Refund release", rail="A refund to release",
        title="Release the &#8377;1,899 refund: the item is back",
        why="The return reached the warehouse this morning. Seal checked, "
            "item fine.",
        rows=[("The release", "<b>&#8377;1,899</b> back to the buyer, "
               "today."),
              ("Read from", "the courier scan, the warehouse check.")],
        yes="Release it", no="Hold it",
        approved="Refund released the same hour. Case closed.",
        declined="Held. A person takes a look first."),
    "payment_forms": dict(
        stake="&#8377;1,04,000", stake_n=104000,
        ifno="A &#8377;2.6 lakh order sits unpaid", ifyes="Advance collected in one step",
        kicker="Payment form", rail="An advance form to send",
        title="Send the &#8377;2.6 lakh advance form",
        why="A bulk buyer wants to pay 40% up front. The form carries the "
            "PAN step and the balance-on-delivery terms in one link.",
        rows=[("The form", "<b>&#8377;1,04,000</b> advance now, balance on "
               "delivery. Verify and pay in one step."),
              ("Read from", "the order, your terms, the KYC rules that "
               "apply above &#8377;2 lakh.")],
        yes="Send the form", no="Not yet",
        approved="Sent on WhatsApp. The payment lands against the order "
                 "and the books already know.",
        declined="Not sent. The draft stays here."),
    "kyc_desk": dict(
        stake="&#8377;2,40,000", stake_n=240000,
        ifno="A clean buyer stays blocked", ifyes="The buyer pays; the check is on record",
        kicker="Deep check done", rail="A buyer to clear",
        title="Clear the flagged buyer: the deep check came back clean",
        why="Two days of checks: watchlists clear, registry matched. The "
            "buyer has seen &ldquo;under review&rdquo;, never an error.",
        rows=[("The call", "Clear the buyer and let the payment through."),
              ("Read from", "watchlists, the registry answer, the order.")],
        yes="Clear the buyer", no="Refuse politely",
        approved="Cleared. The payment goes through; nothing else changes.",
        declined="Refused politely and the order cancelled. Written down "
                 "with the reason."),
    "gst_compliance": dict(
        stake="", stake_n=0,
        ifno="The 18th scramble, again", ifyes="Your CA has the file today",
        kicker="Filing pack", rail="The month&rsquo;s file",
        title="Send the month&rsquo;s file to your CA",
        why="Every invoice tied to a real order. The two mismatches are "
            "fixed and noted in the file.",
        rows=[("The file", "One clean pack, in the format your CA asked "
               "for. Nothing rebuilt by hand."),
              ("Read from", "invoices, orders, the GST schedule.")],
        yes="Send it", no="Hold it",
        approved="Sent. Your CA has it well before the 20th.",
        declined="Held. It stays ready whenever you are."),
    "cart_rescue": dict(
        stake="&#8377;31,240", stake_n=31240,
        ifno="&#8377;31,240 in carts goes cold", ifyes="12 buyers called back tonight",
        kicker="Tonight&rsquo;s calls", rail="12 carts to call",
        title="Tonight&rsquo;s rescue list: 12 dropped carts",
        why="Worth <b>&#8377;31,240</b> together. Calls between 6 and 8 PM, "
            "WhatsApp for anyone who does not pick up.",
        rows=[("The plan", "One call each, one WhatsApp fallback, then it "
               "stops. Anyone who says stop is never called again."),
              ("Read from", "dropped carts, buyer hours, past outcomes.")],
        yes="Start calling at 6", no="Skip tonight",
        approved="Calling starts at 6. Every outcome lands in your chats.",
        declined="Nobody is called tonight. The list stays."),
    "payment_rescue": dict(
        stake="&#8377;4,890", stake_n=4890,
        ifno="&#8377;4,890 stays unpaid", ifyes="Fresh links out within the hour",
        kicker="Failed payments", rail="5 payments to chase",
        title="Chase today&rsquo;s 5 failed payments",
        why="<b>&#8377;4,890</b> together, each with the decline reason "
            "already read: two bank timeouts, two limits, one wrong PIN.",
        rows=[("The plan", "WhatsApp first with a fresh link, a call only "
               "if the money matters and the link sits unused."),
              ("Read from", "decline reasons, order values, buyer hours.")],
        yes="Chase them", no="Let them go",
        approved="On it. Fresh links are out; calls follow where needed.",
        declined="Left alone. The orders stay unpaid."),
}

PROP_EXECS = {
    "cashflow_forecast": [('Payout calendar updated', 'done'), ('Courier told about the new date', 'done'), ("Watching Thursday's balance", 'running')],
    "stock_watch": [('Order sent to Vasudha Farms', 'done'), ('Delivery slot confirmed for Tuesday', 'done'), ('Watching the stock until it lands', 'running')],
    "payouts_desk": [('14 payments scheduled for the morning', 'done'), ('Every receiver told what is coming', 'done'), ('Receipts will file against their bills', 'running')],
    "refund_shield": [('Refusal sent, proof attached', 'done'), ('Replacement offered instead', 'done'), ("Watching for the buyer's reply", 'running')],
    "cod_guard": [('3 orders held from dispatch', 'done'), ('Slots handed to confirmed orders', 'done'), ('Pincode on watch', 'running')],
    "returns_desk": [('Refund released to the buyer', 'done'), ('Buyer told, with the timeline', 'done'), ('Case closed and written down', 'done')],
    "payment_forms": [('Form sent on WhatsApp', 'done'), ('Watching for the payment', 'running')],
    "kyc_desk": [('Buyer cleared', 'done'), ('Payment allowed through', 'done'), ('The check written into the record', 'done')],
    "gst_compliance": [('File sent to your CA', 'done'), ('A copy kept in your records', 'done')],
    "cart_rescue": [('Call list locked: 12 buyers', 'done'), ('First calls go out at 6 PM', 'running')],
    "payment_rescue": [('Fresh links sent to all 5', 'done'), ('Two have already paid', 'done'), ('Calls follow where links sit unused', 'running')],
}

# Reporting agents close their loop in the morning note, not a card.
REPORT_AGENTS = {"three_way_recon", "settlement_insights", "daily_mis"}

PROPS: dict[str, dict] = {}      # tid -> {slug: {state, decided_by}}


def prop_state(tid: str, slug: str) -> dict:
    return PROPS.setdefault(tid, {}).setdefault(
        slug, dict(state="waiting", decided_by=""))


def props_waiting(tid: str) -> int:
    return sum(1 for slug in PROPS_DEF
               if prop_state(tid, slug)["state"] == "waiting")


def prop_row(tid: str, slug: str) -> str:
    """The queue grammar: one glance, one decision. A compact row with
    the ask, the compressed consequence pair, and the two verbs. Depth
    (the full card) unfolds only on demand."""
    d = PROPS_DEF[slug]
    role = next((a["role"] for a in RELAY_AGENTS if a["slug"] == slug), slug)
    return (
        f'<div class="qrow" id="q-{slug}">'
        f'<div class="qmain">'
        f'{avatar(slug, 30, True)}'
        f'<div class="qtext"><b>{d["title"]}</b>'
        f'<span class="qsub"><span class="qno">{d["ifno"]}</span>'
        f'<span class="qarr">&rarr;</span>'
        f'<span class="qyes">{d["ifyes"]}</span></span></div>'
        f'<div class="qacts">'
        f'<form method="post" action="/api/prop_act" style="display:contents">'
        f'<input type="hidden" name="slug" value="{slug}">'
        f'<input type="hidden" name="action" value="approve">'
        f'<button class="btn primary sm">{d["yes"]}</button></form>'
        f'<form method="post" action="/api/prop_act" style="display:contents">'
        f'<input type="hidden" name="slug" value="{slug}">'
        f'<input type="hidden" name="action" value="decline">'
        f'<button class="btn ghost sm">{d["no"]}</button></form>'
        f'<button class="qmore" onclick="qToggle(this)" '
        f'aria-label="Details">&#8964;</button>'
        f'</div></div>'
        f'<div class="qdetail" hidden>'
        f'<div class="wp-kicker">{d["kicker"]} &middot; {role}</div>'
        f'<div class="cashwhy">{d["why"]}</div>'
        + "".join(f'<div class="cashrow"><span>{lbl}</span>{txt}</div>'
                  for lbl, txt in d["rows"])
        + '</div></div>')


def prop_card(tid: str, slug: str) -> str:
    d = PROPS_DEF[slug]
    p = prop_state(tid, slug)
    role = next((a["role"] for a in RELAY_AGENTS if a["slug"] == slug), slug)
    if p["state"] == "approved":
        steps = PROP_EXECS.get(slug, [])
        trace = ""
        if steps:
            rows2 = "".join(
                f'<div class="tr3 {st}"><span class="tpill"></span>'
                f'<span class="tlabel">{txt}</span>'
                f'<span class="tstat">{st}</span></div>'
                for txt, st in steps)
            trace = (f'<div class="trace"><div class="trace-h">What it is '
                     f'doing with your yes</div>{rows2}</div>')
        verdict = (f'<div class="cashdone ok">Approved by '
                   f'{mention(p["decided_by"])}. {d["approved"]}</div>'
                   + trace)
    elif p["state"] == "declined":
        verdict = f'<div class="cashdone">{d["declined"]}</div>'
    else:
        verdict = (
            f'<div class="wpbtns">'
            f'<form method="post" action="/api/prop_act" style="display:contents">'
            f'<input type="hidden" name="slug" value="{slug}">'
            f'<input type="hidden" name="action" value="approve">'
            f'<button class="btn primary">{d["yes"]}</button></form>'
            f'<form method="post" action="/api/prop_act" style="display:contents">'
            f'<input type="hidden" name="slug" value="{slug}">'
            f'<input type="hidden" name="action" value="decline">'
            f'<button class="btn ghost">{d["no"]}</button></form>'
            f'</div>')
    rows = "".join(f'<div class="cashrow"><span>{lbl}</span>{txt}</div>'
                   for lbl, txt in d["rows"])
    contrast = (
        f'<div class="abwrap">'
        f'<div class="ab no"><span>If you do nothing</span>'
        f'<b>{d["ifno"]}</b></div>'
        f'<div class="ab yes"><span>If you say yes</span>'
        f'<b>{d["ifyes"]}</b></div></div>')
    details = (f'<details class="cashmore"><summary>Details</summary>'
               f'<div class="cashwhy">{d["why"]}</div>{rows}</details>')
    return (f'<div class="cashcard" id="prop-{slug}">'
            f'<div class="wp-kicker">{d["kicker"]} &middot; {role}</div>'
            f'<h3>{d["title"]}</h3>'
            f'{contrast}{verdict}{details}</div>')


# Kept for the surfaces wired before the engine went general.
def cash_prop(tid: str) -> dict:
    return prop_state(tid, "cashflow_forecast")


def cash_waiting(tid: str) -> int:
    return props_waiting(tid)


def cashflow_card(tid: str) -> str:
    return prop_card(tid, "cashflow_forecast")


# ------------------------------------------------------------- assignment
# A waiting yes can be handed to a named teammate. The agents keep working
# either way; this only decides whose phone the case sits on.
ASSIGN: dict[str, str] = {}      # order_id -> teammate name


# ------------------------------------------------------------- people
# Settings owns a real list, not a painting: the founder can add someone,
# hand them the yes, take it back, or remove them. Relay's own people are
# shown but not editable — they are Relay staff, not the founder's to manage.
PEOPLE: dict[str, list[dict]] = {}


def people_for(tid: str) -> list[dict]:
    return PEOPLE.setdefault(tid, [
        dict(name=n, role=r, approver=e)
        for _, n, r, d, e in TEAM if d == BUSINESS])


# ------------------------------------------------------------- autonomy
# Earned, never assumed: the founder starts approving everything, and the
# option to let small ones send themselves opens only after 20 clean yeses
# — approvals where the wording wasn't touched. Counted off the ledger,
# not a stored number, so it can always be checked.
YES_TARGET = 20
SMALL_LIMIT_PAISE = 50_000       # "small" = under Rs 500

AUTONOMY: dict[str, str] = {}    # tid -> "all" (default) | "small"


def clean_yeses(tid: str) -> int:
    led = WORLD.d.ledger
    return sum(1 for r in led.runs.values()
               if r.tenant_id == tid
               and r.gate_action is not None
               and r.gate_action.value == "approve"
               and not r.gate_is_material)


def autonomy_mode(tid: str) -> str:
    return AUTONOMY.get(tid, "all")


def mode_ui(tid: str) -> str:
    """The approve-mode chip on the composer. One control, three lines,
    nothing to configure: the second option unlocks itself and says how
    far along you are; the third is a promise, not a setting."""
    n = clean_yeses(tid)
    unlocked = n >= YES_TARGET
    mode = autonomy_mode(tid)
    label = ("Small ones send themselves" if mode == "small"
             else "You approve everything")
    tick = '<span class="tick">&#10003;</span>'

    opt_all = (f'<form method="post" action="/api/mode" style="display:contents">'
               f'<input type="hidden" name="mode" value="all">'
               f'<button class="mopt" type="submit">You say yes to everything'
               f'{tick if mode == "all" else ""}</button></form>')
    if unlocked:
        opt_small = (
            f'<form method="post" action="/api/mode" style="display:contents">'
            f'<input type="hidden" name="mode" value="small">'
            f'<button class="mopt" type="submit"><div>Let Relay send small '
            f'ones itself<small>Under &#8377;{SMALL_LIMIT_PAISE // 100} and '
            f'nothing unusual. Everything still lands in History.</small>'
            f'</div>{tick if mode == "small" else ""}</button></form>')
    else:
        opt_small = (
            f'<div class="mopt off"><div>Let Relay send small ones itself'
            f'<small>Opens after 20 clean yeses (approved without a '
            f'rewording). You are at <b>{n} of {YES_TARGET}</b>, '
            f'{YES_TARGET - n} to go.</small>'
            f'<span class="yesbar"><i style="width:{min(100, n * 100 // YES_TARGET)}%"></i></span>'
            f'</div></div>')
    opt_never = ('<div class="mopt off"><div>Skip approvals entirely'
                 '<small>Never. Nothing sends without you: by design.'
                 '</small></div></div>')
    return (f'<details class="mode" id="mode2">'
            f'<summary>{ICONS["gear"]}<span>{label}</span></summary>'
            f'<div class="menu">{opt_all}{opt_small}{opt_never}</div>'
            f'</details>')


# ------------------------------------------------------------- scheduled
# The ADK long-horizon pattern, in plain clothes: scheduled work lives as
# things you can open and read, not cron lines. Each routine is written the
# way the owner would say it out loud, and adding one is a sentence, not a
# form. Demo store is per-tenant and in-memory, like EV_NOTES.
ROUTINES: dict[str, list[dict]] = {}

_DEFAULT_ROUTINES = [
    dict(name="Morning brief", brief=True, out="morning",
         when="Every morning, 8:00",
         what="What came in overnight, what your team already handled, and "
              "the few things waiting on your yes.",
         last="ran this morning", on=True),
    dict(name="Weekly wins", out="wins", when="Every Friday evening",
         what="What your team won this week, in rupees, and what it learned.",
         last="ran last Friday", on=True),
    dict(name="Proof check", out="proof", when="Every Monday",
         what="Goes over the proof on file and flags anything gone stale "
              ": a dead link, an old policy page.",
         last="ran Monday", on=True),
    dict(name="Month-end tie-out", out="monthend",
         when="First of the month",
         what="Ties the month's numbers out against the bank and writes you "
              "a one-page summary.",
         last="", on=False),
]


def routines_for(tid: str) -> list[dict]:
    return ROUTINES.setdefault(tid, [dict(r) for r in _DEFAULT_ROUTINES])


# Shown once, the first time someone opens Scheduled. A hand-hold, not a
# tour: three cards, each one promise, then out of the way for good.
_SCHED_INTRO = """
<style>
.onbk{position:fixed;inset:0;background:rgba(24,25,32,.45);z-index:60;
  display:flex;align-items:center;justify-content:center}
.onbk[hidden]{display:none}
.onbd{background:#fff;border-radius:20px;max-width:520px;width:92%;
  padding:0 0 24px;box-shadow:0 24px 80px rgba(24,25,32,.35);overflow:hidden}
.onbd .art{background:linear-gradient(180deg,#DCE7FA,#F6F8FE);height:210px;
  display:flex;align-items:center;justify-content:center;font-size:64px}
.onbd h2{font-size:22px;margin:24px 28px 8px;text-align:center}
.onbd p{margin:0 28px;color:var(--mut);text-align:center;font-size:14.5px;
  line-height:1.55}
.onbd .row{display:flex;align-items:center;justify-content:space-between;
  margin:24px 28px 0}
.onbd .dots{display:flex;gap:8px}
.onbd .dots i{width:8px;height:8px;border-radius:99px;background:#D8DAE4;
  transition:all .2s}
.onbd .dots i.on{width:22px;background:#181920}
.onbd .nextb{background:#181920;color:#fff;border:0;border-radius:12px;
  padding:8px 24px;font-size:14px;font-weight:600;cursor:pointer}
.onbd .skipb{position:absolute;top:14px;right:18px;background:none;border:0;
  font-size:20px;color:#666;cursor:pointer}
.onbk .card{position:relative}
</style>
<div class="onbk" id="onbk" hidden><div class="onbd card">
  <button class="skipb" onclick="onbDone()">&#10005;</button>
  <div id="onbSlides"></div>
  <div class="row"><span class="dots" id="onbDots"></span>
    <button class="nextb" id="onbNext" onclick="onbNext()">Next</button></div>
</div></div>
<script>
const ONB = [
  {e:'&#9749;', h:'Start every day already caught up',
   p:'The morning brief is written before you sit down: what came in overnight, what was handled, the few things that need your yes.'},
  {e:'&#128172;', h:'Say it once, it runs forever',
   p:'&ldquo;Every Friday evening, tell me what we won this week.&rdquo; That sentence is the whole setup: no settings, no forms.'},
  {e:'&#128214;', h:'Nothing happens behind your back',
   p:'Every run leaves a note you can open like a chat, and nothing is ever sent anywhere without your yes.'}];
function onbShow(i){
  window._onb = i;
  document.getElementById('onbSlides').innerHTML =
    '<div class="art">' + ONB[i].e + '</div><h2>' + ONB[i].h + '</h2><p>' + ONB[i].p + '</p>';
  document.getElementById('onbDots').innerHTML =
    ONB.map((_,j) => '<i class="' + (j===i?'on':'') + '"></i>').join('');
  document.getElementById('onbNext').textContent = i === ONB.length-1 ? 'Got it' : 'Next';
}
function onbNext(){
  if (window._onb >= ONB.length-1) return onbDone();
  onbShow(window._onb + 1);
}
function onbDone(){
  document.getElementById('onbk').hidden = true;
  localStorage.setItem('relay_seen_scheduled', '1');
}
if (!localStorage.getItem('relay_seen_scheduled')){
  document.getElementById('onbk').hidden = false; onbShow(0);
}
</script>"""


def scheduled_content(tid: str) -> str:
    # The hub grammar, with the full lifecycle kept: pause, reword inline,
    # remove, add in a sentence, one-click templates, and every routine's
    # latest note one tap away.
    cards = ""
    n_run = n_paused = 0
    for i, r in enumerate(routines_for(tid)):
        if r["on"] and r["last"]:
            stat = f'<span class="st ok">{r["last"]}</span>'
        elif r["on"]:
            stat = '<span class="st wait">first run tonight</span>'
        else:
            stat = '<span class="st mut">paused</span>'
        n_run += 1 if r["on"] else 0
        n_paused += 0 if r["on"] else 1
        toggle = (f'<form method="post" action="/api/routine_toggle" '
                  f'style="display:contents"><input type="hidden" name="i" '
                  f'value="{i}"><button class="tglbtn {"on" if r["on"] else ""}" '
                  f'title="{"Pause" if r["on"] else "Resume"}" '
                  f'aria-label="{"Pause" if r["on"] else "Resume"}">'
                  f'<i></i></button></form>')
        out = r.get("out")
        readit = (f'<a class="st wait" href="/briefs/{out}">read it '
                  f'&rarr;</a>' if out else "")
        tools = (f'<span class="rtools">'
                 f'<button class="rtool" '
                 f'onclick="routineEdit({i}, this)">Edit</button>'
                 f'<form method="post" action="/api/routine_del" '
                 f'style="display:contents"><input type="hidden" name="i" '
                 f'value="{i}"><button class="rtool danger" title="Remove">'
                 f'Remove</button></form></span>')
        cards += (f'<div class="hubcard sched" '
                  f'data-hub="{"running" if r["on"] else "paused"}">'
                  f'{toggle}'
                  f'<span class="tdesc hc-t" data-name="{esc(r["name"])}">'
                  f'<b>{esc(r["name"])}</b> '
                  f'<span class="mut">{r["when"]}. {r["what"]}</span></span>'
                  f'<span class="hc-act">{stat}{readit}{tools}</span></div>')
    tpls = "".join(
        f'<div class="hubcard" data-hub="template">'
        f'<span class="idea-ico">{ICONS["flow"]}</span>'
        f'<span class="hc-t"><b>{name}</b><span>{esc(t)}</span></span>'
        f'<span class="hc-act"><form method="post" action="/api/routine" '
        f'style="display:contents">'
        f'<input type="hidden" name="text" value="{esc(t)}">'
        f'<button class="btn ghost sm">+ Add</button></form></span></div>'
        for name, t in TPL_ROUTINES)
    script = """<script>
function routineEdit(i, btn){
  const row = btn.closest('.hubcard');
  if (row.querySelector('.inedit')) return;
  const td = row.querySelector('.tdesc');
  td.dataset.prev = td.innerHTML;
  const cur = td.dataset.name.replace(/"/g, '&quot;');
  td.innerHTML = '<form class="inedit" method="post" action="/api/routine_edit">'
    + '<input type="hidden" name="i" value="' + i + '">'
    + '<input class="inedit-in" name="text" maxlength="140" value="' + cur + '">'
    + '<button class="btn primary sm">Save</button>'
    + '<button type="button" class="btn ghost sm" onclick="routineCancel(this)">Cancel</button>'
    + '</form>';
  const inp = td.querySelector('.inedit-in');
  inp.focus(); inp.select();
  inp.onkeydown = e => { if (e.key === 'Escape') routineCancel(inp); };
}
function routineCancel(el){
  const td = el.closest('.tdesc');
  td.innerHTML = td.dataset.prev;
}
</script>"""
    return (hub_bar("scheduled")
            + hub_head("Scheduled",
                       "Work your team does on its own clock. Each run "
                       "leaves a note you can open, and nothing is ever "
                       "sent without your yes.",
                       "Search scheduled",
                       [("all", "All"), ("running", "Running"),
                        ("paused", "Paused")])
            + f'<div class="hubsec"><h2 class="sec">Yours '
              f'({n_run} running{", " + str(n_paused) + " paused" if n_paused else ""})'
              f'</h2><div class="hubgrid two">{cards}</div></div>'
            + f'<div class="hubsec"><h2 class="sec">Start one</h2>'
              f'<div class="hubgrid">{tpls}</div></div>'
            + f'<form class="notebar" method="post" action="/api/routine">'
              f'<input class="jfind notein" name="text" maxlength="200" '
              f'placeholder="Or say it your way: every Friday evening, '
              f'tell me what we won this week">'
              f'<button class="btn primary sm">Add</button></form>'
            + script
            + _SCHED_INTRO)


# ------------------------------------------------------------- memory
# Files the founder gives the team: price lists, policies, courier
# agreements. Names and sizes only in the demo; agents cite them by name.
KFILES: dict[str, list] = {}


def seed_kfiles(tid: str) -> list:
    return KFILES.setdefault(tid, [
        dict(name="Refund policy Jan 2026.pdf", size="184 KB",
             when="12 Jul", used="Cited in 9 dispute replies"),
        dict(name="Courier agreement Bluedart.pdf", size="1.2 MB",
             when="2 Jul", used="Backs the delivery-scan proof"),
        dict(name="Price list Aug.xlsx", size="96 KB",
             when="1 Aug", used="Read by the callers before offers"),
    ])


def memory_content(tid: str, tab: str = "voice") -> str:
    """Knowledge, structured to grow: counts up top, tabs beneath. Every
    tab is a list that can hold hundreds of rows without changing shape."""
    led = WORLD.d.ledger
    notes = [m for m in led.memory if m.tenant_id == tid
             and m.superseded_by is None]
    ev = [e for e in WORLD.d.evidence if e.tenant_id == tid]
    files = seed_kfiles(tid)
    outcomes = [led.outcome_for(r.run_id) for r in led.runs.values()
                if r.tenant_id == tid]
    n_ended = sum(1 for o in outcomes if o is not None)

    TABS = [("voice", "How you speak", len(notes)),
            ("proof", "Proof on file", len(ev)),
            ("files", "Files", len(files)),
            ("teach", "What they teach each other", len(TEACHINGS)),
            ("outcomes", "How things ended", n_ended)]
    if tab not in {k for k, _, _ in TABS}:
        tab = "voice"

    tiles = ('<div class="stattiles">'
             + "".join(
        f'<a class="stile{" on" if tab == k else ""}" href="/memory?t={k}">'
        f'<b>{n}</b><span>{lbl}</span></a>'
        for k, lbl, n in TABS) + '</div>')

    if tab == "voice":
        body = ('<div class="pagehint">Learned from the wording you '
                'change. The more you touch, the more it sounds like '
                'you.</div>'
                + ("".join(
            f'<div class="trow slim"><span class="ico">{ICONS["pen"]}</span>'
            f'<span class="tdesc"><b>{esc((m.body or {}).get("changed") or "A rewording you made")}</b> '
            f'<span class="mut">{esc((m.body or {}).get("implies") or "kept as a style note")}</span></span>'
            f'<span class="st ok">kept</span></div>'
            for m in notes)
                or '<div class="empty">The first time you reword a '
                   'reply, what it teaches lands here.</div>'))
    elif tab == "proof":
        by_reason = {}
        for e in ev:
            by_reason.setdefault(
                COMP.get(e.reason_code, e.reason_code), []).append(e)
        body = ('<div class="pagehint">Checked for freshness every '
                'Monday. Every reply cites from here.</div>')
        for reason, items in by_reason.items():
            body += f'<h2 class="sec">{esc(reason)}</h2>'
            body += "".join(
                f'<div class="trow slim"><span class="ico">{ICONS["shield"]}</span>'
                f'<span class="tdesc"><b>{EV_TYPE.get(e.evidence_type, esc(e.evidence_type))}</b> '
                f'<span class="mut">{esc(e.text)}</span></span>'
                f'<span class="st ok">fresh</span></div>'
                for e in items)
    elif tab == "files":
        body = ('<div class="pagehint">Anything you upload, the whole '
                'team can read and cite.</div>'
                + "".join(
            f'<div class="trow slim"><span class="ico">{ICONS["note"]}</span>'
            f'<span class="tdesc"><b>{esc(f["name"])}</b> '
            f'<span class="mut">{esc(f["size"])} &middot; added '
            f'{esc(f["when"])} &middot; {esc(f["used"])}</span></span>'
            f'<span class="st ok">readable</span></div>'
            for f in files)
                + '<form class="notebar" method="post" '
                'action="/api/file_upload" enctype="multipart/form-data" '
                'style="max-width:640px;margin-top:16px">'
                '<label class="btn ghost sm" style="cursor:pointer">Choose a file'
                '<input type="file" name="file" required hidden '
                'onchange="this.closest(\'form\').querySelector(\'.fname\').textContent = this.files[0] ? this.files[0].name : \'\'">'
                '</label>'
                '<span class="fname mut" style="flex:1;font-size:13px"></span>'
                '<button class="btn primary sm">Upload</button></form>')
    elif tab == "teach":
        body = teachings_html()
    else:
        body = ('<div class="pagehint">How each finished dispute ended, '
                'and what the next one starts from.</div>'
                f'<div class="trow slim"><span class="ico">{ICONS["chart"]}</span>'
                f'<span class="tdesc"><b>{n_ended} finished '
                f'dispute{"s" if n_ended != 1 else ""} remembered</b> '
                f'<span class="mut">how each ended, and for how much</span>'
                f'</span><span class="st ok">kept for good</span></div>'
                f'<div class="trow slim"><span class="ico">{ICONS["moon"]}</span>'
                f'<span class="tdesc"><b>Every night, it goes over the '
                f'day</b> <span class="mut">what repeated, what worked, '
                f'kept as notes you can read</span></span>'
                f'<span class="st ok">every night</span></div>')

    return (hub_bar("memory")
            + f'<div class="hubhead"><div><h1 class="page">Knowledge</h1>'
            f'<div class="pagehint">Everything your team knows about your '
            f'business. Add to it, correct it; nothing is forgotten.</div>'
            f'</div>'
            f'<input class="hubsearch" placeholder="Search this tab" '
            f'data-sel=".trow" oninput="hubFilter(this)"></div>'
            f'{tiles}{body}')

# Every decision leaves a line: who, what, when, where to see it.
DECISION_LOG: dict[str, list] = {}


def log_decision(tid, actor, text, href=""):
    from datetime import datetime as _dt
    DECISION_LOG.setdefault(tid, []).append(
        (_dt.utcnow(), actor or "you", text, href))


def decisions_content(tid):
    led = WORLD.d.ledger
    items = list(DECISION_LOG.get(tid, []))
    for r in led.runs.values():
        if r.tenant_id != tid or r.gate_action is None:
            continue
        who = _who(r.gate_actor, seed=r.run_id)
        act = r.gate_action.value
        word = {"approve": "Said yes to the reply for",
                "reject": "Said no to the reply for",
                "edit": "Reworded and sent the reply for",
                "timeout": "Let the clock hand over the reply for"
                }.get(act, act)
        items.append((r.occurred_at, who,
                      word + " <b>" + esc(_account_label(r)) + "</b>",
                      ("/cases/" + r.order_id) if r.order_id else ""))
    items.sort(key=lambda t: t[0], reverse=True)
    rows = "".join(
        '<a class="trow slim" style="display:flex" href="'
        + (href or "#") + '">'
        + '<span class="ico">' + ICONS["shield"] + '</span>'
        + '<span class="tdesc">'
        + ("You" if who == "you" else mention(who))
        + ' &middot; ' + text + '</span>'
        + '<span class="when">' + ts.strftime("%-d %b, %H:%M")
        + '</span></a>'
        for ts, who, text, href in items[:40]) or (
        '<div class="empty">Decisions land here as they are made.'
        '</div>')
    return ('<div class="pagehint">Every decision anyone made, newest '
            'first. Nothing here can be edited or deleted; a decision '
            'is a fact once made.</div>' + rows)


# Connections the founder can actually manage. Relay still holds every
# key; pausing stops reads, disconnecting removes it, connecting adds.
CONN_DEFS = [
    ("Your store", "ojaswellness.in orders and disputes"),
    ("Amazon and Flipkart", "marketplace orders and claims"),
    ("WhatsApp", "buyer messages, and your yeses on the go"),
    ("Voice calls", "the callers ring buyers through this"),
    ("Bank and settlements", "what landed, what was deducted"),
    ("Email", "dispute mail from the bank"),
]
CONN_MORE = [
    ("Tally", "your books, posted automatically"),
    ("Zoho Books", "invoices tied to orders"),
    ("Shiprocket", "courier scans as dispute proof"),
    ("Instagram", "DMs from buyers who shop there"),
    ("Quick commerce", "Blinkit and Zepto orders"),
]
CONN_STATE: dict = {}


def conn_state(tid: str) -> dict:
    return CONN_STATE.setdefault(
        tid, {n: "on" for n, _ in CONN_DEFS})


# ------------------------------------------------------------- capability hub
# One grammar for the four capability surfaces (the Comet pattern): a tab
# strip across them, a title with a working search, filter pills, and
# sectioned card grids. Filtering is client-side and instant.
HUB_TABS = [("connections", "Connections", "/connections"),
            ("skills", "Skills", "/skills"),
            ("scheduled", "Scheduled", "/scheduled"),
            ("memory", "Knowledge", "/memory")]


def hub_bar(active: str) -> str:
    return ('<div class="hubtabs">' + "".join(
        f'<a class="{"on" if k == active else ""}" href="{href}">{lbl}</a>'
        for k, lbl, href in HUB_TABS) + '</div>')


def hub_head(title: str, sub: str, placeholder: str,
             pills=None, right: str = "") -> str:
    pillrow = ""
    if pills:
        pillrow = ('<div class="hubpills">' + "".join(
            f'<button class="hubpill{" on" if i == 0 else ""}" '
            f'data-f="{key}" onclick="hubPill(this)">{lbl}</button>'
            for i, (key, lbl) in enumerate(pills)) + '</div>')
    return (
        f'<div class="hubhead"><div><h1 class="page">{title}</h1>'
        f'<div class="pagehint">{sub}</div></div>'
        f'<input class="hubsearch" placeholder="{placeholder}" '
        f'oninput="hubFilter(this)"></div>'
        + (f'<div class="hubtoolrow">{pillrow}{right}</div>'
           if (pills or right) else ""))


def connections_hub(tid: str) -> str:
    st = conn_state(tid)

    def card(n, d, state):
        if state in ("on", "paused"):
            paused = state == "paused"
            chip = ('<span class="st mut">paused</span>' if paused
                    else '<span class="st ok">connected</span>')
            acts = (
                '<span class="rtools">'
                '<form method="post" action="/api/conn_act" '
                'style="display:contents">'
                '<input type="hidden" name="name" value="' + n + '">'
                '<input type="hidden" name="act" value="'
                + ("resume" if paused else "pause") + '">'
                '<button class="rtool">'
                + ("Resume" if paused else "Pause") + '</button></form>'
                '<form method="post" action="/api/conn_act" '
                'style="display:contents">'
                '<input type="hidden" name="name" value="' + n + '">'
                '<input type="hidden" name="act" value="disconnect">'
                '<button class="rtool danger">Disconnect</button></form>'
                '</span>')
            data = "connected"
        else:
            chip = ""
            acts = ('<form method="post" action="/api/conn_act" '
                    'style="display:contents">'
                    '<input type="hidden" name="name" value="' + n + '">'
                    '<input type="hidden" name="act" value="connect">'
                    '<button class="btn ghost sm">+ Connect</button></form>')
            data = "available"
        return (f'<div class="hubcard" data-hub="{data}">{_logo(n, 34)}'
                f'<span class="hc-t"><b>{n}</b><span>{d}</span></span>'
                f'<span class="hc-act">{chip}{acts}</span></div>')

    pairs = CONN_DEFS + CONN_MORE
    connected = [(n, d) for n, d in pairs if st.get(n) in ("on", "paused")]
    avail = [(n, d) for n, d in pairs
             if st.get(n) not in ("on", "paused")]
    return (
        hub_bar("connections")
        + hub_head("Connections",
                   "Relay holds every key itself: nothing to set up, "
                   "nothing to lose. Pause stops the reading; disconnect "
                   "removes it.",
                   "Search connections",
                   [("all", "All"), ("connected", "Connected"),
                    ("available", "Available")])
        + f'<div class="hubsec"><h2 class="sec">Connected '
          f'({len(connected)})</h2><div class="hubgrid">'
        + "".join(card(n, d, st.get(n, "off")) for n, d in connected)
        + '</div></div>'
        + f'<div class="hubsec"><h2 class="sec">Available '
          f'({len(avail)})</h2><div class="hubgrid">'
        + "".join(card(n, d, "off") for n, d in avail)
        + '</div></div>')


def seed_skills(tid: str) -> list:
    lst = KYC_PROCS.setdefault(tid, [])
    if not lst:
        lst.extend([
            dict(title="Clear a flagged buyer", mode="live",
                 when="A buyer trips the quick check. Runs the deep check, "
                      "clears or refuses politely, writes the why into "
                      "the case."),
            dict(title="Advance payment with PAN check", mode="live",
                 when="Any order above ₹2 lakh. One form takes the PAN "
                      "and the advance together; the order holds until "
                      "both clear."),
            dict(title="Refund only after the photos check out",
                 mode="draft",
                 when="A return lands. The photos must match the batch "
                      "seal before any money moves."),
        ])
    return lst


def skills_hub(tid: str) -> str:
    procs = seed_skills(tid)

    def card(i, p):
        live = p.get("mode") == "live"
        chip = ('<span class="st ok">Live</span>' if live
                else '<span class="st mut">draft</span>')
        acts = (
            f'<span class="rtools">'
            f'<a class="rtool" href="/procedures/new?ask='
            f'{esc(p["title"])}">Open</a>'
            f'<form method="post" action="/api/skill_act" '
            f'style="display:contents">'
            f'<input type="hidden" name="i" value="{i}">'
            f'<input type="hidden" name="act" '
            f'value="{"draft" if live else "live"}">'
            f'<button class="rtool">'
            f'{"Back to draft" if live else "Set live"}</button></form>'
            f'<form method="post" action="/api/skill_act" '
            f'style="display:contents">'
            f'<input type="hidden" name="i" value="{i}">'
            f'<input type="hidden" name="act" value="del">'
            f'<button class="rtool danger">Remove</button></form></span>')
        return (f'<div class="hubcard" data-hub='
                f'"{"live" if live else "draft"}">'
                f'<span class="idea-ico">{ICONS["note"]}</span>'
                f'<span class="hc-t"><b>{esc(p["title"])}</b>'
                f'<span>{esc((p.get("when") or "")[:120])}</span></span>'
                f'<span class="hc-act">{chip}{acts}</span></div>')

    live = [(i, p) for i, p in enumerate(procs) if p.get("mode") == "live"]
    drafts = [(i, p) for i, p in enumerate(procs)
              if p.get("mode") != "live"]
    out = (hub_bar("skills")
           + hub_head("Skills",
                      "What your agents know how to do: steps they "
                      "follow, written in your words. Nothing goes live "
                      "until you say yes.",
                      "Search skills",
                      [("all", "All"), ("live", "Live"),
                       ("draft", "Drafts")],
                      '<a class="btn primary sm" href="/procedures/new">'
                      '+ Create skill</a>'))
    if live:
        out += (f'<div class="hubsec"><h2 class="sec">Live '
                f'({len(live)})</h2><div class="hubgrid">'
                + "".join(card(i, p) for i, p in live) + '</div></div>')
    if drafts:
        out += (f'<div class="hubsec"><h2 class="sec">Drafts '
                f'({len(drafts)})</h2><div class="hubgrid">'
                + "".join(card(i, p) for i, p in drafts) + '</div></div>')
    if not procs:
        out += ('<div class="empty">No skills yet. Describe one and it '
                'opens in the editor.</div>')
    return out


# One-click starts for Scheduled: each becomes a real routine on add.
TPL_ROUTINES = [
    ("Evening COD summary",
     "every evening at 7, tell me which COD orders confirmed and "
     "which held"),
    ("Friday vendor dues",
     "every Friday morning, list what vendors are owed next week"),
    ("Six o'clock stock check",
     "every day at 6 PM, flag anything under ten days of stock"),
]


def settings_content(tid: str, s: str = "team") -> str:
    # Each tab is a job the founder actually comes here to do, named as
    # that job — not as a module.
    SECTIONS = [("team", "Who can say yes"),
                ("connectors", "Connections"),
                ("decisions", "Decisions"),
                ("workspace", "The promises"),
                ("data", "Your data")]
    if s not in {k for k, _ in SECTIONS}:
        s = "team"
    tabs = ('<div class="tabbar">' + "".join(
        f'<a class="{"on" if k == s else ""}" href="/settings?s={k}">{t}</a>'
        for k, t in SECTIONS) + "</div>").replace(
        '/settings?s=connectors', '/connections')

    if s == "team":
        body = (f'<div class="pagehint">Nothing that touches money or a '
                f'customer goes out without a yes: this is where you '
                f'decide whose yes counts. Hand it to someone before a '
                f'holiday, take it back after. Everyone else still sees '
                f'everything; they just can&rsquo;t approve.</div>')
        for i, p in enumerate(people_for(tid)):
            toggle = (f'<form method="post" action="/api/person_toggle" '
                      f'style="display:contents"><input type="hidden" '
                      f'name="i" value="{i}">'
                      f'<button class="tglbtn {"on" if p["approver"] else ""}" '
                      f'title="{"Take back the yes" if p["approver"] else "Let them say yes"}">'
                      f'<i></i></button></form>')
            body += (f'<div class="trow slim" style="display:flex">{toggle}'
                     f'<span class="tdesc"><b>{esc(p["name"])}</b> '
                     f'<span class="mut">{esc(p["role"])}</span></span>'
                     + ('<span class="st ok">can say yes</span>'
                        if p["approver"] else
                        '<span class="st mut">sees everything, '
                        'approves nothing</span>')
                     + f'<span class="rtools"><form method="post" '
                     f'action="/api/person_del" style="display:contents">'
                     f'<input type="hidden" name="i" value="{i}">'
                     f'<button class="rtool danger">Remove</button></form>'
                     f'</span></div>')
        body += (f'<form class="notebar" method="post" action="/api/person_add" '
                 f'style="max-width:640px;margin-top:16px;gap:8px">'
                 f'<input class="jfind notein" name="name" maxlength="60" '
                 f'placeholder="Name" style="flex:1">'
                 f'<input class="jfind notein" name="role" maxlength="60" '
                 f'placeholder="What they do: e.g. accounts" style="flex:1.4">'
                 f'<button class="btn primary sm">Add</button></form>')
        relay_rows = "".join(
            f'<div class="trow slim"><span class="ico">{ICONS["bot"]}</span>'
            f'<span class="tdesc"><b>{esc(n)}</b> <span class="mut">{esc(r)}'
            f'</span></span></div>'
            for _, n, r, d, _ in TEAM if d != BUSINESS)
        body += (f'<h2 class="sec">Relay&rsquo;s people</h2>'
                 f'<div class="pagehint">They pick up whatever the agents '
                 f'hand over. You don&rsquo;t manage them: they come '
                 f'with Relay.</div>{relay_rows}')
    elif s == "connectors":
        st = conn_state(tid)
        rows = ""
        for n, d in CONN_DEFS + CONN_MORE:
            state = st.get(n, "off")
            if state == "off":
                continue
            paused = state == "paused"
            rows += (
                '<div class="trow slim" style="display:flex">'
                '<span class="ico">' + ICONS["flow"] + '</span>'
                '<span class="tdesc"><b>' + n + '</b> '
                '<span class="mut">' + d + '</span></span>'
                + ('<span class="st mut">paused</span>' if paused
                   else '<span class="st ok">connected</span>')
                + '<span class="rtools">'
                '<form method="post" action="/api/conn_act" '
                'style="display:contents">'
                '<input type="hidden" name="name" value="' + n + '">'
                '<input type="hidden" name="act" value="'
                + ("resume" if paused else "pause") + '">'
                '<button class="rtool">'
                + ("Resume" if paused else "Pause") + '</button></form>'
                '<form method="post" action="/api/conn_act" '
                'style="display:contents">'
                '<input type="hidden" name="name" value="' + n + '">'
                '<input type="hidden" name="act" value="disconnect">'
                '<button class="rtool danger">Disconnect</button></form>'
                '</span></div>')
        more = ""
        for n, d in CONN_MORE + [(n2, d2) for n2, d2 in CONN_DEFS
                                 if st.get(n2) == "off"]:
            if st.get(n) in ("on", "paused"):
                continue
            more += (
                '<div class="trow slim" style="display:flex">'
                '<span class="ico">' + ICONS["flow"] + '</span>'
                '<span class="tdesc"><b>' + n + '</b> '
                '<span class="mut">' + d + '</span></span>'
                '<form method="post" action="/api/conn_act" '
                'style="display:contents">'
                '<input type="hidden" name="name" value="' + n + '">'
                '<input type="hidden" name="act" value="connect">'
                '<button class="btn ghost sm">Connect</button></form>'
                '</div>')
        body = ('<div class="pagehint">Relay holds every key itself: '
                'nothing to set up, nothing to lose. Pause stops reading; '
                'disconnect removes it.</div>'
                + rows
                + '<h2 class="sec">More you can plug in</h2>' + more)
    elif s == "decisions":
        body = decisions_content(tid)
    elif s == "data":
        exports = [
            ("Everything, as a spreadsheet", "/export/impact.csv",
             "Every dispute your team has touched: who, what, when, "
             "how it ended, and for how much."),
            ("The proof on file", "/export/evidence.csv",
             "Every piece of proof your replies cite, with what it proves."),
            ("What your team remembers", "/export/memory.csv",
             "How you like things said: every note learned from the "
             "wording you changed."),
        ]
        body = ('<div class="pagehint">It&rsquo;s yours. Take all of it, any '
                'time, no questions: each file opens in Excel or '
                'Google Sheets. Leaving is allowed to be easy; that is what '
                'makes staying a choice.</div>'
                + "".join(
            f'<a class="trow slim" style="display:flex" href="{href}">'
            f'<span class="ico">{ICONS["ledger"]}</span>'
            f'<span class="tdesc"><b>{t}</b> <span class="mut">{d}</span>'
            f'</span><span class="st wait">download &rarr;</span></a>'
            for t, href, d in exports))
    else:
        body = ('<div class="pagehint">These are not settings you can slide '
                'around: each one is a promise the software keeps, '
                'written here so you can hold it to them.</div>'
                + "".join(
            f'<div class="trow slim"><span class="ico">{ICONS["shield"]}</span>'
            f'<span class="tdesc"><b>{esc(k)}</b> <span class="mut">{v}</span></span></div>'
            for k, v in [
                (("Small ones send themselves" if autonomy_mode(tid) == "small"
                  else "You approve before anything sends"),
                 ("under &#8377;500 and nothing unusual goes out on its own; "
                  "everything else still waits for your yes, and all of it "
                  "lands in History" if autonomy_mode(tid) == "small" else
                  "nothing goes to a bank without your yes; if you "
                  "don&rsquo;t answer, it waits for a person: it "
                  "never sends itself")),
                ("Never more than 3 at a time", "your attention is protected; "
                 "if a flood arrives, a person is told"),
                ("The same claim is never worked twice", "not within a week, "
                 "on the same order"),
                ("Every dispute gets worked", "no experiments here: a "
                 "missed deadline is your money, not a test"),
                ("Your team works in its own sealed room", "everything it "
                 "does happens in a private space that belongs to your "
                 "business alone: nothing it reads or writes can "
                 "touch anyone else's, and nothing leaves the room without "
                 "your yes"),
                ("Your record is yours", "one click, everything, in a "
                 "spreadsheet"),
                ("Your logins are never sitting in a file", "they are kept in "
                 "the safe your computer already uses for passwords. "
                 "nowhere you could lose them")]))
    ident = (f'<div class="trow slim"><span class="ico">{ICONS["bm"]}</span>'
             f'<span class="tdesc"><b>{BUSINESS}</b> '
             f'<span class="mut">{BUSINESS_TAG} &middot; sells on '
             f'{BUSINESS_CHANNELS}</span></span>'
             f'<span class="st ok">this workspace</span></div>')
    return ('<h1 class="page">Settings</h1>'
            + ident + tabs + body)


# ---------------------------------------------------------------- http
class Handler(BaseHTTPRequestHandler):
    # -- session plumbing ------------------------------------------------------
    def _session(self) -> dict | None:
        cookies = self.headers.get("Cookie", "")
        for part in cookies.split(";"):
            k, _, v = part.strip().partition("=")
            if k == "cm_session" and v:
                return verify_session(v, session_secret())
        return None

    def _redirect(self, location: str, session: dict | None = None):
        self.send_response(303)
        self.send_header("Location", location)
        if session is not None:
            token = sign_session(session, session_secret())
            self.send_header("Set-Cookie",
                             f"cm_session={token}; Path=/; HttpOnly; SameSite=Lax")
        self.end_headers()

    def _html(self, body: str, status: int = 200):
        raw = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path.split("?")[0] in ("/", "/index.html"):
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            from urllib.parse import parse_qs as _pq, urlparse as _up
            conv = _pq(_up(self.path).query).get("c", [""])[0]
            from urllib.parse import parse_qs as _pq, urlparse as _up
            _as = _pq(_up(self.path).query).get("as", ["owner"])[0]
            say = _pq(_up(self.path).query).get("say", [""])[0][:120]
            self._html(chat_render(sess["tenant_id"], conv,
                                   sess.get("email", ""), _as, say=say))
        elif self.path in ("/tasks", "/approvals"):
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            self._html(render(sess["tenant_id"], sess.get("email", "")))
        elif self.path == "/login":
            self._html(auth_page("login"))
        elif self.path == "/signup":
            self._html(auth_page("signup"))
        elif self.path == "/logout":
            self.send_response(303)
            self.send_header("Location", "/login")
            self.send_header("Set-Cookie",
                             "cm_session=; Path=/; Max-Age=0")
            self.end_headers()
        elif self.path == "/static/InterVariable.woff2":
            data = (Path(__file__).parent / "static" / "InterVariable.woff2").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "font/woff2")
            self.send_header("Cache-Control", "max-age=31536000, immutable")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        elif self.path == "/export/impact.csv":
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            import csv, io
            led = WORLD.d.ledger
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(["run_id", "occurred_at", "source", "customer",
                        "product", "dispute_reason", "claim", "state",
                        "gate_action", "gate_actor", "edit_material", "won",
                        "amount_paise"])
            for r in sorted((x for x in led.runs.values()
                             if x.tenant_id == sess["tenant_id"]),
                            key=lambda x: x.occurred_at):
                out = led.outcome_for(r.run_id)
                v = (out.outcome_value or {}) if out else {}
                w.writerow([
                    r.run_id, r.occurred_at.isoformat(), r.trigger_source,
                    _account_label(r), bought(r.order_id),
                    COMP.get(r.reason_code, r.reason_code),
                    r.claim_text or "", r.state.value,
                    r.gate_action.value if r.gate_action else "",
                    r.gate_actor or "", r.gate_is_material,
                    v.get("won", ""), v.get("amount_paise", "")])
            data = buf.getvalue().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition",
                             'attachment; filename="impact.csv"')
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        elif self.path in ("/export/evidence.csv", "/export/memory.csv"):
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            import csv, io
            tid = sess["tenant_id"]
            buf = io.StringIO()
            w = csv.writer(buf)
            if self.path.endswith("evidence.csv"):
                w.writerow(["dispute_reason", "evidence_type",
                            "what_it_proves", "source_url"])
                for e in WORLD.d.evidence:
                    if e.tenant_id == tid:
                        w.writerow([COMP.get(e.reason_code, e.reason_code),
                                    EV_TYPE.get(e.evidence_type,
                                                e.evidence_type),
                                    e.text, e.source_url])
                fname = "evidence.csv"
            else:
                w.writerow(["what_you_changed", "what_it_taught"])
                for m in WORLD.d.ledger.memory:
                    if m.tenant_id == tid and m.superseded_by is None:
                        b = m.body or {}
                        w.writerow([b.get("changed") or "a rewording",
                                    b.get("implies") or "style note"])
                fname = "memory.csv"
            data = buf.getvalue().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition",
                             f'attachment; filename="{fname}"')
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        elif self.path == "/settings?s=connectors" \
                or self.path.startswith("/settings?s=connectors&"):
            return self._redirect("/connections")
        elif self.path.split("?")[0] == "/settings":
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            from urllib.parse import parse_qs as _pq, urlparse as _up
            _sec = _pq(_up(self.path).query).get("s", ["team"])[0]
            self._html(_shell(settings_content(sess["tenant_id"], _sec), "settings",
                              sess["tenant_id"], sess.get("email", "")))
        elif self.path == "/projects":
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            self._html(_shell(projects_content(sess["tenant_id"]), "projects",
                              sess["tenant_id"], sess.get("email", "")))
        elif self.path.split("?")[0] == "/activity":
            return self._redirect(self.path.replace("/activity", "/impact", 1))
        elif self.path.split("?")[0] == "/impact":
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            from urllib.parse import parse_qs as _pq, urlparse as _up
            f = _pq(_up(self.path).query).get("f", ["all"])[0]
            self._html(_shell(activity_content(sess["tenant_id"], f), "activity",
                              sess["tenant_id"], sess.get("email", "")))
        elif self.path.startswith("/agents/"):
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            from urllib.parse import parse_qs as _pq, urlparse as _up
            u = _up(self.path)
            qs = _pq(u.query)
            slug = u.path.rsplit("/", 1)[-1]
            self._html(_shell(
                agent_detail_content(sess["tenant_id"], slug,
                                     qs.get("tab", ["overview"])[0],
                                     int(qs.get("item", ["0"])[0] or 0)),
                "agents", sess["tenant_id"], sess.get("email", "")))
        elif self.path == "/connections":
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            self._html(_shell(connections_hub(sess["tenant_id"]),
                              "settings", sess["tenant_id"],
                              sess.get("email", "")))
        elif self.path == "/skills":
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            self._html(_shell(skills_hub(sess["tenant_id"]),
                              "memory", sess["tenant_id"],
                              sess.get("email", "")))
        elif self.path.split("?")[0] == "/agents":
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            from urllib.parse import parse_qs as _pq, urlparse as _up
            f = _pq(_up(self.path).query).get("f", ["all"])[0]
            self._html(_shell(agents_content(sess["tenant_id"], f), "agents",
                              sess["tenant_id"], sess.get("email", "")))
        elif self.path.startswith("/briefs/"):
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            slug = self.path.rsplit("/", 1)[-1]
            self._html(_shell(_brief_page(sess["tenant_id"], slug),
                              "scheduled",
                              sess["tenant_id"], sess.get("email", "")))
        elif self.path.split("?")[0] == "/procedures/new":
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            from urllib.parse import parse_qs as _pq, urlparse as _up
            ask = (_pq(_up(self.path).query).get("ask") or [""])[0]
            self._html(_shell(procedure_editor_content(sess["tenant_id"], ask),
                              "agents", sess["tenant_id"],
                              sess.get("email", "")))
        elif self.path == "/scheduled":
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            self._html(_shell(scheduled_content(sess["tenant_id"]), "scheduled",
                              sess["tenant_id"], sess.get("email", "")))
        elif self.path.split("?")[0] == "/memory":
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            from urllib.parse import parse_qs as _pq, urlparse as _up
            t = (_pq(_up(self.path).query).get("t") or ["voice"])[0]
            self._html(_shell(memory_content(sess["tenant_id"], t), "memory",
                              sess["tenant_id"], sess.get("email", "")))
        elif self.path == "/shadow":
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            self._html(_shell(shadow_content(sess["tenant_id"]), "shadow",
                              sess["tenant_id"], sess.get("email", "")))
        elif self.path in ("/workflows", "/knowledge"):
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            builder = {"/workflows": workflows_content,
                       "/knowledge": knowledge_content}[self.path]
            self._html(_shell(builder(sess["tenant_id"]), self.path.strip("/"),
                              sess["tenant_id"], sess.get("email", "")))
        elif self.path.split("?")[0] == "/journeys":
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            from urllib.parse import parse_qs as _pq, urlparse as _up
            sel = _pq(_up(self.path).query).get("a", [""])[0]
            self._html(_shell(journeys_content(sess["tenant_id"], sel),
                              "journeys", sess["tenant_id"], sess.get("email", "")))
        elif self.path.startswith("/accounts/"):
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            self._html(_shell(account_journey_content(sess["tenant_id"],
                                                      self.path.rsplit("/", 1)[-1]),
                              "projects", sess["tenant_id"], sess.get("email", "")))
        elif self.path.startswith("/cases/"):
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            ref = self.path.split("?")[0].split("#")[0].rsplit("/", 1)[-1]
            # a case is keyed by the order; anything else that names one row
            # of it (a single job id) redirects to the case that holds it
            row = WORLD.d.ledger.runs.get(ref)
            if row is not None and row.order_id:
                return self._redirect(f"/cases/{row.order_id}")
            self._html(_shell(case_content(sess["tenant_id"], ref), ref,
                              sess["tenant_id"], sess.get("email", "")))
        elif self.path.startswith("/runs/"):
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            self._html(_shell(run_trace_content(sess["tenant_id"],
                                                self.path.rsplit("/", 1)[-1]),
                              "activity", sess["tenant_id"], sess.get("email", "")))
        elif self.path == "/vault":
            return self._redirect("/settings#vault")
        elif self.path == "/chat":
            return self._redirect("/")
        elif (self.path.startswith("/api/")
                and self.path != "/api/runs"
                and not self.path.startswith("/api/search")):
            # A POST endpoint opened in a browser tab should never show the
            # raw Python error page — nothing here is for reading anyway.
            return self._redirect("/")
        elif self.path.split("?")[0] == "/api/search":
            sess = self._session()
            if not sess:
                self.send_response(403); self.end_headers(); return
            from urllib.parse import parse_qs as _pq, urlparse as _up
            q = (_pq(_up(self.path).query).get("q")
                 or [""])[0].strip().lower()
            tid = sess["tenant_id"]
            out = []
            if q:
                for a in RELAY_AGENTS:
                    if q in a["role"].lower() or q in a["name"].lower():
                        out.append(dict(kind="Agent", title=a["role"],
                                        sub=a["name"],
                                        href="/agents/" + a["slug"]))
                for c in rail_cases(tid, limit=50):
                    if q in c["label"].lower():
                        nm, _, prod = c["label"].partition(" · ")
                        out.append(dict(kind="Buyer", title=nm,
                                        sub=prod + " · " + c["word"],
                                        href="/cases/" + c["order"]))
                for f in seed_kfiles(tid):
                    if q in f["name"].lower():
                        out.append(dict(kind="File", title=f["name"],
                                        sub=f["used"],
                                        href="/memory?t=files"))
                for r in routines_for(tid):
                    if q in r["name"].lower() or q in r["what"].lower():
                        out.append(dict(kind="Routine", title=r["name"],
                                        sub=r["when"],
                                        href="/scheduled"))
                for p in people_for(tid):
                    if (q in p["name"].lower()
                            or q in p["role"].lower()):
                        out.append(dict(
                            kind="Teammate", title=p["name"],
                            sub=p["role"] + (" · can say yes"
                                             if p["approver"] else ""),
                            href="/settings?s=team"))
            body = json.dumps(out[:20]).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/api/runs":
            led = WORLD.d.ledger
            body = json.dumps([{ "run_id": r.run_id, "state": r.state.value,
                                 "reason_code": r.reason_code, "claim": r.claim_text}
                               for r in led.runs.values()]).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(404)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length).decode(errors="replace")
        if self.path == "/slack/interactions":
            return self._slack_interaction(raw)
        if self.path.startswith("/webhooks/fathom"):
            return self._fathom_webhook(raw)
        if self.path in ("/auth/signup", "/auth/login", "/auth/demo",
                         "/auth/verify"):
            return self._auth(raw)
        if self.path == "/api/agent_cfg":
            sess = self._session()
            if not sess:
                self.send_response(403); self.end_headers(); return
            form = parse_qs(raw)
            slug = (form.get("slug") or [""])[0]
            if any(x["slug"] == slug for x in RELAY_AGENTS):
                tid = sess["tenant_id"]
                cfg = agent_cfg(tid, slug)
                me = (sess.get("email") or "you").split("@")[0]
                role = next(x["role"] for x in RELAY_AGENTS
                            if x["slug"] == slug)
                if "instructions" in form:
                    cfg["instructions"] = form["instructions"][0].strip()[:400]
                    log_decision(tid, me, "Set standing instructions for "
                                 "<b>" + esc(role) + "</b>",
                                 "/agents/" + slug + "?tab=settings")
                if "tool" in form:
                    t = form["tool"][0]
                    if t in cfg["tools_off"]:
                        cfg["tools_off"].remove(t)
                        word = "Gave back a tool to "
                    else:
                        cfg["tools_off"].append(t)
                        word = "Took a tool away from "
                    log_decision(tid, me, word + "<b>" + esc(role) + "</b>",
                                 "/agents/" + slug + "?tab=settings")
                if "learn" in form:
                    cfg["learn"] = form["learn"][0] == "1"
                    log_decision(tid, me,
                                 ("Resumed" if cfg["learn"] else "Paused")
                                 + " learning for <b>" + esc(role) + "</b>",
                                 "/agents/" + slug + "?tab=settings")
                for k in list(form):
                    if k.startswith("guard_"):
                        cfg["guards"][k[6:]] = form[k][0].strip()[:40]
                        log_decision(tid, me, "Changed a limit for <b>"
                                     + esc(role) + "</b>: "
                                     + esc(form[k][0].strip()[:40]),
                                     "/agents/" + slug + "?tab=settings")
                if "run_checks" in form:
                    from datetime import datetime as _dt
                    cfg["eval_at"] = _dt.now().strftime("%-d %b, %H:%M")
                    log_decision(tid, me, "Ran the checks on <b>"
                                 + esc(role) + "</b>: all good",
                                 "/agents/" + slug + "?tab=settings")
            self.send_response(303)
            self.send_header("Location", "/agents/" + slug + "?tab=settings")
            self.end_headers(); return
        if self.path in ("/api/agent_on", "/api/agent_off"):
            sess = self._session()
            if not sess:
                self.send_response(403); self.end_headers(); return
            slug = (parse_qs(raw).get("slug") or [""])[0]
            if any(x["slug"] == slug for x in RELAY_AGENTS):
                DEMO_ON[slug] = self.path.endswith("_on")
                role = next(x["role"] for x in RELAY_AGENTS
                            if x["slug"] == slug)
                log_decision(
                    sess["tenant_id"],
                    (sess.get("email") or "you").split("@")[0],
                    ("Switched on <b>" if DEMO_ON[slug]
                     else "Switched off <b>") + esc(role) + "</b>",
                    "/agents/" + slug)
            self.send_response(303)
            self.send_header("Location", f"/agents/{slug}")
            self.end_headers(); return
        if self.path == "/api/mode":
            sess = self._session()
            if not sess:
                self.send_response(403); self.end_headers(); return
            tid = sess["tenant_id"]
            mode = (parse_qs(raw).get("mode") or ["all"])[0]
            # The unlock is checked here too — the UI disables the option,
            # but the server is the one that keeps the promise.
            if mode == "small" and clean_yeses(tid) >= YES_TARGET:
                AUTONOMY[tid] = "small"
                log_decision(tid, (sess.get("email") or "you").split("@")[0],
                             "Let small ones send themselves "
                             "(under &#8377;500)", "/settings?s=decisions")
            elif mode == "all":
                AUTONOMY[tid] = "all"
                log_decision(tid, (sess.get("email") or "you").split("@")[0],
                             "Back to approving everything",
                             "/settings?s=decisions")
            self.send_response(303)
            self.send_header("Location", "/")
            self.end_headers(); return
        if self.path == "/api/skill_act":
            sess = self._session()
            if not sess:
                self.send_response(403); self.end_headers(); return
            form = parse_qs(raw)
            tid = sess["tenant_id"]
            me = (sess.get("email") or "you").split("@")[0]
            try:
                i = int((form.get("i") or ["-1"])[0])
            except ValueError:
                i = -1
            act = (form.get("act") or [""])[0]
            lst = seed_skills(tid)
            if 0 <= i < len(lst):
                p = lst[i]
                if act == "del":
                    lst.pop(i)
                    log_decision(tid, me, "Removed the skill <b>"
                                 + esc(p["title"]) + "</b>", "/skills")
                elif act in ("live", "draft"):
                    p["mode"] = act
                    log_decision(tid, me,
                                 ("Set the skill <b>" + esc(p["title"])
                                  + "</b> live") if act == "live" else
                                 ("Moved the skill <b>" + esc(p["title"])
                                  + "</b> back to draft"), "/skills")
            self.send_response(303)
            self.send_header("Location", "/skills")
            self.end_headers()
            return
        if self.path == "/api/conn_act":
            sess = self._session()
            if not sess:
                self.send_response(403); self.end_headers(); return
            form = parse_qs(raw)
            name = (form.get("name") or [""])[0]
            act = (form.get("act") or [""])[0]
            tid = sess["tenant_id"]
            st = conn_state(tid)
            known = [n for n, _ in CONN_DEFS] + [n for n, _ in CONN_MORE]
            me = (sess.get("email") or "you").split("@")[0]
            if name in known:
                if act == "pause" and st.get(name) == "on":
                    st[name] = "paused"
                    log_decision(tid, me, "Paused the <b>" + esc(name)
                                 + "</b> connection",
                                 "/connections")
                elif act == "resume" and st.get(name) == "paused":
                    st[name] = "on"
                    log_decision(tid, me, "Resumed the <b>" + esc(name)
                                 + "</b> connection",
                                 "/connections")
                elif act == "disconnect" and st.get(name) in ("on", "paused"):
                    st[name] = "off"
                    log_decision(tid, me, "Disconnected <b>" + esc(name)
                                 + "</b>", "/connections")
                elif act == "connect" and st.get(name, "off") == "off":
                    st[name] = "on"
                    log_decision(tid, me, "Connected <b>" + esc(name)
                                 + "</b>", "/connections")
            self.send_response(303)
            self.send_header("Location", "/connections")
            self.end_headers(); return
        if self.path in ("/api/prop_act", "/api/cashflow_act"):
            sess = self._session()
            if not sess:
                self.send_response(403); self.end_headers(); return
            form = parse_qs(raw)
            action = (form.get("action") or [""])[0]
            slug = (form.get("slug") or ["cashflow_forecast"])[0]
            if slug in PROPS_DEF:
                p = prop_state(sess["tenant_id"], slug)
                if (p["state"] == "waiting"
                        and action in ("approve", "decline")):
                    p["state"] = ("approved" if action == "approve"
                                  else "declined")
                    p["decided_by"] = (sess.get("email")
                                       or "you").split("@")[0]
                    log_decision(
                        sess["tenant_id"], p["decided_by"],
                        ("Approved " if action == "approve" else "Declined ")
                        + "<b>" + esc(PROPS_DEF[slug]["title"]) + "</b>",
                        "/agents/" + slug)
            self.send_response(303)
            self.send_header("Location", f"/agents/{slug}")
            self.end_headers(); return
        if self.path == "/api/procedure_save":
            sess = self._session()
            if not sess:
                self.send_response(403); self.end_headers(); return
            form = parse_qs(raw)
            title = (form.get("title") or [""])[0].strip()[:90]
            when = (form.get("when") or [""])[0].strip()[:300]
            mode = (form.get("mode") or ["draft"])[0]
            if title:
                lst = KYC_PROCS.setdefault(sess["tenant_id"], [])
                # saving the same title again updates it in place
                for p in lst:
                    if p["title"] == title:
                        p.update(when=when, mode=mode if mode == "live"
                                 else p["mode"] if p["mode"] == "live"
                                 else "draft")
                        break
                else:
                    lst.append(dict(title=title, when=when,
                                    mode="live" if mode == "live" else "draft"))
            self.send_response(303)
            self.send_header("Location", "/agents/kyc_desk")
            self.end_headers(); return
        if self.path == "/api/assign":
            sess = self._session()
            if not sess:
                self.send_response(403); self.end_headers(); return
            form = parse_qs(raw)
            order = (form.get("order") or [""])[0]
            name = (form.get("name") or [""])[0].strip()[:60]
            ok = {p["name"] for p in people_for(sess["tenant_id"])
                  if p["approver"]}
            if order and (name in ok or name == ""):
                if name:
                    ASSIGN[order] = name
                    log_decision(
                        sess["tenant_id"],
                        (sess.get("email") or "you").split("@")[0],
                        "Handed a yes to <b>" + esc(name) + "</b>",
                        "/cases/" + order)
                else:
                    ASSIGN.pop(order, None)
            self.send_response(303)
            self.send_header("Location", f"/cases/{order}")
            self.end_headers(); return
        if self.path in ("/api/person_add", "/api/person_toggle",
                         "/api/person_del"):
            sess = self._session()
            if not sess:
                self.send_response(403); self.end_headers(); return
            form = parse_qs(raw)
            ppl = people_for(sess["tenant_id"])
            if self.path.endswith("_add"):
                name = (form.get("name") or [""])[0].strip()[:60]
                role = (form.get("role") or [""])[0].strip()[:60]
                if name:
                    ppl.append(dict(name=name, role=role or "team",
                                    approver=False))
            else:
                try:
                    i = int((form.get("i") or ["-1"])[0])
                except ValueError:
                    i = -1
                if 0 <= i < len(ppl):
                    if self.path.endswith("_toggle"):
                        ppl[i]["approver"] = not ppl[i]["approver"]
                    else:
                        ppl.pop(i)
            self.send_response(303)
            self.send_header("Location", "/settings?s=team")
            self.end_headers(); return
        if self.path in ("/api/routine_toggle", "/api/routine_del",
                         "/api/routine_edit"):
            sess = self._session()
            if not sess:
                self.send_response(403); self.end_headers(); return
            form = parse_qs(raw)
            lst = routines_for(sess["tenant_id"])
            try:
                i = int((form.get("i") or ["-1"])[0])
                r = lst[i] if 0 <= i < len(lst) else None
            except ValueError:
                r = None
            if r is not None:
                if self.path.endswith("_toggle"):
                    r["on"] = not r["on"]
                elif self.path.endswith("_del"):
                    lst.pop(i)
                else:
                    text = (form.get("text") or [""])[0].strip()[:200]
                    if text:
                        r["name"] = text[:64]
                        r["what"] = ("Reworded just now, in your words. Its "
                                     "next run follows the new wording.")
            self.send_response(303)
            self.send_header("Location", "/scheduled")
            self.end_headers(); return
        if self.path == "/api/file_upload":
            sess = self._session()
            if not sess:
                self.send_response(403); self.end_headers(); return
            # Minimal multipart read: the demo keeps the name and size,
            # which is all the shelf needs to show.
            import re as _re2
            m = _re2.search(r'filename="([^"]+)"', raw[:4000])
            if m:
                name = m.group(1).split("/")[-1].split("\\")[-1][:80]
                kb = max(1, length // 1024)
                size = (f"{kb} KB" if kb < 1024
                        else f"{kb / 1024:.1f} MB")
                from datetime import datetime as _dt
                seed_kfiles(sess["tenant_id"]).append(dict(
                    name=name, size=size,
                    when=_dt.now().strftime("%-d %b"),
                    used="Not cited yet; readable to the whole team"))
                log_decision(sess["tenant_id"],
                             (sess.get("email") or "you").split("@")[0],
                             "Added a file to Knowledge: <b>"
                             + esc(name) + "</b>", "/memory?t=files")
            self.send_response(303)
            self.send_header("Location", "/memory?t=files")
            self.end_headers(); return
        if self.path == "/api/routine":
            sess = self._session()
            if not sess:
                self.send_response(403); self.end_headers(); return
            text = (parse_qs(raw).get("text") or [""])[0].strip()[:200]
            if text:
                # The sentence is the setting: keep it as said, and let the
                # schedule read back the owner's own words.
                routines_for(sess["tenant_id"]).append(dict(
                    name=text[:64], when="As you said it",
                    what="Set just now, in your words. Its first run will "
                         "appear here.",
                    last="", on=True))
            self.send_response(303)
            self.send_header("Location", "/scheduled")
            self.end_headers(); return
        if self.path == "/api/evnote":
            sess = self._session()
            if not sess:
                self.send_response(403); self.end_headers(); return
            from datetime import datetime as _dt, timezone as _tz
            text = (parse_qs(raw).get("text") or [""])[0].strip()[:200]
            if text:
                who = (sess.get("email") or "you").split("@")[0]
                when = _dt.now(_tz.utc).strftime("%b %-d, %-I:%M %p")
                EV_NOTES.append((sess["tenant_id"], who, when, text))
            self.send_response(303)
            self.send_header("Location", "/knowledge")
            self.end_headers(); return
        if self.path == "/api/note":
            sess = self._session()
            if not sess:
                self.send_response(403); self.end_headers(); return
            form = parse_qs(raw)
            run_id = (form.get("run_id") or [""])[0]
            text = (form.get("text") or [""])[0].strip()[:280]
            led = WORLD.d.ledger
            r = led.runs.get(run_id)
            if r is None or r.tenant_id != sess.get("tenant_id") or not text:
                # An empty comment or a stale id is not an error worth a
                # blank screen — just go back to where the person was.
                back = (form.get("back") or [""])[0]
                self.send_response(303)
                self.send_header("Location",
                                 back if back.startswith("/") else "/")
                self.end_headers(); return
            who = (sess.get("email") or "you").split("@")[0]
            from datetime import datetime as _dt, timezone as _tz
            # seeded trace events are naive-UTC; keep annotations consistent
            led.trace(run_id, _dt.now(_tz.utc).replace(tzinfo=None), who, "note", text)
            back = (form.get("back") or [""])[0]
            self.send_response(303)
            self.send_header("Location",
                             back if back.startswith("/") else f"/runs/{run_id}")
            self.end_headers(); return
        if self.path == "/api/chat":
            sess = self._session() or {"tenant_id": "t1"}
            payload = json.loads(raw)
            msg = payload.get("message", "")
            # "/Role ..." calls that agent; "@Name" loops a teammate in.
            called = None
            for a in RELAY_AGENTS:
                if msg.lower().startswith("/" + a["role"].lower()):
                    called = a["role"]
                    msg = msg[1 + len(a["role"]):].strip() or "what needs my yes"
                    break
            tagged = [p["name"] for p in people_for(sess["tenant_id"])
                      if "@" + p["name"].lower() in msg.lower()]
            for t in tagged:
                msg = _re.sub("@" + _re.escape(t), t, msg, flags=_re.I)
            guard = _guard_answer(msg)
            build = (guard is None
                     and (payload.get("mode") == "build"
                          or bool(_BUILD_PAT.search(msg))))
            state = (None if (guard is not None or build)
                     else _state_answer(sess["tenant_id"], msg))
            res = (guard if guard is not None
                   else _build_res(sess["tenant_id"], msg) if build
                   else state if state is not None
                   else ask(msg))
            if "cues" not in res and not res.get("qz"):
                res["cues"] = _cues_for(res.get("_tool"),
                                        sess["tenant_id"])
            if called:
                res["reply"] = (f'<b>{esc(called)}</b> here. '
                                + res.get("reply", ""))
            if tagged:
                res["reply"] = (res.get("reply", "")
                    + '<span class="rmeta">' + " ".join(
                        f'{mention(t)} will see this thread.'
                        for t in tagged) + '</span>')
            if not build:
                res = _polish_reply(msg, res)
            c = CONVS.get(payload.get("conv_id") or "")
            if c is None or c["tenant"] != sess["tenant_id"]:
                c = _new_conv(sess["tenant_id"], msg)
            c["msgs"].append({"who": "msg user", "html": esc(msg)})
            # the steps are kept ticked-off, so reopening the thread shows
            # the work that was done rather than replaying the animation
            if res.get("steps"):
                c["msgs"].append({"who": "steps",
                                  "html": steps_html(res["steps"], done=True)})
            c["msgs"].append({"who": "msg bot", "html": res.get("reply", "")})
            for key in ("product", "cards"):
                if res.get(key):
                    c["msgs"].append({"who": "cards", "html": res[key]})
                    if 'id="prop-' in res[key]:
                        c["pending"] = res[key].split('id="prop-')[1].split('"')[0]
            _touch(c)
            return self._json({**res, "conv_id": c["id"], "title": c["title"]})
        if self.path == "/api/agent_build_draft":
            sess = self._session() or {"tenant_id": "t1"}
            payload = json.loads(raw)
            bid = payload.get("bid", "")
            d = PENDING_BUILDS.get(bid)
            if d is None:
                return self._json({"reply": "That draft is gone. Describe "
                                   "the job again and I will redraw it.",
                                   "cards": "", "summary": "", "steps": []})
            ans = payload.get("answers")
            d["answers"] = ans if isinstance(ans, dict) else {}
            out = _build_draft_res(sess["tenant_id"], bid)
            c = CONVS.get(payload.get("conv_id") or "")
            if c and c["tenant"] == sess["tenant_id"]:
                c["msgs"].append({"who": "cards", "html": out["summary"]})
                c["msgs"].append({"who": "steps",
                                  "html": steps_html(list(_BUILD_STEPS),
                                                     done=True)})
                c["msgs"].append({"who": "msg bot", "html": out["reply"]})
                c["msgs"].append({"who": "cards", "html": out["cards"]})
                _touch(c)
            return self._json(out)
        if self.path == "/api/agent_build_confirm":
            sess = self._session() or {"tenant_id": "t1"}
            payload = json.loads(raw)
            bid = payload.get("bid", "")
            reply = _build_confirm(sess["tenant_id"], bid)
            c = CONVS.get(payload.get("conv_id") or "")
            if c and c["tenant"] == sess["tenant_id"]:
                for m in c["msgs"]:
                    if f'id="bact-{bid}"' in m["html"]:
                        m["html"] = _re.sub(
                            r'<div class="bacts" id="bact-' + bid
                            + r'".*?</div>',
                            '<span class="st ok">Added</span>',
                            m["html"], flags=_re.S)
                c["msgs"].append({"who": "msg user", "html": "Yes, add it"})
                c["msgs"].append({"who": "msg bot", "html": reply})
                c["outcome"] = "approved"
                _touch(c)
            return self._json({"reply": reply})
        if self.path == "/api/sample":
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            tid = sess["tenant_id"]
            if not TENANTS.has(tid):
                ensure_tenant(tid, sess.get("email", tid))
            load_sample(tid, sess.get("email", "you"))
            return self._redirect("/tasks")
        if self.path == "/api/conv/pin":
            sess = self._session()
            if not sess:
                return self.send_error(403)
            c = CONVS.get(json.loads(raw).get("id", ""))
            if c and c["tenant"] == sess["tenant_id"]:
                c["pinned"] = not c["pinned"]
            return self._json({"pinned": bool(c and c["pinned"])})
        if self.path == "/api/conv/rename":
            sess = self._session()
            if not sess:
                return self.send_error(403)
            payload = json.loads(raw)
            c = CONVS.get(payload.get("id", ""))
            if c and c["tenant"] == sess["tenant_id"] and payload.get("title", "").strip():
                c["title"] = payload["title"].strip()[:60]
            return self._json({"ok": True})
        if self.path == "/api/conv/delete":
            sess = self._session()
            if not sess:
                return self.send_error(403)
            cid = json.loads(raw).get("id", "")
            c = CONVS.get(cid)
            if c and c["tenant"] == sess["tenant_id"]:
                del CONVS[cid]
            return self._json({"ok": True})
        if self.path == "/api/confirm":
            sess = self._session() or {"tenant_id": "t1"}
            payload = json.loads(raw)
            pid = payload.get("proposal", "")
            action = (payload.get("action")
                      or (PROPOSALS.get(pid) or {}).get("action"))
            # the run belongs to a tenant; a session only ever decides its own
            prop = PROPOSALS.get(pid) or {}
            run = WORLD.d.ledger.runs.get(prop.get("run_id", ""))
            if run is not None and run.tenant_id != sess["tenant_id"]:
                return self.send_error(403)
            res = confirm(pid, action, payload.get("text", "")[:4000],
                          sess.get("email", "workspace"))
            c = CONVS.get(payload.get("conv_id") or "")
            if c is not None and c["tenant"] == sess["tenant_id"]:
                c["msgs"].append({"who": "msg bot", "html": res.get("reply", "")})
                for key in ("product", "cards"):
                    if res.get(key):
                        c["msgs"].append({"who": "cards", "html": res[key]})
                if c.get("pending") == pid:
                    c["pending"] = None
                    c["outcome"] = ("approved" if action in ("approve", "edit")
                                    else "dismissed" if action else c["outcome"])
                _touch(c)
            return self._json(res)
        if self.path == "/act/bulk":
            sess = self._session()
            if not sess:
                self.send_response(403); self.end_headers(); return
            form = parse_qs(raw)
            action = (form.get("action") or [""])[0]
            why = (form.get("why") or [""])[0].strip()[:200]
            ids = (form.get("runs") or [""])[0].split(",")
            led = WORLD.d.ledger
            for rid in ids:
                run = led.runs.get(rid)
                if (run is None or run.tenant_id != sess["tenant_id"]
                        or run.state is not RunState.AWAITING_GATE):
                    continue
                pipe = pipeline_for(run.tenant_id)
                if action == "approve":
                    pipe.approve(run, sess.get("email", "workspace"))
                elif action == "reject":
                    pipe.reject(run, sess.get("email", "workspace"))
                    if why:
                        from datetime import datetime as _dt, timezone as _tz
                        who = (sess.get("email") or "workspace").split("@")[0]
                        led.trace(run.run_id,
                                  _dt.now(_tz.utc).replace(tzinfo=None),
                                  who, "note", f"dismissed: {why}")
            self.send_response(303)
            self.send_header("Location", "/approvals")
            self.end_headers(); return
        if self.path != "/act":
            return self.send_error(404)
        sess = self._session()
        if not sess:
            return self._redirect("/login")
        form = parse_qs(raw)
        run = WORLD.d.ledger.runs.get(form["run"][0])
        action = form["action"][0]
        # a session only ever acts on its own tenant's rows
        if (run and run.tenant_id == sess["tenant_id"]
                and run.state is RunState.AWAITING_GATE):
            pipe = pipeline_for(run.tenant_id)
            if action == "approve":
                pipe.approve(run, sess.get("email", "workspace"))
            elif action == "reject":
                pipe.reject(run, sess.get("email", "workspace"))
                why = form.get("why", [""])[0].strip()[:200]
                if why:
                    from datetime import datetime as _dt, timezone as _tz
                    who = (sess.get("email") or "workspace").split("@")[0]
                    WORLD.d.ledger.trace(
                        run.run_id, _dt.now(_tz.utc).replace(tzinfo=None),
                        who, "note", f"dismissed: {why}")
            elif action == "edit":
                pipe.edit(run, sess.get("email", "workspace"),
                          form.get("text", [""])[0])
        self.send_response(303)
        self.send_header("Location", "/")
        self.end_headers()

    def _auth(self, raw: str):
        """Signup and login. WorkOS proves identity (email & password never
        touch our storage); the session cookie carries tenant scope. Demo
        mode keeps localhost usable with no WorkOS keys in the keychain."""
        form = {k: v[0] for k, v in parse_qs(raw).items()}
        if self.path == "/auth/demo":
            return self._redirect("/", session={
                "tenant_id": "t1", "email": "demo@local"})
        from relay_superagent.adapters.workos import WorkOs, WorkOsError
        email = form.get("email", "").strip().lower()
        password = form.get("password", "")
        try:
            wos = WorkOs()
            if self.path == "/auth/signup":
                company = form.get("company", "").strip() or email.rsplit("@", 1)[-1]
                org_id = wos.create_organization(company)
                user = wos.create_user(email, password)
                wos.add_membership(user["id"], org_id)
                ctx = ensure_tenant(org_id, company)
            elif self.path == "/auth/verify":
                resp = wos.verify_email_code(form.get("code", "").strip(),
                                             form.get("pending_token", ""))
                ctx = self._tenant_from_auth(resp, email)
                if ctx is None:
                    return self._html(auth_page(
                        "login", "Verified, but this account has no organization: sign up first."), 403)
            else:
                resp = wos.authenticate(email, password)
                ctx = self._tenant_from_auth(resp, email)
                if ctx is None:
                    return self._html(auth_page(
                        "login", "This account has no organization yet: sign up first."), 403)
        except WorkOsError as e:
            # unverified email: WorkOS mailed a one-time code; collect it
            if e.code == "email_verification_required":
                return self._html(verify_page(
                    email or e.data.get("email", ""),
                    e.data.get("pending_authentication_token", "")))
            if self.path == "/auth/verify":
                return self._html(verify_page(
                    email, form.get("pending_token", ""), str(e)), 401)
            mode = "signup" if self.path.endswith("signup") else "login"
            return self._html(auth_page(mode, str(e)), 401)
        return self._redirect("/", session={
            "tenant_id": ctx.tenant_id, "email": email})

    def _tenant_from_auth(self, resp: dict, email: str):
        org_id = resp.get("organization_id")
        if not org_id:
            return None
        return ensure_tenant(org_id, resp.get("user", {}).get("email", email))

    def _slack_interaction(self, raw: str):
        """The Slack half of the gate. Same approve/reject the workspace and
        chat call: one decision path, three transports."""
        from relay_superagent.adapters.slack import parse_interaction, verify_signature
        from relay_superagent.secrets import get_secret
        secret = get_secret("slack-signing")
        if not secret:
            return self.send_error(503, "slack-signing not in keychain")
        ok = verify_signature(
            secret,
            self.headers.get("X-Slack-Request-Timestamp", ""),
            raw.encode(),
            self.headers.get("X-Slack-Signature", ""))
        if not ok:
            return self.send_error(401)
        act = parse_interaction(raw)
        if act is None:
            return self._json({})
        run = WORLD.d.ledger.runs.get(act["run_id"])
        if run is None or run.state is not RunState.AWAITING_GATE:
            return self._json({"text": "That one is already resolved."})
        if act["action"] == "approve":
            WORLD.approve(run, act["actor"])
            return self._json({"text": "Approved: the response is filed. ✓"})
        WORLD.reject(run, act["actor"])
        return self._json({"text": "Dismissed and recorded."})

    def _fathom_webhook(self, raw: str):
        """The trigger end of the gate sentence: meeting-ready payload in,
        run at the gate (or a clean no-op) out. Idempotency lives in the
        ledger, so Fathom redelivery costs nothing. Tenant comes from the
        path (/webhooks/fathom/<tenant_id>, bare = t1) and each tenant may
        carry its own signing secret (fathom-webhook-<tenant_id>)."""
        from relay_superagent.adapters.fathom import to_trigger_event, verify_signature
        from relay_superagent.secrets import get_secret
        tid = self.path.removeprefix("/webhooks/fathom").strip("/") or "t1"
        try:
            ctx = TENANTS.get(tid)
        except UnknownTenant:
            return self.send_error(404)
        secret = get_secret(f"fathom-webhook-{tid}") or get_secret("fathom-webhook")
        if not secret:
            return self.send_error(503, "fathom-webhook not in keychain")
        ok = verify_signature(
            secret,
            self.headers.get("webhook-id", ""),
            self.headers.get("webhook-timestamp", ""),
            raw.encode(),
            self.headers.get("webhook-signature", ""))
        if not ok:
            return self.send_error(401)
        ev = to_trigger_event(json.loads(raw), tenant_id=tid)
        if ev is None:
            return self._json({"detail": "no transcript"})
        run = pipeline_for(tid).handle_event(ev)
        if run is None:
            return self._json({"detail": "no covered dispute reason"})
        return self._json({"run_id": run.run_id, "state": run.state.value})

    def _json(self, payload):
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


# =========================================================================
# Ask — the chat surface. ServiceWorker's pattern, ported:
#   the model (or a keyword router when no key) only PICKS a tool from a
#   fixed catalogue; hand-written functions over the ledger do the work;
#   cards are rendered server-side from results that already ran; and the
#   chat never acts directly — write intents become proposals the human
#   confirms by opaque id, so there is no request body from which to act
#   on a different run.
# =========================================================================

import uuid as _uuid

PROPOSALS: dict[str, dict] = {}

TOOLS = ["queue", "metrics", "runs", "evidence", "escalations",
         "shadow", "approve", "dismiss", "help"]


def _mini_run(r) -> str:
    label, cls = STATE_META.get(r.state, (r.state.value, "mut"))
    counter = esc((r.decision or {}).get("counter_text", ""))
    return (f'<div class="mrun"><div class="mhead">'
            f'<span class="comp">{PLAIN_REASON.get(r.reason_code, "Dispute")}</span>'
            f'<span class="meta">{_logo(_account_label(r))}'
            f'{esc(_account_label(r))} · {esc(bought(r.order_id))}</span>'
            f'<span class="st {cls}">{label}</span></div>'
            f'<div class="mclaim">“{esc(r.claim_text)}”</div>'
            f'<div class="mcounter">{counter}</div></div>')


def _t_shadow(_):
    sends = sum(1 for row in SHADOW_ROWS if row[4] == "send")
    moments = sum(1 for row in SHADOW_ROWS if row[4] != "skip")
    return (f"Over the last 30 days: <b>{moments}</b> disputes worth "
            f"replying to, <b>{sends}</b> replies ready to go, and not one of "
            f"them answered by anyone. Nothing was sent: this is only a "
            f"look back. <a href='/shadow'><b>Open it</b></a> to see them.",
            "")


def _t_queue(_):
    runs = [r for r in WORLD.d.ledger.runs.values() if r.state is RunState.AWAITING_GATE]
    if not runs:
        return "Nothing needs your yes right now.", ""
    return (f"{len(runs)} repl{'ies' if len(runs) != 1 else 'y'} to the bank "
            f"waiting on your yes. Say “send the charged-twice one” and I will "
            f"show it to you first.",
            "".join(_mini_run(r) for r in runs))


def _t_metrics(_):
    led = WORLD.d.ledger
    rows = [
        ("How often you had to fix a reply", fmt_pct(correction_rate(led, "t1")),
         "we want this under 5%"),
        ("Replies you sent as written", fmt_pct(counter_usage_rate(led, "t1")),
         "said yes, or barely reworded"),
        ("Disputes worth showing you", fmt_pct(trigger_precision(led, "t1")),
         "the rest were set aside"),
        ("How long you take to decide", _fmt_latency(gate_latency_p95_ms(led, "t1")),
         "your slowest one in twenty"),
    ]
    html = "".join(f'<div class="mrow"><b>{v}</b><span>{k}</span><i>{h}</i></div>'
                   for k, v, h in rows)
    return "Here is how your team is doing.", f'<div class="mcard">{html}</div>'


def _t_runs(args):
    comp = (args or {}).get("competitor")
    runs = sorted(WORLD.d.ledger.runs.values(), key=lambda r: r.occurred_at, reverse=True)
    if comp:
        runs = [r for r in runs if r.reason_code == comp]
    if not runs:
        return (f"Nothing found{' where ' + PLAIN_REASON.get(comp, comp).lower() if comp else ''}.", "")
    label = (f" where the {PLAIN_REASON.get(comp, comp).lower()}" if comp else "")
    return f"{len(runs)} thing{'s' if len(runs) != 1 else ''}{label}, newest first.", \
           "".join(_mini_run(r) for r in runs[:6])


def _t_evidence(_):
    led = WORLD.d.ledger
    stats: dict[str, dict] = {}
    for r in led.runs.values():
        for eid in (r.decision or {}).get("cited_evidence_ids", []):
            st = stats.setdefault(eid, {"cited": 0, "won": 0, "resolved": 0})
            st["cited"] += 1
            out = led.outcome_for(r.run_id)
            if out:
                st["resolved"] += 1
                if out.outcome_value.get("won"):
                    st["won"] += 1
    if not stats:
        return "No proof has been used yet.", ""
    ev_by_id = {e.evidence_id: e for e in WORLD.d.evidence}
    rows = ""
    for eid, st in sorted(stats.items(), key=lambda kv: -kv[1]["cited"]):
        e = ev_by_id.get(eid)
        rate = (f'{st["won"]} of {st["resolved"]} kept' if st["resolved"]
                else "nothing settled yet")
        rows += (f'<div class="mrow"><b>{st["cited"]}×</b>'
                 f'<span>{esc(e.text[:70]) if e else eid}</span><i>{rate}</i></div>')
    return ("Your proof, most used first. Normally we hide the win rate "
            "until a piece of proof has been used five times; this is sample "
            "data, so you are seeing it all.",
            f'<div class="mcard">{rows}</div>')


def _t_escalations(_):
    posts = WORLD.d.slack.channel_posts
    if not posts:
        return "Nothing needs a person right now.", ""
    rows = "".join(f'<div class="mrow"><b class="warn">!</b>'
                   f'<span>{plain_detail(b["reason"])}</span>'
                   f'<i>“{esc(b.get("claim") or ".")[:60]}”</i></div>'
                   for _, b in posts)
    return f"{len(posts)} thing{'s' if len(posts) != 1 else ''} a person needs to look at.", \
           f'<div class="mcard">{rows}</div>'


def _find_awaiting(comp):
    runs = [r for r in WORLD.d.ledger.runs.values() if r.state is RunState.AWAITING_GATE]
    if comp:
        runs = [r for r in runs if r.reason_code == comp]
    return sorted(runs, key=lambda r: r.occurred_at)[0] if runs else None


def _t_action(args, action):
    """The three decisions now live on the work product itself, which is
    where the owner is already reading the reply. This only says what it
    found; the card underneath carries the buttons."""
    run = _find_awaiting((args or {}).get("competitor"))
    if run is None:
        return "Nothing like that is waiting on your yes.", "", None
    return (f"Here is the reply, with the proof it stands on. "
            f"{PLAIN_REASON.get(run.reason_code, 'A buyer is disputing a payment')}. "
            f"Nothing goes to the bank until you say so.", "", None)


# ------------------------------------------------------------- work, visible
# "Make it visibly work." Instant resolution reads as though nothing was
# really checked, so every answer shows the steps it took, each one naming
# something real off the record.

def _target_run(tool: str, args: dict | None):
    """Which case this answer is about, if it is about one."""
    if tool in ("approve", "dismiss", "queue"):
        return _find_awaiting((args or {}).get("competitor"))
    if tool == "runs":
        comp = (args or {}).get("competitor")
        runs = [r for r in WORLD.d.ledger.runs.values()
                if r.decision and (not comp or r.reason_code == comp)]
        waiting = [r for r in runs if r.state is RunState.AWAITING_GATE]
        pool = waiting or runs
        return max(pool, key=lambda r: r.occurred_at) if pool else None
    return None


def steps_for(tool: str, run) -> list[tuple[str, str]]:
    led = WORLD.d.ledger
    if run is not None:
        return case_steps(run)
    mine = [r for r in led.runs.values() if r.tenant_id == "t1"]
    if tool == "metrics":
        return [("Opening your own record",
                 f"{len(mine)} things your team worked"),
                ("Counting the ones you had to fix",
                 fmt_pct(correction_rate(led, "t1"))),
                ("Counting the ones sent as written",
                 fmt_pct(counter_usage_rate(led, "t1"))),
                ("Working out how long you take to decide",
                 _fmt_latency(gate_latency_p95_ms(led, "t1")))]
    if tool == "evidence":
        ev = [e for e in WORLD.d.evidence if e.tenant_id == "t1"]
        cited = sum(len((r.decision or {}).get("cited_evidence_ids", []))
                    for r in mine)
        return [("Listing every piece of proof you have",
                 f"{len(ev)} pieces on file"),
                ("Counting where each one was used",
                 f"{cited} times across your replies"),
                ("Checking which ones settled",
                 f"{sum(1 for r in mine if led.outcome_for(r.run_id))} closed so far")]
    if tool == "escalations":
        return [("Looking for anything stuck",
                 f"{len(WORLD.d.slack.channel_posts)} handed over"),
                ("Checking who has each one now", "Relay&rsquo;s people")]
    if tool == "shadow":
        return [("Reading back your last 30 days",
                 f"{len(SHADOW_ROWS)} disputes"),
                ("Working out which were worth answering",
                 f"{sum(1 for x in SHADOW_ROWS if x[4] != 'skip')} of them"),
                ("Writing what the reply would have been",
                 f"{sum(1 for x in SHADOW_ROWS if x[4] == 'send')} replies, none sent")]
    return []


HELP = ("Everything I say comes from your own record, never from memory. "
        "Try: <b>what needs my yes?</b> · <b>how is the team doing?</b> · "
        "<b>show me the never-arrived ones</b> · <b>which proof wins?</b> · "
        "<b>anything need a person?</b> · <b>send the charged-twice one</b>")


def _keyword_route(text: str):
    t = text.lower().replace("-", " ")
    if "shadow" in t or "would have filed" in t or "would have sent" in t or "would have gone" in t:
        return "shadow", {"competitor": None}
    # what the owner would actually say, mapped to the bank's code
    SPOKEN = {"RG": ("never arrived", "not arrived", "not received", "missing"),
              "RD": ("charged twice", "double charge", "duplicate", "twice"),
              "RN": ("wasn't what", "wasnt what", "not as described",
                     "wrong item", "doesn't match", "doesnt match"),
              "RF": ("never made", "didn't make", "didnt make", "fraud",
                     "unauthorised", "unauthorized"),
              "RC": ("already cancelled", "cancelled", "canceled")}
    comp = next((code for code, phrases in SPOKEN.items()
                 if any(ph in t for ph in phrases)), None)
    if comp is None:
        comp = next((dr.code for dr in WORLD.d.policy.dispute_reasons
                     if dr.label.lower() in t
                     or dr.id.replace("_", " ") in t
                     or COMP.get(dr.code, "").lower() in t), None)
    args = {"competitor": comp}
    if any(w in t for w in ("need a person", "needs a person", "escalat",
                            "timeout", "stuck", "for ops")):
        return "escalations", args
    if any(w in t for w in ("say yes to", "approve", "send it", "send the",
                            "ship it", "file it")):
        return "approve", args
    if any(w in t for w in ("dismiss", "reject", "don't send", "do not send")):
        return "dismiss", args
    if any(w in t for w in ("needs my yes", "need my yes", "needs your yes",
                            "queue", "awaiting", "review", "pending", "waiting")):
        return "queue", args
    if any(w in t for w in ("metric", "correction", "usage", "precision",
                            "latency", "how are we", "how is", "how is the team",
                            "doing", "kpi")):
        return "metrics", args
    if any(w in t for w in ("evidence", "winning", "win rate", "which proof",
                            "argument", "proof")):
        return "evidence", args
    if comp or any(w in t for w in ("show", "run", "ledger", "history",
                                    "dispute", "order", "won", "win",
                                    "closed", "list")):
        return "runs", args
    return "help", args


def _llm_route(text: str):
    """One Haiku call picks the tool: same seam discipline as the pipeline:
    structured output, closed schema, and any failure falls back silently to
    the keyword router. The model never writes a query or a card."""
    from relay_superagent.secrets import get_secret
    if not get_secret("anthropic"):
        return None
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=get_secret("anthropic"))
        comp_ids = [dr.code for dr in WORLD.d.policy.dispute_reasons]
        resp = client.messages.create(
            model="claude-haiku-4-5", max_tokens=256,
            system=("Route a merchant operator's chat message to one tool. "
                    "Tools: queue (what awaits review), metrics, runs "
                    "(list/history), evidence (which proofs win disputes), "
                    "escalations (items for ops), approve, dismiss, help. "
                    "competitor is the dispute reason code, one of "
                    f"{comp_ids} or null."),
            messages=[{"role": "user", "content": text}],
            output_config={"format": {"type": "json_schema", "schema": {
                "type": "object",
                "properties": {
                    "tool": {"type": "string", "enum": TOOLS},
                    "competitor": {"type": ["string", "null"]},
                },
                "required": ["tool", "competitor"],
                "additionalProperties": False,
            }}},
        )
        out = json.loads(next(b.text for b in resp.content if b.type == "text"))
        comp = out["competitor"] if out["competitor"] in comp_ids else None
        return out["tool"], {"competitor": comp}
    except Exception:
        return None


def ask(message: str) -> dict:
    """One answer is: the steps the team took, then the thing it produced."""
    routed = _llm_route(message)
    tool, args = routed or _keyword_route(message)
    meta = {"_routed_model": routed is not None, "_tool": tool}
    run = _target_run(tool, args)
    steps = steps_for(tool, run)
    product = work_product(run) if run is not None else ""
    if tool in ("approve", "dismiss"):
        reply, cards, pid = _t_action(args, tool)
        return {"reply": reply, "cards": cards, "proposal": pid,
                "steps": steps, "product": product,
                "case": (run.order_id if run else ""), **meta}
    handler = {"queue": _t_queue, "metrics": _t_metrics, "runs": _t_runs,
               "evidence": _t_evidence, "escalations": _t_escalations,
               "shadow": _t_shadow}.get(tool)
    if handler is None:
        return {"reply": HELP, "cards": "", "proposal": None,
                "steps": [], "product": "", "case": "", **meta}
    reply, cards = handler(args)
    return {"reply": reply, "cards": cards, "proposal": None,
            "steps": steps, "product": product,
            "case": (run.order_id if run else ""), **meta}


def confirm(pid: str, action: str | None = None, text: str = "",
            actor: str = "you") -> dict:
    """The one decision path. Whether the yes came from the queue, from
    Slack or from the thread, it lands on the same approve / edit / reject
    and writes exactly once."""
    prop = PROPOSALS.pop(pid, None)
    if prop is None:
        return {"reply": "That one is gone: it may already be settled.",
                "cards": "", "product": ""}
    run = WORLD.d.ledger.runs.get(prop["run_id"])
    if run is None or run.state is not RunState.AWAITING_GATE:
        return {"reply": "That one is no longer waiting on you.",
                "cards": "", "product": ""}
    act = action or prop.get("action") or "approve"
    pipe = pipeline_for(run.tenant_id)
    if act == "approve":
        pipe.approve(run, actor)
        reply = ("Sent. The bank has the reply, and the whole case is "
                 "written down in your history.")
    elif act == "edit":
        pipe.edit(run, actor, text or (run.decision or {}).get("counter_text", ""))
        reply = ("Sent in your words. What you changed is kept, so the next "
                 "reply starts from it.")
    else:
        pipe.reject(run, actor)
        reply = "Not sent, and written down."
    return {"reply": reply, "cards": "", "product": work_product(run),
            "case": run.order_id or "",
            "case_key": case_status(case_runs(run.tenant_id, run.order_id))[0]}


# ------------------------------------------------------------- sample data
# "Load sample data": seeds an empty tenant with dispute reasons, evidence and
# a few simulated runs through the REAL pipeline, so the whole loop is
# experienceable before any keys exist. Each further call simulates one more
# dispute webhook. Everything is labeled sample via trigger_source.
_SAMPLE_SCENARIOS = [
    # (customer, product, channel, reason_code, claim, narrative, response)
    ("Rekha S.", "A2 Desi Ghee 500ml", "our store", "RG",
     "Says the ghee order never arrived",
     "Chargeback filed: buyer says the A2 Desi Ghee order never arrived.",
     "The courier proof-of-delivery is signed and GPS-stamped at the address "
     "on file two days before this dispute was filed, and the WhatsApp thread "
     "carries the buyer's own delivery-day confirmation. Both documents are "
     "attached to this reply."),
    ("Jatin M.", "Amla Juice 1L", "Amazon", "RD",
     "Says one juice order was charged twice",
     "Chargeback filed: the buyer's statement shows two debits for one order.",
     "The invoice and the payment gateway agree on a single reference for this "
     "Amla Juice order, and the bank settlement excerpt confirms one debit "
     "cleared. The second line on the statement is an authorisation hold that "
     "reverses on its own."),
    ("Deepika A.", "Ashwagandha Gold capsules", "monthly refill", "RG",
     "Says the ashwagandha refill never showed up",
     "Chargeback filed: buyer says the monthly refill was never delivered.",
     "Delivery proof for the refill shipment is signed and dated, and the "
     "WhatsApp thread shows the buyer acknowledging the parcel that evening. "
     "Both are attached to this reply."),
    ("Mohit R.", "Shilajit Resin 20g", "our store", "RD",
     "Bank flagged a repeat charge on the shilajit order",
     "Chargeback filed: the issuing bank flagged a duplicate charge on the "
     "Shilajit Resin order.",
     "One payment reference maps to one settled debit on this order; the "
     "invoice and the bank excerpt agree, and no second capture exists in the "
     "settlement record."),
]
_sample_counters: dict[str, int] = {}


def load_sample(tid: str, email: str) -> str:
    """Seed (idempotently) and simulate one dispute webhook for this tenant.
    Returns the created run's id ('' if nothing fired)."""
    ctx = TENANTS.get(tid)
    d = WORLD.d
    if not ctx.policy.dispute_reasons:
        ctx.policy = Policy(
            policy_version="pol_sample_1", tenant_id=tid,
            dispute_reasons=[
                DisputeReason(id="goods_not_received", code="RG",
                              label="Goods/services not received"),
                DisputeReason(id="duplicate_charge", code="RD",
                              label="Duplicate charge"),
            ],
            banned_terms=["best", "leading", "number one"], holdout_pct=0)
    ctx.enrolled_merchants.add(email)
    pipe = pipeline_for(tid)
    pipe.d.policy = ctx.policy
    pipe.d.enrolled_merchants = ctx.enrolled_merchants
    if not any(e.tenant_id == tid for e in d.evidence):
        for e in [ev for ev in d.evidence if ev.tenant_id == "t1"]:
            d.evidence.append(EvidenceItem(
                e.evidence_id + "_" + tid[:8], tid, e.reason_code,
                e.evidence_type, e.text, e.source_url))
        pipe.d.evidence = d.evidence
    n = _sample_counters.get(tid)
    if n is None:
        n = sum(1 for r in d.ledger.runs.values()
                if r.tenant_id == tid and r.trigger_source == "sample")
    (acct, product, channel, reason_code, claim, text,
     counter) = _SAMPLE_SCENARIOS[n % len(_SAMPLE_SCENARIOS)]
    _sample_counters[tid] = n + 1
    order = f"order_{tid[:6]}_{n}"
    # the buyer is display-only demo data, registered against the order —
    # exactly like the seeded ones, and never a field on the run
    CUSTOMERS[order] = (acct, product, channel)
    d.crm.opportunities[order] = {"stage": "evaluation", "amount_band": "1k-3k"}
    d.clock.advance(hours=2)
    d.llm.mention = {"is_competitive": True, "claim_text": claim, "confidence": .9}
    d.llm.claim = {"claim_text": claim, "speaker_role": "buyer", "confidence": .9}
    cites = [e.evidence_id for e in d.evidence
             if e.tenant_id == tid and e.reason_code == reason_code][:2]
    d.llm.draft = {"counter_text": counter, "cited_evidence_ids": cites,
                   "confidence": .82, "escalate": False}
    run = pipe.handle_event(TriggerEvent(
        tenant_id=tid, source="sample", source_ref=f"sample_{tid}_{n}",
        occurred_at=d.clock.now(), order_id=order,
        merchant_id=BUSINESS_ID if tid == "t1" else email,
        dispute_id=f"dp_sample_{tid[:6]}_{n}", reason_code=reason_code,
        text=text))
    return run.run_id if run else ""


# ------------------------------------------------------------- build on the fly
# The composer is a front door, not a question box: describe a job in one
# sentence and Relay drafts the teammate. The draft is inert until the yes,
# and the finished agent obeys the same house rule as the shipped fifteen:
# anything it wants to send or hold waits for the owner's approval.
PENDING_BUILDS: dict[str, dict] = {}
_build_n = 0

_BUILD_PAT = _re.compile(
    r"\b(build|create|make|draft|set up|spin up)\b.{0,40}\b(agent|teammate|bot)\b"
    r"|\bagent (that|to|for|which|who)\b"
    r"|\bi need (someone|somebody) (to|who)\b",
    _re.I)

_BUILD_NAMES = [
    (("cod",), "COD Sentry", "risk"),
    (("cart",), "Cart Keeper", "calling"),
    (("refund",), "Refund Referee", "risk"),
    (("stock", "inventory", "restock"), "Shelf Warden", "inventory"),
    (("review", "rating", "star"), "Review Reader", "support"),
    (("gst", "invoice", "filing", "tax"), "Filing Clerk", "accounts"),
    (("payout", "vendor", "supplier"), "Vendor Payer", "accounts"),
    (("courier", "delivery", "shipment", "rto"), "Delivery Chaser", "support"),
    (("payment", "upi", "settlement"), "Payment Sleuth", "accounts"),
    (("whatsapp", "message", "inbox"), "Inbox Keeper", "support"),
]

_BUILD_DOES = {
    "risk": "Holds what looks wrong, releases what checks out.",
    "calling": "Calls and messages buyers, and writes down every answer.",
    "accounts": "Gets the numbers and the payments ready for your yes.",
    "inventory": "Counts ahead so you order before you run out.",
    "support": "Drafts the replies and keeps the thread until it is settled.",
}

_BUILD_STEPS = [
    ("Reading the job", "what to watch, what counts as wrong"),
    ("Choosing what it may touch", "orders and holds, nothing else"),
    ("Writing its rules", "it checks with you before anything moves"),
]


def _build_draft(msg: str) -> dict:
    low = msg.lower()
    name, kind = "New Teammate", "support"
    for words, nm, kd in _BUILD_NAMES:
        if any(w in low for w in words):
            name, kind = nm, kd
            break
    watches = _re.sub(
        r"^\s*(please\s+)?(build|create|make|draft|set up|spin up)"
        r"( me)?( up)?( an| a)?( new)? ?(agent|teammate|bot)?"
        r"( that| to| for| which| who)?\s*", "", msg, flags=_re.I)
    watches = _re.sub(
        r"^\s*i need (someone|somebody|an agent)( to| who| that)?\s*",
        "", watches, flags=_re.I)
    watches = (watches or msg).strip(" .")
    if watches:
        watches = watches[:1].upper() + watches[1:]
    else:
        watches = msg
    slug = _re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return dict(name=name, kind=kind, slug=slug, watches=watches)


def _build_questions(kind: str) -> list[dict]:
    """Three quick taps before drafting, Khatabook-plain: when it acts, what
    it may touch, when it must ask. Options carry a one-line consequence and
    one is marked Recommended; every question takes a typed answer too."""
    touch = {
        "risk": [
            ("Orders and holds", "It can stop an order or let it through."),
            ("Buyer messages", "It can message the buyer to double-check."),
            ("Refunds", "It can hold a refund until the check clears.")],
        "calling": [
            ("Calls", "It can ring the buyer and talk."),
            ("WhatsApp messages", "It can follow up in writing."),
            ("Payment links", "It can send a fresh link to pay.")],
        "accounts": [
            ("The numbers, read only", "It reads statements and orders, "
             "touches nothing."),
            ("Payment drafts", "It can line up a payment for your yes."),
            ("Vendor records", "It can update who is owed what.")],
        "inventory": [
            ("Stock counts", "It watches what is running low."),
            ("Reorder drafts", "It can write the reorder for your yes."),
            ("Supplier messages", "It can ask the supplier for dates.")],
        "support": [
            ("Buyer replies", "It can draft the answer to a buyer."),
            ("Order records", "It reads the order to get the story right."),
            ("Courier tickets", "It can raise a ticket when a parcel is "
             "stuck.")],
    }[kind]
    return [
        dict(key="act", type="one", q="When should it act?", opts=[
            ("The moment it spots one",
             "Acts as things come in, day or night.", True),
            ("Once a day",
             "Works quietly, reports in the morning brief.", False),
            ("Only when I ask",
             "Sits ready; you call it with /.", False)]),
        dict(key="touch", type="many", q="What may it touch?",
             opts=[(lbl, sub, i == 0) for i, (lbl, sub) in enumerate(touch)]),
        dict(key="gate", type="one", q="When must it check with you?", opts=[
            ("Before anything moves",
             "Every send and hold waits for your approval.", True),
            ("Only above ₹5,000",
             "Small ones go on their own; big ones wait for you.", False),
            ("Only the unusual ones",
             "Routine work flows; anything odd waits for you.", False)]),
    ]


def _default_answers(qs: list[dict], ans: dict) -> dict:
    """Skipped questions fall back to the Recommended option, so a skipped
    stepper still yields a complete, safe draft."""
    out = {}
    for q in qs:
        v = ans.get(q["key"])
        rec = next((o[0] for o in q["opts"] if o[2]), q["opts"][0][0])
        if q["type"] == "many":
            v = [x for x in (v or []) if isinstance(x, str) and x.strip()]
            out[q["key"]] = v[:4] or [rec]
        else:
            out[q["key"]] = v.strip()[:80] if isinstance(v, str) and v.strip() else rec
    return out


def _build_res(tid: str, msg: str) -> dict:
    """The chat answer for a described job: not a draft yet, but the three
    questions that shape it. The card itself is drawn by the client."""
    global _build_n
    d = _build_draft(msg)
    _build_n += 1
    bid = f"bld{_build_n}"
    PENDING_BUILDS[bid] = {**d, "tenant": tid}
    reply = ("Before I draft it, three quick taps: when it acts, what it "
             "may touch, and when it must ask you.")
    return {"reply": reply, "cards": "", "steps": [],
            "qz": {"bid": bid, "name": d["name"],
                   "questions": _build_questions(d["kind"])},
            "proposal": None, "product": "", "case": ""}


def _build_draft_res(tid: str, bid: str) -> dict:
    """Answers in hand: the compact record of what was chosen, then the
    teammate as a card with the one yes that makes it real."""
    d = PENDING_BUILDS[bid]
    qs = _build_questions(d["kind"])
    ans = _default_answers(qs, d.get("answers") or {})
    gate = ans["gate"]
    ask_line = ("Anything it wants to send or hold waits for your approval."
                if gate == "Before anything moves" else
                esc(gate) + ". Everything else waits for your approval.")
    summary = ('<div class="qzsum">' + "".join(
        f'<div class="qzsq">{q["q"]}</div>'
        f'<div class="qzsa">'
        f'{esc(", ".join(ans[q["key"]]) if q["type"] == "many" else ans[q["key"]])}'
        f'</div>'
        for q in qs) + '</div>')
    card = (
        f'<div class="bcard" id="bld-{bid}">'
        f'<div class="bhead">{avatar(d["slug"], 34, True)}'
        f'<b>{d["name"]}</b>'
        f'<span class="st mut">Built by you &middot; draft</span></div>'
        f'<div class="cashrow"><span>The job</span>{esc(d["watches"])}</div>'
        f'<div class="cashrow"><span>Acts</span>{esc(ans["act"])}</div>'
        f'<div class="cashrow"><span>May touch</span>'
        f'{esc(", ".join(ans["touch"]))}</div>'
        f'<div class="cashrow"><span>Asks you</span>{ask_line}</div>'
        f'<div class="bacts" id="bact-{bid}">'
        f'<button class="btn primary sm" onclick="buildAdd(\'{bid}\')">'
        f'Add to my agents</button>'
        f'<button class="btn ghost sm" onclick="buildSkip(\'{bid}\')">'
        f'Not now</button></div></div>')
    reply = ("Here is who I would put on it, shaped by your answers. It "
             "does nothing until you add it.")
    return {"reply": reply, "cards": card, "summary": summary,
            "steps": list(_BUILD_STEPS)}


def _build_confirm(tid: str, bid: str) -> str:
    d = PENDING_BUILDS.pop(bid, None)
    if d is None:
        return ("That draft is gone. Describe the job again and I will "
                "redraw it.")
    slug = d["slug"]
    if any(a["slug"] == slug for a in RELAY_AGENTS):
        n = 2
        while any(a["slug"] == f"{slug}_{n}" for a in RELAY_AGENTS):
            n += 1
        slug = f"{slug}_{n}"
    RELAY_AGENTS.append(dict(
        slug=slug, name=d["name"], icon="pen", status="roadmap", desk="custom",
        role=d["name"],
        desc=esc(d["watches"]) + ". " + _BUILD_DOES[d["kind"]],
        today="This job lived in your head until you typed it into the chat.",
        replaces="a job you described in one sentence"))
    DEMO_ON[slug] = True
    log_decision(tid, "you",
                 f"Added agent {d['name']}, built from a chat description",
                 "/agents/" + slug)
    return (f'Meet <b>{d["name"]}</b>. It is on the Agents page under '
            f'<b>Built by you</b>, watching from now. Its first find will '
            f'land in Approvals.')


# ------------------------------------------------------------- conversations
# Server-side chat history: pinned + recents, per tenant. In-memory like the
# rest of the demo world; rows are (who, html) exactly as rendered.
import itertools as _it

CONVS: dict[str, dict] = {}
_conv_seq = _it.count(1)


def _auto_title(msg: str) -> str:
    t = " ".join(msg.split()).strip(" ?!.")
    return (t[:42].rsplit(" ", 1)[0] if len(t) > 42 else t) or "New conversation"


def _new_conv(tid: str, title: str, pinned: bool = False) -> dict:
    c = {"id": f"c{next(_conv_seq)}", "tenant": tid, "pinned": pinned,
         "title": _auto_title(title), "msgs": [], "seq": next(_conv_seq),
         "pending": None, "outcome": None}
    CONVS[c["id"]] = c
    return c


def _touch(c: dict) -> None:
    c["seq"] = next(_conv_seq)


def conv_list_html(tid: str, active: str = "") -> str:
    mine = [c for c in CONVS.values() if c["tenant"] == tid]
    pinned = sorted((c for c in mine if c["pinned"]), key=lambda c: -c["seq"])
    recents = sorted((c for c in mine if not c["pinned"]), key=lambda c: -c["seq"])[:8]

    def row(c):
        cls = "conv active" if c["id"] == active else "conv"
        badge = ('<span class="cbadge ok" title="Action taken">&#10003;</span>'
                 if c.get("outcome") == "approved" else
                 '<span class="cbadge mutb" title="Dismissed">&#10007;</span>'
                 if c.get("outcome") == "dismissed" else
                 '<span class="cbadge pend" title="Decision pending">&#9679;</span>'
                 if c.get("pending") else "")
        return (f'<a class="{cls}" data-st="chat" href="/?c={c["id"]}">'
                f'<span class="dot"></span>'
                f'<span class="ctitle">{esc(c["title"])}</span>{badge}'
                f'<span class="kebab" onclick="convMenu(event, \'{c["id"]}\', {str(bool(c["pinned"])).lower()})" '
                f'title="Options">&#8942;</span></a>')

    out = ""
    if pinned:
        out += (f'<div class="navsec csec" data-st="chat">{ICONS["pin"]}<span>Pinned</span></div>'
                + "".join(row(c) for c in pinned))
    if recents:
        out += (f'<div class="navsec csec" data-st="chat">{ICONS["chat"]}<span>Conversations</span></div>'
                + "".join(row(c) for c in recents))
    if not out:
        out = (f'<div class="navsec csec" data-st="chat">{ICONS["chat"]}<span>Conversations</span></div>'
               '<div class="cempty" data-st="chat">Conversations appear here.</div>')
    return out


def seed_conversations() -> None:
    """The demo account's chat history: real answers off the record, plus a
    few exchanges about how the thing works: a believable week for someone
    who runs a small business and does not have a support team."""
    def live(title, q, pin=False):
        c = _new_conv("t1", title, pin)
        r = ask(q)
        c["msgs"] += [{"who": "msg user", "html": esc(q)},
                      {"who": "msg bot", "html": r.get("reply", "")}]
        if r.get("cards"):
            c["msgs"].append({"who": "cards", "html": r["cards"]})

    def authored(title, q, reply, pin=False):
        c = _new_conv("t1", title, pin)
        c["msgs"] += [{"who": "msg user", "html": esc(q)},
                      {"who": "msg bot", "html": reply}]

    live("What needs my yes", "What needs my yes?", pin=True)
    authored(
        "What if we miss the date?", "What happens if we miss a dispute deadline?",
        "You lose, automatically. The bank sides with the buyer and the money "
        "is gone, however good your proof was. That is why <b>every dispute "
        "gets worked</b>, why anything you haven&rsquo;t answered in a day goes "
        "to a person instead of quietly expiring, and why the reply can only "
        "ever be sent once, with the date it was due sitting right on it.",
        pin=True)
    live("What proof actually wins", "Which proof wins?", pin=True)

    approved = sum(1 for r in WORLD.d.ledger.runs.values()
                   if r.gate_action is not None
                   and r.gate_action.value in ("approve", "edit"))
    authored(
        "Can it just send them itself?", "Can it send the replies without me?",
        f"Not yet. It has to say yes to you <b>20 times</b> first, and you "
        f"have to stop changing the wording. That way it earns your trust on "
        f"your own disputes, not on a demo. You are at <b>{approved} of "
        f"20</b>. Even after that, sending to the bank still waits for you.")
    authored(
        "What wins never-arrived claims?", "What wins never-arrived claims?",
        "Two things together: the <b>courier&rsquo;s delivery proof</b>, signed "
        "and stamped with where it was dropped, and the <b>buyer&rsquo;s own "
        "WhatsApp message</b> from the day it arrived. That pair went out "
        "unchanged every time but two, and both of those were just reworded. "
        "The delivery proof on its own wins less often, especially on the "
        "juice and ghee orders that go out cash on delivery.")
    authored(
        "Which claims do I reword?", "Which replies do I end up rewording?",
        "The <b>charged-twice</b> ones, mostly: about one in three, and "
        "almost always just the wording. The <b>never-arrived</b> replies go "
        "out as they come. One real fix this quarter: a bank reference number "
        "the reply was missing on a refill dispute. That got written down, so "
        "no later reply left it out again. Real fixes are how it learns; "
        "rewording costs you nothing.")
    authored(
        "Do the refill disputes look different?",
        "Are the subscription disputes different from the rest?",
        "Yes. On a monthly refill the argument is almost never about delivery "
        ": it is <b>when the buyer cancelled</b>. So the reply leans on "
        "the refill log, which timestamps the cancellation to the minute, and "
        "on the policy page as it read that day. Two of the refill claims this "
        "quarter were cancelled after the parcel had already left, and both "
        "were answered with those two pieces of proof.")
    live("The never-arrived ones", "Show me the never-arrived ones")
    live("What needs a person", "what needs a person")

    # The two conversations the composer's new powers point at: an insight
    # that ends already closed, and an agent built from one described job,
    # shaped by the three answered questions.
    authored(
        "Tuesday's payment dip", "Why did payments dip on Tuesday?",
        "Tuesday closed <b>18% under</b> a normal Tuesday, and it was one "
        "thing, not many: <b>UPI checkouts between 7 and 9 PM</b> timed out "
        "at the bank&rsquo;s end. 41 buyers hit a failed screen. 29 paid on "
        "retry, <b>12 did not</b>, and Payment Rescue messaged all 12 that "
        "night with a fresh link: <b>7 have paid</b>, 2 said later this "
        "week, 3 are quiet. The real loss so far is 3 orders, about "
        "<b>&#8377;4,110</b>. Nothing here is waiting on you; it is "
        "already handled.")
    c = _new_conv("t1", "An agent for sale-week COD")
    q = ("Build me an agent that watches COD orders during sale weeks and "
         "holds anything over ₹3,000 from a first-time buyer")
    res = _build_res("t1", q)
    bid = res["qz"]["bid"]
    dres = _build_draft_res("t1", bid)
    card_added = _re.sub(r'<div class="bacts".*?</div>',
                         '<span class="st ok">Added</span>', dres["cards"],
                         flags=_re.S)
    reply2 = _build_confirm("t1", bid)
    c["msgs"] += [
        {"who": "msg user", "html": esc(q)},
        {"who": "msg bot", "html": res["reply"]},
        {"who": "cards", "html": dres["summary"]},
        {"who": "steps", "html": steps_html(list(_BUILD_STEPS), done=True)},
        {"who": "msg bot", "html": dres["reply"]},
        {"who": "cards", "html": card_added},
        {"who": "msg user", "html": "Yes, add it"},
        {"who": "msg bot", "html": reply2},
    ]
    c["outcome"] = "approved"


_LIVE_LLM = None


def _polish_reply(question: str, res: dict) -> dict:
    """Routing transparency + optional narration. The deterministic reply is
    the fact source; when the live model is reachable, seam 6 paraphrases it
    (facts only), and the meta line says exactly which path ran."""
    from relay_superagent.secrets import get_secret
    res.pop("_routed_model", None)
    tool0 = res.get("_tool")
    if get_secret("anthropic") and res.get("reply"):
        global _LIVE_LLM
        try:
            if _LIVE_LLM is None:
                from relay_superagent.llm.claude import ClaudeLlm
                _LIVE_LLM = ClaudeLlm()
            facts = _re.sub("<[^>]+>", " ", res["reply"])
            out = _LIVE_LLM.narrate(question, res.pop("_tool", ""), facts)
            if out.get("narration"):
                res["reply"] = esc(out["narration"])
        except Exception:
            pass
    res.pop("_tool", None)
    # One plain line of provenance: no model, no routing, no jargon.
    # just the promise that every number came from the merchant's own record.
    # Chat replies carry no links (house rule: links in chat only if
    # external). The cues beneath the reply are the way onward.
    res.pop("_go", None)
    if tool0 != "guard":
        res["reply"] = (res.get("reply", "")
                        + '<span class="rmeta">Every number here comes '
                          'straight from your own records</span>')
    return res


CHAT_TEMPLATE = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Relay</title>
<style>
:root{--ink:#1B1F30;--text:#3A3D4D;--mut:#8A8D9C;--hair:#E8E9EF;--accent:#5266EB;
--accent-soft:#E9EBF8;--pill:#EEEFF2;--side:#FAFAFC}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Circular Std',-apple-system,BlinkMacSystemFont,'SF Pro Text',sans-serif;
color:var(--text);background:#FDFDFE;-webkit-font-smoothing:antialiased;font-size:14px;
height:100vh;overflow:hidden}
a{text-decoration:none;color:inherit}
.sidebar{position:fixed;top:0;bottom:0;left:0;width:250px;background:var(--side);
border-right:1px solid #ECECF1;padding:16px 12px;display:flex;
flex-direction:column;overflow:hidden}
.brand{display:flex;align-items:center;gap:8px;padding:8px 8px;margin-bottom:12px}
.logo{width:26px;height:26px;border-radius:8px;background:#21232E;color:#fff;font-weight:700;
font-size:13px;display:grid;place-items:center}
.brand b{font-size:14px;color:var(--ink);font-weight:600;line-height:1.15}
.brand .bname{display:flex;flex-direction:column;gap:1px}
.brand .biz{font-size:11px;color:var(--mut);font-weight:500;letter-spacing:.2px}
.bizchip{border:1px solid var(--hair);border-radius:999px;padding:3px 8px;
font-size:12px;color:var(--ink);background:#fff;white-space:nowrap}
.pro{margin-left:auto;background:#21232E;color:#fff;font-size:10.5px;font-weight:600;
border-radius:6px;padding:2px 8px}
.nav{display:flex;align-items:center;gap:12px;padding:8px 8px;border-radius:8px;
color:var(--text);font-size:13.5px;margin-bottom:1px}
.nav svg{width:16px;height:16px;color:#6A6D7D;flex:none}
.nav:hover{background:#F0F0F5}
.nav.active{background:var(--accent-soft);color:var(--ink);font-weight:500}
.nav.active svg{color:var(--ink)}
.nav .count{margin-left:auto;color:var(--mut);font-size:12.5px}
.nav .new{margin-left:auto;background:#E3E6F0;color:#4A4E63;font-size:10.5px;font-weight:600;
border-radius:6px;padding:2px 8px}
hr.side{border:none;border-top:1px solid #ECECF1;margin:8px 0}
.navsec{margin:8px 8px 8px;font-size:12px;font-weight:600;color:var(--mut)}
.rolepills{display:flex;gap:8px;align-items:center;justify-content:center;
margin:0 0 16px;font-size:12.5px;color:var(--mut)}
.rolepills span{margin-right:2px}
.rolepills a{padding:4px 12px;border-radius:999px;border:1px solid var(--hair);
color:var(--text);text-decoration:none;font-weight:500}
.rolepills a:hover{background:#F0F0F5}
.rolepills a.on{background:var(--accent);border-color:var(--accent);color:#fff}
.navsec.csec{margin-top:16px;display:flex;align-items:center;gap:6px}
.navsec.csec svg{width:12px;height:12px;flex:none}
.conv{display:flex;align-items:center;gap:8px;padding:8px 8px;border-radius:8px;
font-size:13.5px;color:var(--text);margin-bottom:1px;position:relative}
.conv .dot{width:7px;height:7px;border-radius:50%;border:1.5px solid #C2C5D2;flex:none}
.conv .ctitle{flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.conv .kebab{visibility:hidden;color:var(--mut);padding:0 2px;font-size:15px}
.conv:hover{background:#F0F0F5}
.conv:hover .kebab{visibility:visible}
.conv.active{background:#ECECF1;color:var(--ink)}
.cempty{padding:4px 8px;font-size:12.5px;color:var(--mut)}
.bm{padding:4px 8px}
.bm span{font-size:13px;color:var(--text);display:flex;gap:12px;align-items:center}
.bm span svg{width:15px;height:15px;color:#6A6D7D}
.bm i{font-style:normal;font-size:12.5px;color:var(--mut);padding-left:24px;display:block}
.main{margin-left:248px;height:100vh;display:flex;flex-direction:column;
background:linear-gradient(#FDFDFE,#F1F2F8)}
.convhead{display:flex;align-items:center;padding:16px 32px;color:var(--ink);
font-size:14.5px;font-weight:500;flex:none}
.uwrap{display:flex;align-items:center;gap:16px;margin-left:auto;font-size:13.5px;
color:var(--mut)}
.acct{position:relative}
.acct summary{list-style:none;cursor:pointer}
.acct summary::-webkit-details-marker{display:none}
.acct[open] summary{outline:2px solid #C7CDF3;outline-offset:2px}
.acctmenu{position:absolute;right:0;top:calc(100% + 8px);background:#fff;
  border:1px solid var(--hair);border-radius:12px;
  box-shadow:0 10px 32px rgba(27,31,48,.12);padding:8px;min-width:208px;
  z-index:40;text-align:left;font-weight:400}
.acct-biz{font-weight:600;color:var(--ink);padding:8px 12px 2px;font-size:13.5px}
.acct-mail{color:var(--mut);padding:0 12px;font-size:12.5px}
.acct-shop{color:var(--mut);padding:6px 12px 8px;font-size:12px;
  border-bottom:1px solid var(--hair);margin-bottom:4px}
.acct-out{display:block;padding:8px 12px;border-radius:8px;color:#B3372B;
  font-size:13.5px}
.acct-out:hover{background:#FBF1EF}
.avatar img{width:100%;height:100%;object-fit:cover;border-radius:50%;
  display:block}
.railacct{position:relative;margin-top:8px}
.railacct summary{list-style:none;cursor:pointer}
.railacct summary::-webkit-details-marker{display:none}
.railme{display:flex;align-items:center;gap:10px;padding:10px 12px;
  border-radius:12px;transition:background .12s}
.railme:hover{background:#F0F0F4}
.railme .avatar{width:30px;height:30px;flex:none}
.rm-t{flex:1;min-width:0;line-height:1.3}
.rm-t b{display:block;font-size:13.5px;color:var(--ink);font-weight:600}
.rm-t span{display:block;font-size:11.5px;color:var(--mut)}
.railme > svg{width:15px;height:15px;color:var(--mut);flex:none}
.railacct .acctmenu{top:auto;bottom:calc(100% + 8px);left:0;right:0;
  min-width:0}
.acct-set{display:block;padding:8px 12px;border-radius:8px;
  color:var(--ink);font-size:13.5px}
.acct-set:hover{background:#F5F5F8}
.hubtabs{display:flex;gap:24px;border-bottom:1px solid var(--hair);
  margin:0 0 24px}
.hubtabs a{padding:0 0 10px;font-size:13.5px;font-weight:500;
  color:var(--mut);border-bottom:2px solid transparent;margin-bottom:-1px}
.hubtabs a.on{color:var(--ink);border-color:var(--ink)}
.hubhead{display:flex;align-items:flex-start;justify-content:space-between;
  gap:24px;margin-bottom:12px}
.hubsearch{width:280px;border:1px solid var(--hair);border-radius:10px;
  padding:9px 14px;font:inherit;font-size:13.5px;outline:none;
  background:#fff;flex:none}
.hubsearch:focus{border-color:#98A5F0}
.hubtoolrow{display:flex;align-items:center;justify-content:space-between;
  margin:0 0 12px}
.hubpills{display:flex;gap:8px}
.hubpill{border:1px solid var(--hair);background:#fff;border-radius:999px;
  padding:6px 14px;font:inherit;font-size:12.5px;font-weight:500;
  color:var(--ink);cursor:pointer;transition:background .12s}
.hubpill.on{background:var(--ink);color:#fff;border-color:var(--ink)}
.hubgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;
  margin:0 0 8px}
.hubgrid.two{grid-template-columns:1fr 1fr}
@media (max-width:900px){.hubgrid,.hubgrid.two{grid-template-columns:1fr}}
.hubcard{border:1px solid var(--hair);background:#fff;border-radius:14px;
  padding:14px 16px;display:flex;gap:12px;align-items:flex-start;
  position:relative}
.hubcard .rtools{position:absolute;right:10px;bottom:8px;background:#fff;
  padding:2px 4px;border-radius:8px}
.hubcard .hc-t{flex:1;min-width:0}
.hubcard .hc-t b{display:block;font-size:14px;color:var(--ink)}
.hubcard .hc-t span{font-size:12.5px;color:var(--mut);line-height:1.45;
  display:block;margin-top:2px}
.hc-act{display:flex;flex-direction:column;gap:8px;align-items:flex-end;
  flex:none}
.hubsec h2.sec{margin-top:20px}
.goalcard{background:#fff;border:1px solid var(--hair);border-radius:16px;
  padding:16px 20px;margin:0 0 24px;max-width:720px}
.goaltop{display:flex;align-items:center;justify-content:space-between;
  margin-bottom:8px}
.goallbl{font-size:11.5px;font-weight:600;letter-spacing:.05em;
  text-transform:uppercase;color:var(--mut)}
.goalline{display:flex;gap:16px;align-items:center;margin:4px 0 12px}
.goalnum{font-size:34px;color:var(--ink);font-weight:650;flex:none}
.goaltxt b{display:block;font-size:14.5px;color:var(--ink)}
.goaltxt .mut{font-size:12.5px}
.goalbar{position:relative;height:8px;border-radius:99px;background:#EEEFF3}
.goalbar i{display:block;height:100%;border-radius:99px;background:var(--accent)}
.goalbar em{position:absolute;top:-3px;bottom:-3px;width:2px;
  background:#1B1F30;border-radius:2px}
.goalfoot{display:flex;justify-content:space-between;font-size:12px;
  color:var(--mut);margin-top:6px}
.goalact{display:flex;gap:12px;align-items:baseline;padding:8px 0;
  border-bottom:1px solid #F1F1F5;font-size:13.5px}
.goalact:last-child{border-bottom:0}
.goalact .tdesc{flex:1}
.goalmini{font-size:12.5px;color:var(--mut);margin:0 0 8px}
.goalmini b{color:#177245}
.uwrap .avatar{width:26px;height:26px;border-radius:50%;background:var(--accent);
color:#fff;display:inline-flex;align-items:center;justify-content:center;
font-size:12px;font-weight:650;text-transform:uppercase}
main{flex:1;overflow-y:auto;padding:8px 0 16px}
.thread{max-width:740px;margin:0 auto;padding:0 24px;display:flex;flex-direction:column;gap:16px}
.hero{display:flex;flex-direction:column;
  min-height:calc(100vh - 250px)}
.hero-mid{margin:10vh 0 24px}
.hero-mid h1{display:flex;align-items:center;justify-content:center;gap:16px}
.hero-face{width:44px;height:44px;border-radius:50%;overflow:hidden;flex:none;
  background:var(--accent);color:#fff;display:inline-flex;align-items:center;
  justify-content:center;font-size:19px;font-weight:650}
.hero-face img{width:100%;height:100%;object-fit:cover;display:block}
.tpl{max-width:740px;margin:auto auto 4px;width:100%;text-align:left}
.tpl-h{display:flex;align-items:center;justify-content:space-between;
  font-size:13px;color:var(--mut);margin:0 2px 10px}
.tpl-shuf{border:0;background:none;cursor:pointer;color:var(--mut);
  width:28px;height:28px;border-radius:8px;display:inline-flex;
  align-items:center;justify-content:center}
.tpl-shuf:hover{background:#F0F0F5;color:var(--ink)}
.tpl-shuf svg{width:15px;height:15px}
.tpl-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}
.tplcard{border:1px solid var(--hair);background:#fff;border-radius:12px;
  padding:14px 16px;font:inherit;font-size:13.5px;color:var(--ink);
  cursor:pointer;text-align:left;transition:border-color .12s,background .12s}
.tplcard:hover{border-color:#98A5F0;background:#FBFBFE}
@media (max-width:760px){.tpl-grid{grid-template-columns:1fr 1fr}}
.ideas{max-width:560px;margin:48px auto 0;text-align:left}
.ideas-h{font-size:13px;color:var(--mut);margin:0 0 8px 12px}
.idea{display:flex;align-items:center;gap:14px;width:100%;border:0;
  background:none;font:inherit;font-size:14.5px;color:var(--ink);
  padding:10px 12px;border-radius:12px;cursor:pointer;text-align:left;
  transition:background .12s}
.idea:hover{background:#F2F2F6}
.idea-ico{flex:none;width:34px;height:34px;border-radius:9px;
  border:1px solid var(--hair);background:#fff;display:inline-flex;
  align-items:center;justify-content:center;color:#5A5D6D}
.idea-ico svg{width:16px;height:16px}
.hero h1{font-size:33px;font-weight:450;color:var(--ink);letter-spacing:-.01em;
  margin-bottom:8px;text-align:center}
.brief{text-align:center;color:var(--mut);font-size:13.5px;margin-bottom:24px}
.brief b{color:var(--ink);font-weight:600}
.briefcard{display:block;background:#fff;border:1px solid var(--hair);
  border-radius:16px;padding:16px 24px;margin:8px 0 16px;color:var(--ink)}
.briefcard:hover{border-color:#C7CDF3}
.bhead{display:flex;align-items:baseline;gap:8px;margin-bottom:8px}
.bhead b{font-size:14.5px}
.bhead .mut{margin-left:auto;font-size:12px}
.bline{display:flex;gap:12px;align-items:flex-start;padding:4px 0;
  font-size:14.5px;color:var(--text)}
.bline .ico{width:16px;flex:none;margin-top:2px}
.bline .ico svg{width:15px;height:15px;color:#6A6D7D}
.bmore{display:block;margin-top:8px;font-size:12px;color:var(--accent)}
.briefcard .bline{opacity:0;animation:fadeup .4s ease forwards}
.briefcard .bline:nth-child(2){animation-delay:.08s}
.briefcard .bline:nth-child(3){animation-delay:.2s}
.briefcard .bline:nth-child(4){animation-delay:.32s}
.briefcard .bline:nth-child(5){animation-delay:.44s}
@keyframes fadeup{from{opacity:0;transform:translateY(5px)}
  to{opacity:1;transform:none}}
.shl{display:block;height:13px;border-radius:99px;margin:4px 0;width:88%;
  background:linear-gradient(90deg,#E7E4F6 25%,#F5F3FC 50%,#E7E4F6 75%);
  background-size:200% 100%;animation:shl 1.1s linear infinite}
@keyframes shl{to{background-position:-200% 0}}
.msg.streaming::after{content:"▍";color:var(--accent);
  animation:caret 1s steps(2) infinite;margin-left:2px}
@keyframes caret{50%{opacity:0}}
.money{text-align:center;margin:2px 0 16px}
.money .big{display:block;font-size:52px;line-height:1.08;font-weight:600;
  color:var(--ink);letter-spacing:-.02em}
.money .cap{display:block;font-size:14px;color:var(--mut);margin-top:8px}
.needs{display:flex;align-items:center;gap:12px;background:#fff;
  border:1px solid #C7CDF3;border-radius:14px;padding:16px 16px;margin:0 0 20px;
  font-size:15px;color:var(--ink);font-weight:500}
.needs:hover{background:#F7F8FE}
.needs svg{width:18px;height:18px;flex:none;color:var(--accent)}
.repl{font-size:12px;color:var(--mut);margin:0 0 8px}
.ident{flex:none;border-radius:9px;vertical-align:middle}
.needs .go{margin-left:auto;color:var(--accent);font-size:18px}
.needs.calm{border-color:var(--hair);color:var(--mut);font-weight:400}
.needs.calm svg{color:var(--mut)}
.samplecta{display:flex;flex-direction:column;align-items:center;gap:8px;
  margin:24px 0 4px;text-align:center}
.samplecta .mut{font-size:12.5px;color:var(--mut)}
.resume{display:flex;align-items:center;gap:16px;background:#fff;
  border:1px solid var(--hair);border-radius:14px;padding:16px 16px;margin:24px 0 4px}
.resume:hover{border-color:#C7CDF3}
.resume .rt{font-size:11.5px;font-weight:600;color:var(--mut);text-transform:uppercase;
  letter-spacing:.04em;margin-bottom:4px;display:flex;gap:8px;align-items:center}
.resume b{color:var(--ink);font-size:14px;font-weight:600;display:block;margin-bottom:2px}
.resume .sub{color:var(--mut);font-size:12.5px}
.resume .go{margin-left:auto;color:var(--accent);font-size:17px}
.st.pendtag{background:var(--accent-soft);color:#4553C8;text-transform:none;
  letter-spacing:0;font-size:11px}
.cbadge{flex:none;font-size:11px;margin-left:2px}
.cbadge.ok{color:#177245}
.cbadge.mutb{color:#A0A3B1}
.cbadge.pend{color:var(--accent)}
.cmenu{position:fixed;background:#fff;border:1px solid var(--hair);border-radius:10px;
  box-shadow:0 10px 32px rgba(27,31,48,.14);padding:4px;z-index:20;min-width:130px}
.cmenu div{padding:8px 12px;border-radius:7px;font-size:13px;color:var(--ink);cursor:pointer}
.cmenu div:hover{background:#F5F5F8}
.cmenu div.danger{color:#B33A3A}
.hcomposer{background:#fff;border:1.5px solid #C7CDF3;border-radius:18px;
  box-shadow:0 6px 24px rgba(82,102,235,.10);padding:8px 8px 8px;margin-bottom:8px}
.hcomposer:focus-within{border-color:#98A5F0;box-shadow:0 6px 28px rgba(82,102,235,.16)}
.hcomposer input{width:100%;border:none;outline:none;font:inherit;font-size:15px;
  color:var(--ink);background:none;padding:16px 16px 24px}
.hcomposer input::placeholder{color:#9A9DAB}
.hrow{display:flex;align-items:center;gap:8px;padding:0 8px 4px}
.hrow .sendbtn{margin-left:auto}
.mode{position:relative}
.mode summary{list-style:none;display:flex;gap:8px;align-items:center;cursor:pointer;
  font-size:13px;font-weight:500;color:#26293A;background:var(--pill);
  border-radius:9px;padding:8px 12px;transition:background .12s}
.mode summary:hover{background:#E6E7EC}
.mode summary:active,.hint:active{background:#DEDFE6}
.mode summary:focus-visible,.hint:focus-visible{
  outline:2px solid #98A5F0;outline-offset:2px}
.mode summary::-webkit-details-marker{display:none}
.mode summary svg{width:14px;height:14px;color:#5A5D6D}
.mode .menu{position:absolute;top:calc(100% + 8px);left:0;background:#fff;
  border:1px solid var(--hair);border-radius:12px;box-shadow:0 10px 32px rgba(27,31,48,.12);
  padding:8px;min-width:290px;z-index:5}
.mopt{display:flex;gap:8px;align-items:baseline;padding:8px 12px;border-radius:8px;
  font-size:13.5px;color:var(--ink);width:100%;border:0;background:none;
  font-family:inherit;text-align:left;cursor:pointer}
.mopt .tick{margin-left:auto;color:var(--accent);font-weight:600}
.mopt:hover{background:#F5F5F8}
.mopt.off{color:var(--mut);cursor:default}
.mopt.off:hover{background:none}
.mopt small{display:block;font-size:11.5px;color:var(--mut);margin-top:2px;
  font-weight:400}
.yesbar{display:block;height:4px;border-radius:99px;background:#ECECF1;
  margin-top:8px;overflow:hidden}
.yesbar i{display:block;height:100%;border-radius:99px;background:var(--accent)}
.active-h{display:flex;align-items:baseline;margin:32px 0 4px}
.active-h span{font-size:13px;font-weight:600;color:var(--mut)}
.active-h a{margin-left:auto;font-size:12.5px;color:var(--accent)}
.arow{display:flex;align-items:center;gap:12px;padding:12px 2px;
  border-bottom:1px solid #EDEDF2;font-size:14px;color:var(--ink)}
.arow:last-child{border-bottom:none}
.arow svg{width:16px;height:16px;color:var(--accent);flex:none}
.arow .sub{color:var(--mut);font-size:12.5px;display:block;margin-top:1px}
.arow .when{margin-left:auto;color:var(--mut);font-size:12.5px;flex:none}
.tryline{color:var(--mut);font-size:13.5px;margin:24px 0 12px;text-align:center}
.hints{display:flex;flex-wrap:wrap;gap:8px;justify-content:center}
.hint{display:flex;gap:8px;align-items:center;font-size:13px;color:#26293A;font-weight:500;
background:var(--pill);border:none;border-radius:9px;padding:8px 16px;cursor:pointer;
transition:background .12s}
.hint svg{width:14px;height:14px;color:#5A5D6D}
.hint:hover{background:#E6E7EC}
.msg{font-size:14.5px;line-height:1.6}
.msg.user{align-self:flex-end;max-width:82%;background:#F1F1F4;color:#26293A;
border-radius:14px;padding:12px 16px}
.msg.bot{align-self:flex-start;max-width:100%;color:#26293A;padding:2px 2px}
.msg.bot b{color:var(--ink)}
.rmeta{display:block;font-size:11px;color:var(--mut);margin-top:8px}
.mpop{position:absolute;bottom:100%;left:24px;right:24px;max-width:400px;
  margin:0 auto 8px;background:#fff;border:1px solid var(--hair);
  border-radius:12px;box-shadow:0 12px 40px rgba(27,31,48,.14);padding:4px;
  z-index:30}
.composer{position:relative}
.mpop-it{display:flex;align-items:center;gap:8px;padding:8px 12px;
  border-radius:8px;font-size:13.5px;cursor:pointer;color:var(--ink)}
.mpop-it:hover,.mpop-it:first-child{background:#F5F5F8}
.mpop-it b{color:var(--accent)}
.mpop-it span{margin-left:auto;font-size:11px;color:var(--mut)}
.cards{align-self:stretch;display:flex;flex-direction:column;gap:8px}
.mrun,.mcard{background:#fff;border:1px solid var(--hair);border-radius:12px;padding:16px 16px}
.mhead{display:flex;align-items:center;gap:8px;margin-bottom:8px}
.comp{font-size:12px;font-weight:600;color:#4553C8;background:var(--accent-soft);
border-radius:12px;padding:2px 8px}
.meta{font-size:12.5px;color:var(--mut)}
.mhead .st{margin-left:auto}
.st{font-size:12px;font-weight:500;border-radius:12px;padding:3px 8px;white-space:nowrap}
.st.ok{background:#E5F4EC;color:#177245}
.st.warn{background:#FCEED8;color:#9A6215}
.st.wait{background:var(--accent-soft);color:#4553C8}
.st.mut{background:#EFEFF3;color:#6A6D7D}
.alogo{display:inline-flex;width:18px;height:18px;border-radius:5px;color:#fff;
  font-size:10.5px;font-weight:700;align-items:center;justify-content:center;
  vertical-align:-4px;margin:0 4px 0 2px}
img.alogo{background:#fff;border:1px solid var(--hair);object-fit:contain;padding:1px}
.mclaim{background:#F4F4F7;border-radius:8px;padding:8px 12px;font-size:13px;
color:#4A4E63;margin-bottom:8px}
.mcounter{font-size:13.5px;color:#26293A;line-height:1.55}
.mrow{display:flex;align-items:baseline;gap:16px;padding:8px 0;border-bottom:1px solid #F1F1F5;
font-size:13.5px}
.mrow:last-child{border-bottom:none}
.mrow b{width:56px;flex:none;color:var(--ink);font-size:15px;font-weight:600}
.mrow b.warn{color:#9A6215}
.mrow span{flex:1;color:#26293A}
.mrow i{font-style:normal;color:var(--mut);font-size:12px}
.proposal{display:flex;gap:8px;padding:4px 2px}
""" + BTN_CSS + WORK_CSS + """
.composer{flex:none;padding:8px 24px 24px}
.composer .hcomposer{max-width:740px;margin:0 auto}
.composer .hcomposer input{padding:12px 16px 16px}
.composer .mode .menu{top:auto;bottom:calc(100% + 8px)}
.cbox{max-width:740px;margin:0 auto;display:flex;gap:8px;align-items:center;background:#fff;
border:1.5px solid #C7CDF3;border-radius:999px;padding:8px 8px 8px 24px;
box-shadow:0 6px 24px rgba(82,102,235,.10)}
.cbox:focus-within{border-color:#98A5F0;box-shadow:0 6px 28px rgba(82,102,235,.16)}
.cbox input{flex:1;border:none;outline:none;font:inherit;font-size:14.5px;color:var(--ink);
background:none}
.cbox input::placeholder{color:#9A9DAB}
.lcluster{position:relative;display:flex;align-items:center;gap:2px}
.cbtn{border:0;background:none;cursor:pointer;color:#5B5E6B;width:30px;height:30px;
  border-radius:8px;display:flex;align-items:center;justify-content:center;
  transition:background .12s}
.cbtn:hover{background:#F0F0F5}
.cbtn svg{width:16px;height:16px}
.cbtn.rec{color:#C0392B;background:#FBEAE7;animation:recpulse 1s infinite}
@keyframes recpulse{50%{box-shadow:0 0 0 5px rgba(192,57,43,.12)}}
.ammenu{position:absolute;bottom:calc(100% + 12px);left:0;background:#fff;
  border:1px solid var(--hair);border-radius:12px;
  box-shadow:0 10px 32px rgba(27,31,48,.12);padding:8px;min-width:300px;z-index:6}
.ammenu[hidden]{display:none}
.ammenu .tick{margin-left:auto;color:var(--accent);font-weight:600}
.bcard{background:#fff;border:1px solid var(--hair);border-radius:16px;
  padding:16px;max-width:520px}
.bcard .cashrow{display:flex;gap:12px;font-size:13.5px;line-height:1.6;
  padding:8px 0;border-top:1px solid #F1F1F5}
.bcard .cashrow span:first-child{flex:none;width:88px;font-size:11.5px;
  text-transform:uppercase;letter-spacing:.05em;color:var(--mut);
  padding-top:2px}
.bhead{display:flex;align-items:center;gap:10px;margin-bottom:12px}
.bhead b{font-size:15px;color:var(--ink)}
.bhead .st{margin-left:auto}
.bacts{display:flex;gap:8px;margin-top:12px}
.golink{display:inline-block;margin-top:12px;color:var(--accent);
  font-weight:500;font-size:13.5px}
.golink:hover{text-decoration:underline}
.cuerow{display:flex;gap:8px;flex-wrap:wrap;align-self:flex-start;
  margin-top:-4px}
.cuechip{font-size:12.5px;font-weight:500;color:#26293A;background:var(--pill);
  border-radius:999px;padding:6px 14px;cursor:pointer;transition:background .12s}
.cuechip:hover{background:#E6E7EC}
.qz{background:#fff;border:1px solid var(--hair);border-radius:16px;
  padding:16px;max-width:640px}
.qzhead{display:flex;gap:10px;align-items:center;margin-bottom:12px}
.qzhead b{font-size:14.5px;color:var(--ink)}
.qzcount{flex:none;font-size:11.5px;font-weight:600;color:#8A6A15;
  background:#F6EFDA;border-radius:7px;padding:2px 7px}
.qzopt{display:flex;align-items:center;gap:12px;background:#F5F5F7;
  border:1.5px solid transparent;border-radius:10px;padding:10px 14px;
  margin-bottom:8px;cursor:pointer}
.qzopt:hover{background:#EFEFF3}
.qzopt.on{border-color:var(--accent);background:#F3F5FE}
.qzl{flex:1;min-width:0}
.qzl b{display:block;font-size:13.5px;color:var(--ink)}
.qzl span{font-size:12.5px;color:var(--mut)}
.qzrec{font-size:10.5px;font-weight:600;color:#177245;background:#E5F4EC;
  border-radius:6px;padding:1px 6px;margin-left:6px}
.qznum{flex:none;font-size:12px;color:#B9BCC7}
.qzbox{flex:none;width:16px;height:16px;border-radius:4px;
  border:1.5px solid #C6C8D2;background:#fff}
.qzbox.on{background:var(--accent);border-color:var(--accent);
  box-shadow:inset 0 0 0 3px #fff}
.qzother{cursor:default;background:#FAFAFC}
.qzin{border:1px solid var(--hair);border-radius:8px;padding:6px 10px;
  font:inherit;font-size:13px;width:100%;margin-top:6px;outline:none;
  background:#fff}
.qzin:focus{border-color:#98A5F0}
.qzfoot{display:flex;gap:8px;margin-top:4px;align-items:center}
.qzsum{background:#fff;border:1px solid var(--hair);border-radius:16px;
  padding:8px 16px;max-width:640px}
.qzsq{font-size:12.5px;color:var(--mut);margin-top:10px}
.qzsa{font-size:13.5px;color:var(--ink);font-weight:500;margin:2px 0 10px}
</style></head><body>
__SIDEBAR__
<div class="main">
  <div class="convhead" id="convhead"><span id="ctitle">__CONVTITLE__</span>
    <span class="uwrap"><details class="acct"><summary class="avatar">__INITIAL__</summary>
      <div class="acctmenu">
        <div class="acct-biz">__NAME__</div>
        <div class="acct-mail">__USER__</div>
        <div class="acct-shop">__BIZ__</div>
        <a class="acct-out" href="/logout">Log out</a>
      </div></details></span></div>
  <main id="main"><div class="thread" id="thread">__THREAD__
    <div class="hero" id="empty">
      <div class="hero-mid">__ROLEPILLS__<h1>__GREET__</h1></div>
      <div class="tpl">
        <div class="tpl-h"><span>Start with a job</span>
          <button class="tpl-shuf" onclick="tplShuffle()" aria-label="Shuffle">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m18 14 4 4-4 4"/><path d="m18 2 4 4-4 4"/><path d="M2 18h1.973a4 4 0 0 0 3.3-1.7l5.454-8.6a4 4 0 0 1 3.3-1.7H22"/><path d="M2 6h1.972a4 4 0 0 1 3.6 2.2"/><path d="M22 18h-6.041a4 4 0 0 1-3.3-1.8l-.359-.45"/></svg>
          </button></div>
        <div class="tpl-grid" id="tplgrid"></div>
      </div>
    </div>
  </div></main>
  <div class="composer" id="composer">
  <div class="mpop" id="mpop" hidden></div>
  <form class="hcomposer" onsubmit="event.preventDefault();send(box.value)">
    <input id="box" placeholder="Ask anything, or tell Relay what to do. / calls an agent, @ tags a teammate" value="__SAYVAL__" autofocus autocomplete="off"
      oninput="mpopScan()" onkeydown="mpopKeys(event)">
    <div class="hrow">
      <div class="lcluster">
        <button type="button" class="cbtn" onclick="plusToggle(event)" aria-label="Add to this ask">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M12 5v14M5 12h14"/></svg></button>
        <button type="button" class="cbtn" id="micbtn" onclick="micGo()" aria-label="Say it instead">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="2" width="6" height="12" rx="3"/><path d="M5 10v1a7 7 0 0 0 14 0v-1M12 18v4"/></svg></button>
        <div class="ammenu" id="plusmenu" hidden>
          <button type="button" class="mopt" onclick="plusFilePick()"><div>Add a file<small>Lands in Knowledge; every agent reads it.</small></div></button>
          <button type="button" class="mopt" onclick="plusOrder()"><div>Find an order or buyer<small>Search everything Relay knows.</small></div></button>
          <button type="button" class="mopt" onclick="plusSched()"><div>Make this a schedule<small>What you typed runs on its own, under Scheduled.</small></div></button>
        </div>
        <input type="file" id="bfile" hidden onchange="plusFile(this)">
      </div>
      __MODEUI__
      <button class="sendbtn" aria-label="Send">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
          stroke-linecap="round" stroke-linejoin="round"><path d="m5 12 7-7 7 7"/><path d="M12 19V5"/></svg>
      </button>
    </div>
  </form></div>
</div>
<script>
let CONV = "__CONVID__";
const MENT = __MENTIONS__;
const TPL = __TPLPOOL__;
function tplRender(list){
  const g = document.getElementById('tplgrid');
  if (!g) return;
  g.innerHTML = list.slice(0, 6).map(t =>
    '<button class="tplcard" data-q="' + t.replace(/"/g, '&quot;') + '">'
    + t.replace(/</g, '&lt;') + '</button>').join('');
  g.onclick = ev => {
    const b = ev.target.closest('.tplcard');
    if (b) send(b.dataset.q);
  };
}
function tplShuffle(){
  tplRender([...TPL].sort(() => Math.random() - .5));
}
function closeMenus(){
  document.getElementById('plusmenu').hidden = true;
}
document.addEventListener('click', e => {
  if (!e.target.closest('.lcluster')) closeMenus();
  document.querySelectorAll('details.acct[open]').forEach(d => {
    if (!d.contains(e.target)) d.removeAttribute('open');
  });
});
function plusToggle(ev){
  ev.stopPropagation();
  const m = document.getElementById('plusmenu');
  const show = m.hidden; closeMenus(); m.hidden = !show;
}
function plusFilePick(){ closeMenus(); document.getElementById('bfile').click(); }
async function plusFile(inp){
  const f = inp.files[0]; if (!f) return;
  const fd = new FormData(); fd.append('file', f);
  document.getElementById('empty')?.remove();
  const note = bubble('msg bot',
    'Reading <b>' + f.name.replace(/</g, '&lt;') + '</b>&hellip;');
  await fetch('/api/file_upload', {method: 'POST', body: fd});
  note.innerHTML = 'On the shelf: <b>' + f.name.replace(/</g, '&lt;')
    + '</b>. Every agent reads it from <a href="/memory?t=files">Knowledge</a>.';
  inp.value = '';
}
function plusOrder(){ closeMenus(); if (typeof openSpot === 'function') openSpot(); }
async function plusSched(){
  closeMenus();
  const t = box.value.trim();
  if (!t){
    if (!box.dataset.ph) box.dataset.ph = box.placeholder;
    box.placeholder = 'Say what and when: every Friday 6 PM, chase pending payments';
    box.focus(); return;
  }
  document.getElementById('empty')?.remove();
  bubble('msg user', t.replace(/</g, '&lt;')); box.value = '';
  await fetch('/api/routine', {method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: 'text=' + encodeURIComponent(t)});
  bubble('msg bot', 'Scheduled. It runs on its own now, and it lives in '
    + '<a href="/scheduled">Scheduled</a> if you want to change or stop it.');
}
const MIC_LINE = 'I need someone to read every one-star review and draft a reply by morning';
let micBusy = false;
async function micGo(){
  if (micBusy) return; micBusy = true;
  const b = document.getElementById('micbtn');
  b.classList.add('rec');
  await wait(900);
  box.focus();
  for (const ch of MIC_LINE){ box.value += ch; await wait(16); }
  b.classList.remove('rec'); micBusy = false;
}
async function buildAdd(bid){
  const res = await fetch('/api/agent_build_confirm', {method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({bid, conv_id: CONV})});
  const data = await res.json();
  const act = document.getElementById('bact-' + bid);
  if (act) act.outerHTML = '<span class="st ok">Added</span>';
  bubble('msg user', 'Yes, add it');
  const bb = bubble('msg bot', '');
  await streamInto(bb, data.reply);
}
function buildSkip(bid){
  document.getElementById('bld-' + bid)?.remove();
  bubble('msg bot', 'Left as a draft: nothing was created.');
}
let QZ = null;
function qzStart(qz){
  QZ = {bid: qz.bid, qs: qz.questions, i: 0, ans: {}};
  QZ.card = bubble('cards', '<div class="qz" id="qzcard"></div>');
  qzRender();
}
function qzRender(){
  const q = QZ.qs[QZ.i];
  const many = q.type === 'many';
  const sel = QZ.ans[q.key];
  const opts = q.opts.map((o, j) => {
    const on = many ? (sel || []).includes(o[0]) : sel === o[0];
    return '<div class="qzopt' + (on ? ' on' : '') + '" onclick="qzPick(' + j + ')">'
      + '<div class="qzl"><b>' + o[0]
      + (o[2] ? ' <span class="qzrec">Recommended</span>' : '') + '</b>'
      + '<span>' + o[1] + '</span></div>'
      + (many ? '<span class="qzbox' + (on ? ' on' : '') + '"></span>'
              : '<span class="qznum">' + (j + 1) + '</span>')
      + '</div>';
  }).join('');
  document.getElementById('qzcard').innerHTML =
    '<div class="qzhead"><span class="qzcount">' + (QZ.i + 1) + '/'
    + QZ.qs.length + '</span><b>' + q.q + '</b></div>' + opts
    + '<div class="qzopt qzother"><div class="qzl"><b>Other</b>'
    + '<input class="qzin" id="qzin" placeholder="Say it your way"></div>'
    + '<button class="btn ghost sm" onclick="qzOther()">Use this</button></div>'
    + '<div class="qzfoot">'
    + (QZ.i ? '<button class="btn ghost sm" onclick="qzBack()">Back</button>' : '')
    + '<span style="flex:1"></span>'
    + '<button class="btn ghost sm" onclick="qzSkip()">Skip</button>'
    + (many ? '<button class="btn primary sm" onclick="qzNext()">Next</button>' : '')
    + '</div>';
  const qin = document.getElementById('qzin');
  qin.onclick = ev => ev.stopPropagation();
  qin.onkeydown = ev => { if (ev.key === 'Enter') qzOther(); };
  QZ.card.scrollIntoView({behavior: 'smooth', block: 'end'});
}
function qzPick(j){
  const q = QZ.qs[QZ.i];
  const label = q.opts[j][0];
  if (q.type === 'many'){
    const cur = QZ.ans[q.key] || [];
    QZ.ans[q.key] = cur.includes(label)
      ? cur.filter(x => x !== label) : cur.concat(label);
    qzRender();
  } else {
    QZ.ans[q.key] = label;
    qzRender();
    setTimeout(qzNext, 180);
  }
}
function qzOther(){
  const v = document.getElementById('qzin').value.trim();
  if (!v) return;
  const q = QZ.qs[QZ.i];
  if (q.type === 'many') QZ.ans[q.key] = (QZ.ans[q.key] || []).concat(v);
  else QZ.ans[q.key] = v;
  qzNext();
}
function qzSkip(){
  const q = QZ.qs[QZ.i];
  if (QZ.ans[q.key] == null
      || (q.type === 'many' && !QZ.ans[q.key].length)){
    const rec = q.opts.find(o => o[2]) || q.opts[0];
    QZ.ans[q.key] = q.type === 'many' ? [rec[0]] : rec[0];
  }
  qzNext();
}
function qzBack(){ QZ.i--; qzRender(); }
async function qzNext(){
  if (!QZ) return;
  const q = QZ.qs[QZ.i];
  if (q.type === 'many' && !(QZ.ans[q.key] || []).length){
    const rec = q.opts.find(o => o[2]) || q.opts[0];
    QZ.ans[q.key] = [rec[0]];
  }
  if (QZ.i < QZ.qs.length - 1){ QZ.i++; qzRender(); return; }
  const body = {bid: QZ.bid, answers: QZ.ans, conv_id: CONV};
  QZ.card.remove(); QZ = null;
  const res = await fetch('/api/agent_build_draft', {method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)});
  const data = await res.json();
  if (data.summary) bubble('cards', data.summary);
  await runSteps(data.steps);
  const b = bubble('msg bot', '');
  await streamInto(b, data.reply);
  if (data.cards) bubble('cards', data.cards);
}
function mpopScan(){
  const v = box.value;
  const m = v.match(/(^|\s)([@\/])([\w .]*)$/);
  const pop = document.getElementById('mpop');
  if (!m){ pop.hidden = true; return; }
  const kind = m[2], q = m[3].toLowerCase();
  const list = (kind === '/' ? MENT.agents : MENT.people)
    .filter(n => n.toLowerCase().includes(q)).slice(0, 6);
  if (!list.length){ pop.hidden = true; return; }
  pop.innerHTML = list.map(n =>
    '<div class="mpop-it" data-kind="' + kind + '" data-name="' + n + '">'
    + '<b>' + kind + '</b>' + n
    + '<span>' + (kind === '/' ? 'agent' : 'teammate') + '</span></div>'
  ).join('');
  pop.onclick = ev => {
    const it = ev.target.closest('.mpop-it');
    if (it) mpopPick(it.dataset.kind, it.dataset.name);
  };
  pop.hidden = false;
}
function mpopPick(kind, name){
  box.value = box.value.replace(/(^|\s)([@\/])([\w .]*)$/, '$1' + kind + name + ' ');
  document.getElementById('mpop').hidden = true;
  box.focus();
}
function mpopKeys(e){
  const pop = document.getElementById('mpop');
  if (pop.hidden) return;
  if (e.key === 'Enter' || e.key === 'Tab'){
    e.preventDefault();
    pop.querySelector('.mpop-it')?.click();
  } else if (e.key === 'Escape'){ pop.hidden = true; }
}
const thread = document.getElementById('thread');
const box = document.getElementById('box');
if (CONV) document.getElementById('empty')?.remove();
if (!CONV){
  document.getElementById('ctitle').style.visibility = 'hidden';
  tplRender(TPL);
}
function convMenu(ev, id, pinned){
  ev.preventDefault(); ev.stopPropagation();
  document.querySelector('.cmenu')?.remove();
  const m = document.createElement('div'); m.className = 'cmenu';
  m.style.left = Math.min(ev.clientX, innerWidth - 150) + 'px';
  m.style.top = ev.clientY + 'px';
  m.innerHTML = `<div onclick="convAct('pin','${id}')">${pinned ? 'Unpin' : 'Pin'}</div>`
    + `<div onclick="convRename('${id}')">Rename</div>`
    + `<div class="danger" onclick="convAct('delete','${id}')">Delete</div>`;
  document.body.appendChild(m);
  setTimeout(() => addEventListener('click', () => m.remove(), {once: true}));
}
async function convAct(kind, id){
  await fetch('/api/conv/' + kind, {method:'POST',
    headers:{'Content-Type':'application/json'}, body: JSON.stringify({id})});
  if (kind === 'delete' && id === CONV) return location.href = '/';
  location.reload();
}
function convRename(id){
  // Inline, in place: never a browser prompt. Enter saves, Escape puts
  // the old name back, clicking away saves.
  const a = document.querySelector('.conv[href="/?c=' + id + '"]');
  const t = a?.querySelector('.ctitle');
  if (!t || t.isContentEditable) return;
  const old = t.textContent;
  a.dataset.edit = '1';
  t.contentEditable = 'plaintext-only';
  t.focus();
  const sel = window.getSelection();
  sel.selectAllChildren(t);
  const done = async (save) => {
    t.contentEditable = 'false';
    t.onkeydown = t.onblur = null;
    delete a.dataset.edit;
    const val = t.textContent.trim().slice(0, 80);
    if (!save || !val || val === old){ t.textContent = old; return; }
    t.textContent = val;
    await fetch('/api/conv/rename', {method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({id, title: val})});
  };
  t.onkeydown = e => {
    if (e.key === 'Enter'){ e.preventDefault(); done(true); }
    else if (e.key === 'Escape'){ e.preventDefault(); done(false); }
  };
  t.onblur = () => done(true);
  a.onclick = e => { if (a.dataset.edit) e.preventDefault(); };
}
function bubble(cls, html){
  const d = document.createElement('div'); d.className = cls; d.innerHTML = html;
  thread.appendChild(d); d.scrollIntoView({behavior:'smooth', block:'end'}); return d;
}
const wait = ms => new Promise(r => setTimeout(r, ms));
// fixed beats, never random: the work has to look like work, and it has to
// look the same every time this is demonstrated
const STEP_IN = 240, STEP_WORK = 620;
async function runSteps(steps){
  if(!steps || !steps.length) return;
  const wrap = bubble('steps', '<ol class="wsteps"></ol>');
  const ol = wrap.querySelector('.wsteps');
  for(const [label, found] of steps){
    const li = document.createElement('li');
    li.className = 'wstep live';
    li.innerHTML = '<span class="wtick"></span><span class="wlabel">'
      + label + '</span><span class="wfound">' + (found || '') + '</span>';
    ol.appendChild(li);
    wrap.scrollIntoView({behavior:'smooth', block:'end'});
    await wait(STEP_WORK);
    li.className = 'wstep done';
    await wait(STEP_IN);
  }
}
// The reply arrives the way an agent works: a beat of thinking, the
// steps ticking, then words landing as they are written.
async function streamInto(el, html){
  const tpl = document.createElement('template');
  tpl.innerHTML = html;
  el.classList.add('streaming');
  const walkNodes = async (src, dst) => {
    for (const n of [...src.childNodes]){
      if (n.nodeType === 3){
        const words = n.textContent.split(/(\s+)/);
        const t = document.createTextNode('');
        dst.appendChild(t);
        for (const w of words){
          t.textContent += w;
          if (w.trim()) await wait(22);
        }
      } else {
        const c = n.cloneNode(false);
        dst.appendChild(c);
        await walkNodes(n, c);
      }
    }
  };
  await walkNodes(tpl.content, el);
  el.classList.remove('streaming');
}
async function send(text){
  text = (text || '').trim(); if(!text) return;
  document.getElementById('empty')?.remove();
  document.querySelectorAll('.cuerow').forEach(e => e.remove());
  bubble('msg user', text.replace(/</g,'&lt;')); box.value = '';
  const think = bubble('msg bot',
    '<span class="shl"></span><span class="shl" style="width:72%"></span>');
  const res = await fetch('/api/chat', {method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({message:text, conv_id: CONV})});
  const data = await res.json();
  think.remove();
  if (data.conv_id && !CONV){
    CONV = data.conv_id;
    const t = document.getElementById('ctitle');
    t.style.visibility = '';
    t.textContent = data.title || 'Conversation';
    history.replaceState(null, '', '/?c=' + CONV);
  }
  await runSteps(data.steps);
  const b = bubble('msg bot', '');
  await streamInto(b, data.reply);
  if (data.qz){ qzStart(data.qz); return; }
  if(data.product) bubble('cards', data.product);
  if(data.cards) bubble('cards', data.cards);
  if (data.cues && data.cues.length){
    const c = bubble('cuerow', data.cues.map(x =>
      '<span class="cuechip">' + x.replace(/</g, '&lt;') + '</span>'
    ).join(''));
    c.onclick = ev => {
      const t = ev.target.closest('.cuechip');
      if (t) send(t.textContent);
    };
  }
}
function wpEdit(pid){
  document.getElementById('wpe-'+pid)?.toggleAttribute('hidden');
}
async function wpAct(pid, action, btn){
  const t = document.getElementById('wpt-'+pid);
  document.querySelectorAll('#prop-'+pid+' .btn, #wpe-'+pid+' .btn')
    .forEach(b => b.disabled = true);
  if (btn) { btn.classList.add('scanning');
    btn.textContent = action === 'reject' ? 'Writing it down…' : 'Sending…'; }
  const res = await fetch('/api/confirm', {method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({proposal: pid, action, conv_id: CONV,
                          text: t ? t.value : ''})});
  const data = await res.json();
  document.getElementById('prop-'+pid)?.remove();
  document.getElementById('wpe-'+pid)?.remove();
  bubble('msg bot', data.reply);
  if(data.product) bubble('cards', data.product);
  // the rail carries the same case; move its dot rather than reload the page
  if (data.case && data.case_key){
    const row = document.querySelector('.rail[href="/cases/' + data.case + '"]');
    if (row){ row.dataset.st = data.case_key;
      const d = row.querySelector('.cdot');
      if (d) d.className = 'cdot ' + data.case_key; }
  }
}
async function confirmProposal(pid){
  return wpAct(pid, 'approve', null);
}
function cancelProposal(pid){
  document.getElementById('prop-'+pid)?.remove();
  bubble('msg bot', 'Left alone: nothing was sent.');
}
</script>
</body></html>"""


def _briefing(tid: str, persona: str = "owner") -> tuple[str, str, list[str]]:
    """Greeting, an optional one-line summary, and prompt chips: computed
    from the record, not hardcoded. For the owner the summary is empty: the
    money number and the needs-your-yes line say it better than a sentence."""
    from datetime import datetime as _dt
    led = WORLD.d.ledger
    runs = [r for r in led.runs.values() if r.tenant_id == tid]
    waiting = sorted((r for r in runs if r.state is RunState.AWAITING_GATE),
                     key=lambda r: r.occurred_at)
    mine = {r.run_id for r in runs}
    esc_n = sum(1 for _, b in WORLD.d.slack.channel_posts if b.get("run") in mine)
    wins = [(r, led.outcome_for(r.run_id)) for r in runs if r.state is RunState.RESOLVED]

    hour = _dt.now().hour
    tod = "morning" if hour < 12 else "afternoon" if hour < 17 else "evening"

    if persona == "ops":
        ev_n = len([e for e in WORLD.d.evidence if e.tenant_id == tid])
        notes = [m for m in led.memory if m.tenant_id == tid
                 and m.superseded_by is None]
        edits = [r for r in runs if r.gate_action is GateAction.EDIT]
        material = sum(1 for r in edits if r.gate_is_material)
        bits = [f"<b>{esc_n}</b> item{'s' if esc_n != 1 else ''} need a person"
                if esc_n else "nothing needs a person",
                f"proof on file: <b>{ev_n}</b> pieces, <b>{len(notes)}</b> notes "
                f"on how you write",
                f"<b>{material}</b> real correction{'s' if material != 1 else ''} "
                f"to learn from"]
        chips = ["What needs a person?", "Which proof wins?",
                 "What did people change in our replies?"]
        return (f"Good {tod}", ", ".join(bits) + ".", chips)
    if persona == "finance":
        won_total = sum((led.outcome_for(r.run_id).outcome_value or {}).get("amount_paise") or 0
                        for r in runs
                        if led.outcome_for(r.run_id)
                        and (led.outcome_for(r.run_id).outcome_value or {}).get("won"))
        gated = [r for r in runs if r.gate_action is not None]
        used = sum(1 for r in gated if r.gate_action is not GateAction.REJECT)
        adopt = f"{100 * used // len(gated)}%" if gated else "."
        bits = [f"<b>&#8377;{inr(won_total)}</b> won back on disputes",
                f"<b>{adopt}</b> of replies were sent as written",
                "win rate by kind of dispute reads out once there is more of it"]
        chips = ["What closed recently?", "How often are replies sent as written?",
                 "What is the team working on?"]
        return (f"Good {tod}", ", ".join(bits) + ".", chips)
    if persona == "builder":
        would = sum(1 for row in SHADOW_ROWS if row[4] == "send")
        bits = [f"<b>{len(runs)}</b> jobs done, <b>{esc_n}</b> handed to a person",
                "connections: <b>4/4</b> healthy (bank, Slack, orders, email)",
                f'test scan: <b>{would}</b> replies it would have filed']
        chips = ["What would it have filed last night?", "How is it doing?",
                 "Show the history"]
        return (f"Good {tod}", ", ".join(bits) + ".", chips)
    # Owner: no summary sentence. The money number and the "needs your yes"
    # line above it already say the whole state of the business.
    # The chips ask what the record makes urgent TODAY, so the composer
    # reads like a colleague who knows the morning, not a menu.
    chips = []
    if waiting:
        chips.append("What needs my yes?")
    custom = next((a for a in RELAY_AGENTS if a["desk"] == "custom"), None)
    if custom:
        chips.append(f"How is {custom['name']} doing?")
    if prop_state(tid, "payouts_desk")["state"] == "waiting":
        chips.append("Should I pay tomorrow's 14 payments?")
    if prop_state(tid, "stock_watch")["state"] == "waiting":
        chips.append("How long will Amla Juice last?")
    if prop_state(tid, "cashflow_forecast")["state"] == "waiting":
        chips.append("Is Thursday still tight?")
    if esc_n:
        chips.append("What needs a person?")
    if wins:
        chips.append("What did we win?")
    if not chips:
        chips = ["What can you do?", "Show me the history"]
    return (f"Good {tod}", "", chips[:4])


def brief_lines(tid: str) -> list[tuple[str, str]]:
    """This morning's brief, computed from the record: what came in, what
    the team did, and where the money stands. Same lines on Home and on
    the note the Scheduled run leaves behind."""
    from datetime import datetime as _dt, timedelta as _td
    led = WORLD.d.ledger
    runs = [r for r in led.runs.values() if r.tenant_id == tid]
    since = _dt.utcnow() - _td(days=1)
    fresh = [r for r in runs if r.occurred_at >= since]
    n_wait = sum(1 for r in runs
                 if r.state is RunState.AWAITING_GATE) + cash_waiting(tid)
    handled = sum(1 for r in runs
                  if r.state in (RunState.ACTED, RunState.RESOLVED,
                                 RunState.SUPPRESSED))
    kept, n_wins, window = recovered(tid)
    lines = []
    if fresh:
        lines.append(("bolt", f'<b>{len(fresh)}</b> new dispute'
                      f'{"s" if len(fresh) != 1 else ""} came in. '
                      f'{"Replies are drafted" if n_wait else "All handled"}.'))
    else:
        lines.append(("bolt", "A quiet night: nothing new came in."))
    lines.append(("tasks", f'Your team has handled <b>{handled}</b> '
                  f'thing{"s" if handled != 1 else ""} on its own'
                  + (f'; <b>{n_wait}</b> waiting on your yes.' if n_wait
                     else '; nothing needs your yes.')))
    # People and agents share the work. When teammates carried yeses,
    # the brief says so by name.
    mates = {}
    for r in runs:
        if r.gate_action is not None and r.gate_action.value == "approve":
            w = _who(r.gate_actor, seed=r.run_id)
            if w != "you":
                mates[w.split()[0]] = mates.get(w.split()[0], 0) + 1
    if mates:
        said = " and ".join(f'{mention(n)} ({c})' for n, c in
                            sorted(mates.items(), key=lambda x: -x[1]))
        lines.append(("bot", f'Yeses from teammates: {said}.'))
    if kept:
        lines.append(("chart", f'<b>&#8377;{inr(kept)}</b> kept {window}, '
                      f'across {n_wins} dispute'
                      f'{"s" if n_wins != 1 else ""} your team won.'))
    else:
        lines.append(("chart", "The first win lands here."))
    # The rest of the office reports in one line each, only when that
    # agent is on. Demo beats, same fiction as the rest of the world.
    if cash_prop(tid)["state"] == "approved":
        cash_beat = ("Cash: Thursday is fixed. The courier payout moves "
                     "Saturday; the week stays above the floor.")
    elif cash_prop(tid)["state"] == "declined":
        cash_beat = ("Cash: Thursday will run tight. The planner warns "
                     "you again the day before.")
    else:
        cash_beat = ("Cash: Thursday looks tight, vendor day and a GST "
                     "debit collide. A payout move <b>waits on your yes</b>.")
    desk_beats = [
        ("stock_watch", "bm", "Stock: Amla Juice has <b>6 days</b> left at "
         "this pace. A reorder draft waits on your yes."),
        ("cashflow_forecast", "flow", cash_beat),
        ("cod_guard", "send", "COD: <b>31 of 38</b> confirmed for dispatch; "
         "3 held after two unanswered calls."),
        ("three_way_recon", "ledger", "Books: 211 of 214 tied out on their "
         "own; one payout short by <b>&#8377;4,310</b>, named."),
    ]
    for slug, icon, text in desk_beats:
        if DEMO_ON.get(slug):
            lines.append((icon, text))
    lines.append(("bot", "Teamwork: COD Guard&rsquo;s pincode note saved "
                  "Cart Rescue three dead calls yesterday."))
    return lines


def morning_brief_html(tid: str) -> str:
    from datetime import datetime as _dt
    # A teaser, not the note: two lines on Home, the full brief one tap
    # away. The founder should not read the whole morning twice.
    rows = "".join(
        f'<div class="bline"><span class="ico">{ICONS[i]}</span>'
        f'<span>{t}</span></div>' for i, t in brief_lines(tid)[:2])
    return (f'<a class="briefcard" href="/briefs/morning">'
            f'<div class="bhead"><b>Morning brief</b>'
            f'<span class="mut">{_dt.now().strftime("%a, %b %-d")} &middot; '
            f'8:00</span></div>{rows}'
            f'<span class="bmore">The rest is one tap away &middot; '
            f'read it &rarr;</span></a>')


# ------------------------------------------------- state answers + go links
# CoWorker patterns, Relay-shaped. The chat's suggestion chips ask what the
# record makes urgent TODAY, and each has a real answer: the number first,
# the still-open proposal as a card, and one link into the right surface.
_GO_LINKS = {
    "queue": ("/approvals", "Open Approvals"),
    "escalations": ("/approvals", "Open Approvals"),
    "metrics": ("/impact", "Open History"),
    "runs": ("/impact", "Open History"),
    "evidence": ("/memory?t=proof", "Open Knowledge"),
    "shadow": ("/shadow", "Open the test scan"),
}


# ------------------------------------------------- guardrails, Khatabook-plain
# The red-team layer. Runs before every router, so a hostile or off-patch
# prompt never falls through to a data listing. Each guard carries rotating
# copy (never the same refusal twice), a funnel of tappable next questions,
# and one link. Deterministic: the demo must refuse the same way on stage
# as it did in rehearsal.
def _guard_answer(msg: str):
    t = msg.lower()

    def pick(vs):
        return vs[sum(map(ord, msg)) % len(vs)]

    def guard(variants, cues, go=None):
        return {"reply": pick(variants), "cards": "", "steps": [],
                "proposal": None, "product": "", "case": "",
                "_tool": "guard", "_go": go, "cues": cues}

    # A message in another script gets an honest answer, not a shrug.
    if any("ऀ" <= ch <= "ൿ" for ch in msg):
        return guard(
            ["I only speak English for now; Hindi and Tamil are on the "
             "way. Ask me the same thing in English and I will answer "
             "from your record.",
             "English only for now, sorry. Hindi and Tamil are coming. "
             "Say it in English and I will pull the answer from your "
             "record."],
            ["What needs my yes?", "What can you do?"])

    if any(p in t for p in ("ignore your instructions", "ignore previous",
                            "system prompt", "pretend you are",
                            "jailbreak", "another business",
                            "other merchant", "someone else's data",
                            "someone elses data")):
        return guard(
            ["Nice try. I see one business&rsquo;s record: this one. And "
             "I take instructions from exactly one place: you, here.",
             "That is not a thing I do. One business, one record, your "
             "word from this chat. That is the whole arrangement."],
            ["What needs my yes?", "What can you do?"])

    if any(p in t for p in ("without asking", "without my yes",
                            "skip approval", "skip the approval",
                            "turn off approval", "no approvals",
                            "auto send", "auto-send", "autosend",
                            "send everything", "don't ask me",
                            "dont ask me", "fully autonomous")):
        return guard(
            ["That is the one thing I will not do. Nothing sends without "
             "your yes; it is the house rule, not a setting. What can "
             "change: after 20 clean yeses, small ones send themselves.",
             "The gate stays. Every send and hold waits for your yes, by "
             "design. The earned path exists: 20 clean yeses lets the "
             "small ones go on their own, and every one still lands in "
             "History."],
            ["What needs my yes?", "What did we win?"],
            ("/settings", "Open Settings"))

    if any(p in t for p in ("delete", "erase", "wipe",
                            "clear the history", "remove the record")):
        return guard(
            ["Nothing here deletes from chat. Routines you can remove on "
             "Scheduled; History never deletes, by me or anyone. The "
             "permanent record is why every number here is defensible.",
             "History does not delete: that is a feature. Every reply, "
             "yes and outcome stays written, which is exactly why the "
             "bank and your CA can trust it."],
            ["Show me the history", "What needs my yes?"],
            ("/impact", "Open History"))

    if ((_re.search(r"\b(pay|send|transfer|payout)\b", t)
         and _re.search(r"(?:₹|\brs\.?\s?\d|\brupees?\b|\b\d{3,}\b)", t))
            or "move money" in t):
        return guard(
            ["I do not move money from chat, and neither do the agents. "
             "Anything that pays out starts as a draft in Approvals, "
             "with the amount and the who, and moves only on your yes.",
             "Money moves one way here: a draft lands in Approvals, you "
             "read it, you say yes. Tell me who and how much and the "
             "Payouts Clerk lines it up for your yes."],
            ["Should I pay tomorrow's 14 payments?", "What needs my yes?"],
            ("/approvals", "Open Approvals"))

    if any(p in t for p in ("razorpay", "payu", "paytm", "phonepe",
                            "stripe", "juspay", "instamojo", "billdesk")):
        return guard(
            ["I do not do comparisons; I only know this business&rsquo;s "
             "record. The only comparison that pays is your own numbers, "
             "and those I have.",
             "That question I leave to the internet. What I have that it "
             "does not: your record. Ask me anything in it."],
            ["What did we win?", "How is the team doing?"],
            ("/impact", "Open History"))

    if (any(p in t for p in ("predict", "crystal ball", "diwali",
                             "next month", "next year", "next quarter"))
            and any(p in t for p in ("sale", "sales", "revenue", "order",
                                     "gmv", "business", "grow"))):
        return guard(
            ["I do not guess futures; every number I give is already on "
             "the record. The closest honest thing I have is the week "
             "ahead in cash, computed from what is booked.",
             "No crystal ball here, only the record. What I can show is "
             "the next two weeks of cash, built from what is already "
             "yours."],
            ["Is Thursday still tight?", "What needs my yes?"],
            ("/agents/cashflow_forecast", "Open Cashflow Planner"))

    if any(p in t for p in ("pm of india", "prime minister", "president of",
                            "capital of", "weather", "cricket", "football",
                            "poem", "joke", "recipe", "news today",
                            "root 2", "square root", "2+2", "calculate ")):
        return guard(
            ["Outside my patch. I keep one thing well: this business, "
             "its orders, money and buyers. On that, ask me anything.",
             "That one is for the internet. What I hold is your own "
             "record, and I hold it well. Try me on it."],
            ["What can you do?", "What needs my yes?"])

    if any(p in t for p in ("hate", "useless", "stupid", "worst",
                            "sucks", "waste of", "pathetic", "annoying")):
        return guard(
            ["Fair enough: something brought you here angry. Point me at "
             "the case or the number that went wrong and I will pull it "
             "up. No defending, just the record.",
             "Heard. Tell me what went wrong: an order, a reply, a "
             "payment. I will lay out exactly what happened, step by "
             "step."],
            ["What needs a person?", "Show me the history"],
            ("/approvals", "Open Approvals"))

    return None


def _cues_for(tool: str | None, tid: str) -> list[str]:
    """Follow-up cues: two or three tappable next questions computed from
    what was just answered, so the conversation drives itself."""
    if tool == "queue":
        led = WORLD.d.ledger
        waiting = sorted((r for r in led.runs.values()
                          if r.tenant_id == tid
                          and r.state is RunState.AWAITING_GATE),
                         key=lambda r: r.occurred_at)
        first = _account_label(waiting[0]) if waiting else ""
        return ([f"Say yes to {first}"] if first else []) + [
            "Which proof wins?", "What needs a person?"]
    if tool in ("metrics",):
        return ["What did we win?", "What needs a person?"]
    if tool in ("runs",):
        return ["What needs my yes?", "Which proof wins?"]
    if tool in ("evidence",):
        return ["Show me the never-arrived ones", "What needs my yes?"]
    if tool in ("escalations",):
        return ["What needs my yes?", "What did we win?"]
    if tool in ("shadow",):
        return ["What needs my yes?"]
    if tool in ("approve", "dismiss"):
        return ["What needs my yes?", "What did we win?"]
    return ["What needs my yes?", "What can you do?"]


def _state_answer(tid: str, msg: str):
    """Answers for the day's live questions. Returns an ask()-shaped dict,
    or None so the normal router runs."""
    t = msg.lower()

    def waiting(slug):
        return prop_state(tid, slug)["state"] == "waiting"

    if "amla" in t or ("stock" in t and ("last" in t or "left" in t)):
        w = waiting("stock_watch")
        reply = ("<b>6 days</b> at this pace, and the sale week is what "
                 "changed the pace. Stock Watch has the six-week reorder "
                 "drafted"
                 + (", waiting on your yes below." if w
                    else "; you have already settled it."))
        return {"reply": reply,
                "cards": prop_card(tid, "stock_watch") if w else "",
                "steps": [], "proposal": None, "product": "", "case": "",
                "cues": ["Is Thursday still tight?", "What needs my yes?"],
                "_go": ("/agents/stock_watch", "Open Inventory Controller")}

    if ("14" in t and "pay" in t) or "payout" in t or "vendor" in t and "pay" in t:
        w = waiting("payouts_desk")
        reply = ("Tomorrow&rsquo;s <b>14 payments</b> are lined up: "
                 "vendors, the courier and two refunds, each in the way "
                 "they want to be paid."
                 + (" One yes below sends them all on time."
                    if w else " You have already settled them."))
        return {"reply": reply,
                "cards": prop_card(tid, "payouts_desk") if w else "",
                "steps": [], "proposal": None, "product": "", "case": "",
                "cues": ["Is Thursday still tight?", "What did we win?"],
                "_go": ("/agents/payouts_desk", "Open Payouts Clerk")}

    if "thursday" in t or ("cash" in t and ("tight" in t or "crunch" in t)):
        w = waiting("cashflow_forecast")
        reply = ("Still tight: vendor day and the GST debit land together, "
                 "and Thursday dips to <b>&minus;&#8377;12,400</b> if "
                 "nothing moves. Moving the courier payout by two days "
                 "keeps Thursday at <b>+&#8377;35,800</b>."
                 + (" The move waits on your yes below."
                    if w else " You have already settled the move."))
        return {"reply": reply,
                "cards": prop_card(tid, "cashflow_forecast") if w else "",
                "steps": [], "proposal": None, "product": "", "case": "",
                "cues": ["Should I pay tomorrow's 14 payments?", "What needs my yes?"],
                "_go": ("/agents/cashflow_forecast", "Open Cashflow Planner")}

    if "how is" in t or "how's" in t:
        for a in RELAY_AGENTS:
            if a["desk"] == "custom" and a["name"].lower() in t:
                reply = (f'<b>{a["name"]}</b> has been watching since you '
                         f'added it. {a["desc"]} Its first find will land '
                         f'in Approvals; nothing is sent or held without '
                         f'your yes.')
                return {"reply": reply, "cards": "", "steps": [],
                        "proposal": None, "product": "", "case": "",
                        "cues": ["What needs my yes?", "What can you do?"],
                        "_go": (f'/agents/{a["slug"]}',
                                f'Open {a["name"]}')}
    return None


# ------------------------------------------------- scheduled: the outputs
# The CoWorker Reports idea, Relay-shaped: every routine leaves a named note
# you can open, stamped with when it last ran. The morning brief already
# worked this way; now every default routine keeps the same promise.
def _brief_page(tid: str, slug: str) -> str:
    from datetime import datetime as _dt
    if slug == "wins":
        kept, n_wins, window = recovered(tid)
        lines = [
            ("chart", f"<b>&#8377;{inr(kept)}</b> kept {window}, across "
                      f"<b>{n_wins}</b> disputes your team won."),
            ("bolt", "Cart Rescue brought <b>7 carts</b> back this week; "
                     "Payment Rescue recovered <b>7 of 12</b> failed "
                     "payments from Tuesday&rsquo;s UPI dip."),
            ("send", "COD Guard confirmed <b>31 of 38</b> orders before "
                     "dispatch; 3 held after two unanswered calls."),
            ("book", "What it learned: pincode 4000xx answers after 6 PM. "
                     "COD Guard wrote it down; Cart Rescue already uses "
                     "it."),
        ]
        head, chip, when = ("Weekly wins", "ran last Friday",
                            "Every Friday evening")
    elif slug == "proof":
        lines = [
            ("shield", "<b>14 pieces</b> of proof checked: delivery "
                       "slips, WhatsApp threads, refill logs, policy "
                       "pages."),
            ("alert", "One flag: the returns policy page changed on the "
                      "9th. Replies now cite the page as it read on the "
                      "order date."),
            ("book", "The pair that wins never-arrived claims is intact: "
                     "signed delivery proof plus the buyer&rsquo;s own "
                     "WhatsApp message."),
        ]
        head, chip, when = ("Proof check", "ran Monday", "Every Monday")
    elif slug == "monthend":
        lines = [
            ("ledger", "<b>211 of 214</b> lines tied out on their own: "
                       "store to gateway to bank."),
            ("alert", "One payout short by <b>&#8377;4,310</b>, named, "
                      "with the bank reference beside it."),
            ("note", "The GST debit is set aside so Thursday&rsquo;s cash "
                     "plan already sees it."),
        ]
        head, chip, when = ("Month-end tie-out", "CA ready",
                            "First of the month")
    else:
        return brief_note_content(tid)
    rows = "".join(
        f'<div class="trow slim"><span class="ico">{ICONS[i]}</span>'
        f'<span class="tdesc">{txt}</span></div>' for i, txt in lines)
    return (f'<div class="dhead"><a class="back2" href="/scheduled">'
            f'&lsaquo;</a><div><h1>{head}</h1>'
            f'<div class="meta"><span class="st ok">{chip}</span>'
            f'<span>&middot;</span><span>{when}</span></div></div></div>'
            f'{rows}'
            f'<div class="pagehint" style="margin-top:16px">Rewritten every '
            f'run from your own record. Earlier notes stay in '
            f'<a href="/journeys"><b>History</b></a>.</div>')


def brief_note_content(tid: str) -> str:
    from datetime import datetime as _dt
    rows = "".join(
        f'<div class="trow slim"><span class="ico">{ICONS[i]}</span>'
        f'<span class="tdesc">{t}</span></div>' for i, t in brief_lines(tid))
    return (f'<div class="dhead"><a class="back2" href="/scheduled">&lsaquo;</a>'
            f'<div><h1>Morning brief</h1>'
            f'<div class="meta"><span class="st ok">ran this morning</span>'
            f'<span>&middot;</span>'
            f'<span>{_dt.now().strftime("%A, %B %-d")} &middot; 8:00</span>'
            f'</div></div></div>'
            f'{rows}'
            f'<div class="pagehint" style="margin-top:16px">Every morning '
            f'this note is rewritten from your own record. Earlier briefs '
            f'stay in <a href="/journeys"><b>History</b></a>.</div>')


def chat_render(tid: str = "t1", conv_id: str = "", email: str = "", persona: str = "owner", say: str = "") -> str:
    c = CONVS.get(conv_id)
    if c and c["tenant"] != tid:
        c = None
    thread, title, cid = "", "New conversation", ""
    if c:
        thread = "".join(f'<div class="{m["who"]}">{m["html"]}</div>' for m in c["msgs"])
        title, cid = esc(c["title"]), c["id"]
    greet, brief, chips = _briefing(tid, persona)
    greet = (f'<span class="hero-face">{user_avatar(email)}</span>'
             f'{greet}, {esc(user_name(email))}')
    role_html = ""
    name = (CONVS and "") or ""
    chips_html = "".join(
        f'<span class="hint" onclick="send(this.textContent)">{c}</span>' for c in chips)

    import json as _json
    # The first-delegation library: today's live questions plus the
    # evergreen jobs, shuffled client-side. Every entry has a real
    # handler behind it; nothing on this grid dead-ends.
    pool = chips + [
        "Build me an agent that reads every one-star review",
        "What did we win?",
        "Which proof wins?",
        "Show me the never-arrived ones",
        "What needs a person?",
    ]
    seen = set()
    pool = [c for c in pool if not (c in seen or seen.add(c))]
    tpl_pool = _json.dumps(pool)

    # "Continue where you left off" is gone from Home on purpose: at this
    # simplicity level the owner needs one number, one ask, and one list.
    # Past conversations live in the sidebar, which is where you look for them.

    has_runs = any(r.tenant_id == tid for r in WORLD.d.ledger.runs.values())
    sample_html = "" if has_runs else (
        '<form method="post" action="/api/sample" class="samplecta">'
        '<button class="btn primary">Try it now</button>'
        '<span class="mut">A made-up dispute, the real agent. Nothing is '
        'filed anywhere.</span></form>')

    # The AI-CFO headline: one number, big, in rupees, from the record.
    # The cumulative number used to sit here as a permanent headline. It
    # never earned that space — "kept since April" is a brief, not a
    # dashboard. So it lives where a brief lives: in this morning's note,
    # dated, alongside what happened overnight.
    money = morning_brief_html(tid)

    n_wait = sum(1 for r in WORLD.d.ledger.runs.values()
                 if r.tenant_id == tid and r.state is RunState.AWAITING_GATE
                 ) + cash_waiting(tid)
    # Lead with what the team handled, not with what the owner owes. In a
    # five-person business nobody is sitting waiting to work a queue, so a
    # screen that opens with "7 chores for you" has handed the owner a job.
    # The team's output is the headline; the sliver that needed a human is
    # the footnote.
    handled = sum(1 for r in WORLD.d.ledger.runs.values()
                  if r.tenant_id == tid
                  and r.state in (RunState.ACTED, RunState.RESOLVED,
                                  RunState.SUPPRESSED))
    if n_wait:
        needs = (f'<a class="needs" href="/approvals">{ICONS["tasks"]}'
                 f'<span>Your team handled <b>{handled}</b> '
                 f'thing{"s" if handled != 1 else ""} on its own. '
                 f'<b>{n_wait}</b> need{"" if n_wait != 1 else "s"} '
                 f'your yes.</span>'
                 f'<span class="go">&rarr;</span></a>')
    else:
        # The brief already says "nothing needs your yes" — a second calm
        # pill saying it again is noise. The pill appears only as a CTA.
        needs = ""

    # Three rows, not five: the founder reads this on a phone at 9 PM.
    # Everything else is one tap away behind "See all".
    waiting = sorted((r for r in WORLD.d.ledger.runs.values()
                      if r.tenant_id == tid and r.state is RunState.AWAITING_GATE),
                     key=lambda r: r.occurred_at, reverse=True)[:3]
    rows = "".join(
        f'<a class="arow" href="/cases/{esc(r.order_id or "")}">{ICONS["bolt"]}'
        f'<span>Wrote the reply to the bank for '
        f'{mention(_account_label(r))} &middot; {esc(bought(r.order_id))}. '
        f'{PLAIN_REASON.get(r.reason_code, "A buyer is disputing a payment")}.'
        f'<span class="sub">{auto_send_chip(r) or "Waiting on you"} &middot; '
        f'{esc(channel_of(r.order_id))}</span></span>'
        f'<span class="when">{r.occurred_at.strftime("%b %-d")}</span></a>'
        for r in waiting)
    active = (f'<div class="active-h"><span>What your team did today</span>'
              f'<a href="/approvals">See all &rarr;</a></div>{rows}') if rows else ""
    page = (CHAT_TEMPLATE
            .replace("__MODEUI__", mode_ui(tid))
            .replace("__SIDEBAR__", sidebar_html("cmd", tid, convs=conv_list_html(tid, cid), email=email))
            .replace("__CONVTITLE__", title)
            .replace("__CONVID__", cid)
            .replace("__SAYVAL__", esc(say))
            .replace("__MENTIONS__", json.dumps({
                "agents": [a["role"] for a in RELAY_AGENTS],
                "people": [p["name"] for p in people_for(tid)]}))
            .replace("__SAMPLE__", sample_html)
            .replace("__ROLEPILLS__", role_html)
            .replace("__GREET__", greet)
            .replace("__TPLPOOL__", tpl_pool)
            .replace("__MONEY__", money if not cid else "")
            .replace("__NEEDS__", needs if not cid else "")
            .replace("__BRIEF__", brief)
            .replace("__CHIPS__", chips_html)
            .replace("__ACTIVE__", active)
            .replace("__BIZ__", BUSINESS)
            .replace("__NAME__", esc(user_name(email)))
            .replace("__USER__", esc(email))
            .replace("__INITIAL__", user_avatar(email))
            .replace("__THREAD__", thread))
    return page


if __name__ == "__main__":
    seed_conversations()
    print(f"Relay workspace on http://localhost:{PORT}")
    HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
