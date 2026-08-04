"""The workspace, first design pass — run `uv run python demo/server.py`.

Serves the merchant operator view on http://localhost:8790 with the REAL pipeline
running on fakes underneath: every card is a genuine ledger row produced by
`Pipeline.handle_event`, and the buttons call the same `approve` / `edit` /
`reject` the Slack webhook will call. No JS framework, no external assets;
this page is a design surface for spec §7.3's components (QueueTable,
CounterCard, DiffView, metrics strip) before AG-UI streaming lands.
"""

from __future__ import annotations

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

# The six merchants on this Relay workspace. Defined here (ahead of
# build_world) since the seed data and the Team/roster pages both need the
# same canonical ids — a merchant is a merchant everywhere in this demo.
MERCHANTS = [
    ("m_loomcraft",  "Loomcraft Textiles",   "D2C apparel"),
    ("m_kavali",     "Kavali Kitchens",      "QSR"),
    ("m_verve",      "Verve Wellness",       "D2C skincare"),
    ("m_bumblebee",  "Bumblebee Mobility",   "D2C EV accessories"),
    ("m_sundar",     "Sundar Studio Prints", "Print-on-demand"),
    ("m_northgate",  "Northgate Fresh Mart", "Grocery"),
]
MERCHANT_IDS = [m[0] for m in MERCHANTS]


def _make_ledger():
    """Postgres-backed when the local cluster is up; in-memory otherwise —
    and it says which, loudly, at boot."""
    try:
        from demo.persist import PersistentLedger
        led = PersistentLedger()
        print(f"ledger: postgres (hydrated {len(led.runs)} runs)")
        return led
    except Exception as e:                                  # noqa: BLE001
        print(f"ledger: IN-MEMORY (postgres unavailable: {str(e)[:80]}) — "
              f"data will not survive restarts; run `make pg`")
        return Ledger()



# ---------------------------------------------------------------- seed world
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
        banned_terms=["best", "leading", "number one"], holdout_pct=0)
    crm = FakeCrm(opportunities={
        f"order_{i}": {"stage": "evaluation", "amount_band": b}
        for i, b in enumerate(
            ["under-1k", "1k-3k", "3k-10k", "under-1k", "1k-3k",
             "10k+", "1k-3k", "3k-10k", "1k-3k", "under-1k",
             "10k+", "3k-10k", "1k-3k", "under-1k", "1k-3k",
             "3k-10k", "1k-3k", "under-1k", "3k-10k", "1k-3k"], 1)})
    evidence = [
        EvidenceItem("ev_pod", "t1", "RG", "delivery_proof",
                     "Courier proof-of-delivery, signed and GPS-stamped at the doorstep",
                     "https://ours.example/pod"),
        EvidenceItem("ev_wa", "t1", "RG", "communication_log",
                     "WhatsApp delivery-confirmation thread with the buyer",
                     "https://ours.example/whatsapp-log"),
        EvidenceItem("ev_inv", "t1", "RD", "invoice",
                     "Original invoice matched to a single gateway transaction id",
                     "https://ours.example/invoice"),
        EvidenceItem("ev_bank", "t1", "RD", "communication_log",
                     "Bank settlement excerpt showing one debit, not two",
                     "https://ours.example/bank-note"),
        EvidenceItem("ev_listing", "t1", "RN", "invoice",
                     "Product listing snapshot matching the SKU that shipped",
                     "https://ours.example/listing"),
        EvidenceItem("ev_returnphotos", "t1", "RN", "communication_log",
                     "Buyer's own return-request photos matching the listed product",
                     "https://ours.example/return-photos"),
    ]
    deps = Deps(clock=clock, llm=ScriptedLlm(), crm=crm, slack=FakeSlack(),
                url_checker=FakeUrlChecker(), ledger=_make_ledger(), policy=policy,
                evidence=evidence, enrolled_merchants=set(MERCHANT_IDS))
    p = Pipeline(deps)

    def fire(ref, order, merchant, reason, text, claim, counter, cites):
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
                           ("merchant_not_enrolled", "lc_unenrolled"),
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
    fire("c_old", "order_6", "m_northgate",  "RD",
         "Same order billed twice on our card, please refund one.",
         "Buyer says the order was charged twice",
         "The gateway settlement shows a single successful debit for this order id; "
         "the invoice and the bank settlement excerpt both tie to one transaction, "
         "and the second attempt shown to the buyer never captured.",
         ["ev_inv", "ev_bank"])
    clock.advance(hours=25)
    Supervisor(deps.ledger, clock, deps.slack, policy).sweep()

    # 2) resolved win — approved 6 weeks ago, dispute won
    r = fire("c_won", "order_1", "m_loomcraft", "RG",
             "Buyer says the order never arrived.",
             "Buyer says the order never arrived",
             "Courier proof-of-delivery shows this order signed for and GPS-stamped "
             "at the delivery address on the date claimed; the WhatsApp thread with "
             "the buyer confirms receipt the same evening.",
             ["ev_pod", "ev_wa"])
    clock.advance(minutes=4)
    p.approve(r, "m_loomcraft")
    clock.advance(days=40)
    p.record_resolution(r, won=True, amount_paise=840_000)

    # 3) an edit (non-material) that landed and is still open
    r = fire("c_edit", "order_2", "m_kavali", "RD",
             "Buyer's bank shows two debits for one order.",
             "Buyer says they were charged twice",
             "The invoice and the payment gateway both show one transaction id for "
             "this order; the bank settlement excerpt confirms only one debit "
             "cleared, the second was an authorization that was never captured.",
             ["ev_inv", "ev_bank"])
    clock.advance(minutes=11)
    p.edit(r, "m_kavali",
           "Only one debit settled for this order — the gateway transaction id and "
           "the bank settlement excerpt both confirm it. The second attempt shown "
           "on the buyer's statement was an authorization hold that was never "
           "captured and will drop off within a few days.")

    # 4) rejected
    r = fire("c_rej", "order_3", "m_verve", "RN",
             "Buyer says the cream isn't the one they ordered.",
             "Buyer says the product doesn't match the listing",
             "The listing snapshot from the order date matches the SKU that "
             "shipped exactly; the buyer's own return-request photos show the "
             "same packaging and batch code as the listing.",
             ["ev_listing", "ev_returnphotos"])
    clock.advance(minutes=27)
    p.reject(r, "m_verve")

    # 5) suppressed — same order, same reason again inside the window
    fire("c_dup", "order_2", "m_kavali", "RD",
         "Buyer reopened the case — still says they were double charged.",
         "Buyer says they were charged twice", "unused", ["ev_inv"])

    # 6–8) three live cards awaiting review
    fire("c_live1", "order_4", "m_bumblebee", "RG",
         "Buyer says the scooter charger never showed up.",
         "Buyer says the order never arrived",
         "Courier proof-of-delivery shows the package signed for at the registered "
         "address two days before the dispute was filed; the WhatsApp thread has "
         "the buyer's own delivery-day confirmation message.",
         ["ev_pod"])
    clock.advance(minutes=9)
    fire("c_live2", "order_5", "m_sundar", "RD",
         "Buyer's statement shows the print order billed twice.",
         "Buyer says they were charged twice",
         "One transaction id, one settlement — the invoice and the bank excerpt "
         "for this order agree, and the duplicate line on the buyer's statement "
         "is a pending authorization that will reverse on its own.",
         ["ev_inv", "ev_bank"])
    clock.advance(minutes=6)
    fire("c_live3", "order_7", "m_northgate", "RN",
         "Buyer says the grocery box had substitutes they didn't approve.",
         "Buyer says the order doesn't match what was listed",
         "The listing snapshot and the packed-order photo the buyer sent back "
         "both show the same items; two SKUs were out of stock and substituted "
         "per the standing substitution policy on file for this account.",
         ["ev_listing", "ev_returnphotos"])
    # a quarter of activity across the merchant roster (claims and responses
    # cite the same evidence bank; every row passes the same checks as the
    # live path)
    RG_RESPONSE = ("Courier proof-of-delivery shows this order signed for at the "
                   "registered address, and the WhatsApp thread has the buyer's "
                   "own delivery-day confirmation — both attached.")
    RD_RESPONSE = ("The invoice and the payment gateway agree on one transaction "
                   "id for this order; the bank settlement excerpt confirms a "
                   "single debit, and the second line on the buyer's statement "
                   "is an authorization hold, not a charge.")
    QUARTER = [
        # ref, order, merchant, claim, reason, action, won_amount_paise
        ("q1",  "order_9",  "m_loomcraft", "Buyer says a second parcel never arrived",       "RG", "approve", 1_200_00),
        ("q2",  "order_10", "m_kavali",    "Buyer's card statement shows a duplicate line",  "RD", "approve",   650_00),
        ("q3",  "order_11", "m_verve",     "Buyer says the refill order was never delivered","RG", "approve",    None),
        ("q4",  "order_12", "m_bumblebee", "Buyer's bank flagged a repeat charge",           "RD", "edit",      450_00),
        ("q5",  "order_13", "m_sundar",    "Buyer says the print run never showed up",       "RG", "edit",       None),
        ("q6",  "order_14", "m_northgate", "Buyer says the delivery slot was a no-show",     "RG", "reject",     None),
        ("q7",  "order_15", "m_loomcraft", "Buyer's statement lists the order twice",        "RD", "approve",    None),
        ("q8",  "order_16", "m_kavali",    "Buyer says the order never left the kitchen",    "RG", "wait",       None),
        ("q9",  "order_9",  "m_loomcraft", "Buyer reopened — still says it's missing",       "RG", "wait",       None),
        ("q10", "order_11", "m_verve",     "Buyer's bank disputes the refill charge",        "RD", "wait",       None),
    ]
    for ref, order, merchant, claim, reason, action, won in QUARTER:
        clock.advance(hours=13)
        counter = RG_RESPONSE if reason == "RG" else RD_RESPONSE
        cites = ["ev_pod", "ev_wa"] if reason == "RG" else ["ev_inv", "ev_bank"]
        r = fire(ref, order, merchant, reason, claim + ".", claim, counter, cites)
        clock.advance(minutes=18)
        if action == "approve":
            p.approve(r, merchant)
        elif action == "edit":
            deps.llm.diff = {"changed": "tightened the numbers", "is_material": False,
                             "implies": "keep it concrete"}
            p.edit(r, merchant, counter + " Happy to share the raw settlement export too.")
        elif action == "reject":
            p.reject(r, merchant)
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

    # signal heard + gated (a merchant forwarding a chargeback email this time)
    clock.advance(hours=3)
    deps.llm.mention = {"is_competitive": True,
                        "claim_text": "Buyer says the order never arrived",
                        "confidence": .9}
    deps.llm.claim = {"claim_text": "Buyer says the order never arrived",
                      "speaker_role": "buyer", "confidence": .9}
    deps.llm.draft = {"counter_text":
        "The delivery address on file matches the courier's proof-of-delivery scan, "
        "signed the same afternoon the buyer filed; the WhatsApp confirmation "
        "thread from that day is attached, and neither shows any return request "
        "on record before the dispute.",
        "cited_evidence_ids": ["ev_pod", "ev_wa"], "confidence": .8,
        "escalate": False}
    r = p.handle_event(TriggerEvent(
        tenant_id="t1", source="email_forward", source_ref="m_lifecycle_1",
        occurred_at=clock.now(), order_id="order_17",
        merchant_id="m_bumblebee", dispute_id="dp_m_lifecycle_1", reason_code="RG",
        text="Forwarding the chargeback notice — buyer says the order never arrived."))
    mark("review", r)

    # a material edit, then filed
    clock.advance(hours=1)
    r2 = fire("lc_edit", "order_18", "m_kavali", "RD",
              "Buyer says two separate charges hit their card for one order.",
              "Buyer says two separate charges hit their card",
              "The invoice and the gateway transaction id agree on one charge for "
              "this order; the bank settlement excerpt confirms a single debit, "
              "and the duplicate line will drop off the buyer's statement.",
              ["ev_inv", "ev_bank"])
    clock.advance(minutes=25)
    deps.llm.diff = {"changed": "replaced the generic settlement language with the "
                     "buyer's own bank reference number", "is_material": True,
                     "implies": "quote the buyer's own reference number back"}
    p.edit(r2, "m_kavali",
           "Only one debit settled for this order — reference the gateway "
           "transaction id and the buyer's own bank reference number, both "
           "attached. The second line is an authorization hold, not a charge, "
           "and reverses automatically within 3-5 business days.")
    mark("edited", r2)

    # a loss, honestly recorded
    clock.advance(hours=2)
    r3 = fire("lc_lost", "order_19", "m_verve", "RN",
              "Buyer says the serum shipped is a different shade than ordered.",
              "Buyer says the product doesn't match the listing",
              "The listing snapshot and the batch code on the shipped unit "
              "match; the buyer's return photos show unopened packaging, which "
              "the shade-mismatch policy on file still credits in full.",
              ["ev_listing", "ev_returnphotos"])
    clock.advance(minutes=12)
    p.approve(r3, "m_verve")
    clock.advance(days=14)
    p.record_resolution(r3, won=False)
    mark("lost", r3)

    # QA blocks a draft that breaks the rules (banned superlative)
    clock.advance(hours=1)
    r4 = fire("lc_qa", "order_20", "m_sundar", "RG",
              "Buyer says the whole print order is missing.",
              "Buyer says the order never arrived",
              "Our delivery record is simply the best in the industry and "
              "everyone knows it; the numbers speak for themselves and the "
              "buyer is clearly wrong about the whole thing.",
              ["ev_pod"])
    mark("qa_blocked", r4)

    # triage suppressions, one per reason
    clock.advance(hours=1)
    deps.llm.mention = {"is_competitive": False, "claim_text": "", "confidence": .9}
    r5 = p.handle_event(TriggerEvent(
        tenant_id="t1", source="bank_webhook", source_ref="lc_noise",
        occurred_at=clock.now(), order_id="order_4",
        merchant_id="m_bumblebee", dispute_id="dp_lc_noise", reason_code="RG",
        text="Duplicate redelivery of a webhook already actioned — no new claim."))
    mark("not_actionable", r5)

    deps.llm.mention = {"is_competitive": True, "claim_text": "Buyer says the order never arrived",
                        "confidence": .9}
    r6 = p.handle_event(TriggerEvent(
        tenant_id="t1", source="bank_webhook", source_ref="lc_unenrolled",
        occurred_at=clock.now(), order_id="order_5",
        merchant_id="m_unlisted", dispute_id="dp_lc_unenrolled", reason_code="RG",
        text="Buyer says the order never arrived."))
    mark("merchant_not_enrolled", r6)

    r7 = p.handle_event(TriggerEvent(
        tenant_id="t1", source="bank_webhook", source_ref="lc_noopp",
        occurred_at=clock.now(), order_id="order_missing",
        merchant_id="m_northgate", dispute_id="dp_lc_noopp", reason_code="RD",
        text="Buyer's bank flagged a duplicate charge on an order we can't find."))
    mark("no_order", r7)

    r8 = p.handle_event(TriggerEvent(
        tenant_id="t1", source="bank_webhook", source_ref="lc_recent",
        occurred_at=clock.now(), order_id="order_18",
        merchant_id="m_kavali", dispute_id="dp_lc_recent", reason_code="RD",
        text="Buyer reopened the same duplicate-charge claim again."))
    mark("recently_countered", r8)

    # two shorter merchant sagas, for texture beyond the single-run exemplars
    clock.advance(days=2)
    r10 = fire("lc_1", "order_1", "m_loomcraft", "RG",
               "Buyer reopened: still says the replacement order never showed.",
               "Buyer says the replacement order never arrived",
               "The courier proof-of-delivery for the replacement shipment is "
               "signed and dated three days after the original claim; the "
               "WhatsApp thread has the buyer confirming receipt that evening.",
               ["ev_pod", "ev_wa"])
    clock.advance(minutes=31)
    p.reject(r10, "m_loomcraft")

    clock.advance(days=9)
    r11 = fire("lc_2", "order_1", "m_loomcraft", "RG",
               "Buyer's bank escalated: proof of delivery wasn't enough for them.",
               "Buyer disputes the delivery proof itself",
               "The courier's GPS stamp places the delivery at the registered "
               "address, and the buyer's own WhatsApp message thanking the "
               "rider is timestamped the same afternoon — both attached again "
               "for the bank's escalation review.",
               ["ev_pod", "ev_wa"])
    clock.advance(minutes=14)
    deps.llm.diff = {"changed": "tightened to the bank's escalation format",
                     "is_material": False, "implies": "mirror the bank's own language"}
    p.edit(r11, "m_loomcraft",
           "The courier's GPS-stamped delivery scan places this at the "
           "registered address, and the buyer's own WhatsApp message thanking "
           "the rider is timestamped the same afternoon. Both attached for the "
           "bank's escalation review.")

    clock.advance(days=16)         # past the 7-day suppress window
    r12 = fire("lc_3", "order_1", "m_loomcraft", "RG",
               "Final round: the bank asked for a signed acknowledgement too.",
               "Bank requests a signed delivery acknowledgement",
               "The courier's proof-of-delivery already carries a signature "
               "captured on the handheld device at drop-off; that signature "
               "scan is attached alongside the WhatsApp confirmation.",
               ["ev_pod", "ev_wa"])
    clock.advance(minutes=9)
    p.approve(r12, "m_loomcraft")

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

# The six merchants (defined near the top, alongside MERCHANT_IDS) live on
# this Relay workspace, plus the small Relay-side ops team who own
# escalations. Merchants are the "enrolled reviewers" here — each merchant's
# own ops contact is who approves (or edits, or dismisses) the response
# Dispute Defender drafts on their orders.
TEAM = (
    [(mid, name, f"Merchant · {category}", "Merchants", True)
     for mid, name, category in MERCHANTS]
    # Relay Ops (3) — own escalations, never see a queue of their own
    + [("ops_deepa",  "Deepa Krishnan", "Dispute Ops Lead", "Relay Ops", False),
       ("ops_farhan", "Farhan Sheikh",  "Merchant Success", "Relay Ops", False),
       ("ops_riya",   "Riya Kapoor",    "Support",          "Relay Ops", False)]
)
REP = {tid: name for tid, name, _, _, _ in TEAM}
STATE_META = {
    RunState.AWAITING_GATE: ("Awaiting review", "wait"),
    RunState.ACTED: ("Filed with the bank", "ok"),
    RunState.RESOLVED: ("Resolved", "ok"),
    RunState.EDITED: ("Edited & filed", "ok"),
    RunState.REJECTED: ("Dismissed", "mut"),
    RunState.TIMED_OUT: ("Escalated to ops", "warn"),
    RunState.SUPPRESSED: ("Suppressed", "mut"),
    RunState.FAILED: ("Escalated to ops", "warn"),
}
STEPS = ["safety", "retrieve", "draft", "check", "gate"]


def auth_page(mode: str, error: str = "") -> str:
    """Login/signup card. Same Inter + periwinkle language as the workspace;
    passwords go form → WorkOS over TLS, nothing stored here."""
    signup = mode == "signup"
    company = ('<label>Company<input name="company" required '
               'placeholder="Loomcraft Textiles"></label>') if signup else ""
    err = f'<div class="err">{esc(error)}</div>' if error else ""
    swap = (('Already have an account? <a href="/login">Log in</a>') if signup
            else ('New here? <a href="/signup">Create a workspace</a>'))
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Relay — {'Sign up' if signup else 'Log in'}</title>
<style>
*{{box-sizing:border-box;margin:0}}
body{{font-family:'Circular Std',-apple-system,BlinkMacSystemFont,'SF Pro Text',sans-serif;background:#FAFAFC;min-height:100vh;
  display:flex;align-items:center;justify-content:center;color:#1a1c23}}
.card{{background:#fff;border:1px solid #E6E7EE;border-radius:14px;
  padding:36px 32px;width:360px}}
.brand{{display:flex;align-items:center;gap:8px;margin-bottom:22px;font-weight:650}}
.logo{{display:inline-flex;width:26px;height:26px;border-radius:7px;background:#5266EB;
  color:#fff;align-items:center;justify-content:center;font-size:14px;font-weight:700}}
h1{{font-size:17px;font-weight:600;margin-bottom:18px}}
label{{display:block;font-size:12.5px;color:#5a5e6e;margin-bottom:12px}}
input{{display:block;width:100%;margin-top:5px;padding:9px 11px;font:inherit;
  font-size:14px;border:1px solid #E3E4EA;border-radius:9px;background:#fff;
  transition:border-color .15s,box-shadow .15s}}
input:hover{{border-color:#C9CBD6}}
input::placeholder{{color:#9A9DAB}}
input:focus{{outline:none;border-color:#98A5F0;box-shadow:0 0 0 3px rgba(82,102,235,.13)}}
{BTN_CSS}
.err{{background:#FDF2F2;border:1px solid #F5C6C6;color:#9b3535;font-size:12.5px;
  border-radius:8px;padding:8px 11px;margin-bottom:14px}}
.swap{{font-size:12.5px;color:#5a5e6e;margin-top:16px;text-align:center}}
.swap a{{color:#5266EB;text-decoration:none}}
.demo{{margin-top:10px;text-align:center}}
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
  <form class="demo" method="post" action="/auth/demo"><button class="btn ghost wide">Continue with the demo workspace</button></form>
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
<title>Relay — Verify your email</title>
<style>{style}
.code input{{font-size:22px;letter-spacing:8px;text-align:center;font-variant-numeric:tabular-nums}}
.hint{{font-size:12.5px;color:#5a5e6e;margin-bottom:14px}}
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


# One button system for every surface (decisions: consistency over variety).
BTN_CSS = """
.btn{font:inherit;font-size:13px;font-weight:500;border-radius:9px;padding:8px 15px;
  border:1px solid transparent;cursor:pointer;
  transition:background .12s,border-color .12s,color .12s}
.btn.primary{background:var(--accent,#5266EB);color:#fff}
.btn.primary:hover{background:#4557D6}
.btn.primary:active{background:#3D4EC4}
.btn.ghost{background:#fff;border-color:#E3E4EA;color:var(--text,#3A3D4D)}
.btn.ghost:hover{border-color:#C9CBD6;color:var(--ink,#1B1F30)}
.btn.ghost:active{background:#F5F5F8}
.btn.wide{width:100%;padding:10px;font-size:14px;font-weight:600;margin-top:6px}
.btn.sm{padding:5px 12px;font-size:12.5px}
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
    landed — the operator's proof-of-impact number."""
    led = WORLD.d.ledger
    total = 0
    for r in led.runs.values():
        if (r.tenant_id == tid and r.gate_action is not None
                and r.gate_action.value in ("approve", "edit")):
            out = led.outcome_for(r.run_id)
            if out and (out.outcome_value or {}).get("won"):
                total += (out.outcome_value or {}).get("amount_paise") or 0
    return total


def signals_html(tid: str = "t1") -> str:
    led = WORLD.d.ledger
    raw = [correction_rate(led, tid), counter_usage_rate(led, tid),
           trigger_precision(led, tid), gate_latency_p95_ms(led, tid)]
    if all(v is None for v in raw):
        return ""              # no numbers yet: show nothing, not an excuse
    won = influenced_won(tid)
    sig = ([("Disputes won", f"&#8377;{won/100000:.0f}k")] if won else []) + [
        ("Responses merchants used", fmt_pct(raw[1])),
        ("Real alerts", fmt_pct(raw[2])),
        ("Response time", _fmt_latency(raw[3])),
        ("Needed edits", fmt_pct(raw[0]))]
    return "".join(
        f'<div class="bm"><span>{ICONS["bm"]}{k}</span><i>{v}</i></div>' for k, v in sig)


def sidebar_html(active: str, tid: str = "t1", convs: str | None = None) -> str:
    led = WORLD.d.ledger
    n_wait = sum(1 for r in led.runs.values()
                 if r.state is RunState.AWAITING_GATE and r.tenant_id == tid)
    def nav(href, icon, label, extra="", key=""):
        cls = "nav active" if key == active else "nav"
        return f'<a class="{cls}" href="{href}">{ICONS[icon]}<span>{label}</span>{extra}</a>'
    return f"""
  <aside class="sidebar">
    <div class="brand"><span class="logo">C</span><b>Relay</b></div>
    <div class="brandline" style="font-size:11px;color:#8a8d9c;padding:0 14px 10px;line-height:1.35">Pre-built agents for payment and compliance ops. You approve before anything sends.</div>
    {nav("/", "home", "Home", key="cmd")}
    {nav("/approvals", "tasks", "Approvals", f'<span class="count">{n_wait}</span>' if n_wait else "", "tasks")}
    {nav("/projects", "folder", "Pipeline", key="projects")}
    {nav("/journeys", "bolt", "Journeys", key="journeys")}
    {nav("/impact", "ledger", "Impact", key="activity")}
    <hr class="side">
    {nav("/knowledge", "book", "Evidence", key="knowledge")}
    {nav("/workflows", "flow", "Playbooks", key="workflows")}
    {nav("/agents", "bot", "Agents", key="agents")}
    {nav("/shadow", "search", "Shadow trial", key="shadow")}
    <hr class="side">
    {nav("/settings", "gear", "Settings", key="settings")}
    <hr class="side">
    {convs if convs is not None else (lambda b: '<div class="navsec">Signals</div>' + b if b else '')(signals_html(tid))}
  </aside>"""


_LOGO_COLORS = ["#5266EB", "#E8590C", "#0CA678", "#B33AB3", "#E0A800",
                "#2B8A9E", "#D6336C", "#6741D9"]

# These six are fictional demo SMB merchants (not real Cashfree customers) —
# no real logos or domains, so every merchant renders as a letter-mark
# avatar. Real tenants' merchants arrive with their own display name from
# the onboarding flow and get the same letter-mark treatment automatically.
_DOMAIN_BY_NAME: dict[str, str] = {}


def _logo(label: str, size: int = 18) -> str:
    """Favicon when the merchant has a real domain on file; letter-mark
    fallback otherwise (every fictional demo merchant, today)."""
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


def _account_label(r) -> str:
    """A run's merchant id maps to its display name; anything unmapped
    (a raw domain, an unenrolled test id) passes through as-is."""
    a = r.merchant_id or ""
    return REP.get(a, a)


def task_row(r) -> str:
    reason = esc(COMP.get(r.reason_code, r.reason_code))
    merchant = esc(REP.get(r.merchant_id, r.merchant_id))
    when = r.occurred_at.strftime("%b %-d")
    rid = r.run_id
    counter = esc((r.decision or {}).get("counter_text", ""))
    return f"""
<div class="taskwrap">
  <div class="trow">
    <input type="checkbox" class="selrun" value="{rid}" onclick="bulksync()">
    <span class="ico">{ICONS["bolt"]}</span>
    <span class="tdesc">Approve response to a <b>{reason}</b> dispute
      <span class="stream">&ldquo;{esc(r.claim_text)}&rdquo;</span> <span class="mut">merchant <b>{merchant}</b> &middot;
      {_logo(_account_label(r))}{esc(_account_label(r))}</span></span>
    <span class="tacts">
      <form method="post" action="/act"><input type="hidden" name="run" value="{rid}">
        <button class="btn primary" name="action" value="approve">Approve</button>
        <input class="whyin" name="why" maxlength="200" placeholder="why? (optional)">
        <button class="btn ghost" name="action" value="reject">Dismiss</button>
      </form>
      <button class="btn ghost" onclick="toggleEdit('e-{rid}')">Edit</button>
    </span>
    <span class="when">{when}</span>
  </div>
  <div class="editbox" id="e-{rid}" hidden>
    <div class="draft">{counter}</div>
    <form method="post" action="/act">
      <input type="hidden" name="run" value="{rid}">
      <textarea name="text" rows="3">{counter}</textarea>
      <button class="btn primary" name="action" value="edit">Send edited</button>
    </form>
  </div>
</div>"""


def ledger_row(r) -> str:
    label, cls = STATE_META.get(r.state, (r.state.value, "mut"))
    extra = f' &middot; {esc(r.suppressed_reason)}' if r.state is RunState.SUPPRESSED else ""
    out = WORLD.d.ledger.outcome_for(r.run_id)
    won = ""
    if out:
        v = out.outcome_value
        amt = v.get("amount_paise")
        won = (f'<span class="st ok">won{f" &#8377;{amt/100000:.0f}k" if amt else ""}</span>'
               if v.get("won") else '<span class="st warn">lost</span>')
    mat = ""
    if r.gate_action is GateAction.EDIT:
        mat = ('<span class="st mut">edit &middot; style</span>' if r.gate_is_material is False
               else '<span class="st warn">edit &middot; material</span>')
    return (f'<a class="trow slim" href="/runs/{r.run_id}"><span class="ico">{ICONS["ledger"]}</span>'
            f'<span class="tdesc"><b>{esc(COMP.get(r.reason_code, "&ndash;"))}</b> '
            f'<span class="stream">&ldquo;{esc(r.claim_text) if r.claim_text else "&mdash;"}&rdquo;</span> '
            f'<span class="mut">{esc(REP.get(r.merchant_id, r.merchant_id or "&ndash;"))}</span></span>'
            f'{mat}{won}<span class="st {cls}">{label}{extra}</span>'
            f'<span class="when">{r.occurred_at.strftime("%b %-d")}</span></a>')



HOME_CONTENT = """
    <h1 class="page" id="tasks">Approvals</h1>
    <div class="pagehint">Nothing gets filed with the bank without your yes.
      <form method="post" action="/api/sample" style="display:inline;margin-left:8px">
      <button class="btn ghost sm">Simulate a dispute webhook</button></form></div>
    <div id="bulkbar" class="bulkbar" hidden>
      <b id="bcount"></b>
      <input id="bwhy" class="whyin" style="width:190px" maxlength="200"
             placeholder="why? (applies to dismissals)">
      <button class="btn primary sm" onclick="bulk('approve', this)">Approve selected</button>
      <button class="btn ghost sm" onclick="bulk('reject', this)">Dismiss selected</button>
      <span class="mut" style="font-size:12px">j/k move &middot; x select &middot; a approve &middot; d dismiss &middot; e edit</span>
    </div>
    __TASKS__"""

def render(tid: str = "t1", email: str = "") -> str:
    led = WORLD.d.ledger
    runs = sorted((r for r in led.runs.values() if r.tenant_id == tid),
                  key=lambda r: r.occurred_at, reverse=True)
    waiting = [r for r in runs if r.state is RunState.AWAITING_GATE]
    tasks = "\n".join(task_row(r) for r in waiting) or (
        '<div class="empty">All clear &mdash; nothing needs review.</div>')
    return (TEMPLATE
            .replace("__CONTENT__", HOME_CONTENT)
            .replace("__SIDEBAR__", sidebar_html("tasks", tid))
            .replace("__USER__", esc(email))
            .replace("__INITIAL__", esc((email or "?")[0]))
            .replace("__NWAIT__", str(len(waiting)))
            .replace("__TASKS__", tasks))


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
border-right:1px solid #ECECF1;padding:14px 12px;overflow-y:auto}
.brand{display:flex;align-items:center;gap:10px;padding:8px 10px;margin-bottom:12px}
.logo{width:26px;height:26px;border-radius:8px;background:#21232E;color:#fff;font-weight:700;
font-size:13px;display:grid;place-items:center}
.brand b{font-size:14px;color:var(--ink);font-weight:600}
.pro{margin-left:auto;background:#21232E;color:#fff;font-size:10.5px;font-weight:600;
border-radius:6px;padding:2px 7px}
.nav{display:flex;align-items:center;gap:11px;padding:8px 10px;border-radius:8px;
color:var(--text);font-size:13.5px;margin-bottom:1px}
.nav svg{width:16px;height:16px;color:#6A6D7D;flex:none}
.nav:hover{background:#F0F0F5}
.nav.active{background:var(--accent-soft);color:var(--ink);font-weight:500}
.nav.active svg{color:var(--ink)}
.nav .count{margin-left:auto;color:var(--mut);font-size:12.5px}
.nav .new{margin-left:auto;background:#E3E6F0;color:#4A4E63;font-size:10.5px;font-weight:600;
border-radius:6px;padding:2px 7px}
hr.side{border:none;border-top:1px solid #ECECF1;margin:10px 0}
.navsec{margin:6px 10px 8px;font-size:12px;font-weight:600;color:var(--mut)}
.bm{padding:5px 10px}
.bm span{font-size:13px;color:var(--text);display:flex;gap:11px;align-items:center}
.bm span svg{width:15px;height:15px;color:#6A6D7D}
.bm i{font-style:normal;font-size:12.5px;color:var(--mut);padding-left:26px;display:block}
.main{margin-left:250px;min-height:100vh;background:linear-gradient(#FDFDFE,#F4F5F9)}
.topbar{display:flex;align-items:center;padding:20px 44px;color:var(--mut);font-size:13.5px}
.topbar .search input{border:none;outline:none;background:none;font:inherit;font-size:13.5px;color:var(--ink);width:220px}
.search input::placeholder{color:#9A9DAB}
.search{display:flex;gap:9px;align-items:center;cursor:pointer}
.topbar .search svg{width:15px;height:15px}
.topbar .right{margin-left:auto;display:flex;gap:14px;align-items:center}
.avatar{width:26px;height:26px;border-radius:50%;background:#5266EB;color:#fff;
  display:inline-flex;align-items:center;justify-content:center;
  font-size:12px;font-weight:650;text-transform:uppercase}
.topbar .logout{color:var(--mut);text-decoration:none}
.topbar .logout:hover{color:#1a1c23}
.content{max-width:1120px;margin:0 auto;padding:10px 44px 90px}
.alogo{display:inline-flex;width:18px;height:18px;border-radius:5px;color:#fff;
  font-size:10.5px;font-weight:700;align-items:center;justify-content:center;
  vertical-align:-4px;margin:0 5px 0 2px}
img.alogo{background:#fff;border:1px solid var(--hair);object-fit:contain;padding:1px}
h1.page{font-size:32px;font-weight:450;color:var(--ink);letter-spacing:-.01em;margin:8px 0 16px}
.pagehint{color:var(--mut);font-size:13.5px;margin:-6px 0 18px}
.impactline{background:#E5F4EC;color:#177245;border-radius:10px;padding:11px 16px;
  font-size:13.5px;margin-bottom:14px}
.impactline b{font-weight:650}
.pills{display:flex;gap:8px;margin:0 0 4px}
.fpill{font-size:13.5px;padding:7px 14px;border-radius:9px;background:var(--accent-soft);
color:var(--ink);font-weight:500}
.fpill.off{background:#fff;border:1px solid #E3E4EA;color:var(--text);font-weight:400}
.cols-h{display:flex;font-size:12.5px;color:var(--mut);padding:16px 0 9px;
border-bottom:1px solid var(--hair)}
.cols-h .right{margin-left:auto}
.taskwrap{border-bottom:1px solid #EEEFF3}
.trow{display:flex;align-items:center;gap:13px;row-gap:10px;padding:17px 0;
font-size:14.5px;color:#26293A;flex-wrap:wrap}
.trow.slim{border-bottom:1px solid #EEEFF3;padding:14px 0;font-size:14px}
.trow .ico{width:16px;height:16px;color:#6A6D7D;flex:none}
.trow .ico svg{width:16px;height:16px;display:block}
.trow .ico.warn-i{color:#B47816}
.tdesc{flex:1 1 340px;min-width:240px;line-height:1.5}
.tdesc b{font-weight:600;color:var(--ink)}
.mut{color:var(--mut)}
.tacts{display:flex;gap:7px;align-items:center;flex:none;opacity:.92;margin-left:auto}
.tacts form{display:flex;gap:7px}
.when{color:#5A5D6D;font-size:13.5px;flex:none;width:52px;text-align:right}
""" + BTN_CSS + """
.bulkbar{display:flex;gap:10px;align-items:center;margin:14px 0 2px;
padding:10px 14px;border:1px solid #DFDBFA;background:#F4F3FE;border-radius:10px}
.taskwrap.kfocus{background:#FAFAFE;box-shadow:inset 3px 0 0 var(--accent)}
.selrun{width:15px;height:15px;accent-color:var(--accent);flex:none;cursor:pointer}
.rolepills{display:flex;gap:6px;align-items:center;justify-content:center;
margin:0 0 14px;font-size:12.5px;color:var(--mut)}
.rolepills a{padding:4px 12px;border-radius:999px;border:1px solid var(--hair);
color:var(--text);text-decoration:none;font-weight:500}
.rolepills a.on{background:var(--accent);border-color:var(--accent);color:#fff}
.sechead{display:flex;align-items:center;justify-content:space-between}
.editbox{padding:2px 0 16px 29px}
.editbox .draft{display:none}
/* one form-control system: same border, radius, hover, focus ring and
   placeholder as the button system — sizes vary, states never do */
.jfind,.whyin,.editbox textarea{background:#fff;border:1px solid #E3E4EA;
border-radius:9px;color:#26293A;font:inherit;
transition:border-color .15s,box-shadow .15s}
.jfind:hover,.whyin:hover,.editbox textarea:hover{border-color:#C9CBD6}
.jfind:focus,.whyin:focus,.editbox textarea:focus{outline:none;
border-color:#98A5F0;box-shadow:0 0 0 3px rgba(82,102,235,.13)}
.jfind::placeholder,.whyin::placeholder,.editbox textarea::placeholder{color:#9A9DAB}
.jfind:disabled,.whyin:disabled,.editbox textarea:disabled{opacity:.45;cursor:not-allowed}
.editbox textarea{width:100%;font-size:13.5px;padding:10px 12px;margin-bottom:8px}
h2.sec{font-size:15px;font-weight:600;color:var(--ink);margin:38px 0 2px}
.st{font-size:12px;font-weight:500;border-radius:12px;padding:3px 10px;white-space:nowrap;flex:none}
.st.ok{background:#E5F4EC;color:#177245}
.st.warn{background:#FCEED8;color:#9A6215}
.st.wait{background:var(--accent-soft);color:#4553C8}
.st.mut{background:#EFEFF3;color:#6A6D7D}
.empty{color:var(--mut);padding:18px 2px;font-size:13.5px}
.atoolbar{display:flex;gap:12px;margin:4px 0 16px}
.atoolbar .search-in{flex:1;max-width:420px;display:flex;gap:9px;align-items:center;
  background:#fff;border:1px solid var(--hair);border-radius:10px;padding:9px 13px}
.atoolbar .search-in svg{width:15px;height:15px;color:var(--mut)}
.atoolbar .search-in input{border:none;outline:none;font:inherit;font-size:13.5px;
  width:100%;background:none;color:var(--ink)}
.seg{display:flex;background:var(--pill);border-radius:10px;padding:3px}
.seg a{font-size:13px;font-weight:500;padding:6px 14px;border-radius:8px;color:var(--mut)}
.seg a.on{background:#fff;color:var(--ink);box-shadow:0 1px 3px rgba(27,31,48,.08)}
.atable{background:#fff;border:1px solid var(--hair);border-radius:12px;overflow:hidden}
.atable .thead{display:grid;grid-template-columns:2.2fr 1fr 1fr 1fr 60px;
  padding:11px 18px;font-size:11.5px;font-weight:600;letter-spacing:.05em;
  text-transform:uppercase;color:var(--mut);background:#FAFAFC;
  border-bottom:1px solid var(--hair)}
.atable .arow2{display:grid;grid-template-columns:2.2fr 1fr 1fr 1fr 60px;
  align-items:center;padding:13px 18px;border-bottom:1px solid #F1F1F5;
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
.tabbar{display:flex;gap:4px;border-bottom:1px solid var(--hair);margin:18px 0 22px}
.tabbar a{padding:9px 14px;font-size:13.5px;font-weight:500;color:var(--mut);
  border-bottom:2px solid transparent;margin-bottom:-1px}
.tabbar a.on{color:var(--accent);border-bottom-color:var(--accent)}
.tabbar a:hover{color:var(--ink)}
.twopane{display:grid;grid-template-columns:300px 1fr;gap:16px;align-items:start}
.pane-list .pitem{display:flex;gap:11px;align-items:center;background:#fff;
  border:1px solid var(--hair);border-radius:10px;padding:12px 14px;margin-bottom:8px;
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
  padding:18px 20px;min-height:120px}
.pane-detail h3{font-size:14.5px;font-weight:600;color:var(--ink);margin-bottom:6px}
.pane-detail p{font-size:13.5px;color:var(--text);line-height:1.6}
.trow{flex-wrap:wrap;row-gap:6px}
.trow .tdesc{min-width:min(320px,100%)}
.rgt{display:flex;gap:8px;align-items:center;margin-left:auto;flex-wrap:wrap}
.jbtime{display:block;font-size:11px;color:var(--mut);margin-top:5px;text-align:right;max-width:460px}
.jtabs{display:flex;gap:8px;margin:6px 0 18px;flex-wrap:wrap}
.jtab{display:flex;gap:8px;align-items:center;font-size:13.5px;font-weight:500;
  padding:8px 16px;border-radius:10px;background:var(--pill);color:var(--text)}
.jtab.on{background:#21232E;color:#fff}
.jtab.on .alogo{border-color:transparent}
.jtab:hover:not(.on){background:#E6E7EC}
.jbubble{display:inline-block;background:#21232E;color:#fff;border-radius:12px;
  padding:10px 14px;font-size:13px;line-height:1.5;max-width:460px}
.jwrap{margin:20px 0 6px}
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
.jback{display:inline-block;margin:0 0 14px;color:var(--mut);font-size:13px;font-weight:600;text-decoration:none}
.jback:hover{color:var(--ink)}
.jfind{width:100%;max-width:440px;margin:6px 0 18px;padding:9px 14px;font-size:14px}
.jgoalrow{cursor:pointer}
.shstats{display:flex;gap:14px;margin:18px 0 8px}
.shstat{flex:1;border:1px solid var(--hair);border-radius:12px;padding:14px 18px}
.shstat .n{font-size:26px;font-weight:700;color:var(--ink);font-variant-numeric:tabular-nums}
.shstat .l{font-size:12px;color:var(--mut);margin-top:2px}
.dgridwrap{overflow-x:auto;margin-top:16px;border:1px solid #E4E5EC;border-radius:10px}
.dgrid{width:100%;border-collapse:collapse;font-size:13.5px;min-width:980px}
.dgrid th{position:sticky;top:0;background:#F7F7FA;border-bottom:1px solid #E4E5EC;
border-right:1px solid #ECEDF2;padding:10px 13px;font-weight:600;color:var(--ink);
text-align:left;font-size:12.5px;white-space:nowrap}
.dgrid th.ghost{color:#A6ACBB;font-weight:500}
.dgrid td{border-bottom:1px solid #F0F1F5;border-right:1px solid #F0F1F5;
padding:9px 13px;vertical-align:middle;background:#fff;height:46px;line-height:1.45}
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
.gridcount{font-size:12px;color:var(--mut);margin:12px 2px -6px}
.gmore{margin-top:12px}
.rowin{animation:rowin .45s ease both}
@keyframes rowin{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:none}}
.shband{margin-top:22px;padding:16px 20px;border:1px solid #DFDBFA;background:#F4F3FE;
border-radius:12px;display:none;align-items:center;gap:14px;justify-content:space-between}
.shband.on{display:flex}
.shband b{color:#4A3FD6}
.notebar{display:flex;gap:10px;align-items:stretch;margin:14px 0 4px}
.notebar .notein{margin:0;max-width:560px;flex:1}
.notebar .btn{padding-top:0;padding-bottom:0;display:inline-flex;align-items:center}
.whyin{width:130px;padding:8px 10px;font-size:12.5px}
.ehist{max-width:660px;margin-top:6px}
.eh-item{position:relative;padding:0 0 22px 34px}
.eh-item:not(:last-child):after{content:"";position:absolute;left:7px;top:20px;bottom:-2px;width:2px;background:#ECEEF3}
.eh-node{position:absolute;left:1px;top:3px;width:12px;height:12px;border:2px solid #D0D5DE;border-radius:50%;background:#fff}
.eh-head{display:flex;align-items:center;gap:8px;font-size:14px;color:var(--ink)}
.eh-av{width:22px;height:22px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;font-size:9.5px;font-weight:700;color:#fff;flex:none}
.eh-when{color:var(--mut);font-size:12.5px;font-weight:500}
.eh-line{margin:7px 0 0 30px;display:flex;align-items:center;gap:7px;flex-wrap:wrap}
.eh-verb{color:#3F4A5C;font-size:13.5px;font-weight:600}
.echip{padding:3px 11px;border-radius:8px;font-size:12.5px;font-weight:600;border:1px solid transparent}
.ec-purple{background:#F1EBFE;color:#6B3FD6;border-color:#E4D9FB}
.ec-pink{background:#FDE7F1;color:#C0316E;border-color:#F9D3E4}
.ec-blue{background:#E5EEFB;color:#2E5AAC;border-color:#D3E2F7}
.ec-green{background:#E3F5EA;color:#1F7A46;border-color:#CDEBD9}
.ec-amber{background:#FBF3D8;color:#8A6A12;border-color:#F3E6B8}
.ec-orange{background:#FDEDDE;color:#B05E1E;border-color:#F8DDC2}

.jaxis{stroke:var(--hair)}
.jhalo{fill:none;stroke:var(--accent);stroke-width:1.2;opacity:0;transition:opacity .15s}
.jcard{display:grid;grid-template-columns:130px 1fr;gap:20px;background:#fff;
  border:1px solid var(--hair);border-radius:12px;padding:18px 20px;margin-top:12px}
.jday2{font-size:11px;letter-spacing:.08em;color:var(--mut);font-weight:600;margin:6px 0 2px}
.jtime{font-size:21px;font-weight:600;color:var(--ink)}
.jcard h3{font-size:15px;font-weight:600;color:var(--ink);margin-bottom:10px}
.jkv{display:grid;grid-template-columns:130px 1fr;gap:6px 14px;font-size:13px}
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
  text-transform:uppercase;margin-bottom:5px}
.dhead{display:flex;align-items:center;gap:14px;margin:6px 0 2px}
.dhead .back2{display:flex;align-items:center;justify-content:center;
  width:38px;height:38px;flex:none;border-radius:10px;border:1px solid var(--hair);
  background:#fff;color:var(--text);font-size:20px;transition:background .12s}
.dhead .back2:hover{background:var(--pill);color:var(--ink)}
.dhead .back2:active{background:#DEDFE6}
.dhead .tile{width:44px;height:44px}
.dhead .meta{font-size:12.5px;color:var(--mut);margin-top:3px;display:flex;gap:7px;align-items:center}
.dhead h1{font-size:22px;font-weight:600;color:var(--ink);margin:0}
.agchips{display:flex;gap:5px;flex-wrap:wrap}
.agchip{display:inline-flex;width:26px;height:26px;border-radius:7px;
  background:var(--accent-soft);color:var(--accent);align-items:center;
  justify-content:center;border:1px solid transparent}
.agchip svg{width:14px;height:14px}
.agchip:hover{border-color:#98A5F0}
.agchip.off{background:var(--pill);color:var(--mut)}
.agrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));
  gap:14px;margin:6px 0 26px}
.acard{background:#fff;border:1px solid var(--hair);border-radius:12px;
  padding:18px 18px 15px;display:flex;flex-direction:column;gap:10px}
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
    <label class="search">""" + ICONS["search"] + """<input id="pgfilter" placeholder="Filter this page&hellip;  ( / )"></label>
    <div class="right"><span>__USER__</span><a class="logout" href="/logout">Log out</a><span class="avatar">__INITIAL__</span></div>
  </div>
  <div class="content">__CONTENT__</div>
</div>
<script>
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
  if (c) c.textContent = 'Showing ' + n + ' of ' + rows.length + ' merchants';
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
  if (!f) return;
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
def _shell(content: str, active: str, tid: str, email: str) -> str:
    return (TEMPLATE
            .replace("__CONTENT__", content)
            .replace("__SIDEBAR__", sidebar_html(active, tid))
            .replace("__USER__", esc(email))
            .replace("__INITIAL__", esc((email or "?")[0])))


def _card(title: str, sub: str, right: str = "") -> str:
    return (f'<div class="trow slim"><span class="ico">{ICONS["bolt"]}</span>'
            f'<span class="tdesc"><b>{title}</b> <span class="mut">{sub}</span></span>'
            f'{right}</div>')


# Relay's eight real public agents (the canonical intro-deck lineup, verbatim
# one-liners). Dispute Defender is the only one wired to a live pipeline in
# this codebase; the other seven are named here so the console can show the
# whole fleet honestly, badged as roadmap.
RELAY_AGENTS = [
    dict(slug="dispute_defender", name="Dispute Defender", icon="shield",
        status="live",
        persona="For the support lead at an online merchant.",
        desc="Gathers the evidence, builds the case, and files before the "
             "deadline.",
        tagline="Never miss a deadline &middot; Evidence auto-gathered (order, "
                "delivery, comms) &middot; More disputes won."),
    dict(slug="cod_guard", name="COD Guard", icon="note",
        status="roadmap",
        persona="For the ops lead at a COD-heavy D2C brand.",
        desc="Confirms each COD order before dispatch via WhatsApp or voice, "
             "offers a prepaid option to flagged orders, and blocks repeat "
             "bad addresses.",
        tagline="Fewer returns-to-origin before they cost a courier run."),
    dict(slug="payment_rescue", name="Payment Rescue", icon="bolt",
        status="roadmap",
        persona="For the growth lead at a UPI-heavy online store.",
        desc="Reads the decline reason. Waits 3 minutes for self-retry. If "
             "still open, places a voice call explaining what failed and "
             "sends a WhatsApp link to retry.",
        tagline="Failed payments recovered while the intent is still warm."),
    dict(slug="cart_rescue", name="Cart Rescue", icon="tasks",
        status="roadmap",
        persona="For the growth lead at an ad-spending D2C brand.",
        desc="On a cart drop, calls the buyer in their language within "
             "minutes, presents the offer, and sends a Cashfree payment link.",
        tagline="Outcome-based pricing, charged only on recovered orders. "
                "TRAI-compliant."),
    dict(slug="settlement_clarity", name="Settlement Clarity", icon="ledger",
        status="roadmap",
        persona="For the finance lead at a multi-channel merchant.",
        desc="Matches each payout to its order against the bank, flags the "
             "gaps, and posts to your books.",
        tagline="Bank-verified match &middot; silent failures caught."),
    dict(slug="refund_shield", name="Refund Shield", icon="moon",
        status="roadmap",
        persona="For the risk lead at a high-refund D2C brand.",
        desc="Scores each claim against fraud signals, holds the risky ones "
             "before you pay.",
        tagline="Cross-merchant fraud signals."),
    dict(slug="loan_recovery", name="Loan Recovery", icon="send",
        status="roadmap",
        persona="For the collections lead at an NBFC or lender.",
        desc="On a bounce, verifies the borrower, states the EMI, captures "
             "consent, and takes the agreed action.",
        tagline="Every step on record."),
    dict(slug="due_diligence", name="Due Diligence", icon="lock",
        status="roadmap",
        persona="For the compliance lead at an LSP or lender.",
        desc="On a new application, verifies on Secure ID, risk-tiers the "
             "customer, auto-clears the low-risk, and escalates for enhanced "
             "review.",
        tagline="Compliance throughput without compliance risk."),
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


# Journey scenarios: reference-style goal names per merchant saga.
GOAL_META = {
    "m_loomcraft": ("Repeat RG defense",
                    "Hold Loomcraft Textiles&rsquo; goods-not-received win rate through the bank&rsquo;s escalation."),
    "m_kavali":    ("Duplicate-charge cleanup",
                    "Clear Kavali Kitchens&rsquo; duplicate-charge disputes before the next settlement cycle."),
    "m_verve":     ("Not-as-described review",
                    "Get Verve Wellness&rsquo; not-as-described disputes resolved without a chargeback fee."),
    "m_bumblebee": ("Delivery proof coverage",
                    "Make sure Bumblebee Mobility&rsquo;s delivery proof reaches every goods-not-received claim."),
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
                       '<span class="mut">Relay&rsquo;s other seven agents &mdash; '
                       'see the full fleet on the Agents console.</span>',
                       '<span class="st mut">not yet wired in this demo</span>',
                       '<span class="st wait">roadmap</span>', ""]),
    ]
    body = (_tbl(PCOLS, ["Playbook", "What it does", "Staffed by", "Status",
                         "Activity"], play_rows)
            + '<div class="pagehint" style="margin-top:10px">Playbooks share their '
              'crew &mdash; the same sub-agents staff every run, carrying what '
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
         "No matching order, duplicate claim, merchant over cap &mdash; each no "
         "is a logged row with its reason, not a silent drop.",
         f"{n_sup} filtered out", "recently_countered"),
        ("pen", "3 · A response is drafted and checked",
         "Written from your evidence library in the merchant&rsquo;s voice, then "
         "compliance blocks anything with dead links, invented facts or hype "
         "before a human sees it.",
         f"{n_drafted} drafted &middot; {n_blocked} blocked or escalated", "qa_blocked"),
        ("tasks", "4 · You decide",
         "The merchant gets the card. Approve, edit or dismiss &mdash; nothing "
         "files without a yes. Unanswered cards escalate to ops, never auto-file.",
         f"{n_wait} waiting now", "edited"),
        ("note", "5 · The response is filed with the bank",
         "Approved responses are filed onto the order exactly once, with the "
         "evidence attached.", f"{n_acted} filed", "approved"),
        ("chart", "6 · The outcome comes back",
         "Days later the bank settles the dispute and the result attaches to the "
         "same run &mdash; wins, losses, and what your edits taught the drafter.",
         f"&#8377;{won/100000:.0f}k won", "won"),
    ]
    SCOLS = "2.6fr 1.2fr 1.1fr"
    def stage_row(icon, title, desc, stat, ex_key):
        rid = EXEMPLARS.get(ex_key)
        ex_run = led.runs.get(rid) if rid else None
        link = (f'<a class="st wait" href="/runs/{rid}">'
                f'see example journey &rarr;</a>'
                if ex_run is not None and ex_run.tenant_id == tid else
                '<span class="mut">&mdash;</span>')
        ag = STAGE_AGENT.get(icon)
        who = (f'<a class="st wait" href="/agents/{ag}">{ag}</a>' if ag
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
            f'by stage &mdash; every count below is real rows in this '
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


EV_NOTES: list = []       # (tenant_id, actor, when-str, text) — demo store


def knowledge_content(tid: str) -> str:
    ev = [e for e in WORLD.d.evidence if e.tenant_id == tid]
    notes = [m for m in WORLD.d.ledger.memory if m.tenant_id == tid
             and m.superseded_by is None]
    ECOLS = "1fr 1fr 3fr .8fr"
    ev_rows = _tbl(ECOLS, ["Dispute reason", "Evidence type", "What it proves", "Source"],
                   [_trow2(ECOLS, [
                       f"<b>{esc(COMP.get(e.reason_code, e.reason_code))}</b>",
                       f'<span class="st mut">{esc(e.evidence_type)}</span>',
                       f'<span class="mut">{esc(e.text)}</span>',
                       f'<a class="st wait" href="{esc(e.source_url)}">source &rarr;</a>'])
                    for e in ev])
    NCOLS = "1.2fr 3fr"
    note_rows = _tbl(NCOLS, ["Merchant", "What their edits taught the drafter"],
                     [_trow2(NCOLS, [
                         f"<b>{esc(REP.get(m.subject_id, m.subject_id))}</b>",
                         f'<span class="mut">{esc((m.body or {}).get("implies") or (m.body or {}).get("changed") or "style note")}</span>'])
                      for m in notes])
    return (f'<h1 class="page">Evidence vault</h1>'
            f'<div class="pagehint">Evidence packs by reason code. Every '
            f'dispute response cites from here &mdash; no claim ships that '
            f'you didn&rsquo;t arm it with.</div>'
            f'<h2 class="sec" id="knowledge">Evidence packs</h2>{ev_rows}'
            f'<h2 class="sec">Your merchants&rsquo; voice &mdash; learned from their edits</h2>{note_rows}'
            f'<h2 class="sec">Edit history</h2>'
            f'<div class="pagehint">The evidence vault is governed: every change '
            f'has an author, a timestamp, and an approval behind it.</div>'
            + _edit_history([(a, w, "Commented", [(t[:64], "ec-blue")])
                             for tn, a, w, t in reversed(EV_NOTES) if tn == tid] + [
                ("Deepa Krishnan", "Jul 28, 4:12 PM", "Added",
                 [("Courier POD feed v2", "ec-purple"), ("GPS-stamp attachment", "ec-blue")]),
                ("Autopilot proposal", "Jul 21, 9:05 AM", "Approved by Deepa Krishnan",
                 [("Duplicate-charge bank excerpt refresh", "ec-green")]),
                ("Farhan Sheikh", "Jul 12, 3:08 PM", "Removed",
                 [("Expired refund-policy PDF", "ec-amber")]),
                ("Deepa Krishnan", "Jul 12, 3:07 PM", "Added",
                 [("WhatsApp comms-log export", "ec-blue"), ("Listing snapshot archive", "ec-orange")]),
                ("Riya Kapoor", "Jul 2, 6:24 PM", "Added",
                 [("Invoice-to-transaction matcher", "ec-pink")])])
            + f'<form class="notebar" method="post" action="/api/evnote">'
            f'<input class="jfind notein" name="text" maxlength="200" '
            f'placeholder="Comment on the evidence vault &mdash; e.g. a proof that went stale">'
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
            orders.setdefault(r.order_id or f"merchant:{r.merchant_id}", []).append(r)
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
        value = f"&#8377;{won_amt / 100:,.0f}" if won_amt else '<span class="mut">&mdash;</span>'
        last = max(r.occurred_at for r in runs).strftime("%b %-d")
        entries.append((0 if waiting else (1 if not outs else 2),
                        _trow2(PCOLS, [
            f'{_logo(label)}<b>{esc(label)}</b>',
            stage,
            f'<span class="mut">{esc(", ".join(reasons)) or "&mdash;"}</span>',
            f'{len(runs)} dispute{"s" if len(runs) != 1 else ""} on this order',
            value,
            f'<span class="mut">{last}</span>',
            f'<a class="st wait" href="/journeys?a={esc(r0.merchant_id)}">journey &rarr;</a>'])))
    rows = [h for _, h in sorted(entries, key=lambda t: t[0])]
    table = _tbl(PCOLS, ["Order", "Stage", "Dispute reason",
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
    rails = [("Anthropic (drafting models)", "anthropic"),
             ("Slack (gate cards)", "slack-bot"),
             ("Slack signing (button security)", "slack-signing"),
             ("Fathom (reference trigger, from the fork)", "fathom-webhook"),
             ("Gmail (email trigger)", "gmail-token"),
             ("HubSpot (order notes &mdash; reference)", "hubspot-token"),
             ("WorkOS (sign-in)", "workos-api-key")]
    def rail_status(acct):
        if get_secret(acct):
            return '<span class="st ok">connected</span>'
        # every adapter is built; what remains is the key — say exactly that
        return ('<span class="st wait">ready &mdash; add key</span>')
    rows = "".join(
        f'<div class="trow slim"><span class="ico">{ICONS["lock"]}</span>'
        f'<span class="tdesc"><b>{esc(name)}</b> <span class="mut">keychain: relay_superagent / {acct}</span></span>'
        + rail_status(acct) + '</div>'
        for name, acct in rails)
    hint = ('<div class="empty">Keys live in the macOS keychain, never in files. '
            'Connect one: <code>security add-generic-password -U -s relay_superagent '
            '-a &lt;name&gt; -w \'&hellip;\'</code></div>')
    return f'<h2 class="sec" id="vault">Connectors</h2>{rows}{hint}'


# Per-agent console data (CM-21 demo world). Access grants and memory
# fields mirror the real architecture; playbook rules quote the seeded
# field-notes rules. Slug = URL identity.
AGENT_DEFS = {
    "detection-agent": dict(icon="ear", name="detection-agent",
        charter="Reads every inbound dispute webhook, classifies it by reason code.",
        reads=["bank/processor webhooks", "inbound chargeback email"],
        effects=[], scope="Writes signals; reads nothing personal beyond the dispute narrative.",
        session=["claim_text", "reason_code", "confidence"],
        profile=["merchant &rarr; prior dispute history"],
        rules=["A dispute is actionable only when its reason code has evidence coverage.",
               "A duplicate webhook redelivery never re-triggers."]),
    "eligibility-agent": dict(icon="funnel", name="eligibility-agent",
        charter="Decides what deserves attention. Every no is logged with a reason.",
        reads=["open orders (CRM)", "prior runs (ledger)", "merchant enrollment"],
        effects=[], scope="Reads the ledger; writes suppression reasons only.",
        session=["suppressed_reason"], profile=[],
        rules=["No matching order &rarr; suppress.", "Same claim recently handled &rarr; suppress.",
               "Merchant over daily cap &rarr; suppress. Caps are hard, not advisory."]),
    "response-agent": dict(icon="pen", name="response-agent",
        charter="Drafts the dispute response with cited evidence, in the merchant&rsquo;s learned voice.",
        reads=["evidence library", "learned voice notes", "order context"],
        effects=[], scope="Reads the evidence library + voice notes; never sends anything.",
        session=["counter_text", "cited_evidence"], profile=["merchant voice notes (from edits)"],
        rules=["Every factual claim cites an evidence-library entry.",
               "No superlatives (best / leading / number one).",
               "Uncertain &rarr; escalate to ops, never guess."]),
    "compliance-agent": dict(icon="shield", name="compliance-agent",
        charter="Checks links, claims and quality before a human ever sees a draft.",
        reads=["drafts", "evidence sources"], effects=["escalate to ops"],
        scope="Reads drafts; its only power is to block them.",
        session=["check_failures", "judge_scores"], profile=[],
        rules=["A dead source link blocks the draft.",
               "Contact info in a response blocks the draft.",
               "Judge below threshold &rarr; ops, not the merchant."]),
    "filing-agent": dict(icon="note", name="filing-agent",
        charter="Files the approved response exactly once, attaches the outcome.",
        reads=["approved runs"], effects=["file response (bank/processor)"],
        scope="The only agent that files with the bank &mdash; and only after your yes.",
        session=["note_ref", "acted_at"], profile=["dispute outcome (won/lost, amount)"],
        rules=["Exactly-once: a crash can never double-file.",
               "Acts only on approved or edited runs."]),
    "escalation-agent": dict(icon="moon", name="escalation-agent",
        charter="Escalates stuck work and floods. Never sends anything itself.",
        reads=["all run states", "queue depths"], effects=["escalate to ops"],
        scope="Reads everything, sends nothing. Deliberately has no AI in it.",
        session=["timeout / stall reasons"], profile=[],
        rules=["A card unanswered for 24h escalates &mdash; never auto-files.",
               "A crash-parked run is failed loudly, never silently."]),
    "reporting-agent": dict(icon="chart", name="reporting-agent",
        charter="Keeps the decision ledger and the Signals in the sidebar.",
        reads=["the ledger"], effects=[],
        scope="Read-only. Every number it shows is recomputable from your own audit log.",
        session=[], profile=[],
        rules=["Metrics are computed from rows, never estimated.",
               "The correction rate is the bill &mdash; merchant-auditable."]),
}


def agent_detail_content(tid: str, slug: str, tab: str = "overview",
                         item: int = 0) -> str:
    a = AGENT_DEFS.get(slug)
    if not a:
        return '<h1 class="page">Unknown agent</h1>'
    led = WORLD.d.ledger
    runs = [r for r in led.runs.values() if r.tenant_id == tid]
    corr = correction_rate(led, tid)

    TABS = [("overview", "Overview"), ("activity", "Activity"),
            ("access", "Access"), ("memory", "Memory"),
            ("playbook", "Playbook"), ("quality", "Quality")]
    tabbar = '<div class="tabbar">' + "".join(
        f'<a class="{"on" if key == tab else ""}" '
        f'href="/agents/{slug}?tab={key}">{label}</a>'
        for key, label in TABS) + "</div>"

    head = (f'<div class="dhead"><a class="back2" href="/agents">&lsaquo;</a>'
            f'<span class="tile">{ICONS[a["icon"]]}</span>'
            f'<div><h1>{a["name"]}</h1>'
            f'<div class="meta"><span class="st ok">active &middot; trial</span>'
            f'<span>&middot;</span><span>{len(runs)} runs</span>'
            f'<span>&middot;</span><span>needed edits {fmt_pct(corr)}</span></div></div>'
            f'<form method="post" action="/api/sample" style="margin-left:auto">'
            f'<button class="btn ghost">Simulate a dispute webhook</button></form></div>')

    if tab == "access":
        items = ([("Reads: " + r, "Read access. " + a["scope"], True) for r in a["reads"]]
                 + [("May do: " + e, "A side effect this agent may fire — always "
                     "through the exactly-once effect table, always after your yes.", True)
                    for e in a["effects"]]
                 + [("Everything else", "Deny by default. This agent cannot touch "
                     "anything not listed here.", False)])
        item = min(item, len(items) - 1)
        left = "".join(
            f'<a class="pitem {"on" if i == item else ""}" '
            f'href="/agents/{slug}?tab=access&item={i}"><span>{t}</span>'
            f'<span class="tgl {"on" if on else ""}"></span></a>'
            for i, (t, _, on) in enumerate(items))
        title, desc, _ = items[item]
        autonomy = _edit_history([
            ("Mothi Venkatesh", "Jul 30, 11:40 AM", "Granted",
             [("draft for approval", "ec-green")]),
            ("Mothi Venkatesh", "Jul 18, 9:15 AM", "Revoked",
             [("weekend triage", "ec-amber")]),
            ("Orchestrator", "Jul 12, 10:02 AM", "Hired on probation",
             [("shadow mode", "ec-purple"), ("exams required", "ec-blue")])])
        body = (f'<div class="twopane"><div class="pane-list">{left}</div>'
                f'<div class="pane-detail"><h3>{title}</h3><p>{desc}</p></div></div>'
                f'<h2 class="sec">Autonomy history</h2>'
                f'<div class="pagehint">Trust is earned in steps and every step '
                f'is written down &mdash; who granted what, when, and what was '
                f'taken back. Reversible by design.</div>' + autonomy)
    elif tab == "memory":
        panes = [("Per run", "Extracted on every run and stored on the run row — "
                  "the customer-auditable record.", a["session"]),
                 ("Persists", "Merges into durable memory (evidence packs, "
                  "voice notes, outcomes) with provenance — never overwritten, "
                  "only superseded.", a["profile"])]
        item = min(item, 1)
        left = "".join(
            f'<a class="pitem {"on" if i == item else ""}" '
            f'href="/agents/{slug}?tab=memory&item={i}"><span>{t}'
            f'<span class="sub2">{len(fields) or "no"} fields</span></span></a>'
            for i, (t, _, fields) in enumerate(panes))
        t, d, fields = panes[item]
        rows = "".join(f'<div class="trow slim"><span class="ico">{ICONS["note"]}</span>'
                       f'<span class="tdesc"><b>{f}</b></span></div>'
                       for f in fields) or '<div class="empty">Nothing tracked.</div>'
        body = (f'<div class="twopane"><div class="pane-list">{left}</div>'
                f'<div class="pane-detail"><h3>{t}</h3><p>{d}</p>{rows}</div></div>')
    elif tab == "playbook":
        rows = "".join(
            f'<div class="trow slim"><span class="ico">{ICONS["bm"]}</span>'
            f'<span class="tdesc">{r} <span class="mut">signed &middot; playbook v1'
            f'</span></span></div>' for r in a["rules"])
        from relay_superagent.llm.claude import SEAM_PROMPTS
        seams = {"detection-agent": ["confirm_mention", "extract_claim"],
                 "response-agent": ["draft_counter"],
                 "compliance-agent": ["judge", "semantic_diff"],
                 "reporting-agent": ["narrate"]}.get(slug, [])
        under = ""
        if seams:
            blocks = "".join(
                f'<div class="pane-detail" style="margin-bottom:10px"><h3>{sm}</h3>'
                f'<p style="white-space:pre-wrap;font-size:13px;color:var(--mut)">'
                f'{esc(SEAM_PROMPTS[sm])}</p></div>' for sm in seams)
            under = (f'<h2 class="sec">Under the hood &mdash; model instructions</h2>'
                     f'<div class="pagehint" style="margin:2px 0 14px">'
                     f'<span class="st mut" style="margin-right:8px">read-only</span>'
                     f'<span class="st mut" style="margin-right:8px">versioned in git</span>'
                     f'<span class="st mut">tested by evals</span><br>'
                     f'<span style="display:inline-block;margin-top:8px">Your playbook '
                     f'rules, voice notes and policy are injected into these fixed '
                     f'templates as data. Editing them is an engineering change, '
                     f'validated before it ships &mdash; not a textarea.</span></div>{blocks}')
        body = (f'<div class="pagehint">The rules this agent works under. Written '
                f'in plain language, signed, and versioned &mdash; there is no '
                f'editable prompt box, by design.</div>{rows}{under}')
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
            f'<span class="mut">{esc(e["detail"] or "&mdash;")}</span>',
            f'{_logo(_account_label(r))}{esc(_account_label(r))}',
            f'<a class="st wait" href="/runs/{r.run_id}">journey &rarr;</a>'],
        ) for e, r in mine[:25]]
        body = (f'<div class="pagehint">What this agent did, most recent '
                f'first &mdash; every row opens the full run journey it '
                f'belongs to.</div>'
                + _tbl(ACOLS, ["Action", "Detail", "Account", "Run"], rows2))
    elif tab == "quality":
        import json as _json
        from pathlib import Path as _Path
        AGENT_SEAMS = {"detection-agent": ["confirm_mention", "extract_claim"],
                       "response-agent": ["draft_counter"],
                       "compliance-agent": ["judge", "semantic_diff"],
                       "reporting-agent": ["narrate"]}
        evdir = _Path(__file__).resolve().parents[1] / "evals"
        results = {}
        if (evdir / "results.json").exists():
            results = _json.loads((evdir / "results.json").read_text())
        seam_rows = ""
        for seam in AGENT_SEAMS.get(slug, []):
            fpath = evdir / f"{seam}.json"
            n = len(_json.loads(fpath.read_text())["fixtures"]) if fpath.exists() else 0
            res = results.get(seam)
            if res:
                cls = "ok" if res["passed"] == res["total"] else "warn"
                status = f'<span class="st {cls}">{res["passed"]}/{res["total"]} passed</span>'
            else:
                status = '<span class="st wait">not run yet &mdash; needs API credits</span>'
            seam_rows += (
                f'<div class="trow slim"><span class="ico">{ICONS["shield"]}</span>'
                f'<span class="tdesc"><b>{seam}</b> <span class="mut">{n} scenarios '
                f'&middot; uv run python scripts/run_evals.py {seam}</span></span>'
                f'{status}</div>')
        if not seam_rows:
            seam_rows = (f'<div class="trow slim"><span class="ico">{ICONS["shield"]}</span>'
                         f'<span class="tdesc">Deterministic agent <span class="mut">'
                         f'no model inside &mdash; covered by the harness test suite '
                         f'on every commit</span></span><span class="st ok">tested</span></div>')
        body = (f'<div class="trow slim"><span class="ico">{ICONS["chart"]}</span>'
                f'<span class="tdesc"><b>Needed edits</b> <span class="mut">workspace-wide'
                f'</span></span><span class="st mut">{fmt_pct(corr)}</span></div>'
                + seam_rows)
    else:
        plays = AGENT_PLAYS.get(slug, [])
        play_html = "".join(
            f'<span class="st ok" style="margin-right:6px">{pl} &middot; live</span>'
            for pl in plays) or '<span class="st mut">supporting role</span>'
        body = (f'<div class="pane-detail" style="max-width:640px"><h3>Charter</h3>'
                f'<p>{a["charter"]}</p><p style="margin-top:10px" class="mut">'
                f'{a["scope"]}</p>'
                f'<h3 style="margin-top:16px">Works on</h3><p>{play_html}</p></div>')

    return head + tabbar + body


def _relay_agent_card(a: dict) -> str:
    live = a["status"] == "live"
    badge = ('<span class="st ok">live in this demo</span>' if live
             else '<span class="st wait">Coming next &middot; roadmap</span>')
    inner = (f'<div class="aname" style="margin-bottom:6px">'
             f'<span class="tile">{ICONS[a["icon"]]}</span>'
             f'<span class="dot2 {"on" if live else "off"}"></span>'
             f'<b style="font-size:15px">{a["name"]}</b></div>'
             f'<div class="mut" style="margin:0 0 6px;font-size:12px">{a["persona"]}</div>'
             f'<div style="margin:2px 0 8px">&ldquo;{a["desc"]}&rdquo;</div>'
             f'<div class="mut"><b>{a["tagline"]}</b></div>'
             f'<div style="margin-top:10px">{badge}</div>')
    if live:
        return f'<a class="arow2" style="display:block;padding:16px" href="/agents/detection-agent">{inner}</a>'
    return f'<div class="arow2" style="display:block;padding:16px;opacity:.75">{inner}</div>'


def agents_content(tid: str, f: str = "all", q: str = "") -> str:
    led = WORLD.d.ledger
    runs = [r for r in led.runs.values() if r.tenant_id == tid]
    n_sup = sum(1 for r in runs if r.state is RunState.SUPPRESSED)
    n_drafted = sum(1 for r in runs if r.decision)
    n_acted = sum(1 for r in runs if r.state in (RunState.ACTED, RunState.RESOLVED))
    n_esc = len(WORLD.d.slack.channel_posts)

    agents = RELAY_AGENTS
    if f in ("active", "planned"):
        want = "live" if f == "active" else "roadmap"
        agents = [a for a in agents if a["status"] == want]
    if q:
        agents = [a for a in agents if q.lower() in a["name"].lower()]
    cards = ("".join(_relay_agent_card(a) for a in agents)
             or '<div class="empty">No agents match.</div>')

    crew_rows = [
        ("ear", "detection-agent", f"{len(runs)} triggers"),
        ("funnel", "eligibility-agent", f"{n_sup} filtered"),
        ("pen", "response-agent", f"{n_drafted} drafts"),
        ("shield", "compliance-agent", "every draft"),
        ("note", "filing-agent", f"{n_acted} filed"),
        ("moon", "escalation-agent", f"{n_esc} escalations"),
        ("chart", "reporting-agent", f"{len(runs)} rows"),
    ]
    def crew_row(icon, name, stat):
        inner = (f'<span class="aname"><span class="tile">{ICONS[icon]}</span>'
                 f'<span class="dot2 on"></span><b>{name}</b></span>'
                 f'<span class="st ok">active</span>'
                 f'<span class="mut" style="font-size:12.5px">{stat}</span>'
                 f'<span class="go2">&rsaquo;</span>')
        return f'<a class="arow2" href="/agents/{name}">{inner}</a>'
    crew_table = "".join(crew_row(*r) for r in crew_rows)

    seg = lambda key, label: (f'<a class="{"on" if f == key else ""}" '
                              f'href="/agents?f={key}">{label}</a>')
    return (f'<h1 class="page">Agents</h1>'
            f'<div class="pagehint">Pre-built agents for payment and '
            f'compliance ops. Or build your own. Eight agents, one workspace '
            f'&mdash; Dispute Defender is wired end-to-end below; the other '
            f'seven are next. You approve before anything sends.</div>'
            f'<div class="atoolbar">'
            f'<span class="search-in">{ICONS["search"]}'
            f'<input placeholder="Search agents&hellip;" '
            f'oninput="[...document.querySelectorAll(\'.arow2\')].forEach(r=>'
            f'r.style.display=r.textContent.toLowerCase().includes(this.value.toLowerCase())?\'\':\'none\')"></span>'
            f'<span class="seg">{seg("all", "All")}{seg("active", "Live")}{seg("planned", "Roadmap")}</span></div>'
            f'<div class="atable" style="grid-template-columns:1fr">{cards}</div>'
            f'<h2 class="sec">Dispute Defender&rsquo;s crew</h2>'
            f'<div class="pagehint">The seven sub-agents inside the pipeline &mdash; '
            f'each with its own access, memory, playbook and quality tabs.</div>'
            f'<div class="atable"><div class="thead"><span>Agent name</span>'
            f'<span>Status</span><span>Activity</span><span></span></div>'
            f'{crew_table}</div>')


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
             + chip("all", "All", len(runs))
             + chip("escalated", "Escalated", len(esc_rows))
             + chip("resolved", "Won",
                    sum(1 for r in runs if r.state is RunState.RESOLVED))
             + "</div>")
    rows = "\n".join(ledger_row(r) for r in shown) or (
        '<div class="empty">Nothing here yet.</div>')
    won = influenced_won(tid)
    banner = (f'<div class="impactline">Counters you approved influenced '
              f'<b class="countup" data-n="{won/1000:.0f}" data-pre="$" data-suf="k">$0k</b> in closed-won deals.</div>' if won else "")
    return (f'<h1 class="page">Impact</h1>'
            f'<div class="pagehint">Every decision, recomputable from your own '
            f'record &mdash; <a href="/export/impact.csv"><b>export CSV</b></a>.'
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
        kv_base = {"source": run.trigger_source,
                   "merchant": _account_label(run),
                   "reason": COMP.get(run.reason_code, run.reason_code)}
    else:
        kv_base = {}
    payload = _json.dumps([
        {"agent": e["agent"], "kind": e["kind"], "t": int(e["ts"].timestamp()),
         "lane": _LANES[1 if e["kind"] == "note" else _EVENT_LANE.get((e["agent"], e["kind"]), 2)],
         "day": (e["ts"].date() - day0).days + 1,
         "time": e["ts"].strftime("%-I:%M %p").lower(),
         "bubble": e.get("bubble", ""),
         "kv": {**kv_base, **e.get("kv", {}), "detail": e["detail"] or "&mdash;"}}
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
                   if r.tenant_id == tid and r.merchant_id == acct_id),
                  key=lambda r: r.occurred_at)
    if not runs:
        return '<h1 class="page">Account not found</h1>'
    label = _account_label(runs[0])
    comps = sorted({COMP.get(r.reason_code, r.reason_code)
                    for r in runs if r.reason_code})
    merged = []
    for r in runs:
        for e in led.trace_for(r.run_id):
            bubble = ""
            if e["kind"] in ("surfaced", "approved", "edited") and r.decision:
                bubble = esc((r.decision or {}).get("counter_text", "")[:280])
            merged.append({**e, "bubble": bubble, "kv": {
                "claim": r.claim_text or "&mdash;",
                "reason": COMP.get(r.reason_code, r.reason_code),
                "merchant": REP.get(r.merchant_id, r.merchant_id),
                "run": f'<a href="/runs/{r.run_id}">open this run &rarr;</a>'}})
    merged.sort(key=lambda e: e["ts"])
    won = 0
    for r in runs:
        out = led.outcome_for(r.run_id)
        if out and (out.outcome_value or {}).get("won"):
            won += (out.outcome_value or {}).get("amount_paise") or 0
    waiting = sum(1 for r in runs if r.state is RunState.AWAITING_GATE)
    goal_line = (GOAL_META[acct_id][1] if acct_id in GOAL_META
                 else f'Keep {esc(label)}&rsquo;s dispute win rate healthy on '
                      f'{esc(" & ".join(comps) or "open reason codes")}.')
    stats = (f'{len(runs)} moments &middot; {waiting} waiting'
             + (f' &middot; &#8377;{won/100000:.0f}k won' if won else ""))
    rows = "\n".join(ledger_row(r) for r in reversed(runs))
    return (
        '<div class="tkscope">'
        f'<div class="dhead"><a class="back2" href="/projects">&lsaquo;</a>'
        f'<div><h1><span class="goalk">Goal:</span> {goal_line}</h1>'
        f'<div class="meta">{_logo(label)}{esc(label)}'
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
        f'placeholder="Add account context &mdash; what does the team know about {esc(label)} that the record doesn&rsquo;t?">'
        f'<button class="btn primary sm">Add note</button></form>'
        + f'<h2 class="sec">Every moment</h2>{rows}')


def journeys_content(tid: str, sel: str = "") -> str:
    # Scales past the demo world: an index of every account with a journey,
    # searchable client-side, sorted by last activity. One journey view per
    # account behind it. Lanes are agent-agnostic (signal/decision/task/
    # interaction), so new plays and agents land here without redesign; goal
    # titles derive from the runs and GOAL_META only overrides the wording.
    led = WORLD.d.ledger
    accts: dict[str, dict] = {}
    for r in led.runs.values():
        if r.tenant_id != tid or not r.merchant_id or not led.trace_for(r.run_id):
            continue
        a = accts.setdefault(r.merchant_id, {
            "n": 0, "waiting": 0, "won": 0, "lost": False,
            "last": r.occurred_at, "comps": set(), "name": _account_label(r)})
        a["n"] += 1
        a["last"] = max(a["last"], r.occurred_at)
        if r.reason_code:
            a["comps"].add(COMP.get(r.reason_code, r.reason_code))
        if r.state is RunState.AWAITING_GATE:
            a["waiting"] += 1
        out = led.outcome_for(r.run_id)
        if out:
            if (out.outcome_value or {}).get("won"):
                a["won"] += (out.outcome_value or {}).get("amount_paise") or 0
            else:
                a["lost"] = True
    if not accts:
        return ('<h1 class="page">Journeys</h1>'
                '<div class="pagehint">Every goal the team works becomes a '
                'replayable timeline: what was heard, what was decided, what '
                'you approved, what came back.</div>'
                '<div class="empty">No journeys yet &mdash; load sample data '
                'from Home, then come back.</div>')

    if sel in accts:
        return ('<a class="jback" href="/journeys">&lsaquo; All journeys</a>'
                + account_journey_content(tid, sel))

    def _goal(a, d):
        # GOAL_META strings are trusted copy (may carry entities); derived
        # titles are escaped at build below.
        if a in GOAL_META:
            return GOAL_META[a][1]
        comps = ", ".join(sorted(d["comps"])) or "open reason codes"
        return esc(f"Defend {d['name']}'s disputes: {comps}.")

    order = sorted(accts, key=lambda a: accts[a]["last"], reverse=True)
    SHOW = 60  # chunked render: everything is in the DOM for search/sort,
    rows = []  # but only the first chunk paints until asked.
    for i, a in enumerate(order):
        d = accts[a]
        if d["waiting"]:
            chip = f'<span class="st amber">{d["waiting"]} waiting</span>'
        elif d["won"]:
            chip = f'<span class="st ok">won &#8377;{d["won"]/100:,.0f}</span>'
        elif d["lost"]:
            chip = '<span class="st mut">lost &middot; recorded</span>'
        else:
            chip = '<span class="st mut">active</span>'
        comps = esc(", ".join(sorted(d["comps"]))) or '<span class="mut">&mdash;</span>'
        stream = " stream" if i < 8 else ""
        hid = " hidden" if i >= SHOW else ""
        rows.append(
            f'<tr class="jgoalrow" data-q="{esc(d["name"].lower())}" data-i="{i}"{hid} '
            f'onclick="location=\'/journeys?a={a}\'">'
            f'<td class="acct">{_logo(d["name"])}<b>{esc(d["name"])}</b></td>'
            f'<td><span class="ell{stream}">{_goal(a, d)}</span></td>'
            f'<td>{comps}</td>'
            f'<td class="num">{d["n"]}</td>'
            f'<td>{chip}</td>'
            f'<td class="num" data-s="{int(d["last"].timestamp())}">'
            f'{d["last"].strftime("%b %-d")}</td></tr>')
    more_btn = (f'<button class="btn ghost sm gmore" id="jmore" '
                f'onclick="jshowall(this)">Show all {len(order)} merchants</button>'
                if len(order) > SHOW else "")
    shown = min(SHOW, len(order))
    return (
        '<h1 class="page">Journeys</h1>'
        '<div class="pagehint">Every merchant on this workspace becomes a '
        'replayable timeline. Search, sort, open, scrub.</div>'
        '<input class="jfind" placeholder="Search merchants, goals or dispute reasons&hellip;" '
        'oninput="jfilter(this.value)">'
        f'<div class="gridcount" id="jcount">Showing {shown} of {len(order)} merchants</div>'
        '<div class="dgridwrap"><table class="dgrid" id="jgrid">'
        '<thead><tr>'
        '<th onclick="dsort(this)">Merchant</th>'
        '<th onclick="dsort(this)">Goal</th>'
        '<th onclick="dsort(this)">Dispute reasons</th>'
        '<th onclick="dsort(this)" class="num">Moments</th>'
        '<th onclick="dsort(this)">Status</th>'
        '<th onclick="dsort(this)" class="num">Last activity</th>'
        '</tr></thead>'
        f'<tbody>{"".join(rows)}</tbody></table></div>'
        + more_btn)


def run_trace_content(tid: str, run_id: str) -> str:
    led = WORLD.d.ledger
    r = led.runs.get(run_id)
    if r is None or r.tenant_id != tid:
        return '<h1 class="page">Run not found</h1>'
    label, cls = STATE_META.get(r.state, (r.state.value, "mut"))
    events = [dict(e) for e in led.trace_for(run_id)]
    for e in events:
        if e["kind"] in ("surfaced", "approved", "edited") and r.decision:
            e["bubble"] = esc((r.decision or {}).get("counter_text", "")[:280])
    rows = "".join(
        f'<div class="trow slim"><span class="ico">'
        f'{ICONS[_AGENT_ICONS.get(e["agent"], "bolt")]}</span>'
        f'<span class="tdesc"><b>{esc(e["agent"])}</b> {esc(e["kind"])} '
        f'<span class="mut">{esc(e["detail"])}</span></span>'
        f'<span class="when">{e["ts"].strftime("%b %-d, %H:%M")}</span></div>'
        for e in events) or (
        '<div class="empty">No trace events — this run predates tracing.</div>')
    comp_name = esc(COMP.get(r.reason_code, r.reason_code))
    return (
        '<div class="tkscope">'
        f'<div class="dhead"><a class="back2" href="/journeys" onclick="if(history.length>1){{history.back();return false}}">&lsaquo;</a>'
        f'<div><h1><span class="goalk">Goal:</span> Defend the {comp_name} '
        f'dispute &mdash; &ldquo;{esc(r.claim_text or "claim")}&rdquo; at {esc(_account_label(r))}.</h1>'
        f'<div class="meta">{_logo(_account_label(r))}{esc(_account_label(r))}'
        f'<span>&middot;</span><span class="st {cls}">{label}</span></div></div>'
        f'<span class="jnav"><button class="btn ghost sm jcirc" onclick="jstep(-1)">&larr;</button>'
        f'<button class="btn primary sm jcirc" onclick="jplay(this)">&#9654;</button>'
        f'<button class="btn ghost sm jcirc" onclick="jstep(1)">&rarr;</button></span></div>'
        + _journey_svg(events, r)
        + '</div>'
        + f'<form class="notebar" method="post" action="/api/note">'
        f'<input type="hidden" name="run_id" value="{run_id}">'
        f'<input class="jfind notein" name="text" maxlength="280" '
        f'placeholder="Add a note to this journey &mdash; what did you know that the record doesn&rsquo;t?">'
        f'<button class="btn primary sm">Add note</button></form>'
        + f'<h2 class="sec">Full log</h2>{rows}')


# (days ago, merchant, reason label, buyer claim, verdict, evidence, draft)
SHADOW_ROWS = [
    (27, "Loomcraft Textiles",   "Goods not received", "Kurta set never arrived",             "send", "2 proofs",
     "Courier proof-of-delivery is signed and GPS-stamped at the doorstep; the WhatsApp thread confirms receipt the same evening."),
    (25, "Kavali Kitchens",      "Duplicate charge",   "Two debits for one thali order",      "send", "2 proofs",
     "One gateway transaction id, one settled debit; the second line is an authorization hold that reverses on its own."),
    (24, "Verve Wellness",       "",                   "Refund status query, not a dispute",  "skip", "",
     ""),
    (21, "Bumblebee Mobility",   "Goods not received", "Scooter charger missing",             "send", "2 proofs",
     "POD shows delivery two days before the dispute was filed; the buyer's own delivery-day message is attached."),
    (19, "Sundar Studio Prints", "Not as described",   "Print colours don't match listing",   "send", "1 proof",
     "The listing snapshot from the order date matches the shipped SKU's batch code exactly."),
    (17, "Northgate Fresh Mart", "Duplicate charge",   "Grocery order billed twice",          "send", "2 proofs",
     "Invoice and bank settlement excerpt agree on a single debit for this order id."),
    (16, "Verve Wellness",       "",                   "Buyer praised the delivery speed",    "skip", "",
     ""),
    (14, "Kavali Kitchens",      "Fraud claim",        "Card owner says order wasn't theirs", "esc",  "none",
     ""),
    (12, "Loomcraft Textiles",   "Duplicate charge",   "Statement lists the saree twice",     "send", "2 proofs",
     "The gateway and the invoice tie this order to one transaction; the duplicate line never captured."),
    (9,  "Bumblebee Mobility",   "Goods not received", "Helmet order marked undelivered",     "send", "1 proof",
     "The courier scan places the parcel at the registered address, signed for on the handheld device."),
    (7,  "Northgate Fresh Mart", "",                   "Delivery-slot reschedule request",    "skip", "",
     ""),
    (5,  "Sundar Studio Prints", "Goods not received", "Wedding-card box never showed",       "send", "2 proofs",
     "POD plus the buyer's WhatsApp confirmation from delivery day; both attach to the filing."),
    (2,  "Verve Wellness",       "Not as described",   "Serum shade differs from photos",     "send", "2 proofs",
     "Listing snapshot and the buyer's own return photos show the same packaging and batch code."),
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
            proof_v = '<span class="st warn">none &mdash; gap</span>'
            send_v = '<span class="st warn">escalate &mdash; no proof on file</span>'
        else:
            proof_v = '<span class="mut">&mdash;</span>'
            send_v = '<span class="mut">&mdash;</span>'
        body.append(
            f'<tr data-v="{verdict}">'
            f'<td class="chk"><input type="checkbox"></td>'
            f'<td class="acct">{_logo(acct)}<b>{esc(acct)}</b> '
            f'<span class="mut">{when}</span></td>'
            + cell("listening&hellip;", comp_v)
            + cell("extracting claim&hellip;", claim_v)
            + cell("checking the evidence vault&hellip;", proof_v)
            + cell("drafting the response&hellip;", send_v, "wide")
            + '<td class="ghost"></td></tr>')

    return (
        '<h1 class="page">Shadow trial</h1>'
        '<div class="pagehint">Run the last 30 days of disputes through '
        'Dispute Defender. Nothing files, nobody is contacted, and the scan '
        'is free &mdash; you just see what you missed.</div>'
        '<button class="btn primary" id="shbtn" onclick="shstart()">Scan the last 30 days</button>'
        '<div class="shstats">'
        '<div class="shstat"><div class="n" id="sh-calls">0</div><div class="l">webhooks scanned</div></div>'
        '<div class="shstat"><div class="n" id="sh-moments">0</div><div class="l">actionable disputes</div></div>'
        '<div class="shstat"><div class="n" id="sh-sends">0</div><div class="l">responses it would have filed</div></div>'
        '<div class="shstat"><div class="n" id="sh-missed">0</div><div class="l">answered by anyone today</div></div>'
        '</div>'
        '<div class="dgridwrap"><table class="dgrid">'
        '<thead><tr><th class="chk"></th>'
        '<th onclick="dsort(this)">Dispute</th>'
        '<th onclick="dsort(this)">Reason code covered?</th>'
        '<th onclick="dsort(this)">The claim</th>'
        '<th onclick="dsort(this)">Evidence on file?</th>'
        '<th class="wide" onclick="dsort(this)">Would it have filed?</th>'
        '<th class="ghost">+ New question</th></tr></thead>'
        f'<tbody>{"".join(body)}</tbody></table></div>'
        '<div class="shband" id="shband"><span><b>Shadow mode:</b> nothing was '
        'filed, nobody was contacted, and this scan cost nothing. This is what '
        'last month looked like without Dispute Defender.</span>'
        '<a class="btn primary sm" href="/settings?s=connectors">Go live &rarr;</a></div>'
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


def settings_content(tid: str, s: str = "team") -> str:
    # Osvi-style sectioned settings: Team / Connectors / Workspace behind a
    # tab bar (same component as the agent console) instead of one long scroll.
    SECTIONS = [("team", "Team"), ("connectors", "Connectors"),
                ("workspace", "Workspace")]
    if s not in {k for k, _ in SECTIONS}:
        s = "team"
    tabs = '<div class="tabbar">' + "".join(
        f'<a class="{"on" if k == s else ""}" href="/settings?s={k}">{t}</a>'
        for k, t in SECTIONS) + "</div>"

    if s == "team":
        by_dept: dict[str, list] = {}
        for tid_, name, role, dept, enrolled in TEAM:
            by_dept.setdefault(dept, []).append((name, role, enrolled))
        body = ('<div class="pagehint">Who the agents work with. '
                '&ldquo;Reviews responses&rdquo; means their approvals feed '
                'the Track Record.</div>')
        for dept, members in by_dept.items():
            rows = "".join(
                f'<div class="trow slim"><span class="ico">{ICONS["bot" if enrolled else "bm"]}</span>'
                f'<span class="tdesc"><b>{esc(name)}</b> <span class="mut">{esc(role)}</span></span>'
                + ('<span class="st ok">reviews responses</span>' if enrolled else '')
                + '</div>'
                for name, role, enrolled in members)
            body += (f'<h2 class="sec">{dept} ({len(members)})</h2>{rows}')
    elif s == "connectors":
        body = ('<div class="pagehint">Every rail is built; a connector goes '
                'live the moment its key lands in the keychain.</div>'
                + vault_content(tid))
    else:
        body = ('<div class="pagehint">How this workspace runs. These are '
                'policy, not preferences &mdash; each maps to an enforced '
                'guarantee.</div>'
                + "".join(
            f'<div class="trow slim"><span class="ico">{ICONS["shield"]}</span>'
            f'<span class="tdesc"><b>{esc(k)}</b> <span class="mut">{v}</span></span></div>'
            for k, v in [
                ("Human gate", "nothing files without an explicit yes; "
                 "timeouts escalate, never auto-file"),
                ("Queue cap", "at most 3 cards per merchant &mdash; attention "
                 "is protected, floods escalate"),
                ("Repeat suppression", "the same claim on the same order is "
                 "not re-worked within 7 days"),
                ("No holdouts", "every covered dispute is worked &mdash; a "
                 "missed chargeback deadline is real money, never an A/B arm"),
                ("Your Track Record", "export is a right &mdash; one click, "
                 "everything, recomputable (arriving with the first live "
                 "workspace)"),
                ("Secrets", "macOS keychain only; keys never touch files or "
                 "this database")]))
    return '<h1 class="page">Settings</h1>' + tabs + body


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
            self._html(chat_render(sess["tenant_id"], conv,
                                   sess.get("email", ""), _as))
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
            w.writerow(["run_id", "occurred_at", "source", "merchant",
                        "dispute_reason", "claim", "state", "gate_action",
                        "gate_actor", "edit_material", "won", "amount_paise"])
            for r in sorted((x for x in led.runs.values()
                             if x.tenant_id == sess["tenant_id"]),
                            key=lambda x: x.occurred_at):
                out = led.outcome_for(r.run_id)
                v = (out.outcome_value or {}) if out else {}
                w.writerow([
                    r.run_id, r.occurred_at.isoformat(), r.trigger_source,
                    _account_label(r), COMP.get(r.reason_code, r.reason_code),
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
        elif self.path.split("?")[0] == "/agents":
            sess = self._session()
            if not sess:
                return self._redirect("/login")
            from urllib.parse import parse_qs as _pq, urlparse as _up
            f = _pq(_up(self.path).query).get("f", ["all"])[0]
            self._html(_shell(agents_content(sess["tenant_id"], f), "agents",
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
        raw = self.rfile.read(length).decode()
        if self.path == "/slack/interactions":
            return self._slack_interaction(raw)
        if self.path.startswith("/webhooks/fathom"):
            return self._fathom_webhook(raw)
        if self.path in ("/auth/signup", "/auth/login", "/auth/demo",
                         "/auth/verify"):
            return self._auth(raw)
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
                self.send_response(400); self.end_headers(); return
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
            res = ask(msg)
            res = _polish_reply(msg, res)
            c = CONVS.get(payload.get("conv_id") or "")
            if c is None or c["tenant"] != sess["tenant_id"]:
                c = _new_conv(sess["tenant_id"], msg)
            c["msgs"].append({"who": "msg user", "html": esc(msg)})
            c["msgs"].append({"who": "msg bot", "html": res.get("reply", "")})
            if res.get("cards"):
                c["msgs"].append({"who": "cards", "html": res["cards"]})
                if 'id="prop-' in res["cards"]:
                    c["pending"] = res["cards"].split('id="prop-')[1].split('"')[0]
            _touch(c)
            return self._json({**res, "conv_id": c["id"], "title": c["title"]})
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
            action = (PROPOSALS.get(pid) or {}).get("action")
            res = confirm(pid)
            c = CONVS.get(payload.get("conv_id") or "")
            if c is not None and c["tenant"] == sess["tenant_id"]:
                c["msgs"].append({"who": "msg bot", "html": res.get("reply", "")})
                if res.get("cards"):
                    c["msgs"].append({"who": "cards", "html": res["cards"]})
                if c.get("pending") == pid:
                    c["pending"] = None
                    c["outcome"] = ("approved" if action == "approve"
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
                        "login", "Verified, but this account has no organization — sign up first."), 403)
            else:
                resp = wos.authenticate(email, password)
                ctx = self._tenant_from_auth(resp, email)
                if ctx is None:
                    return self._html(auth_page(
                        "login", "This account has no organization yet — sign up first."), 403)
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
        chat call — one decision path, three transports."""
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
            return self._json({"text": "Approved — the response is filed. ✓"})
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
            f'<span class="comp">{esc(COMP.get(r.reason_code, "–"))}</span>'
            f'<span class="meta">{esc(REP.get(r.merchant_id, r.merchant_id or "–"))}'
            f' · {_logo(_account_label(r))}{esc(_account_label(r) or "–")}</span>'
            f'<span class="st {cls}">{label}</span></div>'
            f'<div class="mclaim">“{esc(r.claim_text)}”</div>'
            f'<div class="mcounter">{counter}</div></div>')


def _t_shadow(_):
    sends = sum(1 for row in SHADOW_ROWS if row[4] == "send")
    moments = sum(1 for row in SHADOW_ROWS if row[4] != "skip")
    return (f"Shadow scan over the last 30 days: <b>{moments}</b> actionable "
            f"disputes, <b>{sends}</b> responses ready to file, none answered "
            f"by anyone. Nothing was filed &mdash; shadow mode never contacts "
            f"a bank. <a href='/shadow'><b>Open the scan</b></a> to replay it.",
            "")


def _t_queue(_):
    runs = [r for r in WORLD.d.ledger.runs.values() if r.state is RunState.AWAITING_GATE]
    if not runs:
        return "The queue is clear — nothing is waiting on you.", ""
    return (f"{len(runs)} dispute response{'s' if len(runs) != 1 else ''} awaiting review. "
            f"Say “approve the &lt;dispute reason&gt; one” to act on one.",
            "".join(_mini_run(r) for r in runs))


def _t_metrics(_):
    led = WORLD.d.ledger
    rows = [
        ("Correction rate", fmt_pct(correction_rate(led, "t1")), "target <5%"),
        ("Response usage", fmt_pct(counter_usage_rate(led, "t1")), "approved or lightly edited"),
        ("Trigger precision", fmt_pct(trigger_precision(led, "t1")), "disputes worth surfacing"),
        ("Gate p95", _fmt_latency(gate_latency_p95_ms(led, "t1")), "time to a decision"),
    ]
    html = "".join(f'<div class="mrow"><b>{v}</b><span>{k}</span><i>{h}</i></div>'
                   for k, v, h in rows)
    return "Here is how the loop is running.", f'<div class="mcard">{html}</div>'


def _t_runs(args):
    comp = (args or {}).get("competitor")
    runs = sorted(WORLD.d.ledger.runs.values(), key=lambda r: r.occurred_at, reverse=True)
    if comp:
        runs = [r for r in runs if r.reason_code == comp]
    if not runs:
        return f"No runs found{' for ' + COMP.get(comp, comp) if comp else ''}.", ""
    label = f" on {COMP.get(comp, comp)} disputes" if comp else ""
    return f"{len(runs)} run{'s' if len(runs) != 1 else ''}{label}, newest first.", \
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
        return "No evidence has been cited yet.", ""
    ev_by_id = {e.evidence_id: e for e in WORLD.d.evidence}
    rows = ""
    for eid, st in sorted(stats.items(), key=lambda kv: -kv[1]["cited"]):
        e = ev_by_id.get(eid)
        rate = (f'{st["won"]}/{st["resolved"]} won' if st["resolved"]
                else "no outcomes yet")
        rows += (f'<div class="mrow"><b>{st["cited"]}×</b>'
                 f'<span>{esc(e.text[:70]) if e else eid}</span><i>{rate}</i></div>')
    return ("Evidence by citations. Win rates stay hidden until n ≥ 5 — "
            "these are shown raw because this is seed data.",
            f'<div class="mcard">{rows}</div>')


def _t_escalations(_):
    posts = WORLD.d.slack.channel_posts
    if not posts:
        return "Nothing needs ops right now.", ""
    rows = "".join(f'<div class="mrow"><b class="warn">!</b>'
                   f'<span>{esc(b["reason"])}</span><i>“{esc(b.get("claim") or "—")[:60]}”</i></div>'
                   for _, b in posts)
    return f"{len(posts)} escalation{'s' if len(posts) != 1 else ''} in the review channel.", \
           f'<div class="mcard">{rows}</div>'


def _find_awaiting(comp):
    runs = [r for r in WORLD.d.ledger.runs.values() if r.state is RunState.AWAITING_GATE]
    if comp:
        runs = [r for r in runs if r.reason_code == comp]
    return sorted(runs, key=lambda r: r.occurred_at)[0] if runs else None


def _t_action(args, action):
    run = _find_awaiting((args or {}).get("competitor"))
    if run is None:
        return "There is no matching counter awaiting review.", "", None
    pid = str(_uuid.uuid4())
    PROPOSALS[pid] = {"run_id": run.run_id, "action": action}
    verb = "Approve & file with the bank" if action == "approve" else "Dismiss"
    card = (f'{_mini_run(run)}'
            f'<div class="proposal" id="prop-{pid}">'
            f'<button class="btn primary" onclick="confirmProposal(\'{pid}\')">{verb}</button>'
            f'<button class="btn ghost" onclick="cancelProposal(\'{pid}\')">Cancel</button></div>')
    return (f"Here is the {COMP.get(run.reason_code)} response. Confirm to "
            f"{'approve it' if action == 'approve' else 'dismiss it'} — nothing "
            f"happens until you do.", card, pid)


HELP = ("I answer from the ledger, never from memory. Try: "
        "<b>what's awaiting review?</b> · <b>how are the metrics?</b> · "
        "<b>show goods-not-received runs</b> · <b>which evidence is winning?</b> · "
        "<b>anything escalated?</b> · <b>approve the duplicate-charge one</b>")


def _keyword_route(text: str):
    t = text.lower().replace("-", " ")
    if "shadow" in t or "would have filed" in t or "would have sent" in t or "would have gone" in t:
        return "shadow", {"competitor": None}
    comp = next((dr.code for dr in WORLD.d.policy.dispute_reasons
                 if dr.label.lower() in t
                 or dr.id.replace("_", " ") in t
                 or COMP.get(dr.code, "").lower() in t), None)
    args = {"competitor": comp}
    if any(w in t for w in ("approve", "send it", "ship it", "file it")):
        return "approve", args
    if any(w in t for w in ("dismiss", "reject")):
        return "dismiss", args
    if any(w in t for w in ("queue", "awaiting", "review", "pending", "waiting")):
        return "queue", args
    if any(w in t for w in ("metric", "correction", "usage", "precision",
                            "latency", "how are we", "how is", "kpi")):
        return "metrics", args
    if any(w in t for w in ("evidence", "winning", "win rate", "argument", "proof")):
        return "evidence", args
    if any(w in t for w in ("escalat", "timeout", "stuck", "for ops")):
        return "escalations", args
    if comp or any(w in t for w in ("run", "ledger", "history", "dispute", "order")):
        return "runs", args
    return "help", args


def _llm_route(text: str):
    """One Haiku call picks the tool — same seam discipline as the pipeline:
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
    routed = _llm_route(message)
    tool, args = routed or _keyword_route(message)
    meta = {"_routed_model": routed is not None, "_tool": tool}
    if tool in ("approve", "dismiss"):
        reply, cards, pid = _t_action(args, tool)
        return {"reply": reply, "cards": cards, "proposal": pid, **meta}
    handler = {"queue": _t_queue, "metrics": _t_metrics, "runs": _t_runs,
               "evidence": _t_evidence, "escalations": _t_escalations,
               "shadow": _t_shadow}.get(tool)
    if handler is None:
        return {"reply": HELP, "cards": "", "proposal": None, **meta}
    reply, cards = handler(args)
    return {"reply": reply, "cards": cards, "proposal": None, **meta}


def confirm(pid: str) -> dict:
    prop = PROPOSALS.pop(pid, None)
    if prop is None:
        return {"reply": "That proposal is gone — it may already be resolved.", "cards": ""}
    run = WORLD.d.ledger.runs.get(prop["run_id"])
    if run is None or run.state is not RunState.AWAITING_GATE:
        return {"reply": "That run is no longer awaiting review.", "cards": ""}
    if prop["action"] == "approve":
        WORLD.approve(run, "ask")
        return {"reply": "Approved — the response is filed with the bank and "
                         "the ledger has the row.", "cards": _mini_run(run)}
    WORLD.reject(run, "ask")
    return {"reply": "Dismissed and recorded.", "cards": _mini_run(run)}


# ------------------------------------------------------------- sample data
# "Load sample data": seeds an empty tenant with dispute reasons, evidence and
# a few simulated runs through the REAL pipeline, so the whole loop is
# experienceable before any keys exist. Each further call simulates one more
# dispute webhook. Everything is labeled sample via trigger_source.
_SAMPLE_SCENARIOS = [
    # (merchant, reason_code, claim, dispute narrative, drafted response)
    ("Anokhi Threads", "RG", "Buyer says the lehenga order never arrived",
     "Chargeback filed: buyer says the lehenga order never arrived.",
     "The courier proof-of-delivery is signed and GPS-stamped at the "
     "registered address two days before this dispute was filed; the "
     "WhatsApp thread includes the buyer's own delivery-day confirmation. "
     "Both documents are attached to this filing."),
    ("Dosa Junction", "RD", "Buyer says one order was charged twice",
     "Chargeback filed: the buyer's statement shows two debits for one order.",
     "The invoice and the payment gateway agree on a single transaction id "
     "for this order, and the bank settlement excerpt confirms one debit "
     "cleared. The second line on the statement is an authorization hold "
     "that reverses automatically."),
    ("Meraki Skincare", "RG", "Buyer says the serum refill never showed up",
     "Chargeback filed: buyer claims the monthly refill was never delivered.",
     "Delivery proof for the refill shipment is signed and dated, and the "
     "communication log shows the buyer acknowledging receipt that evening. "
     "Both attach to this response."),
    ("Trailhead Gear", "RD", "Buyer's bank flagged a repeat charge",
     "Chargeback filed: the issuing bank flagged a duplicate charge on the "
     "trekking-pole order.",
     "One gateway transaction id maps to one settled debit for this order; "
     "the invoice and the bank excerpt agree, and no second capture exists "
     "in the settlement record."),
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
    acct, reason_code, claim, text, counter = _SAMPLE_SCENARIOS[n % len(_SAMPLE_SCENARIOS)]
    _sample_counters[tid] = n + 1
    order = f"order_{tid[:6]}_{n}"
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
        occurred_at=d.clock.now(), order_id=order, merchant_id=email,
        dispute_id=f"dp_sample_{tid[:6]}_{n}", reason_code=reason_code,
        text=text))
    return run.run_id if run else ""


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
        return (f'<a class="{cls}" href="/?c={c["id"]}"><span class="dot"></span>'
                f'<span class="ctitle">{esc(c["title"])}</span>{badge}'
                f'<span class="kebab" onclick="convMenu(event, \'{c["id"]}\', {str(bool(c["pinned"])).lower()})" '
                f'title="Options">&#8942;</span></a>')

    out = ""
    if pinned:
        out += '<div class="navsec">Pinned</div>' + "".join(row(c) for c in pinned)
    if recents:
        out += '<div class="navsec csec">Recents</div>' + "".join(row(c) for c in recents)
    if not out:
        out = '<div class="navsec">Recents</div><div class="cempty">Conversations appear here.</div>'
    return out


def seed_conversations() -> None:
    """The demo account's chat history: live ledger answers plus exchanges
    grounded in Dispute Defender's operating rules (deadlines, evidence
    coverage, edit-learning) — a believable merchant week."""
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

    live("My review queue", "What's awaiting review?", pin=True)
    authored(
        "Why do deadlines matter so much?", "What happens if we miss a dispute deadline?",
        "A missed deadline is an automatic loss &mdash; the bank rules for the "
        "buyer and the money is gone, no matter how strong your evidence was. "
        "That is why <b>disputes are never held out</b>, why an unanswered "
        "card escalates to ops at 24h instead of expiring silently, and why "
        "the filing step is exactly-once with the deadline stamped on the "
        "run.", pin=True)
    live("Evidence that wins disputes", "Which evidence is winning?", pin=True)

    approved = sum(1 for r in WORLD.d.ledger.runs.values()
                   if r.gate_action is not None
                   and r.gate_action.value in ("approve", "edit"))
    authored(
        "Automate dispute filing?", "Can we automate the dispute responses yet?",
        f"Not yet. The rule is <b>20&ndash;30 approved manual responses before "
        f"anything automates</b> &mdash; it exists so the drafter earns trust "
        f"on your own dispute history, not a demo. You are at "
        f"<b>{approved} of 20</b> approved runs; the Conductor unlocks "
        f"auto-drafting when the count clears and your edit rate stays flat. "
        f"Filing itself always keeps the approval gate.")
    authored(
        "Goods-not-received pattern", "What wins goods-not-received disputes?",
        "The pair that survives every round: the <b>courier proof-of-delivery "
        "(signed + GPS-stamped)</b> and the <b>buyer's own WhatsApp "
        "confirmation</b> from delivery day. Merchants filed that pack "
        "unedited in all but two cases, both style edits. A POD alone wins "
        "less often than POD plus the comms log.")
    authored(
        "Who edits the drafts?", "Which merchants edit the drafts most?",
        "<b>Kavali Kitchens</b> edits most &mdash; about 1 in 3 drafts, mostly "
        "style. <b>Loomcraft Textiles</b> files as-is. One <b>material</b> "
        "edit this quarter: a bank reference number the draft lacked, and "
        "that correction was written back to the evidence vault so no later "
        "draft repeated it. Material edits are the learning signal; style "
        "edits are free.")
    live("Goods-not-received runs", "Show goods-not-received runs")
    live("Escalations this week", "escalations")


_LIVE_LLM = None


def _polish_reply(question: str, res: dict) -> dict:
    """Routing transparency + optional narration. The deterministic reply is
    the fact source; when the live model is reachable, seam 6 paraphrases it
    (facts only), and the meta line says exactly which path ran."""
    from relay_superagent.secrets import get_secret
    routed_by = "Haiku" if res.pop("_routed_model", False) else "keywords"
    voiced_by = "deterministic reply"
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
                voiced_by = "narrated by Haiku"
        except Exception:
            pass
    res.pop("_tool", None)
    res["reply"] = (res.get("reply", "")
                    + f'<span class="rmeta">routed by {routed_by} &middot; '
                      f'{voiced_by} &middot; facts from the ledger</span>')
    return res


CHAT_TEMPLATE = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Relay — Command</title>
<style>
:root{--ink:#1B1F30;--text:#3A3D4D;--mut:#8A8D9C;--hair:#E8E9EF;--accent:#5266EB;
--accent-soft:#E9EBF8;--pill:#EEEFF2;--side:#FAFAFC}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Circular Std',-apple-system,BlinkMacSystemFont,'SF Pro Text',sans-serif;
color:var(--text);background:#FDFDFE;-webkit-font-smoothing:antialiased;font-size:14px;
height:100vh;overflow:hidden}
a{text-decoration:none;color:inherit}
.sidebar{position:fixed;top:0;bottom:0;left:0;width:250px;background:var(--side);
border-right:1px solid #ECECF1;padding:14px 12px;overflow-y:auto}
.brand{display:flex;align-items:center;gap:10px;padding:8px 10px;margin-bottom:12px}
.logo{width:26px;height:26px;border-radius:8px;background:#21232E;color:#fff;font-weight:700;
font-size:13px;display:grid;place-items:center}
.brand b{font-size:14px;color:var(--ink);font-weight:600}
.pro{margin-left:auto;background:#21232E;color:#fff;font-size:10.5px;font-weight:600;
border-radius:6px;padding:2px 7px}
.nav{display:flex;align-items:center;gap:11px;padding:8px 10px;border-radius:8px;
color:var(--text);font-size:13.5px;margin-bottom:1px}
.nav svg{width:16px;height:16px;color:#6A6D7D;flex:none}
.nav:hover{background:#F0F0F5}
.nav.active{background:var(--accent-soft);color:var(--ink);font-weight:500}
.nav.active svg{color:var(--ink)}
.nav .count{margin-left:auto;color:var(--mut);font-size:12.5px}
.nav .new{margin-left:auto;background:#E3E6F0;color:#4A4E63;font-size:10.5px;font-weight:600;
border-radius:6px;padding:2px 7px}
hr.side{border:none;border-top:1px solid #ECECF1;margin:10px 0}
.navsec{margin:6px 10px 8px;font-size:12px;font-weight:600;color:var(--mut)}
.rolepills{display:flex;gap:6px;align-items:center;justify-content:center;
margin:0 0 14px;font-size:12.5px;color:var(--mut)}
.rolepills span{margin-right:2px}
.rolepills a{padding:4px 12px;border-radius:999px;border:1px solid var(--hair);
color:var(--text);text-decoration:none;font-weight:500}
.rolepills a:hover{background:#F0F0F5}
.rolepills a.on{background:var(--accent);border-color:var(--accent);color:#fff}
.navsec.csec{margin-top:16px}
.conv{display:flex;align-items:center;gap:10px;padding:7px 10px;border-radius:8px;
font-size:13.5px;color:var(--text);margin-bottom:1px;position:relative}
.conv .dot{width:7px;height:7px;border-radius:50%;border:1.5px solid #C2C5D2;flex:none}
.conv .ctitle{flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.conv .kebab{visibility:hidden;color:var(--mut);padding:0 2px;font-size:15px}
.conv:hover{background:#F0F0F5}
.conv:hover .kebab{visibility:visible}
.conv.active{background:#ECECF1;color:var(--ink)}
.cempty{padding:4px 10px;font-size:12.5px;color:var(--mut)}
.bm{padding:5px 10px}
.bm span{font-size:13px;color:var(--text);display:flex;gap:11px;align-items:center}
.bm span svg{width:15px;height:15px;color:#6A6D7D}
.bm i{font-style:normal;font-size:12.5px;color:var(--mut);padding-left:26px;display:block}
.main{margin-left:250px;height:100vh;display:flex;flex-direction:column;
background:linear-gradient(#FDFDFE,#F1F2F8)}
.convhead{display:flex;align-items:center;padding:18px 34px;color:var(--ink);
font-size:14.5px;font-weight:500;flex:none}
.convhead .newchat{margin-left:auto;font-size:13px;font-weight:500;color:var(--text);
background:var(--pill);border-radius:9px;padding:7px 13px}
.convhead .newchat:hover{background:#E6E7EC}
.uwrap{display:flex;align-items:center;gap:14px;margin-left:18px;font-size:13.5px;
color:var(--mut)}
.uwrap .logout{color:var(--mut)}
.uwrap .logout:hover{color:var(--ink)}
.uwrap .avatar{width:26px;height:26px;border-radius:50%;background:var(--accent);
color:#fff;display:inline-flex;align-items:center;justify-content:center;
font-size:12px;font-weight:650;text-transform:uppercase}
main{flex:1;overflow-y:auto;padding:8px 0 14px}
.thread{max-width:740px;margin:0 auto;padding:0 24px;display:flex;flex-direction:column;gap:16px}
.hero{margin-top:9vh}
.hero h1{font-size:33px;font-weight:450;color:var(--ink);letter-spacing:-.01em;
  margin-bottom:10px;text-align:center}
.brief{text-align:center;color:var(--mut);font-size:13.5px;margin-bottom:24px}
.brief b{color:var(--ink);font-weight:600}
.samplecta{display:flex;flex-direction:column;align-items:center;gap:10px;
  margin:26px 0 4px;text-align:center}
.samplecta .mut{font-size:12.5px;color:var(--mut)}
.resume{display:flex;align-items:center;gap:14px;background:#fff;
  border:1px solid var(--hair);border-radius:14px;padding:14px 18px;margin:24px 0 4px}
.resume:hover{border-color:#C7CDF3}
.resume .rt{font-size:11.5px;font-weight:600;color:var(--mut);text-transform:uppercase;
  letter-spacing:.04em;margin-bottom:5px;display:flex;gap:8px;align-items:center}
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
  box-shadow:0 10px 32px rgba(27,31,48,.14);padding:5px;z-index:20;min-width:130px}
.cmenu div{padding:8px 12px;border-radius:7px;font-size:13px;color:var(--ink);cursor:pointer}
.cmenu div:hover{background:#F5F5F8}
.cmenu div.danger{color:#B33A3A}
.hcomposer{background:#fff;border:1.5px solid #C7CDF3;border-radius:18px;
  box-shadow:0 6px 24px rgba(82,102,235,.10);padding:6px 8px 8px;margin-bottom:10px}
.hcomposer:focus-within{border-color:#98A5F0;box-shadow:0 6px 28px rgba(82,102,235,.16)}
.hcomposer input{width:100%;border:none;outline:none;font:inherit;font-size:15px;
  color:var(--ink);background:none;padding:16px 16px 22px}
.hcomposer input::placeholder{color:#9A9DAB}
.hrow{display:flex;align-items:center;gap:8px;padding:0 8px 4px}
.hrow .sendbtn{margin-left:auto}
.mode{position:relative}
.mode summary{list-style:none;display:flex;gap:7px;align-items:center;cursor:pointer;
  font-size:13px;font-weight:500;color:#26293A;background:var(--pill);
  border-radius:9px;padding:7px 13px;transition:background .12s}
.mode summary:hover{background:#E6E7EC}
.mode summary:active,.newchat:active,.hint:active{background:#DEDFE6}
.mode summary:focus-visible,.newchat:focus-visible,.hint:focus-visible{
  outline:2px solid #98A5F0;outline-offset:2px}
.mode summary::-webkit-details-marker{display:none}
.mode summary svg{width:14px;height:14px;color:#5A5D6D}
.mode .menu{position:absolute;top:calc(100% + 8px);left:0;background:#fff;
  border:1px solid var(--hair);border-radius:12px;box-shadow:0 10px 32px rgba(27,31,48,.12);
  padding:6px;min-width:290px;z-index:5}
.mopt{display:flex;gap:10px;align-items:baseline;padding:9px 11px;border-radius:8px;
  font-size:13.5px;color:var(--ink)}
.mopt .tick{margin-left:auto;color:var(--accent);font-weight:600}
.mopt:hover{background:#F5F5F8}
.mopt.off{color:var(--mut)}
.mopt.off small{display:block;font-size:11.5px;color:var(--mut);margin-top:2px}
.active-h{display:flex;align-items:baseline;margin:30px 0 4px}
.active-h span{font-size:13px;font-weight:600;color:var(--mut)}
.active-h a{margin-left:auto;font-size:12.5px;color:var(--accent)}
.arow{display:flex;align-items:center;gap:13px;padding:13px 2px;
  border-bottom:1px solid #EDEDF2;font-size:14px;color:var(--ink)}
.arow:last-child{border-bottom:none}
.arow svg{width:16px;height:16px;color:var(--accent);flex:none}
.arow .sub{color:var(--mut);font-size:12.5px;display:block;margin-top:1px}
.arow .when{margin-left:auto;color:var(--mut);font-size:12.5px;flex:none}
.tryline{color:var(--mut);font-size:13.5px;margin:26px 0 12px;text-align:center}
.hints{display:flex;flex-wrap:wrap;gap:10px;justify-content:center}
.hint{display:flex;gap:8px;align-items:center;font-size:13px;color:#26293A;font-weight:500;
background:var(--pill);border:none;border-radius:9px;padding:8px 14px;cursor:pointer;
transition:background .12s}
.hint svg{width:14px;height:14px;color:#5A5D6D}
.hint:hover{background:#E6E7EC}
.msg{font-size:14.5px;line-height:1.6}
.msg.user{align-self:flex-end;max-width:82%;background:#F1F1F4;color:#26293A;
border-radius:14px;padding:12px 16px}
.msg.bot{align-self:flex-start;max-width:100%;color:#26293A;padding:2px 2px}
.msg.bot b{color:var(--ink)}
.rmeta{display:block;font-size:11px;color:var(--mut);margin-top:7px}
.cards{align-self:stretch;display:flex;flex-direction:column;gap:10px}
.mrun,.mcard{background:#fff;border:1px solid var(--hair);border-radius:12px;padding:14px 18px}
.mhead{display:flex;align-items:center;gap:10px;margin-bottom:9px}
.comp{font-size:12px;font-weight:600;color:#4553C8;background:var(--accent-soft);
border-radius:12px;padding:2px 10px}
.meta{font-size:12.5px;color:var(--mut)}
.mhead .st{margin-left:auto}
.st{font-size:12px;font-weight:500;border-radius:12px;padding:3px 10px;white-space:nowrap}
.st.ok{background:#E5F4EC;color:#177245}
.st.warn{background:#FCEED8;color:#9A6215}
.st.wait{background:var(--accent-soft);color:#4553C8}
.st.mut{background:#EFEFF3;color:#6A6D7D}
.alogo{display:inline-flex;width:18px;height:18px;border-radius:5px;color:#fff;
  font-size:10.5px;font-weight:700;align-items:center;justify-content:center;
  vertical-align:-4px;margin:0 5px 0 2px}
img.alogo{background:#fff;border:1px solid var(--hair);object-fit:contain;padding:1px}
.mclaim{background:#F4F4F7;border-radius:8px;padding:8px 12px;font-size:13px;
color:#4A4E63;margin-bottom:9px}
.mcounter{font-size:13.5px;color:#26293A;line-height:1.55}
.mrow{display:flex;align-items:baseline;gap:14px;padding:9px 0;border-bottom:1px solid #F1F1F5;
font-size:13.5px}
.mrow:last-child{border-bottom:none}
.mrow b{width:56px;flex:none;color:var(--ink);font-size:15px;font-weight:600}
.mrow b.warn{color:#9A6215}
.mrow span{flex:1;color:#26293A}
.mrow i{font-style:normal;color:var(--mut);font-size:12px}
.proposal{display:flex;gap:9px;padding:4px 2px}
""" + BTN_CSS + """
.composer{flex:none;padding:10px 24px 26px}
.composer .hcomposer{max-width:740px;margin:0 auto}
.composer .hcomposer input{padding:13px 16px 15px}
.composer .mode .menu{top:auto;bottom:calc(100% + 8px)}
.cbox{max-width:740px;margin:0 auto;display:flex;gap:10px;align-items:center;background:#fff;
border:1.5px solid #C7CDF3;border-radius:999px;padding:8px 8px 8px 22px;
box-shadow:0 6px 24px rgba(82,102,235,.10)}
.cbox:focus-within{border-color:#98A5F0;box-shadow:0 6px 28px rgba(82,102,235,.16)}
.cbox input{flex:1;border:none;outline:none;font:inherit;font-size:14.5px;color:var(--ink);
background:none}
.cbox input::placeholder{color:#9A9DAB}
</style></head><body>
__SIDEBAR__
<div class="main">
  <div class="convhead" id="convhead"><span id="ctitle">__CONVTITLE__</span>
    <a class="newchat" id="newchat" href="/">+&ensp;New chat</a>
    <span class="uwrap"><span class="uemail">__USER__</span>
    <a class="logout" href="/logout">Log out</a>
    <span class="avatar">__INITIAL__</span></span></div>
  <main id="main"><div class="thread" id="thread">__THREAD__
    <div class="hero" id="empty">
      __ROLEPILLS__<h1>__GREET__</h1>
      <div class="brief">__BRIEF__</div>
      <form class="hcomposer" onsubmit="event.preventDefault();send(hbox.value)">
        <input id="hbox" placeholder="How can I help you today?" autofocus autocomplete="off">
        <div class="hrow">
          <details class="mode" id="mode">
            <summary>""" + ICONS["gear"] + """<span>Approval mode: Manual</span></summary>
            <div class="menu">
              <div class="mopt" onclick="document.getElementById('mode').open=false">
                Manually approve<span class="tick">&#10003;</span></div>
              <div class="mopt off"><div>Automatically approve
                <small>Unlocks after 20 approved runs per play &mdash; the Conductor&rsquo;s rule.</small></div></div>
              <div class="mopt off"><div>Skip all approvals
                <small>Never. Nothing sends without you &mdash; by design.</small></div></div>
            </div>
          </details>
          <button class="sendbtn" aria-label="Send">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
              stroke-linecap="round" stroke-linejoin="round"><path d="m5 12 7-7 7 7"/><path d="M12 19V5"/></svg>
          </button>
        </div>
      </form>
      __SAMPLE__
      __RESUME__
      __ACTIVE__
      <div class="tryline">Try one of these:</div>
      <div class="hints">__CHIPS__</div>
    </div>
  </div></main>
  <div class="composer" id="composer">
  <form class="hcomposer" onsubmit="event.preventDefault();send(box.value)">
    <input id="box" placeholder="How can I help you today?" autocomplete="off">
    <div class="hrow">
      <details class="mode" id="mode2">
        <summary>""" + ICONS["gear"] + """<span>Approval mode: Manual</span></summary>
        <div class="menu">
          <div class="mopt" onclick="document.getElementById('mode2').open=false">
            Manually approve<span class="tick">&#10003;</span></div>
          <div class="mopt off"><div>Automatically approve
            <small>Unlocks after 20 approved runs per play &mdash; the Conductor&rsquo;s rule.</small></div></div>
          <div class="mopt off"><div>Skip all approvals
            <small>Never. Nothing sends without you &mdash; by design.</small></div></div>
        </div>
      </details>
      <button class="sendbtn" aria-label="Send">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
          stroke-linecap="round" stroke-linejoin="round"><path d="m5 12 7-7 7 7"/><path d="M12 19V5"/></svg>
      </button>
    </div>
  </form></div>
</div>
<script>
let CONV = "__CONVID__";
const thread = document.getElementById('thread');
const box = document.getElementById('box');
if (CONV) document.getElementById('empty')?.remove();
if (!CONV){
  document.getElementById('composer').style.display = 'none';
  document.getElementById('ctitle').style.visibility = 'hidden';
  document.getElementById('newchat').style.visibility = 'hidden';
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
async function convRename(id){
  const t = prompt('Rename conversation:');
  if (!t) return;
  await fetch('/api/conv/rename', {method:'POST',
    headers:{'Content-Type':'application/json'}, body: JSON.stringify({id, title: t})});
  location.reload();
}
function bubble(cls, html){
  const d = document.createElement('div'); d.className = cls; d.innerHTML = html;
  thread.appendChild(d); d.scrollIntoView({behavior:'smooth', block:'end'}); return d;
}
async function send(text){
  text = (text || '').trim(); if(!text) return;
  document.getElementById('empty')?.remove();
  document.getElementById('composer').style.display = '';
  bubble('msg user', text.replace(/</g,'&lt;')); box.value = '';
  const typing = bubble('msg bot', '&hellip;');
  const res = await fetch('/api/chat', {method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({message:text, conv_id: CONV})});
  const data = await res.json();
  if (data.conv_id && !CONV){
    CONV = data.conv_id;
    const t = document.getElementById('ctitle');
    t.style.visibility = '';
    t.textContent = data.title || 'Conversation';
    document.getElementById('newchat').style.visibility = '';
    history.replaceState(null, '', '/?c=' + CONV);
  }
  typing.innerHTML = data.reply;
  if(data.cards) bubble('cards', data.cards);
}
async function confirmProposal(pid){
  const res = await fetch('/api/confirm', {method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({proposal: pid, conv_id: CONV})});
  const data = await res.json();
  document.getElementById('prop-'+pid)?.remove();
  bubble('msg bot', data.reply);
  if(data.cards) bubble('cards', data.cards);
}
function cancelProposal(pid){
  document.getElementById('prop-'+pid)?.remove();
  bubble('msg bot', 'Cancelled — nothing was sent.');
}
</script>
</body></html>"""


def _briefing(tid: str, persona: str = "owner") -> tuple[str, str, list[str]]:
    """Greeting, one-line state summary, and prompt chips — computed from the
    ledger, not hardcoded. The home is a briefing officer, not a chatbox."""
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
        bits = [f"<b>{esc_n}</b> escalation{'s' if esc_n != 1 else ''} need you"
                if esc_n else "no escalations",
                f"evidence vault: <b>{ev_n}</b> proofs, <b>{len(notes)}</b> voice notes",
                f"<b>{material}</b> material edit{'s' if material != 1 else ''} to learn from"]
        chips = ["Show escalations", "Which evidence is winning?",
                 "What did merchants change in our drafts?"]
        return (f"Good {tod}", ", ".join(bits) + ".", chips)
    if persona == "finance":
        won_total = sum((led.outcome_for(r.run_id).outcome_value or {}).get("amount_paise") or 0
                        for r in runs
                        if led.outcome_for(r.run_id)
                        and (led.outcome_for(r.run_id).outcome_value or {}).get("won"))
        gated = [r for r in runs if r.gate_action is not None]
        used = sum(1 for r in gated if r.gate_action is not GateAction.REJECT)
        adopt = f"{100 * used // len(gated)}%" if gated else "&mdash;"
        bits = [f"<b>&#8377;{won_total / 100:,.0f}</b> disputes won this quarter",
                f"<b>{adopt}</b> of responses used by merchants",
                "win-rate by reason code reads out once volume builds"]
        chips = ["What closed recently?", "How is adoption trending?",
                 "Show the pipeline"]
        return (f"Good {tod}", ", ".join(bits) + ".", chips)
    if persona == "builder":
        would = sum(1 for row in SHADOW_ROWS if row[4] == "send")
        bits = [f"<b>{len(runs)}</b> runs through the pipeline, "
                f"<b>{esc_n}</b> escalated",
                "connectors: <b>4/4</b> healthy (bank webhook, Slack, CRM, email)",
                f'<a href="/shadow">shadow scan: <b>{would}</b> responses it '
                f'would have filed</a>']
        chips = ["What would have filed last night?", "How are the metrics?",
                 "Show the run ledger"]
        return (f"Good {tod}", ", ".join(bits) + ".", chips)
    bits, chips = [], []
    if waiting:
        age = WORLD.d.clock.now() - waiting[0].occurred_at
        h = int(age.total_seconds() // 3600)
        oldest = (f"oldest {h // 24}d" if h >= 48
                  else f"oldest {h}h" if h
                  else f"oldest {int(age.total_seconds() // 60)}m")
        bits.append(f'<a href="/approvals"><b>{len(waiting)}</b> '
                    f"dispute response{'s' if len(waiting) > 1 else ''} "
                    f"awaiting your approval ({oldest})</a>")
        chips.append("What&rsquo;s awaiting review?")
        comp = COMP.get(waiting[0].reason_code, waiting[0].reason_code)
        chips.append(f"Approve {esc(comp)} at {esc(_account_label(waiting[0]))}")
    if esc_n:
        bits.append(f"<b>{esc_n}</b> escalation{'s' if esc_n > 1 else ''} for ops")
        chips.append("Show escalations")
    for r, out in wins[:1]:
        v = (out.outcome_value or {}) if out else {}
        if v.get("won"):
            bits.append(f"<b>{esc(_account_label(r))}</b> won a "
                        f"&#8377;{v.get('amount_paise', 0) / 100000:.0f}k dispute")
            chips.append("What closed recently?")
    if not bits:
        bits.append("your workspace is quiet &mdash; load sample data below, "
                    "or connect your rails in Settings")
        chips = ["What can you do?", "Show the ledger"]
    chips.append("Which evidence is winning?")
    return (f"Good {tod}", ", ".join(bits[:3]) + ".", chips[:4])


def chat_render(tid: str = "t1", conv_id: str = "", email: str = "", persona: str = "owner") -> str:
    c = CONVS.get(conv_id)
    if c and c["tenant"] != tid:
        c = None
    thread, title, cid = "", "New conversation", ""
    if c:
        thread = "".join(f'<div class="{m["who"]}">{m["html"]}</div>' for m in c["msgs"])
        title, cid = esc(c["title"]), c["id"]
    greet, brief, chips = _briefing(tid, persona)
    pills = "".join(
        f'<a class="{"on" if persona == k else ""}" href="/?as={k}">{t}</a>'
        for k, t in (("owner", "Owner"), ("ops", "Ops lead"),
                     ("finance", "Finance lead"), ("builder", "Builder")))
    role_html = f'<div class="rolepills"><span>View as</span>{pills}</div>' 
    name = (CONVS and "") or ""
    chips_html = "".join(
        f'<span class="hint" onclick="send(this.textContent)">{c}</span>' for c in chips)

    # resume: newest conversation with an undecided proposal, else newest
    mine_convs = sorted((c for c in CONVS.values() if c["tenant"] == tid),
                        key=lambda c: -c["seq"])
    resume = next((c for c in mine_convs if c.get("pending")),
                  mine_convs[0] if mine_convs else None)
    resume_html = ""
    if resume and not cid:
        last = next((m["html"] for m in reversed(resume["msgs"])
                     if m["who"] == "msg bot"), "")
        snippet = esc(" ".join(_re.sub("<[^>]+>", " ", last).split())[:90])
        if len(snippet) < 45 or snippet.lower().startswith("nothing"):
            snippet = ""
        tag = ('<span class="st pendtag">decision pending</span>'
               if resume.get("pending") else "")
        resume_html = (
            f'<a class="resume" href="/?c={resume["id"]}">'
            f'<div><div class="rt">Continue where you left off {tag}</div>'
            f'<b>{esc(resume["title"])}</b>'
            + (f'<span class="sub">{snippet}&hellip;</span>' if snippet else '')
            + '</div>'
            f'<span class="go">&rarr;</span></a>')

    has_runs = any(r.tenant_id == tid for r in WORLD.d.ledger.runs.values())
    sample_html = "" if has_runs else (
        '<form method="post" action="/api/sample" class="samplecta">'
        '<button class="btn primary">See it working &mdash; load sample data</button>'
        '<span class="mut">Simulates a dispute webhook through the real '
        'pipeline. Nothing files anywhere.</span></form>')
    waiting = sorted((r for r in WORLD.d.ledger.runs.values()
                      if r.tenant_id == tid and r.state is RunState.AWAITING_GATE),
                     key=lambda r: r.occurred_at, reverse=True)[:5]
    rows = "".join(
        f'<a class="arow" href="/tasks">{ICONS["bolt"]}'
        f'<span>Response to the {esc(COMP.get(r.reason_code, r.reason_code))} dispute '
        f'&ldquo;{esc(r.claim_text)}&rdquo;'
        f'<span class="sub">merchant &middot; '
        f'{_logo(_account_label(r))}{esc(_account_label(r))}</span></span>'
        f'<span class="when">{r.occurred_at.strftime("%b %-d")}</span></a>'
        for r in waiting)
    active = (f'<div class="active-h"><span>Active</span>'
              f'<a href="/tasks">Review all &rarr;</a></div>{rows}') if rows else ""
    page = (CHAT_TEMPLATE
            .replace("__SIDEBAR__", sidebar_html("cmd", tid, convs=conv_list_html(tid, cid)))
            .replace("__CONVTITLE__", title)
            .replace("__CONVID__", cid)
            .replace("__SAMPLE__", sample_html)
            .replace("__ROLEPILLS__", role_html)
            .replace("__GREET__", greet)
            .replace("__BRIEF__", brief)
            .replace("__CHIPS__", chips_html)
            .replace("__RESUME__", resume_html)
            .replace("__ACTIVE__", active)
            .replace("__USER__", esc(email))
            .replace("__INITIAL__", esc((email or "?")[0]))
            .replace("__THREAD__", thread))
    return page


if __name__ == "__main__":
    seed_conversations()
    print(f"Relay workspace on http://localhost:{PORT}")
    HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
