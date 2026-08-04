from __future__ import annotations

from datetime import datetime

import pytest

from relay_superagent.domain.models import DisputeReason, EvidenceItem, Policy, TriggerEvent
from relay_superagent.ledger import Ledger
from relay_superagent.pipeline import Deps, Pipeline
from relay_superagent.ports.fakes import (
    FakeClock, FakeCrm, FakeSlack, FakeUrlChecker, ScriptedLlm,
)

MON_9AM = datetime(2026, 8, 3, 9, 0)  # a Monday


def make_policy(**kw) -> Policy:
    defaults = dict(
        policy_version="pol_1",
        tenant_id="t1",
        dispute_reasons=[DisputeReason(id="goods_not_received", code="RG",
                                       label="Goods/services not received")],
        banned_terms=["best", "leading", "number one"],
        holdout_pct=0,          # deterministic tests default to treated
    )
    defaults.update(kw)
    return Policy(**defaults)


EVIDENCE = [
    EvidenceItem("ev_tco", "t1", "RG", "delivery_proof",
                 "Courier proof-of-delivery, signed", "https://ours.example/tco"),
    EvidenceItem("ev_forrester", "t1", "RG", "communication_log",
                 "WhatsApp delivery confirmation with the buyer", "https://ours.example/forrester"),
]


@pytest.fixture
def world():
    clock = FakeClock(MON_9AM)
    crm = FakeCrm(opportunities={
        "order_1": {"stage": "evaluation", "amount_band": "50-100k",
                    "prior_dispute_history": ["RG"]},
    })
    deps = Deps(
        clock=clock,
        llm=ScriptedLlm(),
        crm=crm,
        slack=FakeSlack(),
        url_checker=FakeUrlChecker(),
        ledger=Ledger(),
        policy=make_policy(),
        evidence=list(EVIDENCE),
        enrolled_merchants={"merchant_7"},
    )
    return Pipeline(deps)


def event(**kw) -> TriggerEvent:
    defaults = dict(
        tenant_id="t1", source="bank_webhook", source_ref="dispute_42", occurred_at=MON_9AM,
        order_id="order_1", merchant_id="merchant_7", dispute_id="dp_42", reason_code="RG",
        text="Buyer says the order never arrived, disputing the charge.",
    )
    defaults.update(kw)
    return TriggerEvent(**defaults)
