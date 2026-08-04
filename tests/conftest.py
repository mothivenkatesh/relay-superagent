from __future__ import annotations

from datetime import datetime

import pytest

from relay_superagent.domain.models import Competitor, EvidenceItem, Policy, TriggerEvent
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
        competitors=[Competitor(id="acme", names=["Acme", "Acme Corp"],
                                domains=["acme.com"])],
        banned_terms=["best", "leading", "number one"],
        holdout_pct=0,          # deterministic tests default to treated
    )
    defaults.update(kw)
    return Policy(**defaults)


EVIDENCE = [
    EvidenceItem("ev_tco", "t1", "acme", "pricing",
                 "Three-year TCO comparison", "https://ours.example/tco"),
    EvidenceItem("ev_forrester", "t1", "acme", "pricing",
                 "Forrester note on hidden per-seat overage", "https://ours.example/forrester"),
]


@pytest.fixture
def world():
    clock = FakeClock(MON_9AM)
    crm = FakeCrm(opportunities={
        "opp_1": {"stage": "evaluation", "amount_band": "50-100k",
                  "competitor_history": ["acme"]},
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
        enrolled_reps={"rep_7"},
    )
    return Pipeline(deps)


def event(**kw) -> TriggerEvent:
    defaults = dict(
        tenant_id="t1", source="gong", source_ref="call_42", occurred_at=MON_9AM,
        opportunity_id="opp_1", account_id="acct_1", rep_user_id="rep_7",
        text="Well, we're also looking at Acme, and honestly they are cheaper.",
    )
    defaults.update(kw)
    return TriggerEvent(**defaults)
