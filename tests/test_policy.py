"""The two deterministic gates (§6.3 safety, §6.6 layer 1), rule by rule."""

from __future__ import annotations

from relay_superagent.domain.models import Draft, RunState
from relay_superagent.policy import layer1

from .conftest import event, make_policy


# -- safety (exercised through the pipeline so the ordering is real) ----------

def test_closed_order_suppresses_before_any_drafting(world):
    world.d.crm.opportunities["order_1"]["stage"] = "closed_lost"
    run = world.handle_event(event())
    assert run.state is RunState.SUPPRESSED
    assert run.suppressed_reason == "order_closed"
    assert not any(n == "draft_counter" for n, _ in world.d.llm.calls)


def test_unknown_order_suppresses(world):
    run = world.handle_event(event(order_id="order_missing"))
    assert run.suppressed_reason == "no_order"


def test_unenrolled_merchant_suppresses(world):
    run = world.handle_event(event(merchant_id="merchant_99"))
    assert run.suppressed_reason == "merchant_not_enrolled"


def test_merchant_daily_cap_suppresses_the_sixth_run(world):
    # cap is 5; give each run its own order so dedupe doesn't fire first
    for i in range(5):
        world.d.crm.opportunities[f"order_{i+10}"] = {"stage": "evaluation"}
        r = world.handle_event(event(source_ref=f"d{i}", order_id=f"order_{i+10}"))
        assert r.state is RunState.AWAITING_GATE
    world.d.crm.opportunities["order_99"] = {"stage": "evaluation"}
    sixth = world.handle_event(event(source_ref="d9", order_id="order_99"))
    assert sixth.suppressed_reason == "merchant_daily_cap"


def test_token_ceiling_suppresses(world):
    world.d.tokens_spent_today = 10_000_000
    run = world.handle_event(event())
    assert run.suppressed_reason == "tenant_token_ceiling"


def test_suppressed_runs_are_still_written(world):
    world.d.crm.opportunities["order_1"]["stage"] = "closed_won"
    world.handle_event(event())
    assert len(world.d.ledger.runs) == 1     # the precision denominator survives


# -- layer 1 ------------------------------------------------------------------

def _draft(**kw):
    d = dict(counter_text="A perfectly reasonable dispute response." + " " * 110,
             cited_evidence_ids=["ev_tco"], confidence=0.9, escalate=False)
    d.update(kw)
    return Draft(**d)


def _check(draft, world, run):
    return layer1(draft, run, world.d.policy,
                  known_evidence_ids={"ev_tco", "ev_forrester"},
                  urls={"ev_tco": "https://ours.example/tco",
                        "ev_forrester": "https://ours.example/forrester"},
                  url_checker=world.d.url_checker)


def test_banned_term_blocks(world):
    run = world.handle_event(event())
    bad = _draft(counter_text="We are simply the best choice here." + " " * 100)
    assert any(f.startswith("banned_term:best") for f in _check(bad, world, run))


def test_unresolved_evidence_blocks(world):
    run = world.handle_event(event())
    bad = _draft(cited_evidence_ids=["ev_invented"])
    assert "evidence_unresolved:ev_invented" in _check(bad, world, run)


def test_dead_source_blocks_and_routes_to_pmm_not_merchant(world):
    world.d.url_checker.dead.add("https://ours.example/tco")
    run = world.handle_event(event())
    assert run.state is RunState.FAILED           # escalated, not surfaced
    assert world.d.slack.dms == []
    assert "source_dead" in world.d.slack.channel_posts[0][1]["reason"]


def test_contact_info_in_counter_blocks(world):
    run = world.handle_event(event())
    bad = _draft(counter_text="Email me at ops@ours.example for a discount." + " " * 90)
    assert "contact_info_in_counter" in _check(bad, world, run)


def test_low_judge_score_escalates_to_pmm(world):
    world.d.llm.verdict = {"addresses_claim": 2, "matches_register": 5,
                           "evidence_grounded": 5}
    run = world.handle_event(event())
    assert run.state is RunState.FAILED
    assert world.d.slack.dms == []
