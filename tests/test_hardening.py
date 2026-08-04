"""Idempotency, the no-holdout guarantee, classification edges, and the queue
cap: the guarantees that make replays, crashes and floods boring."""

from __future__ import annotations

from relay_superagent.detect import classify_dispute
from relay_superagent.domain.models import Arm, RunState
from relay_superagent.supervisor import Supervisor

from .conftest import event, make_policy


def test_effect_table_fires_a_side_effect_exactly_once(world):
    run = world.handle_event(event())
    calls = []
    for _ in range(5):
        world.d.ledger.effect(run.run_id, "slack_post", "merchant_7",
                              lambda: calls.append(1) or "ref")
    assert calls == []                       # already fired during handle_event
    assert len(world.d.slack.dms) == 1


def test_policy_version_change_permits_a_new_run(world):
    first = world.handle_event(event())
    world.approve(first, "merchant_7")
    world.d.policy = make_policy(policy_version="pol_2",
                                 suppress_window_days=0)
    second = world.handle_event(event())
    assert second.run_id != first.run_id     # new policy, new idempotency key


def test_disputes_are_never_held_out_even_at_100pct_holdout(world):
    """A missed chargeback deadline is real merchant financial harm, not a
    valid A/B experiment — so unlike the GTM watcher this fork descends
    from, holdout_pct is ignored and every dispute run lands TREATED."""
    world.d.policy = make_policy(holdout_pct=100)
    run = world.handle_event(event())
    assert run.arm is Arm.TREATED
    assert run.state is RunState.AWAITING_GATE


def test_classify_dispute_matches_known_codes_and_rejects_unknown(world):
    reason = classify_dispute("RG", world.d.policy)
    assert reason is not None and reason.id == "goods_not_received"
    assert classify_dispute("fraud_claim", world.d.policy) is None
    assert classify_dispute(None, world.d.policy) is None


def test_queue_cap_reports_merchant_as_flooded(world):
    for i in range(3):
        world.d.crm.opportunities[f"order_{i+10}"] = {"stage": "evaluation"}
        world.handle_event(event(source_ref=f"d{i}", order_id=f"order_{i+10}"))
    sup = Supervisor(world.d.ledger, world.d.clock, world.d.slack, world.d.policy)
    assert sup.over_cap("merchant_7")
    assert "merchant_7" in sup.sweep().merchants_over_cap


def test_crash_parked_run_is_escalated_on_the_second_sweep(world):
    """A run dead between draft and the Slack post (CHECKED forever) must not
    sit silent: sweep 1 marks, sweep 2 fails it and tells the PMM."""
    run = world.handle_event(event())
    assert run.state is RunState.AWAITING_GATE
    run.state = RunState.CHECKED          # simulate the crash park directly

    sup = Supervisor(world.d.ledger, world.d.clock, world.d.slack, world.d.policy)
    assert sup.sweep().stalled == []      # first sighting only marks
    report = sup.sweep()
    assert report.stalled == [run.run_id]
    assert run.state is RunState.FAILED
    assert any("stalled_at_checked" in str(m)
               for m in world.d.slack.channel_posts)


def test_run_that_progressed_between_sweeps_is_not_touched(world):
    run = world.handle_event(event())
    run.state = RunState.RUNNING
    sup = Supervisor(world.d.ledger, world.d.clock, world.d.slack, world.d.policy)
    sup.sweep()
    run.state = RunState.CHECKED          # it moved: alive, not stalled
    assert sup.sweep().stalled == []
    run.state = RunState.AWAITING_GATE    # reached the gate: forgotten
    assert sup.sweep().stalled == []
    assert run.state is RunState.AWAITING_GATE
