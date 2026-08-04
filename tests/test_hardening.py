"""Idempotency, holdout, detection edges, and the queue cap: the guarantees
that make replays, crashes and floods boring."""

from __future__ import annotations

from relay_superagent.detect import match_competitors
from relay_superagent.domain.models import RunState
from relay_superagent.supervisor import Supervisor

from .conftest import event, make_policy


def test_effect_table_fires_a_side_effect_exactly_once(world):
    run = world.handle_event(event())
    calls = []
    for _ in range(5):
        world.d.ledger.effect(run.run_id, "slack_post", "rep_7",
                              lambda: calls.append(1) or "ref")
    assert calls == []                       # already fired during handle_event
    assert len(world.d.slack.dms) == 1


def test_policy_version_change_permits_a_new_run(world):
    first = world.handle_event(event())
    world.approve(first, "rep_7")
    world.d.policy = make_policy(policy_version="pol_2",
                                 suppress_window_days=0)
    second = world.handle_event(event())
    assert second.run_id != first.run_id     # new policy, new idempotency key


def test_holdout_account_is_recorded_but_untreated():
    from .conftest import EVIDENCE, MON_9AM
    from relay_superagent.ledger import Ledger
    from relay_superagent.pipeline import Deps, Pipeline
    from relay_superagent.ports.fakes import FakeClock, FakeCrm, FakeSlack, FakeUrlChecker, ScriptedLlm

    deps = Deps(clock=FakeClock(MON_9AM), llm=ScriptedLlm(),
                crm=FakeCrm(opportunities={"opp_1": {"stage": "evaluation"}}),
                slack=FakeSlack(), url_checker=FakeUrlChecker(), ledger=Ledger(),
                policy=make_policy(holdout_pct=100), evidence=list(EVIDENCE),
                enrolled_reps={"rep_7"})
    p = Pipeline(deps)
    run = p.handle_event(event())
    assert run.state is RunState.SUPPRESSED
    assert run.suppressed_reason == "holdout"
    assert deps.slack.dms == []              # counted for the experiment, never surfaced


def test_word_boundary_match_ignores_acme_street(world):
    # 'Acmeville' must not fire; a bare 'Acme' must.
    assert match_competitors("We drove through Acmeville yesterday",
                             world.d.policy) == []
    assert match_competitors("Acme quoted us last week",
                             world.d.policy)[0].id == "acme"


def test_queue_cap_reports_rep_as_flooded(world):
    for i in range(3):
        world.d.crm.opportunities[f"opp_{i+10}"] = {"stage": "evaluation"}
        world.handle_event(event(source_ref=f"c{i}", opportunity_id=f"opp_{i+10}"))
    sup = Supervisor(world.d.ledger, world.d.clock, world.d.slack, world.d.policy)
    assert sup.over_cap("rep_7")
    assert "rep_7" in sup.sweep().reps_over_cap


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
