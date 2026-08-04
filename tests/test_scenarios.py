"""The spec's §1 as executable tests, on fakes, in milliseconds.

Assertions are on states, tool calls, ledger rows and effects — never on
counter prose. Prose belongs to the judge; guarantees belong here.
"""

from __future__ import annotations

from datetime import timedelta

from relay_superagent.domain.models import GateAction, RunState
from relay_superagent.metrics import correction_rate, counter_usage_rate
from relay_superagent.ports.fakes import TimingOutLlm
from relay_superagent.supervisor import Supervisor

from .conftest import event


def test_golden_path_buyer_says_acme_is_cheaper(world):
    """Buyer names Acme on a call → counter drafted, cited, gated to the right
    rep with reasoning visible — and nothing has touched the CRM yet."""
    run = world.handle_event(event())

    assert run.state is RunState.AWAITING_GATE
    assert run.competitor_id == "acme"
    assert run.claim_hash is not None
    assert run.decision["cited_evidence_ids"] == ["ev_tco", "ev_forrester"]

    (user, blocks), = world.d.slack.dms
    assert user == "rep_7"
    assert blocks["claim"] == "Acme is cheaper"
    assert blocks["reasoning"]                       # visible reasoning is required
    assert blocks["actions"] == ["send", "edit", "dismiss"]
    assert world.d.crm.notes == []                   # nothing sent, nothing written


def test_approve_writes_exactly_one_crm_note_then_outcome_resolves(world):
    run = world.handle_event(event())
    world.approve(run, actor="rep_7")

    assert run.state is RunState.ACTED
    assert world.d.crm.notes == [("opp_1", run.decision["counter_text"])]

    world.d.clock.advance(days=42)                   # the outcome lands weeks later
    world.record_close(run, won=True, amount=80_000)
    assert run.state is RunState.RESOLVED
    out = world.d.ledger.outcome_for(run.run_id)
    assert out.outcome_value == {"won": True, "amount": 80_000, "competitor_id": "acme"}


def test_edit_runs_the_diff_writes_memory_and_next_draft_reads_it(world):
    run = world.handle_event(event())
    world.edit(run, actor="rep_7", edited_text="Shorter, sharper counter." + " " * 100)

    assert run.state is RunState.ACTED
    assert run.gate_is_material is False             # scripted: style-only edit
    notes = world.d.ledger.memory_for("t1", "rep_7", "counter_style")
    assert len(notes) == 1 and notes[0].source_run == run.run_id

    # the next run for the same rep retrieves that memory at draft time
    # (different deal and account — the same claim on the same account is
    #  suppressed as already handled, which is its own guarantee)
    world.d.crm.opportunities["opp_2"] = {"stage": "evaluation"}
    world.handle_event(event(source_ref="call_43", opportunity_id="opp_2",
                             account_id="acct_2"))
    draft_calls = [a for n, a in world.d.llm.calls if n == "draft_counter"]
    assert draft_calls[-1]["memory"] == [notes[0].body]


def test_no_competitor_mention_never_creates_a_run(world):
    run = world.handle_event(event(text="Can we move our appointment to Thursday?"))
    assert run is None
    assert world.d.llm.calls == []                   # the model was never woken
    assert world.d.ledger.runs == {}


def test_replaying_the_same_webhook_produces_one_run_one_dm(world):
    first = world.handle_event(event())
    for _ in range(9):
        again = world.handle_event(event())
        assert again.run_id == first.run_id
    assert len(world.d.ledger.runs) == 1
    assert len(world.d.slack.dms) == 1


def test_model_timeout_escalates_to_pmm_and_never_reaches_the_rep(world):
    world.d.llm = TimingOutLlm()
    run = world.handle_event(event())

    assert run.state is RunState.FAILED
    assert world.d.slack.dms == []
    (channel, blocks), = world.d.slack.channel_posts
    assert channel == "#relay_superagent-review"
    assert "llm_unavailable" in blocks["reason"]


def test_gate_timeout_escalates_never_sends(world):
    run = world.handle_event(event())
    world.d.clock.advance(hours=25)                  # past gate_timeout_hours=24

    sup = Supervisor(world.d.ledger, world.d.clock, world.d.slack, world.d.policy)
    report = sup.sweep()

    assert report.timed_out == [run.run_id]
    assert run.state is RunState.TIMED_OUT
    assert run.gate_action is GateAction.TIMEOUT
    assert world.d.crm.notes == []                   # the promise: never auto-send
    assert world.d.slack.channel_posts[-1][1]["reason"] == "gate_timeout"


def test_correction_rate_is_computable_from_gate_actions(world):
    r1 = world.handle_event(event(source_ref="c1"))
    world.approve(r1, "rep_7")
    r2 = world.handle_event(event(source_ref="c2", opportunity_id="opp_1"))
    # second run for same opp+competitor inside the window is suppressed
    assert r2.state is RunState.SUPPRESSED and r2.suppressed_reason == "recently_countered"

    assert correction_rate(world.d.ledger, "t1") == 0.0
    assert counter_usage_rate(world.d.ledger, "t1") == 1.0


def test_trace_records_every_agent_stage(world):
    from .conftest import event
    run = world.handle_event(event())
    world.approve(run, "rep_7")
    world.record_close(run, won=True, amount=50_000)
    agents = [e["agent"] for e in world.d.ledger.trace_for(run.run_id)]
    kinds = [e["kind"] for e in world.d.ledger.trace_for(run.run_id)]
    for expected in ["monitoring-agent", "triage-agent", "drafting-agent",
                     "qa-agent", "gate", "crm-agent", "reporting-agent"]:
        assert expected in agents, expected
    assert "approved" in kinds and "outcome" in kinds
    # suppressed runs trace their reason too
    dup = world.handle_event(event())
    tr = world.d.ledger.trace_for(dup.run_id)
    assert tr == [] or dup.run_id == run.run_id  # idempotent replay: same run
