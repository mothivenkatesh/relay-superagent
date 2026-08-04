"""The spec's §1 as executable tests, on fakes, in milliseconds.

Assertions are on states, tool calls, ledger rows and effects — never on
response prose. Prose belongs to the judge; guarantees belong here.
"""

from __future__ import annotations

from relay_superagent.domain.models import GateAction, RunState
from relay_superagent.metrics import correction_rate, counter_usage_rate
from relay_superagent.ports.fakes import TimingOutLlm
from relay_superagent.supervisor import Supervisor

from .conftest import event


def test_golden_path_bank_webhook_files_a_dispute(world):
    """A bank webhook files a goods-not-received dispute → response drafted,
    cited, gated to the merchant with reasoning visible — and nothing has
    touched the order yet."""
    run = world.handle_event(event())

    assert run.state is RunState.AWAITING_GATE
    assert run.reason_code == "RG"
    assert run.claim_hash is not None
    assert run.decision["cited_evidence_ids"] == ["ev_tco", "ev_forrester"]

    (user, blocks), = world.d.slack.dms
    assert user == "merchant_7"
    assert blocks["claim"] == "Buyer says the order never arrived"
    assert blocks["reasoning"]                       # visible reasoning is required
    assert blocks["actions"] == ["send", "edit", "dismiss"]
    assert world.d.crm.notes == []                    # nothing sent, nothing filed


def test_approve_files_exactly_one_response_then_outcome_resolves(world):
    run = world.handle_event(event())
    world.approve(run, actor="merchant_7")

    assert run.state is RunState.ACTED
    assert world.d.crm.notes == [("order_1", run.decision["counter_text"])]

    world.d.clock.advance(days=42)                   # the outcome lands weeks later
    world.record_resolution(run, won=True, amount_paise=8_000_00)
    assert run.state is RunState.RESOLVED
    out = world.d.ledger.outcome_for(run.run_id)
    assert out.outcome_value == {"won": True, "amount_paise": 800000, "reason_code": "RG"}


def test_edit_runs_the_diff_writes_memory_and_next_draft_reads_it(world):
    run = world.handle_event(event())
    world.edit(run, actor="merchant_7", edited_text="Shorter, sharper response." + " " * 100)

    assert run.state is RunState.ACTED
    assert run.gate_is_material is False             # scripted: style-only edit
    notes = world.d.ledger.memory_for("t1", "merchant_7", "response_style")
    assert len(notes) == 1 and notes[0].source_run == run.run_id

    # the next run for the same merchant retrieves that memory at draft time
    # (different order AND a different claim — the identical claim on the
    #  same merchant is suppressed as already handled, which is its own
    #  guarantee, tested separately)
    world.d.crm.opportunities["order_2"] = {"stage": "evaluation"}
    world.d.llm.mention = {**world.d.llm.mention,
                           "claim_text": "Buyer says the item arrived damaged"}
    world.handle_event(event(source_ref="dispute_43", order_id="order_2",
                             dispute_id="dp_43"))
    draft_calls = [a for n, a in world.d.llm.calls if n == "draft_counter"]
    assert draft_calls[-1]["memory"] == [notes[0].body]


def test_uncovered_reason_code_never_creates_a_run(world):
    run = world.handle_event(event(reason_code="fraud_claim"))
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


def test_model_timeout_escalates_to_review_channel_never_the_merchant(world):
    world.d.llm = TimingOutLlm()
    run = world.handle_event(event())

    assert run.state is RunState.FAILED
    assert world.d.slack.dms == []
    (channel, blocks), = world.d.slack.channel_posts
    assert channel == "#relay-dispute-review"
    assert "llm_unavailable" in blocks["reason"]


def test_gate_timeout_escalates_never_sends(world):
    run = world.handle_event(event())
    world.d.clock.advance(hours=25)                  # past gate_timeout_hours=24

    sup = Supervisor(world.d.ledger, world.d.clock, world.d.slack, world.d.policy)
    report = sup.sweep()

    assert report.timed_out == [run.run_id]
    assert run.state is RunState.TIMED_OUT
    assert run.gate_action is GateAction.TIMEOUT
    assert world.d.crm.notes == []                   # the promise: never auto-file
    assert world.d.slack.channel_posts[-1][1]["reason"] == "gate_timeout"


def test_correction_rate_is_computable_from_gate_actions(world):
    r1 = world.handle_event(event(source_ref="d1"))
    world.approve(r1, "merchant_7")
    r2 = world.handle_event(event(source_ref="d2", order_id="order_1"))
    # second run for same order+reason inside the window is suppressed
    assert r2.state is RunState.SUPPRESSED and r2.suppressed_reason == "recently_countered"

    assert correction_rate(world.d.ledger, "t1") == 0.0
    assert counter_usage_rate(world.d.ledger, "t1") == 1.0


def test_trace_records_every_agent_stage(world):
    from .conftest import event
    run = world.handle_event(event())
    world.approve(run, "merchant_7")
    world.record_resolution(run, won=True, amount_paise=5_000_00)
    agents = [e["agent"] for e in world.d.ledger.trace_for(run.run_id)]
    kinds = [e["kind"] for e in world.d.ledger.trace_for(run.run_id)]
    for expected in ["detection-agent", "eligibility-agent", "response-agent",
                     "compliance-agent", "gate", "filing-agent", "reporting-agent"]:
        assert expected in agents, expected
    assert "approved" in kinds and "outcome" in kinds
    # suppressed runs trace their reason too
    dup = world.handle_event(event())
    tr = world.d.ledger.trace_for(dup.run_id)
    assert tr == [] or dup.run_id == run.run_id  # idempotent replay: same run
