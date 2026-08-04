"""The state machine: every legal edge walks, every illegal edge raises, gate
fields write exactly once, and arm assignment is stable per account."""

from __future__ import annotations

from datetime import datetime

import pytest

from relay_superagent.domain.models import (
    Arm, GateAction, IllegalTransition, LEGAL, Run, RunState, assign_arm,
)


def make_run(state=RunState.RECEIVED) -> Run:
    r = Run(run_id="r1", tenant_id="t1", idempotency_key="k1",
            trigger_source="gong", trigger_ref="c1",
            occurred_at=datetime(2026, 8, 3, 9, 0),
            policy_version="pol_1", arm=Arm.TREATED)
    r.state = state
    return r


def test_every_legal_edge_transitions():
    for src, dsts in LEGAL.items():
        for dst in dsts:
            run = make_run(src)
            run.transition(dst)
            assert run.state is dst


def test_every_illegal_edge_raises():
    all_states = set(RunState)
    for src in all_states:
        for dst in all_states - LEGAL[src]:
            run = make_run(src)
            with pytest.raises(IllegalTransition):
                run.transition(dst)


def test_terminal_states_have_no_exits():
    for s in (RunState.SUPPRESSED, RunState.REJECTED, RunState.TIMED_OUT,
              RunState.RESOLVED, RunState.FAILED):
        assert LEGAL[s] == set()


def test_gate_fields_write_exactly_once():
    run = make_run(RunState.AWAITING_GATE)
    run.record_gate("rep_7", GateAction.APPROVE, datetime(2026, 8, 3, 10, 0))
    with pytest.raises(IllegalTransition):
        run.record_gate("rep_7", GateAction.REJECT, datetime(2026, 8, 3, 11, 0))


def test_arm_assignment_is_stable_and_account_level():
    a = assign_arm("acct_42", holdout_pct=20)
    for _ in range(100):
        assert assign_arm("acct_42", holdout_pct=20) is a   # two reps, one deal, one arm


def test_holdout_pct_zero_and_hundred_are_absolute():
    for acct in ("a", "b", "c", "acct_42"):
        assert assign_arm(acct, 0) is Arm.TREATED
        assert assign_arm(acct, 100) is Arm.HOLDOUT
