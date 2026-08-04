"""The supervisor: a worker loop over the run table with no model in it.

It does the three things that have exactly one correct answer — expire stalled
gates into PMM escalation (never auto-send), enforce the per-rep queue cap, and
resolve acted runs whose outcome has arrived. In production this is
`SELECT … FOR UPDATE SKIP LOCKED` over Postgres; the sweep's shape is identical.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta

from relay_superagent.domain.models import GateAction, Policy, Run, RunState
from relay_superagent.ledger import Ledger
from relay_superagent.ports.base import Clock, SlackPort

# A rep with more than this many undecided cards stops getting new ones (§8).
QUEUE_CAP = 3

# Working states a run can crash-park in (e.g. dead between draft and the
# Slack post → CHECKED forever). All of them have a legal edge to FAILED.
# AWAITING_GATE is excluded — that's the timeout path; ACTED is excluded —
# acted runs legitimately wait weeks for an outcome.
STALL_STATES = {
    RunState.RECEIVED, RunState.RUNNING, RunState.DRAFTED, RunState.CHECKED,
    RunState.APPROVED, RunState.EDITED, RunState.ACTING,
}


@dataclass
class SweepReport:
    timed_out: list[str] = field(default_factory=list)
    resolved: list[str] = field(default_factory=list)
    stalled: list[str] = field(default_factory=list)
    reps_over_cap: set[str] = field(default_factory=set)


@dataclass
class Supervisor:
    ledger: Ledger
    clock: Clock
    slack: SlackPort
    policy: Policy
    pmm_channel: str = "#relay_superagent-review"
    # run_id -> state as of the previous sweep. Same working state on two
    # consecutive sweeps = stalled. Deliberately in-memory: after a process
    # restart the first sweep re-marks and the second escalates, which is
    # exactly the crash case this exists for. No schema change needed.
    _seen: dict[str, RunState] = field(default_factory=dict)

    def sweep(self) -> SweepReport:
        report = SweepReport()
        now = self.clock.now()
        timeout = timedelta(hours=self.policy.gate_timeout_hours)

        waiting: dict[str, int] = {}
        for run in list(self.ledger.runs.values()):
            if run.state in STALL_STATES:
                if self._seen.get(run.run_id) is run.state:
                    self._fail_stalled(run)
                    report.stalled.append(run.run_id)
                    self._seen.pop(run.run_id, None)
                else:
                    self._seen[run.run_id] = run.state
                continue
            self._seen.pop(run.run_id, None)
            if run.state is RunState.AWAITING_GATE:
                waiting[run.rep_user_id] = waiting.get(run.rep_user_id, 0) + 1
                if run.surfaced_at and now - run.surfaced_at >= timeout:
                    self._time_out(run)
                    report.timed_out.append(run.run_id)
            elif run.state is RunState.ACTED and self.ledger.outcome_for(run.run_id):
                run.transition(RunState.RESOLVED)
                self.ledger.save(run)
                report.resolved.append(run.run_id)

        report.reps_over_cap = {rep for rep, n in waiting.items() if n >= QUEUE_CAP}
        return report

    def over_cap(self, rep_user_id: str) -> bool:
        n = sum(1 for r in self.ledger.runs.values()
                if r.state is RunState.AWAITING_GATE and r.rep_user_id == rep_user_id)
        return n >= QUEUE_CAP

    def _time_out(self, run: Run) -> None:
        self.ledger.effect(
            run.run_id, "pmm_escalation", "gate_timeout",
            lambda: self.slack.channel_post(
                self.pmm_channel,
                {"run": run.run_id, "reason": "gate_timeout", "claim": run.claim_text}))
        run.record_gate("system", GateAction.TIMEOUT, self.clock.now())
        run.transition(RunState.TIMED_OUT)
        self.ledger.trace(run.run_id, self.clock.now(), "escalation-agent",
                          "timed out", "gate unanswered 24h — escalated, not sent")
        self.ledger.save(run)

    def _fail_stalled(self, run: Run) -> None:
        """A crash-parked run: never silence, never auto-send. The PMM gets
        the run id and where it died; a human decides whether to re-drive."""
        self.ledger.effect(
            run.run_id, "pmm_escalation", f"stalled:{run.state}",
            lambda: self.slack.channel_post(
                self.pmm_channel,
                {"run": run.run_id, "reason": f"stalled_at_{run.state}",
                 "claim": run.claim_text}))
        run.transition(RunState.FAILED)
        self.ledger.trace(run.run_id, self.clock.now(), "escalation-agent",
                          "stalled", f"crash-parked at {run.state} — escalated")
        self.ledger.save(run)
