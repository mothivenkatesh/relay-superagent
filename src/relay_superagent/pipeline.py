"""The Watcher pipeline (§6): trigger → safety → retrieve → draft → check →
gate → act, with the outcome arriving whenever the world settles.

The model appears in exactly four calls (confirm, extract, draft, judge), each
behind LlmPort. Everything else — and everything that guarantees anything — is
deterministic code in this file, `policy.py` and `ledger.py`. If the model dies
mid-run the run row survives in a legal state and the caller retries or the
supervisor escalates; a rep never sees a stack trace and never gets silence.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any

from relay_superagent.detect import match_competitors
from relay_superagent.domain.models import (
    Arm, Draft, EvidenceItem, GateAction, MemoryNote, Policy, Run, RunState,
    TriggerEvent, assign_arm, sha,
)
from relay_superagent.ledger import DuplicateRun, Ledger
from relay_superagent.policy import judge_passes, layer1, safety
from relay_superagent.ports.base import Clock, CrmPort, LlmPort, LlmUnavailable, SlackPort, UrlChecker

PMM_CHANNEL = "#relay_superagent-review"


@dataclass
class Deps:
    clock: Clock
    llm: LlmPort
    crm: CrmPort
    slack: SlackPort
    url_checker: UrlChecker
    ledger: Ledger
    policy: Policy
    evidence: list[EvidenceItem] = field(default_factory=list)
    enrolled_reps: set[str] = field(default_factory=set)
    pmm_channel: str = PMM_CHANNEL
    tokens_spent_today: int = 0


class Pipeline:
    def __init__(self, deps: Deps):
        self.d = deps

    def _trace(self, run: Run, agent: str, kind: str, detail: str = "") -> None:
        self.d.ledger.trace(run.run_id, self.d.clock.now(), agent, kind, detail)

    # -- ingest → gate --------------------------------------------------------
    def handle_event(self, ev: TriggerEvent) -> Run | None:
        """Returns the run, or None when no competitor matched (no row: this is
        pre-trigger noise, not a suppression)."""
        hits = match_competitors(ev.text, self.d.policy)
        if not hits:
            return None
        comp = hits[0]

        run = Run(
            run_id=str(uuid.uuid4()),
            tenant_id=ev.tenant_id,
            idempotency_key=sha(ev.tenant_id, f"{ev.source}:{ev.source_ref}",
                                self.d.policy.policy_version),
            trigger_source=ev.source,
            trigger_ref=ev.source_ref,
            occurred_at=ev.occurred_at,
            policy_version=self.d.policy.policy_version,
            arm=assign_arm(ev.account_id or "none", self.d.policy.holdout_pct),
            opportunity_id=ev.opportunity_id,
            account_id=ev.account_id,
            rep_user_id=ev.rep_user_id,
            competitor_id=comp.id,
        )
        try:
            self.d.ledger.insert(run)
        except DuplicateRun as dup:
            return dup.existing
        self._trace(run, "monitoring-agent", "signal",
                    f"{ev.source} · {comp.id} mentioned")

        # model confirms the deterministic hit is a real competitive mention
        try:
            mention = self.d.llm.confirm_mention(ev.text, comp.names)
        except LlmUnavailable:
            return self._escalate(run, "llm_unavailable_at_confirm")
        if not mention.get("is_competitive"):
            self._trace(run, "monitoring-agent", "dismissed", "not competitive")
            return self._suppress(run, "not_competitive")
        self._trace(run, "monitoring-agent", "confirmed",
                    mention.get("claim_text") or "")
        run.claim_text = mention.get("claim_text") or ""
        run.claim_hash = sha(run.claim_text.strip().lower())

        # holdout stops here: assigned before any work, counted, untreated (§6.4)
        if run.arm is Arm.HOLDOUT:
            return self._suppress(run, "holdout")

        # email triggers carry no deal; resolve account → open deal before the
        # safety gate so "no_opportunity" still means what it says
        if run.opportunity_id is None and run.account_id:
            run.opportunity_id = self.d.crm.open_deal_for_account(run.account_id)

        reason = safety(run, self.d.policy, self.d.ledger, self.d.crm,
                        self.d.enrolled_reps, self.d.tokens_spent_today)
        if reason:
            self._trace(run, "triage-agent", "suppressed", reason)
            return self._suppress(run, reason)
        self._trace(run, "triage-agent", "qualified", "all safety checks passed")

        run.transition(RunState.RUNNING)
        try:
            return self._draft_and_gate(run, ev)
        except LlmUnavailable:
            return self._escalate(run, "llm_unavailable")

    def _draft_and_gate(self, run: Run, ev: TriggerEvent) -> Run:
        claim = self.d.llm.extract_claim(ev.text, run.competitor_id)
        deal = self.d.crm.deal_context(run.opportunity_id)
        candidates = [e for e in self.d.evidence
                      if e.tenant_id == run.tenant_id
                      and e.competitor_id == run.competitor_id]
        memory = [m.body for m in self.d.ledger.memory_for(
            run.tenant_id, run.rep_user_id, "counter_style")]

        raw = self.d.llm.draft_counter(
            claim["claim_text"], deal,
            [{"evidence_id": e.evidence_id, "text": e.text, "source_url": e.source_url}
             for e in candidates],
            memory)
        draft = Draft(**raw)
        run.decision = raw
        run.retrieved_refs = {"deal": run.opportunity_id,
                              "evidence": [e.evidence_id for e in candidates]}
        run.transition(RunState.DRAFTED)
        self._trace(run, "drafting-agent", "drafted",
                    f"{len(draft.cited_evidence_ids)} citations")

        failures = layer1(
            draft, run, self.d.policy,
            known_evidence_ids={e.evidence_id for e in candidates},
            urls={e.evidence_id: e.source_url for e in candidates},
            url_checker=self.d.url_checker)
        if failures or draft.escalate:
            self._trace(run, "qa-agent", "blocked",
                        ",".join(failures) or "drafter escalated")
            return self._escalate(run, f"layer1:{','.join(failures) or 'drafter_escalate'}")

        verdict = self.d.llm.judge(claim["claim_text"], draft.counter_text,
                                   "narrative_map")
        if not judge_passes(verdict, self.d.policy.judge_threshold):
            self._trace(run, "qa-agent", "blocked", f"judge {verdict}")
            return self._escalate(run, f"judge:{verdict}")
        run.transition(RunState.CHECKED)
        self._trace(run, "qa-agent", "passed", "checks + judge clean")

        # the gate: Slack DM through the effect table, exactly once
        blocks = {
            "run_id": run.run_id,          # buttons resolve back to the run
            "claim": run.claim_text,
            "counter": draft.counter_text,
            "evidence": [e.source_url for e in candidates
                         if e.evidence_id in draft.cited_evidence_ids],
            "reasoning": f"{run.competitor_id} was named on {run.trigger_source} "
                         f"for an open deal; this argument has been used against "
                         f"this claim before.",   # visible reasoning is required (§6.7)
            "actions": ["send", "edit", "dismiss"],
        }
        self.d.ledger.effect(run.run_id, "slack_post", run.rep_user_id,
                             lambda: self.d.slack.dm(run.rep_user_id, blocks))
        run.surfaced_at = self.d.clock.now()
        run.transition(RunState.AWAITING_GATE)
        self._trace(run, "gate", "surfaced", f"card sent to {run.rep_user_id}")
        self.d.ledger.save(run)
        return run

    # -- gate resolutions ------------------------------------------------------
    def approve(self, run: Run, actor: str) -> Run:
        run.record_gate(actor, GateAction.APPROVE, self.d.clock.now())
        self._trace(run, "gate", "approved", f"by {actor}")
        run.transition(RunState.APPROVED)
        return self._act(run, run.decision["counter_text"])

    def edit(self, run: Run, actor: str, edited_text: str) -> Run:
        run.record_gate(actor, GateAction.EDIT, self.d.clock.now())
        original = run.decision["counter_text"]
        diff = self.d.llm.semantic_diff(original, edited_text)
        run.gate_diff = diff
        run.gate_is_material = bool(diff.get("is_material"))
        self._trace(run, "gate", "edited",
                    f"by {actor} · {'material' if run.gate_is_material else 'style only'}")
        self.d.ledger.append_memory(MemoryNote(
            memory_id=str(uuid.uuid4()), tenant_id=run.tenant_id,
            subject_type="rep", subject_id=run.rep_user_id,
            concern="counter_style", body=diff, source_run=run.run_id))
        run.transition(RunState.EDITED)
        return self._act(run, edited_text)

    def reject(self, run: Run, actor: str) -> Run:
        run.record_gate(actor, GateAction.REJECT, self.d.clock.now())
        self._trace(run, "gate", "dismissed", f"by {actor}")
        run.transition(RunState.REJECTED)
        self.d.ledger.save(run)
        return run

    def _act(self, run: Run, text: str) -> Run:
        run.transition(RunState.ACTING)
        ref = self.d.ledger.effect(
            run.run_id, "crm_note", run.opportunity_id,
            lambda: self.d.crm.write_note(run.opportunity_id, text))
        run.act_ref = ref
        run.acted_at = self.d.clock.now()
        self._trace(run, "crm-agent", "filed", f"note {ref}")
        run.transition(RunState.ACTED)
        self.d.ledger.save(run)
        return run

    # -- outcomes --------------------------------------------------------------
    def record_close(self, run: Run, won: bool, amount: int | None = None) -> Run:
        self.d.ledger.append_outcome(
            run.tenant_id, run.run_id, "opportunity_closed",
            {"won": won, "amount": amount, "competitor_id": run.competitor_id},
            self.d.clock.now(), source="crm")
        self._trace(run, "reporting-agent", "outcome",
                    f"{'won' if won else 'lost'}" + (f" ${amount:,}" if amount else ""))
        if run.state is RunState.ACTED:
            run.transition(RunState.RESOLVED)
            self.d.ledger.save(run)
        return run

    # -- terminal helpers ------------------------------------------------------
    def _suppress(self, run: Run, reason: str) -> Run:
        run.suppressed_reason = reason
        run.transition(RunState.SUPPRESSED)
        self.d.ledger.save(run)
        return run

    def _escalate(self, run: Run, reason: str) -> Run:
        """Anything the rep must not see routes to the PMM channel — a failed
        check, a low judge score, a dead model. Never auto-send, never silence."""
        self._trace(run, "escalation-agent", "escalated", reason)
        self.d.ledger.effect(
            run.run_id, "pmm_escalation", reason,
            lambda: self.d.slack.channel_post(
                self.d.pmm_channel,
                {"run": run.run_id, "reason": reason, "claim": run.claim_text}))
        if run.state is RunState.AWAITING_GATE:
            run.record_gate("system", GateAction.TIMEOUT, self.d.clock.now())
            run.transition(RunState.TIMED_OUT)
        else:
            run.transition(RunState.FAILED)
        self.d.ledger.save(run)
        return run
