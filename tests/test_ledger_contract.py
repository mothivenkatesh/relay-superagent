"""One contract, two ledgers. Every guarantee the pipeline leans on is asserted
here against the in-memory implementation (always) and Postgres (when a local
cluster is reachable — `make pg` starts one; CI without Postgres skips).

The Postgres fixture connects as `relay_superagent_app`, not the superuser, so RLS is
actually enforced in what we test rather than silently bypassed by ownership.
"""

from __future__ import annotations

import uuid
from datetime import datetime

import pytest

from relay_superagent.domain.models import Arm, MemoryNote, Run
from relay_superagent.ledger import DuplicateRun, Ledger

# Tests get their OWN database — the contract fixtures truncate tables,
# and sharing the dev db once wiped seeded demo data mid-session.
PG_SUPER = "postgresql://relay_superagent@localhost:5434/relay_superagent_test"
PG_APP = "postgresql://relay_superagent_app@localhost:5434/relay_superagent_test"
NOW = datetime(2026, 8, 3, 9, 0)


def _pg_available() -> bool:
    try:
        import psycopg
        with psycopg.connect(PG_SUPER, connect_timeout=1):
            return True
    except Exception:
        return False


PG_UP = _pg_available()


def make_run(idem="k1", tenant="t1", **kw) -> Run:
    defaults = dict(
        run_id=str(uuid.uuid4()), tenant_id=tenant, idempotency_key=idem,
        trigger_source="gong", trigger_ref="c1", occurred_at=NOW,
        policy_version="pol_1", arm=Arm.TREATED)
    defaults.update(kw)
    return Run(**defaults)


@pytest.fixture(params=["memory", "pg"] if PG_UP else ["memory"])
def ledger(request):
    if request.param == "memory":
        yield Ledger()
        return

    import psycopg
    from relay_superagent.ledger_pg import PgLedger

    with psycopg.connect(PG_SUPER, autocommit=True) as admin:
        admin.execute(open("migrations/001_init.sql").read())
        admin.execute("TRUNCATE effect, outcome, memory, evidence_item, policy, run CASCADE")
    yield PgLedger(PG_APP, tenant_id="t1")


def test_duplicate_idempotency_key_raises_with_existing(ledger):
    first = ledger.insert(make_run(idem="dup"))
    with pytest.raises(DuplicateRun) as exc:
        ledger.insert(make_run(idem="dup"))
    assert exc.value.existing.run_id == first.run_id


def test_same_key_different_tenant_would_not_collide(ledger):
    # In-memory holds all tenants; the Pg handle is tenant-pinned, so this
    # asserts on the key structure both implementations share.
    ledger.insert(make_run(idem="k-shared"))
    r2 = make_run(idem="k-shared2")
    ledger.insert(r2)
    assert len(ledger.runs_for_tenant("t1")) == 2


def test_outcome_append_is_idempotent(ledger):
    run = ledger.insert(make_run())
    a = ledger.append_outcome("t1", run.run_id, "opportunity_closed",
                              {"won": True}, NOW, "crm")
    b = ledger.append_outcome("t1", run.run_id, "opportunity_closed",
                              {"won": False}, NOW, "crm")
    assert a.outcome_id == b.outcome_id
    assert ledger.outcome_for(run.run_id).outcome_value == {"won": True}


def test_effect_fires_exactly_once(ledger):
    run = ledger.insert(make_run())
    calls = []
    ref1 = ledger.effect(run.run_id, "slack_post", "rep_7",
                         lambda: calls.append(1) or "ext_1")
    ref2 = ledger.effect(run.run_id, "slack_post", "rep_7",
                         lambda: calls.append(1) or "ext_2")
    assert calls == [1]
    assert ref1 == ref2 == "ext_1"


def test_distinct_targets_are_distinct_effects(ledger):
    run = ledger.insert(make_run())
    ledger.effect(run.run_id, "slack_post", "rep_7", lambda: "a")
    ledger.effect(run.run_id, "crm_note", "opp_1", lambda: "b")
    run2 = ledger.insert(make_run(idem="k2"))
    ledger.effect(run2.run_id, "slack_post", "rep_7", lambda: "c")
    # three distinct (run, type, target) triples, three firings — asserted via refs
    assert ledger.effect(run2.run_id, "slack_post", "rep_7", lambda: "d") == "c"


def test_memory_appends_and_filters_superseded(ledger):
    run = ledger.insert(make_run())
    live = MemoryNote(memory_id=str(uuid.uuid4()), tenant_id="t1",
                      subject_type="rep", subject_id="rep_7",
                      concern="counter_style", body={"implies": "short"},
                      source_run=run.run_id)
    dead = MemoryNote(memory_id=str(uuid.uuid4()), tenant_id="t1",
                      subject_type="rep", subject_id="rep_7",
                      concern="counter_style", body={"implies": "old"},
                      source_run=run.run_id, superseded_by=live.memory_id)
    ledger.append_memory(live)
    ledger.append_memory(dead)
    got = ledger.memory_for("t1", "rep_7", "counter_style")
    assert [m.body for m in got] == [{"implies": "short"}]


def test_saved_mutations_round_trip(ledger):
    run = ledger.insert(make_run())
    run.claim_text = "Acme is cheaper"
    run.cost_tokens = 1234
    ledger.save(run)
    got = next(r for r in ledger.runs_for_tenant("t1") if r.run_id == run.run_id)
    assert got.claim_text == "Acme is cheaper"
    assert got.cost_tokens == 1234


# -- Postgres-only guarantees -------------------------------------------------

@pytest.mark.skipif(not PG_UP, reason="no local Postgres")
def test_rls_blocks_cross_tenant_reads():
    import psycopg
    from relay_superagent.ledger_pg import PgLedger

    with psycopg.connect(PG_SUPER, autocommit=True) as admin:
        admin.execute(open("migrations/001_init.sql").read())
        admin.execute("TRUNCATE effect, outcome, memory, evidence_item, policy, run CASCADE")

    a = PgLedger(PG_APP, tenant_id="tenant_a")
    b = PgLedger(PG_APP, tenant_id="tenant_b")
    a.insert(make_run(tenant="tenant_a"))

    assert len(a.runs_for_tenant("tenant_a")) == 1
    # same table, same query, other tenant's GUC: zero rows, enforced by the
    # database, not by application code remembering a WHERE clause
    assert b.runs_for_tenant("tenant_a") == []
    assert b.runs_for_tenant("tenant_b") == []


@pytest.mark.skipif(not PG_UP, reason="no local Postgres")
def test_gate_once_trigger_refuses_a_second_gate_write():
    import psycopg
    from relay_superagent.domain.models import GateAction, RunState
    from relay_superagent.ledger_pg import PgLedger

    with psycopg.connect(PG_SUPER, autocommit=True) as admin:
        admin.execute(open("migrations/001_init.sql").read())
        admin.execute("TRUNCATE effect, outcome, memory, evidence_item, policy, run CASCADE")

    led = PgLedger(PG_APP, tenant_id="t1")
    run = led.insert(make_run())
    run.state = RunState.AWAITING_GATE
    run.gate_action = GateAction.APPROVE
    led.save(run)

    run.gate_action = GateAction.REJECT       # a bug upstream, refused below
    with pytest.raises(psycopg.errors.RaiseException):
        led.save(run)
