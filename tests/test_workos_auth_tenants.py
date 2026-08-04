"""The WorkOS rail on a stub transport, session cookies, and tenant
isolation: the three pieces the signup flow is made of, none touching the
network."""

from __future__ import annotations

import pytest

from relay_superagent.adapters.workos import WorkOs, WorkOsError
from relay_superagent.auth import sign_session, verify_session
from relay_superagent.domain.models import RunState, TriggerEvent
from relay_superagent.pipeline import Deps, Pipeline
from relay_superagent.tenants import TenantContext, TenantRegistry, UnknownTenant, default_policy

from .conftest import MON_9AM, make_policy

# -- workos adapter -----------------------------------------------------------

class StubResp:
    def __init__(self, payload, status=200):
        self.status_code, self._p = status, payload
    def json(self):
        return self._p


class StubHttp:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), []
    def post(self, path, json=None, headers=None):
        self.calls.append({"path": path, "json": json, "headers": headers or {}})
        return self.responses.pop(0)


def test_signup_flow_hits_the_three_endpoints_with_bearer_auth():
    http = StubHttp([StubResp({"id": "org_1"}),
                     StubResp({"id": "user_1", "email": "a@b.com"}),
                     StubResp({"id": "om_1"})])
    wos = WorkOs(api_key="sk_test", client_id="client_test", client=http)
    org = wos.create_organization("Meridian")
    user = wos.create_user("a@b.com", "hunter2hunter2")
    wos.add_membership(user["id"], org)
    paths = [c["path"] for c in http.calls]
    assert paths == ["/organizations", "/user_management/users",
                     "/user_management/organization_memberships"]
    assert all(c["headers"]["Authorization"] == "Bearer sk_test"
               for c in http.calls)
    assert http.calls[1]["json"] == {"email": "a@b.com",
                                     "password": "hunter2hunter2"}


def test_authenticate_uses_password_grant_with_client_credentials():
    http = StubHttp([StubResp({"user": {"email": "a@b.com"},
                               "organization_id": "org_1"})])
    wos = WorkOs(api_key="sk_test", client_id="client_test", client=http)
    resp = wos.authenticate("a@b.com", "hunter2hunter2")
    call = http.calls[0]
    assert call["path"] == "/user_management/authenticate"
    assert call["headers"] == {}                    # no Bearer on this one
    assert call["json"]["grant_type"] == "password"
    assert call["json"]["client_id"] == "client_test"
    assert call["json"]["client_secret"] == "sk_test"
    assert resp["organization_id"] == "org_1"


def test_verify_email_code_uses_the_otp_grant():
    http = StubHttp([StubResp({"user": {"email": "a@b.com", "email_verified": True},
                               "organization_id": "org_1"})])
    wos = WorkOs(api_key="sk_test", client_id="client_test", client=http)
    resp = wos.verify_email_code("123456", "pat_token")
    call = http.calls[0]
    assert call["json"]["grant_type"] == \
        "urn:workos:oauth:grant-type:email-verification:code"
    assert call["json"]["code"] == "123456"
    assert call["json"]["pending_authentication_token"] == "pat_token"
    assert resp["organization_id"] == "org_1"


def test_unverified_email_error_carries_the_pending_token():
    http = StubHttp([StubResp({
        "message": "Email ownership must be verified before authentication.",
        "code": "email_verification_required",
        "email": "a@b.com",
        "pending_authentication_token": "pat_xyz"}, status=422)])
    wos = WorkOs(api_key="sk_test", client_id="client_test", client=http)
    with pytest.raises(WorkOsError) as exc:
        wos.authenticate("a@b.com", "hunter2hunter2")
    assert exc.value.code == "email_verification_required"
    assert exc.value.data["pending_authentication_token"] == "pat_xyz"


def test_workos_error_carries_their_code():
    http = StubHttp([StubResp({"message": "Invalid credentials.",
                               "code": "invalid_credentials"}, status=401)])
    wos = WorkOs(api_key="sk_test", client_id="client_test", client=http)
    with pytest.raises(WorkOsError, match="Invalid credentials") as exc:
        wos.authenticate("a@b.com", "wrong")
    assert exc.value.code == "invalid_credentials"


def test_missing_keys_fail_loud_with_the_fix():
    import relay_superagent.adapters.workos as mod
    orig = mod.get_secret
    mod.get_secret = lambda name: None
    try:
        with pytest.raises(WorkOsError, match="add-generic-password"):
            WorkOs()
    finally:
        mod.get_secret = orig


# -- sessions -----------------------------------------------------------------

def test_session_round_trip_tamper_and_expiry():
    tok = sign_session({"tenant_id": "t1", "email": "a@b.com"}, "s3cret",
                       now=lambda: 1000)
    assert verify_session(tok, "s3cret", now=lambda: 2000)["tenant_id"] == "t1"
    assert verify_session(tok, "wrong", now=lambda: 2000) is None
    assert verify_session(tok + "x", "s3cret", now=lambda: 2000) is None
    body, sig = tok.split(".", 1)
    assert verify_session(body[:-4] + "AAAA." + sig, "s3cret",
                          now=lambda: 2000) is None
    week = 7 * 24 * 3600
    assert verify_session(tok, "s3cret", now=lambda: 1000 + week + 1) is None
    assert verify_session("garbage", "s3cret") is None


# -- tenant isolation ---------------------------------------------------------

def test_registry_lookup_and_unknown_tenant():
    reg = TenantRegistry()
    ctx = reg.add(TenantContext(tenant_id="org_1", name="Meridian",
                                policy=default_policy("org_1"),
                                workos_org_id="org_1"))
    assert reg.get("org_1") is ctx
    assert reg.by_workos_org("org_1") is ctx
    assert reg.by_workos_org("org_none") is None
    with pytest.raises(UnknownTenant):
        reg.get("org_ghost")


def test_two_tenants_share_a_ledger_but_not_policy_or_rows(world):
    """t2 has no competitors configured, so the same text that gates for t1
    produces nothing for t2 — and t1's rows never contain t2's id."""
    d = world.d
    t2 = Pipeline(Deps(clock=d.clock, llm=d.llm, crm=d.crm, slack=d.slack,
                       url_checker=d.url_checker, ledger=d.ledger,
                       policy=default_policy("t2"), evidence=[],
                       enrolled_reps=set()))
    text = "We're also looking at Acme, honestly they are cheaper."
    r1 = world.handle_event(TriggerEvent(
        tenant_id="t1", source="gmail", source_ref="m_1", occurred_at=MON_9AM,
        opportunity_id="opp_1", account_id="a1", rep_user_id="rep_7", text=text))
    assert r1.state is RunState.AWAITING_GATE
    r2 = t2.handle_event(TriggerEvent(
        tenant_id="t2", source="gmail", source_ref="m_1", occurred_at=MON_9AM,
        opportunity_id=None, account_id="a1", rep_user_id="x", text=text))
    assert r2 is None                       # empty competitor list: no trigger
    assert {r.tenant_id for r in d.ledger.runs.values()} == {"t1"}


def test_default_policy_is_scoped_to_its_tenant():
    p = default_policy("org_9")
    assert p.tenant_id == "org_9"
    assert p.competitors == []
    q = make_policy()
    assert q.competitors                    # the demo tenant still has some
