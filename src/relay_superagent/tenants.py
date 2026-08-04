"""Multi-tenancy as data, not architecture.

The ledger has been tenant-scoped since day one (tenant_id on every row, RLS
in Postgres); what was missing was the other half: per-tenant policy and
identity. A TenantContext is exactly that — the policy, the rep directory,
and the auth linkage — and the registry maps ids to contexts. Pipelines are
cheap (a dataclass over shared ports), so the serving layer builds one per
tenant against the same ledger and the same adapters.

A fresh signup gets `default_policy`: empty competitor list, so nothing
fires until the tenant configures competitors. Onboarding is policy edits,
not deploys.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from relay_superagent.domain.models import Policy


def default_policy(tenant_id: str) -> Policy:
    return Policy(
        policy_version="pol_default_1",
        tenant_id=tenant_id,
        competitors=[],
        banned_terms=["best", "leading", "number one"],
    )


@dataclass
class TenantContext:
    tenant_id: str
    name: str
    policy: Policy
    rep_directory: dict[str, str] = field(default_factory=dict)
    enrolled_reps: set[str] = field(default_factory=set)
    workos_org_id: str | None = None
    inbox_email: str | None = None       # the Gmail rail's connected inbox


class UnknownTenant(Exception):
    pass


class TenantRegistry:
    def __init__(self) -> None:
        self._by_id: dict[str, TenantContext] = {}

    def add(self, ctx: TenantContext) -> TenantContext:
        self._by_id[ctx.tenant_id] = ctx
        return ctx

    def get(self, tenant_id: str) -> TenantContext:
        try:
            return self._by_id[tenant_id]
        except KeyError:
            raise UnknownTenant(tenant_id) from None

    def has(self, tenant_id: str) -> bool:
        return tenant_id in self._by_id

    def by_workos_org(self, org_id: str) -> TenantContext | None:
        return next((c for c in self._by_id.values()
                     if c.workos_org_id == org_id), None)

    def all(self) -> list[TenantContext]:
        return list(self._by_id.values())
