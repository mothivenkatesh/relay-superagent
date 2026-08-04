"""The HubSpot rail — inherited reference material from the GTM fork, kept
compiling against the Dispute Defender port shapes but not wired into a real
dispute flow (a real deployment would file responses with the bank/payment
processor, not a CRM deal). See README: reference implementation only.

CrmPort over the v3 CRM API with a private-app token from the keychain:

- `opportunity` / `deal_context` read an order record. Runs carry the order
  ref as whatever the trigger provided — a raw id or a record URL (Fathom's
  crm_matches sends URLs) — so ids are parsed from either.
- `write_note` is the act: notes v3 with the note→deal association
  (HUBSPOT_DEFINED type 214, verified against their associations docs).
  Returns the note id — the effect table's external ref.
- `open_deal_for_account` implements the seam some triggers need:
  merchant domain → company search → associated deals → first open one.

Stage normalization: HubSpot's default-pipeline internal values
(closedwon/closedlost) map onto the policy layer's closed_won/closed_lost;
custom-pipeline stages pass through and count as open, which fails safe —
a mistaken response filed on a live order is gated anyway; a suppressed
response on a closed order costs nothing.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

import httpx

from relay_superagent.secrets import get_secret

API = "https://api.hubapi.com"
NOTE_TO_DEAL = 214

_STAGE_MAP = {"closedwon": "closed_won", "closedlost": "closed_lost"}


class HubSpotError(Exception):
    pass


def _deal_id(ref: str | None) -> str | None:
    """'12345' or 'https://app.hubspot.com/.../record/0-3/12345' -> '12345'."""
    if not ref:
        return None
    m = re.search(r"(\d+)\s*$", ref)
    return m.group(1) if m else None


def _band(amount: float | None) -> str:
    if amount is None:
        return "unknown"
    for cap, label in ((25_000, "<25k"), (50_000, "25-50k"),
                       (100_000, "50-100k"), (250_000, "100-250k")):
        if amount < cap:
            return label
    return "250k+"


class HubSpot:
    def __init__(self, token: str | None = None,
                 client: httpx.Client | None = None):
        self.token = token or get_secret("hubspot-token")
        if not self.token:
            raise HubSpotError(
                "no token — security add-generic-password -U -s relay_superagent "
                "-a hubspot-token -w 'pat-…' (private app: crm.objects.deals "
                "read, crm.objects.companies read, notes write)")
        self.client = client or httpx.Client(
            base_url=API, timeout=15,
            headers={"Authorization": f"Bearer {self.token}"})

    def _req(self, method: str, path: str, **kw) -> dict[str, Any] | None:
        try:
            resp = self.client.request(method, path, **kw)
        except httpx.HTTPError as e:
            raise HubSpotError(f"transport: {e}") from e
        if resp.status_code == 404:
            return None
        if resp.status_code == 401:
            raise HubSpotError("401 — token invalid or missing scopes "
                               "(needs deals read, companies read, notes write)")
        if resp.status_code >= 400:
            raise HubSpotError(f"{resp.status_code}: {resp.text[:200]}")
        return resp.json()

    # -- reads ----------------------------------------------------------------

    def opportunity(self, order_id: str) -> dict[str, Any] | None:
        did = _deal_id(order_id)
        if not did:
            return None
        data = self._req("GET", f"/crm/v3/objects/deals/{did}",
                         params={"properties": "dealname,dealstage,amount"})
        if data is None:
            return None
        props = data.get("properties") or {}
        stage_raw = (props.get("dealstage") or "").lower()
        amount = float(props["amount"]) if props.get("amount") else None
        return {"id": data.get("id"), "name": props.get("dealname"),
                "stage": _STAGE_MAP.get(stage_raw, stage_raw or "unknown"),
                "amount": amount, "amount_band": _band(amount)}

    def deal_context(self, order_id: str) -> dict[str, Any]:
        opp = self.opportunity(order_id) or {}
        return {"stage": opp.get("stage"),
                "amount_band": opp.get("amount_band", "unknown"),
                "prior_dispute_history": [], "prior_losses": []}

    # -- the act ---------------------------------------------------------------

    def write_note(self, order_id: str, text: str) -> str:
        did = _deal_id(order_id)
        if not did:
            raise HubSpotError(f"no deal id in ref: {order_id!r}")
        data = self._req("POST", "/crm/v3/objects/notes", json={
            "properties": {
                "hs_note_body": text,
                "hs_timestamp": datetime.now(timezone.utc).isoformat(),
            },
            "associations": [{
                "to": {"id": did},
                "types": [{"associationCategory": "HUBSPOT_DEFINED",
                           "associationTypeId": NOTE_TO_DEAL}],
            }],
        })
        if not data or not data.get("id"):
            raise HubSpotError("note created but no id returned")
        return data["id"]

    # -- merchant -> open order (the Gmail seam) -------------------------------

    def open_deal_for_account(self, merchant_id: str) -> str | None:
        domain = (merchant_id or "").strip().lower()
        if not domain or "." not in domain:
            return None
        found = self._req("POST", "/crm/v3/objects/companies/search", json={
            "filterGroups": [{"filters": [
                {"propertyName": "domain", "operator": "EQ", "value": domain}]}],
            "properties": ["name", "domain"], "limit": 1,
        }) or {}
        companies = found.get("results") or []
        if not companies:
            return None
        assoc = self._req(
            "GET", f"/crm/v4/objects/companies/{companies[0]['id']}/associations/deals",
            params={"limit": 20}) or {}
        for row in assoc.get("results") or []:
            deal_id = row.get("toObjectId")
            opp = self.opportunity(str(deal_id)) if deal_id else None
            if opp and opp["stage"] not in ("closed_won", "closed_lost"):
                return str(deal_id)
        return None
