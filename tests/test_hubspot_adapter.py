"""The HubSpot rail on a stub transport: id parsing from record URLs,
stage normalization, the note-with-association act, and the company-domain
→ open-deal seam. No network, no token."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import pytest

from relay_superagent.adapters.hubspot import (
    NOTE_TO_DEAL, HubSpot, HubSpotError, _band, _deal_id,
)


@dataclass
class StubResp:
    payload: Any = None
    status_code: int = 200
    @property
    def text(self):
        return json.dumps(self.payload)
    def json(self):
        return self.payload


@dataclass
class StubHttp:
    routes: dict[str, Any] = field(default_factory=dict)   # "METHOD path" -> resp
    calls: list = field(default_factory=list)
    def request(self, method, path, **kw):
        self.calls.append({"method": method, "path": path, **kw})
        for key, resp in self.routes.items():
            m, p = key.split(" ", 1)
            if m == method and path.startswith(p):
                return resp if isinstance(resp, StubResp) else StubResp(resp)
        return StubResp({"m": "no route"}, 404)


def hs(routes) -> HubSpot:
    return HubSpot(token="pat-test", client=StubHttp(routes))


DEAL = {"id": "789", "properties": {
    "dealname": "Q3 Renewal", "dealstage": "closedwon", "amount": "84000"}}


# -- helpers -------------------------------------------------------------------

def test_deal_id_parses_raw_ids_and_record_urls():
    assert _deal_id("789") == "789"
    assert _deal_id("https://app.hubspot.com/deals/789") == "789"
    assert _deal_id("https://app.hubspot.com/contacts/123/record/0-3/456") == "456"
    assert _deal_id("no digits here") is None
    assert _deal_id(None) is None


def test_amount_bands():
    assert _band(None) == "unknown"
    assert _band(10_000) == "<25k"
    assert _band(84_000) == "50-100k"
    assert _band(300_000) == "250k+"


# -- reads ---------------------------------------------------------------------

def test_opportunity_normalizes_stage_and_bands_amount():
    opp = hs({"GET /crm/v3/objects/deals/789": DEAL}).opportunity(
        "https://app.hubspot.com/deals/789")
    assert opp["stage"] == "closed_won"          # closedwon -> policy vocabulary
    assert opp["amount_band"] == "50-100k"
    assert opp["name"] == "Q3 Renewal"


def test_missing_deal_is_none_not_an_error():
    assert hs({}).opportunity("123") is None


def test_401_names_the_fix():
    with pytest.raises(HubSpotError, match="scopes"):
        hs({"GET /crm/v3/objects/deals/1": StubResp({}, 401)}).opportunity("1")


# -- the act -------------------------------------------------------------------

def test_write_note_posts_body_and_deal_association_214():
    client_routes = {"POST /crm/v3/objects/notes": {"id": "note_9"}}
    h = hs(client_routes)
    ref = h.write_note("https://app.hubspot.com/deals/789", "the counter text")
    assert ref == "note_9"
    call = h.client.calls[0]
    body = call["json"]
    assert body["properties"]["hs_note_body"] == "the counter text"
    assert body["properties"]["hs_timestamp"]
    assoc = body["associations"][0]
    assert assoc["to"]["id"] == "789"
    assert assoc["types"][0]["associationTypeId"] == NOTE_TO_DEAL


def test_write_note_without_deal_id_fails_loud():
    with pytest.raises(HubSpotError, match="no deal id"):
        hs({}).write_note("garbage-ref", "text")


# -- account -> open deal ------------------------------------------------------

def test_open_deal_for_account_skips_closed_and_returns_open():
    h = hs({
        "POST /crm/v3/objects/companies/search": {"results": [{"id": "c1"}]},
        "GET /crm/v4/objects/companies/c1/associations/deals": {
            "results": [{"toObjectId": 111}, {"toObjectId": 222}]},
        "GET /crm/v3/objects/deals/111": {
            "id": "111", "properties": {"dealstage": "closedlost"}},
        "GET /crm/v3/objects/deals/222": {
            "id": "222", "properties": {"dealstage": "evaluation"}},
    })
    assert h.open_deal_for_account("client.com") == "222"


def test_open_deal_for_account_handles_no_company_and_non_domains():
    h = hs({"POST /crm/v3/objects/companies/search": {"results": []}})
    assert h.open_deal_for_account("client.com") is None
    assert h.open_deal_for_account("Meridian Systems") is None
    assert h.open_deal_for_account("") is None


def test_missing_token_fails_loud_with_the_fix():
    import relay_superagent.adapters.hubspot as mod
    orig = mod.get_secret
    mod.get_secret = lambda name: None
    try:
        with pytest.raises(HubSpotError, match="add-generic-password"):
            HubSpot()
    finally:
        mod.get_secret = orig
