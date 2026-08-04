"""The WorkOS rail — signup and login with email & password.

Ground truth from the WorkOS API reference (2026-07-31):
- POST /user_management/users            {email, password}      Bearer sk_…
- POST /organizations                    {name}                 Bearer sk_…
- POST /user_management/organization_memberships {user_id, organization_id}
- POST /user_management/authenticate     {client_id, client_secret,
                                          grant_type: "password",
                                          email, password}      (no Bearer)

The client_secret for the password grant IS the sk_ API key. Both secrets
come from the keychain at point of use (workos-api-key, workos-client-id);
passwords pass straight through to WorkOS over TLS and are never stored or
logged here. Password policy, hashing, breach checks, verification emails —
all WorkOS's job; this adapter only maps requests and failures.
"""

from __future__ import annotations

from typing import Any

import httpx

from relay_superagent.secrets import get_secret

API = "https://api.workos.com"


class WorkOsError(Exception):
    """WorkOS said no. `code` carries their machine-readable error when
    present (e.g. email_verification_required, invalid_credentials);
    `data` carries the full body — the verification flow needs
    pending_authentication_token from it."""

    def __init__(self, message: str, code: str | None = None,
                 data: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.data = data or {}


class WorkOs:
    def __init__(self, api_key: str | None = None, client_id: str | None = None,
                 client: httpx.Client | None = None):
        self.api_key = api_key or get_secret("workos-api-key")
        self.client_id = client_id or get_secret("workos-client-id")
        if not self.api_key or not self.client_id:
            raise WorkOsError(
                "missing keys — security add-generic-password -U -s relay_superagent "
                "-a workos-api-key -w 'sk_…' (and -a workos-client-id -w 'client_…')")
        self.client = client or httpx.Client(base_url=API, timeout=15)

    def _post(self, path: str, payload: dict[str, Any],
              authed: bool = True) -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {self.api_key}"} if authed else {}
        try:
            resp = self.client.post(path, json=payload, headers=headers)
            data = resp.json()
        except (httpx.HTTPError, ValueError) as e:
            raise WorkOsError(f"transport: {e}") from e
        if resp.status_code >= 400:
            raise WorkOsError(
                data.get("message") or data.get("error_description")
                or f"http {resp.status_code}",
                code=data.get("code") or data.get("error"),
                data=data)
        return data

    # -- signup ---------------------------------------------------------------

    def create_organization(self, name: str) -> str:
        return self._post("/organizations", {"name": name})["id"]

    def create_user(self, email: str, password: str) -> dict[str, Any]:
        return self._post("/user_management/users",
                          {"email": email, "password": password})

    def add_membership(self, user_id: str, organization_id: str) -> str:
        return self._post("/user_management/organization_memberships",
                          {"user_id": user_id,
                           "organization_id": organization_id})["id"]

    # -- login ----------------------------------------------------------------

    def authenticate(self, email: str, password: str) -> dict[str, Any]:
        """Returns WorkOS's response: user, organization_id (when the user
        belongs to exactly one org), tokens. Raises WorkOsError with their
        code on bad credentials."""
        return self._post("/user_management/authenticate", {
            "client_id": self.client_id,
            "client_secret": self.api_key,
            "grant_type": "password",
            "email": email,
            "password": password,
        }, authed=False)

    def verify_email_code(self, code: str,
                          pending_token: str) -> dict[str, Any]:
        """Completes login when password auth raised
        email_verification_required: the one-time code from the user's
        inbox + the pending token from that error."""
        return self._post("/user_management/authenticate", {
            "client_id": self.client_id,
            "client_secret": self.api_key,
            "grant_type": "urn:workos:oauth:grant-type:email-verification:code",
            "code": code,
            "pending_authentication_token": pending_token,
        }, authed=False)
