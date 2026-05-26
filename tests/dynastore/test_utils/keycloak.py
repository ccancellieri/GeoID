"""
Keycloak test utilities — obtain real tokens from the Keycloak service.

Requires Keycloak to be running (via docker-compose) with the ``geoid``
realm imported.  All helpers use httpx to call the token endpoint.

Environment variables (read from docker/.env via compose):
    KEYCLOAK_ISSUER_URL  — e.g. http://keycloak:8080/realms/geoid
    KEYCLOAK_CLIENT_ID   — e.g. geoid-api
    KEYCLOAK_CLIENT_SECRET — secret for the confidential client
"""

from __future__ import annotations

import os
import logging

import httpx

logger = logging.getLogger(__name__)

# When running outside Docker (local pytest), Keycloak is on localhost:8180
_DEFAULT_ISSUER = os.getenv(
    "KEYCLOAK_ISSUER_URL", "http://localhost:8180/realms/geoid"
)
_DEFAULT_CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID", "geoid-api")
_DEFAULT_CLIENT_SECRET = os.getenv("KEYCLOAK_CLIENT_SECRET", "geoid-api-secret")


def _token_url(issuer_url: str | None = None) -> str:
    base = (issuer_url or _DEFAULT_ISSUER).rstrip("/")
    return f"{base}/protocol/openid-connect/token"


async def get_user_token(
    username: str,
    password: str,
    *,
    issuer_url: str | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
) -> str:
    """
    Obtain an access token via the Resource Owner Password Credentials
    (direct access) grant.

    Requires ``directAccessGrantsEnabled=true`` on the client.
    """
    url = _token_url(issuer_url)
    data = {
        "grant_type": "password",
        "client_id": client_id or _DEFAULT_CLIENT_ID,
        "client_secret": client_secret or _DEFAULT_CLIENT_SECRET,
        "username": username,
        "password": password,
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, data=data)
        resp.raise_for_status()
        return resp.json()["access_token"]


async def get_service_account_token(
    client_id: str | None = None,
    client_secret: str | None = None,
    *,
    issuer_url: str | None = None,
) -> str:
    """
    Obtain an access token via the Client Credentials grant
    (service account flow).

    The resulting JWT will have ``client_id`` claim and no ``email``,
    identifying it as a service account.
    """
    url = _token_url(issuer_url)
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id or _DEFAULT_CLIENT_ID,
        "client_secret": client_secret or _DEFAULT_CLIENT_SECRET,
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, data=data)
        resp.raise_for_status()
        return resp.json()["access_token"]


def is_keycloak_available(issuer_url: str | None = None) -> bool:
    """
    Decide whether Keycloak integration tests should run.

    These tests need both a reachable Keycloak sidecar AND a test
    application wired to trust the same issuer/JWKS — the latter is
    not guaranteed by the default in-process ``app_lifespan`` fixture,
    so a reachable sidecar is necessary but not sufficient.

    Gating rules (fail-closed, fast, no flakiness):
      1. Opt-in env: ``KEYCLOAK_INTEGRATION=1`` must be set. Without it,
         tests skip even if Keycloak is up — avoids spurious 401s when
         the test app's OIDC config does not match the local sidecar's
         issuer URL.
      2. The realm's ``.well-known/openid-configuration`` endpoint must
         return 200 within a 2 s timeout (sidecar is up and realm is
         imported).
      3. A ``client_credentials`` token grant against the test client
         must succeed (client exists, secret matches).

    On any failure (env unset, network error, 404 realm, bad secret,
    timeout) returns False so dependent integration tests SKIP with a
    clear reason rather than fail with an opaque 401.

    Use as a pytest skip marker::

        pytestmark = pytest.mark.skipif(
            not is_keycloak_available(),
            reason="Keycloak sidecar not reachable / usable",
        )
    """
    if os.getenv("KEYCLOAK_INTEGRATION", "").strip() not in {"1", "true", "True"}:
        return False

    url = (issuer_url or _DEFAULT_ISSUER).rstrip("/")
    try:
        resp = httpx.get(
            f"{url}/.well-known/openid-configuration", timeout=2.0
        )
        if resp.status_code != 200:
            return False
    except Exception:
        return False

    # Reachable — but also confirm the realm/client is wired for tokens.
    try:
        token_resp = httpx.post(
            f"{url}/protocol/openid-connect/token",
            data={
                "grant_type": "client_credentials",
                "client_id": _DEFAULT_CLIENT_ID,
                "client_secret": _DEFAULT_CLIENT_SECRET,
            },
            timeout=2.0,
        )
        return token_resp.status_code == 200
    except Exception:
        return False
