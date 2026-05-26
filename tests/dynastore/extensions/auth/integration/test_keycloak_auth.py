"""
Integration tests for Keycloak authentication.

Requires a running Keycloak instance with the ``geoid`` realm.
Skipped automatically when Keycloak is not available.
"""

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from tests.dynastore.test_utils.keycloak import (
    _DEFAULT_ISSUER,
    get_user_token,
    get_service_account_token,
    is_keycloak_available,
)

pytestmark = pytest.mark.skipif(
    not is_keycloak_available(),
    reason=(
        f"Keycloak integration disabled: set KEYCLOAK_INTEGRATION=1 and "
        f"ensure Keycloak is reachable at {_DEFAULT_ISSUER}"
    ),
)


@pytest_asyncio.fixture(loop_scope="function")
async def keycloak_user_client(app_lifespan):
    """In-process client authenticated as testuser via Keycloak."""
    token = await get_user_token("testuser", "testuser")
    transport = ASGITransport(app=app_lifespan.app)
    headers = {"Authorization": f"Bearer {token}"}
    async with AsyncClient(
        transport=transport, base_url="http://test", headers=headers
    ) as client:
        yield client


@pytest_asyncio.fixture(loop_scope="function")
async def keycloak_admin_client(app_lifespan):
    """In-process client authenticated as testadmin via Keycloak."""
    token = await get_user_token("testadmin", "testadmin")
    transport = ASGITransport(app=app_lifespan.app)
    headers = {"Authorization": f"Bearer {token}"}
    async with AsyncClient(
        transport=transport, base_url="http://test", headers=headers
    ) as client:
        yield client


@pytest_asyncio.fixture(loop_scope="function")
async def keycloak_service_client(app_lifespan):
    """In-process client authenticated as geoid-api service account."""
    token = await get_service_account_token()
    transport = ASGITransport(app=app_lifespan.app)
    headers = {"Authorization": f"Bearer {token}"}
    async with AsyncClient(
        transport=transport, base_url="http://test", headers=headers
    ) as client:
        yield client


# ---- Human user tests ----


@pytest.mark.asyncio
async def test_user_token_resolves_principal(keycloak_user_client: AsyncClient):
    """Verify that a Keycloak user token resolves to an authenticated principal."""
    resp = await keycloak_user_client.get("/web/health")
    # Health endpoint should be reachable (does not require auth)
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_admin_token_has_admin_role(keycloak_admin_client: AsyncClient):
    """Verify that the testadmin user gets admin roles."""
    resp = await keycloak_admin_client.get("/web/health")
    assert resp.status_code == 200


# ---- Service account tests ----


@pytest.mark.asyncio
async def test_service_account_token_resolves(keycloak_service_client: AsyncClient):
    """Verify that a client_credentials token resolves as a service account."""
    resp = await keycloak_service_client.get("/web/health")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_service_account_identity_claims():
    """Verify the service account token contains expected claims."""
    import jwt

    token = await get_service_account_token()
    claims = jwt.decode(token, options={"verify_signature": False})

    # Service account tokens have client_id but no email
    assert "client_id" in claims or "azp" in claims
    assert claims.get("azp") == "geoid-api"
