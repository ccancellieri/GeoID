#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

"""
API endpoint tests for the self-service authorization surface (`/iam/me/*`).

Admin-side role management is served by the `/admin/*` surface (admin
extension) and covered by that extension's own integration tests.
"""

import pytest
import pytest_asyncio
from httpx import AsyncClient
from dynastore.tools.identifiers import generate_uuidv7  # noqa: F401
from tests.dynastore.test_utils import generate_test_id

import logging

logger = logging.getLogger(__name__)


@pytest_asyncio.fixture(scope="function")
async def setup_catalogs(sysadmin_in_process_client: AsyncClient):
    """Ensure test catalogs exist for the class, cleaned up after use."""
    catalog_ids = [f"cat_{generate_test_id()}", f"cat_{generate_test_id()}"]
    for catalog_id in catalog_ids:
        payload = {
            "id": catalog_id,
            "title": catalog_id,
            "description": "Test catalog for auth API",
        }
        response = await sysadmin_in_process_client.post(
            "/features/catalogs", json=payload
        )
        assert response.status_code in [201, 409]

    yield catalog_ids

    # Cleanup: delete catalogs created for this test
    for catalog_id in catalog_ids:
        try:
            await sysadmin_in_process_client.delete(f"/features/catalogs/{catalog_id}")
        except Exception:
            pass


@pytest.mark.asyncio
@pytest.mark.usefixtures("setup_catalogs")
class TestSelfServiceAPI:
    """Test self-service API endpoints."""

    async def test_get_my_global_roles(
        self, in_process_client: AsyncClient, user_token: str
    ):
        """Test getting my own global roles."""
        response = await in_process_client.get(
            "/iam/me/roles/global", headers={"Authorization": f"Bearer {user_token}"}
        )

        assert response.status_code == 200
        # User might have no roles initially
        assert isinstance(response.json(), list)

    async def test_get_my_catalogs(
        self, in_process_client: AsyncClient, user_token: str
    ):
        """Test getting my accessible catalogs."""
        response = await in_process_client.get(
            "/iam/me/catalogs", headers={"Authorization": f"Bearer {user_token}"}
        )

        assert response.status_code == 200
        assert isinstance(response.json(), list)

    async def test_get_my_catalog_roles(
        self, in_process_client: AsyncClient, user_token: str, setup_catalogs
    ):
        """Test getting my roles in a specific catalog."""
        response = await in_process_client.get(
            f"/iam/me/roles/catalogs/{setup_catalogs[0]}",
            headers={"Authorization": f"Bearer {user_token}"},
        )

        assert response.status_code == 200
        assert isinstance(response.json(), list)

    async def test_unauthenticated_access(self, in_process_client: AsyncClient):
        """Test that unauthenticated requests are rejected."""
        response = await in_process_client.get("/iam/me/roles/global")

        assert response.status_code in [401, 403]


# Fixtures


@pytest_asyncio.fixture
async def user_token(
    sysadmin_in_process_client: AsyncClient, in_process_client: AsyncClient
):
    """Creates a test principal with no special roles and returns a JWT token."""
    import jwt as pyjwt
    from datetime import datetime, timezone, timedelta
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import AuthenticatorProtocol

    iam_svc = get_protocol(AuthenticatorProtocol)
    if not iam_svc:
        pytest.skip("AuthenticatorProtocol not available")

    subj_id = f"int_user_{generate_test_id()}"
    principal_payload = {
        "username": subj_id,
        "subject_id": subj_id,
        "provider": "local",
        "roles": [],
    }

    resp = await sysadmin_in_process_client.post(
        "/admin/principals", json=principal_payload
    )
    assert resp.status_code in [200, 201, 409]

    secret = await iam_svc.get_jwt_secret()
    now = datetime.now(timezone.utc)
    payload = {
        "sub": subj_id,
        "roles": [],
        "iat": now,
        "exp": now + timedelta(hours=1),
        "iss": "dynastore-test",
    }
    return pyjwt.encode(payload, secret, algorithm="HS256")
