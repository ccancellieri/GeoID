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
API endpoint tests for Simplified IAG v2.1

Tests REST API endpoints for authorization management.
"""

import pytest
import pytest_asyncio
from types import SimpleNamespace
from httpx import AsyncClient
from dynastore.tools.identifiers import generate_uuidv7
from tests.dynastore.test_utils import generate_test_id

import logging
from dynastore.tools.discovery import get_protocol
from dynastore.modules.iam.iam_service import IamService

logger = logging.getLogger(__name__)


@pytest_asyncio.fixture
async def created_user_email(sysadmin_in_process_client: AsyncClient) -> str:
    """Creates a principal with identity link and returns their email."""
    iam_service = get_protocol(IamService)
    assert iam_service is not None
    storage = iam_service.storage
    assert storage is not None

    email = f"test_{generate_test_id(12)}@example.com"
    subject_id = f"sub_{generate_test_id()}"

    logger.info(f"Fixture creating principal for {email}")

    # Create principal
    from dynastore.modules.iam.models import Principal
    principal = Principal(
        id=generate_uuidv7(),
        identifier=email,
        display_name=email,
        roles=[],
        is_active=True,
    )
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.models.protocols import DatabaseProtocol
    db = get_protocol(DatabaseProtocol)
    async with managed_transaction(db.engine) as conn:
        await storage.create_principal(principal, conn=conn)
        await storage.create_identity_link(
            provider="local", subject_id=subject_id,
            principal_id=principal.id, conn=conn,
        )

    logger.info(f"Principal {principal.id} created with email {email}")
    return email


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
# @pytest.mark.skip()
class TestAuthorizationAPI:
    """Test authorization REST API endpoints."""

    async def test_grant_global_roles(
        self, in_process_client: AsyncClient, admin_token: str, created_user_email: str
    ):
        """Test granting global roles via API."""
        email = created_user_email
        print(f"\nDEBUG: Testing with email: {email}")

        print(f"\nDEBUG: Using principal-based identity for {email}")

        response = await in_process_client.post(
            f"/iam/admin/users/{email}/roles/global",
            json={"roles": ["admin", "user"]},
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        if response.status_code != 201:
            print(f"DEBUG: Response error: {response.text}")

        assert response.status_code == 201
        assert "admin" in response.json()["roles"]

    async def test_get_global_roles(
        self, in_process_client: AsyncClient, admin_token: str, created_user_email: str
    ):
        """Test getting global roles via API."""
        email = created_user_email

        # Grant roles first
        await in_process_client.post(
            f"/iam/admin/users/{email}/roles/global",
            json={"roles": ["admin"]},
            headers={"Authorization": f"Bearer {admin_token}"},
        )

        # Get roles
        response = await in_process_client.get(
            f"/iam/admin/users/{email}/roles/global",
            headers={"Authorization": f"Bearer {admin_token}"},
        )

        assert response.status_code == 200
        assert "admin" in response.json()

    async def test_revoke_global_role(
        self, in_process_client: AsyncClient, admin_token: str, created_user_email: str
    ):
        """Test revoking a global role via API."""
        email = created_user_email

        # Grant roles
        await in_process_client.post(
            f"/iam/admin/users/{email}/roles/global",
            json={"roles": ["admin", "user"]},
            headers={"Authorization": f"Bearer {admin_token}"},
        )

        # Revoke one role
        role = "user"
        response = await in_process_client.delete(
            f"/iam/admin/users/{email}/roles/global/{role}",
            headers={"Authorization": f"Bearer {admin_token}"},
        )

        assert response.status_code == 204

        # Verify
        response = await in_process_client.get(
            f"/iam/admin/users/{email}/roles/global",
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        assert "admin" in response.json()
        assert "user" not in response.json()

    async def test_grant_catalog_roles(
        self,
        in_process_client: AsyncClient,
        admin_token: str,
        created_user_email: str,
        setup_catalogs,
    ):
        """Test granting catalog-specific roles via API."""
        email = created_user_email
        catalog_id = setup_catalogs[0]

        response = await in_process_client.post(
            f"/iam/admin/users/{email}/roles/catalogs/{catalog_id}",
            json={"roles": ["editor", "allUsers"]},
            headers={"Authorization": f"Bearer {admin_token}"},
        )

        assert response.status_code == 201
        assert "editor" in response.json()["roles"]

    async def test_grant_global_roles_unknown_role_422(
        self, in_process_client: AsyncClient, admin_token: str, created_user_email: str
    ):
        """Unknown platform role -> 422 (issue #499: align with /admin/.../roles)."""
        response = await in_process_client.post(
            f"/iam/admin/users/{created_user_email}/roles/global",
            json={"roles": ["NotARealRole"]},
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        assert response.status_code == 422, response.text
        assert "NotARealRole" in response.text

    async def test_grant_catalog_roles_unknown_role_422(
        self,
        in_process_client: AsyncClient,
        admin_token: str,
        created_user_email: str,
        setup_catalogs,
    ):
        """Unknown catalog-scope role -> 422 (issue #499)."""
        catalog_id = setup_catalogs[0]
        response = await in_process_client.post(
            f"/iam/admin/users/{created_user_email}/roles/catalogs/{catalog_id}",
            json={"roles": ["NotARealRole"]},
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        assert response.status_code == 422, response.text
        assert "NotARealRole" in response.text

    async def test_get_user_catalogs(
        self,
        in_process_client: AsyncClient,
        admin_token: str,
        created_user_email: str,
        setup_catalogs,
    ):
        """Test getting all catalogs for a user via API."""
        email = created_user_email

        # Grant global roles
        await in_process_client.post(
            f"/iam/admin/users/{email}/roles/global",
            json={"roles": ["admin"]},
            headers={"Authorization": f"Bearer {admin_token}"},
        )

        # Grant catalog roles
        resp = await in_process_client.post(
            f"/iam/admin/users/{email}/roles/catalogs/{setup_catalogs[1]}",
            json={"roles": ["editor"]},
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        assert resp.status_code == 201, f"Failed to grant catalog role: {resp.text}"

        # Get catalogs
        response = await in_process_client.get(
            f"/iam/admin/users/{email}/catalogs",
            headers={"Authorization": f"Bearer {admin_token}"},
        )

        assert response.status_code == 200
        catalogs = response.json()

        # Should have global and catalog_a
        catalog_ids = [c["catalog_id"] for c in catalogs]
        assert "*" in catalog_ids  # Global
        assert setup_catalogs[1] in catalog_ids

    async def test_get_effective_authorization(
        self,
        in_process_client: AsyncClient,
        admin_token: str,
        created_user_email: str,
        setup_catalogs,
    ):
        """Test getting effective authorization via API."""
        email = created_user_email
        catalog_id = setup_catalogs[0]

        # Grant global and catalog roles
        await in_process_client.post(
            f"/iam/admin/users/{email}/roles/global",
            json={"roles": ["admin"]},
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        await in_process_client.post(
            f"/iam/admin/users/{email}/roles/catalogs/{catalog_id}",
            json={"roles": ["editor"]},
            headers={"Authorization": f"Bearer {admin_token}"},
        )

        # Get effective authorization
        response = await in_process_client.get(
            f"/iam/admin/users/{email}/catalogs/{catalog_id}",
            headers={"Authorization": f"Bearer {admin_token}"},
        )

        assert response.status_code == 200
        auth = response.json()

        assert auth["email"] == email
        assert "admin" in auth["global_roles"]
        assert "editor" in auth["catalog_roles"]
        assert set(auth["effective_roles"]) == {"admin", "editor"}

    async def test_email_not_found(
        self, in_process_client: AsyncClient, admin_token: str
    ):
        """Test API with non-existent email."""
        response = await in_process_client.get(
            "/iam/admin/users/nonexistent@example.com/roles/global",
            headers={"Authorization": f"Bearer {admin_token}"},
        )

        assert response.status_code == 404


@pytest.mark.asyncio
# @pytest.mark.skip()
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
async def admin_token(
    sysadmin_in_process_client: AsyncClient, in_process_client: AsyncClient
):
    """Creates a test principal with admin role and returns a JWT token."""
    import jwt as pyjwt
    from datetime import datetime, timezone, timedelta
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import AuthenticatorProtocol

    iam_svc = get_protocol(AuthenticatorProtocol)
    if not iam_svc:
        pytest.skip("AuthenticatorProtocol not available")

    subj_id = f"int_admin_user_{generate_test_id()}"
    principal_payload = {
        "provider": "local",
        "subject_id": subj_id,
        "roles": ["admin"],
        "attributes": {
            "description": "Integration test admin user for authorization API"
        },
    }

    resp = await sysadmin_in_process_client.post(
        "/iam/governance/principals", json=principal_payload
    )
    assert resp.status_code in [200, 201, 409]

    secret = await iam_svc.get_jwt_secret()
    now = datetime.now(timezone.utc)
    payload = {
        "sub": subj_id,
        "roles": ["admin"],
        "iat": now,
        "exp": now + timedelta(hours=1),
        "iss": "dynastore-test",
    }
    return pyjwt.encode(payload, secret, algorithm="HS256")


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
        "provider": "local",
        "subject_id": subj_id,
        "roles": [],
        "attributes": {
            "description": "Integration test regular user for authorization API"
        },
    }

    resp = await sysadmin_in_process_client.post(
        "/iam/governance/principals", json=principal_payload
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

# """
# API endpoint tests for Simplified IAG v2.1

# Tests REST API endpoints for authorization management.
# """

# import pytest
# import pytest_asyncio
# from httpx import AsyncClient
# from uuid import uuid4


# @pytest.mark.asyncio
# @pytest.mark.skip()
# class TestAuthorizationAPI:
#     """Test authorization REST API endpoints."""

#     async def test_grant_global_roles(self, in_process_client: AsyncClient, admin_token: str):
#         """Test granting global roles via API."""
#         email = f"test_{uuid4()}@example.com"

#         response = await in_process_client.post(
#             f"/admin/users/{email}/roles/global",
#             json={"roles": ["admin", "user"]},
#             headers={"Authorization": f"Bearer {admin_token}"}
#         )

#         assert response.status_code == 201
#         assert "admin" in response.json()["roles"]

#     async def test_get_global_roles(self, in_process_client: AsyncClient, admin_token: str):
#         """Test getting global roles via API."""
#         email = f"test_{uuid4()}@example.com"

#         # Grant roles first
#         await in_process_client.post(
#             f"/admin/users/{email}/roles/global",
#             json={"roles": ["admin"]},
#             headers={"Authorization": f"Bearer {admin_token}"}
#         )

#         # Get roles
#         response = await in_process_client.get(
#             f"/admin/users/{email}/roles/global",
#             headers={"Authorization": f"Bearer {admin_token}"}
#         )

#         assert response.status_code == 200
#         assert "admin" in response.json()

#     async def test_revoke_global_role(self, in_process_client: AsyncClient, admin_token: str):
#         """Test revoking a global role via API."""
#         email = f"test_{uuid4()}@example.com"

#         # Grant roles
#         await in_process_client.post(
#             f"/admin/users/{email}/roles/global",
#             json={"roles": ["admin", "user"]},
#             headers={"Authorization": f"Bearer {admin_token}"}
#         )

#         # Revoke one role
#         response = await in_process_client.delete(
#             f"/admin/users/{email}/roles/global/user",
#             headers={"Authorization": f"Bearer {admin_token}"}
#         )

#         assert response.status_code == 204

#         # Verify
#         response = await in_process_client.get(
#             f"/admin/users/{email}/roles/global",
#             headers={"Authorization": f"Bearer {admin_token}"}
#         )
#         assert "admin" in response.json()
#         assert "user" not in response.json()

#     async def test_grant_catalog_roles(self, in_process_client: AsyncClient, admin_token: str):
#         """Test granting catalog-specific roles via API."""
#         email = f"test_{uuid4()}@example.com"
#         catalog_id = "test_catalog"

#         response = await in_process_client.post(
#             f"/admin/users/{email}/roles/catalogs/{catalog_id}",
#             json={"roles": ["editor", "allUsers"]},
#             headers={"Authorization": f"Bearer {admin_token}"}
#         )

#         assert response.status_code == 201
#         assert "editor" in response.json()["roles"]

#     async def test_get_user_catalogs(self, in_process_client: AsyncClient, admin_token: str):
#         """Test getting all catalogs for a user via API."""
#         email = f"test_{uuid4()}@example.com"

#         # Grant global roles
#         await in_process_client.post(
#             f"/admin/users/{email}/roles/global",
#             json={"roles": ["admin"]},
#             headers={"Authorization": f"Bearer {admin_token}"}
#         )

#         # Grant catalog roles
#         await in_process_client.post(
#             f"/admin/users/{email}/roles/catalogs/catalog_a",
#             json={"roles": ["editor"]},
#             headers={"Authorization": f"Bearer {admin_token}"}
#         )

#         # Get catalogs
#         response = await in_process_client.get(
#             f"/admin/users/{email}/catalogs",
#             headers={"Authorization": f"Bearer {admin_token}"}
#         )

#         assert response.status_code == 200
#         catalogs = response.json()

#         # Should have global and catalog_a
#         catalog_ids = [c["catalog_id"] for c in catalogs]
#         assert "*" in catalog_ids  # Global
#         assert "catalog_a" in catalog_ids

#     async def test_get_effective_authorization(self, in_process_client: AsyncClient, admin_token: str):
#         """Test getting effective authorization via API."""
#         email = f"test_{uuid4()}@example.com"
#         catalog_id = "test_catalog"

#         # Grant global and catalog roles
#         await in_process_client.post(
#             f"/admin/users/{email}/roles/global",
#             json={"roles": ["admin"]},
#             headers={"Authorization": f"Bearer {admin_token}"}
#         )
#         await in_process_client.post(
#             f"/admin/users/{email}/roles/catalogs/{catalog_id}",
#             json={"roles": ["editor"]},
#             headers={"Authorization": f"Bearer {admin_token}"}
#         )

#         # Get effective authorization
#         response = await in_process_client.get(
#             f"/admin/users/{email}/catalogs/{catalog_id}",
#             headers={"Authorization": f"Bearer {admin_token}"}
#         )

#         assert response.status_code == 200
#         auth = response.json()

#         assert auth["email"] == email
#         assert "admin" in auth["global_roles"]
#         assert "editor" in auth["catalog_roles"]
#         assert set(auth["effective_roles"]) == {"admin", "editor"}

#     async def test_email_not_found(self, in_process_client: AsyncClient, admin_token: str):
#         """Test API with non-existent email."""
#         response = await in_process_client.get(
#             "/admin/users/nonexistent@example.com/roles/global",
#             headers={"Authorization": f"Bearer {admin_token}"}
#         )

#         assert response.status_code == 404


# @pytest.mark.asyncio
# @pytest.mark.skip()
# class TestSelfServiceAPI:
#     """Test self-service API endpoints."""

#     async def test_get_my_global_roles(self, in_process_client: AsyncClient, user_token: str):
#         """Test getting my own global roles."""
#         response = await in_process_client.get(
#             "/me/roles/global",
#             headers={"Authorization": f"Bearer {user_token}"}
#         )

#         assert response.status_code == 200
#         # User might have no roles initially
#         assert isinstance(response.json(), list)

#     async def test_get_my_catalogs(self, in_process_client: AsyncClient, user_token: str):
#         """Test getting my accessible catalogs."""
#         response = await in_process_client.get(
#             "/me/catalogs",
#             headers={"Authorization": f"Bearer {user_token}"}
#         )

#         assert response.status_code == 200
#         assert isinstance(response.json(), list)

#     async def test_get_my_catalog_roles(self, in_process_client: AsyncClient, user_token: str):
#         """Test getting my roles in a specific catalog."""
#         response = await in_process_client.get(
#             "/me/roles/catalogs/test_catalog",
#             headers={"Authorization": f"Bearer {user_token}"}
#         )

#         assert response.status_code == 200
#         assert isinstance(response.json(), list)

#     async def test_unauthenticated_access(self, in_process_client: AsyncClient):
#         """Test that unauthenticated requests are rejected."""
#         response = await in_process_client.get("/me/roles/global")

#         assert response.status_code == 401


# # Fixtures

# @pytest_asyncio.fixture
# async def admin_token(sysadmin_in_process_client: AsyncClient, in_process_client: AsyncClient):
#     """Creates a test principal with admin role, an API key, and returns a JWT token."""
#     subj_id = f"int_admin_user_{generate_test_id()}"
#     principal_payload = {
#         "provider": "local",
#         "subject_id": subj_id,
#         "roles": ["admin"],
#         "attributes": {"description": "Integration test admin user for authorization API"}
#     }

#     resp = await sysadmin_in_process_client.post("/iam/governance/principals", json=principal_payload)
#     assert resp.status_code in [200, 201, 409]

#     key_payload = {
#         "provider": "local",
#         "subject_id": subj_id,
#         "description": "Integration test admin API key"
#     }

#     resp = await sysadmin_in_process_client.post("/iam/credentials/keys", json=key_payload)
#     assert resp.status_code in [200, 201]

#     raw_key = resp.json().get("raw_key")
#     assert raw_key

#     login_resp = await in_process_client.post("/iam/auth/login", json={"api_key": raw_key, "ttl_seconds": 3600})
#     assert login_resp.status_code == 200

#     return login_resp.json()["access_token"]


# @pytest_asyncio.fixture
# async def user_token(sysadmin_in_process_client: AsyncClient, in_process_client: AsyncClient):
#     """Creates a test principal with no special roles, an API key, and returns a JWT token."""
#     subj_id = f"int_user_{generate_test_id()}"
#     principal_payload = {
#         "provider": "local",
#         "subject_id": subj_id,
#         "roles": [],
#         "attributes": {"description": "Integration test regular user for authorization API"}
#     }

#     resp = await sysadmin_in_process_client.post("/iam/governance/principals", json=principal_payload)
#     assert resp.status_code in [200, 201, 409]

#     key_payload = {
#         "provider": "local",
#         "subject_id": subj_id,
#         "description": "Integration test user API key"
#     }

#     resp = await sysadmin_in_process_client.post("/iam/credentials/keys", json=key_payload)
#     assert resp.status_code in [200, 201]

#     raw_key = resp.json().get("raw_key")
#     assert raw_key

#     login_resp = await in_process_client.post("/iam/auth/login", json={"api_key": raw_key, "ttl_seconds": 3600})
#     assert login_resp.status_code == 200

#     return login_resp.json()["access_token"]
