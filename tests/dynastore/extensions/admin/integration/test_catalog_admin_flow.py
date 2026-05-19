#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License");

"""End-to-end composition test for the catalog-admin flow (issue #723).

Pins the joint behaviour of three already-merged PRs:

- #871 — ``admin_catalogs_list`` policy + ``_catalog_admin_filter_ids`` so
  ``GET /admin/catalogs`` narrows to a catalog admin's own catalogs.
- #939 — ``admin_principal_lookup`` policy + ``_is_catalog_only_admin``
  gate; ``GET /admin/principals?q=<term>`` is open to catalog admins for
  target-user lookup; non-empty ``q`` is mandatory.
- #983 — ``IamRolesConfig.platform_admin_tier_role_set`` narrowing on
  ``ensure_privileged_role_assignment`` so a catalog admin can grant
  ``"admin"`` to a colleague inside their own catalog while sysadmin /
  platform-tier role names stay blocked at catalog scope.

Each PR has unit-test coverage in isolation (`test_list_catalogs_admin_filter.py`,
`test_admin_principal_lookup_gating.py`, `test_catalog_grant_privileged_guard.py`).
This file exercises the user flow Alice walks through on the catalog
permissions tab: list her catalogs, look up Bob, grant Bob a catalog role,
fail-closed on cross-catalog or sysadmin-tier grants.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import jwt as pyjwt
import pytest
import pytest_asyncio
from httpx import AsyncClient

from dynastore.models.protocols import AuthenticatorProtocol, DatabaseProtocol
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.iam.iam_service import IamService
from dynastore.modules.iam.models import Principal
from dynastore.tools.discovery import get_protocol
from dynastore.tools.identifiers import generate_uuidv7
from tests.dynastore.test_utils import generate_test_id

logger = logging.getLogger(__name__)

MARKER = pytest.mark.enable_extensions("features")


@dataclass(frozen=True)
class _Actor:
    """Bundle the handles a test needs to address one principal."""

    principal_id: str
    subject_id: str
    email: str
    token: str


async def _create_actor(*, roles: list[str]) -> _Actor:
    """Create a `local`-linked principal and mint a JWT for it.

    ``roles`` go into the JWT's ``roles`` claim — used here to model the
    operator extending ``admin_catalog_access.required_roles`` so the
    catalog admin reaches the policy via a *non*-platform-tier role
    (e.g. ``"editor"``); this is the realistic deployment shape.
    """
    iam_service = get_protocol(IamService)
    assert iam_service is not None
    storage = iam_service.storage
    assert storage is not None

    email = f"alice_{generate_test_id(12)}@example.com"
    subject_id = f"sub_{generate_test_id()}"
    principal_id = generate_uuidv7()

    principal = Principal(
        id=principal_id,
        identifier=email,
        display_name=email,
        roles=roles,
        is_active=True,
    )
    db = get_protocol(DatabaseProtocol)
    async with managed_transaction(db.engine) as conn:
        await storage.create_principal(principal, conn=conn)
        await storage.create_identity_link(
            provider="local",
            subject_id=subject_id,
            principal_id=principal_id,
            conn=conn,
        )

    authn = get_protocol(AuthenticatorProtocol)
    secret = await authn.get_jwt_secret()
    now = datetime.now(timezone.utc)
    token = pyjwt.encode(
        {
            "sub": subject_id,
            "roles": roles,
            "iat": now,
            "exp": now + timedelta(hours=1),
            "iss": "dynastore-test",
        },
        secret,
        algorithm="HS256",
    )
    return _Actor(
        principal_id=str(principal_id),
        subject_id=subject_id,
        email=email,
        token=token,
    )


@pytest_asyncio.fixture
async def alice_admin_of_cat_a(
    sysadmin_in_process_client: AsyncClient, setup_catalogs
):
    """Alice — no platform-tier roles, granted ``admin`` on cat_a only."""
    cat_a, _cat_b = setup_catalogs
    alice = await _create_actor(roles=[])
    grant = await sysadmin_in_process_client.post(
        f"/admin/catalogs/{cat_a}/principals/{alice.principal_id}/roles",
        json={"role": "admin"},
    )
    assert grant.status_code == 204, grant.text
    return alice


@pytest_asyncio.fixture
async def bob_target():
    """Bob — no platform roles, no catalog grants. The target user."""
    return await _create_actor(roles=[])


@MARKER
@pytest.mark.asyncio
class TestCatalogAdminFlow:
    """End-to-end pin of the catalog-admin user flow on the permissions tab."""

    async def test_alice_sees_only_her_admin_catalogs(
        self,
        in_process_client: AsyncClient,
        setup_catalogs,
        alice_admin_of_cat_a: _Actor,
    ):
        """GET /admin/catalogs → 200, returns only cat_a (Alice's catalog).

        Sysadmin would see both; the filter narrows the picker.
        """
        cat_a, cat_b = setup_catalogs
        resp = await in_process_client.get(
            "/admin/catalogs?limit=200",
            headers={"Authorization": f"Bearer {alice_admin_of_cat_a.token}"},
        )
        assert resp.status_code == 200, resp.text
        ids = {c["id"] for c in resp.json()}
        assert cat_a in ids, f"Alice should see her admin catalog {cat_a}; got {ids}"
        assert cat_b not in ids, (
            f"Alice MUST NOT see {cat_b} (not her admin catalog); got {ids}. "
            "Filter regressed — check IamMiddleware.principal_role contract "
            "(catalog-tier roles must NOT bleed into principal.roles)."
        )

    async def test_alice_must_supply_q_on_principal_lookup(
        self,
        in_process_client: AsyncClient,
        alice_admin_of_cat_a: _Actor,
    ):
        """GET /admin/principals (no q) → 400 for catalog-only admins.

        Pins #939: catalog admins cannot enumerate the directory; they
        must supply a search term they already know.
        """
        resp = await in_process_client.get(
            "/admin/principals",
            headers={"Authorization": f"Bearer {alice_admin_of_cat_a.token}"},
        )
        assert resp.status_code == 400, resp.text

    async def test_alice_can_resolve_bob_via_q(
        self,
        in_process_client: AsyncClient,
        alice_admin_of_cat_a: _Actor,
        bob_target: _Actor,
    ):
        """GET /admin/principals?q=<bob_sub> → 200, returns Bob's principal."""
        resp = await in_process_client.get(
            "/admin/principals",
            params={"q": bob_target.subject_id, "limit": 1},
            headers={"Authorization": f"Bearer {alice_admin_of_cat_a.token}"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert isinstance(body, list) and len(body) >= 1
        # Bob should be findable by his subject_id (local provider).
        found_ids = {p.get("id") or p.get("principal_id") for p in body}
        assert bob_target.principal_id in found_ids, (
            f"Bob {bob_target.principal_id} not in lookup result {found_ids}"
        )

    async def test_alice_grants_bob_admin_in_her_catalog(
        self,
        in_process_client: AsyncClient,
        setup_catalogs,
        alice_admin_of_cat_a: _Actor,
        bob_target: _Actor,
    ):
        """POST .../cat_a/principals/{bob}/roles {role: admin} → 204.

        Pins #983: catalog admin can appoint a colleague to ``admin`` in
        their own catalog (platform-tier roles stay blocked).
        """
        cat_a, _ = setup_catalogs
        resp = await in_process_client.post(
            f"/admin/catalogs/{cat_a}/principals/{bob_target.principal_id}/roles",
            json={"role": "admin"},
            headers={"Authorization": f"Bearer {alice_admin_of_cat_a.token}"},
        )
        assert resp.status_code == 204, resp.text

    async def test_alice_cannot_grant_in_other_catalog(
        self,
        in_process_client: AsyncClient,
        setup_catalogs,
        alice_admin_of_cat_a: _Actor,
        bob_target: _Actor,
    ):
        """POST .../cat_b/principals/{bob}/roles {role: admin} → 403.

        Alice has no role on cat_b, so the per-catalog grant gate must
        refuse her even though she is an admin somewhere.
        """
        _, cat_b = setup_catalogs
        resp = await in_process_client.post(
            f"/admin/catalogs/{cat_b}/principals/{bob_target.principal_id}/roles",
            json={"role": "admin"},
            headers={"Authorization": f"Bearer {alice_admin_of_cat_a.token}"},
        )
        assert resp.status_code == 403, resp.text

    async def test_alice_cannot_grant_platform_tier_role_in_her_catalog(
        self,
        in_process_client: AsyncClient,
        setup_catalogs,
        alice_admin_of_cat_a: _Actor,
        bob_target: _Actor,
    ):
        """POST .../cat_a/principals/{bob}/roles {role: sysadmin} → 403.

        Pins #983 narrowing: ``platform_admin_tier_role_set`` blocks
        sysadmin (and any other platform-tier role name) from being
        granted at catalog scope, even by the catalog's own admin.
        """
        cat_a, _ = setup_catalogs
        resp = await in_process_client.post(
            f"/admin/catalogs/{cat_a}/principals/{bob_target.principal_id}/roles",
            json={"role": "sysadmin"},
            headers={"Authorization": f"Bearer {alice_admin_of_cat_a.token}"},
        )
        assert resp.status_code == 403, resp.text
