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


async def _create_actor(*, roles: list[str], handle: str = "actor") -> _Actor:
    """Create an `internal`-linked principal and mint an HS256 JWT for it.

    Provider must be ``"internal"`` to match the synthesized Principal the
    HS256 fallback in :meth:`IamService.authenticate_and_get_role` builds
    when no OIDC provider validates the token — that synthesized Principal
    carries ``provider="internal"``, and ``_augment_with_catalog_sentinels``
    keys catalog membership lookup on ``(provider, subject_id)``. Linking
    as ``"local"`` would silently lose the catalog-tier sentinel and
    misdiagnose policy failures as authentication ones.

    ``roles`` go into the JWT's ``roles`` claim — used here to model the
    operator extending ``admin_catalog_access.required_roles`` so the
    catalog admin reaches the policy via a *non*-platform-tier role
    (e.g. ``"editor"``); this is the realistic deployment shape.

    ``handle`` distinguishes principals in the email/display_name so the
    ``GET /admin/principals?q=<handle>`` lookup can find them — the
    underlying SQL searches ``identifier LIKE`` / ``display_name LIKE``,
    not the JWT subject_id.
    """
    iam_service = get_protocol(IamService)
    assert iam_service is not None
    storage = iam_service.storage
    assert storage is not None

    email = f"{handle}_{generate_test_id(12)}@example.com"
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
            provider="internal",
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
    alice = await _create_actor(roles=[], handle="alice")
    grant = await sysadmin_in_process_client.post(
        f"/admin/catalogs/{cat_a}/principals/{alice.principal_id}/roles",
        json={"role": "admin"},
    )
    assert grant.status_code == 204, grant.text
    return alice


@pytest_asyncio.fixture
async def bob_target():
    """Bob — no platform roles, no catalog grants. The target user."""
    return await _create_actor(roles=[], handle="bob")


@pytest_asyncio.fixture
async def enable_catalog_admin_delegation(
    sysadmin_in_process_client: AsyncClient,
):
    """Operator wiring: let the catalog's own admin delegate within
    their catalog.

    Two changes ship to a deployment that adopts this pattern:

    1. ``admin_catalog_access.conditions[catalog_admin_required].required_roles``
       gains ``"admin"`` — the policy's per-catalog gate admits anyone
       holding ``"admin"`` in the request's catalog.
    2. The ``catalog_admin`` sentinel role gains ``admin_catalog_access``
       in its ``policies`` list — without a role→policy binding the
       policy engine never even *evaluates* the condition for the
       catalog-only admin (intentional default per
       :func:`admin_role_bindings` so operators opt in deliberately).

    Both are restored on teardown.
    """
    policy_id = "admin_catalog_access"
    sentinel_role = "catalog_admin"
    # ---- snapshot prior state for clean restore ---------------------------
    pol_list = await sysadmin_in_process_client.get("/admin/policies")
    assert pol_list.status_code == 200, pol_list.text
    prior_pol = next(p for p in pol_list.json() if p.get("id") == policy_id)
    prior_conds = prior_pol.get("conditions") or []

    role_get = await sysadmin_in_process_client.get(f"/admin/roles/{sentinel_role}")
    assert role_get.status_code == 200, role_get.text
    prior_role = role_get.json()
    prior_policies = list(prior_role.get("policies") or [])

    # ---- apply the delegation wiring -------------------------------------
    resp = await sysadmin_in_process_client.put(
        f"/admin/policies/{policy_id}",
        json={
            "conditions": [
                {
                    "type": "catalog_admin_required",
                    "config": {"required_roles": ["admin"]},
                }
            ]
        },
    )
    assert resp.status_code == 200, resp.text

    extended = prior_policies + [policy_id] if policy_id not in prior_policies else prior_policies
    bind = await sysadmin_in_process_client.put(
        f"/admin/roles/{sentinel_role}",
        json={"policies": extended},
    )
    assert bind.status_code == 200, bind.text
    yield
    # ---- restore prior state ---------------------------------------------
    await sysadmin_in_process_client.put(
        f"/admin/policies/{policy_id}",
        json={"conditions": prior_conds},
    )
    await sysadmin_in_process_client.put(
        f"/admin/roles/{sentinel_role}",
        json={"policies": prior_policies},
    )


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
        """GET /admin/principals?q=<bob_email> → 200, returns Bob's principal.

        The directory search runs ``identifier LIKE :q OR display_name LIKE :q``
        (see ``iam_queries.build_search_principals_query``) — JWT subject_id
        is NOT indexed for search, so a real operator looking up a target
        user always types something visible (their email or display name).
        """
        resp = await in_process_client.get(
            "/admin/principals",
            params={"q": bob_target.email, "limit": 5},
            headers={"Authorization": f"Bearer {alice_admin_of_cat_a.token}"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert isinstance(body, list) and len(body) >= 1
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
        enable_catalog_admin_delegation,
    ):
        """POST .../cat_a/principals/{bob}/roles {role: admin} → 204.

        Pins #983: with the operator delegation wired
        (``enable_catalog_admin_delegation`` binds ``admin_catalog_access``
        to the ``catalog_admin`` sentinel and sets
        ``required_roles=["admin"]``), the catalog's own admin can appoint
        a colleague to ``admin`` inside her catalog while platform-tier
        roles stay blocked.

        Path of the ALLOW: ``IamMiddleware._augment_with_catalog_sentinels``
        adds the ``catalog_admin`` sentinel to Alice's flat role list
        (she holds ``admin`` in cat_a), the sentinel's ``admin_catalog_access``
        binding makes the per-catalog mutation policy reachable, and that
        policy's ``catalog_admin_required`` condition re-confirms Alice's
        ``admin`` grant for cat_a — all on the live request path.

        This previously carried a non-strict xfail attributing an observed
        ``No matching ALLOW policy found`` to an "undiagnosed request-path
        gap". That observation was a misdiagnosis: the denial originated in
        the ``setup_catalogs`` / ``alice_admin_of_cat_a`` fixtures failing
        under cross-process test-DB churn (catalog creation / sysadmin
        grant 403/401/500 while a concurrent run reset the shared schema),
        not in Alice's own grant. Run in isolation against a stable DB the
        grant is deterministically 204.
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
        enable_catalog_admin_delegation,
    ):
        """POST .../cat_b/principals/{bob}/roles {role: admin} → 403.

        Alice has no role on cat_b, so the per-catalog grant gate must
        refuse her even though she is an admin somewhere. Even with the
        delegation policy wired (``required_roles=["admin"]``), the
        condition checks Alice's catalog-tier role *for cat_b
        specifically* — she has none.
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
        enable_catalog_admin_delegation,
    ):
        """POST .../cat_a/principals/{bob}/roles {role: sysadmin} → 403.

        Pins #983 narrowing: ``platform_admin_tier_role_set`` blocks
        sysadmin (and any other platform-tier role name) from being
        granted at catalog scope, even by the catalog's own admin.
        Without ``enable_catalog_admin_delegation`` Alice would 403 at
        the *policy* layer (condition denies her); with it she passes
        the policy, so the assertion verifies the deeper
        ``admin_role_set`` guard, not the outer one.
        """
        cat_a, _ = setup_catalogs
        resp = await in_process_client.post(
            f"/admin/catalogs/{cat_a}/principals/{bob_target.principal_id}/roles",
            json={"role": "sysadmin"},
            headers={"Authorization": f"Bearer {alice_admin_of_cat_a.token}"},
        )
        assert resp.status_code == 403, resp.text
