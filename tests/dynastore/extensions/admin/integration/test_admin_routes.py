"""
Integration tests for the Admin extension.

Covers read-only admin endpoints exercised by the sysadmin client:
- GET /admin/principals              — list/search principals (q/role/catalog_id/provider)
- GET /admin/roles              — list roles
- GET /admin/policies           — list policies
- GET /admin/catalogs/{id}/principals           — list users assigned to a catalog

Scope-first role-grant endpoints (Option B unified grants model):
- POST /admin/platform/principals/{pid}/roles                   — grant platform role
- DELETE /admin/platform/principals/{pid}/roles/{role}          — revoke platform role
- GET /admin/platform/principals/{pid}/roles                    — list platform roles
- POST /admin/catalogs/{cid}/principals/{pid}/roles             — grant catalog role
- DELETE /admin/catalogs/{cid}/principals/{pid}/roles/{role}    — revoke catalog role
- GET /admin/catalogs/{cid}/principals/{pid}/roles              — list catalog roles

Write operations (create/update/delete) are tested for basic acceptance
(status 201/204) to exercise the route handlers without asserting
domain-specific invariants.
"""

import uuid

import pytest
from httpx import AsyncClient

from .conftest import CreatedPrincipal


MARKER = pytest.mark.enable_extensions("features")


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_list_users_returns_200(sysadmin_in_process_client: AsyncClient):
    """GET /admin/principals — returns 200 with a list."""
    r = await sysadmin_in_process_client.get("/admin/principals")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)


@MARKER
@pytest.mark.asyncio
async def test_list_users_pagination(sysadmin_in_process_client: AsyncClient):
    """GET /admin/principals?limit=1 — pagination parameters accepted."""
    r = await sysadmin_in_process_client.get("/admin/principals", params={"limit": 1})
    assert r.status_code == 200


@MARKER
@pytest.mark.asyncio
async def test_update_user_deactivation_persists(
    sysadmin_in_process_client: AsyncClient, created_principal: "CreatedPrincipal"
):
    """PUT /admin/principals/{id} with {is_active: false} persists deactivation (issue #494).

    Prior bug: ``UPDATE_PRINCIPAL`` SQL omitted ``is_active`` from the
    SET clause, so the response echoed ``is_active: true`` and the user
    remained active in DB.
    """
    pid = created_principal.principal_id
    r = await sysadmin_in_process_client.put(
        f"/admin/principals/{pid}", json={"is_active": False}
    )
    assert r.status_code == 200, r.text
    assert r.json()["is_active"] is False
    # Verify persistence: a fresh GET reflects the new state.
    r2 = await sysadmin_in_process_client.get(f"/admin/principals/{pid}")
    assert r2.status_code == 200
    assert r2.json()["is_active"] is False


@MARKER
@pytest.mark.asyncio
async def test_create_user_raw_principal_path(
    sysadmin_in_process_client: AsyncClient,
):
    """POST /admin/principals without password — raw-principal create path.

    Synthetic identities (vault test principals, integration-test fixtures)
    must be creatable without going through the local-IdP password flow.
    With `password` omitted, the handler skips the local_provider hop and
    constructs the Principal directly using `subject_id` (defaulting to
    `username`).
    """
    suffix = uuid.uuid4().hex[:8]
    subject = f"raw-test-{suffix}"
    body = {
        "username": subject,
        "subject_id": subject,
        "roles": ["user"],
    }
    r = await sysadmin_in_process_client.post("/admin/principals", json=body)
    assert r.status_code == 201, r.text
    out = r.json()
    assert out["provider"] == "local"
    assert out["subject_id"] == subject
    assert out["display_name"] == subject

    # Teardown so re-running the test stays clean.
    pid = out["id"]
    await sysadmin_in_process_client.delete(f"/admin/principals/{pid}")


@MARKER
@pytest.mark.asyncio
async def test_get_unknown_user_404(sysadmin_in_process_client: AsyncClient):
    """GET /admin/principals/{id} — nonexistent user returns 404.

    The route declares ``principal_id: UUID`` so a non-UUID path segment
    fails FastAPI path validation with 422 before reaching the handler.
    Use a well-formed UUID that won't exist in IAM to exercise the 404 path.
    """
    import uuid
    nonexistent = str(uuid.uuid4())
    r = await sysadmin_in_process_client.get(f"/admin/principals/{nonexistent}")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Principals
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_search_principals_returns_200(sysadmin_in_process_client: AsyncClient):
    """GET /admin/principals — returns 200 (list mode)."""
    r = await sysadmin_in_process_client.get("/admin/principals")
    assert r.status_code == 200


@MARKER
@pytest.mark.asyncio
async def test_search_principals_with_query(sysadmin_in_process_client: AsyncClient):
    """GET /admin/principals?q= — search mode accepts the identifier filter."""
    r = await sysadmin_in_process_client.get("/admin/principals", params={"q": "admin"})
    assert r.status_code == 200


@MARKER
@pytest.mark.asyncio
async def test_search_principals_role_filter(sysadmin_in_process_client: AsyncClient):
    """GET /admin/principals?role= — role filter routes through search_principals."""
    r = await sysadmin_in_process_client.get("/admin/principals", params={"role": "admin"})
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# Roles
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_list_roles_returns_200(sysadmin_in_process_client: AsyncClient):
    """GET /admin/roles — returns a list of roles."""
    r = await sysadmin_in_process_client.get("/admin/roles")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)


@MARKER
@pytest.mark.asyncio
async def test_create_and_delete_role(sysadmin_in_process_client: AsyncClient):
    """POST /admin/roles + DELETE /admin/roles/{name} — role lifecycle."""
    role_name = "test_role_coverage_fixture"

    # Create
    r = await sysadmin_in_process_client.post(
        "/admin/roles",
        json={
            "name": role_name,
            "description": "Temporary test role for coverage",
            "permissions": [],
        },
    )
    # Accept 201 or 409 (already exists from a prior run)
    assert r.status_code in (201, 409), f"Unexpected: {r.status_code} {r.text}"

    # Delete
    r = await sysadmin_in_process_client.delete(f"/admin/roles/{role_name}")
    assert r.status_code in (204, 404)


# ---------------------------------------------------------------------------
# Policies
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_list_policies_returns_200(sysadmin_in_process_client: AsyncClient):
    """GET /admin/policies — returns a list."""
    r = await sysadmin_in_process_client.get("/admin/policies")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    # PR #927 added Policy.priority — every wire row must carry it.
    if data:
        assert "priority" in data[0], (
            f"PolicyResponse missing 'priority' field: keys={list(data[0])}"
        )


@MARKER
@pytest.mark.asyncio
async def test_policy_priority_roundtrip(sysadmin_in_process_client: AsyncClient):
    """POST/PUT/GET /admin/policies — priority field set, updated, and surfaced.

    Pins the wire contract for PR #927: clients (admin panel, automation)
    must be able to read/set Policy.priority through the HTTP surface.
    """
    pid = f"test-priority-{uuid.uuid4().hex[:8]}"
    create_body = {
        "id": pid,
        "description": "priority round-trip probe",
        "actions": ["GET"],
        "resources": [f"^/never-matched/{pid}$"],
        "effect": "ALLOW",
        "priority": 250,
    }
    try:
        r = await sysadmin_in_process_client.post("/admin/policies", json=create_body)
        assert r.status_code == 201, r.text
        assert r.json()["priority"] == 250

        r = await sysadmin_in_process_client.put(
            f"/admin/policies/{pid}", json={"priority": -300},
        )
        assert r.status_code == 200, r.text
        assert r.json()["priority"] == -300

        r = await sysadmin_in_process_client.get("/admin/policies")
        match = next((p for p in r.json() if p["id"] == pid), None)
        assert match is not None and match["priority"] == -300
    finally:
        await sysadmin_in_process_client.delete(f"/admin/policies/{pid}")


# ---------------------------------------------------------------------------
# Catalog users + scope-first role assignment (Option B unified grants model)
#
# These pin down the API surface of the admin endpoints for role management.
# The URL convention is **scope-first**:
#   POST   /admin/platform/principals/{pid}/roles                    (D6 — iam.grants)
#   POST   /admin/catalogs/{cid}/principals/{pid}/roles              (D6 — {schema}.grants)
#   DELETE /admin/platform/principals/{pid}/roles/{role}
#   DELETE /admin/catalogs/{cid}/principals/{pid}/roles/{role}
#   GET    /admin/platform/principals/{pid}/roles
#   GET    /admin/catalogs/{cid}/principals/{pid}/roles
#
# Behaviour pinned:
#  - Unknown catalog ⇒ 404 (handler pre-checks via CatalogsProtocol).
#  - Unknown principal ⇒ 404.
#  - Round-trip: POST a catalog grant ⇒ 204; GET /admin/catalogs/{cid}/principals
#    includes the principal; DELETE the grant ⇒ 204.
#  - **Linchpin scoping**: a grant against catalog A is invisible from
#    catalog B's role-list endpoint (the bug PR #65 left in place).
#    Tenants own their role definitions, so seed roles (admin/editor/
#    user/unauthenticated) are present in every fresh catalog and can
#    be granted directly without first POSTing a role definition.
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_list_catalog_users_returns_200(
    sysadmin_in_process_client: AsyncClient, setup_catalogs
):
    """GET /admin/catalogs/{catalog_id}/principals — 200 with a list."""
    catalog_id = setup_catalogs[0]
    r = await sysadmin_in_process_client.get(f"/admin/catalogs/{catalog_id}/principals")
    assert r.status_code == 200, r.text
    assert isinstance(r.json(), list)


@MARKER
@pytest.mark.asyncio
async def test_list_catalogs_for_admin_returns_200(
    sysadmin_in_process_client: AsyncClient, setup_catalogs
):
    """GET /admin/catalogs — 200 with a list of {id, title} entries (issue #495)."""
    r = await sysadmin_in_process_client.get("/admin/catalogs")
    assert r.status_code == 200, r.text
    data = r.json()
    assert isinstance(data, list)
    assert all("id" in c and "title" in c for c in data)
    seen = {c["id"] for c in data}
    assert setup_catalogs[0] in seen


@MARKER
@pytest.mark.asyncio
async def test_list_catalog_users_unknown_catalog_404(
    sysadmin_in_process_client: AsyncClient,
):
    """GET /admin/catalogs/{nonexistent}/principals — 404."""
    bogus = f"nonexistent_{uuid.uuid4().hex[:8]}"
    r = await sysadmin_in_process_client.get(f"/admin/catalogs/{bogus}/principals")
    assert r.status_code == 404


@MARKER
@pytest.mark.asyncio
async def test_grant_catalog_role_unknown_principal_404(
    sysadmin_in_process_client: AsyncClient, setup_catalogs
):
    """POST /admin/catalogs/{cid}/principals/{bogus_uuid}/roles — 404."""
    nonexistent = str(uuid.uuid4())
    r = await sysadmin_in_process_client.post(
        f"/admin/catalogs/{setup_catalogs[0]}/principals/{nonexistent}/roles",
        json={"role": "editor"},
    )
    assert r.status_code == 404


@MARKER
@pytest.mark.asyncio
async def test_grant_catalog_role_unknown_catalog_404(
    sysadmin_in_process_client: AsyncClient, created_principal: CreatedPrincipal
):
    """POST /admin/catalogs/{bogus}/principals/{pid}/roles — 404.

    The handler pre-checks catalog existence via ``CatalogsProtocol``
    before touching IAM storage, so an unknown catalog short-circuits
    to 404 instead of silently writing to the global ``iam`` schema
    (the fallback ``IamService.resolve_schema`` would otherwise return).
    """
    bogus = f"nonexistent_{uuid.uuid4().hex[:8]}"
    r = await sysadmin_in_process_client.post(
        f"/admin/catalogs/{bogus}/principals/{created_principal.principal_id}/roles",
        json={"role": "editor"},
    )
    assert r.status_code == 404, (
        f"Expected 404 for unknown catalog, got {r.status_code}: {r.text}"
    )


@MARKER
@pytest.mark.asyncio
async def test_revoke_catalog_role_unknown_catalog_404(
    sysadmin_in_process_client: AsyncClient, created_principal: CreatedPrincipal
):
    """DELETE /admin/catalogs/{bogus}/principals/{pid}/roles/{role} — 404.

    Same pre-check as the POST counterpart; an unknown catalog must not
    silently revoke against the global ``iam`` schema.
    """
    bogus = f"nonexistent_{uuid.uuid4().hex[:8]}"
    r = await sysadmin_in_process_client.delete(
        f"/admin/catalogs/{bogus}"
        f"/principals/{created_principal.principal_id}/roles/editor"
    )
    assert r.status_code == 404, (
        f"Expected 404 for unknown catalog, got {r.status_code}: {r.text}"
    )


@MARKER
@pytest.mark.asyncio
async def test_grant_and_revoke_catalog_role_round_trip(
    sysadmin_in_process_client: AsyncClient,
    setup_catalogs,
    created_principal: CreatedPrincipal,
):
    """Round-trip: grant a catalog-scoped role, see the principal in
    the catalog-users listing and the per-principal role list, then revoke."""
    catalog_id = setup_catalogs[0]
    pid = created_principal.principal_id

    # Grant
    r = await sysadmin_in_process_client.post(
        f"/admin/catalogs/{catalog_id}/principals/{pid}/roles",
        json={"role": "editor"},
    )
    assert r.status_code == 204, f"Grant failed: {r.status_code} {r.text}"

    # Listing should now include the principal
    r = await sysadmin_in_process_client.get(f"/admin/catalogs/{catalog_id}/principals")
    assert r.status_code == 200
    user_ids = [u.get("id") for u in r.json()]
    assert pid in user_ids, (
        f"Principal {pid} not in catalog-users after grant; got {user_ids}"
    )

    # Per-principal role list reports the grant
    r = await sysadmin_in_process_client.get(
        f"/admin/catalogs/{catalog_id}/principals/{pid}/roles"
    )
    assert r.status_code == 200, r.text
    assert "editor" in r.json(), f"Expected 'editor' in roles, got {r.json()}"

    # Revoke
    r = await sysadmin_in_process_client.delete(
        f"/admin/catalogs/{catalog_id}/principals/{pid}/roles/editor"
    )
    assert r.status_code == 204, f"Revoke failed: {r.status_code} {r.text}"

    # Per-principal role list no longer reports the grant
    r = await sysadmin_in_process_client.get(
        f"/admin/catalogs/{catalog_id}/principals/{pid}/roles"
    )
    assert r.status_code == 200
    assert "editor" not in r.json(), (
        f"Expected 'editor' to be absent after revoke; got {r.json()}"
    )


# ---------------------------------------------------------------------------
# Linchpin scoping tests — the bug PR #65 left untouched
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_catalog_role_grant_does_not_leak_across_catalogs(
    sysadmin_in_process_client: AsyncClient,
    setup_catalogs,
    created_principal: CreatedPrincipal,
):
    """Grant `editor` on catalog A; catalog B's role list MUST NOT include it.

    This is the linchpin scoping assertion. Before Option B, the storage
    layer accepted ``schema=`` but ignored it and persisted role grants
    on the global ``iam.principals.roles`` JSONB column — so a grant on
    catalog A was visible from catalog B. With the per-tenant
    ``{catalog_schema}.grants`` table, that leak is structurally
    impossible.
    """
    catalog_a, catalog_b = setup_catalogs[0], setup_catalogs[1]
    pid = created_principal.principal_id

    # Grant on catalog A only
    r = await sysadmin_in_process_client.post(
        f"/admin/catalogs/{catalog_a}/principals/{pid}/roles",
        json={"role": "editor"},
    )
    assert r.status_code == 204, f"Grant on A failed: {r.status_code} {r.text}"

    try:
        # Catalog A reports the role
        r_a = await sysadmin_in_process_client.get(
            f"/admin/catalogs/{catalog_a}/principals/{pid}/roles"
        )
        assert r_a.status_code == 200, r_a.text
        assert "editor" in r_a.json(), (
            f"Catalog A should have 'editor' grant; got {r_a.json()}"
        )

        # Catalog B does NOT report the role — this is the linchpin
        r_b = await sysadmin_in_process_client.get(
            f"/admin/catalogs/{catalog_b}/principals/{pid}/roles"
        )
        assert r_b.status_code == 200, r_b.text
        assert "editor" not in r_b.json(), (
            f"Catalog B leaked grant from catalog A; got {r_b.json()}"
        )

        # Catalog B's user listing also excludes the principal
        r_users_b = await sysadmin_in_process_client.get(
            f"/admin/catalogs/{catalog_b}/principals"
        )
        assert r_users_b.status_code == 200
        user_ids_b = [u.get("id") for u in r_users_b.json()]
        assert pid not in user_ids_b, (
            f"Principal {pid} leaked into catalog B's user list; got {user_ids_b}"
        )
    finally:
        # Clean up so the round-trip test can reuse the principal cleanly.
        await sysadmin_in_process_client.delete(
            f"/admin/catalogs/{catalog_a}/principals/{pid}/roles/editor"
        )


@MARKER
@pytest.mark.asyncio
async def test_platform_role_grant_round_trip(
    sysadmin_in_process_client: AsyncClient,
    created_principal: CreatedPrincipal,
):
    """Round-trip a platform-scope role grant via /admin/platform/...

    Platform grants live in ``iam.grants`` and are visible regardless of
    the request's catalog context. The seeded ``sysadmin`` role is a
    platform-only role per D1 + D5.
    """
    pid = created_principal.principal_id

    r = await sysadmin_in_process_client.post(
        f"/admin/platform/principals/{pid}/roles",
        json={"role": "sysadmin"},
    )
    assert r.status_code == 204, f"Platform grant failed: {r.status_code} {r.text}"

    try:
        r_list = await sysadmin_in_process_client.get(
            f"/admin/platform/principals/{pid}/roles"
        )
        assert r_list.status_code == 200, r_list.text
        assert "sysadmin" in r_list.json(), (
            f"Platform role list should include 'sysadmin'; got {r_list.json()}"
        )
    finally:
        r_del = await sysadmin_in_process_client.delete(
            f"/admin/platform/principals/{pid}/roles/sysadmin"
        )
        assert r_del.status_code == 204, (
            f"Platform revoke failed: {r_del.status_code} {r_del.text}"
        )


@MARKER
@pytest.mark.asyncio
async def test_platform_grant_does_not_appear_in_catalog_role_list(
    sysadmin_in_process_client: AsyncClient,
    setup_catalogs,
    created_principal: CreatedPrincipal,
):
    """Platform grants must not surface in any catalog's per-principal role list.

    `Principal.roles` (the middleware-resolved view) is platform ∪ catalog,
    but the *catalog-scope* admin endpoint must report only catalog-scope
    grants. A leak in either direction breaks D1 (scope intrinsic to role)
    and D6 (one grants table per scope).
    """
    catalog_id = setup_catalogs[0]
    pid = created_principal.principal_id

    r = await sysadmin_in_process_client.post(
        f"/admin/platform/principals/{pid}/roles",
        json={"role": "sysadmin"},
    )
    assert r.status_code == 204, r.text

    try:
        r_cat = await sysadmin_in_process_client.get(
            f"/admin/catalogs/{catalog_id}/principals/{pid}/roles"
        )
        assert r_cat.status_code == 200, r_cat.text
        assert "sysadmin" not in r_cat.json(), (
            f"Catalog role list leaked platform grant; got {r_cat.json()}"
        )
    finally:
        await sysadmin_in_process_client.delete(
            f"/admin/platform/principals/{pid}/roles/sysadmin"
        )
