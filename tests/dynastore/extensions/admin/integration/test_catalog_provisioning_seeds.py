"""
Catalog-provisioning seed assertions for the Option B unified-grants model.

A freshly-provisioned catalog must arrive with:
  - ``{schema}.roles``     — table exists, contains the four seeded rows
                             (admin / editor / user / unauthenticated).
  - ``{schema}.grants``    — table exists, count(*) == 0.
  - ``{schema}.role_hierarchy`` — table exists with the seeded admin→editor edge.

These tests reach into the database directly (rather than hitting the REST
layer) because the goal is to pin the **provisioning lifecycle**: any
regression in ``catalog_service._build_tenant_core_ddl_batch`` or the
seed helper would break role-grant scoping for every catalog created
afterwards. Hitting the API would test the same pipeline via a longer
chain and would not distinguish a missing table from a missing seed.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from dynastore.tools.discovery import get_protocol
from dynastore.modules.iam.iam_service import IamService
from dynastore.models.protocols import DatabaseProtocol
from dynastore.modules.db_config.query_executor import managed_transaction


MARKER = pytest.mark.enable_extensions("features")

EXPECTED_SEED_ROLES = {"admin", "editor", "user", "unauthenticated"}


async def _resolve_schema(catalog_id: str) -> str:
    iam = get_protocol(IamService)
    assert iam is not None, "IamService must be registered for this test."
    schema = await iam.resolve_schema(catalog_id)
    assert schema and schema != "iam", (
        f"resolve_schema fell back to 'iam' for catalog '{catalog_id}'; "
        f"the catalog is either missing or unprovisioned."
    )
    return schema


@MARKER
@pytest.mark.asyncio
async def test_fresh_catalog_seeds_four_default_roles(setup_catalogs):
    """{schema}.roles holds the four seeded rows on a fresh catalog."""
    catalog_id = setup_catalogs[0]
    schema = await _resolve_schema(catalog_id)
    db = get_protocol(DatabaseProtocol)
    assert db is not None

    async with managed_transaction(db.engine) as conn:
        result = await conn.execute(
            text(f'SELECT id FROM "{schema}".roles ORDER BY id;')
        )
        ids = {row[0] for row in result.fetchall()}

    assert EXPECTED_SEED_ROLES.issubset(ids), (
        f"Catalog {catalog_id!r} schema {schema!r}: expected seed roles "
        f"{EXPECTED_SEED_ROLES} ⊆ actual {ids}"
    )


@MARKER
@pytest.mark.asyncio
async def test_fresh_catalog_grants_table_starts_empty(setup_catalogs):
    """{schema}.grants exists and is empty on a fresh catalog."""
    catalog_id = setup_catalogs[0]
    schema = await _resolve_schema(catalog_id)
    db = get_protocol(DatabaseProtocol)
    assert db is not None

    async with managed_transaction(db.engine) as conn:
        result = await conn.execute(
            text(f'SELECT COUNT(*) FROM "{schema}".grants;')
        )
        count = result.scalar_one()

    assert count == 0, (
        f"Fresh catalog {catalog_id!r} should arrive with an empty "
        f"grants table; got {count} row(s)."
    )


@MARKER
@pytest.mark.asyncio
async def test_fresh_catalog_role_hierarchy_seeded(setup_catalogs):
    """{schema}.role_hierarchy carries the admin→editor seed edge.

    Encodes D5: the chain of authority `admin → editor` is established
    on every fresh catalog so an admin's policies cascade to editor
    capabilities without requiring tenants to wire it themselves.
    """
    catalog_id = setup_catalogs[0]
    schema = await _resolve_schema(catalog_id)
    db = get_protocol(DatabaseProtocol)
    assert db is not None

    async with managed_transaction(db.engine) as conn:
        result = await conn.execute(
            text(
                f'SELECT parent_role, child_role FROM "{schema}".role_hierarchy '
                f"WHERE parent_role = 'admin' AND child_role = 'editor';"
            )
        )
        rows = result.fetchall()

    assert rows, (
        f"Catalog {catalog_id!r} schema {schema!r}: "
        f"missing the seeded admin→editor role-hierarchy edge."
    )


@MARKER
@pytest.mark.asyncio
async def test_two_fresh_catalogs_do_not_share_grants(setup_catalogs):
    """Each catalog has its own grants partition — no shared `iam.principals.roles` JSONB.

    Sanity check for D6: the unified grants table is per-tenant, so a row
    inserted into catalog A's grants must be invisible from catalog B's
    grants. (The linchpin URL-level test in test_admin_routes lives at
    the HTTP layer; this peers at the storage layer for fail-fast
    evidence when the URL test goes red.)
    """
    catalog_a, catalog_b = setup_catalogs[0], setup_catalogs[1]
    schema_a = await _resolve_schema(catalog_a)
    schema_b = await _resolve_schema(catalog_b)
    assert schema_a != schema_b, (
        f"Distinct catalogs collapsed to one schema: {schema_a!r}"
    )

    db = get_protocol(DatabaseProtocol)
    assert db is not None

    async with managed_transaction(db.engine) as conn:
        # Each grants table is its own physical relation. The schema name is
        # validated upstream by ``_resolve_schema`` (rejects 'iam' fallback)
        # and is asserted-distinct on the Python side, so direct interpolation
        # is acceptable here in tests.
        result_a = await conn.execute(
            text(f"SELECT to_regclass('\"{schema_a}\".grants')::text;")
        )
        a_relname = result_a.scalar_one()
        result_b = await conn.execute(
            text(f"SELECT to_regclass('\"{schema_b}\".grants')::text;")
        )
        b_relname = result_b.scalar_one()

    assert a_relname == f"{schema_a}.grants", (
        f"Schema A grants table missing or misnamed: {a_relname!r}"
    )
    assert b_relname == f"{schema_b}.grants", (
        f"Schema B grants table missing or misnamed: {b_relname!r}"
    )
    assert a_relname != b_relname
