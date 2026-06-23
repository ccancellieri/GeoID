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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Unit tests for asset_physical_id as the immutable asset join key.

Pure-unit tests — no live DB.

Covers:
- DDL: asset_physical_id NOT NULL + assets_physical_uq constraint in assets
- DDL: asset_references keyed on asset_physical_id (not asset_id) with correct PK
- DDL: blocking index on asset_physical_id
- index_asset mints asset_physical_id (never overwrites on conflict)
- rename_asset source: no UPDATE asset_references statement
- Asset model: asset_physical_id is INTERNAL-ONLY — NOT a Pydantic field on Asset (#2296)
- AssetBase: no asset_physical_id (creation payload does not include it)
- generate_geoid: UUID format + uniqueness
"""
from __future__ import annotations

import re
import uuid


# ---------------------------------------------------------------------------
# 1. assets DDL — physical_id NOT NULL
# ---------------------------------------------------------------------------


def test_assets_ddl_contains_physical_id_column():
    """The assets CREATE TABLE DDL must declare an asset_physical_id UUID column."""
    import inspect
    from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver

    src = inspect.getsource(AssetPostgresqlDriver.ensure_storage)
    assert "asset_physical_id" in src, (
        "ensure_storage DDL source must mention asset_physical_id column"
    )
    assert "UUID" in src, (
        "asset_physical_id must be typed UUID in the assets DDL"
    )


def test_assets_ddl_physical_id_is_not_null():
    """asset_physical_id must be NOT NULL — clean-break deployment, all insert paths mint it."""
    import inspect
    from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver

    src = inspect.getsource(AssetPostgresqlDriver.ensure_storage)
    # Locate the asset_physical_id column definition line
    match = re.search(r"asset_physical_id\s+UUID([^\n,]+)", src)
    assert match is not None, "asset_physical_id column definition not found in ensure_storage source"
    col_def = match.group(0)
    assert "NOT NULL" in col_def.upper(), (
        f"asset_physical_id must be NOT NULL in the assets DDL: {col_def!r}"
    )


def test_assets_ddl_physical_uq_constraint_present():
    """assets must have assets_physical_uq UNIQUE (collection_physical_id, asset_physical_id)."""
    import inspect
    from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver

    src = inspect.getsource(AssetPostgresqlDriver.ensure_storage)
    assert "assets_physical_uq" in src, (
        "ensure_storage DDL must declare CONSTRAINT assets_physical_uq"
    )
    # The UNIQUE clause may be on the next line after the CONSTRAINT name (multiline DDL).
    # Search for the block spanning both lines.
    uq_match = re.search(
        r"assets_physical_uq[\s\S]{0,120}UNIQUE\s*\(([^)]+)\)",
        src,
    )
    assert uq_match is not None, "assets_physical_uq UNIQUE constraint definition not found"
    uq_cols = uq_match.group(1)
    assert "asset_physical_id" in uq_cols, (
        f"assets_physical_uq must include asset_physical_id: {uq_cols!r}"
    )
    assert "collection_physical_id" in uq_cols, (
        "assets_physical_uq must include collection_physical_id "
        "(Postgres requires partition key in UNIQUE on partitioned tables)"
    )


def test_assets_identity_uq_still_present():
    """assets_identity_uq (logical REST contract) must remain unchanged."""
    import inspect
    from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver

    src = inspect.getsource(AssetPostgresqlDriver.ensure_storage)
    assert "assets_identity_uq" in src, (
        "assets_identity_uq UNIQUE NULLS NOT DISTINCT must still be present"
    )


# ---------------------------------------------------------------------------
# 2. asset_references DDL — keyed on physical_id
# ---------------------------------------------------------------------------


def test_asset_references_ddl_has_physical_id():
    """asset_references must declare asset_physical_id UUID NOT NULL."""
    import inspect
    from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver

    src = inspect.getsource(AssetPostgresqlDriver.ensure_storage)
    # The refs_ddl portion follows the assets_ddl; both are in ensure_storage source.
    assert re.search(r"asset_physical_id\s+UUID\s+NOT NULL", src), (
        "asset_references must have asset_physical_id UUID NOT NULL column"
    )


def test_asset_references_ddl_primary_key_on_physical_id():
    """asset_references PRIMARY KEY must be (asset_physical_id, ref_type, ref_id) — not asset_id."""
    import inspect
    from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver

    src = inspect.getsource(AssetPostgresqlDriver.ensure_storage)
    pk_match = re.search(r"PRIMARY KEY\s*\(([^)]+)\)", src)
    assert pk_match is not None, "PRIMARY KEY not found in ensure_storage source"
    pk_cols = pk_match.group(1)
    assert "asset_physical_id" in pk_cols, (
        f"asset_references PRIMARY KEY must include asset_physical_id: {pk_cols!r}"
    )
    assert "asset_id" not in pk_cols, (
        f"asset_references PRIMARY KEY must NOT include mutable asset_id: {pk_cols!r}"
    )


def test_asset_references_ddl_blocking_index_on_physical_id():
    """The blocking index must be on (asset_physical_id), not (catalog_id, asset_id)."""
    import inspect
    from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver

    src = inspect.getsource(AssetPostgresqlDriver.ensure_storage)
    # Find idx_asset_refs_blocking index definition
    idx_match = re.search(
        r"idx_asset_refs_blocking[^\n]+\n\s+ON[^\n]+\(([^)]+)\)", src
    )
    assert idx_match is not None, "idx_asset_refs_blocking index definition not found"
    idx_cols = idx_match.group(1)
    assert "asset_physical_id" in idx_cols, (
        f"idx_asset_refs_blocking must index on asset_physical_id: {idx_cols!r}"
    )
    assert "asset_id" not in idx_cols, (
        f"idx_asset_refs_blocking must NOT index on mutable asset_id: {idx_cols!r}"
    )


def test_asset_references_ddl_no_asset_id_column():
    """asset_references must NOT declare an asset_id column (dropped in #2296)."""
    import inspect
    from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver

    src = inspect.getsource(AssetPostgresqlDriver.ensure_storage)
    # Isolate the CREATE TABLE asset_references block only (not comments/docstring).
    # The DDL string starts at 'CREATE TABLE IF NOT EXISTS' for asset_references.
    create_pos = src.find('CREATE TABLE IF NOT EXISTS')
    # Skip past the first CREATE TABLE (assets table); find the second one.
    second_create = src.find('CREATE TABLE IF NOT EXISTS', create_pos + 1)
    assert second_create != -1, "Second CREATE TABLE (asset_references) not found"
    # Grab from that point to the end of the closing parenthesis + semicolon.
    refs_ddl_fragment = src[second_create : second_create + 1200]
    # The column declaration would look like 'asset_id   VARCHAR'
    assert not re.search(r"\basset_id\s+VARCHAR", refs_ddl_fragment), (
        "asset_references CREATE TABLE must not declare an asset_id VARCHAR column — dropped in #2296"
    )


# ---------------------------------------------------------------------------
# 3. index_asset mints physical_id via generate_geoid
# ---------------------------------------------------------------------------


def test_index_asset_source_mints_physical_id():
    """index_asset must call generate_geoid() to mint asset_physical_id for new rows."""
    import inspect
    from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver

    src = inspect.getsource(AssetPostgresqlDriver.index_asset)
    assert "generate_geoid" in src, (
        "index_asset must import and call generate_geoid to mint asset_physical_id"
    )
    assert "asset_physical_id" in src, (
        "index_asset source must include asset_physical_id in the INSERT"
    )


def test_index_asset_physical_id_not_overwritten_on_conflict():
    """The ON CONFLICT DO UPDATE clause must NOT set asset_physical_id — it must
    remain immutable across upserts."""
    import inspect
    from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver

    src = inspect.getsource(AssetPostgresqlDriver.index_asset)
    do_update_match = re.search(
        r"ON CONFLICT.*?DO UPDATE SET(.+?)(?:\"\"\"|$)", src, re.DOTALL
    )
    assert do_update_match is not None, "ON CONFLICT DO UPDATE SET block not found"
    update_block = do_update_match.group(1)
    assert "asset_physical_id" not in update_block, (
        "asset_physical_id must NOT appear in the DO UPDATE SET clause (it is immutable)"
    )


# ---------------------------------------------------------------------------
# 4. rename_asset does NOT touch asset_references
# ---------------------------------------------------------------------------


def test_rename_asset_source_no_asset_references_update():
    """rename_asset must NOT issue an UPDATE against asset_references.

    asset_references keys on physical_id so the rename is a one-column
    label change on assets — zero propagation needed.
    """
    import inspect
    from dynastore.modules.catalog.asset_service import AssetService

    src = inspect.getsource(AssetService.rename_asset)
    # An UPDATE of asset_references would look like:
    #   UPDATE "{...}".asset_references SET asset_id = ...
    assert not re.search(r"UPDATE[^\"]*asset_references", src), (
        "rename_asset must not UPDATE asset_references — it keys on physical_id"
    )


# ---------------------------------------------------------------------------
# 5. generate_geoid produces a valid UUIDv7-shaped string
# ---------------------------------------------------------------------------


def test_generate_geoid_returns_valid_uuid_string():
    """generate_geoid() must return a string parseable as a UUID."""
    from dynastore.tools.identifiers import generate_geoid

    raw = generate_geoid()
    assert isinstance(raw, str), "generate_geoid must return a str"
    parsed = uuid.UUID(raw)  # raises ValueError if invalid
    assert str(parsed) == raw, "generate_geoid must return a canonical lowercase UUID"


def test_generate_geoid_is_unique():
    """Two successive generate_geoid() calls must not collide."""
    from dynastore.tools.identifiers import generate_geoid

    ids = {generate_geoid() for _ in range(50)}
    assert len(ids) == 50, "Unexpected collision in generate_geoid()"


# ---------------------------------------------------------------------------
# 6. Pydantic model fields — physical_id surface
# ---------------------------------------------------------------------------


def test_asset_base_asset_id_field_unchanged():
    """AssetBase.asset_id must still be the public logical identifier."""
    from dynastore.modules.catalog.asset_service import AssetBase

    assert "asset_id" in AssetBase.model_fields, (
        "AssetBase.asset_id field must still exist"
    )


def test_asset_model_does_not_expose_physical_id_field():
    """Asset (read model) must NOT declare asset_physical_id as a Pydantic field.

    asset_physical_id is INTERNAL-ONLY (#2296): the immutable join key never
    surfaces to the REST/STAC API — users deal only with asset_id/external_id/geoid.
    Internal callers read it from the row dict (it is in the SQL projections) or
    via AssetsProtocol.resolve_asset_physical_id; the public model strips any
    stray copy by simply not declaring it (extra='ignore').
    """
    from dynastore.modules.catalog.asset_service import Asset

    assert "asset_physical_id" not in Asset.model_fields, (
        "Asset must NOT expose asset_physical_id — it is an internal-only join key"
    )
    assert "physical_id" not in Asset.model_fields, (
        "Asset must NOT expose physical_id under the old name either"
    )


def test_asset_base_does_not_expose_physical_id():
    """AssetBase (create payload) must NOT declare asset_physical_id.

    The asset_physical_id is minted server-side; callers must not supply it.
    """
    from dynastore.modules.catalog.asset_service import AssetBase

    assert "asset_physical_id" not in AssetBase.model_fields, (
        "AssetBase must not expose asset_physical_id — it is minted server-side"
    )
    assert "physical_id" not in AssetBase.model_fields, (
        "AssetBase must not expose physical_id under the old name either"
    )


# ---------------------------------------------------------------------------
# 7. add_asset_reference source resolves physical_id
# ---------------------------------------------------------------------------


def test_add_asset_reference_source_resolves_physical_id():
    """add_asset_reference in the driver must look up asset_physical_id from assets
    before inserting into asset_references."""
    import inspect
    from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver

    src = inspect.getsource(AssetPostgresqlDriver.add_asset_reference)
    assert "asset_physical_id" in src, (
        "add_asset_reference must resolve and store asset_physical_id"
    )
    # Must NOT hard-code asset_id as the PK in the INSERT
    assert not re.search(
        r"INSERT INTO[^\n]*asset_references[^\n]*\n[^)]*\basset_id\b",
        src,
    ), "add_asset_reference INSERT must not write asset_id into asset_references"
