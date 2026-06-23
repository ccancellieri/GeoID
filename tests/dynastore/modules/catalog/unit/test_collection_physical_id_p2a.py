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

"""Unit tests for P2a: collections.physical_id column in the tenant DDL and
resolve_physical_id registry-column-first behaviour.

These are pure-unit tests — no live DB required.
"""
from __future__ import annotations

import re
from unittest.mock import AsyncMock, patch, MagicMock

import pytest


_CAT_ID = "test_catalog_p2a"
_COL_ID = "test_collection_p2a"
_SCHEMA = "s_abc12345"
_PHYS_ID = "c_xyz98765"  # format produced by generate_physical_name("c")
_LEGACY_TABLE = "t_legacyabc"  # legacy: only in collection_configs JSONB


@pytest.fixture(autouse=True)
def _clear_collection_physical_id_cache():
    """Bust the shared ``_collection_physical_id_cache`` around every test.

    ``resolve_physical_id`` (called with no ``db_resource``) now serves the
    collection physical id from the central L1/L2 ``@cached`` accelerator. These
    tests all probe the same ``(_CAT_ID, _COL_ID)`` key with different mocked
    DB results, so without an explicit invalidation a value cached by an earlier
    test leaks into a later one (e.g. the "absent" cases would see a stale hit
    instead of exercising the fresh-read/raise path).
    """
    from dynastore.modules.catalog.catalog_service import (
        _invalidate_collection_physical_id_cache,
    )

    _invalidate_collection_physical_id_cache(_CAT_ID, _COL_ID)
    yield
    _invalidate_collection_physical_id_cache(_CAT_ID, _COL_ID)


# ---------------------------------------------------------------------------
# 1. DDL contains physical_id column
# ---------------------------------------------------------------------------


def test_tenant_collections_ddl_has_physical_id_column():
    """TENANT_COLLECTIONS_DDL must declare a physical_id VARCHAR column."""
    from dynastore.modules.catalog.catalog_service import TENANT_COLLECTIONS_DDL

    assert "physical_id" in TENANT_COLLECTIONS_DDL, (
        "TENANT_COLLECTIONS_DDL is missing the physical_id column"
    )
    assert "UNIQUE" in TENANT_COLLECTIONS_DDL, (
        "TENANT_COLLECTIONS_DDL physical_id must have a UNIQUE constraint"
    )


def test_tenant_collections_ddl_physical_id_is_varchar():
    """physical_id column must be VARCHAR (not UUID) to match generate_physical_name output."""
    from dynastore.modules.catalog.catalog_service import TENANT_COLLECTIONS_DDL

    # find the physical_id line and confirm it says VARCHAR
    match = re.search(r"physical_id\s+(\w+)", TENANT_COLLECTIONS_DDL)
    assert match is not None, "physical_id column definition not found in DDL"
    assert match.group(1).upper() in ("VARCHAR", "TEXT"), (
        f"Expected VARCHAR/TEXT for physical_id, got {match.group(1)}"
    )


# ---------------------------------------------------------------------------
# 2. generate_physical_name("c") produces the right format
# ---------------------------------------------------------------------------


def test_generate_physical_name_c_prefix_format():
    """generate_physical_name('c') returns a token matching 'c_<8chars>'.

    The helper appends the "_" separator itself, so the collection mint passes
    a bare "c" prefix (not "c_") — yielding a single-underscore "c_<suffix>",
    consistent with the catalog "s_<suffix>" convention.
    """
    from dynastore.modules.catalog.catalog_service import generate_physical_name

    name = generate_physical_name("c")
    # prefix is "c", separator "_", 8 base36 chars
    assert name.startswith("c_"), f"Expected 'c_' prefix, got: {name!r}"
    assert not name.startswith("c__"), f"Double underscore regression: {name!r}"
    suffix = name[2:]
    assert len(suffix) == 8, f"Expected 8-char suffix, got {len(suffix)}: {name!r}"
    assert re.match(r"^[0-9a-z]+$", suffix), (
        f"Suffix must be base36 [0-9a-z]: {name!r}"
    )


def test_generate_physical_name_c_prefix_is_unique():
    """Two successive generate_physical_name('c') calls should not collide."""
    from dynastore.modules.catalog.catalog_service import generate_physical_name

    names = {generate_physical_name("c") for _ in range(50)}
    assert len(names) == 50, "Unexpected collision in generate_physical_name('c')"


# ---------------------------------------------------------------------------
# 3. resolve_physical_id — registry column path (new rows)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_physical_id_reads_registry_column_when_set():
    """When collections.physical_id is set, resolve_physical_id returns it
    without falling back to resolve_physical_table."""
    from dynastore.modules.catalog.catalog_service import CatalogService

    svc = CatalogService.__new__(CatalogService)
    # Give the service a fake engine so _resolve_collection_physical_id_db
    # doesn't short-circuit on the engine=None guard.
    svc.engine = MagicMock()

    with patch.object(
        CatalogService,
        "_resolve_collection_physical_id_db",
        new=AsyncMock(return_value=_PHYS_ID),
    ), patch.object(
        CatalogService,
        "resolve_physical_table",
        new=AsyncMock(return_value="should_not_be_called"),
    ) as mock_table:
        result = await svc.resolve_physical_id(_CAT_ID, _COL_ID)

    assert result == _PHYS_ID
    mock_table.assert_not_awaited()


@pytest.mark.asyncio
async def test_resolve_physical_id_raises_when_registry_column_null():
    """When collections.physical_id is NULL, resolve_physical_id must raise.

    The JSONB fallback to physical_table has been retired.  A NULL physical_id
    means the collection row is absent or corrupt — the caller must see an error,
    not a silent None, so it can diagnose the state rather than silently
    operating on the wrong table.
    """
    from dynastore.modules.catalog.catalog_service import CatalogService

    svc = CatalogService.__new__(CatalogService)
    svc.engine = MagicMock()

    with patch.object(
        CatalogService,
        "_resolve_collection_physical_id_db",
        new=AsyncMock(return_value=None),
    ):
        with pytest.raises(ValueError, match=_COL_ID):
            await svc.resolve_physical_id(_CAT_ID, _COL_ID)


@pytest.mark.asyncio
async def test_resolve_physical_id_no_engine_raises():
    """With no engine, _resolve_collection_physical_id_db returns None
    immediately and resolve_physical_id must raise (allow_missing=False default)."""
    from dynastore.modules.catalog.catalog_service import CatalogService

    svc = CatalogService.__new__(CatalogService)
    # No engine set — _resolve_collection_physical_id_db returns None
    # without raising, then resolve_physical_id raises (no fallback).

    with pytest.raises(ValueError, match=_COL_ID):
        await svc.resolve_physical_id(_CAT_ID, _COL_ID)


@pytest.mark.asyncio
async def test_resolve_physical_id_collection_raises_when_absent():
    """When the registry column returns None, allow_missing=False (default)
    must raise ValueError.  There is no JSONB fallback."""
    from dynastore.modules.catalog.catalog_service import CatalogService

    svc = CatalogService.__new__(CatalogService)
    svc.engine = MagicMock()

    with patch.object(
        CatalogService,
        "_resolve_collection_physical_id_db",
        new=AsyncMock(return_value=None),
    ):
        with pytest.raises(ValueError, match=_COL_ID):
            await svc.resolve_physical_id(_CAT_ID, _COL_ID)


@pytest.mark.asyncio
async def test_resolve_physical_id_allow_missing_returns_none_when_absent():
    """allow_missing=True returns None instead of raising."""
    from dynastore.modules.catalog.catalog_service import CatalogService

    svc = CatalogService.__new__(CatalogService)
    svc.engine = MagicMock()

    with patch.object(
        CatalogService,
        "_resolve_collection_physical_id_db",
        new=AsyncMock(return_value=None),
    ):
        result = await svc.resolve_physical_id(_CAT_ID, _COL_ID, allow_missing=True)

    assert result is None


# ---------------------------------------------------------------------------
# 4. physical_id NOT exposed on the public Collection model
# ---------------------------------------------------------------------------


def test_physical_id_not_in_collection_model_fields():
    """physical_id must not appear as a declared Pydantic field on Collection
    (it is in get_internal_columns, so stray copies are stripped)."""
    from dynastore.models.shared_models import Collection

    assert "physical_id" not in Collection.model_fields, (
        "physical_id must not be a declared field on the public Collection model"
    )


def test_physical_id_in_base_metadata_internal_columns():
    """BaseMetadata.get_internal_columns() must list physical_id so it is
    filtered from any external metadata dump."""
    from dynastore.models.shared_models import BaseMetadata

    assert "physical_id" in BaseMetadata.get_internal_columns(), (
        "physical_id missing from get_internal_columns()"
    )


# ---------------------------------------------------------------------------
# 5. create_collection INSERT SQL includes physical_id
# ---------------------------------------------------------------------------


def test_create_collection_insert_includes_physical_id():
    """The INSERT SQL in create_collection must carry physical_id in both the
    column list and the VALUES clause."""
    import ast
    import inspect
    from dynastore.modules.catalog.collection_service import CollectionService

    src = inspect.getsource(CollectionService.create_collection)
    # Check both the column list and parameter name appear in the source
    assert "physical_id" in src, (
        "create_collection source must mention physical_id"
    )
    assert "collection_physical_id" in src, (
        "create_collection source must mint collection_physical_id"
    )
