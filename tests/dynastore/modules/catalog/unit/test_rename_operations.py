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

"""Unit tests for the physical_id rename feature.

Covers:
- validate_asset_id: charset / length / reserved-word rules
- FK definition changes: ON UPDATE CASCADE present in DDL literals
- RenameRequest / RenameResponse model round-trips
- rename_catalog service: happy path, 404, 409 (incl. tombstone), cache bust,
  no tenant-table writes
- rename_collection service: happy path (only collections row), 404, 409,
  partition key untouched, assets never written
- rename_asset service: happy path, 404, 409, catalog-tier (collection_id=None)
- Route-registration smoke-tests: correct method and path suffix

These are pure-unit tests — no live DB.  All DB calls are mocked.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator, List
from unittest.mock import AsyncMock, MagicMock

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _noop_txn(engine, **kw) -> AsyncGenerator:
    """Minimal managed_transaction shim: yields a mock connection."""
    conn = AsyncMock()
    yield conn


def _make_dql_factory(seq: List):
    """Return a DQLQuery class whose execute() draws from seq in order."""
    idx = {"v": 0}

    class _FakeDQL:
        def __init__(self, *a, **kw):
            pass

        async def execute(self, conn, **kw):
            v = seq[idx["v"]] if idx["v"] < len(seq) else None
            idx["v"] += 1
            return v

    return _FakeDQL


def _make_ddl_factory(calls_list: List[str]):
    """Return a DDLQuery class that records SQL strings into calls_list."""

    class _FakeDDL:
        def __init__(self, sql, **kw):
            self._sql = sql

        async def execute(self, conn, **kw):
            calls_list.append(self._sql)
            return None

    return _FakeDDL


# ===========================================================================
# 1. validate_asset_id
# ===========================================================================


class TestValidateAssetId:
    def test_simple_lowercase(self):
        from dynastore.tools.db import validate_asset_id

        assert validate_asset_id("my_asset") == "my_asset"

    def test_mixed_case_preserved(self):
        from dynastore.tools.db import validate_asset_id

        assert validate_asset_id("MyAsset_v2") == "MyAsset_v2"

    def test_dots_and_hyphens(self):
        from dynastore.tools.db import validate_asset_id

        assert validate_asset_id("asset.v1-prod") == "asset.v1-prod"

    def test_empty_raises(self):
        from dynastore.tools.db import validate_asset_id, InvalidIdentifierError

        with pytest.raises(InvalidIdentifierError):
            validate_asset_id("")

    def test_too_long_raises(self):
        from dynastore.tools.db import validate_asset_id, InvalidIdentifierError

        with pytest.raises(InvalidIdentifierError):
            validate_asset_id("a" * 256)

    def test_reserved_word_raises(self):
        from dynastore.tools.db import validate_asset_id, InvalidIdentifierError

        with pytest.raises(InvalidIdentifierError):
            validate_asset_id("select")

    def test_template_placeholder_raises(self):
        from dynastore.tools.db import validate_asset_id, InvalidIdentifierError

        with pytest.raises(InvalidIdentifierError):
            validate_asset_id("{{asset}}")

    def test_space_raises(self):
        from dynastore.tools.db import validate_asset_id, InvalidIdentifierError

        with pytest.raises(InvalidIdentifierError):
            validate_asset_id("bad asset")


# ===========================================================================
# 2. FK DDL — ON UPDATE CASCADE present
# ===========================================================================


class TestFKDefinitions:
    def test_catalog_core_ddl_has_on_update_cascade(self):
        from dynastore.modules.catalog.db_init.core_tables import CATALOG_METADATA_CORE_DDL

        assert "ON UPDATE CASCADE" in CATALOG_METADATA_CORE_DDL, (
            "catalog_core FK must include ON UPDATE CASCADE for rename to work"
        )

    def test_catalog_stac_ddl_has_on_update_cascade(self):
        from dynastore.modules.stac.db_init.stac_tables import CATALOG_METADATA_STAC_DDL

        assert "ON UPDATE CASCADE" in CATALOG_METADATA_STAC_DDL, (
            "catalog_stac FK must include ON UPDATE CASCADE for rename to work"
        )


# ===========================================================================
# 3. RenameRequest / RenameResponse models
# ===========================================================================


class TestRenameModels:
    def test_rename_request_round_trip(self):
        from dynastore.extensions.ogc_models_shared import RenameRequest

        r = RenameRequest(new_id="foo_bar")
        assert r.new_id == "foo_bar"

    def test_rename_response_fields(self):
        from dynastore.extensions.ogc_models_shared import RenameResponse

        resp = RenameResponse(
            old_id="old",
            new_id="new",
            level="catalog",
            warnings=["es_reindex_required"],
            reindex_required=True,
            iam_manual_update_required=True,
        )
        d = resp.model_dump()
        assert d["old_id"] == "old"
        assert d["new_id"] == "new"
        assert d["level"] == "catalog"
        assert d["reindex_required"] is True
        assert d["iam_manual_update_required"] is True

    def test_rename_response_asset_level(self):
        from dynastore.extensions.ogc_models_shared import RenameResponse

        resp = RenameResponse(
            old_id="a1",
            new_id="a2",
            level="asset",
            reindex_required=True,
            iam_manual_update_required=False,
        )
        assert resp.level == "asset"
        assert resp.iam_manual_update_required is False

    def test_rename_response_noop_default_warnings(self):
        from dynastore.extensions.ogc_models_shared import RenameResponse

        resp = RenameResponse(
            old_id="x",
            new_id="x",
            level="collection",
            reindex_required=False,
            iam_manual_update_required=False,
        )
        assert resp.warnings == []


# ===========================================================================
# 4. rename_catalog — CatalogService unit tests
# ===========================================================================


class TestRenameCatalog:
    """Pure-unit tests for CatalogService.rename_catalog."""

    def _make_service(self):
        from dynastore.modules.catalog.catalog_service import CatalogService

        svc = CatalogService.__new__(CatalogService)
        svc.engine = MagicMock()
        return svc

    @pytest.mark.asyncio
    async def test_happy_path(self, monkeypatch):
        svc = self._make_service()

        # DQLQuery.execute sequence:
        # 1. pg_advisory_xact_lock → None
        # 2. existence check → 'old_catalog'
        # 3. collision check → None
        dql_seq = [None, "old_catalog", None]
        ddl_calls: List[str] = []
        invalidated: List[str] = []

        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.managed_transaction",
            _noop_txn,
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.DQLQuery",
            _make_dql_factory(dql_seq),
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.DDLQuery",
            _make_ddl_factory(ddl_calls),
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service._invalidate_catalog_model_cache",
            lambda cid: invalidated.append(cid),
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.get_catalog_engine",
            lambda db=None: db,
        )

        result = await svc.rename_catalog("old_catalog", "new_catalog")

        assert result.old_id == "old_catalog"
        assert result.new_id == "new_catalog"
        assert result.level == "catalog"
        assert result.reindex_required is True
        assert result.iam_manual_update_required is True
        assert "es_reindex_required" in result.warnings
        assert "iam_manual_update_required" in result.warnings
        assert "old_catalog" in invalidated
        assert "new_catalog" in invalidated

    @pytest.mark.asyncio
    async def test_noop_same_id(self, monkeypatch):
        """When old == new (after normalization), return immediately without opening a txn."""
        svc = self._make_service()
        txn_entered = []

        @asynccontextmanager
        async def _no_txn(*a, **kw):
            txn_entered.append(True)
            raise AssertionError("Should not open a transaction for no-op rename")
            yield  # make it a generator (unreachable)

        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.managed_transaction", _no_txn
        )

        result = await svc.rename_catalog("mycatalog", "mycatalog")

        assert result.old_id == "mycatalog"
        assert result.new_id == "mycatalog"
        assert result.reindex_required is False
        assert txn_entered == []

    @pytest.mark.asyncio
    async def test_404_not_found(self, monkeypatch):
        svc = self._make_service()
        # lock → None; existence → None (not found)
        dql_seq = [None, None]

        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.managed_transaction",
            _noop_txn,
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.DQLQuery",
            _make_dql_factory(dql_seq),
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.get_catalog_engine",
            lambda db=None: db,
        )

        with pytest.raises(ValueError, match="not found"):
            await svc.rename_catalog("ghost", "new_name")

    @pytest.mark.asyncio
    async def test_409_collision(self, monkeypatch):
        from dynastore.modules.catalog.catalog_service import _CatalogRenameConflictError

        svc = self._make_service()
        # lock → None; existence → 'old'; collision → 'taken'
        dql_seq = [None, "old", "taken"]

        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.managed_transaction",
            _noop_txn,
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.DQLQuery",
            _make_dql_factory(dql_seq),
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.get_catalog_engine",
            lambda db=None: db,
        )

        with pytest.raises(_CatalogRenameConflictError) as exc_info:
            await svc.rename_catalog("old", "taken")
        assert exc_info.value.new_id == "taken"

    @pytest.mark.asyncio
    async def test_409_includes_tombstoned(self, monkeypatch):
        """Collision query has no deleted_at filter — tombstoned ids are also blocked."""
        from dynastore.modules.catalog.catalog_service import _CatalogRenameConflictError

        svc = self._make_service()
        dql_seq = [None, "old_cat", "tombstoned_cat"]

        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.managed_transaction",
            _noop_txn,
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.DQLQuery",
            _make_dql_factory(dql_seq),
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.get_catalog_engine",
            lambda db=None: db,
        )

        with pytest.raises(_CatalogRenameConflictError):
            await svc.rename_catalog("old_cat", "tombstoned_cat")

    @pytest.mark.asyncio
    async def test_both_ids_cache_busted(self, monkeypatch):
        svc = self._make_service()
        dql_seq = [None, "c1", None]
        ddl_calls: List[str] = []
        invalidated: List[str] = []

        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.managed_transaction",
            _noop_txn,
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.DQLQuery",
            _make_dql_factory(dql_seq),
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.DDLQuery",
            _make_ddl_factory(ddl_calls),
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service._invalidate_catalog_model_cache",
            lambda cid: invalidated.append(cid),
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.get_catalog_engine",
            lambda db=None: db,
        )

        await svc.rename_catalog("c1", "c2")

        assert set(invalidated) == {"c1", "c2"}

    @pytest.mark.asyncio
    async def test_no_tenant_table_writes(self, monkeypatch):
        """rename_catalog issues exactly ONE UPDATE (catalog.catalogs) and must
        not write to any per-tenant table (no SET catalog_id = :new_id against
        a tenant schema; no collections/assets/logs DDL)."""
        svc = self._make_service()
        # lock → None; existence → 'old'; collision → None
        dql_seq = [None, "old", None]
        ddl_calls: List[str] = []

        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.managed_transaction",
            _noop_txn,
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.DQLQuery",
            _make_dql_factory(dql_seq),
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.DDLQuery",
            _make_ddl_factory(ddl_calls),
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service._invalidate_catalog_model_cache",
            lambda cid: None,
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.catalog_service.get_catalog_engine",
            lambda db=None: db,
        )

        await svc.rename_catalog("old", "new")

        # Exactly one UPDATE — the registry row.
        assert len(ddl_calls) == 1, (
            f"Expected exactly 1 DDL call. Got {len(ddl_calls)}: {ddl_calls}"
        )
        assert "UPDATE catalog.catalogs" in ddl_calls[0], (
            f"The single DDL must target catalog.catalogs. Got: {ddl_calls[0]!r}"
        )
        # No tenant-schema writes of any kind.
        for sql in ddl_calls:
            assert "SET catalog_id = :new_id" not in sql, (
                f"Tenant re-key must NOT be issued. Got: {sql!r}"
            )
            assert ".assets" not in sql, (
                f"No tenant .assets write expected. Got: {sql!r}"
            )
            assert ".logs" not in sql, (
                f"No tenant .logs write expected. Got: {sql!r}"
            )


# ===========================================================================
# 5. rename_collection — CollectionService unit tests
# ===========================================================================


class TestRenameCollection:
    def _make_service(self):
        from dynastore.modules.catalog.collection_service import CollectionService

        svc = CollectionService.__new__(CollectionService)
        svc.engine = MagicMock()
        return svc

    @pytest.mark.asyncio
    async def test_happy_path_updates_only_collections_row(self, monkeypatch):
        """rename_collection issues exactly ONE UPDATE against {phys_schema}.collections
        (SET id = :new_id) and must not write to collection_core, collection_stac,
        or assets — those tables key on the immutable collection_physical_id."""
        svc = self._make_service()

        # _resolve_physical_schema is patched on the instance; only DQLQuery
        # calls inside managed_transaction count:
        # 1. pg_advisory_xact_lock → None
        # 2. existence check → "old_col"
        # 3. collision check → None
        dql_seq = [None, "old_col", None]
        ddl_calls: List[str] = []
        invalidated: List[tuple] = []

        async def _fake_resolve_schema(cat, db_resource=None):
            return "s_schema1"

        monkeypatch.setattr(svc, "_resolve_physical_schema", _fake_resolve_schema)

        monkeypatch.setattr(
            "dynastore.modules.catalog.collection_service.managed_transaction",
            _noop_txn,
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.collection_service.DQLQuery",
            _make_dql_factory(dql_seq),
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.collection_service.DDLQuery",
            _make_ddl_factory(ddl_calls),
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.collection_service._invalidate_collection_lifecycle_caches",
            lambda cat, col: invalidated.append((cat, col)),
        )

        result = await svc.rename_collection("cat1", "old_col", "new_col")

        assert result.old_id == "old_col"
        assert result.new_id == "new_col"
        assert result.level == "collection"
        assert result.reindex_required is True
        assert result.iam_manual_update_required is True

        # Exactly one UPDATE — the thin registry row only.
        assert len(ddl_calls) == 1, (
            f"Expected exactly 1 DDL call. Got {len(ddl_calls)}: {ddl_calls}"
        )
        assert '"s_schema1".collections' in ddl_calls[0], (
            f"The single DDL must target {{phys_schema}}.collections. Got: {ddl_calls[0]!r}"
        )
        assert "SET id = :new_id" in ddl_calls[0], (
            f"The UPDATE must SET id = :new_id. Got: {ddl_calls[0]!r}"
        )

        # Metadata tables and assets must NOT be written.
        for sql in ddl_calls:
            assert "collection_core" not in sql, (
                f"collection_core must NOT be written on rename. Got: {sql!r}"
            )
            assert "collection_stac" not in sql, (
                f"collection_stac must NOT be written on rename. Got: {sql!r}"
            )
            assert "assets" not in sql, (
                f"assets must NOT be written on collection rename. Got: {sql!r}"
            )

        # Both old and new collection_id must be cache-busted.
        assert ("cat1", "old_col") in invalidated
        assert ("cat1", "new_col") in invalidated

    @pytest.mark.asyncio
    async def test_partition_key_not_updated(self, monkeypatch):
        """No UPDATE should touch collection_physical_id or the assets table.

        The rename touches only the thin collections registry row (id label).
        Assets key on the immutable collection_physical_id partition key which
        must never be rewritten by a rename operation.
        """
        svc = self._make_service()

        dql_seq = [None, "col", None]
        ddl_calls: List[str] = []

        async def _fake_resolve_schema(cat, db_resource=None):
            return "s_schema1"

        monkeypatch.setattr(svc, "_resolve_physical_schema", _fake_resolve_schema)
        monkeypatch.setattr(
            "dynastore.modules.catalog.collection_service.managed_transaction",
            _noop_txn,
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.collection_service.DQLQuery",
            _make_dql_factory(dql_seq),
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.collection_service.DDLQuery",
            _make_ddl_factory(ddl_calls),
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.collection_service._invalidate_collection_lifecycle_caches",
            lambda *a: None,
        )

        await svc.rename_collection("cat1", "col", "col2")

        for sql in ddl_calls:
            assert "collection_physical_id" not in sql, (
                f"collection_physical_id (partition key) must NOT be updated: {sql!r}"
            )
            assert "assets" not in sql, (
                f"assets table must NOT be written on collection rename: {sql!r}"
            )

    @pytest.mark.asyncio
    async def test_404_not_found(self, monkeypatch):
        svc = self._make_service()
        # lock → None; existence → None (not found)
        dql_seq = [None, None]

        async def _fake_resolve_schema(cat, db_resource=None):
            return "s_schema1"

        monkeypatch.setattr(svc, "_resolve_physical_schema", _fake_resolve_schema)
        monkeypatch.setattr(
            "dynastore.modules.catalog.collection_service.managed_transaction",
            _noop_txn,
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.collection_service.DQLQuery",
            _make_dql_factory(dql_seq),
        )

        with pytest.raises(ValueError, match="not found"):
            await svc.rename_collection("cat1", "ghost", "new_col")

    @pytest.mark.asyncio
    async def test_409_collision(self, monkeypatch):
        from dynastore.modules.catalog.collection_service import CollectionRenameConflictError

        svc = self._make_service()
        # lock → None; existence → "col"; collision → "taken"
        dql_seq = [None, "col", "taken"]

        async def _fake_resolve_schema(cat, db_resource=None):
            return "s_schema1"

        monkeypatch.setattr(svc, "_resolve_physical_schema", _fake_resolve_schema)
        monkeypatch.setattr(
            "dynastore.modules.catalog.collection_service.managed_transaction",
            _noop_txn,
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.collection_service.DQLQuery",
            _make_dql_factory(dql_seq),
        )

        with pytest.raises(CollectionRenameConflictError) as exc_info:
            await svc.rename_collection("cat1", "col", "taken")
        assert exc_info.value.new_id == "taken"


# ===========================================================================
# 6. rename_asset — AssetService unit tests
# ===========================================================================


class TestRenameAsset:
    def _make_service(self):
        from dynastore.modules.catalog.asset_service import AssetService

        svc = AssetService.__new__(AssetService)
        svc.engine = MagicMock()
        # The cached get_asset method
        svc.get_asset_cached = MagicMock()
        svc.get_asset_cached.cache_invalidate = MagicMock()
        return svc

    @pytest.mark.asyncio
    async def test_happy_path_catalog_tier(self, monkeypatch):
        """Catalog-tier rename (collection_id=None) updates assets only.

        asset_references keys on the immutable physical_id so zero propagation
        is needed there — the rename is a one-column label change on assets.
        """
        svc = self._make_service()

        # _resolve_schema is patched on the instance; DQLQuery calls inside txn:
        # 1. pg_advisory_xact_lock → None
        # 2. existence check (no coll scope) → "old_asset"
        # 3. collision check → None
        dql_seq = [None, "old_asset", None]
        ddl_calls: List[str] = []

        async def _fake_resolve_schema(cat, conn):
            return "s_cat"

        monkeypatch.setattr(svc, "_resolve_schema", _fake_resolve_schema)
        monkeypatch.setattr(
            "dynastore.modules.catalog.asset_service.managed_transaction",
            _noop_txn,
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.asset_service.DQLQuery",
            _make_dql_factory(dql_seq),
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.asset_service.DDLQuery",
            _make_ddl_factory(ddl_calls),
        )

        result = await svc.rename_asset(
            catalog_id="cat1",
            asset_id="old_asset",
            new_id="new_asset",
            collection_id=None,
        )

        assert result.old_id == "old_asset"
        assert result.new_id == "new_asset"
        assert result.level == "asset"
        assert result.reindex_required is True
        assert result.iam_manual_update_required is False

        # Exactly one DDL statement: UPDATE assets.asset_id.
        assert any("assets" in s for s in ddl_calls), (
            f"Expected UPDATE on assets table. Got: {ddl_calls}"
        )
        # asset_references must NOT be touched — it keys on physical_id.
        assert not any("asset_references" in s for s in ddl_calls), (
            f"rename_asset must NOT UPDATE asset_references (physical_id key). Got: {ddl_calls}"
        )

        # Cache must be busted for both old and new id.
        assert svc.get_asset_cached.cache_invalidate.call_count == 2

    @pytest.mark.asyncio
    async def test_noop_same_id(self, monkeypatch):
        svc = self._make_service()
        txn_entered = []

        @asynccontextmanager
        async def _no_txn(*a, **kw):
            txn_entered.append(True)
            raise AssertionError("Should not open a transaction for no-op rename")
            yield  # unreachable

        monkeypatch.setattr(
            "dynastore.modules.catalog.asset_service.managed_transaction", _no_txn
        )

        result = await svc.rename_asset(
            catalog_id="cat1", asset_id="same", new_id="same"
        )
        assert result.old_id == "same"
        assert result.new_id == "same"
        assert result.reindex_required is False
        assert txn_entered == []

    @pytest.mark.asyncio
    async def test_404_not_found(self, monkeypatch):
        svc = self._make_service()
        # lock → None; existence → None
        dql_seq = [None, None]

        async def _fake_resolve_schema(cat, conn):
            return "s_cat"

        monkeypatch.setattr(svc, "_resolve_schema", _fake_resolve_schema)
        monkeypatch.setattr(
            "dynastore.modules.catalog.asset_service.managed_transaction",
            _noop_txn,
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.asset_service.DQLQuery",
            _make_dql_factory(dql_seq),
        )

        with pytest.raises(ValueError, match="not found"):
            await svc.rename_asset(
                catalog_id="cat1", asset_id="ghost", new_id="new_name"
            )

    @pytest.mark.asyncio
    async def test_409_conflict(self, monkeypatch):
        from dynastore.modules.catalog.asset_service import AssetRenameConflictError

        svc = self._make_service()
        # lock → None; existence → "old_a"; collision → "taken_a"
        dql_seq = [None, "old_a", "taken_a"]

        async def _fake_resolve_schema(cat, conn):
            return "s_cat"

        monkeypatch.setattr(svc, "_resolve_schema", _fake_resolve_schema)
        monkeypatch.setattr(
            "dynastore.modules.catalog.asset_service.managed_transaction",
            _noop_txn,
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.asset_service.DQLQuery",
            _make_dql_factory(dql_seq),
        )

        with pytest.raises(AssetRenameConflictError) as exc_info:
            await svc.rename_asset(
                catalog_id="cat1", asset_id="old_a", new_id="taken_a"
            )
        assert exc_info.value.new_id == "taken_a"

    @pytest.mark.asyncio
    async def test_collection_scoped_queries_include_collection_physical_id(self, monkeypatch):
        """When collection_id is given, existence and collision queries scope by
        collection_physical_id — the immutable partition key."""
        svc = self._make_service()

        # DQL sequence for collection-scoped rename:
        # 1. resolve collection physical_id → "phys_coll_1"
        # 2. pg_advisory_xact_lock → None
        # 3. existence check (scoped) → "old_asset"
        # 4. collision check (scoped) → None
        dql_seq = ["phys_coll_1", None, "old_asset", None]
        ddl_calls: List[str] = []
        captured_sqls: List[str] = []

        class _CaptureDQL:
            def __init__(self, sql, **kw):
                self._sql = sql

            async def execute(self, conn, **kw):
                captured_sqls.append(self._sql)
                idx = len(captured_sqls) - 1
                return dql_seq[idx] if idx < len(dql_seq) else None

        async def _fake_resolve_schema(cat, conn):
            return "s_cat"

        monkeypatch.setattr(svc, "_resolve_schema", _fake_resolve_schema)
        monkeypatch.setattr(
            "dynastore.modules.catalog.asset_service.managed_transaction",
            _noop_txn,
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.asset_service.DQLQuery",
            _CaptureDQL,
        )
        monkeypatch.setattr(
            "dynastore.modules.catalog.asset_service.DDLQuery",
            _make_ddl_factory(ddl_calls),
        )

        result = await svc.rename_asset(
            catalog_id="cat1",
            asset_id="old_asset",
            new_id="new_asset",
            collection_id="col1",
        )

        assert result.old_id == "old_asset"
        assert result.new_id == "new_asset"

        # The existence check (index 2 in captured_sqls) must scope by
        # collection_physical_id.
        existence_sql = captured_sqls[2]
        assert "collection_physical_id" in existence_sql, (
            f"Existence query must scope by collection_physical_id. SQL: {existence_sql!r}"
        )


# ===========================================================================
# 7. Route registration smoke-tests
# ===========================================================================


class TestRouteRegistration:
    """Check that rename routes are registered with the correct method and path.
    We inspect source code directly (no HTTP server required)."""

    def test_stac_catalog_rename_registered(self):
        """rename_stac_catalog must be in the STAC route table."""
        import inspect
        from dynastore.extensions.stac.stac_service import STACService

        src = inspect.getsource(STACService._register_routes)
        assert "{catalog_id}:rename" in src, (
            "STAC route table must contain the catalog rename path"
        )
        assert "rename_stac_catalog" in src

    def test_stac_collection_rename_registered(self):
        """rename_stac_collection must be in the STAC route table."""
        import inspect
        from dynastore.extensions.stac.stac_service import STACService

        src = inspect.getsource(STACService._register_routes)
        assert "{collection_id}:rename" in src, (
            "STAC route table must contain the collection rename path"
        )
        assert "rename_stac_collection" in src

    def test_asset_catalog_rename_registered(self):
        """rename_catalog_asset must be wired in AssetService._setup_routes."""
        import inspect
        from dynastore.extensions.assets.assets_service import AssetService

        src = inspect.getsource(AssetService._setup_routes)
        assert "rename_catalog_asset" in src
        assert "asset_id}:rename" in src

    def test_asset_collection_rename_registered(self):
        """rename_collection_asset must be wired in AssetService._setup_routes."""
        import inspect
        from dynastore.extensions.assets.assets_service import AssetService

        src = inspect.getsource(AssetService._setup_routes)
        assert "rename_collection_asset" in src
