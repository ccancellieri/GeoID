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

"""Tests for resolve_collection_ids method and migration helpers (Issue #2430).

The resolve_collection_ids method resolves a collection ID (either external
or internal form) to a ResolvedCollectionIds model containing both forms.
This is the canonical resolution point for config persistence to ensure
configs are keyed on immutable internal IDs.

Also covers scripts/migrate_collection_configs_to_internal_id.py, especially
the deployment-window collision case where both an (external_id, ref_key) row
and an (internal_id, ref_key) row coexist.
"""

from typing import Any

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.models.resolved_ids import ResolvedCollectionIds


class TestResolveCollectionIds:
    """Tests for CollectionService.resolve_collection_ids."""

    @pytest.fixture
    def mock_collection_service(self):
        """Mock CollectionService for testing."""
        from dynastore.modules.catalog.collection_service import CollectionService
        svc = CollectionService(engine=MagicMock())
        return svc

    @pytest.mark.asyncio
    async def test_resolve_from_external_id(self, mock_collection_service):
        """Resolve external_id to get both internal and external IDs."""
        # Mock the catalog resolution
        mock_catalogs = MagicMock()
        mock_catalogs.resolve_catalog_id = AsyncMock(return_value="c_internal123")

        # Patch get_protocol at the location where it's imported inside the method
        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_catalogs
        ):
            # Mock the collection resolution
            with patch.object(
                mock_collection_service,
                "resolve_collection_id",
                AsyncMock(return_value="col_abc123xyz4567")
            ):
                result = await mock_collection_service.resolve_collection_ids(
                    "catalog_external", "my_collection"
                )

        assert result.id == "col_abc123xyz4567"
        assert result.external_id == "my_collection"
        assert result.catalog_id == "c_internal123"

    @pytest.mark.asyncio
    async def test_resolve_from_internal_id(self, mock_collection_service):
        """Resolve internal_id to get both internal and external IDs."""
        # Mock the catalog resolution
        mock_catalogs = MagicMock()
        mock_catalogs.resolve_catalog_id = AsyncMock(return_value="c_internal123")

        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_catalogs
        ):
            # Mock the collection resolution
            with patch.object(
                mock_collection_service,
                "resolve_collection_external_id",
                AsyncMock(return_value="my_collection")
            ):
                # Use a valid internal ID format: col_ + 13 chars from [2-9a-x]
                result = await mock_collection_service.resolve_collection_ids(
                    "catalog_external", "col_tooimv7odhd9k"
                )

        assert result.id == "col_tooimv7odhd9k"
        assert result.external_id == "my_collection"
        assert result.catalog_id == "c_internal123"

    @pytest.mark.asyncio
    async def test_resolve_missing_external_id_raises(self, mock_collection_service):
        """Resolving missing external_id raises ValueError when allow_missing=False."""
        mock_catalogs = MagicMock()
        mock_catalogs.resolve_catalog_id = AsyncMock(return_value="c_internal123")

        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_catalogs
        ):
            with patch.object(
                mock_collection_service,
                "resolve_collection_id",
                AsyncMock(return_value=None)
            ):
                with pytest.raises(ValueError, match="not found"):
                    await mock_collection_service.resolve_collection_ids(
                        "catalog_external", "missing_collection", allow_missing=False
                    )

    @pytest.mark.asyncio
    async def test_resolve_missing_with_allow_missing(self, mock_collection_service):
        """Resolving missing ID with allow_missing=True returns original ID for both fields."""
        mock_catalogs = MagicMock()
        mock_catalogs.resolve_catalog_id = AsyncMock(return_value="c_internal123")

        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_catalogs
        ):
            with patch.object(
                mock_collection_service,
                "resolve_collection_id",
                AsyncMock(return_value=None)
            ):
                result = await mock_collection_service.resolve_collection_ids(
                    "catalog_external", "missing_collection", allow_missing=True
                )

        # When not found, both id and external_id are set to the input
        assert result.id == "missing_collection"
        assert result.external_id == "missing_collection"


class TestResolvedCollectionIdsModel:
    """Tests for the ResolvedCollectionIds Pydantic model."""

    def test_model_creation(self):
        """Test creating a ResolvedCollectionIds model."""
        resolved = ResolvedCollectionIds(
            id="col_abc123xyz4567",
            external_id="my_collection",
            catalog_id="c_xyz789"
        )
        assert resolved.id == "col_abc123xyz4567"
        assert resolved.external_id == "my_collection"
        assert resolved.catalog_id == "c_xyz789"

    def test_model_serialization(self):
        """Test serializing a ResolvedCollectionIds model."""
        resolved = ResolvedCollectionIds(
            id="col_abc123xyz4567",
            external_id="my_collection",
            catalog_id="c_xyz789"
        )
        data = resolved.model_dump()
        assert data["id"] == "col_abc123xyz4567"
        assert data["external_id"] == "my_collection"
        assert data["catalog_id"] == "c_xyz789"


# ---------------------------------------------------------------------------
# Migration script helpers (scripts/migrate_collection_configs_to_internal_id)
# ---------------------------------------------------------------------------


def _make_managed_transaction_mock(mock_conn: Any) -> MagicMock:
    """Return a mock that behaves as an async context manager when called.

    Usage: patch(..., new=_make_managed_transaction_mock(mock_conn))
    Then ``async with managed_transaction(engine) as conn:`` will bind
    ``conn`` to ``mock_conn``.
    """
    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_cm.__aexit__ = AsyncMock(return_value=None)
    return MagicMock(return_value=mock_cm)


class TestMigrateCatalogSchema:
    """Unit tests for migrate_catalog_schema (Issue #2430 migration script)."""

    @pytest.mark.asyncio
    async def test_already_internal_skipped(self):
        """Rows already keyed on internal_id are counted as already_internal, not migrated."""
        from scripts.migrate_collection_configs_to_internal_id import migrate_catalog_schema

        engine = MagicMock()
        schema = "c_testschema"
        mock_conn = AsyncMock()
        # A row whose collection_id is already in internal format (col_<13 chars>)
        internal_row = {
            "collection_id": "col_tooimv7odhd9k",
            "ref_key": "some.config",
            "class_key": "some.config",
        }

        with patch(
            "scripts.migrate_collection_configs_to_internal_id.managed_transaction",
            new=_make_managed_transaction_mock(mock_conn),
        ):
            with patch(
                "scripts.migrate_collection_configs_to_internal_id.get_collection_configs",
                new=AsyncMock(return_value=[internal_row]),
            ):
                stats = await migrate_catalog_schema(engine, schema, dry_run=False)

        assert stats["already_internal"] == 1
        assert stats["migrated"] == 0
        assert stats["collision_deleted"] == 0

    @pytest.mark.asyncio
    async def test_normal_migration(self):
        """An external_id row with no collision is updated to internal_id."""
        from scripts.migrate_collection_configs_to_internal_id import migrate_catalog_schema

        engine = MagicMock()
        schema = "c_testschema"
        mock_conn = AsyncMock()
        ext_row = {
            "collection_id": "my_collection",
            "ref_key": "some.config",
            "class_key": "some.config",
        }
        executed_sqls: list = []

        class _MockDQLQuery:
            def __init__(self, sql, *, result_handler):
                self._sql = sql

            async def execute(self, conn, **kwargs):
                executed_sqls.append(self._sql)
                return 1

        with patch(
            "scripts.migrate_collection_configs_to_internal_id.managed_transaction",
            new=_make_managed_transaction_mock(mock_conn),
        ):
            with patch(
                "scripts.migrate_collection_configs_to_internal_id.get_collection_configs",
                new=AsyncMock(return_value=[ext_row]),
            ):
                with patch(
                    "scripts.migrate_collection_configs_to_internal_id.get_internal_id_for_external",
                    new=AsyncMock(return_value="col_tooimv7odhd9k"),
                ):
                    with patch(
                        "scripts.migrate_collection_configs_to_internal_id.internal_id_row_exists",
                        new=AsyncMock(return_value=False),
                    ):
                        with patch(
                            "scripts.migrate_collection_configs_to_internal_id.DQLQuery",
                            side_effect=_MockDQLQuery,
                        ):
                            stats = await migrate_catalog_schema(engine, schema, dry_run=False)

        assert stats["migrated"] == 1
        assert stats["collision_deleted"] == 0
        assert any("UPDATE" in sql for sql in executed_sqls), f"expected UPDATE: {executed_sqls}"

    @pytest.mark.asyncio
    async def test_collision_deletes_stale_row(self):
        """Deployment-window case: (internal_id, ref_key) already exists.

        The stale (external_id, ref_key) row must be DELETED rather than updated,
        to avoid a PK collision.  The event is counted under collision_deleted, not
        migrated, and a WARNING is logged.
        """
        from scripts.migrate_collection_configs_to_internal_id import migrate_catalog_schema

        engine = MagicMock()
        schema = "c_testschema"
        mock_conn = AsyncMock()
        ext_row = {
            "collection_id": "my_collection",
            "ref_key": "some.config",
            "class_key": "some.config",
        }
        executed_sqls: list = []

        class _MockDQLQuery:
            def __init__(self, sql, *, result_handler):
                self._sql = sql

            async def execute(self, conn, **kwargs):
                executed_sqls.append(self._sql)
                return 1

        with patch(
            "scripts.migrate_collection_configs_to_internal_id.managed_transaction",
            new=_make_managed_transaction_mock(mock_conn),
        ):
            with patch(
                "scripts.migrate_collection_configs_to_internal_id.get_collection_configs",
                new=AsyncMock(return_value=[ext_row]),
            ):
                with patch(
                    "scripts.migrate_collection_configs_to_internal_id.get_internal_id_for_external",
                    new=AsyncMock(return_value="col_tooimv7odhd9k"),
                ):
                    # Collision: the internal_id row already exists
                    with patch(
                        "scripts.migrate_collection_configs_to_internal_id.internal_id_row_exists",
                        new=AsyncMock(return_value=True),
                    ):
                        with patch(
                            "scripts.migrate_collection_configs_to_internal_id.DQLQuery",
                            side_effect=_MockDQLQuery,
                        ):
                            stats = await migrate_catalog_schema(engine, schema, dry_run=False)

        # Collision path: stale external row deleted, NOT updated
        assert stats["collision_deleted"] == 1, f"expected collision_deleted=1, got {stats}"
        assert stats["migrated"] == 0
        # Confirm a DELETE was issued and no UPDATE
        delete_issued = any("DELETE" in sql for sql in executed_sqls)
        update_issued = any("UPDATE" in sql for sql in executed_sqls)
        assert delete_issued, f"expected a DELETE statement, got: {executed_sqls}"
        assert not update_issued, f"unexpected UPDATE statement: {executed_sqls}"

    @pytest.mark.asyncio
    async def test_dry_run_collision_no_db_write(self):
        """Dry-run: collision case is detected and counted but no DB write occurs."""
        from scripts.migrate_collection_configs_to_internal_id import migrate_catalog_schema

        engine = MagicMock()
        schema = "c_testschema"
        mock_conn = AsyncMock()
        ext_row = {
            "collection_id": "my_collection",
            "ref_key": "some.config",
            "class_key": "some.config",
        }
        executed_sqls: list = []

        class _MockDQLQuery:
            def __init__(self, sql, *, result_handler):
                self._sql = sql

            async def execute(self, conn, **kwargs):
                executed_sqls.append(self._sql)
                return 1

        with patch(
            "scripts.migrate_collection_configs_to_internal_id.managed_transaction",
            new=_make_managed_transaction_mock(mock_conn),
        ):
            with patch(
                "scripts.migrate_collection_configs_to_internal_id.get_collection_configs",
                new=AsyncMock(return_value=[ext_row]),
            ):
                with patch(
                    "scripts.migrate_collection_configs_to_internal_id.get_internal_id_for_external",
                    new=AsyncMock(return_value="col_tooimv7odhd9k"),
                ):
                    with patch(
                        "scripts.migrate_collection_configs_to_internal_id.internal_id_row_exists",
                        new=AsyncMock(return_value=True),
                    ):
                        with patch(
                            "scripts.migrate_collection_configs_to_internal_id.DQLQuery",
                            side_effect=_MockDQLQuery,
                        ):
                            stats = await migrate_catalog_schema(engine, schema, dry_run=True)

        # Dry-run: counted but no SQL write issued
        assert stats["collision_deleted"] == 1
        assert stats["migrated"] == 0
        # No DELETE or UPDATE should have been executed in dry-run mode
        assert not executed_sqls, f"unexpected DB writes in dry-run: {executed_sqls}"

    @pytest.mark.asyncio
    async def test_idempotency_all_already_internal(self):
        """Re-running after a complete migration is a no-op (all rows already internal)."""
        from scripts.migrate_collection_configs_to_internal_id import migrate_catalog_schema

        engine = MagicMock()
        schema = "c_testschema"
        mock_conn = AsyncMock()
        # Both IDs must be in the internal format: col_ + 13 chars from [2-9a-x].
        rows = [
            {"collection_id": "col_tooimv7odhd9k", "ref_key": "cfg.a", "class_key": "cfg.a"},
            {"collection_id": "col_abcdefghi2345", "ref_key": "cfg.b", "class_key": "cfg.b"},
        ]

        with patch(
            "scripts.migrate_collection_configs_to_internal_id.managed_transaction",
            new=_make_managed_transaction_mock(mock_conn),
        ):
            with patch(
                "scripts.migrate_collection_configs_to_internal_id.get_collection_configs",
                new=AsyncMock(return_value=rows),
            ):
                stats = await migrate_catalog_schema(engine, schema, dry_run=False)

        assert stats["already_internal"] == 2
        assert stats["migrated"] == 0
        assert stats["collision_deleted"] == 0
        assert stats["orphaned"] == 0

    @pytest.mark.asyncio
    async def test_orphaned_external_id_skipped(self):
        """An external_id not found in collections table is counted as orphaned (no data loss)."""
        from scripts.migrate_collection_configs_to_internal_id import migrate_catalog_schema

        engine = MagicMock()
        schema = "c_testschema"
        mock_conn = AsyncMock()
        ext_row = {
            "collection_id": "ghost_collection",
            "ref_key": "some.config",
            "class_key": "some.config",
        }

        with patch(
            "scripts.migrate_collection_configs_to_internal_id.managed_transaction",
            new=_make_managed_transaction_mock(mock_conn),
        ):
            with patch(
                "scripts.migrate_collection_configs_to_internal_id.get_collection_configs",
                new=AsyncMock(return_value=[ext_row]),
            ):
                with patch(
                    "scripts.migrate_collection_configs_to_internal_id.get_internal_id_for_external",
                    new=AsyncMock(return_value=None),
                ):
                    stats = await migrate_catalog_schema(engine, schema, dry_run=False)

        assert stats["orphaned"] == 1
        assert stats["migrated"] == 0
        assert stats["collision_deleted"] == 0
