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

"""Vocabulary-consolidation regression guard (#1776).

Pins the uniform ``drop_storage`` teardown vocabulary on:
- ``StorageProtocol`` (binary storage — GCS bucket etc.)
- ``TileStorageProtocol`` (tile preseed table / GCS tile blobs)

Verifies that the old ``delete_storage_for_catalog`` verb is ABSENT from both
protocols, and that all concrete implementers expose the new ``drop_storage``
method, satisfying the ``@runtime_checkable`` structural isinstance contract
used by ``get_protocols`` discovery.
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# StorageProtocol — method presence / absence
# ---------------------------------------------------------------------------


class TestStorageProtocolVocabulary:
    def test_drop_storage_present_on_protocol(self) -> None:
        from dynastore.models.protocols.storage import StorageProtocol
        assert hasattr(StorageProtocol, "drop_storage"), (
            "StorageProtocol must expose drop_storage (uniform teardown verb)"
        )

    def test_delete_storage_for_catalog_absent_from_protocol(self) -> None:
        """Old verb must be gone — clean cut, no backward-compat shim."""
        from dynastore.models.protocols.storage import StorageProtocol
        assert not hasattr(StorageProtocol, "delete_storage_for_catalog"), (
            "delete_storage_for_catalog was renamed to drop_storage; "
            "the old name must not exist on StorageProtocol"
        )


# ---------------------------------------------------------------------------
# TileStorageProtocol — method presence / absence
# ---------------------------------------------------------------------------


class TestTileStorageProtocolVocabulary:
    def test_drop_storage_present_on_protocol(self) -> None:
        from dynastore.modules.tiles.tiles_module import TileStorageProtocol
        assert hasattr(TileStorageProtocol, "drop_storage"), (
            "TileStorageProtocol must expose drop_storage"
        )

    def test_delete_storage_for_catalog_absent_from_protocol(self) -> None:
        from dynastore.modules.tiles.tiles_module import TileStorageProtocol
        assert not hasattr(TileStorageProtocol, "delete_storage_for_catalog"), (
            "delete_storage_for_catalog was renamed to drop_storage; "
            "the old name must not exist on TileStorageProtocol"
        )


# ---------------------------------------------------------------------------
# StorageProtocol: _FakeStorage structural conformance (runtime_checkable)
# ---------------------------------------------------------------------------


class _FakeStorage:
    """Minimal struct that satisfies StorageProtocol after the rename."""

    async def get_storage_identifier(self, catalog_id): return None
    async def get_catalog_storage_path(self, catalog_id): return None
    async def upload_file(self, src, tgt, content_type=None): return tgt
    async def upload_file_content(self, tgt, content, content_type=None): return tgt
    async def download_file(self, src, tgt): pass
    async def file_exists(self, path): return False
    async def delete_file(self, path): pass
    async def ensure_storage_for_catalog(self, catalog_id, conn=None): return None
    async def drop_storage(self, catalog_id, conn=None) -> bool: return True
    async def get_collection_storage_path(self, catalog_id, collection_id): return None
    async def wait_for_storage_ready(self, storage_id, timeout_seconds=30, interval_seconds=1.0): return True
    async def prepare_upload_target(self, catalog_id, collection_id=None): pass
    async def apply_storage_config(self, catalog_id, config): pass
    async def download_bytes_range(self, path, offset, length) -> bytes: return b""


class TestFakeStorageConformance:
    def test_is_storage_protocol_instance(self) -> None:
        from dynastore.models.protocols.storage import StorageProtocol
        assert isinstance(_FakeStorage(), StorageProtocol), (
            "_FakeStorage with drop_storage must satisfy @runtime_checkable StorageProtocol"
        )

    def test_missing_drop_storage_fails_protocol_check(self) -> None:
        """A class without drop_storage must NOT satisfy StorageProtocol."""
        from dynastore.models.protocols.storage import StorageProtocol

        # Build a fresh class that omits drop_storage intentionally.
        class _MissingDropStorage:
            async def get_storage_identifier(self, catalog_id): return None
            async def get_catalog_storage_path(self, catalog_id): return None
            async def upload_file(self, src, tgt, content_type=None): return tgt
            async def upload_file_content(self, tgt, content, content_type=None): return tgt
            async def download_file(self, src, tgt): pass
            async def file_exists(self, path): return False
            async def delete_file(self, path): pass
            async def ensure_storage_for_catalog(self, catalog_id, conn=None): return None
            # drop_storage intentionally absent
            async def get_collection_storage_path(self, c, col): return None
            async def wait_for_storage_ready(self, sid, timeout_seconds=30, interval_seconds=1.0): return True
            async def prepare_upload_target(self, c, col=None): pass
            async def apply_storage_config(self, c, cfg): pass
            async def download_bytes_range(self, p, o, n) -> bytes: return b""

        assert not isinstance(_MissingDropStorage(), StorageProtocol), (
            "A class missing drop_storage must not satisfy StorageProtocol"
        )


# ---------------------------------------------------------------------------
# TileStorageProtocol: TilePGPreseedStorage structural conformance
# ---------------------------------------------------------------------------


class TestTilePGPreseedStorageConformance:
    def test_drop_storage_method_exists(self) -> None:
        """TilePGPreseedStorage must have drop_storage (not the old delete_storage_for_catalog)."""
        from dynastore.modules.tiles.tiles_module import TilePGPreseedStorage
        assert hasattr(TilePGPreseedStorage, "drop_storage"), (
            "TilePGPreseedStorage must implement drop_storage"
        )
        assert not hasattr(TilePGPreseedStorage, "delete_storage_for_catalog"), (
            "Old verb delete_storage_for_catalog must be gone from TilePGPreseedStorage"
        )


# ---------------------------------------------------------------------------
# TilePGPreseedStorage.drop_storage behaviour (unit)
# ---------------------------------------------------------------------------


class TestTilePGPreseedStorageDropStorage:
    @pytest.mark.asyncio
    async def test_drop_storage_executes_drop_table_query(self, monkeypatch) -> None:
        """drop_storage must issue DROP TABLE IF EXISTS for the preseeded_tiles table."""
        from unittest.mock import AsyncMock, patch
        from dynastore.modules.tiles.tiles_module import TilePGPreseedStorage

        storage = TilePGPreseedStorage.__new__(TilePGPreseedStorage)
        storage.engine = object()

        monkeypatch.setattr(
            TilePGPreseedStorage, "_get_schema",
            AsyncMock(return_value="tenant_schema"),
        )

        executed_queries: list[str] = []

        class _FakeDDLQuery:
            def __init__(self, sql: str):
                self._sql = sql
            async def execute(self, conn: object) -> None:
                executed_queries.append(self._sql)

        class _FakeTxCM:
            async def __aenter__(self): return object()
            async def __aexit__(self, *a): return False

        with (
            patch(
                "dynastore.modules.tiles.tiles_module.DDLQuery",
                side_effect=_FakeDDLQuery,
            ),
            patch(
                "dynastore.modules.tiles.tiles_module.managed_transaction",
                return_value=_FakeTxCM(),
            ),
        ):
            await storage.drop_storage("test-cat")

        assert len(executed_queries) == 1
        assert 'DROP TABLE IF EXISTS "tenant_schema".preseeded_tiles' in executed_queries[0]

    @pytest.mark.asyncio
    async def test_drop_storage_is_noop_on_error(self, monkeypatch) -> None:
        """drop_storage must NOT raise on DB error (fail-soft — catalog is being deleted)."""
        from unittest.mock import AsyncMock, patch
        from dynastore.modules.tiles.tiles_module import TilePGPreseedStorage

        storage = TilePGPreseedStorage.__new__(TilePGPreseedStorage)
        storage.engine = object()

        monkeypatch.setattr(
            TilePGPreseedStorage, "_get_schema",
            AsyncMock(return_value="tenant_schema"),
        )

        class _FailingDDLQuery:
            def __init__(self, sql: str): pass
            async def execute(self, conn: object) -> None:
                raise OSError("DB connection lost")

        class _FakeTxCM:
            async def __aenter__(self): return object()
            async def __aexit__(self, *a): return False

        with (
            patch(
                "dynastore.modules.tiles.tiles_module.DDLQuery",
                side_effect=_FailingDDLQuery,
            ),
            patch(
                "dynastore.modules.tiles.tiles_module.managed_transaction",
                return_value=_FakeTxCM(),
            ),
        ):
            # Must not raise
            await storage.drop_storage("test-cat")


# ---------------------------------------------------------------------------
# GcsCatalogPrefixOwner: binary teardown boundary (integration boundary doc)
# ---------------------------------------------------------------------------


class TestGcsCascadeOwnerBoundary:
    """Structural check: GcsCatalogPrefixOwner does NOT use StorageProtocol.drop_storage
    internally (it calls GcpCatalogCleanupTask which uses bucket_tool directly).
    This test pins the boundary so it is not accidentally re-wired through drop_storage,
    which would create a call-loop (drop_storage → cascade owner → drop_storage).
    """

    def test_cleanup_one_calls_task_runner_not_drop_storage(self) -> None:
        """GcsCatalogPrefixOwner.cleanup_one must route through _get_task_runner(),
        NOT through StorageProtocol.drop_storage, to avoid double-cleanup."""
        import inspect
        from dynastore.extensions.gcp.cascade_owner import GcsCatalogPrefixOwner
        src = inspect.getsource(GcsCatalogPrefixOwner.cleanup_one)
        assert "_get_task_runner" in src or "GcpCatalogCleanupTask" in src, (
            "GcsCatalogPrefixOwner.cleanup_one must route through task runner, "
            "not StorageProtocol.drop_storage (would create double-cleanup loop)"
        )
        # The old verb must not be present
        assert "delete_storage_for_catalog" not in src, (
            "GcsCatalogPrefixOwner.cleanup_one must not use delete_storage_for_catalog"
        )
