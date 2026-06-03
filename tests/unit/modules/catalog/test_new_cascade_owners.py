"""Unit tests for new cascade owners added in gap-closure.

Covers:
- IamCatalogScopedOwner: describe_scope, cleanup_one HARD/SOFT/dry-run/error.
- TilePreseedOwner: describe_scope catalog + collection scope, cleanup_one.
- ProxyUrlOwner: describe_scope, cleanup_one.
- GcsCatalogPrefixOwner / GcsCollectionPrefixOwner: describe_scope, cleanup_one.
- cascade_runtime fail-closed: orchestrator raises on describe_scope failure.
- collection_service orchestrator call: snapshot_and_enqueue called before purge.

Note: EsItemsPublicIndexOwner, EsAssetIndexOwner, EsItemsEnvelopeIndexOwner
tests have been removed — those owners are retired and superseded by
RoutingDrivenCascadeOwner (tested in
tests/dynastore/modules/storage/unit/test_routing_driven_cascade_owner.py).
EsItemsIndexOwner (private) tests remain in test_es_cleanup_owners.py but
the class is now a retired stub.
"""

from __future__ import annotations

from typing import Any, Iterable
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.cascade_registry import CascadeCleanupRegistry
from dynastore.modules.catalog.resource_owner import (
    CleanupMode,
    CleanupOutcome,
    CleanupRef,
    ResourceScope,
    ScopeRef,
)
from dynastore.modules.tasks.models import PermanentTaskFailure


# ---------------------------------------------------------------------------
# Helpers shared across test classes
# ---------------------------------------------------------------------------

_CATALOG_ID = "test-cat-123"
_COLLECTION_ID = "test-col-456"
_PREFIX = "fao"


# ---------------------------------------------------------------------------
# IamCatalogScopedOwner
# ---------------------------------------------------------------------------


class TestIamCatalogScopedOwner:
    def _make_owner(self):
        from dynastore.modules.iam.cascade_owner import IamCatalogScopedOwner
        return IamCatalogScopedOwner()

    @pytest.mark.asyncio
    async def test_describe_scope_catalog_returns_one_ref(self) -> None:
        owner = self._make_owner()
        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)
        refs = await owner.describe_scope(scope_ref, MagicMock())
        assert len(refs) == 1
        ref = refs[0]
        assert ref.kind == "iam_policies"
        assert ref.locator == _CATALOG_ID
        assert ref.metadata["partition_key"] == _CATALOG_ID

    @pytest.mark.asyncio
    async def test_describe_scope_collection_returns_empty(self) -> None:
        owner = self._make_owner()
        scope_ref = ScopeRef(
            scope=ResourceScope.COLLECTION,
            catalog_id=_CATALOG_ID,
            collection_id=_COLLECTION_ID,
        )
        refs = await owner.describe_scope(scope_ref, MagicMock())
        assert refs == []

    @pytest.mark.asyncio
    async def test_cleanup_one_soft_returns_done(self) -> None:
        owner = self._make_owner()
        ref = CleanupRef(
            kind="iam_policies", locator=_CATALOG_ID,
            owner_id="iam.catalog_scoped",
            metadata={"partition_key": _CATALOG_ID},
        )
        outcome = await owner.cleanup_one(ref, CleanupMode.SOFT)
        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_cleanup_one_dry_run_returns_done(self) -> None:
        owner = self._make_owner()
        ref = CleanupRef(
            kind="iam_policies", locator=_CATALOG_ID,
            owner_id="iam.catalog_scoped",
            metadata={"partition_key": _CATALOG_ID},
        )
        outcome = await owner.cleanup_one(ref, CleanupMode.HARD, dry_run=True)
        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_cleanup_one_no_engine_returns_retry(self) -> None:
        owner = self._make_owner()
        ref = CleanupRef(
            kind="iam_policies", locator=_CATALOG_ID,
            owner_id="iam.catalog_scoped",
            metadata={"partition_key": _CATALOG_ID},
        )
        with patch("dynastore.tools.protocol_helpers.get_engine", return_value=None):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)
        assert outcome == CleanupOutcome.RETRY

    def test_register_owners_adds_to_registry(self) -> None:
        from dynastore.modules.iam.cascade_owner import register_owners, IamCatalogScopedOwner
        reg = CascadeCleanupRegistry()
        register_owners(reg)
        assert reg.get(IamCatalogScopedOwner.owner_id) is not None
        catalog_owners = [o.owner_id for o in reg.owners_for_scope(ResourceScope.CATALOG)]
        assert IamCatalogScopedOwner.owner_id in catalog_owners


# ---------------------------------------------------------------------------
# TilePreseedOwner
# ---------------------------------------------------------------------------


class TestTilePreseedOwner:
    def _make_owner(self):
        from dynastore.modules.tiles.cascade_owner import TilePreseedOwner
        return TilePreseedOwner()

    @pytest.mark.asyncio
    async def test_describe_scope_catalog_returns_one_ref(self) -> None:
        owner = self._make_owner()
        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)
        refs = await owner.describe_scope(scope_ref, MagicMock())
        assert len(refs) == 1
        ref = refs[0]
        assert ref.kind == "tile_cache"
        assert ref.metadata["scope"] == "catalog"
        assert ref.metadata["catalog_id"] == _CATALOG_ID

    @pytest.mark.asyncio
    async def test_describe_scope_collection_returns_one_ref(self) -> None:
        owner = self._make_owner()
        scope_ref = ScopeRef(
            scope=ResourceScope.COLLECTION,
            catalog_id=_CATALOG_ID,
            collection_id=_COLLECTION_ID,
        )
        refs = await owner.describe_scope(scope_ref, MagicMock())
        assert len(refs) == 1
        ref = refs[0]
        assert ref.kind == "tile_cache"
        assert ref.metadata["scope"] == "collection"
        assert ref.metadata["catalog_id"] == _CATALOG_ID
        assert ref.metadata["collection_id"] == _COLLECTION_ID

    @pytest.mark.asyncio
    async def test_cleanup_one_soft_returns_done(self) -> None:
        owner = self._make_owner()
        ref = CleanupRef(
            kind="tile_cache", locator=_CATALOG_ID, owner_id="tiles.preseed",
            metadata={"scope": "catalog", "catalog_id": _CATALOG_ID},
        )
        outcome = await owner.cleanup_one(ref, CleanupMode.SOFT)
        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_cleanup_one_catalog_hard_calls_invalidate_catalog_tiles(self) -> None:
        owner = self._make_owner()
        ref = CleanupRef(
            kind="tile_cache", locator=_CATALOG_ID, owner_id="tiles.preseed",
            metadata={"scope": "catalog", "catalog_id": _CATALOG_ID},
        )
        with patch(
            "dynastore.modules.tiles.tiles_module.invalidate_catalog_tiles",
            new=AsyncMock(),
        ) as mock_invalidate:
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)
        mock_invalidate.assert_awaited_once_with(_CATALOG_ID)
        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_cleanup_one_collection_hard_calls_invalidate_collection_tiles(self) -> None:
        owner = self._make_owner()
        ref = CleanupRef(
            kind="tile_cache",
            locator=f"{_CATALOG_ID}:{_COLLECTION_ID}",
            owner_id="tiles.preseed",
            metadata={
                "scope": "collection",
                "catalog_id": _CATALOG_ID,
                "collection_id": _COLLECTION_ID,
            },
        )
        with patch(
            "dynastore.modules.tiles.tiles_module.invalidate_collection_tiles",
            new=AsyncMock(),
        ) as mock_invalidate:
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)
        mock_invalidate.assert_awaited_once_with(_CATALOG_ID, _COLLECTION_ID)
        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_cleanup_one_exception_returns_retry(self) -> None:
        owner = self._make_owner()
        ref = CleanupRef(
            kind="tile_cache", locator=_CATALOG_ID, owner_id="tiles.preseed",
            metadata={"scope": "catalog", "catalog_id": _CATALOG_ID},
        )
        with patch(
            "dynastore.modules.tiles.tiles_module.invalidate_catalog_tiles",
            new=AsyncMock(side_effect=RuntimeError("tile store offline")),
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)
        assert outcome == CleanupOutcome.RETRY

    @pytest.mark.asyncio
    async def test_cleanup_one_dry_run_returns_done(self) -> None:
        owner = self._make_owner()
        ref = CleanupRef(
            kind="tile_cache", locator=_CATALOG_ID, owner_id="tiles.preseed",
            metadata={"scope": "catalog", "catalog_id": _CATALOG_ID},
        )
        outcome = await owner.cleanup_one(ref, CleanupMode.HARD, dry_run=True)
        assert outcome == CleanupOutcome.DONE

    def test_register_owners_adds_to_registry(self) -> None:
        from dynastore.modules.tiles.cascade_owner import register_owners, TilePreseedOwner
        reg = CascadeCleanupRegistry()
        register_owners(reg)
        assert reg.get(TilePreseedOwner.owner_id) is not None
        catalog_owners = [o.owner_id for o in reg.owners_for_scope(ResourceScope.CATALOG)]
        collection_owners = [o.owner_id for o in reg.owners_for_scope(ResourceScope.COLLECTION)]
        assert TilePreseedOwner.owner_id in catalog_owners
        assert TilePreseedOwner.owner_id in collection_owners


# ---------------------------------------------------------------------------
# ProxyUrlOwner
# ---------------------------------------------------------------------------


class TestProxyUrlOwner:
    def _make_owner(self):
        from dynastore.extensions.proxy.cascade_owner import ProxyUrlOwner
        return ProxyUrlOwner()

    @pytest.mark.asyncio
    async def test_describe_scope_non_collection_returns_empty(self) -> None:
        owner = self._make_owner()
        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)
        refs = await owner.describe_scope(scope_ref, MagicMock())
        assert refs == []

    @pytest.mark.asyncio
    async def test_describe_scope_collection_no_catalogs_protocol_returns_empty(self) -> None:
        owner = self._make_owner()
        scope_ref = ScopeRef(
            scope=ResourceScope.COLLECTION,
            catalog_id=_CATALOG_ID,
            collection_id=_COLLECTION_ID,
        )
        # get_protocol is imported lazily inside the method — patch at source
        with patch("dynastore.modules.get_protocol", return_value=None):
            refs = await owner.describe_scope(scope_ref, MagicMock())
        assert refs == []

    @pytest.mark.asyncio
    async def test_describe_scope_collection_returns_one_ref_per_key(self) -> None:
        owner = self._make_owner()
        scope_ref = ScopeRef(
            scope=ResourceScope.COLLECTION,
            catalog_id=_CATALOG_ID,
            collection_id=_COLLECTION_ID,
        )
        mock_catalogs = MagicMock()
        mock_catalogs.resolve_physical_schema = AsyncMock(return_value="schema_abc")
        mock_conn = MagicMock()

        mock_dql_instance = MagicMock()
        mock_dql_instance.execute = AsyncMock(return_value=["key1", "key2"])

        # Lazy imports are at source modules; DriverContext validation bypassed
        with (
            patch("dynastore.modules.get_protocol", return_value=mock_catalogs),
            patch(
                "dynastore.modules.db_config.query_executor.DQLQuery",
                return_value=mock_dql_instance,
            ),
            patch(
                "dynastore.models.driver_context.DriverContext",
                return_value=MagicMock(),
            ),
        ):
            refs = await owner.describe_scope(scope_ref, mock_conn)

        assert len(refs) == 2
        locators = {r.locator for r in refs}
        assert locators == {"key1", "key2"}
        for r in refs:
            assert r.kind == "proxy_short_url"
            assert r.metadata["catalog_id"] == _CATALOG_ID
            assert r.metadata["collection_id"] == _COLLECTION_ID

    @pytest.mark.asyncio
    async def test_cleanup_one_soft_returns_done(self) -> None:
        owner = self._make_owner()
        ref = CleanupRef(
            kind="proxy_short_url", locator="key1", owner_id="proxy.urls",
            metadata={"catalog_id": _CATALOG_ID, "collection_id": _COLLECTION_ID},
        )
        outcome = await owner.cleanup_one(ref, CleanupMode.SOFT)
        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_cleanup_one_hard_calls_delete_short_url(self) -> None:
        owner = self._make_owner()
        ref = CleanupRef(
            kind="proxy_short_url", locator="key1", owner_id="proxy.urls",
            metadata={"catalog_id": _CATALOG_ID, "collection_id": _COLLECTION_ID},
        )
        mock_conn = MagicMock()

        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _managed(_engine):
            yield mock_conn

        mock_delete = AsyncMock()

        # All imports are lazy inside cleanup_one — patch at source
        with (
            patch("dynastore.tools.protocol_helpers.get_engine", return_value=MagicMock()),
            patch("dynastore.modules.db_config.query_executor.managed_transaction", _managed),
            patch("dynastore.modules.proxy.proxy_module.delete_short_url", mock_delete),
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)

        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_cleanup_one_no_engine_returns_retry(self) -> None:
        owner = self._make_owner()
        ref = CleanupRef(
            kind="proxy_short_url", locator="key1", owner_id="proxy.urls",
            metadata={"catalog_id": _CATALOG_ID, "collection_id": _COLLECTION_ID},
        )
        # get_engine is imported lazily inside the method — patch at source
        with patch("dynastore.tools.protocol_helpers.get_engine", return_value=None):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)
        assert outcome == CleanupOutcome.RETRY


# ---------------------------------------------------------------------------
# GcsCatalogPrefixOwner / GcsCollectionPrefixOwner
# ---------------------------------------------------------------------------


class TestGcsCatalogPrefixOwner:
    def _make_owner(self):
        from dynastore.extensions.gcp.cascade_owner import GcsCatalogPrefixOwner
        return GcsCatalogPrefixOwner()

    @pytest.mark.asyncio
    async def test_describe_scope_catalog_returns_one_ref(self) -> None:
        owner = self._make_owner()
        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)
        with patch(
            "dynastore.extensions.gcp.cascade_owner._resolve_bucket_name",
            new=AsyncMock(return_value="my-bucket"),
        ):
            refs = await owner.describe_scope(scope_ref, MagicMock())
        assert len(refs) == 1
        ref = refs[0]
        assert ref.kind == "gcs_bucket"
        assert ref.metadata["catalog_id"] == _CATALOG_ID
        assert ref.metadata["bucket_name"] == "my-bucket"

    @pytest.mark.asyncio
    async def test_describe_scope_collection_returns_empty(self) -> None:
        owner = self._make_owner()
        scope_ref = ScopeRef(
            scope=ResourceScope.COLLECTION, catalog_id=_CATALOG_ID, collection_id=_COLLECTION_ID
        )
        refs = await owner.describe_scope(scope_ref, MagicMock())
        assert refs == []

    @pytest.mark.asyncio
    async def test_cleanup_one_soft_returns_done(self) -> None:
        owner = self._make_owner()
        ref = CleanupRef(
            kind="gcs_bucket", locator=f"gs://my-bucket", owner_id="gcp.gcs.catalog_prefix",
            metadata={"catalog_id": _CATALOG_ID, "bucket_name": "my-bucket"},
        )
        outcome = await owner.cleanup_one(ref, CleanupMode.SOFT)
        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_cleanup_one_hard_calls_cleanup_catalog(self) -> None:
        owner = self._make_owner()
        ref = CleanupRef(
            kind="gcs_bucket", locator=f"gs://my-bucket", owner_id="gcp.gcs.catalog_prefix",
            metadata={"catalog_id": _CATALOG_ID, "bucket_name": "my-bucket"},
        )
        mock_task = MagicMock()
        mock_task._cleanup_catalog = AsyncMock(return_value={"status": "cleaned"})
        with patch(
            "dynastore.extensions.gcp.cascade_owner._get_task_runner",
            return_value=mock_task,
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)
        mock_task._cleanup_catalog.assert_awaited_once_with(_CATALOG_ID, bucket_name="my-bucket")
        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_cleanup_one_exception_returns_retry(self) -> None:
        owner = self._make_owner()
        ref = CleanupRef(
            kind="gcs_bucket", locator="gs://x", owner_id="gcp.gcs.catalog_prefix",
            metadata={"catalog_id": _CATALOG_ID, "bucket_name": None},
        )
        mock_task = MagicMock()
        mock_task._cleanup_catalog = AsyncMock(side_effect=RuntimeError("GCS unavailable"))
        with patch("dynastore.extensions.gcp.cascade_owner._get_task_runner", return_value=mock_task):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)
        assert outcome == CleanupOutcome.RETRY


class TestGcsCollectionPrefixOwner:
    def _make_owner(self):
        from dynastore.extensions.gcp.cascade_owner import GcsCollectionPrefixOwner
        return GcsCollectionPrefixOwner()

    @pytest.mark.asyncio
    async def test_describe_scope_collection_returns_one_ref(self) -> None:
        owner = self._make_owner()
        scope_ref = ScopeRef(
            scope=ResourceScope.COLLECTION,
            catalog_id=_CATALOG_ID,
            collection_id=_COLLECTION_ID,
        )
        # bucket_tool is imported lazily inside the method — patch at source
        with (
            patch(
                "dynastore.extensions.gcp.cascade_owner._resolve_bucket_name",
                new=AsyncMock(return_value="my-bucket"),
            ),
            patch(
                "dynastore.modules.gcp.tools.bucket.get_blob_path_for_collection_folder",
                return_value=f"collections/{_COLLECTION_ID}/",
            ),
        ):
            refs = await owner.describe_scope(scope_ref, MagicMock())
        assert len(refs) == 1
        ref = refs[0]
        assert ref.kind == "gcs_prefix"
        assert ref.metadata["catalog_id"] == _CATALOG_ID
        assert ref.metadata["collection_id"] == _COLLECTION_ID

    @pytest.mark.asyncio
    async def test_describe_scope_catalog_returns_empty(self) -> None:
        owner = self._make_owner()
        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)
        refs = await owner.describe_scope(scope_ref, MagicMock())
        assert refs == []

    @pytest.mark.asyncio
    async def test_cleanup_one_hard_calls_cleanup_collection(self) -> None:
        owner = self._make_owner()
        ref = CleanupRef(
            kind="gcs_prefix",
            locator=f"gs://my-bucket/collections/{_COLLECTION_ID}/",
            owner_id="gcp.gcs.collection_prefix",
            metadata={
                "catalog_id": _CATALOG_ID,
                "collection_id": _COLLECTION_ID,
                "bucket_name": "my-bucket",
                "folder_prefix": f"collections/{_COLLECTION_ID}/",
            },
        )
        mock_task = MagicMock()
        mock_task._cleanup_collection = AsyncMock(return_value={"status": "cleaned"})
        with patch("dynastore.extensions.gcp.cascade_owner._get_task_runner", return_value=mock_task):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)
        mock_task._cleanup_collection.assert_awaited_once_with(
            _CATALOG_ID, _COLLECTION_ID, bucket_name="my-bucket"
        )
        assert outcome == CleanupOutcome.DONE


# ---------------------------------------------------------------------------
# Per-ref retry counter in CascadeCleanupTask
# ---------------------------------------------------------------------------


class TestCascadeCleanupTaskPerRefRetry:
    """Verify per-ref retry count exhaustion marks ref DEAD."""

    def _make_owner_raising(self, owner_id: str) -> Any:
        class _RaisingOwner:
            pass
        _RaisingOwner.owner_id = owner_id  # type: ignore[attr-defined]

        def supported_scopes(self: Any) -> Iterable[ResourceScope]:
            return (ResourceScope.CATALOG,)

        async def describe_scope(self: Any, scope_ref: ScopeRef, conn: Any) -> list[CleanupRef]:
            return []

        async def cleanup_one(
            self: Any, ref: CleanupRef, mode: CleanupMode, *, dry_run: bool = False
        ) -> CleanupOutcome:
            raise RuntimeError("transient failure")

        _RaisingOwner.supported_scopes = supported_scopes  # type: ignore[attr-defined]
        _RaisingOwner.describe_scope = describe_scope  # type: ignore[attr-defined]
        _RaisingOwner.cleanup_one = cleanup_one  # type: ignore[attr-defined]
        return _RaisingOwner()  # type: ignore[return-value]

    def _run_task(self, payload, registry):
        from dynastore.tasks.cascade_cleanup.task import CascadeCleanupTask
        import asyncio
        with patch(
            "dynastore.modules.catalog.cascade_registry.cascade_cleanup_registry",
            new=registry,
        ):
            with patch("dynastore.tasks.cascade_cleanup.task._persist_updated_refs", new=AsyncMock()):
                try:
                    # asyncio.run (not get_event_loop().run_until_complete):
                    # Python 3.12 removed the implicit current-loop.
                    return asyncio.run(CascadeCleanupTask().run(payload)), None
                except (RuntimeError, PermanentTaskFailure) as exc:
                    return None, exc

    def test_ref_with_max_retries_exhausted_marked_dead(self) -> None:
        from dynastore.tasks.cascade_cleanup.task import _DEFAULT_MAX_REF_RETRIES, _RETRY_COUNT_KEY

        reg = CascadeCleanupRegistry()
        reg.register(self._make_owner_raising("owner-a"))
        reg.freeze()

        ref_dict = {
            "kind": "es_index",
            "locator": "idx-a",
            "owner_id": "owner-a",
            "metadata": {_RETRY_COUNT_KEY: _DEFAULT_MAX_REF_RETRIES},
        }
        payload = MagicMock()
        payload.inputs = {"scope_ref": {"scope": "catalog", "catalog_id": "cat-1"}, "mode": "hard", "refs": [ref_dict]}
        payload.task_id = None
        payload.schema_name = "system"

        _, exc = self._run_task(payload, reg)
        assert exc is not None
        assert "permanently failed" in str(exc)

    def test_ref_below_max_retries_marked_retry(self) -> None:
        from dynastore.tasks.cascade_cleanup.task import _DEFAULT_MAX_REF_RETRIES, _RETRY_COUNT_KEY

        reg = CascadeCleanupRegistry()
        reg.register(self._make_owner_raising("owner-b"))
        reg.freeze()

        ref_dict = {
            "kind": "es_index",
            "locator": "idx-b",
            "owner_id": "owner-b",
            "metadata": {_RETRY_COUNT_KEY: _DEFAULT_MAX_REF_RETRIES - 1},
        }
        payload = MagicMock()
        payload.inputs = {"scope_ref": {"scope": "catalog", "catalog_id": "cat-2"}, "mode": "hard", "refs": [ref_dict]}
        payload.task_id = None
        payload.schema_name = "system"

        _, exc = self._run_task(payload, reg)
        assert exc is not None
        assert "retry" in str(exc).lower()
