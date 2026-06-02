"""Unit tests for RoutingDrivenCascadeOwner and CollectionElasticsearchDriver.drop_storage.

Covers:
- RoutingDrivenCascadeOwner: owner_id, supported_scopes, describe_scope emits
  one ref per enumerated driver with correct metadata, cleanup_one resolves the
  right registry and calls drop_storage, SOFT/dry_run behaviour, missing-driver
  returns DONE, unexpected exception returns RETRY.
- _enumerate_configured_drivers: deduplication, GCS exclusion.
- CollectionElasticsearchDriver.drop_storage: catalog scope issues
  delete_by_query with term catalog_id + routing; collection scope delegates
  to delete_metadata; mock asserts correct call args.
- register_owners adds the owner to a fresh registry.
"""

from __future__ import annotations

from typing import Any
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
from dynastore.modules.storage.drivers.routing_driven_cascade_owner import (
    RoutingDrivenCascadeOwner,
    _enumerate_configured_drivers,
    _is_excluded_driver,
    _resolve_driver,
    register_owners,
    _REGISTRY_ITEMS,
    _REGISTRY_ASSET,
    _REGISTRY_COLLECTION,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_CATALOG_ID = "test-catalog"
_COLLECTION_ID = "test-collection"


def _make_ref(
    driver_ref: str = "items_elasticsearch_driver",
    registry: str = _REGISTRY_ITEMS,
    catalog_id: str = _CATALOG_ID,
    collection_id: str | None = None,
) -> CleanupRef:
    return CleanupRef(
        kind="storage_driver",
        locator=driver_ref,
        owner_id=RoutingDrivenCascadeOwner.owner_id,
        metadata={
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "registry": registry,
        },
    )


# ---------------------------------------------------------------------------
# _is_excluded_driver
# ---------------------------------------------------------------------------


class TestIsExcludedDriver:
    def test_gcs_driver_excluded(self) -> None:
        assert _is_excluded_driver("gcs_asset_driver") is True

    def test_bigquery_driver_excluded(self) -> None:
        assert _is_excluded_driver("bigquery_items_driver") is True

    def test_gcp_driver_excluded(self) -> None:
        assert _is_excluded_driver("gcp_storage_driver") is True

    def test_elasticsearch_driver_not_excluded(self) -> None:
        assert _is_excluded_driver("items_elasticsearch_driver") is False

    def test_postgresql_driver_not_excluded(self) -> None:
        assert _is_excluded_driver("items_postgresql_driver") is False

    def test_private_driver_not_excluded(self) -> None:
        assert _is_excluded_driver("items_elasticsearch_private_driver") is False


# ---------------------------------------------------------------------------
# _enumerate_configured_drivers — unit with mocked _resolve_driver_ids_cached
# ---------------------------------------------------------------------------


class TestEnumerateConfiguredDrivers:
    @pytest.mark.asyncio
    async def test_returns_deduped_pairs_across_operations(self) -> None:
        """Same driver_ref appearing in WRITE + READ for items → only one pair."""
        # items WRITE returns driver A; items READ returns driver A again.
        async def _mock_resolve(routing_cls, catalog_id, collection_id, op, hints):
            from dynastore.modules.storage.routing_config import ItemsRoutingConfig
            if routing_cls is ItemsRoutingConfig:
                return [("items_elasticsearch_driver", "FATAL", "SYNC")]
            return []

        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)

        with patch(
            "dynastore.modules.storage.router._resolve_driver_ids_cached",
            new=AsyncMock(side_effect=_mock_resolve),
        ):
            result = await _enumerate_configured_drivers(scope_ref, MagicMock())

        # Only one entry for items_elasticsearch_driver despite multiple ops
        items_entries = [p for p in result if p == (_REGISTRY_ITEMS, "items_elasticsearch_driver")]
        assert len(items_entries) == 1

    @pytest.mark.asyncio
    async def test_excludes_gcs_drivers(self) -> None:
        """GCS driver refs must not appear in the result."""
        async def _mock_resolve(routing_cls, catalog_id, collection_id, op, hints):
            from dynastore.modules.storage.routing_config import AssetRoutingConfig
            if routing_cls is AssetRoutingConfig:
                return [
                    ("asset_elasticsearch_driver", "FATAL", "SYNC"),
                    ("gcs_asset_driver", "WARN", "SYNC"),
                ]
            return []

        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)

        with patch(
            "dynastore.modules.storage.router._resolve_driver_ids_cached",
            new=AsyncMock(side_effect=_mock_resolve),
        ):
            result = await _enumerate_configured_drivers(scope_ref, MagicMock())

        driver_refs = [dr for _, dr in result]
        assert "gcs_asset_driver" not in driver_refs
        assert "asset_elasticsearch_driver" in driver_refs

    @pytest.mark.asyncio
    async def test_benign_exception_treated_as_no_drivers(self) -> None:
        """ConfigsProtocol not available → treated as empty (no raise)."""
        async def _mock_resolve(routing_cls, catalog_id, collection_id, op, hints):
            raise RuntimeError("ConfigsProtocol not available — cannot resolve storage routing")

        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)

        with patch(
            "dynastore.modules.storage.router._resolve_driver_ids_cached",
            new=AsyncMock(side_effect=_mock_resolve),
        ):
            result = await _enumerate_configured_drivers(scope_ref, MagicMock())

        assert result == []

    @pytest.mark.asyncio
    async def test_unexpected_exception_propagates(self) -> None:
        """Unexpected infrastructure errors propagate (fail-closed)."""
        async def _mock_resolve(routing_cls, catalog_id, collection_id, op, hints):
            raise RuntimeError("database connection lost")

        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)

        with patch(
            "dynastore.modules.storage.router._resolve_driver_ids_cached",
            new=AsyncMock(side_effect=_mock_resolve),
        ):
            with pytest.raises(RuntimeError, match="database connection lost"):
                await _enumerate_configured_drivers(scope_ref, MagicMock())

    @pytest.mark.asyncio
    async def test_includes_collection_metadata_driver(self) -> None:
        """CollectionRoutingConfig drivers (incl. collection_elasticsearch_driver) must appear."""
        async def _mock_resolve(routing_cls, catalog_id, collection_id, op, hints):
            from dynastore.modules.storage.routing_config import CollectionRoutingConfig
            if routing_cls is CollectionRoutingConfig:
                return [("collection_elasticsearch_driver", "FATAL", "SYNC")]
            return []

        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)

        with patch(
            "dynastore.modules.storage.router._resolve_driver_ids_cached",
            new=AsyncMock(side_effect=_mock_resolve),
        ):
            result = await _enumerate_configured_drivers(scope_ref, MagicMock())

        assert (_REGISTRY_COLLECTION, "collection_elasticsearch_driver") in result


# ---------------------------------------------------------------------------
# RoutingDrivenCascadeOwner.describe_scope
# ---------------------------------------------------------------------------


class TestDescribeScope:
    @pytest.mark.asyncio
    async def test_catalog_scope_emits_refs_with_correct_metadata(self) -> None:
        owner = RoutingDrivenCascadeOwner()
        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)

        with patch(
            "dynastore.modules.storage.drivers.routing_driven_cascade_owner._enumerate_configured_drivers",
            new=AsyncMock(return_value=[
                (_REGISTRY_ITEMS, "items_elasticsearch_driver"),
                (_REGISTRY_COLLECTION, "collection_elasticsearch_driver"),
            ]),
        ):
            refs = await owner.describe_scope(scope_ref, MagicMock())

        assert len(refs) == 2
        by_locator = {r.locator: r for r in refs}

        r_items = by_locator["items_elasticsearch_driver"]
        assert r_items.kind == "storage_driver"
        assert r_items.owner_id == RoutingDrivenCascadeOwner.owner_id
        assert r_items.metadata["catalog_id"] == _CATALOG_ID
        assert r_items.metadata["collection_id"] is None
        assert r_items.metadata["registry"] == _REGISTRY_ITEMS

        r_col = by_locator["collection_elasticsearch_driver"]
        assert r_col.metadata["registry"] == _REGISTRY_COLLECTION

    @pytest.mark.asyncio
    async def test_collection_scope_passes_collection_id_in_metadata(self) -> None:
        owner = RoutingDrivenCascadeOwner()
        scope_ref = ScopeRef(
            scope=ResourceScope.COLLECTION,
            catalog_id=_CATALOG_ID,
            collection_id=_COLLECTION_ID,
        )

        with patch(
            "dynastore.modules.storage.drivers.routing_driven_cascade_owner._enumerate_configured_drivers",
            new=AsyncMock(return_value=[(_REGISTRY_ITEMS, "items_elasticsearch_driver")]),
        ):
            refs = await owner.describe_scope(scope_ref, MagicMock())

        assert len(refs) == 1
        assert refs[0].metadata["collection_id"] == _COLLECTION_ID

    @pytest.mark.asyncio
    async def test_describe_scope_empty_when_no_drivers(self) -> None:
        owner = RoutingDrivenCascadeOwner()
        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)

        with patch(
            "dynastore.modules.storage.drivers.routing_driven_cascade_owner._enumerate_configured_drivers",
            new=AsyncMock(return_value=[]),
        ):
            refs = await owner.describe_scope(scope_ref, MagicMock())

        assert refs == []

    @pytest.mark.asyncio
    async def test_describe_scope_propagates_exception(self) -> None:
        """describe_scope must propagate (fail-closed) on infra failures."""
        owner = RoutingDrivenCascadeOwner()
        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)

        with patch(
            "dynastore.modules.storage.drivers.routing_driven_cascade_owner._enumerate_configured_drivers",
            new=AsyncMock(side_effect=RuntimeError("db gone")),
        ):
            with pytest.raises(RuntimeError, match="db gone"):
                await owner.describe_scope(scope_ref, MagicMock())

    def test_supported_scopes(self) -> None:
        owner = RoutingDrivenCascadeOwner()
        scopes = list(owner.supported_scopes())
        assert ResourceScope.CATALOG in scopes
        assert ResourceScope.COLLECTION in scopes

    def test_owner_id(self) -> None:
        assert RoutingDrivenCascadeOwner.owner_id == "storage.routing_driven"


# ---------------------------------------------------------------------------
# RoutingDrivenCascadeOwner.cleanup_one
# ---------------------------------------------------------------------------


class TestCleanupOne:
    @pytest.mark.asyncio
    async def test_dry_run_returns_done_without_calling_driver(self) -> None:
        owner = RoutingDrivenCascadeOwner()
        ref = _make_ref()
        mock_driver = MagicMock()
        mock_driver.drop_storage = AsyncMock()

        with patch(
            "dynastore.modules.storage.drivers.routing_driven_cascade_owner._resolve_driver",
            return_value=mock_driver,
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD, dry_run=True)

        mock_driver.drop_storage.assert_not_awaited()
        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_missing_driver_returns_done(self) -> None:
        owner = RoutingDrivenCascadeOwner()
        ref = _make_ref(driver_ref="nonexistent_driver")

        with patch(
            "dynastore.modules.storage.drivers.routing_driven_cascade_owner._resolve_driver",
            return_value=None,
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)

        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_hard_calls_drop_storage_with_correct_args(self) -> None:
        owner = RoutingDrivenCascadeOwner()
        ref = _make_ref(
            driver_ref="items_elasticsearch_driver",
            registry=_REGISTRY_ITEMS,
            catalog_id=_CATALOG_ID,
            collection_id=None,
        )
        mock_driver = MagicMock()
        mock_driver.drop_storage = AsyncMock()

        with patch(
            "dynastore.modules.storage.drivers.routing_driven_cascade_owner._resolve_driver",
            return_value=mock_driver,
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)

        mock_driver.drop_storage.assert_awaited_once_with(
            _CATALOG_ID, None, soft=False
        )
        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_collection_scope_passes_collection_id(self) -> None:
        owner = RoutingDrivenCascadeOwner()
        ref = _make_ref(
            driver_ref="items_elasticsearch_driver",
            collection_id=_COLLECTION_ID,
        )
        mock_driver = MagicMock()
        mock_driver.drop_storage = AsyncMock()

        with patch(
            "dynastore.modules.storage.drivers.routing_driven_cascade_owner._resolve_driver",
            return_value=mock_driver,
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)

        mock_driver.drop_storage.assert_awaited_once_with(
            _CATALOG_ID, _COLLECTION_ID, soft=False
        )
        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_soft_calls_drop_storage_with_soft_true(self) -> None:
        owner = RoutingDrivenCascadeOwner()
        ref = _make_ref()
        mock_driver = MagicMock()
        mock_driver.drop_storage = AsyncMock()

        with patch(
            "dynastore.modules.storage.drivers.routing_driven_cascade_owner._resolve_driver",
            return_value=mock_driver,
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.SOFT)

        mock_driver.drop_storage.assert_awaited_once_with(
            _CATALOG_ID, None, soft=True
        )
        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_soft_delete_not_supported_returns_done(self) -> None:
        """SoftDeleteNotSupportedError on SOFT mode → DONE (retain data)."""
        class SoftDeleteNotSupportedError(Exception):
            pass

        owner = RoutingDrivenCascadeOwner()
        ref = _make_ref()
        mock_driver = MagicMock()
        mock_driver.drop_storage = AsyncMock(side_effect=SoftDeleteNotSupportedError("no soft"))

        with patch(
            "dynastore.modules.storage.drivers.routing_driven_cascade_owner._resolve_driver",
            return_value=mock_driver,
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.SOFT)

        assert outcome == CleanupOutcome.DONE

    @pytest.mark.asyncio
    async def test_hard_unexpected_exception_returns_retry(self) -> None:
        owner = RoutingDrivenCascadeOwner()
        ref = _make_ref()
        mock_driver = MagicMock()
        mock_driver.drop_storage = AsyncMock(side_effect=OSError("connection refused"))

        with patch(
            "dynastore.modules.storage.drivers.routing_driven_cascade_owner._resolve_driver",
            return_value=mock_driver,
        ):
            outcome = await owner.cleanup_one(ref, CleanupMode.HARD)

        assert outcome == CleanupOutcome.RETRY


# ---------------------------------------------------------------------------
# _resolve_driver
# ---------------------------------------------------------------------------


class TestResolveDriver:
    def test_items_registry_uses_collection_index(self) -> None:
        ref = _make_ref(driver_ref="items_elasticsearch_driver", registry=_REGISTRY_ITEMS)
        sentinel = object()
        with patch(
            "dynastore.modules.storage.driver_registry.DriverRegistry.collection_index",
            return_value={"items_elasticsearch_driver": sentinel},
        ):
            driver = _resolve_driver(ref)
        assert driver is sentinel

    def test_asset_registry_uses_asset_index(self) -> None:
        ref = _make_ref(driver_ref="asset_elasticsearch_driver", registry=_REGISTRY_ASSET)
        sentinel = object()
        with patch(
            "dynastore.modules.storage.driver_registry.DriverRegistry.asset_index",
            return_value={"asset_elasticsearch_driver": sentinel},
        ):
            driver = _resolve_driver(ref)
        assert driver is sentinel

    def test_collection_registry_uses_collection_store_index(self) -> None:
        ref = _make_ref(
            driver_ref="collection_elasticsearch_driver",
            registry=_REGISTRY_COLLECTION,
        )
        sentinel = object()
        with patch(
            "dynastore.modules.storage.driver_registry.DriverRegistry.collection_store_index",
            return_value={"collection_elasticsearch_driver": sentinel},
        ):
            driver = _resolve_driver(ref)
        assert driver is sentinel

    def test_missing_driver_returns_none(self) -> None:
        ref = _make_ref(driver_ref="nonexistent_driver", registry=_REGISTRY_ITEMS)
        with patch(
            "dynastore.modules.storage.driver_registry.DriverRegistry.collection_index",
            return_value={},
        ):
            driver = _resolve_driver(ref)
        assert driver is None

    def test_unknown_registry_returns_none(self) -> None:
        ref = _make_ref(registry="unknown_registry")
        with patch(
            "dynastore.modules.storage.driver_registry.DriverRegistry.collection_index",
            return_value={},
        ):
            driver = _resolve_driver(ref)
        assert driver is None


# ---------------------------------------------------------------------------
# register_owners
# ---------------------------------------------------------------------------


class TestRegisterOwners:
    def test_register_adds_routing_driven_owner(self) -> None:
        reg = CascadeCleanupRegistry()
        register_owners(reg)
        assert reg.get(RoutingDrivenCascadeOwner.owner_id) is not None

    def test_owner_in_catalog_scope(self) -> None:
        reg = CascadeCleanupRegistry()
        register_owners(reg)
        catalog_owners = [o.owner_id for o in reg.owners_for_scope(ResourceScope.CATALOG)]
        assert RoutingDrivenCascadeOwner.owner_id in catalog_owners

    def test_owner_in_collection_scope(self) -> None:
        reg = CascadeCleanupRegistry()
        register_owners(reg)
        collection_owners = [o.owner_id for o in reg.owners_for_scope(ResourceScope.COLLECTION)]
        assert RoutingDrivenCascadeOwner.owner_id in collection_owners

    def test_double_register_raises_value_error(self) -> None:
        reg = CascadeCleanupRegistry()
        register_owners(reg)
        with pytest.raises(ValueError, match=RoutingDrivenCascadeOwner.owner_id):
            register_owners(reg)


# ---------------------------------------------------------------------------
# CollectionElasticsearchDriver.drop_storage
# ---------------------------------------------------------------------------


class TestCollectionEsDriverDropStorage:
    def _make_driver(self) -> Any:
        from dynastore.modules.elasticsearch.collection_es_driver import (
            CollectionElasticsearchDriver,
        )
        return CollectionElasticsearchDriver()

    def _patch_index_name(self, name: str = "dynastore-collections"):
        from dynastore.modules.elasticsearch.collection_es_driver import (
            CollectionElasticsearchDriver,
        )
        return patch.object(
            CollectionElasticsearchDriver,
            "_index_name",
            return_value=name,
        )

    def _patch_client(self, client: Any):
        from dynastore.modules.elasticsearch.collection_es_driver import (
            CollectionElasticsearchDriver,
        )
        return patch.object(
            CollectionElasticsearchDriver,
            "_get_client",
            return_value=client,
        )

    @pytest.mark.asyncio
    async def test_catalog_scope_calls_delete_by_query_with_term(self) -> None:
        """Catalog scope: delete_by_query with term catalog_id + routing.

        The tenant-safety guard: the query MUST use term catalog_id so only
        docs belonging to the deleted catalog are removed.  A missing term
        would delete other tenants' docs — a severe regression.
        """
        driver = self._make_driver()
        es = MagicMock()
        es.delete_by_query = AsyncMock(return_value={"deleted": 3})

        index_name = "dynastore-collections"
        with self._patch_index_name(index_name), self._patch_client(es):
            await driver.drop_storage(_CATALOG_ID, collection_id=None, soft=False)

        es.delete_by_query.assert_awaited_once()
        call_kwargs = es.delete_by_query.call_args
        assert call_kwargs.kwargs["index"] == index_name or call_kwargs.args[0] == index_name
        # The key correctness assertion: body contains term catalog_id.
        body = call_kwargs.kwargs.get("body") or call_kwargs.args[1]
        assert body["query"]["term"]["catalog_id"] == _CATALOG_ID
        # routing restricts to the catalog's shard.
        params = call_kwargs.kwargs.get("params", {})
        assert params.get("routing") == _CATALOG_ID

    @pytest.mark.asyncio
    async def test_collection_scope_calls_delete_metadata(self) -> None:
        """Collection scope delegates to delete_metadata for single-doc removal."""
        driver = self._make_driver()
        es = MagicMock()
        es.delete = AsyncMock(return_value={})

        with self._patch_index_name(), self._patch_client(es):
            await driver.drop_storage(
                _CATALOG_ID, collection_id=_COLLECTION_ID, soft=False
            )

        # delete_metadata uses client.delete (hard) or client.update (soft).
        es.delete.assert_awaited_once()
        call_kwargs = es.delete.call_args
        # Verify routing is the catalog_id.
        params = call_kwargs.kwargs.get("params", {})
        assert params.get("routing") == _CATALOG_ID

    @pytest.mark.asyncio
    async def test_soft_collection_scope_calls_update(self) -> None:
        """Collection scope soft=True uses client.update (tombstone)."""
        driver = self._make_driver()
        es = MagicMock()
        es.update = AsyncMock(return_value={})

        with self._patch_index_name(), self._patch_client(es):
            await driver.drop_storage(
                _CATALOG_ID, collection_id=_COLLECTION_ID, soft=True
            )

        es.update.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_catalog_scope_soft_is_noop(self) -> None:
        """Catalog scope soft=True is a no-op (no ES call)."""
        driver = self._make_driver()
        es = MagicMock()
        es.delete_by_query = AsyncMock()

        with self._patch_index_name(), self._patch_client(es):
            await driver.drop_storage(_CATALOG_ID, collection_id=None, soft=True)

        es.delete_by_query.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_catalog_scope_es_exception_is_swallowed(self) -> None:
        """Fail-soft: delete_by_query error logs WARNING but does not raise."""
        driver = self._make_driver()
        es = MagicMock()
        es.delete_by_query = AsyncMock(side_effect=OSError("cluster unavailable"))

        with self._patch_index_name(), self._patch_client(es):
            # Must not raise — fail-soft matches other drivers' drop_storage contract.
            await driver.drop_storage(_CATALOG_ID, collection_id=None, soft=False)

    @pytest.mark.asyncio
    async def test_no_client_is_noop(self) -> None:
        """No ES client → silent no-op (driver not installed)."""
        driver = self._make_driver()
        with self._patch_client(None):
            await driver.drop_storage(_CATALOG_ID, collection_id=None, soft=False)
