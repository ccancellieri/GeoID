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

"""Tests for TeardownLane-based dispatch in RoutingDrivenCascadeOwner.

Covers:
1. Parametrized: each concrete driver class declares the expected lane.
2. _enumerate_configured_drivers with fake DriverRegistry:
   - ASYNC_CASCADE refs are enqueued.
   - INLINE_TXN / ASYNC_DEDICATED / NONE are skipped.
   - A driver_ref absent from the registry is still enqueued (fail-safe).
3. Regression: ES-only scope enqueues all three ES drivers.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.models.protocols.teardown_lane import TeardownLane
from dynastore.modules.catalog.resource_owner import ResourceScope, ScopeRef
from dynastore.modules.storage.drivers.routing_driven_cascade_owner import (
    _REGISTRY_ASSET,
    _REGISTRY_COLLECTION,
    _REGISTRY_ITEMS,
    _enumerate_configured_drivers,
)

_CATALOG_ID = "test-catalog"
_COLLECTION_ID = "test-collection"


# ---------------------------------------------------------------------------
# 1. Parametrized driver-lane declarations
# ---------------------------------------------------------------------------


class TestDriverLaneDeclarations:
    """Each concrete driver class must declare the expected TeardownLane."""

    # PG family → INLINE_TXN
    @pytest.mark.parametrize("driver_cls_import,expected_lane", [
        (
            "dynastore.modules.storage.drivers.postgresql.ItemsPostgresqlDriver",
            TeardownLane.INLINE_TXN,
        ),
        (
            "dynastore.modules.catalog.drivers.pg_asset_driver.AssetPostgresqlDriver",
            TeardownLane.INLINE_TXN,
        ),
        (
            "dynastore.modules.storage.drivers.collection_postgresql.CollectionPostgresqlDriver",
            TeardownLane.INLINE_TXN,
        ),
        (
            "dynastore.modules.storage.drivers.catalog_postgresql.CatalogPostgresqlDriver",
            TeardownLane.INLINE_TXN,
        ),
        # BigQuery → NONE
        (
            "dynastore.modules.storage.drivers.bigquery.ItemsBigQueryDriver",
            TeardownLane.NONE,
        ),
        # ES items / asset / collection / catalog → ASYNC_CASCADE (default)
        (
            "dynastore.modules.storage.drivers.elasticsearch.ItemsElasticsearchDriver",
            TeardownLane.ASYNC_CASCADE,
        ),
        (
            "dynastore.modules.storage.drivers.elasticsearch.AssetElasticsearchDriver",
            TeardownLane.ASYNC_CASCADE,
        ),
        (
            "dynastore.modules.elasticsearch.collection_es_driver.CollectionElasticsearchDriver",
            TeardownLane.ASYNC_CASCADE,
        ),
        (
            "dynastore.modules.elasticsearch.catalog_es_driver.CatalogElasticsearchDriver",
            TeardownLane.ASYNC_CASCADE,
        ),
        # ES private items → ASYNC_CASCADE
        (
            "dynastore.modules.storage.drivers.elasticsearch_private.driver.ItemsElasticsearchPrivateDriver",
            TeardownLane.ASYNC_CASCADE,
        ),
        # DuckDB → ASYNC_CASCADE
        (
            "dynastore.modules.storage.drivers.duckdb.ItemsDuckdbDriver",
            TeardownLane.ASYNC_CASCADE,
        ),
        # Iceberg → ASYNC_CASCADE
        (
            "dynastore.modules.storage.drivers.iceberg.ItemsIcebergDriver",
            TeardownLane.ASYNC_CASCADE,
        ),
    ])
    def test_driver_declares_expected_lane(
        self, driver_cls_import: str, expected_lane: TeardownLane
    ) -> None:
        module_path, cls_name = driver_cls_import.rsplit(".", 1)
        import importlib
        mod = importlib.import_module(module_path)
        cls = getattr(mod, cls_name)
        actual = getattr(cls, "teardown_lane", TeardownLane.ASYNC_CASCADE)
        assert actual is expected_lane, (
            f"{cls_name}.teardown_lane = {actual!r}, expected {expected_lane!r}"
        )


# ---------------------------------------------------------------------------
# 2. _enumerate_configured_drivers with lane-based filtering
# ---------------------------------------------------------------------------


def _make_fake_driver(lane: TeardownLane) -> Any:
    """Return a simple object with a teardown_lane attribute."""
    class _FakeDriver:
        teardown_lane = lane
    return _FakeDriver()


class TestEnumerateLaneDispatch:
    """_enumerate_configured_drivers filters by teardown_lane, not by name."""

    @pytest.mark.asyncio
    async def test_async_cascade_drivers_are_enqueued(self) -> None:
        """Drivers with ASYNC_CASCADE lane must appear in the result."""
        es_driver = _make_fake_driver(TeardownLane.ASYNC_CASCADE)

        async def _mock_resolve(routing_cls, catalog_id, collection_id, op, hints):
            from dynastore.modules.storage.routing_config import ItemsRoutingConfig
            if routing_cls is ItemsRoutingConfig:
                return [("items_elasticsearch_driver", "FATAL", "SYNC")]
            return []

        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)

        with (
            patch(
                "dynastore.modules.storage.router._resolve_driver_ids_cached",
                new=AsyncMock(side_effect=_mock_resolve),
            ),
            patch(
                "dynastore.modules.storage.drivers.routing_driven_cascade_owner._resolve_driver_by_parts",
                return_value=es_driver,
            ),
        ):
            result = await _enumerate_configured_drivers(scope_ref)

        assert (_REGISTRY_ITEMS, "items_elasticsearch_driver") in result

    @pytest.mark.asyncio
    async def test_inline_txn_drivers_are_skipped(self) -> None:
        """Drivers with INLINE_TXN lane must be excluded from the result."""
        pg_driver = _make_fake_driver(TeardownLane.INLINE_TXN)

        async def _mock_resolve(routing_cls, catalog_id, collection_id, op, hints):
            from dynastore.modules.storage.routing_config import ItemsRoutingConfig
            if routing_cls is ItemsRoutingConfig:
                return [("items_postgresql_driver", "FATAL", "SYNC")]
            return []

        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)

        with (
            patch(
                "dynastore.modules.storage.router._resolve_driver_ids_cached",
                new=AsyncMock(side_effect=_mock_resolve),
            ),
            patch(
                "dynastore.modules.storage.drivers.routing_driven_cascade_owner._resolve_driver_by_parts",
                return_value=pg_driver,
            ),
        ):
            result = await _enumerate_configured_drivers(scope_ref)

        assert (_REGISTRY_ITEMS, "items_postgresql_driver") not in result

    @pytest.mark.asyncio
    async def test_async_dedicated_drivers_are_skipped(self) -> None:
        """Drivers with ASYNC_DEDICATED lane must not be enqueued."""
        dedicated_driver = _make_fake_driver(TeardownLane.ASYNC_DEDICATED)

        async def _mock_resolve(routing_cls, catalog_id, collection_id, op, hints):
            from dynastore.modules.storage.routing_config import AssetRoutingConfig
            if routing_cls is AssetRoutingConfig:
                return [("gcs_asset_driver", "WARN", "SYNC")]
            return []

        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)

        with (
            patch(
                "dynastore.modules.storage.router._resolve_driver_ids_cached",
                new=AsyncMock(side_effect=_mock_resolve),
            ),
            patch(
                "dynastore.modules.storage.drivers.routing_driven_cascade_owner._resolve_driver_by_parts",
                return_value=dedicated_driver,
            ),
        ):
            result = await _enumerate_configured_drivers(scope_ref)

        driver_refs = [dr for _, dr in result]
        assert "gcs_asset_driver" not in driver_refs

    @pytest.mark.asyncio
    async def test_none_lane_drivers_are_skipped(self) -> None:
        """Drivers with NONE lane must not be enqueued."""
        bq_driver = _make_fake_driver(TeardownLane.NONE)

        async def _mock_resolve(routing_cls, catalog_id, collection_id, op, hints):
            from dynastore.modules.storage.routing_config import ItemsRoutingConfig
            if routing_cls is ItemsRoutingConfig:
                return [("items_big_query_driver", "WARN", "SYNC")]
            return []

        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)

        with (
            patch(
                "dynastore.modules.storage.router._resolve_driver_ids_cached",
                new=AsyncMock(side_effect=_mock_resolve),
            ),
            patch(
                "dynastore.modules.storage.drivers.routing_driven_cascade_owner._resolve_driver_by_parts",
                return_value=bq_driver,
            ),
        ):
            result = await _enumerate_configured_drivers(scope_ref)

        driver_refs = [dr for _, dr in result]
        assert "items_big_query_driver" not in driver_refs

    @pytest.mark.asyncio
    async def test_absent_driver_defaults_to_async_cascade(self) -> None:
        """A driver_ref absent from the registry (None) defaults to ASYNC_CASCADE.

        Fail-safe: cleanup_one treats a missing driver as DONE, so enqueuing
        is safe and correct — we must never silently drop a teardown item.
        """
        async def _mock_resolve(routing_cls, catalog_id, collection_id, op, hints):
            from dynastore.modules.storage.routing_config import ItemsRoutingConfig
            if routing_cls is ItemsRoutingConfig:
                return [("mystery_driver", "FATAL", "SYNC")]
            return []

        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)

        with (
            patch(
                "dynastore.modules.storage.router._resolve_driver_ids_cached",
                new=AsyncMock(side_effect=_mock_resolve),
            ),
            patch(
                "dynastore.modules.storage.drivers.routing_driven_cascade_owner._resolve_driver_by_parts",
                return_value=None,  # driver not in registry
            ),
        ):
            result = await _enumerate_configured_drivers(scope_ref)

        # Must be enqueued despite absence from registry
        assert (_REGISTRY_ITEMS, "mystery_driver") in result

    @pytest.mark.asyncio
    async def test_mixed_lanes_only_async_cascade_enqueued(self) -> None:
        """Items scope with mixed lanes: only ASYNC_CASCADE driver is enqueued."""
        async def _mock_resolve(routing_cls, catalog_id, collection_id, op, hints):
            from dynastore.modules.storage.routing_config import ItemsRoutingConfig
            if routing_cls is ItemsRoutingConfig and op == "WRITE":
                return [
                    ("items_elasticsearch_driver", "FATAL", "SYNC"),
                    ("items_postgresql_driver", "FATAL", "SYNC"),
                    ("items_big_query_driver", "WARN", "SYNC"),
                ]
            return []

        driver_map = {
            "items_elasticsearch_driver": _make_fake_driver(TeardownLane.ASYNC_CASCADE),
            "items_postgresql_driver": _make_fake_driver(TeardownLane.INLINE_TXN),
            "items_big_query_driver": _make_fake_driver(TeardownLane.NONE),
        }

        def _fake_resolve_by_parts(registry_kind: str, driver_ref: str) -> Any:
            return driver_map.get(driver_ref)

        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)

        with (
            patch(
                "dynastore.modules.storage.router._resolve_driver_ids_cached",
                new=AsyncMock(side_effect=_mock_resolve),
            ),
            patch(
                "dynastore.modules.storage.drivers.routing_driven_cascade_owner._resolve_driver_by_parts",
                side_effect=_fake_resolve_by_parts,
            ),
        ):
            result = await _enumerate_configured_drivers(scope_ref)

        driver_refs = [dr for _, dr in result]
        assert "items_elasticsearch_driver" in driver_refs
        assert "items_postgresql_driver" not in driver_refs
        assert "items_big_query_driver" not in driver_refs


# ---------------------------------------------------------------------------
# 3. Regression: ES-only scope enqueues all three ES drivers
# ---------------------------------------------------------------------------


class TestEsOnlyScopeRegression:
    """An ES-only routing scope (items + asset + collection all ES) must enqueue
    all three ES drivers in a single catalog-scope describe_scope call."""

    @pytest.mark.asyncio
    async def test_es_only_scope_enqueues_all_three_es_drivers(self) -> None:
        es_driver = _make_fake_driver(TeardownLane.ASYNC_CASCADE)

        async def _mock_resolve(routing_cls, catalog_id, collection_id, op, hints):
            from dynastore.modules.storage.routing_config import (
                AssetRoutingConfig,
                CollectionRoutingConfig,
                ItemsRoutingConfig,
            )
            if routing_cls is ItemsRoutingConfig:
                return [("items_elasticsearch_driver", "FATAL", "SYNC")]
            if routing_cls is AssetRoutingConfig:
                return [("asset_elasticsearch_driver", "FATAL", "SYNC")]
            if routing_cls is CollectionRoutingConfig:
                return [("collection_elasticsearch_driver", "FATAL", "SYNC")]
            return []

        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)

        with (
            patch(
                "dynastore.modules.storage.router._resolve_driver_ids_cached",
                new=AsyncMock(side_effect=_mock_resolve),
            ),
            patch(
                "dynastore.modules.storage.drivers.routing_driven_cascade_owner._resolve_driver_by_parts",
                return_value=es_driver,
            ),
        ):
            result = await _enumerate_configured_drivers(scope_ref)

        assert (_REGISTRY_ITEMS, "items_elasticsearch_driver") in result
        assert (_REGISTRY_ASSET, "asset_elasticsearch_driver") in result
        assert (_REGISTRY_COLLECTION, "collection_elasticsearch_driver") in result
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_pg_in_es_scope_not_enqueued(self) -> None:
        """PG driver mixed into an ES scope must be filtered out."""
        driver_map = {
            "items_elasticsearch_driver": _make_fake_driver(TeardownLane.ASYNC_CASCADE),
            "items_postgresql_driver": _make_fake_driver(TeardownLane.INLINE_TXN),
        }

        async def _mock_resolve(routing_cls, catalog_id, collection_id, op, hints):
            from dynastore.modules.storage.routing_config import ItemsRoutingConfig
            if routing_cls is ItemsRoutingConfig:
                return [
                    ("items_elasticsearch_driver", "FATAL", "SYNC"),
                    ("items_postgresql_driver", "FATAL", "SYNC"),
                ]
            return []

        def _fake_resolve_by_parts(registry_kind: str, driver_ref: str) -> Any:
            return driver_map.get(driver_ref)

        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=_CATALOG_ID)

        with (
            patch(
                "dynastore.modules.storage.router._resolve_driver_ids_cached",
                new=AsyncMock(side_effect=_mock_resolve),
            ),
            patch(
                "dynastore.modules.storage.drivers.routing_driven_cascade_owner._resolve_driver_by_parts",
                side_effect=_fake_resolve_by_parts,
            ),
        ):
            result = await _enumerate_configured_drivers(scope_ref)

        driver_refs = [dr for _, dr in result]
        assert "items_elasticsearch_driver" in driver_refs
        assert "items_postgresql_driver" not in driver_refs
