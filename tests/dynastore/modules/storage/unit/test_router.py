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

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.modules.storage.errors import ReadOnlyCollectionError
from dynastore.modules.storage.router import (
    ResolvedDriver,
    get_asset_driver,
    get_asset_search_driver,
    get_asset_write_drivers,
    get_driver,
    get_items_search_driver,
    get_write_drivers,
    resolve_drivers,
)
from dynastore.modules.storage.driver_registry import DriverRegistry
from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.routing_config import (
    AssetRoutingConfig,
    FailurePolicy,
    Operation,
    OperationDriverEntry,
    ItemsRoutingConfig,
)


def _make_routing(operations: dict) -> ItemsRoutingConfig:
    """Build a ItemsRoutingConfig from {operation: [(driver_ref, hints, policy), ...]}."""
    ops = {}
    for op, entries in operations.items():
        ops[op] = [
            OperationDriverEntry(
                driver_ref=e[0],
                hints=e[1] if len(e) > 1 else set(),
                on_failure=e[2] if len(e) > 2 else FailurePolicy.FATAL,
            )
            for e in entries
        ]
    return ItemsRoutingConfig(operations=ops)


def _mock_configs_protocol(routing_config):
    """Return a mock ConfigsProtocol that returns the given routing config."""
    mock = MagicMock()
    mock.get_config = AsyncMock(return_value=routing_config)
    return mock


def _mock_driver(driver_ref: str, supported_hints=frozenset()):
    """Create a mock driver whose class name equals ``driver_ref``.

    The router builds the driver index via ``type(driver).__name__`` after the
    ``driver_ref`` field was removed in favour of class-name routing keys, so
    mocks must carry that name in their type.
    """
    cls = type(driver_ref, (MagicMock,), {"supported_hints": supported_hints})
    return cls()


# ---------------------------------------------------------------------------
# DriverRegistry — L0 singleton
# ---------------------------------------------------------------------------


class TestDriverRegistry:
    def test_collection_index_built_from_get_protocols(self):
        d1 = _mock_driver("postgresql")
        d2 = _mock_driver("elasticsearch")
        DriverRegistry.clear()
        with patch("dynastore.tools.discovery.get_protocols", return_value=[d1, d2]):
            index = DriverRegistry.collection_index()
            assert index == {"postgresql": d1, "elasticsearch": d2}
        DriverRegistry.clear()

    def test_asset_index_built_from_get_protocols(self):
        d1 = _mock_driver("postgresql")
        DriverRegistry.clear()
        with patch("dynastore.tools.discovery.get_protocols", return_value=[d1]):
            index = DriverRegistry.asset_index()
            assert index == {"postgresql": d1}
        DriverRegistry.clear()

    def test_empty_registry(self):
        DriverRegistry.clear()
        with patch("dynastore.tools.discovery.get_protocols", return_value=[]):
            assert DriverRegistry.collection_index() == {}
            assert DriverRegistry.asset_index() == {}
        DriverRegistry.clear()

    def test_duplicate_driver_id_last_wins(self):
        d1 = _mock_driver("postgresql")
        d2 = _mock_driver("postgresql")
        DriverRegistry.clear()
        with patch("dynastore.tools.discovery.get_protocols", return_value=[d1, d2]):
            index = DriverRegistry.collection_index()
            assert index["postgresql"] is d2
        DriverRegistry.clear()

    def test_clear_forces_rebuild(self):
        d1 = _mock_driver("postgresql")
        d2 = _mock_driver("elasticsearch")
        DriverRegistry.clear()
        with patch("dynastore.tools.discovery.get_protocols", return_value=[d1]):
            first = DriverRegistry.collection_index()
            assert "postgresql" in first

        DriverRegistry.clear()
        with patch("dynastore.tools.discovery.get_protocols", return_value=[d2]):
            second = DriverRegistry.collection_index()
            assert "elasticsearch" in second
        DriverRegistry.clear()

    def test_register_plugin_clears_registry(self):
        """register_plugin must invalidate DriverRegistry so stale index is not served."""
        from dynastore.tools.discovery import register_plugin, unregister_plugin

        d1 = _mock_driver("postgresql")
        DriverRegistry.clear()
        with patch("dynastore.tools.discovery.get_protocols", return_value=[d1]):
            first = DriverRegistry.collection_index()
            assert "postgresql" in first

        # Registering a new plugin should clear the registry
        sentinel = _mock_driver("newdriver")
        with patch("dynastore.tools.discovery.get_protocols", return_value=[d1, sentinel]):
            register_plugin(sentinel)
            second = DriverRegistry.collection_index()
            assert "newdriver" in second

        unregister_plugin(sentinel)
        DriverRegistry.clear()


# ---------------------------------------------------------------------------
# resolve_drivers — cached resolution via ConfigsProtocol
# ---------------------------------------------------------------------------


class TestResolveDrivers:
    @pytest.mark.asyncio
    async def test_write_returns_all_drivers(self):
        routing = _make_routing({
            Operation.WRITE: [("postgresql", set()), ("elasticsearch", set())],
        })
        mock_configs = _mock_configs_protocol(routing)
        pg = _mock_driver("postgresql")
        es = _mock_driver("elasticsearch")

        with (
            patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
            patch.object(DriverRegistry, "collection_index", return_value={"postgresql": pg, "elasticsearch": es}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("postgresql", FailurePolicy.FATAL), ("elasticsearch", FailurePolicy.FATAL)])),
        ):
            result = await resolve_drivers("WRITE", "cat1", "col1")
            assert len(result) == 2
            assert result[0].driver is pg
            assert result[1].driver is es
            assert result[0].on_failure == FailurePolicy.FATAL

    @pytest.mark.asyncio
    async def test_read_returns_single_driver(self):
        pg = _mock_driver("postgresql")

        with (
            patch.object(DriverRegistry, "collection_index", return_value={"postgresql": pg}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("postgresql", FailurePolicy.FATAL)])),
        ):
            result = await resolve_drivers("READ", "cat1", "col1")
            assert len(result) == 1
            assert result[0].driver is pg

    @pytest.mark.asyncio
    async def test_missing_driver_is_skipped(self):
        pg = _mock_driver("postgresql")

        with (
            patch.object(DriverRegistry, "collection_index", return_value={"postgresql": pg}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("nonexistent", FailurePolicy.FATAL), ("postgresql", FailurePolicy.FATAL)])),
        ):
            result = await resolve_drivers("READ", "cat1")
            assert len(result) == 1
            assert result[0].driver is pg

    @pytest.mark.asyncio
    async def test_empty_resolution(self):
        with (
            patch.object(DriverRegistry, "collection_index", return_value={}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(return_value=[])),
        ):
            result = await resolve_drivers("READ", "cat1")
            assert result == []

    @pytest.mark.asyncio
    async def test_asset_routing_uses_asset_driver_index(self):
        pg = _mock_driver("postgresql")

        with (
            patch.object(DriverRegistry, "asset_index", return_value={"postgresql": pg}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("postgresql", FailurePolicy.FATAL)])),
        ):
            result = await resolve_drivers(
                "READ", "cat1", routing_plugin_cls=AssetRoutingConfig,
            )
            assert len(result) == 1
            assert result[0].driver is pg

    @pytest.mark.asyncio
    async def test_failure_policy_preserved(self):
        pg = _mock_driver("postgresql")

        with (
            patch.object(DriverRegistry, "collection_index", return_value={"postgresql": pg}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("postgresql", FailurePolicy.WARN)])),
        ):
            result = await resolve_drivers("WRITE", "cat1")
            assert result[0].on_failure == FailurePolicy.WARN

    @pytest.mark.asyncio
    async def test_index_operation_resolves(self):
        """INDEX is a plain operation key for resolve_drivers, same wiring as
        WRITE/READ — the lane-level strict/relaxed distinction lives inside
        ``_resolve_driver_ids_cached``, not in ``resolve_drivers`` itself."""
        es = _mock_driver("elasticsearch")

        with (
            patch.object(DriverRegistry, "collection_index", return_value={"elasticsearch": es}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("elasticsearch", FailurePolicy.FATAL)])),
        ):
            result = await resolve_drivers(Operation.INDEX, "cat1", "col1")
            assert len(result) == 1
            assert result[0].driver is es


# ---------------------------------------------------------------------------
# get_driver / get_asset_driver — convenience wrappers
# ---------------------------------------------------------------------------


class TestGetDriver:
    @pytest.mark.asyncio
    async def test_returns_first_driver(self):
        pg = _mock_driver("postgresql")

        with (
            patch.object(DriverRegistry, "collection_index", return_value={"postgresql": pg}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("postgresql", FailurePolicy.FATAL)])),
        ):
            result = await get_driver("READ", "cat1", "col1")
            assert result is pg

    @pytest.mark.asyncio
    async def test_raises_on_empty_resolution(self):
        with (
            patch.object(DriverRegistry, "collection_index", return_value={}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(return_value=[])),
        ):
            with pytest.raises(ValueError, match="No collection driver found"):
                await get_driver("READ", "cat1", "col1")

    @pytest.mark.asyncio
    async def test_write_operation(self):
        pg = _mock_driver("postgresql")

        with (
            patch.object(DriverRegistry, "collection_index", return_value={"postgresql": pg}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("postgresql", FailurePolicy.FATAL)])),
        ):
            result = await get_driver("WRITE", "cat1", "col1")
            assert result is pg


class TestGetAssetDriver:
    @pytest.mark.asyncio
    async def test_returns_first_asset_driver(self):
        pg = _mock_driver("postgresql")

        with (
            patch.object(DriverRegistry, "asset_index", return_value={"postgresql": pg}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("postgresql", FailurePolicy.FATAL)])),
        ):
            result = await get_asset_driver("READ", "cat1", "col1")
            assert result is pg

    @pytest.mark.asyncio
    async def test_raises_value_error_on_empty_read_resolution(self):
        """A genuine misconfiguration (no READ driver at all) is a ValueError —
        distinct from the read-only-WRITE case below, which is a typed 405."""
        with (
            patch.object(DriverRegistry, "asset_index", return_value={}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(return_value=[])),
        ):
            with pytest.raises(ValueError, match="No asset driver found"):
                await get_asset_driver("READ", "cat1", "col1")

    @pytest.mark.asyncio
    async def test_raises_read_only_collection_error_on_empty_write_resolution(self):
        """An empty WRITE lane means the asset tier is read-only — the typed
        405 path, not a generic ValueError."""
        with (
            patch.object(DriverRegistry, "asset_index", return_value={}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(return_value=[])),
        ):
            with pytest.raises(ReadOnlyCollectionError):
                await get_asset_driver("WRITE", "cat1", "col1")

    @pytest.mark.asyncio
    async def test_write_operation(self):
        pg = _mock_driver("postgresql")

        with (
            patch.object(DriverRegistry, "asset_index", return_value={"postgresql": pg}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("postgresql", FailurePolicy.FATAL)])),
        ):
            result = await get_asset_driver("WRITE", "cat1", "col1")
            assert result is pg


# ---------------------------------------------------------------------------
# get_write_drivers / get_asset_write_drivers — empty is valid (read-only)
# ---------------------------------------------------------------------------


class TestWriteDriversEmptyIsValid:
    @pytest.mark.asyncio
    async def test_get_write_drivers_returns_empty_list_not_raise(self):
        """An empty WRITE lane is a valid read-only configuration — the
        multi-driver fan-out helper never raises; callers reject at
        dispatch (see ReadOnlyCollectionError in item_service.upsert)."""
        with (
            patch.object(DriverRegistry, "collection_index", return_value={}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(return_value=[])),
        ):
            result = await get_write_drivers("cat1", "col1")
            assert result == []

    @pytest.mark.asyncio
    async def test_get_asset_write_drivers_returns_empty_list_not_raise(self):
        with (
            patch.object(DriverRegistry, "asset_index", return_value={}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(return_value=[])),
        ):
            result = await get_asset_write_drivers("cat1", "col1")
            assert result == []

    @pytest.mark.asyncio
    async def test_get_write_drivers_returns_resolved_fan_out(self):
        pg = _mock_driver("postgresql")

        with (
            patch.object(DriverRegistry, "collection_index", return_value={"postgresql": pg}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("postgresql", FailurePolicy.FATAL)])),
        ):
            result = await get_write_drivers("cat1", "col1")
            assert len(result) == 1
            assert result[0].driver is pg


# ---------------------------------------------------------------------------
# get_items_search_driver / get_asset_search_driver — derived search
# ---------------------------------------------------------------------------


class TestDerivedSearchDriver:
    @pytest.mark.asyncio
    async def test_items_search_prefers_index_lane(self):
        es = _mock_driver("elasticsearch")

        async def _fake_resolve(operation, *a, **kw):
            if operation == Operation.INDEX:
                return [ResolvedDriver(driver=es)]
            return []

        with patch(
            "dynastore.modules.storage.router.resolve_drivers", new=AsyncMock(side_effect=_fake_resolve),
        ):
            resolved = await get_items_search_driver("cat1", "col1")
            assert resolved.driver is es

    @pytest.mark.asyncio
    async def test_items_search_falls_back_to_read_when_index_empty(self):
        pg = _mock_driver("postgresql")

        async def _fake_resolve(operation, *a, **kw):
            if operation == Operation.INDEX:
                return []
            if operation == Operation.READ:
                return [ResolvedDriver(driver=pg)]
            return []

        with patch(
            "dynastore.modules.storage.router.resolve_drivers", new=AsyncMock(side_effect=_fake_resolve),
        ):
            resolved = await get_items_search_driver("cat1", "col1")
            assert resolved.driver is pg

    @pytest.mark.asyncio
    async def test_items_search_raises_when_both_lanes_empty(self):
        with patch(
            "dynastore.modules.storage.router.resolve_drivers", new=AsyncMock(return_value=[]),
        ):
            with pytest.raises(ValueError, match="No items search driver found"):
                await get_items_search_driver("cat1", "col1")

    @pytest.mark.asyncio
    async def test_asset_search_prefers_index_lane_over_read(self):
        es = _mock_driver("asset_elasticsearch_driver")
        pg = _mock_driver("asset_postgresql_driver")

        async def _fake_resolve(operation, *a, **kw):
            if operation == Operation.INDEX:
                return [ResolvedDriver(driver=es)]
            if operation == Operation.READ:
                return [ResolvedDriver(driver=pg)]
            return []

        with patch(
            "dynastore.modules.storage.router.resolve_drivers", new=AsyncMock(side_effect=_fake_resolve),
        ):
            result = await get_asset_search_driver("cat1", "col1")
            assert result is es

    @pytest.mark.asyncio
    async def test_asset_search_raises_when_both_lanes_empty(self):
        with patch(
            "dynastore.modules.storage.router.resolve_drivers", new=AsyncMock(return_value=[]),
        ):
            with pytest.raises(ValueError, match="No asset search driver found"):
                await get_asset_search_driver("cat1", "col1")


class TestRankSearchPoolByHintSearch:
    def test_search_tagged_driver_ranked_first(self):
        from dynastore.modules.storage.router import _rank_search_pool_by_hint_search

        plain = ResolvedDriver(driver=_mock_driver("postgresql", supported_hints=frozenset()))
        searchable = ResolvedDriver(
            driver=_mock_driver("elasticsearch", supported_hints=frozenset({Hint.SEARCH})),
        )
        ranked = _rank_search_pool_by_hint_search([plain, searchable])
        assert ranked[0] is searchable
        assert ranked[1] is plain

    def test_stable_when_none_are_search_tagged(self):
        from dynastore.modules.storage.router import _rank_search_pool_by_hint_search

        a = ResolvedDriver(driver=_mock_driver("a"))
        b = ResolvedDriver(driver=_mock_driver("b"))
        assert _rank_search_pool_by_hint_search([a, b]) == [a, b]


# ---------------------------------------------------------------------------
# ResolvedDriver
# ---------------------------------------------------------------------------


class TestResolvedDriver:
    def test_driver_id_property(self):
        d = _mock_driver("postgresql")
        rd = ResolvedDriver(driver=d)
        assert rd.driver_ref == "postgresql"

    def test_default_failure_policy(self):
        rd = ResolvedDriver(driver=_mock_driver("pg"))
        assert rd.on_failure == FailurePolicy.FATAL

    def test_custom_failure_policy(self):
        rd = ResolvedDriver(driver=_mock_driver("es"), on_failure=FailurePolicy.WARN)
        assert rd.on_failure == FailurePolicy.WARN

    def test_driver_id_falls_back_to_class_name(self):
        rd = ResolvedDriver(driver=object())
        assert rd.driver_ref == "object"
