import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.modules.storage.router import (
    ResolvedDriver,
    get_asset_driver,
    get_driver,
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
    WriteMode,
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


def _mock_driver(driver_ref: str):
    """Create a mock driver whose class name equals ``driver_ref``.

    The router builds the driver index via ``type(driver).__name__`` after the
    ``driver_ref`` field was removed in favour of class-name routing keys, so
    mocks must carry that name in their type.
    """
    cls = type(driver_ref, (MagicMock,), {})
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
                return_value=[("postgresql", FailurePolicy.FATAL, WriteMode.SYNC), ("elasticsearch", FailurePolicy.FATAL, WriteMode.SYNC)])),
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
                return_value=[("postgresql", FailurePolicy.FATAL, WriteMode.SYNC)])),
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
                return_value=[("nonexistent", FailurePolicy.FATAL, WriteMode.SYNC), ("postgresql", FailurePolicy.FATAL, WriteMode.SYNC)])),
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
                return_value=[("postgresql", FailurePolicy.FATAL, WriteMode.SYNC)])),
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
                return_value=[("postgresql", FailurePolicy.WARN, WriteMode.SYNC)])),
        ):
            result = await resolve_drivers("WRITE", "cat1")
            assert result[0].on_failure == FailurePolicy.WARN


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
                return_value=[("postgresql", FailurePolicy.FATAL, WriteMode.SYNC)])),
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
                return_value=[("postgresql", FailurePolicy.FATAL, WriteMode.SYNC)])),
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
                return_value=[("postgresql", FailurePolicy.FATAL, WriteMode.SYNC)])),
        ):
            result = await get_asset_driver("READ", "cat1", "col1")
            assert result is pg

    @pytest.mark.asyncio
    async def test_raises_on_empty_resolution(self):
        with (
            patch.object(DriverRegistry, "asset_index", return_value={}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(return_value=[])),
        ):
            with pytest.raises(ValueError, match="No asset driver found"):
                await get_asset_driver("READ", "cat1", "col1")

    @pytest.mark.asyncio
    async def test_write_operation(self):
        pg = _mock_driver("postgresql")

        with (
            patch.object(DriverRegistry, "asset_index", return_value={"postgresql": pg}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("postgresql", FailurePolicy.FATAL, WriteMode.SYNC)])),
        ):
            result = await get_asset_driver("WRITE", "cat1", "col1")
            assert result is pg

    @pytest.mark.asyncio
    async def test_search_with_hint(self):
        es = _mock_driver("elasticsearch")

        with (
            patch.object(DriverRegistry, "asset_index", return_value={"elasticsearch": es}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("elasticsearch", FailurePolicy.FATAL, WriteMode.SYNC)])),
        ):
            result = await get_asset_driver(
                "SEARCH", "cat1", "col1",
                hints=frozenset({Hint.SEARCH}),
            )
            assert result is es


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
