"""Tile metadata resolution must route via Hint.TILES.

Without the hint, default READ routing returns the first matching driver —
typically Elasticsearch in the FAO config — and ES has no schema/table
identifiers, so tile rendering silently falls back to "No valid collections
found". The fix: tiles_module.get_tile_resolution_params passes
hint=Hint.TILES so routing returns a tile-capable driver (PG today).
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver
from dynastore.modules.tiles import tiles_module


def test_postgresql_driver_advertises_tiles_hint():
    """PG must declare Hint.TILES so routing prefers it for tile reads."""
    assert Hint.TILES in ItemsPostgresqlDriver.supported_hints


def test_default_items_routing_resolves_tiles_hint_to_postgres():
    """Default ItemsRoutingConfig must yield PG when filtering READ by Hint.TILES.

    Regression: 72de4a7 added Hint.TILES to PG's class-level supported_hints
    but the per-entry strict-pinning in _entry_matches blocked fallback when
    the entry's own ``hints`` was non-empty (PG entry pinned to GEOMETRY_EXACT).
    Tile reads against a fresh deploy hit ValueError("No collection driver
    found for ... hint='tiles'"). This test exercises the real default config
    end-to-end through resolve_drivers' filter to pin the contract.
    """
    from dynastore.modules.storage.routing_config import (
        ItemsRoutingConfig, Operation,
    )

    cfg = ItemsRoutingConfig()
    pg_entries = [
        e for e in cfg.operations[Operation.READ]
        if e.driver_ref == "items_postgresql_driver"
    ]
    assert pg_entries, "PG must be in default READ routing"
    # _entry_matches uses strict mode when hints is non-empty: hint must be
    # in entry.hints. PG's class supported_hints is only consulted when the
    # entry's own hints set is empty. So Hint.TILES MUST be pinned on the
    # entry — pinning only on the driver class is not enough.
    assert any(Hint.TILES in e.hints for e in pg_entries), (
        f"PG READ entry must pin Hint.TILES; got "
        f"{[sorted(e.hints) for e in pg_entries]}"
    )


@pytest.mark.asyncio
async def test_get_tile_resolution_params_routes_with_tiles_hint():
    """get_tile_resolution_params must call get_driver with hint=Hint.TILES."""
    tiles_module.get_tile_resolution_params.cache_clear()

    fake_driver = AsyncMock()
    fake_driver.location = AsyncMock(
        return_value=SimpleNamespace(
            identifiers={"schema": "cat1_data", "table": "col1"}
        )
    )

    fake_catalogs = SimpleNamespace(
        configs=AsyncMock(get_config=AsyncMock(return_value=None),
                          set_config=AsyncMock(return_value=None)),
        get_collection_config=AsyncMock(return_value=None),
    )

    get_driver_mock = AsyncMock(return_value=fake_driver)

    # Stand-in for the DriverContext class: skip strict pydantic validation
    # of db_resource (the conn fixture below is a bare AsyncMock, not a real
    # AsyncConnection). The test only cares that get_driver receives hint=
    # Hint.TILES, not how the surrounding code wires the context.
    fake_driver_ctx = lambda **kw: kw  # noqa: E731 — local stub
    with (
        patch.object(tiles_module, "_get_engine", return_value=AsyncMock()),
        patch("dynastore.modules.tiles.tiles_module.managed_transaction") as mt,
        patch("dynastore.modules.tiles.tiles_module.get_protocol",
              return_value=fake_catalogs),
        patch("dynastore.modules.storage.router.get_driver", new=get_driver_mock),
        patch.object(tiles_module, "get_collection_source_srid",
                     new=AsyncMock(return_value=4326)),
        patch.object(tiles_module, "DriverContext", new=fake_driver_ctx),
    ):
        # async context manager stub for managed_transaction
        mt.return_value.__aenter__ = AsyncMock(return_value=AsyncMock())
        mt.return_value.__aexit__ = AsyncMock(return_value=None)

        await tiles_module.get_tile_resolution_params("cat1", "col1")

    # Assert the load-bearing call signature: hints=frozenset({Hint.TILES})
    assert get_driver_mock.await_count == 1
    _args, kwargs = get_driver_mock.await_args
    assert kwargs.get("hints") == frozenset({Hint.TILES}), (
        f"get_tile_resolution_params must pass hints=frozenset({{Hint.TILES}}); "
        f"got kwargs={kwargs}"
    )


@pytest.mark.asyncio
async def test_location_failure_is_not_misreported_as_routing(caplog):
    """A driver.location() failure must surface the REAL error, not "add Hint.TILES".

    Regression: a catalog with no ``physical_schema`` in ``catalog.catalogs``
    makes the PG driver's ``location()`` raise ``ValueError("No physical schema
    found …")``. The handler used to wrap both ``get_driver`` and ``location``
    in one ``except`` and label every failure "no tile-capable driver — add
    Hint.TILES …", sending operators down a routing/hint dead end. The driver
    resolved fine here; the message must say so and carry the real exception.
    """
    tiles_module.get_tile_resolution_params.cache_clear()

    fake_driver = AsyncMock()
    fake_driver.location = AsyncMock(
        side_effect=ValueError("No physical schema found for catalog 'cat1'")
    )

    fake_catalogs = SimpleNamespace(
        configs=AsyncMock(get_config=AsyncMock(return_value=None),
                          set_config=AsyncMock(return_value=None)),
        get_collection_config=AsyncMock(return_value=None),
    )

    get_driver_mock = AsyncMock(return_value=fake_driver)
    fake_driver_ctx = lambda **kw: kw  # noqa: E731 — local stub
    with (
        patch.object(tiles_module, "_get_engine", return_value=AsyncMock()),
        patch("dynastore.modules.tiles.tiles_module.managed_transaction") as mt,
        patch("dynastore.modules.tiles.tiles_module.get_protocol",
              return_value=fake_catalogs),
        patch("dynastore.modules.storage.router.get_driver", new=get_driver_mock),
        patch.object(tiles_module, "get_collection_source_srid",
                     new=AsyncMock(return_value=4326)),
        patch.object(tiles_module, "DriverContext", new=fake_driver_ctx),
        caplog.at_level("WARNING", logger="dynastore.modules.tiles.tiles_module"),
    ):
        mt.return_value.__aenter__ = AsyncMock(return_value=AsyncMock())
        mt.return_value.__aexit__ = AsyncMock(return_value=None)

        result = await tiles_module.get_tile_resolution_params("cat1", "col1")

    assert result == {}, "a location failure must yield no resolution params"
    messages = " ".join(r.getMessage() for r in caplog.records)
    # The real cause must be visible …
    assert "could not resolve a physical location" in messages
    assert "No physical schema found for catalog 'cat1'" in messages
    # … and it must NOT be misattributed to routing/hints.
    assert "no tile-capable driver" not in messages
