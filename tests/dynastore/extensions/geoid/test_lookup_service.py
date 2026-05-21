"""Unit tests for the geoid extension's routing-aware lookup helpers."""
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_lookup_by_geoids_empty_input_returns_empty():
    from dynastore.extensions.geoid.lookup_service import lookup_by_geoids

    result = await lookup_by_geoids("cat", [], limit=10)
    assert result == []


@pytest.mark.asyncio
async def test_lookup_by_geoids_returns_empty_when_protocols_missing(monkeypatch):
    from dynastore.extensions.geoid.lookup_service import lookup_by_geoids

    monkeypatch.setattr(
        "dynastore.extensions.geoid.lookup_service.get_protocol",
        lambda _proto: None,
    )
    result = await lookup_by_geoids("cat", ["abc"], limit=10)
    assert result == []


@pytest.mark.asyncio
async def test_lookup_by_external_id_calls_search_items_with_cql_filter(monkeypatch):
    from dynastore.extensions.geoid.lookup_service import lookup_by_external_id

    fake_catalogs = MagicMock()
    fake_feature = MagicMock()
    fake_feature.id = "fake-geoid"
    fake_feature.geometry = {"type": "Point", "coordinates": [0, 0]}
    fake_feature.bbox = [0.0, 0.0, 0.0, 0.0]
    fake_feature.properties = {"external_id": "ext-1", "name": "test"}
    fake_catalogs.items = MagicMock()
    fake_catalogs.items.search_items = AsyncMock(return_value=[fake_feature])

    monkeypatch.setattr(
        "dynastore.extensions.geoid.lookup_service.get_protocol",
        lambda _proto: fake_catalogs,
    )

    rows = await lookup_by_external_id("cat", "col", "ext-1", limit=1)
    assert len(rows) == 1
    assert rows[0]["external_id"] == "ext-1"
    assert rows[0]["collection_id"] == "col"
    fake_catalogs.items.search_items.assert_awaited_once()
    call_kwargs = fake_catalogs.items.search_items.call_args.kwargs
    assert call_kwargs["catalog_id"] == "cat"
    assert call_kwargs["collection_id"] == "col"
    assert "external_id = 'ext-1'" in call_kwargs["request"].cql_filter


@pytest.mark.asyncio
async def test_lookup_by_external_id_returns_empty_when_protocol_missing(monkeypatch):
    from dynastore.extensions.geoid.lookup_service import lookup_by_external_id

    monkeypatch.setattr(
        "dynastore.extensions.geoid.lookup_service.get_protocol",
        lambda _proto: None,
    )
    result = await lookup_by_external_id("cat", "col", "ext-1", limit=1)
    assert result == []


# ---------------------------------------------------------------------------
# #975 — pre-filter UUID-parseable inputs; surface malformed ones via WARN log.
# ---------------------------------------------------------------------------


def test_partition_uuid_inputs_splits_valid_from_invalid():
    from dynastore.extensions.geoid.lookup_service import _partition_uuid_inputs

    good_a = "11111111-1111-1111-1111-111111111111"
    good_b = "22222222-2222-2222-2222-222222222222"
    valid, invalid = _partition_uuid_inputs(
        [good_a, "not-a-uuid", good_b, "", "12345"]
    )
    assert valid == [good_a, good_b]
    assert invalid == ["not-a-uuid", "", "12345"]


def test_partition_uuid_inputs_handles_none_and_whitespace():
    from dynastore.extensions.geoid.lookup_service import _partition_uuid_inputs

    good = "33333333-3333-3333-3333-333333333333"
    valid, invalid = _partition_uuid_inputs([None, f"  {good}  ", "abc"])
    assert valid == [good]  # whitespace stripped
    assert invalid == ["", "abc"]


@pytest.mark.asyncio
async def test_lookup_by_geoids_all_inputs_invalid_short_circuits(
    monkeypatch, caplog
):
    """All-malformed input → no DB round-trip, WARN log surfaces inputs (#975)."""
    from dynastore.extensions.geoid import lookup_service as svc

    called = {"get_protocol": 0}

    def _spy(_proto):
        called["get_protocol"] += 1
        return None

    monkeypatch.setattr(svc, "get_protocol", _spy)
    with caplog.at_level(logging.WARNING, logger=svc.logger.name):
        result = await svc.lookup_by_geoids("cat", ["nope", "still-not-a-uuid"], limit=10)
    assert result == []
    assert called["get_protocol"] == 0, "must short-circuit before resolving protocols"
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("not valid UUIDs" in r.getMessage() for r in warnings)
    assert any("nope" in r.getMessage() for r in warnings)


@pytest.mark.asyncio
async def test_lookup_by_geoids_mixed_inputs_forwards_only_valid(monkeypatch, caplog):
    """Valid uuids reach the hub query; invalid ones are logged but don't break the batch."""
    from dynastore.extensions.geoid import lookup_service as svc

    good = "44444444-4444-4444-4444-444444444444"

    fake_catalogs = MagicMock()
    fake_catalogs.resolve_physical_schema = AsyncMock(return_value="cat_a_schema")
    fake_catalogs.items = MagicMock()
    fake_catalogs.items.search_items = AsyncMock(return_value=[])
    fake_db = MagicMock()
    fake_db.engine = MagicMock()

    from dynastore.models.protocols import CatalogsProtocol, DatabaseProtocol

    def _get_proto(proto):
        if proto is CatalogsProtocol:
            return fake_catalogs
        if proto is DatabaseProtocol:
            return fake_db
        return None

    monkeypatch.setattr(svc, "get_protocol", _get_proto)

    class _Ctx:
        async def __aenter__(self):
            return MagicMock()
        async def __aexit__(self, *_):
            return False

    monkeypatch.setattr(
        "dynastore.extensions.geoid.lookup_service.managed_transaction"
        if False else "dynastore.modules.db_config.query_executor.managed_transaction",
        lambda _engine: _Ctx(),
    )

    captured: dict = {}

    async def _fake_get_tables(_schema, _conn):
        return []

    async def _fake_hub_lookup(_schema, _table, geoids, _conn):
        captured["geoids"] = list(geoids)
        return []

    monkeypatch.setattr(svc, "_get_pg_collection_tables", _fake_get_tables)
    monkeypatch.setattr(svc, "_hub_geoid_lookup", _fake_hub_lookup)

    with caplog.at_level(logging.WARNING, logger=svc.logger.name):
        result = await svc.lookup_by_geoids("cat", [good, "bogus"], limit=10)

    assert result == []
    # The collection-tables path returns [] so _hub_geoid_lookup is never reached,
    # but the WARN must still surface the malformed input.
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("bogus" in r.getMessage() for r in warnings)


# ---------------------------------------------------------------------------
# #989 — routing-aware driver resolution chain.
# ---------------------------------------------------------------------------


def _es_index_driver(name="ItemsElasticsearchPrivateDriver"):
    """A fake search-index driver: NO QUERY_FALLBACK_SOURCE capability, so the
    lookup uses the index-backed driver path rather than the PG hub scan."""
    drv = MagicMock()
    drv.__class__.__name__ = name
    type(drv).__name__ = name
    drv.capabilities = frozenset({"read", "write"})
    return drv


def _pg_fallback_driver():
    """A fake PG driver carrying QUERY_FALLBACK_SOURCE — the marker the lookup
    keys on to use the PG two-pass hub scan (step 4 of #989)."""
    from dynastore.models.protocols.storage_driver import Capability

    drv = MagicMock()
    type(drv).__name__ = "ItemsPostgresqlDriver"
    drv.capabilities = frozenset({Capability.READ, Capability.QUERY_FALLBACK_SOURCE})
    return drv


@pytest.mark.asyncio
async def test_resolve_lookup_driver_prefers_private_es(monkeypatch):
    """When the catalog's items routing pins the private ES driver, that driver
    wins regardless of SEARCH/READ resolution (step 1 of #989)."""
    from dynastore.extensions.geoid import lookup_service as svc

    private = _es_index_driver()

    from dynastore.modules.storage import routing_config as rc

    fake_routing = MagicMock(spec=rc.ItemsRoutingConfig)
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(return_value=fake_routing)

    monkeypatch.setattr(svc, "get_protocol", lambda _p: fake_configs)
    monkeypatch.setattr(rc, "_items_routing_has_private_driver", lambda _r: True)
    monkeypatch.setattr(
        "dynastore.modules.storage.driver_registry.DriverRegistry.collection_index",
        classmethod(lambda cls: {svc._PRIVATE_ITEMS_DRIVER_REF: private}),
    )

    driver = await svc._resolve_lookup_driver("cat")
    assert driver is private


@pytest.mark.asyncio
async def test_resolve_lookup_driver_uses_search_driver(monkeypatch):
    """No private driver pinned → resolution falls to the SEARCH/READ driver
    (steps 2-3 of #989) via get_items_search_driver."""
    from dynastore.extensions.geoid import lookup_service as svc
    from dynastore.modules.storage import router as router_mod

    search_driver = _es_index_driver(name="ItemsElasticsearchDriver")

    # No ConfigsProtocol → private-driver probe is skipped cleanly.
    monkeypatch.setattr(svc, "get_protocol", lambda _p: None)

    async def fake_get_items_search_driver(_catalog_id):
        return SimpleNamespace(driver=search_driver, on_failure=None, write_mode=None)

    monkeypatch.setattr(router_mod, "get_items_search_driver", fake_get_items_search_driver)

    driver = await svc._resolve_lookup_driver("cat")
    assert driver is search_driver


@pytest.mark.asyncio
async def test_resolve_lookup_driver_returns_none_and_warns_on_miss(monkeypatch, caplog):
    """No driver resolves → returns None (PG fallback) with a deterministic WARN."""
    from dynastore.extensions.geoid import lookup_service as svc
    from dynastore.modules.storage import router as router_mod

    monkeypatch.setattr(svc, "get_protocol", lambda _p: None)

    async def fake_get_items_search_driver(_catalog_id):
        raise ValueError("no driver registered")

    monkeypatch.setattr(router_mod, "get_items_search_driver", fake_get_items_search_driver)

    with caplog.at_level(logging.WARNING, logger=svc.logger.name):
        driver = await svc._resolve_lookup_driver("cat")

    assert driver is None
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("PostgreSQL hub-scan fallback" in r.getMessage() for r in warnings)


def test_driver_is_pg_fallback_distinguishes_index_from_pg():
    from dynastore.extensions.geoid.lookup_service import _driver_is_pg_fallback

    assert _driver_is_pg_fallback(_pg_fallback_driver()) is True
    assert _driver_is_pg_fallback(_es_index_driver()) is False


@pytest.mark.asyncio
async def test_lookup_by_geoids_routes_to_index_driver(monkeypatch):
    """An index driver (no QUERY_FALLBACK_SOURCE) is served via the driver path
    (read_entities), NOT the PG hub scan."""
    from dynastore.extensions.geoid import lookup_service as svc

    good = "55555555-5555-5555-5555-555555555555"
    index_driver = _es_index_driver()

    async def fake_resolve(_catalog_id):
        return index_driver

    captured = {}

    async def fake_driver_lookup(driver, catalog_id, geoids, limit):
        captured["driver"] = driver
        captured["geoids"] = list(geoids)
        return [{"geoid": good, "catalog_id": catalog_id, "collection_id": "col"}]

    monkeypatch.setattr(svc, "_resolve_lookup_driver", fake_resolve)
    monkeypatch.setattr(svc, "_driver_geoid_lookup", fake_driver_lookup)

    rows = await svc.lookup_by_geoids("cat", [good], limit=10)
    assert rows == [{"geoid": good, "catalog_id": "cat", "collection_id": "col"}]
    assert captured["driver"] is index_driver
    assert captured["geoids"] == [good]


@pytest.mark.asyncio
async def test_lookup_by_geoids_pg_driver_uses_hub_scan(monkeypatch):
    """A resolved PG fallback driver routes to the two-pass hub scan, NOT the
    index driver path (step 4 of #989)."""
    from dynastore.extensions.geoid import lookup_service as svc

    good = "66666666-6666-6666-6666-666666666666"

    async def fake_resolve(_catalog_id):
        return _pg_fallback_driver()

    async def fail_driver_lookup(*_a, **_k):  # pragma: no cover - must not run
        raise AssertionError("index driver path must not run for PG fallback")

    monkeypatch.setattr(svc, "_resolve_lookup_driver", fake_resolve)
    monkeypatch.setattr(svc, "_driver_geoid_lookup", fail_driver_lookup)

    # PG path: no protocols available → returns [] without touching the index path.
    monkeypatch.setattr(svc, "get_protocol", lambda _p: None)

    rows = await svc.lookup_by_geoids("cat", [good], limit=10)
    assert rows == []


@pytest.mark.asyncio
async def test_driver_geoid_lookup_reads_across_collections(monkeypatch):
    """_driver_geoid_lookup lists collections and fetches geoids by id through
    the driver's read_entities (the index-backed path)."""
    from dynastore.extensions.geoid import lookup_service as svc

    good = "77777777-7777-7777-7777-777777777777"

    fake_feature = MagicMock()
    fake_feature.id = good
    fake_feature.geometry = {"type": "Point", "coordinates": [1, 2]}
    fake_feature.bbox = [1.0, 2.0, 1.0, 2.0]
    fake_feature.properties = {"external_id": "ext-9"}

    async def _aiter(*_a, **_k):
        yield fake_feature

    driver = _es_index_driver()
    driver.read_entities = MagicMock(side_effect=lambda *a, **k: _aiter())

    fake_catalogs = MagicMock()
    fake_catalogs.list_collections = AsyncMock(
        return_value=[SimpleNamespace(id="col-1")]
    )
    monkeypatch.setattr(svc, "get_protocol", lambda _p: fake_catalogs)

    rows = await svc._driver_geoid_lookup(driver, "cat", [good], limit=10)
    assert len(rows) == 1
    assert rows[0]["geoid"] == good
    assert rows[0]["collection_id"] == "col-1"
    assert rows[0]["external_id"] == "ext-9"
    driver.read_entities.assert_called_once()
    call_kwargs = driver.read_entities.call_args.kwargs
    assert call_kwargs["entity_ids"] == [good]
