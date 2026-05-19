"""Unit tests for the geoid extension's PG-backed lookup helpers."""
import logging
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
