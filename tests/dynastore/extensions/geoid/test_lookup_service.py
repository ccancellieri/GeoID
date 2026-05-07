"""Unit tests for the geoid extension's PG-backed lookup helpers."""
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
