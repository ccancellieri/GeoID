"""
Unit tests for stream_features FieldSelection construction and consumer wiring.

Verifies:
 - unfiltered (property_names=None) without SRID → exactly one FieldSelection: field="*"
 - unfiltered with target_srid → two FieldSelections: transformed geom + field="*"
 - consumer=ConsumerType.OGC_FEATURES is always passed to stream_items

These tests mock ItemsProtocol so no real DB is required.
"""

import pytest
from unittest.mock import MagicMock, patch
from typing import List, Dict, Any

from dynastore.modules.tools.features import FeatureStreamConfig, stream_features
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_items_svc(captured: List[QueryRequest]) -> MagicMock:
    """Return a fake ItemsProtocol that records the request passed to stream_items."""

    async def _empty_stream():
        return
        yield  # pragma: no cover — make it an async generator

    async def _stream_items(
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        ctx=None,
        config=None,
        consumer=None,
    ):
        captured.append((request, consumer))
        response = MagicMock()
        response.items = _empty_stream()
        return response

    svc = MagicMock()
    svc.stream_items = _stream_items
    return svc


def _config(**kwargs) -> FeatureStreamConfig:
    defaults: Dict[str, Any] = {
        "catalog": "cat1",
        "collection": "col1",
        "cql_filter": None,
        "property_names": None,
        "limit": None,
        "offset": None,
        "include_geometry": True,
        "target_srid": None,
    }
    defaults.update(kwargs)
    return FeatureStreamConfig(**defaults)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_unfiltered_no_srid_emits_only_star():
    """No property_names, no target_srid → single FieldSelection(field='*')."""
    captured: List[Any] = []
    svc = _make_items_svc(captured)

    with patch(
        "dynastore.modules.tools.features.get_protocol", return_value=svc
    ):
        cfg = _config()
        async for _ in stream_features(cfg, db_resource=None):
            pass  # pragma: no cover

    assert len(captured) == 1
    req, consumer = captured[0]
    assert isinstance(req, QueryRequest)
    star_fields = [s for s in req.select if s.field == "*"]
    geom_fields = [s for s in req.select if s.field == "geom"]
    # Exactly one * entry, no duplicate geom entry
    assert len(star_fields) == 1, f"expected 1 '*' entry, got {req.select}"
    assert len(geom_fields) == 0, f"expected 0 explicit 'geom' entries, got {req.select}"
    assert len(req.select) == 1


@pytest.mark.asyncio
async def test_unfiltered_with_srid_emits_transformed_geom_plus_star():
    """No property_names, target_srid set → ST_Transform geom + star."""
    captured: List[Any] = []
    svc = _make_items_svc(captured)

    with patch(
        "dynastore.modules.tools.features.get_protocol", return_value=svc
    ):
        cfg = _config(target_srid=3857)
        async for _ in stream_features(cfg, db_resource=None):
            pass

    assert len(captured) == 1
    req, consumer = captured[0]
    star_fields = [s for s in req.select if s.field == "*"]
    geom_fields = [s for s in req.select if s.field == "geom"]
    assert len(star_fields) == 1, f"expected 1 '*' entry, got {req.select}"
    assert len(geom_fields) == 1, f"expected 1 explicit 'geom' entry, got {req.select}"
    geom_sel = geom_fields[0]
    assert geom_sel.transformation == "ST_Transform"
    assert geom_sel.transform_args.get("srid") == 3857
    assert len(req.select) == 2


@pytest.mark.asyncio
async def test_consumer_is_ogc_features():
    """stream_items is always called with consumer=ConsumerType.OGC_FEATURES."""
    captured: List[Any] = []
    svc = _make_items_svc(captured)

    with patch(
        "dynastore.modules.tools.features.get_protocol", return_value=svc
    ):
        cfg = _config()
        async for _ in stream_features(cfg, db_resource=None):
            pass

    assert len(captured) == 1
    _, consumer = captured[0]
    assert consumer == ConsumerType.OGC_FEATURES
