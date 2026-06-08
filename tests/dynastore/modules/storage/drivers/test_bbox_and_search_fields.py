"""DB-free unit tests for the three read-path fixes.

#1928 — GeometriesSidecar.map_row_to_feature now converts stored bbox
         columns (scalar or bbox_geom) into feature.bbox [minx,miny,maxx,maxy].
#1930 — search.py _HYDRATION_INTERNAL_FIELDS strips projection-only columns
         from feature.properties before serialization.
#1929 — (query_optimizer covered by behaviour — verified via STAC id fix note
         in commit message; not directly testable DB-free).

Recipe:
    PYTHONPATH=/Users/ccancellieri/work/code/geoid/packages/core/src \
    /Users/ccancellieri/work/code/geoid/.venv/bin/python -m pytest \
    tests/dynastore/modules/storage/drivers/test_bbox_and_search_fields.py \
    --noconftest -p no:cacheprovider -q
"""
from __future__ import annotations

from typing import Any, Dict

import pytest


# ---------------------------------------------------------------------------
# Helpers — build the minimal objects without DB dependencies
# ---------------------------------------------------------------------------


def _make_sidecar():
    """Return a GeometriesSidecar with the default config (write_bbox=True)."""
    from dynastore.modules.storage.drivers.pg_sidecars.geometries import (
        GeometriesSidecar,
    )
    from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
        GeometriesSidecarConfig,
    )

    return GeometriesSidecar(GeometriesSidecarConfig())


def _make_feature():
    from dynastore.models.shared_models import Feature

    return Feature(type="Feature", id="test-feature-1", geometry=None, properties={})


def _make_context():
    from dynastore.modules.storage.drivers.pg_sidecars.base import (
        FeaturePipelineContext,
    )

    return FeaturePipelineContext()


# ---------------------------------------------------------------------------
# #1928 — bbox from scalar envelope columns (Priority A)
# ---------------------------------------------------------------------------


def test_bbox_from_scalar_columns() -> None:
    sidecar = _make_sidecar()
    feature = _make_feature()
    context = _make_context()
    row: Dict[str, Any] = {
        "bbox_xmin": -10.0,
        "bbox_ymin": -20.0,
        "bbox_xmax": 30.0,
        "bbox_ymax": 40.0,
    }
    sidecar.map_row_to_feature(row, feature, context)
    assert list(feature.bbox or []) == [-10.0, -20.0, 30.0, 40.0], (
        "bbox must be assembled from scalar envelope columns"
    )


def test_bbox_scalar_columns_all_present_all_numeric() -> None:
    """Integer values must also produce valid float bbox."""
    sidecar = _make_sidecar()
    feature = _make_feature()
    context = _make_context()
    row: Dict[str, Any] = {
        "bbox_xmin": -180,
        "bbox_ymin": -90,
        "bbox_xmax": 180,
        "bbox_ymax": 90,
    }
    sidecar.map_row_to_feature(row, feature, context)
    assert list(feature.bbox or []) == [-180.0, -90.0, 180.0, 90.0]


def test_bbox_none_when_any_scalar_column_missing() -> None:
    """Partial scalar columns — fall through to lower-priority paths."""
    sidecar = _make_sidecar()
    feature = _make_feature()
    context = _make_context()
    row: Dict[str, Any] = {
        "bbox_xmin": -10.0,
        "bbox_ymin": -20.0,
        # bbox_xmax and bbox_ymax absent
    }
    sidecar.map_row_to_feature(row, feature, context)
    # No geometry either, so bbox stays None
    assert feature.bbox is None


def test_bbox_none_when_scalar_column_null() -> None:
    """Null scalar column — must not produce a partial bbox."""
    sidecar = _make_sidecar()
    feature = _make_feature()
    context = _make_context()
    row: Dict[str, Any] = {
        "bbox_xmin": None,
        "bbox_ymin": -20.0,
        "bbox_xmax": 30.0,
        "bbox_ymax": 40.0,
    }
    sidecar.map_row_to_feature(row, feature, context)
    assert feature.bbox is None


# ---------------------------------------------------------------------------
# #1928 — bbox falls back to geometry when no scalar columns (Priority C)
# ---------------------------------------------------------------------------


def test_bbox_derived_from_geometry_when_no_scalar_columns() -> None:
    """When only the geometry is set on the Feature, bbox is derived from it."""
    pytest.importorskip("shapely.geometry")

    sidecar = _make_sidecar()
    context = _make_context()

    from dynastore.models.shared_models import Feature, GeoJSONGeometry

    feature = Feature(
        type="Feature",
        id="test-geom-1",
        geometry=GeoJSONGeometry(type="Point", coordinates=[10.0, 20.0]),
        properties={},
    )
    row: Dict[str, Any] = {}  # no scalar or bbox_geom columns
    sidecar.map_row_to_feature(row, feature, context)
    # Point.bounds = (minx, miny, maxx, maxy) = (10, 20, 10, 20)
    assert list(feature.bbox or []) == [10.0, 20.0, 10.0, 20.0]


def test_bbox_polygon_from_geometry() -> None:
    """Polygon geometry produces the tight bounding box."""
    pytest.importorskip("shapely.geometry")

    from dynastore.models.shared_models import Feature, GeoJSONGeometry

    sidecar = _make_sidecar()
    context = _make_context()
    feature = Feature(
        type="Feature",
        id="test-poly-1",
        geometry=GeoJSONGeometry(
            type="Polygon",
            coordinates=[[[0, 0], [0, 5], [10, 5], [10, 0], [0, 0]]],
        ),
        properties={},
    )
    row: Dict[str, Any] = {}
    sidecar.map_row_to_feature(row, feature, context)
    assert list(feature.bbox or []) == [0.0, 0.0, 10.0, 5.0]


# ---------------------------------------------------------------------------
# #1930 — _HYDRATION_INTERNAL_FIELDS constant
# ---------------------------------------------------------------------------


def test_hydration_internal_fields_contains_expected_keys() -> None:
    """All leaking hydration columns must be declared for stripping."""
    from dynastore.extensions.stac.search import _HYDRATION_INTERNAL_FIELDS

    expected = {
        "valid_from",
        "valid_to",
        "bbox_xmin",
        "bbox_ymin",
        "bbox_xmax",
        "bbox_ymax",
        "stac_title",
        "stac_description",
        "stac_keywords",
        "stac_extra_fields",
        "external_assets",
        "external_extensions",
        "geometry_hash",
        "_total_count",
    }
    missing = expected - _HYDRATION_INTERNAL_FIELDS
    assert not missing, (
        f"_HYDRATION_INTERNAL_FIELDS is missing expected keys: {missing}"
    )


def test_hydration_fields_stripped_from_properties() -> None:
    """simulate the stripping loop that search_items applies."""
    from dynastore.extensions.stac.search import _HYDRATION_INTERNAL_FIELDS

    properties: Dict[str, Any] = {
        "valid_from": "2026-01-01",
        "bbox_xmin": -10.0,
        "bbox_xmax": 10.0,
        "bbox_ymin": -5.0,
        "bbox_ymax": 5.0,
        "stac_title": None,
        "stac_description": None,
        "stac_keywords": None,
        "stac_extra_fields": None,
        "external_assets": None,
        "external_extensions": None,
        "geometry_hash": "abc123",
        "_total_count": 42,
        # authored STAC properties — must survive
        "datetime": "2026-06-01T00:00:00Z",
        "eo:cloud_cover": 10.5,
    }
    for k in _HYDRATION_INTERNAL_FIELDS:
        properties.pop(k, None)

    assert "datetime" in properties
    assert "eo:cloud_cover" in properties
    for leaked in (
        "valid_from", "bbox_xmin", "stac_title", "geometry_hash", "_total_count"
    ):
        assert leaked not in properties, f"Internal field {leaked!r} still present"


# ---------------------------------------------------------------------------
# #1930 — unproject_item_from_es passes bbox through correctly
# ---------------------------------------------------------------------------


def test_unproject_item_from_es_preserves_bbox() -> None:
    """bbox stored as a reserved top-level member must survive unprojection."""
    from dynastore.modules.elasticsearch.items_projection import unproject_item_from_es

    source: Dict[str, Any] = {
        "id": "item-1",
        "type": "Feature",
        "collection": "col",
        "geometry": {"type": "Point", "coordinates": [10.0, 20.0]},
        "bbox": [-10.0, -20.0, 30.0, 40.0],
        "properties": {
            "datetime": "2026-06-01T00:00:00Z",
        },
        # internal fields that must be dropped from top-level
        "catalog_id": "cat1",
        "collection_id": "col1",
        "_external_id": "ext-id-1",
        "validity": {"gte": "2026-01-01"},
    }
    result = unproject_item_from_es(source)
    assert result["bbox"] == [-10.0, -20.0, 30.0, 40.0], (
        "bbox must survive unproject_item_from_es as a reserved top-level member"
    )
    # Internal fields must be stripped
    assert "catalog_id" not in result
    assert "collection_id" not in result
    assert "_external_id" not in result
    # User properties preserved
    assert result["properties"]["datetime"] == "2026-06-01T00:00:00Z"


def test_unproject_item_from_es_bbox_none_when_absent() -> None:
    """When bbox is not in source, result must not have bbox either."""
    from dynastore.modules.elasticsearch.items_projection import unproject_item_from_es

    source: Dict[str, Any] = {
        "id": "item-2",
        "type": "Feature",
        "geometry": None,
        "properties": {},
    }
    result = unproject_item_from_es(source)
    # bbox absent in source → absent in output (not forced to [])
    assert result.get("bbox") is None or "bbox" not in result
