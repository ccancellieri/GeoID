"""``ItemQueryMixin._build_base_query_request`` — storage-agnostic projection.

The base query that feeds tiles/features selects property fields via the
sidecars (JSONB blob vs COLUMNAR columns) and the geometry via the geometry
sidecar's column name, instead of hardcoding ``"attributes"`` / ``"geom"``.
"""
from __future__ import annotations

from types import SimpleNamespace

from dynastore.modules.catalog.item_query import ItemQueryMixin
from dynastore.modules.storage.drivers.pg_sidecars import (
    FeatureAttributeSidecarConfig,
    GeometriesSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeSchemaEntry,
    PostgresType,
)


def _mixin() -> ItemQueryMixin:
    return ItemQueryMixin.__new__(ItemQueryMixin)


def _fields(req) -> list:
    return [s.field for s in req.select]


def test_jsonb_collection_selects_blob_and_skips_geom_for_mvt():
    cfg = SimpleNamespace(
        sidecars=[GeometriesSidecarConfig(), FeatureAttributeSidecarConfig()]
    )
    req = _mixin()._build_base_query_request({"geom_format": "MVT"}, cfg)
    fields = _fields(req)
    assert "geoid" in fields
    assert "attributes" in fields  # JSONB blob — behavior-preserving
    assert "geom" not in fields    # MVT geometry handled by the transform


def test_jsonb_collection_selects_geom_for_non_mvt():
    cfg = SimpleNamespace(
        sidecars=[GeometriesSidecarConfig(), FeatureAttributeSidecarConfig()]
    )
    req = _mixin()._build_base_query_request({"geom_format": "WKB"}, cfg)
    fields = _fields(req)
    assert "geom" in fields
    assert "attributes" in fields


def test_columnar_collection_selects_individual_columns():
    cfg = SimpleNamespace(
        sidecars=[
            GeometriesSidecarConfig(),
            FeatureAttributeSidecarConfig(
                attribute_schema=[
                    AttributeSchemaEntry(name="pop_total", type=PostgresType.BIGINT),
                    AttributeSchemaEntry(name="area_km2", type=PostgresType.NUMERIC),
                ]
            ),
        ]
    )
    req = _mixin()._build_base_query_request({"geom_format": "MVT"}, cfg)
    fields = _fields(req)
    assert "pop_total" in fields
    assert "area_km2" in fields
    assert "attributes" not in fields  # no JSONB blob in COLUMNAR mode


def test_renamed_geometry_column_selected_for_non_mvt():
    cfg = SimpleNamespace(
        sidecars=[
            GeometriesSidecarConfig(geom_column="the_geom"),
            FeatureAttributeSidecarConfig(),
        ]
    )
    req = _mixin()._build_base_query_request({"geom_format": "WKB"}, cfg)
    assert "the_geom" in _fields(req)
