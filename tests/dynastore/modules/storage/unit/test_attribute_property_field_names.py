"""Attributes sidecar — storage-agnostic property field names.

``get_property_field_names`` reports the feature property fields a consumer must
select, independent of physical layout: the JSONB blob column for JSONB mode,
the individual columns for COLUMNAR mode. Identity columns and statistics are
excluded. Non-attribute sidecars fall back to the base default (no properties).
"""
from __future__ import annotations

from dynastore.modules.storage.drivers.pg_sidecars import (
    FeatureAttributeSidecar,
    FeatureAttributeSidecarConfig,
    GeometriesSidecar,
    GeometriesSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeSchemaEntry,
    AttributeStorageMode,
    PostgresType,
)


def test_jsonb_mode_returns_blob_column():
    sc = FeatureAttributeSidecar(FeatureAttributeSidecarConfig())
    assert sc.resolved_storage_mode == AttributeStorageMode.JSONB
    assert sc.get_property_field_names() == ["attributes"]


def test_jsonb_mode_honors_custom_blob_column_name():
    sc = FeatureAttributeSidecar(FeatureAttributeSidecarConfig(jsonb_column_name="props"))
    assert sc.get_property_field_names() == ["props"]


def test_columnar_mode_returns_schema_columns_in_order():
    cfg = FeatureAttributeSidecarConfig(
        attribute_schema=[
            AttributeSchemaEntry(name="pop_total", type=PostgresType.BIGINT),
            AttributeSchemaEntry(name="area_km2", type=PostgresType.NUMERIC),
        ]
    )
    sc = FeatureAttributeSidecar(cfg)
    assert sc.resolved_storage_mode == AttributeStorageMode.COLUMNAR
    assert sc.get_property_field_names() == ["pop_total", "area_km2"]


def test_explicit_jsonb_mode_with_schema_still_returns_blob():
    cfg = FeatureAttributeSidecarConfig(
        storage_mode=AttributeStorageMode.JSONB,
        attribute_schema=[AttributeSchemaEntry(name="pop_total", type=PostgresType.BIGINT)],
    )
    sc = FeatureAttributeSidecar(cfg)
    assert sc.get_property_field_names() == ["attributes"]


def test_geometry_sidecar_reports_no_properties():
    sc = GeometriesSidecar(GeometriesSidecarConfig())
    assert sc.get_property_field_names() == []
