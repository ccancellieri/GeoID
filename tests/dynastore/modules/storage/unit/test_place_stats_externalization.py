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

"""Unit tests for 3D place-statistics externalization.

Pins the contract that 3D statistics (SURFACE_AREA, Z_RANGE, CENTROID_3D,
etc.) are declared via ItemsWritePolicy.compute / ComputedField and
materialised by the PG driver, with no PlaceStatisticsConfig involved.
"""

import importlib
import pytest

from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
    StatisticStorageMode,
    _PLACE_TABLE_KINDS,
    _STATISTIC_STORAGE_KINDS,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
)
from dynastore.models.query_builder import FieldSelection, QueryRequest
from dynastore.tools.geospatial import compute_place_derived_fields


# ---------------------------------------------------------------------------
# PlaceStatisticsConfig is gone (clean break)
# ---------------------------------------------------------------------------

def test_place_statistics_config_import_raises():
    """PlaceStatisticsConfig must not exist anywhere in the package."""
    with pytest.raises((ImportError, AttributeError)):
        from dynastore.modules.storage.drivers.pg_sidecars import geometries_config  # noqa: F401
        _ = geometries_config.PlaceStatisticsConfig  # type: ignore[attr-defined]


def test_statistic_index_config_import_raises():
    """StatisticIndexConfig must not exist anywhere in the package."""
    with pytest.raises((ImportError, AttributeError)):
        from dynastore.modules.storage.drivers.pg_sidecars import geometries_config  # noqa: F401
        _ = geometries_config.StatisticIndexConfig  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# ComputedKind additions
# ---------------------------------------------------------------------------

def test_new_3d_kinds_present():
    expected = {
        ComputedKind.SURFACE_AREA,
        ComputedKind.SURFACE_TO_VOLUME_RATIO,
        ComputedKind.NET_FLOOR_AREA,
        ComputedKind.CENTROID_3D,
        ComputedKind.Z_RANGE,
        ComputedKind.VERTICAL_GRADIENT,
        ComputedKind.TEMPORAL_DURATION,
    }
    assert expected <= set(ComputedKind)


def test_3d_kinds_in_statistic_storage_kinds():
    for kind in _PLACE_TABLE_KINDS:
        assert kind in _STATISTIC_STORAGE_KINDS, f"{kind} missing from _STATISTIC_STORAGE_KINDS"


def test_place_table_kinds_is_subset_of_statistic_storage_kinds():
    assert _PLACE_TABLE_KINDS <= _STATISTIC_STORAGE_KINDS


# ---------------------------------------------------------------------------
# ComputedField validation for 3D kinds
# ---------------------------------------------------------------------------

def test_z_range_accepts_storage_mode():
    f = ComputedField(kind=ComputedKind.Z_RANGE, storage_mode=StatisticStorageMode.COLUMNAR)
    assert f.resolved_name == "z_range"


def test_centroid_3d_accepts_storage_mode():
    f = ComputedField(kind=ComputedKind.CENTROID_3D, storage_mode=StatisticStorageMode.COLUMNAR)
    assert f.resolved_name == "centroid_3d"


def test_centroid_3d_does_not_accept_centroid_type():
    with pytest.raises(Exception):
        ComputedField(
            kind=ComputedKind.CENTROID_3D,
            storage_mode=StatisticStorageMode.COLUMNAR,
            centroid_type="POINTZ",
        )


def test_temporal_duration_accepts_storage_mode():
    f = ComputedField(kind=ComputedKind.TEMPORAL_DURATION, storage_mode=StatisticStorageMode.JSONB)
    assert f.resolved_name == "temporal_duration"


# ---------------------------------------------------------------------------
# GeometriesSidecarConfig — no place_statistics field
# ---------------------------------------------------------------------------

def test_sidecar_config_has_no_place_statistics_field():
    cfg = GeometriesSidecarConfig()
    assert not hasattr(cfg, "place_statistics")


def test_sidecar_config_place_statistics_is_not_a_declared_field():
    """The model has no 'place_statistics' field — it is not part of the schema."""
    assert "place_statistics" not in GeometriesSidecarConfig.model_fields


def test_empty_3d_kinds_no_place_overlay():
    """No _PLACE_TABLE_KINDS in overlay → sidecar has no place fields."""
    cfg = GeometriesSidecarConfig(
        compute_fields_overlay=[
            ComputedField(kind=ComputedKind.AREA, storage_mode=StatisticStorageMode.COLUMNAR)
        ]
    )
    place_fields = [f for f in cfg.compute_fields_overlay if f.kind in _PLACE_TABLE_KINDS]
    assert place_fields == []


def test_with_3d_kind_in_overlay_produces_place_fields():
    cfg = GeometriesSidecarConfig(
        compute_fields_overlay=[
            ComputedField(kind=ComputedKind.Z_RANGE, storage_mode=StatisticStorageMode.COLUMNAR),
        ]
    )
    place_fields = [f for f in cfg.compute_fields_overlay if f.kind in _PLACE_TABLE_KINDS]
    assert len(place_fields) == 1
    assert place_fields[0].kind == ComputedKind.Z_RANGE


# ---------------------------------------------------------------------------
# compute_place_derived_fields — Z_RANGE on a 3D LineString
# ---------------------------------------------------------------------------

_LINESTRING_3D = {
    "type": "LineString",
    "coordinates": [[0.0, 0.0, 10.0], [1.0, 0.0, 30.0], [2.0, 0.0, 20.0]],
}


def test_z_range_linestring_3d():
    fields = [ComputedField(kind=ComputedKind.Z_RANGE, storage_mode=StatisticStorageMode.COLUMNAR)]
    result = compute_place_derived_fields(_LINESTRING_3D, fields)
    assert "z_range" in result
    assert abs(result["z_range"] - 20.0) < 1e-9  # max=30, min=10


def test_vertical_gradient_linestring_3d():
    fields = [ComputedField(kind=ComputedKind.VERTICAL_GRADIENT, storage_mode=StatisticStorageMode.COLUMNAR)]
    result = compute_place_derived_fields(_LINESTRING_3D, fields)
    assert "vertical_gradient" in result
    assert result["vertical_gradient"] >= 0.0


def test_empty_fields_returns_empty_dict():
    result = compute_place_derived_fields(_LINESTRING_3D, [])
    assert result == {}


def test_empty_place_dict_returns_empty_dict():
    fields = [ComputedField(kind=ComputedKind.Z_RANGE, storage_mode=StatisticStorageMode.COLUMNAR)]
    result = compute_place_derived_fields({}, fields)
    assert result == {}


# ---------------------------------------------------------------------------
# compute_place_derived_fields — Prism (volume, surface_area, net_floor_area)
# ---------------------------------------------------------------------------

_PRISM = {
    "type": "Prism",
    "base": {
        "type": "Polygon",
        "coordinates": [[[0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [1.0, 0.0], [0.0, 0.0]]],
    },
    "lower": 0.0,
    "upper": 5.0,
}


def test_surface_area_prism():
    fields = [ComputedField(kind=ComputedKind.SURFACE_AREA, storage_mode=StatisticStorageMode.COLUMNAR)]
    result = compute_place_derived_fields(_PRISM, fields)
    assert "surface_area" in result
    # Unit square prism h=5: 2*1 + 4*5 = 22
    assert abs(result["surface_area"] - 22.0) < 1e-6


def test_net_floor_area_prism():
    fields = [ComputedField(kind=ComputedKind.NET_FLOOR_AREA, storage_mode=StatisticStorageMode.COLUMNAR)]
    result = compute_place_derived_fields(_PRISM, fields)
    assert "net_floor_area" in result
    assert abs(result["net_floor_area"] - 1.0) < 1e-9


def test_z_range_prism():
    fields = [ComputedField(kind=ComputedKind.Z_RANGE, storage_mode=StatisticStorageMode.COLUMNAR)]
    result = compute_place_derived_fields(_PRISM, fields)
    assert "z_range" in result
    assert abs(result["z_range"] - 5.0) < 1e-9


def test_centroid_3d_prism():
    # COLUMNAR centroid_3d targets a GEOMETRY(POINTZ) column, so the value is
    # WKB hex (not a coordinate array — that would fail the geometry column).
    from shapely import wkb as _wkb

    fields = [ComputedField(kind=ComputedKind.CENTROID_3D, storage_mode=StatisticStorageMode.COLUMNAR)]
    result = compute_place_derived_fields(_PRISM, fields)
    assert "centroid_3d" in result
    pt = _wkb.loads(result["centroid_3d"], hex=True)
    assert pt.has_z
    assert abs(pt.x - 0.5) < 1e-9
    assert abs(pt.y - 0.5) < 1e-9
    assert abs(pt.z - 2.5) < 1e-9  # midpoint of lower=0 upper=5


def test_surface_to_volume_ratio_prism():
    fields = [
        ComputedField(kind=ComputedKind.SURFACE_AREA, storage_mode=StatisticStorageMode.COLUMNAR),
        ComputedField(kind=ComputedKind.SURFACE_TO_VOLUME_RATIO, storage_mode=StatisticStorageMode.COLUMNAR),
    ]
    result = compute_place_derived_fields(_PRISM, fields)
    assert "surface_to_volume_ratio" in result
    # volume = 1*5 = 5, surface_area = 22, ratio = 22/5 = 4.4
    assert abs(result["surface_to_volume_ratio"] - 4.4) < 1e-6


# ---------------------------------------------------------------------------
# compute_place_derived_fields — TEMPORAL_DURATION
# ---------------------------------------------------------------------------

def test_temporal_duration_dict_interval():
    validity = {"lower": "2020-01-01T00:00:00+00:00", "upper": "2021-01-01T00:00:00+00:00"}
    fields = [ComputedField(kind=ComputedKind.TEMPORAL_DURATION, storage_mode=StatisticStorageMode.COLUMNAR)]
    result = compute_place_derived_fields({}, fields, validity=validity)
    # 2020 is a leap year: 366 days
    assert "temporal_duration" in result
    expected_seconds = 366 * 86400.0
    assert abs(result["temporal_duration"] - expected_seconds) < 1.0


def test_temporal_duration_no_validity():
    fields = [ComputedField(kind=ComputedKind.TEMPORAL_DURATION, storage_mode=StatisticStorageMode.COLUMNAR)]
    result = compute_place_derived_fields({}, fields, validity=None)
    assert "temporal_duration" not in result


# ---------------------------------------------------------------------------
# DDL shape — _place table emitted iff _PLACE_TABLE_KINDS present
# ---------------------------------------------------------------------------

def _make_sidecar_ddl(overlay) -> str:
    """Build a minimal DDL string by instantiating GeometriesSidecar."""
    from dynastore.modules.storage.drivers.pg_sidecars.geometries import GeometriesSidecar
    cfg = GeometriesSidecarConfig(compute_fields_overlay=overlay)
    sidecar = GeometriesSidecar(config=cfg)
    return sidecar.get_ddl("test_table")


def test_no_place_table_without_3d_kinds():
    ddl = _make_sidecar_ddl([
        ComputedField(kind=ComputedKind.AREA, storage_mode=StatisticStorageMode.COLUMNAR)
    ])
    assert "_place" not in ddl


def test_place_table_emitted_with_z_range():
    ddl = _make_sidecar_ddl([
        ComputedField(kind=ComputedKind.Z_RANGE, storage_mode=StatisticStorageMode.COLUMNAR, indexed=True)
    ])
    assert "_place" in ddl
    assert "place_z_range" in ddl


def test_place_table_emitted_with_centroid_3d_columnar():
    ddl = _make_sidecar_ddl([
        ComputedField(kind=ComputedKind.CENTROID_3D, storage_mode=StatisticStorageMode.COLUMNAR)
    ])
    assert "_place" in ddl
    assert "GEOMETRY(POINTZ, 4326)" in ddl


def test_place_table_jsonb_layout():
    ddl = _make_sidecar_ddl([
        ComputedField(kind=ComputedKind.SURFACE_AREA, storage_mode=StatisticStorageMode.JSONB)
    ])
    assert "_place" in ddl
    assert "place_stats JSONB" in ddl
    assert "DOUBLE PRECISION" not in ddl.split("_place")[1]  # no columnar col after _place table


def test_place_table_has_fk_and_pk():
    ddl = _make_sidecar_ddl([
        ComputedField(kind=ComputedKind.Z_RANGE, storage_mode=StatisticStorageMode.COLUMNAR)
    ])
    assert "PRIMARY KEY" in ddl
    assert "FOREIGN KEY" in ddl
    assert "ON DELETE CASCADE" in ddl


# ---------------------------------------------------------------------------
# DDL shape — main-table COLUMNAR/JSONB loops exclude _PLACE_TABLE_KINDS
# (#3222 deferred DDL wart: a place stat used to also get a bare, dead
# column on {table}_geometries in addition to its real place_{name}
# column on {table}_place).
# ---------------------------------------------------------------------------

def test_place_kind_columnar_does_not_mint_bare_column_on_main_table():
    ddl = _make_sidecar_ddl([
        ComputedField(kind=ComputedKind.Z_RANGE, storage_mode=StatisticStorageMode.COLUMNAR)
    ])
    main_table_ddl = ddl.split("_place")[0]
    assert "z_range DOUBLE PRECISION" not in main_table_ddl
    assert "place_z_range DOUBLE PRECISION" in ddl


def test_place_kind_jsonb_does_not_mint_geom_stats_on_main_table():
    ddl = _make_sidecar_ddl([
        ComputedField(kind=ComputedKind.SURFACE_AREA, storage_mode=StatisticStorageMode.JSONB)
    ])
    main_table_ddl = ddl.split("_place")[0]
    assert "geom_stats JSONB" not in main_table_ddl
    assert "place_stats JSONB" in ddl


def test_place_kind_indexed_does_not_mint_bare_index_on_main_table():
    ddl = _make_sidecar_ddl([
        ComputedField(
            kind=ComputedKind.Z_RANGE,
            storage_mode=StatisticStorageMode.COLUMNAR,
            indexed=True,
        )
    ])
    main_table_ddl = ddl.split("_place")[0]
    assert "idx_test_table_geometries_z_range" not in main_table_ddl
    assert "idx_test_table_place_z_range" in ddl


def test_mixed_2d_and_place_kinds_only_place_excluded_from_main_table():
    """A non-place COLUMNAR field alongside a place field: the non-place
    field still gets its bare main-table column; only the place field is
    excluded."""
    ddl = _make_sidecar_ddl([
        ComputedField(kind=ComputedKind.AREA, storage_mode=StatisticStorageMode.COLUMNAR),
        ComputedField(kind=ComputedKind.Z_RANGE, storage_mode=StatisticStorageMode.COLUMNAR),
    ])
    main_table_ddl = ddl.split("_place")[0]
    assert "area DOUBLE PRECISION" in main_table_ddl
    assert "z_range DOUBLE PRECISION" not in main_table_ddl
    assert "place_z_range DOUBLE PRECISION" in ddl


# ---------------------------------------------------------------------------
# Projection shape — get_select_fields / get_queryable_fields exclude
# _PLACE_TABLE_KINDS (#3309). Since the main geometries table no longer
# mints place columns, projecting or registering a place stat off the
# geometries alias is an UndefinedColumnError on every new collection's
# first read.
# ---------------------------------------------------------------------------

def _make_sidecar(overlay):
    from dynastore.modules.storage.drivers.pg_sidecars.geometries import GeometriesSidecar
    cfg = GeometriesSidecarConfig(compute_fields_overlay=overlay)
    return GeometriesSidecar(config=cfg)


_ALL_PLACE_COLUMNAR = [
    ComputedField(kind=k, storage_mode=StatisticStorageMode.COLUMNAR)
    for k in sorted(_PLACE_TABLE_KINDS, key=lambda k: k.value)
]


def test_full_mode_select_fields_exclude_place_kinds():
    sidecar = _make_sidecar(
        [ComputedField(kind=ComputedKind.AREA, storage_mode=StatisticStorageMode.COLUMNAR)]
        + _ALL_PLACE_COLUMNAR
    )
    joined = " ".join(sidecar.get_select_fields(include_all=True))
    assert 'as "area"' in joined
    for kind in _PLACE_TABLE_KINDS:
        assert kind.value not in joined, f"place stat {kind.value} projected off sc_geometries"


def test_selective_mode_select_fields_exclude_requested_place_kind():
    """Explicitly requesting a place stat must not project it off the
    geometries alias — the column only exists on {table}_place."""
    sidecar = _make_sidecar(_ALL_PLACE_COLUMNAR)
    request = QueryRequest(select=[FieldSelection(field="surface_area")])
    fields = sidecar.get_select_fields(request=request)
    assert all("surface_area" not in f for f in fields)


def test_place_only_jsonb_does_not_project_geom_stats():
    """A place-only JSONB overlay mints no geom_stats column (be8b0bfe),
    so the SELECT must not reference it either."""
    sidecar = _make_sidecar([
        ComputedField(kind=ComputedKind.SURFACE_AREA, storage_mode=StatisticStorageMode.JSONB)
    ])
    joined = " ".join(sidecar.get_select_fields(include_all=True))
    assert "geom_stats" not in joined


def test_non_place_jsonb_still_projects_geom_stats():
    sidecar = _make_sidecar([
        ComputedField(kind=ComputedKind.AREA, storage_mode=StatisticStorageMode.JSONB),
        ComputedField(kind=ComputedKind.SURFACE_AREA, storage_mode=StatisticStorageMode.JSONB),
    ])
    joined = " ".join(sidecar.get_select_fields(include_all=True))
    assert "geom_stats" in joined


def test_queryable_fields_exclude_place_kinds():
    sidecar = _make_sidecar(
        [ComputedField(kind=ComputedKind.AREA, storage_mode=StatisticStorageMode.COLUMNAR)]
        + _ALL_PLACE_COLUMNAR
    )
    queryables = sidecar.get_queryable_fields()
    assert "area" in queryables
    for kind in _PLACE_TABLE_KINDS:
        assert kind.value not in queryables, (
            f"place stat {kind.value} registered filterable/sortable off sc_geometries"
        )
