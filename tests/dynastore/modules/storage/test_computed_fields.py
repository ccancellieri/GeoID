#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""Phase 1 of #957/#950 items-policy consolidation — pure-additive models.

Pins the validator behaviour and the resolved-name convention of the new
:class:`ComputedField` / :class:`IdentityRule` / :class:`FeatureType`
models. Nothing here exercises drivers (phase 2) — these tests only
guarantee the models load, validate, and serialise as designed.
"""

import pytest
from pydantic import ValidationError
from shapely.geometry import Polygon, Point

from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
    FeatureType,
    IdentityRule,
    PATH_EXTRACTED_KINDS,
    SPATIAL_CELL_KINDS,
    StatisticStorageMode,
)
from dynastore.modules.storage.driver_config import WriteConflictPolicy
from dynastore.tools.geospatial import compute_derived_fields
from dynastore.tools.geospatial_exceptions import UnsupportedComputedKind


class TestComputedFieldValidators:
    def test_spatial_kinds_require_resolution(self) -> None:
        for kind in SPATIAL_CELL_KINDS:
            with pytest.raises(ValidationError):
                ComputedField(kind=kind)

    def test_non_spatial_kinds_reject_resolution(self) -> None:
        with pytest.raises(ValidationError):
            ComputedField(kind=ComputedKind.AREA, resolution=9)
        with pytest.raises(ValidationError):
            ComputedField(kind=ComputedKind.EXTERNAL_ID, resolution=1)

    @pytest.mark.parametrize(
        "kind,bad",
        [
            (ComputedKind.GEOHASH, 0),
            (ComputedKind.GEOHASH, 13),
            (ComputedKind.H3, -1),
            (ComputedKind.H3, 16),
            (ComputedKind.S2, -1),
            (ComputedKind.S2, 31),
        ],
    )
    def test_resolution_range(self, kind: ComputedKind, bad: int) -> None:
        with pytest.raises(ValidationError):
            ComputedField(kind=kind, resolution=bad)

    def test_resolved_name_defaults(self) -> None:
        assert ComputedField(kind=ComputedKind.AREA).resolved_name == "area"
        assert (
            ComputedField(kind=ComputedKind.H3, resolution=7).resolved_name
            == "h3_7"
        )
        assert (
            ComputedField(kind=ComputedKind.S2, resolution=10).resolved_name
            == "s2_10"
        )
        assert (
            ComputedField(kind=ComputedKind.GEOHASH, resolution=8).resolved_name
            == "geohash_8"
        )

    def test_resolved_name_override(self) -> None:
        assert (
            ComputedField(
                kind=ComputedKind.GEOHASH, resolution=8, name="gh8"
            ).resolved_name
            == "gh8"
        )

    def test_frozen(self) -> None:
        f = ComputedField(kind=ComputedKind.AREA)
        with pytest.raises(ValidationError):
            f.name = "new"  # type: ignore[misc]


class TestIdentityRule:
    def test_match_on_required(self) -> None:
        with pytest.raises(ValidationError):
            IdentityRule(match_on=[])

    def test_on_match_optional(self) -> None:
        r = IdentityRule(
            match_on=[ComputedField(kind=ComputedKind.EXTERNAL_ID)]
        )
        assert r.on_match is None

    def test_on_match_override(self) -> None:
        r = IdentityRule(
            match_on=[ComputedField(kind=ComputedKind.GEOMETRY_HASH)],
            on_match=WriteConflictPolicy.REFUSE_FAIL,
        )
        assert r.on_match == WriteConflictPolicy.REFUSE_FAIL

    def test_and_composition(self) -> None:
        r = IdentityRule(
            match_on=[
                ComputedField(kind=ComputedKind.EXTERNAL_ID),
                ComputedField(kind=ComputedKind.GEOMETRY_HASH),
            ]
        )
        assert len(r.match_on) == 2


class TestFeatureType:
    def test_defaults(self) -> None:
        ft = FeatureType()
        assert ft.expose == []
        assert ft.failure_mode == "best_effort"
        assert ft.external_id_as_feature_id is True

    def test_strict_failure_mode(self) -> None:
        ft = FeatureType(failure_mode="strict")
        assert ft.failure_mode == "strict"

    def test_unknown_failure_mode_rejected(self) -> None:
        with pytest.raises(ValidationError):
            FeatureType(failure_mode="loud")  # type: ignore[arg-type]


class TestComputeDerivedFields:
    @pytest.fixture
    def unit_square(self) -> Polygon:
        return Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])

    def test_external_id_default_path(self, unit_square: Polygon) -> None:
        out = compute_derived_fields(
            unit_square,
            {"external_id": "abc-123"},
            [ComputedField(kind=ComputedKind.EXTERNAL_ID)],
        )
        assert out == {"external_id": "abc-123"}

    def test_external_id_custom_path(self, unit_square: Polygon) -> None:
        out = compute_derived_fields(
            unit_square,
            {"adm2_pcode": "ITA001"},
            [
                ComputedField(
                    kind=ComputedKind.EXTERNAL_ID,
                    name="properties.adm2_pcode",
                )
            ],
        )
        # Key is always "external_id" regardless of source path.
        assert out == {"external_id": "ITA001"}

    def test_external_id_missing_returns_none(
        self, unit_square: Polygon
    ) -> None:
        out = compute_derived_fields(
            unit_square,
            {"other": "x"},
            [
                ComputedField(
                    kind=ComputedKind.EXTERNAL_ID, name="properties.code"
                )
            ],
        )
        assert out == {"external_id": None}

    def test_geometry_metrics(self, unit_square: Polygon) -> None:
        out = compute_derived_fields(
            unit_square,
            {},
            [
                ComputedField(kind=ComputedKind.AREA),
                ComputedField(kind=ComputedKind.PERIMETER),
                ComputedField(kind=ComputedKind.BBOX),
                ComputedField(kind=ComputedKind.CENTROID),
                ComputedField(kind=ComputedKind.VERTEX_COUNT),
                ComputedField(kind=ComputedKind.HOLE_COUNT),
            ],
        )
        assert out["area"] == 1.0
        assert out["perimeter"] == 4.0
        assert out["bbox"] == [0.0, 0.0, 1.0, 1.0]
        assert out["centroid"] == [0.5, 0.5]
        assert out["vertex_count"] == 5
        assert out["hole_count"] == 0

    def test_perimeter_is_zero_for_point(self) -> None:
        out = compute_derived_fields(
            Point(1, 1),
            {},
            [ComputedField(kind=ComputedKind.PERIMETER)],
        )
        assert out["perimeter"] == 0.0

    def test_morphological_indices_on_unit_square(
        self, unit_square: Polygon
    ) -> None:
        import math
        out = compute_derived_fields(
            unit_square,
            {},
            [
                ComputedField(kind=ComputedKind.CIRCULARITY),
                ComputedField(kind=ComputedKind.CONVEXITY),
                ComputedField(kind=ComputedKind.ASPECT_RATIO),
                ComputedField(kind=ComputedKind.VOLUME),
            ],
        )
        # Unit square: perimeter=4, area=1 → circularity = pi/4
        assert out["circularity"] == pytest.approx(math.pi / 4)
        # Square == its convex hull → convexity = 1
        assert out["convexity"] == pytest.approx(1.0)
        # 1×1 bbox → aspect_ratio = 1
        assert out["aspect_ratio"] == pytest.approx(1.0)
        # 2D polygon has no volume
        assert out["volume"] == 0.0

    def test_convexity_lt_one_for_concave_polygon(self) -> None:
        # L-shape — concave; convex hull is the bounding 2x2 square minus
        # nothing, area = 3 (covered) vs hull area = 4 → convexity = 0.75.
        l_shape = Polygon(
            [(0, 0), (2, 0), (2, 1), (1, 1), (1, 2), (0, 2)]
        )
        out = compute_derived_fields(
            l_shape, {}, [ComputedField(kind=ComputedKind.CONVEXITY)]
        )
        assert 0 < out["convexity"] < 1

    def test_hashes_are_deterministic(self, unit_square: Polygon) -> None:
        a = compute_derived_fields(
            unit_square,
            {"k": "v", "n": 1},
            [
                ComputedField(kind=ComputedKind.GEOMETRY_HASH),
                ComputedField(kind=ComputedKind.ATTRIBUTES_HASH),
            ],
        )
        # Key order in properties must not change the attributes hash.
        b = compute_derived_fields(
            unit_square,
            {"n": 1, "k": "v"},
            [
                ComputedField(kind=ComputedKind.GEOMETRY_HASH),
                ComputedField(kind=ComputedKind.ATTRIBUTES_HASH),
            ],
        )
        assert a == b
        assert len(a["geometry_hash"]) == 64  # sha256 hex
        assert len(a["attributes_hash"]) == 64

    def test_attributes_hash_changes_with_value(
        self, unit_square: Polygon
    ) -> None:
        f = [ComputedField(kind=ComputedKind.ATTRIBUTES_HASH)]
        a = compute_derived_fields(unit_square, {"k": "v"}, f)
        b = compute_derived_fields(unit_square, {"k": "w"}, f)
        assert a["attributes_hash"] != b["attributes_hash"]

    def test_spatial_cells(self, unit_square: Polygon) -> None:
        out = compute_derived_fields(
            unit_square,
            {},
            [
                ComputedField(kind=ComputedKind.GEOHASH, resolution=8),
                ComputedField(kind=ComputedKind.H3, resolution=7),
                ComputedField(kind=ComputedKind.S2, resolution=10),
            ],
        )
        # Geohash for (0.5, 0.5): deterministic, length matches precision.
        assert len(out["geohash_8"]) == 8
        # H3/S2 just need to be non-empty strings; library-specific format.
        assert isinstance(out["h3_7"], str) and out["h3_7"]
        assert isinstance(out["s2_10"], str) and out["s2_10"]

    def test_custom_name_overrides_key(self, unit_square: Polygon) -> None:
        out = compute_derived_fields(
            unit_square,
            {},
            [
                ComputedField(kind=ComputedKind.AREA, name="acreage"),
                ComputedField(
                    kind=ComputedKind.GEOHASH, resolution=8, name="cell8"
                ),
            ],
        )
        assert "acreage" in out and "area" not in out
        assert "cell8" in out and "geohash_8" not in out


def test_path_extracted_kinds_membership() -> None:
    assert ComputedKind.EXTERNAL_ID in PATH_EXTRACTED_KINDS
    assert ComputedKind.GEOMETRY_HASH not in PATH_EXTRACTED_KINDS


def test_spatial_cell_kinds_membership() -> None:
    assert SPATIAL_CELL_KINDS == frozenset(
        {ComputedKind.GEOHASH, ComputedKind.H3, ComputedKind.S2}
    )


# ---------------------------------------------------------------------------
# Storage-shape fields on ComputedField (issue #978 cutover).
# ---------------------------------------------------------------------------


class TestStorageShapeValidators:
    def test_columnar_indexed_centroid_pointz_roundtrip(self) -> None:
        f = ComputedField(
            kind=ComputedKind.CENTROID,
            storage_mode=StatisticStorageMode.COLUMNAR,
            indexed=True,
            centroid_type="POINTZ",
        )
        assert f.storage_mode == StatisticStorageMode.COLUMNAR
        assert f.indexed is True
        assert f.centroid_type == "POINTZ"
        assert f.resolved_name == "centroid"

    def test_jsonb_with_indexed_true_is_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ComputedField(
                kind=ComputedKind.AREA,
                storage_mode=StatisticStorageMode.JSONB,
                indexed=True,
            )

    def test_indexed_without_storage_mode_is_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ComputedField(kind=ComputedKind.AREA, indexed=True)

    def test_centroid_type_on_non_centroid_is_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ComputedField(
                kind=ComputedKind.AREA,
                storage_mode=StatisticStorageMode.COLUMNAR,
                centroid_type="POINT",
            )

    def test_storage_mode_on_identity_kind_is_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ComputedField(
                kind=ComputedKind.GEOMETRY_HASH,
                storage_mode=StatisticStorageMode.COLUMNAR,
            )
        with pytest.raises(ValidationError):
            ComputedField(
                kind=ComputedKind.GEOHASH,
                resolution=8,
                storage_mode=StatisticStorageMode.JSONB,
            )

    def test_storage_mode_none_default(self) -> None:
        f = ComputedField(kind=ComputedKind.AREA)
        assert f.storage_mode is None
        assert f.indexed is False
        assert f.centroid_type is None


class TestCentroidStorageMaterialisation:
    def test_centroid_pointz_emits_wkb_hex(self) -> None:
        from shapely.geometry import Polygon as _Poly

        sq = _Poly([(0, 0), (0, 1), (1, 1), (1, 0)])
        out = compute_derived_fields(
            sq,
            {},
            [
                ComputedField(
                    kind=ComputedKind.CENTROID,
                    storage_mode=StatisticStorageMode.COLUMNAR,
                    centroid_type="POINTZ",
                )
            ],
        )
        assert isinstance(out["centroid"], str) and len(out["centroid"]) > 0

    def test_centroid_without_type_returns_xy_array(self) -> None:
        from shapely.geometry import Polygon as _Poly

        sq = _Poly([(0, 0), (0, 1), (1, 1), (1, 0)])
        out = compute_derived_fields(
            sq,
            {},
            [ComputedField(kind=ComputedKind.CENTROID)],
        )
        assert out["centroid"] == [0.5, 0.5]


class TestGeometriesSidecarOverlayDDL:
    def test_columnar_area_emits_double_precision_column(self) -> None:
        from dynastore.modules.storage.drivers.pg_sidecars.geometries import (
            GeometriesSidecar,
        )
        from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
            GeometriesSidecarConfig,
        )

        cfg = GeometriesSidecarConfig(
            compute_fields_overlay=[
                ComputedField(
                    kind=ComputedKind.AREA,
                    storage_mode=StatisticStorageMode.COLUMNAR,
                )
            ]
        )
        sc = GeometriesSidecar(cfg)
        ddl = sc.get_ddl(
            physical_table="t_test",
            partition_keys=[],
            partition_key_types={},
            has_validity=False,
        )
        assert "area DOUBLE PRECISION" in ddl
        # No JSONB column when no field uses JSONB storage.
        assert "geom_stats JSONB" not in ddl

    def test_columnar_indexed_emits_btree_index(self) -> None:
        from dynastore.modules.storage.drivers.pg_sidecars.geometries import (
            GeometriesSidecar,
        )
        from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
            GeometriesSidecarConfig,
        )

        cfg = GeometriesSidecarConfig(
            compute_fields_overlay=[
                ComputedField(
                    kind=ComputedKind.AREA,
                    storage_mode=StatisticStorageMode.COLUMNAR,
                    indexed=True,
                ),
                ComputedField(
                    kind=ComputedKind.LENGTH,
                    storage_mode=StatisticStorageMode.COLUMNAR,
                    indexed=False,
                ),
            ]
        )
        sc = GeometriesSidecar(cfg)
        ddl = sc.get_ddl(
            physical_table="t_test",
            partition_keys=[],
            partition_key_types={},
            has_validity=False,
        )
        assert 'idx_t_test_geometries_area' in ddl
        # length is not indexed.
        assert 'idx_t_test_geometries_length' not in ddl

    def test_jsonb_field_emits_geom_stats_column_only(self) -> None:
        from dynastore.modules.storage.drivers.pg_sidecars.geometries import (
            GeometriesSidecar,
        )
        from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
            GeometriesSidecarConfig,
        )

        cfg = GeometriesSidecarConfig(
            compute_fields_overlay=[
                ComputedField(
                    kind=ComputedKind.CIRCULARITY,
                    storage_mode=StatisticStorageMode.JSONB,
                )
            ]
        )
        sc = GeometriesSidecar(cfg)
        ddl = sc.get_ddl(
            physical_table="t_test",
            partition_keys=[],
            partition_key_types={},
            has_validity=False,
        )
        assert "geom_stats JSONB" in ddl
        # JSONB key does NOT emit a standalone column.
        assert " circularity " not in ddl + " "
        # No B-tree index emitted for JSONB-only fields.
        assert "idx_t_test_geometries_circularity" not in ddl

    def test_mixed_modes_coexist(self) -> None:
        from dynastore.modules.storage.drivers.pg_sidecars.geometries import (
            GeometriesSidecar,
        )
        from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
            GeometriesSidecarConfig,
        )

        cfg = GeometriesSidecarConfig(
            compute_fields_overlay=[
                ComputedField(
                    kind=ComputedKind.AREA,
                    storage_mode=StatisticStorageMode.COLUMNAR,
                    indexed=True,
                ),
                ComputedField(
                    kind=ComputedKind.VERTEX_COUNT,
                    storage_mode=StatisticStorageMode.JSONB,
                ),
                ComputedField(
                    kind=ComputedKind.CENTROID,
                    storage_mode=StatisticStorageMode.COLUMNAR,
                    centroid_type="POINT",
                ),
            ]
        )
        sc = GeometriesSidecar(cfg)
        ddl = sc.get_ddl(
            physical_table="t_test",
            partition_keys=[],
            partition_key_types={},
            has_validity=False,
        )
        assert "area DOUBLE PRECISION" in ddl
        assert "geom_stats JSONB" in ddl
        assert "centroid GEOMETRY(POINT, 4326)" in ddl
        assert "idx_t_test_geometries_area" in ddl

    def test_default_overlay_emits_no_stats(self) -> None:
        from dynastore.modules.storage.drivers.pg_sidecars.geometries import (
            GeometriesSidecar,
        )
        from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
            GeometriesSidecarConfig,
        )

        # Default config carries an empty overlay → no stats columns,
        # no geom_stats JSONB.
        cfg = GeometriesSidecarConfig()
        sc = GeometriesSidecar(cfg)
        ddl = sc.get_ddl(
            physical_table="t_test",
            partition_keys=[],
            partition_key_types={},
            has_validity=False,
        )
        assert "geom_stats" not in ddl
        assert "area DOUBLE PRECISION" not in ddl

    def test_get_internal_columns_tracks_overlay(self) -> None:
        from dynastore.modules.storage.drivers.pg_sidecars.geometries import (
            GeometriesSidecar,
        )
        from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
            GeometriesSidecarConfig,
        )

        cfg = GeometriesSidecarConfig(
            compute_fields_overlay=[
                ComputedField(
                    kind=ComputedKind.CIRCULARITY,
                    storage_mode=StatisticStorageMode.JSONB,
                ),
                ComputedField(
                    kind=ComputedKind.CENTROID,
                    storage_mode=StatisticStorageMode.COLUMNAR,
                    centroid_type="POINT",
                ),
            ]
        )
        sc = GeometriesSidecar(cfg)
        cols = sc.get_internal_columns()
        assert "geom_stats" in cols
        assert "centroid" in cols
