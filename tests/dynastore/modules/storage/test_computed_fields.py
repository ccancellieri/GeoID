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
    AttributeStat,
    ComputedField,
    ComputedKind,
    DeriveSpec,
    FeatureType,
    GeometryStat,
    IdentityRule,
    PATH_EXTRACTED_KINDS,
    SPATIAL_CELL_KINDS,
    SpatialCell,
    StatisticStorageMode,
)
from dynastore.modules.storage.driver_config import WriteConflictPolicy
from dynastore.tools.geospatial import compute_derived_fields


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
        r = IdentityRule(match_on=["external_id"])
        assert r.on_match is None

    def test_on_match_override(self) -> None:
        r = IdentityRule(
            match_on=["geometry_hash"],
            on_match=WriteConflictPolicy.REFUSE_FAIL,
        )
        assert r.on_match == WriteConflictPolicy.REFUSE_FAIL

    def test_and_composition(self) -> None:
        r = IdentityRule(match_on=["external_id", "geometry_hash"])
        assert r.match_on == ["external_id", "geometry_hash"]
        assert len(r.match_on) == 2


class TestFeatureType:
    def test_defaults(self) -> None:
        ft = FeatureType()
        # ``None`` (default) = surface all declared schema fields; ``[]`` =
        # surface nothing; non-empty list = schema baseline + listed computed.
        assert ft.expose is None
        assert ft.failure_mode == "best_effort"
        # geoid is the default feature id; created is opt-in (#1212)
        assert ft.external_id_as_feature_id is False
        assert ft.expose_created is False

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
# Authoring layer — DeriveSpec buckets bridge to ComputedField (#957/#950).
# ---------------------------------------------------------------------------


class TestDeriveSpecBridge:
    def test_every_kind_reachable_via_exactly_one_bucket(self) -> None:
        """Partitioning invariant: each ComputedKind maps to exactly one
        DeriveSpec bucket. Adding a kind without classifying it fails here
        (and would otherwise be rejected by GeometryStat at runtime)."""
        from dynastore.modules.storage.computed_fields import (
            _GEOMETRY_STAT_KINDS,
            _KIND_CONTENT_HASH,
            _KIND_GRID,
        )

        for kind in ComputedKind:
            buckets = []
            if kind == ComputedKind.EXTERNAL_ID:
                buckets.append("external_id")
            if kind in _KIND_CONTENT_HASH:
                buckets.append("content_hashes")
            if kind in _KIND_GRID:
                buckets.append("spatial_cells")
            if kind == ComputedKind.ATTRIBUTE_STAT:
                buckets.append("attribute_stats")
            if kind in _GEOMETRY_STAT_KINDS:
                buckets.append("geometry_stats")
            assert len(buckets) == 1, f"{kind.value} -> {buckets}"

    def test_buckets_flatten_to_computed_fields(self) -> None:
        spec = DeriveSpec(
            external_id="properties.code",
            content_hashes=["geometry", "attributes"],
            spatial_cells=[SpatialCell(grid="geohash", resolution=7)],
            geometry_stats=[
                GeometryStat(
                    stat=ComputedKind.CENTROID,
                    store=StatisticStorageMode.COLUMNAR,
                    indexed=True,
                    type="POINT",
                )
            ],
            attribute_stats=[
                AttributeStat(source="properties.pop", store=StatisticStorageMode.COLUMNAR)
            ],
        )
        names = {cf.resolved_name for cf in spec.to_computed_fields()}
        assert names == {
            "external_id",
            "geometry_hash",
            "attributes_hash",
            "geohash_7",
            "centroid",
            "pop",
        }

    def test_round_trip_classify_back(self) -> None:
        spec = DeriveSpec(
            external_id="properties.code",
            content_hashes=["geometry"],
            spatial_cells=[SpatialCell(grid="h3", resolution=9)],
            geometry_stats=[GeometryStat(stat=ComputedKind.AREA)],
            attribute_stats=[AttributeStat(source="properties.pop")],
        )
        back = DeriveSpec.from_computed_fields(spec.to_computed_fields())
        assert back.external_id == "properties.code"
        assert back.content_hashes == ["geometry"]
        assert back.spatial_cells[0].grid == "h3"
        assert back.spatial_cells[0].resolution == 9
        assert back.geometry_stats[0].stat == ComputedKind.AREA
        assert back.attribute_stats[0].source == "properties.pop"

    def test_geometry_stat_rejects_non_stat_kind(self) -> None:
        with pytest.raises(ValidationError):
            GeometryStat(stat=ComputedKind.GEOHASH)

    def test_geometry_stat_type_only_on_centroid(self) -> None:
        with pytest.raises(ValidationError):
            GeometryStat(stat=ComputedKind.AREA, type="POINT")

    def test_spatial_cell_resolution_range(self) -> None:
        with pytest.raises(ValidationError):
            SpatialCell(grid="geohash", resolution=99)


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


class TestGeodesicMetrics:
    """AREA/PERIMETER/LENGTH must be metric when the geometry CRS is geographic.

    Regression for areas computed in square degrees: a geometry already
    transformed to EPSG:4326 (lon/lat) has a Shapely ``.area`` in degrees²,
    which is meaningless and latitude-dependent. When ``srid`` identifies a
    geographic CRS the metrics are computed on the WGS84 ellipsoid (m² / m).
    Without ``srid`` the planar values are preserved (back-compat).
    """

    @pytest.fixture
    def deg_box(self) -> Polygon:
        # 1°×1° box straddling the equator/prime-meridian.
        return Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])

    def test_area_is_geodesic_m2_when_srid_geographic(self, deg_box: Polygon) -> None:
        from pyproj import Geod

        expected, _ = Geod(ellps="WGS84").geometry_area_perimeter(deg_box)
        out = compute_derived_fields(
            deg_box, {}, [ComputedField(kind=ComputedKind.AREA)], srid=4326
        )
        assert out["area"] == pytest.approx(abs(expected), rel=1e-6)
        # Sanity: a 1°×1° box near the equator is ~1.23e10 m², never 1.0.
        assert out["area"] > 1.0e10

    def test_area_stays_planar_without_srid(self, deg_box: Polygon) -> None:
        out = compute_derived_fields(
            deg_box, {}, [ComputedField(kind=ComputedKind.AREA)]
        )
        assert out["area"] == pytest.approx(1.0)

    def test_perimeter_is_geodesic_m_when_srid_geographic(
        self, deg_box: Polygon
    ) -> None:
        from pyproj import Geod

        _, expected = Geod(ellps="WGS84").geometry_area_perimeter(deg_box)
        out = compute_derived_fields(
            deg_box, {}, [ComputedField(kind=ComputedKind.PERIMETER)], srid=4326
        )
        assert out["perimeter"] == pytest.approx(abs(expected), rel=1e-6)
        assert out["perimeter"] > 1.0e5

    def test_length_is_geodesic_m_when_srid_geographic(self) -> None:
        from shapely.geometry import LineString
        from pyproj import Geod

        line = LineString([(0, 0), (1, 0)])
        expected = Geod(ellps="WGS84").geometry_length(line)
        out = compute_derived_fields(
            line, {}, [ComputedField(kind=ComputedKind.LENGTH)], srid=4326
        )
        assert out["length"] == pytest.approx(expected, rel=1e-6)
        # ~111 km for one degree of longitude at the equator.
        assert out["length"] > 1.0e5

    def test_circularity_stays_dimensionless_when_geographic(
        self, deg_box: Polygon
    ) -> None:
        import math

        out = compute_derived_fields(
            deg_box, {}, [ComputedField(kind=ComputedKind.CIRCULARITY)], srid=4326
        )
        # A near-square remains ~pi/4 whether computed planar or geodesic.
        assert out["circularity"] == pytest.approx(math.pi / 4, rel=1e-2)


class TestVertexCountAllCoordinates:
    """VERTEX_COUNT counts every coordinate (ST_NPoints parity, #3155)."""

    def _count(self, geom) -> int:
        out = compute_derived_fields(
            geom, {}, [ComputedField(kind=ComputedKind.VERTEX_COUNT)]
        )
        return out["vertex_count"]

    def test_polygon_with_hole_counts_interior_ring(self) -> None:
        shell = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
        hole = [(2, 2), (4, 2), (4, 4), (2, 2)]
        poly = Polygon(shell, [hole])
        # 5 shell coords + 4 hole coords, closing points included — what
        # PostGIS ST_NPoints reports for the same geometry.
        assert self._count(poly) == 9

    def test_multipolygon_counts_all_parts_and_rings(self) -> None:
        from shapely.geometry import MultiPolygon

        a = Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])
        b = Polygon(
            [(5, 5), (9, 5), (9, 9), (5, 9), (5, 5)],
            [[(6, 6), (7, 6), (7, 7), (6, 6)]],
        )
        assert self._count(MultiPolygon([a, b])) == 4 + 5 + 4

    def test_linestring_and_point(self) -> None:
        from shapely.geometry import LineString

        assert self._count(LineString([(0, 0), (1, 1), (2, 2)])) == 3
        assert self._count(Point(1, 1)) == 1

    def test_3d_coordinates_count_like_2d(self) -> None:
        from shapely.geometry import LineString

        assert self._count(LineString([(0, 0, 1), (1, 1, 2), (2, 2, 3)])) == 3


class TestDefaultDeriveSpec:
    """The platform default enables every geometry/place stat (#3155)."""

    def test_derive_spec_itself_stays_empty(self) -> None:
        # Regression guard: DeriveSpec() is the explicit opt-out shape and
        # many call sites construct it expecting no derivations.
        assert DeriveSpec().to_computed_fields() == []

    def test_default_contains_every_geometry_stat_kind(self) -> None:
        from dynastore.modules.storage.computed_fields import (
            _GEOMETRY_STAT_KINDS,
            default_derive_spec,
        )

        spec = default_derive_spec()
        kinds = {gs.stat for gs in spec.geometry_stats}
        assert kinds == set(_GEOMETRY_STAT_KINDS)

    def test_default_is_columnar_with_render_rank_indexes(self) -> None:
        from dynastore.modules.storage.computed_fields import default_derive_spec

        spec = default_derive_spec()
        assert all(
            gs.store == StatisticStorageMode.COLUMNAR for gs in spec.geometry_stats
        )
        indexed = {gs.stat for gs in spec.geometry_stats if gs.indexed}
        assert indexed == {
            ComputedKind.AREA,
            ComputedKind.LENGTH,
            ComputedKind.VERTEX_COUNT,
        }

    def test_default_leaves_identity_axes_alone(self) -> None:
        from dynastore.modules.storage.computed_fields import default_derive_spec

        spec = default_derive_spec()
        assert spec.external_id is None
        assert spec.content_hashes == []
        assert spec.spatial_cells == []
        assert spec.attribute_stats == []

    def test_default_flattens_with_unique_names(self) -> None:
        from dynastore.modules.storage.computed_fields import default_derive_spec

        fields = default_derive_spec().to_computed_fields()
        names = [f.resolved_name for f in fields]
        assert len(names) == len(set(names))
        assert all(f.storage_mode is StatisticStorageMode.COLUMNAR for f in fields)

    def test_items_write_policy_defaults_to_all_stats(self) -> None:
        from dynastore.modules.storage.driver_config import ItemsWritePolicy
        from dynastore.modules.storage.computed_fields import default_derive_spec

        policy = ItemsWritePolicy()
        assert policy.derive == default_derive_spec()
        assert len(policy.compute) == len(
            default_derive_spec().to_computed_fields()
        )

    def test_items_write_policy_explicit_empty_derive_opts_out(self) -> None:
        from dynastore.modules.storage.driver_config import ItemsWritePolicy

        policy = ItemsWritePolicy(derive=DeriveSpec())
        assert policy.compute == []

    def test_stored_config_with_explicit_derive_wins_over_default(self) -> None:
        # Backward compatibility: a persisted config that authored its own
        # derive (even a sparse one) must round-trip unchanged.
        from dynastore.modules.storage.driver_config import ItemsWritePolicy

        stored = {
            "derive": {
                "geometry_stats": [
                    {"stat": "vertex_count", "store": "columnar", "indexed": True}
                ]
            }
        }
        policy = ItemsWritePolicy.model_validate(stored)
        assert [gs.stat for gs in policy.derive.geometry_stats] == [
            ComputedKind.VERTEX_COUNT
        ]


class TestGeodesicRingSemantics:
    """Geodesic polygon metrics follow polygon semantics, not ring winding.

    ``Geod.geometry_area_perimeter`` is ring-naive: a hole's area flips sign
    with its winding and its length never reaches the perimeter. The
    ring-summing wrapper pins parity with planar Shapely and with PostGIS
    ``ST_Area``/``ST_Perimeter`` on ``geography`` (#3155).
    """

    SHELL = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
    HOLE_CCW = [(2, 2), (4, 2), (4, 4), (2, 2)]
    HOLE_CW = [(2, 2), (4, 4), (4, 2), (2, 2)]

    def _stats(self, poly: Polygon) -> dict:
        return compute_derived_fields(
            poly,
            {},
            [
                ComputedField(kind=ComputedKind.AREA),
                ComputedField(kind=ComputedKind.PERIMETER),
                ComputedField(kind=ComputedKind.LENGTH),
                ComputedField(kind=ComputedKind.CONVEXITY),
            ],
            srid=4326,
        )

    def test_hole_area_is_subtracted_regardless_of_winding(self) -> None:
        shell_only = self._stats(Polygon(self.SHELL))
        ccw = self._stats(Polygon(self.SHELL, [self.HOLE_CCW]))
        cw = self._stats(Polygon(self.SHELL, [self.HOLE_CW]))
        assert ccw["area"] == pytest.approx(cw["area"], rel=1e-12)
        assert ccw["area"] < shell_only["area"]

    def test_hole_ring_counts_toward_perimeter_and_length(self) -> None:
        shell_only = self._stats(Polygon(self.SHELL))
        holed = self._stats(Polygon(self.SHELL, [self.HOLE_CCW]))
        assert holed["perimeter"] > shell_only["perimeter"]
        # A polygon's geodesic length is its full ring length — identical to
        # the perimeter, mirroring planar ``.length``.
        assert holed["length"] == pytest.approx(holed["perimeter"], rel=1e-12)

    def test_convexity_never_exceeds_one(self) -> None:
        holed = self._stats(Polygon(self.SHELL, [self.HOLE_CCW]))
        assert holed["convexity"] < 1.0
