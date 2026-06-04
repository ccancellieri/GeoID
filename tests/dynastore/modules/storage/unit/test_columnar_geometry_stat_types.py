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
"""Columnar geometry-stat value/column type consistency.

A geometry statistic stored in a dedicated (COLUMNAR) column must materialise a
value whose type matches the column the geometries sidecar declares for it.
``centroid`` / ``centroid_3d`` columns are ``GEOMETRY(POINT[Z])`` — their value
must be WKB hex, not a coordinate array — while a JSONB-stored centroid keeps
the ``[x, y]`` array. ``temporal_duration`` is a numeric (seconds) value, so its
column must be numeric, not ``INTERVAL``.

Regression for the live ingest failure:
``column "centroid" is of type geometry but expression is of type numeric[]``.
"""
from types import SimpleNamespace

from shapely import wkb as shp_wkb
from shapely.geometry import Polygon

from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
    StatisticStorageMode,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries import (
    GeometriesSidecar,
    GeometriesSidecarConfig,
)
from dynastore.tools.geospatial import (
    compute_derived_fields,
    compute_place_derived_fields,
)

_UNIT_SQUARE = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])

_LINESTRING_3D = {
    "type": "LineString",
    "coordinates": [[0.0, 0.0, 10.0], [2.0, 0.0, 30.0]],
}


def _as_point(value):
    """Parse a WKB-hex string into a shapely point (fails if it is not WKB)."""
    assert isinstance(value, str), f"expected WKB hex string, got {type(value)}: {value!r}"
    return shp_wkb.loads(value, hex=True)


# ---------------------------------------------------------------------------
# centroid (2D)
# ---------------------------------------------------------------------------


def test_centroid_columnar_emits_wkb_hex_not_array():
    """COLUMNAR centroid → GEOMETRY(POINT) column → WKB hex, not numeric[].

    This is the live crash: ``type=null`` left ``centroid_type`` unset and the
    value path emitted ``[x, y]`` while the DDL made a geometry column.
    """
    fields = [
        ComputedField(kind=ComputedKind.CENTROID, storage_mode=StatisticStorageMode.COLUMNAR)
    ]
    out = compute_derived_fields(_UNIT_SQUARE, {}, fields)
    pt = _as_point(out["centroid"])
    assert (round(pt.x, 6), round(pt.y, 6)) == (0.5, 0.5)
    assert not pt.has_z


def test_centroid_columnar_with_explicit_point_type_still_wkb():
    fields = [
        ComputedField(
            kind=ComputedKind.CENTROID,
            storage_mode=StatisticStorageMode.COLUMNAR,
            centroid_type="POINT",
        )
    ]
    out = compute_derived_fields(_UNIT_SQUARE, {}, fields)
    pt = _as_point(out["centroid"])
    assert (round(pt.x, 6), round(pt.y, 6)) == (0.5, 0.5)


def test_centroid_jsonb_emits_coordinate_array():
    """JSONB centroid stays a coordinate array (stored in the geom_stats blob)."""
    fields = [
        ComputedField(kind=ComputedKind.CENTROID, storage_mode=StatisticStorageMode.JSONB)
    ]
    out = compute_derived_fields(_UNIT_SQUARE, {}, fields)
    assert out["centroid"] == [0.5, 0.5]


def test_centroid_unstored_emits_coordinate_array():
    """No storage mode (computed only, e.g. to feed an identity rule) → array."""
    fields = [ComputedField(kind=ComputedKind.CENTROID)]
    out = compute_derived_fields(_UNIT_SQUARE, {}, fields)
    assert out["centroid"] == [0.5, 0.5]


# ---------------------------------------------------------------------------
# centroid_3d (place/JSON-FG)
# ---------------------------------------------------------------------------


def test_centroid_3d_columnar_emits_wkb_pointz():
    """COLUMNAR centroid_3d → GEOMETRY(POINTZ) column → 3D WKB hex, not [x,y,z]."""
    fields = [
        ComputedField(kind=ComputedKind.CENTROID_3D, storage_mode=StatisticStorageMode.COLUMNAR)
    ]
    out = compute_place_derived_fields(_LINESTRING_3D, fields)
    pt = _as_point(out["centroid_3d"])
    assert pt.has_z
    assert round(pt.x, 6) == 1.0


def test_centroid_3d_jsonb_emits_coordinate_array():
    fields = [
        ComputedField(kind=ComputedKind.CENTROID_3D, storage_mode=StatisticStorageMode.JSONB)
    ]
    out = compute_place_derived_fields(_LINESTRING_3D, fields)
    assert isinstance(out["centroid_3d"], list)
    assert len(out["centroid_3d"]) == 3


# ---------------------------------------------------------------------------
# temporal_duration column type (numeric seconds, never INTERVAL)
# ---------------------------------------------------------------------------


def test_temporal_duration_columnar_sql_type_is_numeric():
    sidecar = GeometriesSidecar(GeometriesSidecarConfig())
    field = ComputedField(
        kind=ComputedKind.TEMPORAL_DURATION, storage_mode=StatisticStorageMode.COLUMNAR
    )
    sql_type = sidecar._columnar_sql_type(field, 4326)
    assert "INTERVAL" not in sql_type.upper()
    assert "DOUBLE PRECISION" in sql_type.upper()


# ---------------------------------------------------------------------------
# schema-driven geometry-column wrap set (covers renamed centroid + centroid_3d)
# ---------------------------------------------------------------------------


def test_geometry_value_columns_includes_renamed_centroid_and_3d():
    sidecar = GeometriesSidecar(
        GeometriesSidecarConfig(
            compute_fields_overlay=[
                ComputedField(
                    kind=ComputedKind.CENTROID,
                    storage_mode=StatisticStorageMode.COLUMNAR,
                    name="my_centroid",
                ),
                ComputedField(
                    kind=ComputedKind.CENTROID_3D,
                    storage_mode=StatisticStorageMode.COLUMNAR,
                ),
                # numeric stat — must NOT be treated as a geometry column
                ComputedField(
                    kind=ComputedKind.AREA, storage_mode=StatisticStorageMode.COLUMNAR
                ),
            ]
        )
    )
    cols = sidecar.geometry_value_columns()
    assert "geom" in cols
    assert "bbox_geom" in cols
    assert "my_centroid" in cols
    assert "centroid_3d" in cols
    assert "area" not in cols


# ---------------------------------------------------------------------------
# read-for-exposure projection: a COLUMNAR centroid must surface as a clean
# coordinate array (matching the JSONB centroid), NOT raw EWKB. Regression for
# the exposed-columnar-centroid-as-binary gap.
# ---------------------------------------------------------------------------


def _columnar_centroid_sidecar(centroid_type=None, name=None):
    return GeometriesSidecar(
        GeometriesSidecarConfig(
            compute_fields_overlay=[
                ComputedField(
                    kind=ComputedKind.CENTROID,
                    storage_mode=StatisticStorageMode.COLUMNAR,
                    centroid_type=centroid_type,
                    name=name,
                )
            ]
        )
    )


def _centroid_select_expr(fields, key="centroid"):
    # Aliases are quoted to preserve case (#719); accept both the quoted and
    # bare forms so the helper is future-proof against formatting changes.
    matches = [f for f in fields if f.endswith(f' as "{key}"') or f.endswith(f" as {key}")]
    assert matches, f"no projection aliased ' as {key}' (or ' as \"{key}\"') in {fields}"
    return matches[0]


def _select_request(*select_fields):
    """Minimal QueryRequest stand-in for selective-mode get_select_fields."""
    return SimpleNamespace(
        select=[SimpleNamespace(field=f) for f in select_fields],
        filters=[],
        sort=[],
    )


def test_columnar_centroid_select_projects_coordinate_array_not_ewkb():
    """get_select_fields must project a COLUMNAR centroid as ARRAY[ST_X, ST_Y].

    The old projection used ``ST_AsEWKB`` which surfaced raw binary in the
    feature property; the fixed one yields a ``double precision[]`` that asyncpg
    decodes to ``[x, y]`` — matching the JSONB centroid.
    """
    sidecar = _columnar_centroid_sidecar()
    expr = _centroid_select_expr(sidecar.get_select_fields(request=None))
    assert "ST_AsEWKB" not in expr
    assert "ARRAY[" in expr
    assert "ST_X(" in expr and "ST_Y(" in expr
    assert "ST_Z(" not in expr  # 2D POINT: no Z component


def test_columnar_centroid_select_is_null_guarded():
    """A missing centroid stays NULL, not ``[null, null]``."""
    sidecar = _columnar_centroid_sidecar()
    expr = _centroid_select_expr(sidecar.get_select_fields(request=None))
    assert "CASE WHEN" in expr and "IS NULL" in expr


def test_columnar_centroid_pointz_select_includes_z():
    """A POINTZ centroid column projects the third coordinate."""
    sidecar = _columnar_centroid_sidecar(centroid_type="POINTZ")
    expr = _centroid_select_expr(sidecar.get_select_fields(request=None))
    assert "ST_Z(" in expr
    assert "ST_AsEWKB" not in expr


def test_columnar_centroid_select_honours_renamed_column():
    sidecar = _columnar_centroid_sidecar(name="my_centroid")
    expr = _centroid_select_expr(
        sidecar.get_select_fields(request=None), key="my_centroid"
    )
    assert "my_centroid" in expr
    assert "ST_AsEWKB" not in expr


def test_columnar_centroid_selective_read_projects_array():
    """The selective (API) read path also yields the coordinate array."""
    sidecar = _columnar_centroid_sidecar()
    fields = sidecar.get_select_fields(request=_select_request("centroid"))
    expr = _centroid_select_expr(fields)
    assert "ST_AsEWKB" not in expr
    assert "ARRAY[" in expr and "ST_X(" in expr


def test_geom_projection_unchanged():
    """The fix is centroid-only: geom still reads as GeoJSON."""
    sidecar = _columnar_centroid_sidecar()
    geom = _centroid_select_expr(
        sidecar.get_select_fields(request=_select_request()), key="geom"
    )
    assert "ST_AsGeoJSON" in geom
