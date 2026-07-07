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

"""MVT query transform — storage-agnostic geometry resolution.

The MVT transform must source the per-row ``ST_AsMVTGeom`` projection and the
tile-bounds spatial filter from the collection's geometry sidecar, so the
physical geometry column is never hardcoded. These tests build lightweight
collection configs and assert the emitted ``QueryRequest`` references the
sidecar-qualified geometry (``sc_geometries.<col>``) rather than a bare
``geom`` identifier.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from dynastore.models.query_builder import QueryRequest, FieldSelection
from dynastore.modules.tiles.query_transform import MVTQueryTransform
from dynastore.modules.storage.drivers.pg_sidecars import (
    FeatureAttributeSidecarConfig,
    GeometriesSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    SimplificationAlgorithm,
)


def _col_config(geom_column: str = "geom") -> SimpleNamespace:
    return SimpleNamespace(
        sidecars=[
            GeometriesSidecarConfig(geom_column=geom_column),
            FeatureAttributeSidecarConfig(),
        ]
    )


def _mvt_context(col_config, **overrides) -> dict:
    ctx = {
        "geom_format": "MVT",
        "target_srid": 3857,
        "srid": 4326,
        "tile_wkb": b"\x00" * 8,
        "extent": 4096,
        "buffer": 256,
        "col_config": col_config,
    }
    ctx.update(overrides)
    return ctx


def _base_request() -> QueryRequest:
    return QueryRequest(select=[FieldSelection(field="geoid", alias="id")])


def test_mvt_geometry_resolved_via_sidecar_default_column():
    req = MVTQueryTransform().transform_query(_base_request(), _mvt_context(_col_config()))

    raw = " ".join(req.raw_selects)
    assert "ST_AsMVTGeom(" in raw
    assert "sc_geometries.geom" in raw
    assert raw.rstrip().endswith("AS geom")
    # The spatial filter is sidecar-qualified, never a bare ``geom``.
    assert "ST_Intersects(sc_geometries.geom" in (req.raw_where or "")
    # No raw-geometry placeholder leaks into the select — QueryOptimizer.
    # require_geometry already forces the geometry-sidecar JOIN; selecting
    # the raw geom column would surface WKB bytes as an MVT property.
    assert not any(s.alias == "_geom_source" for s in req.select)
    assert req.raw_params["target_srid"] == 3857
    assert req.raw_params["srid"] == 4326
    assert req.raw_params["tile_wkb"] == b"\x00" * 8


def test_mvt_geometry_honors_renamed_geometry_column():
    req = MVTQueryTransform().transform_query(
        _base_request(), _mvt_context(_col_config(geom_column="the_geom"))
    )
    raw = " ".join(req.raw_selects)
    assert "sc_geometries.the_geom" in raw
    assert "ST_Intersects(sc_geometries.the_geom" in (req.raw_where or "")
    assert "sc_geometries.geom " not in raw  # not the default name


@pytest.mark.parametrize(
    "algorithm, expected_fn",
    [
        (SimplificationAlgorithm.DOUGLAS_PEUCKER, "ST_Simplify("),
        (SimplificationAlgorithm.TOPOLOGY_PRESERVING, "ST_SimplifyPreserveTopology("),
        (SimplificationAlgorithm.VISVALINGAM_WHYATT, "ST_SimplifyVW("),
        (SimplificationAlgorithm.SNAP_TO_GRID, "ST_SnapToGrid("),
    ],
)
def test_mvt_simplification_maps_algorithm_to_postgis_function(algorithm, expected_fn):
    """The simplification *algorithm enum value* must map to its PostGIS function.

    The tile pipeline passes ``SimplificationAlgorithm.value`` (e.g.
    ``topology_preserving``) — a bare enum string, never a SQL identifier. Emitting
    it verbatim produces ``function topology_preserving(geometry, unknown) does not
    exist``; it must be translated to ``ST_SimplifyPreserveTopology`` (and siblings).
    """
    req = MVTQueryTransform().transform_query(
        _base_request(),
        _mvt_context(
            _col_config(),
            simplification=0.001,
            simplification_algorithm=algorithm.value,
        ),
    )
    raw = " ".join(req.raw_selects)
    assert expected_fn in raw
    # The raw enum value must never leak through as a SQL function name.
    assert f"{algorithm.value}(" not in raw
    assert req.raw_params["simplification"] == 0.001


def test_mvt_simplification_defaults_to_preserve_topology():
    """Absent an explicit algorithm, simplification falls back to the
    topology-preserving PostGIS function (the accurate default)."""
    req = MVTQueryTransform().transform_query(
        _base_request(),
        _mvt_context(_col_config(), simplification=0.001),
    )
    raw = " ".join(req.raw_selects)
    assert "ST_SimplifyPreserveTopology(" in raw
    assert "ST_SnapToGrid(" not in raw
    assert req.raw_params["simplification"] == 0.001


def test_mvt_simplifies_source_geometry_before_transforming_to_tile_srid():
    """Per-zoom simplification tolerances are source-CRS units.

    For EPSG:4326 sources rendered into WebMercator tiles, simplifying after
    ST_Transform would interpret a degree tolerance as metres and effectively
    disable low-zoom simplification.
    """
    req = MVTQueryTransform().transform_query(
        _base_request(),
        _mvt_context(_col_config(), simplification=0.044),
    )

    raw = " ".join(req.raw_selects)
    assert (
        "ST_AsMVTGeom(ST_Transform("
        "ST_SimplifyPreserveTopology(sc_geometries.geom, :simplification), "
        "CAST(:target_srid AS INTEGER))"
    ) in raw
    assert req.raw_params["simplification"] == 0.044


def test_mvt_extent_buffer_are_literals_not_binds():
    req = MVTQueryTransform().transform_query(_base_request(), _mvt_context(_col_config()))
    raw = " ".join(req.raw_selects)
    assert "4096" in raw and "256" in raw
    assert "extent" not in req.raw_params
    assert "buffer" not in req.raw_params


def test_mvt_transform_srid_is_integer_cast_not_bare_param():
    """The SRID feeding ``ST_Transform`` must be integer-typed in the SQL.

    ``ST_Transform`` is overloaded — ``(geometry, integer)`` and
    ``(geometry, text)`` (a proj string). A bare untyped bind param resolves
    to ``text`` during prepared-statement planning, so asyncpg then rejects the
    integer SRID with ``invalid input for query argument $1: 3857 (expected
    str, got int)``. Wrapping it in ``CAST(... AS INTEGER)`` (as the tile-bounds
    expression already does) pins the param type to integer and resolves the
    overload.
    """
    req = MVTQueryTransform().transform_query(_base_request(), _mvt_context(_col_config()))
    raw = " ".join(req.raw_selects)
    assert "ST_Transform(" in raw
    # No bare ``:target_srid`` param feeding the overloaded ST_Transform.
    assert ", :target_srid)" not in raw
    assert "CAST(:target_srid AS INTEGER)" in raw


def test_mvt_skips_when_no_geometry_sidecar():
    cfg = SimpleNamespace(sidecars=[FeatureAttributeSidecarConfig()])  # no geometry
    req = MVTQueryTransform().transform_query(_base_request(), _mvt_context(cfg))
    assert req.raw_selects == []
    assert not req.raw_where


def test_mvt_skips_when_envelope_params_missing():
    ctx = _mvt_context(_col_config())
    ctx.pop("tile_wkb")
    req = MVTQueryTransform().transform_query(_base_request(), ctx)
    assert req.raw_selects == []
