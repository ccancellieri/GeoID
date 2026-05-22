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

from dynastore.models.query_builder import QueryRequest, FieldSelection
from dynastore.modules.tiles.query_transform import MVTQueryTransform
from dynastore.modules.storage.drivers.pg_sidecars import (
    FeatureAttributeSidecarConfig,
    GeometriesSidecarConfig,
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
    # A placeholder selection forces the geometry-sidecar JOIN.
    assert any(s.alias == "_geom_source" for s in req.select)
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


def test_mvt_simplification_binds_when_configured():
    req = MVTQueryTransform().transform_query(
        _base_request(),
        _mvt_context(
            _col_config(),
            simplification=0.001,
            simplification_algorithm="ST_SimplifyPreserveTopology",
        ),
    )
    raw = " ".join(req.raw_selects)
    assert "ST_SimplifyPreserveTopology(" in raw
    assert req.raw_params["simplification"] == 0.001


def test_mvt_extent_buffer_are_literals_not_binds():
    req = MVTQueryTransform().transform_query(_base_request(), _mvt_context(_col_config()))
    raw = " ".join(req.raw_selects)
    assert "4096" in raw and "256" in raw
    assert "extent" not in req.raw_params
    assert "buffer" not in req.raw_params


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
