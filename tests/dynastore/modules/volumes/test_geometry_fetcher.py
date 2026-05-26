"""Tests for modules/volumes/geometry_fetcher.py."""

from __future__ import annotations

import pytest

from dynastore.modules.volumes.geometry_fetcher import (
    GeometryQuerySpec,
    build_geometry_query,
    row_to_feature_geometry,
    rows_to_geometries,
    SidecarGeometryFetcher,
)
from dynastore.models.protocols.geometry_fetcher import FeatureGeometry


# ---------------------------------------------------------------------------
# build_geometry_query
# ---------------------------------------------------------------------------


def test_build_geometry_query_basic():
    spec = GeometryQuerySpec(
        schema="tenant",
        hub_table="buildings",
        geometries_table="buildings_geometries",
        feature_ids=["f1", "f2"],
    )
    sql = build_geometry_query(spec)
    assert "ST_AsBinary" in sql
    assert "ST_Force3D" in sql
    # feature IDs must be passed as a named bind parameter, never embedded in SQL
    assert ":feature_ids" in sql
    assert "f1" not in sql
    assert "f2" not in sql
    assert '"tenant"."buildings"' in sql
    assert '"tenant"."buildings_geometries"' in sql
    # geom_column must be double-quoted consistently with other columns
    assert '"geom"' in sql


def test_build_geometry_query_with_height_column():
    spec = GeometryQuerySpec(
        schema="s",
        hub_table="h",
        geometries_table="h_geometries",
        feature_ids=["x"],
        height_column="bldg_height",
    )
    sql = build_geometry_query(spec)
    assert '"bldg_height"' in sql
    assert "COALESCE" in sql


def test_build_geometry_query_no_height_column():
    spec = GeometryQuerySpec(
        schema="s",
        hub_table="h",
        geometries_table="h_geometries",
        feature_ids=["x"],
    )
    sql = build_geometry_query(spec)
    assert "COALESCE" not in sql
    assert "0.0" in sql


def test_build_geometry_query_empty_ids_returns_false_clause():
    spec = GeometryQuerySpec(
        schema="s",
        hub_table="h",
        geometries_table="h_geometries",
        feature_ids=[],
    )
    sql = build_geometry_query(spec)
    assert "WHERE false" in sql


# ---------------------------------------------------------------------------
# row_to_feature_geometry
# ---------------------------------------------------------------------------


def test_row_to_feature_geometry_basic():
    wkb = b"\x01\x01\x00\x00\x00" + b"\x00" * 16  # fake WKB
    row = {"feature_id": "abc", "geom_wkb": wkb, "height": 5.0}
    fg = row_to_feature_geometry(row)
    assert fg is not None
    assert fg.feature_id == "abc"
    assert fg.geom_wkb == wkb
    assert fg.height == 5.0


def test_row_to_feature_geometry_memoryview():
    wkb = b"\x01" * 5
    row = {"feature_id": "x", "geom_wkb": memoryview(wkb), "height": 0}
    fg = row_to_feature_geometry(row)
    assert isinstance(fg.geom_wkb, bytes)


def test_row_to_feature_geometry_null_geom_returns_none():
    row = {"feature_id": "x", "geom_wkb": None, "height": 1.0}
    assert row_to_feature_geometry(row) is None


def test_row_to_feature_geometry_missing_height_defaults_zero():
    wkb = b"\x01" * 5
    row = {"feature_id": "y", "geom_wkb": wkb}
    fg = row_to_feature_geometry(row)
    assert fg.height == 0.0


# ---------------------------------------------------------------------------
# rows_to_geometries helper (drain + filter)
# ---------------------------------------------------------------------------


def test_rows_to_geometries_skips_null_geom():
    rows = [
        {"feature_id": "a", "geom_wkb": b"\x01" * 5, "height": 2.0},
        {"feature_id": "b", "geom_wkb": None, "height": 0.0},
    ]
    result = rows_to_geometries(rows)
    assert len(result) == 1
    assert result[0].feature_id == "a"


# ---------------------------------------------------------------------------
# SidecarGeometryFetcher (async, injected connection)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sidecar_geometry_fetcher_returns_empty_for_no_ids():
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _conn():
        yield None  # never called

    fetcher = SidecarGeometryFetcher(
        connection_factory=_conn,
        schema_resolver=lambda _: "s",
        hub_table_for_collection=lambda c, col: col,
        geometries_table_for_collection=lambda c, col: f"{col}_geometries",
    )
    result = await fetcher.get_geometries("cat", "col", [])
    assert result == []


@pytest.mark.asyncio
async def test_sidecar_geometry_fetcher_queries_db():
    """SidecarGeometryFetcher routes through DQLQuery; verify the bind style."""
    import inspect
    from dynastore.modules.volumes import geometry_fetcher as _gf_mod

    src = inspect.getsource(_gf_mod.SidecarGeometryFetcher.get_geometries)
    # Named-parameter style (DQLQuery-compatible); positional $1 is gone.
    assert ":feature_ids" in src or "feature_ids=" in src
    assert "$1::text[]" not in src
