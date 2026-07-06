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

"""Per-zoom simplification defaults and zoom-aware density filter.

Tests cover:
- TilesConfig.simplification_by_zoom has a non-None default with sensible values.
- The default tolerances decrease (coarser at low zoom, ~0 at high zoom).
- TilesConfig.simplification_algorithm defaults to TOPOLOGY_PRESERVING (accurate default;
  SNAP_TO_GRID is opt-in for speed).
- TilesConfig.min_feature_pixel_area_by_zoom defaults to None (opt-in).
- get_features_as_mvt_filtered adds a WHERE ST_Area clause when density is configured.
- The area clause is absent when min_pixel_area resolves to 0.0 or no match.
- The NOT(…) predicate correctly allows area=0 features (points/lines) through.
"""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.modules.tiles.tiles_config import TilesConfig
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    SimplificationAlgorithm,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries import (
    _SIMPLIFY_SQL_FUNCTIONS,
    _DEFAULT_SIMPLIFY_SQL_FUNCTION,
)
from dynastore.modules.tiles import tiles_db


# ---------------------------------------------------------------------------
# TilesConfig: simplification defaults
# ---------------------------------------------------------------------------


def test_simplification_defaults_are_populated():
    """Default simplification_by_zoom must be a non-empty dict."""
    cfg = TilesConfig()
    assert cfg.simplification_by_zoom is not None
    assert len(cfg.simplification_by_zoom) > 0


def test_simplification_defaults_contain_expected_zoom_keys():
    """Default dict must cover the documented zoom brackets 0, 2, 4, 6, 8, 10."""
    cfg = TilesConfig()
    sbz = cfg.simplification_by_zoom
    assert sbz is not None
    for expected_key in (0, 2, 4, 6, 8, 10):
        assert expected_key in sbz, f"Missing zoom key {expected_key} in simplification_by_zoom"


def test_simplification_defaults_coarsen_at_lower_zoom():
    """Lower zoom brackets must carry larger (coarser) tolerance values.

    The default table uses half-pixel tolerances that decrease as zoom increases.
    Values that are 0.0 mark the 'no simplification' brackets; the first non-zero
    tolerance at the lowest zoom must be strictly larger than the next zoom's value.
    """
    cfg = TilesConfig()
    sbz = cfg.simplification_by_zoom
    assert sbz is not None
    sorted_keys = sorted(sbz.keys())
    # Extract non-zero tolerances in zoom order
    nonzero = [(k, sbz[k]) for k in sorted_keys if sbz[k] > 0]
    assert len(nonzero) >= 2, "Need at least two non-zero brackets to verify ordering"
    for (z_lo, tol_lo), (z_hi, tol_hi) in zip(nonzero, nonzero[1:]):
        assert tol_lo > tol_hi, (
            f"Tolerance at z{z_lo} ({tol_lo}) must be > tolerance at z{z_hi} ({tol_hi})"
        )


def test_simplification_high_zoom_disables():
    """The highest zoom bracket must have tolerance 0.0 (no simplification)."""
    cfg = TilesConfig()
    sbz = cfg.simplification_by_zoom
    assert sbz is not None
    max_zoom_key = max(sbz.keys())
    assert sbz[max_zoom_key] == 0.0, (
        f"Highest zoom key {max_zoom_key} should have tolerance 0.0, got {sbz[max_zoom_key]}"
    )


def test_simplification_defaults_overridable():
    """Callers can override simplification_by_zoom with a custom dict or None."""
    cfg_custom = TilesConfig(simplification_by_zoom={0: 0.5, 5: 0.1})
    assert cfg_custom.simplification_by_zoom == {0: 0.5, 5: 0.1}

    cfg_disabled = TilesConfig(simplification_by_zoom=None)
    assert cfg_disabled.simplification_by_zoom is None

    cfg_empty = TilesConfig(simplification_by_zoom={})
    assert cfg_empty.simplification_by_zoom == {}


# ---------------------------------------------------------------------------
# TilesConfig: simplification algorithm default
# ---------------------------------------------------------------------------


def test_simplification_algorithm_default_is_topology_preserving():
    """Default algorithm must be TOPOLOGY_PRESERVING (accurate; snap_to_grid is opt-in for speed)."""
    cfg = TilesConfig()
    assert cfg.simplification_algorithm == SimplificationAlgorithm.TOPOLOGY_PRESERVING


def test_snap_to_grid_maps_to_postgis_function():
    """SNAP_TO_GRID enum value must map to ST_SnapToGrid in the SQL function table."""
    fn = _SIMPLIFY_SQL_FUNCTIONS.get(SimplificationAlgorithm.SNAP_TO_GRID.value)
    assert fn == "ST_SnapToGrid", (
        f"SNAP_TO_GRID should map to ST_SnapToGrid, got {fn!r}"
    )


def test_default_simplify_sql_function_is_topology_preserving():
    """The fallback (unknown algorithm key) must resolve to ST_SimplifyPreserveTopology.

    The safe fallback matches the default TilesConfig.simplification_algorithm so
    that any code path that skips the dict lookup still produces accurate output.
    ST_SnapToGrid is always available as an explicit opt-in via the SNAP_TO_GRID
    enum value; it is not the default to preserve topology fidelity.
    """
    assert _DEFAULT_SIMPLIFY_SQL_FUNCTION == "ST_SimplifyPreserveTopology"


def test_simplification_algorithm_snap_to_grid_opt_in():
    """snap_to_grid must be fully selectable and produce the correct SQL function."""
    cfg = TilesConfig(simplification_algorithm=SimplificationAlgorithm.SNAP_TO_GRID)
    assert cfg.simplification_algorithm == SimplificationAlgorithm.SNAP_TO_GRID
    fn = _SIMPLIFY_SQL_FUNCTIONS.get(SimplificationAlgorithm.SNAP_TO_GRID.value)
    assert fn == "ST_SnapToGrid"


def test_simplification_algorithm_overridable():
    """Callers can select any registered algorithm."""
    cfg_tp = TilesConfig(simplification_algorithm=SimplificationAlgorithm.TOPOLOGY_PRESERVING)
    assert cfg_tp.simplification_algorithm == SimplificationAlgorithm.TOPOLOGY_PRESERVING

    cfg_dp = TilesConfig(simplification_algorithm=SimplificationAlgorithm.DOUGLAS_PEUCKER)
    assert cfg_dp.simplification_algorithm == SimplificationAlgorithm.DOUGLAS_PEUCKER

    cfg_stg = TilesConfig(simplification_algorithm=SimplificationAlgorithm.SNAP_TO_GRID)
    assert cfg_stg.simplification_algorithm == SimplificationAlgorithm.SNAP_TO_GRID


# ---------------------------------------------------------------------------
# TilesConfig: density filter defaults
# ---------------------------------------------------------------------------


def test_density_config_default_is_none():
    """min_feature_pixel_area_by_zoom must default to None (opt-in; safe for all collections)."""
    cfg = TilesConfig()
    assert cfg.min_feature_pixel_area_by_zoom is None


def test_density_config_overridable():
    """Operators can activate the density filter by setting a zoom-keyed dict."""
    cfg = TilesConfig(min_feature_pixel_area_by_zoom={0: 4.0, 4: 2.0, 8: 0.0})
    assert cfg.min_feature_pixel_area_by_zoom == {0: 4.0, 4: 2.0, 8: 0.0}


def test_line_density_config_default_is_none():
    """min_feature_pixel_length_by_zoom must default to None (opt-in; safe for all collections)."""
    cfg = TilesConfig()
    assert cfg.min_feature_pixel_length_by_zoom is None


def test_line_density_config_overridable():
    """Operators can activate the line density filter by setting a zoom-keyed dict."""
    cfg = TilesConfig(min_feature_pixel_length_by_zoom={0: 2.0, 4: 1.0, 8: 0.0})
    assert cfg.min_feature_pixel_length_by_zoom == {0: 2.0, 4: 1.0, 8: 0.0}


# ---------------------------------------------------------------------------
# tiles_db: density WHERE clause in generated SQL
# ---------------------------------------------------------------------------


def _make_collection_meta(
    min_feature_pixel_area_by_zoom=None,
    min_feature_pixel_length_by_zoom=None,
):
    """Return a minimal resolved-collection meta dict for tiles_db tests."""
    return {
        "catalog_id": "cat",
        "collection_id": "col",
        "col_config": MagicMock(),
        "source_srid": 4326,
        "simplification_by_zoom": {},
        "min_feature_pixel_area_by_zoom": min_feature_pixel_area_by_zoom,
        "min_feature_pixel_length_by_zoom": min_feature_pixel_length_by_zoom,
    }


def _tms_def_stub(z_id="2"):
    """Minimal TMS stub that satisfies _calculate_tile_envelope_wkb."""
    matrix = MagicMock()
    matrix.id = z_id
    matrix.pointOfOrigin = [-180.0, 90.0]
    matrix.tileWidth = 256
    matrix.tileHeight = 256
    matrix.cellSize = 90.0 / 256  # roughly z2 cell size
    tms = MagicMock()
    tms.tileMatrices = [matrix]
    return tms


async def _run_mvt_filtered(meta, z="2", extent=4096):
    """Run get_features_as_mvt_filtered with mocked DB / ItemsService and return the SQL."""
    # _srid_exists is memoized (#2960); clear it so every test call re-hits
    # the mocked DQLQuery instead of serving a cached SRID check from a
    # previous test in this module.
    tiles_db._srid_exists.cache_clear()
    conn = AsyncMock()
    captured_sql: list[str] = []

    class _CapturingDQLQuery:
        def __init__(self, sql, **kwargs):
            self._sql = sql

        def execute(self, conn, **params):
            captured_sql.append(self._sql)
            # First call: SRID exists check → True
            # Second call: final MVT query → b"mvt"
            if len(captured_sql) == 1:
                return _async_return(True)
            return _async_return(b"mvt")

    def _async_return(value):
        async def _inner(*args, **kwargs):
            return value
        return _inner()

    with patch("dynastore.modules.tiles.tiles_db.DQLQuery", side_effect=_CapturingDQLQuery):
        with patch("dynastore.tools.discovery.get_protocol") as mock_get_proto:
            from dynastore.models.protocols import ItemsProtocol

            mock_items = AsyncMock()
            mock_items.get_features_query = AsyncMock(return_value=("SELECT 1", {}))
            # Only resolve ItemsProtocol; other protocol lookups (e.g. the
            # cache module's own ConfigsProtocol lookup for
            # slow_path_timeout_seconds) must see "not registered" (None)
            # rather than this unrelated AsyncMock.
            mock_get_proto.side_effect = (
                lambda proto: mock_items if proto is ItemsProtocol else None
            )

            await tiles_db.get_features_as_mvt_filtered(
                conn=conn,
                resolved_collections=[meta],
                tms_def=_tms_def_stub(z),
                target_srid=3857,
                z=z,
                x=0,
                y=0,
                extent=extent,
            )

    # The last captured SQL is the final MVT query.
    return captured_sql[-1] if captured_sql else ""


@pytest.mark.asyncio
async def test_density_filter_sql_added_at_low_zoom():
    """When min_feature_pixel_area_by_zoom is set and the zoom matches, WHERE ST_Area appears."""
    meta = _make_collection_meta(min_feature_pixel_area_by_zoom={0: 4.0, 8: 0.0})
    sql = await _run_mvt_filtered(meta, z="2")  # z2 ≥ key 0 → area = 4.0
    assert "ST_Area" in sql, "Expected density WHERE clause in SQL"
    assert "min_pixel_area" in sql, "Expected :min_pixel_area bind param in WHERE clause"
    assert "NOT" in sql, "Expected NOT(...) predicate to protect area=0 features"


@pytest.mark.asyncio
async def test_density_filter_sql_absent_when_area_is_zero():
    """0.0 in the density map disables the filter for that bracket."""
    meta = _make_collection_meta(min_feature_pixel_area_by_zoom={0: 4.0, 2: 0.0})
    sql = await _run_mvt_filtered(meta, z="2")  # z2 ≥ key 2 → area = 0.0 → disabled
    assert "ST_Area" not in sql, "WHERE clause must be absent when resolved area is 0.0"


@pytest.mark.asyncio
async def test_density_filter_sql_absent_when_config_is_none():
    """None density config (default) produces no WHERE clause."""
    meta = _make_collection_meta(min_feature_pixel_area_by_zoom=None)
    sql = await _run_mvt_filtered(meta, z="2")
    assert "ST_Area" not in sql


@pytest.mark.asyncio
async def test_density_filter_sql_absent_when_zoom_above_cutoff():
    """No filter when the zoom is above the highest density key that has a non-zero value."""
    # {0: 4.0, 4: 0.0} → at z=6, key 4 wins → area = 0.0 → no filter
    meta = _make_collection_meta(min_feature_pixel_area_by_zoom={0: 4.0, 4: 0.0})
    sql = await _run_mvt_filtered(meta, z="6")
    assert "ST_Area" not in sql


@pytest.mark.asyncio
async def test_density_filter_not_predicate_preserves_zero_area():
    """The SQL predicate  NOT (ST_Area > 0 AND ST_Area < :threshold)  must appear verbatim.

    This guarantees that area=0 features (points, lines) are never filtered:
      - ST_Area(point) = 0
      - 0 > 0 is False
      - False AND ... is False regardless of threshold
      - NOT False = True → row kept
    """
    meta = _make_collection_meta(min_feature_pixel_area_by_zoom={0: 4.0})
    sql = await _run_mvt_filtered(meta, z="0")
    # The critical safety clause
    assert "NOT (ST_Area(mvtgeom.geom) > 0" in sql
    assert "AND ST_Area(mvtgeom.geom) < :min_pixel_area)" in sql


# ---------------------------------------------------------------------------
# tiles_db: LINE density (length) WHERE clause in generated SQL
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_line_density_filter_sql_added_at_low_zoom():
    """When min_feature_pixel_length_by_zoom is set and the zoom matches, WHERE ST_Length appears."""
    meta = _make_collection_meta(min_feature_pixel_length_by_zoom={0: 2.0, 8: 0.0})
    sql = await _run_mvt_filtered(meta, z="2")  # z2 ≥ key 0 → length = 2.0
    assert "ST_Length" in sql, "Expected line density WHERE clause in SQL"
    assert "min_pixel_length" in sql, "Expected :min_pixel_length bind param in WHERE clause"


@pytest.mark.asyncio
async def test_line_density_filter_sql_absent_when_length_is_zero():
    """0.0 in the line density map disables the filter for that bracket."""
    meta = _make_collection_meta(min_feature_pixel_length_by_zoom={0: 2.0, 2: 0.0})
    sql = await _run_mvt_filtered(meta, z="2")  # z2 ≥ key 2 → length = 0.0 → disabled
    assert "ST_Length" not in sql, "WHERE clause must be absent when resolved length is 0.0"


@pytest.mark.asyncio
async def test_line_density_filter_sql_absent_when_config_is_none():
    """None line density config (default) produces no ST_Length WHERE clause."""
    meta = _make_collection_meta(min_feature_pixel_length_by_zoom=None)
    sql = await _run_mvt_filtered(meta, z="2")
    assert "ST_Length" not in sql


@pytest.mark.asyncio
async def test_line_density_not_predicate_preserves_zero_length():
    """The SQL predicate  NOT (ST_Length > 0 AND ST_Length < :threshold)  must appear verbatim.

    This guarantees that length=0 features (points, polygons) are never filtered:
      - ST_Length(polygon) = 0 in PostGIS
      - 0 > 0 is False → False AND ... is False → NOT False = True → row kept
    """
    meta = _make_collection_meta(min_feature_pixel_length_by_zoom={0: 2.0})
    sql = await _run_mvt_filtered(meta, z="0")
    assert "NOT (ST_Length(mvtgeom.geom) > 0" in sql
    assert "AND ST_Length(mvtgeom.geom) < :min_pixel_length)" in sql


@pytest.mark.asyncio
async def test_area_and_line_density_filters_compose():
    """Area and length filters are independent and AND-combined in one WHERE clause."""
    meta = _make_collection_meta(
        min_feature_pixel_area_by_zoom={0: 4.0},
        min_feature_pixel_length_by_zoom={0: 2.0},
    )
    sql = await _run_mvt_filtered(meta, z="0")
    assert ":min_pixel_area" in sql and ":min_pixel_length" in sql
    # Both predicates present and AND-combined in the outer density filter
    # (which sits after the FROM mvtgeom clause).
    density_clause = sql.split("FROM mvtgeom", 1)[-1]
    assert "NOT (ST_Area(mvtgeom.geom) > 0" in density_clause
    assert "NOT (ST_Length(mvtgeom.geom) > 0" in density_clause
    assert " AND " in density_clause
