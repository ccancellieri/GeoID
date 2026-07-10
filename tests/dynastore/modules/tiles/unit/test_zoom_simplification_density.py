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
    max_features_per_tile_by_zoom=None,
    feature_rank_column=None,
    min_feature_rank_by_zoom=None,
    tile_byte_budget=None,
    feature_density_column=None,
    max_feature_density_by_zoom=None,
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
        "max_features_per_tile_by_zoom": max_features_per_tile_by_zoom,
        "feature_rank_column": feature_rank_column,
        "min_feature_rank_by_zoom": min_feature_rank_by_zoom,
        "tile_byte_budget": tile_byte_budget,
        "feature_density_column": feature_density_column,
        "max_feature_density_by_zoom": max_feature_density_by_zoom,
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
            # Second call: final MVT query → (mvt bytes, feature_count) row
            if len(captured_sql) == 1:
                return _async_return(True)
            return _async_return((b"mvt", 3))

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


# ---------------------------------------------------------------------------
# Scalable preseed: pre-transform feature cap + rank filter (params pushed
# into the shared query builder), and the new TilesConfig knobs.
# ---------------------------------------------------------------------------

async def _capture_builder_params(meta, z="2"):
    """Run get_features_as_mvt_filtered and return the params dict handed to
    ItemsProtocol.get_features_query (the shared query builder). The cap/rank
    reductions are applied there via ``limit`` / ``where``, so asserting on
    these params verifies they are pushed BEFORE ST_AsMVTGeom."""
    tiles_db._srid_exists.cache_clear()
    conn = AsyncMock()
    captured = []

    class _DQL:
        def __init__(self, sql, **kwargs):
            self._sql = sql

        def execute(self, conn, **params):
            captured.append(self._sql)

            async def _inner(*a, **k):
                return True if len(captured) == 1 else (b"mvt", 3)
            return _inner()

    with patch("dynastore.modules.tiles.tiles_db.DQLQuery", side_effect=_DQL):
        with patch("dynastore.tools.discovery.get_protocol") as mock_get_proto:
            from dynastore.models.protocols import ItemsProtocol

            mock_items = AsyncMock()
            mock_items.get_features_query = AsyncMock(return_value=("SELECT 1", {}))
            mock_get_proto.side_effect = (
                lambda proto: mock_items if proto is ItemsProtocol else None
            )
            await tiles_db.get_features_as_mvt_filtered(
                conn=conn,
                resolved_collections=[meta],
                tms_def=_tms_def_stub(z),
                target_srid=3857,
                z=z, x=0, y=0,
                extent=4096,
            )
            assert mock_items.get_features_query.await_count == 1
            return mock_items.get_features_query.await_args.kwargs["params"]


@pytest.mark.asyncio
async def test_feature_cap_pushes_limit_at_low_zoom():
    """max_features_per_tile_by_zoom becomes a builder LIMIT (pre-transform)."""
    meta = _make_collection_meta(max_features_per_tile_by_zoom={0: 20000, 8: 0})
    params = await _capture_builder_params(meta, z="2")  # z2 ≥ key 0 → 20000
    assert params.get("limit") == 20000


@pytest.mark.asyncio
async def test_feature_cap_absent_when_unset():
    """No cap configured → no LIMIT pushed (uncapped, existing behavior)."""
    meta = _make_collection_meta(max_features_per_tile_by_zoom=None)
    params = await _capture_builder_params(meta, z="2")
    assert params.get("limit") is None


@pytest.mark.asyncio
async def test_feature_cap_bracket_resolves_highest_key_le_zoom():
    """Highest zoom key ≤ current zoom wins, mirroring density/simplification."""
    meta = _make_collection_meta(max_features_per_tile_by_zoom={0: 10000, 4: 50000})
    params = await _capture_builder_params(meta, z="5")  # 5 ≥ 4 → 50000
    assert params.get("limit") == 50000


@pytest.mark.asyncio
async def test_rank_filter_pushes_pretransform_where():
    """feature_rank_column + min_feature_rank_by_zoom → indexed pre-transform WHERE."""
    meta = _make_collection_meta(
        feature_rank_column="length_m",
        min_feature_rank_by_zoom={0: 20000.0, 6: 0.0},
    )
    params = await _capture_builder_params(meta, z="2")  # z2 ≥ key 0 → 20000.0
    assert params.get("where") == '"length_m" >= :feat_rank_min'
    assert params.get("raw_params", {}).get("feat_rank_min") == 20000.0


@pytest.mark.asyncio
async def test_rank_filter_absent_without_column():
    """A min-rank map alone (no rank column) pushes no WHERE — needs the column."""
    meta = _make_collection_meta(
        feature_rank_column=None,
        min_feature_rank_by_zoom={0: 20000.0},
    )
    params = await _capture_builder_params(meta, z="2")
    assert params.get("where") is None


def test_max_features_config_default_is_bounded():
    """Every layer is bounded-cost out of the box (#3155): the cap ships a
    non-None default ladder, tight at world scale and looser as each tile
    covers less ground."""
    from dynastore.modules.tiles.tiles_config import TilesConfig
    assert TilesConfig().max_features_per_tile_by_zoom == {
        0: 20000,
        4: 50000,
        8: 200000,
    }


def test_max_features_config_overridable():
    from dynastore.modules.tiles.tiles_config import TilesConfig
    cfg = TilesConfig(max_features_per_tile_by_zoom={0: 20000, 8: 200000})
    assert cfg.max_features_per_tile_by_zoom == {0: 20000, 8: 200000}


def test_max_features_config_opt_out():
    """{0: 0} is the documented opt-out: 0 resolves for every zoom and a
    0-valued bracket pushes no LIMIT."""
    from dynastore.modules.tiles.tiles_config import TilesConfig
    cfg = TilesConfig(max_features_per_tile_by_zoom={0: 0})
    assert cfg.max_features_per_tile_by_zoom == {0: 0}


@pytest.mark.asyncio
async def test_feature_cap_zero_bracket_uncapped():
    """A 0 value in the resolved bracket disables the cap for that zoom and
    above — the per-zoom opt-out of the default ladder."""
    meta = _make_collection_meta(max_features_per_tile_by_zoom={0: 20000, 8: 0})
    params = await _capture_builder_params(meta, z="8")  # z8 ≥ key 8 → 0 → uncapped
    assert params.get("limit") is None


def test_feature_rank_config_defaults_are_none():
    from dynastore.modules.tiles.tiles_config import TilesConfig
    cfg = TilesConfig()
    assert cfg.feature_rank_column is None
    assert cfg.min_feature_rank_by_zoom is None


# ---------------------------------------------------------------------------
# Self-tuning per-tile byte budget (#3155 option B)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_bpf_estimates():
    """The bytes-per-feature estimator is module-global; isolate every test."""
    tiles_db._BPF_ESTIMATES.clear()
    yield
    tiles_db._BPF_ESTIMATES.clear()


def test_tile_byte_budget_default_is_one_mib():
    from dynastore.modules.tiles.tiles_config import TilesConfig
    assert TilesConfig().tile_byte_budget == 1_048_576


def test_tile_byte_budget_zero_disables():
    from dynastore.modules.tiles.tiles_config import TilesConfig
    assert TilesConfig(tile_byte_budget=0).tile_byte_budget == 0


def test_bpf_update_seeds_then_ewma():
    key = ("cat", "col", 2)
    tiles_db._bpf_update(key, tile_bytes=1000, features=10)  # seed: 100 B/feat
    assert tiles_db._bpf_get(key) == 100.0
    tiles_db._bpf_update(key, tile_bytes=2000, features=10)  # sample: 200
    assert tiles_db._bpf_get(key) == pytest.approx(150.0)  # alpha 0.5


def test_bpf_update_ignores_empty_measurements():
    key = ("cat", "col", 2)
    tiles_db._bpf_update(key, tile_bytes=0, features=10)
    tiles_db._bpf_update(key, tile_bytes=100, features=0)
    assert tiles_db._bpf_get(key) is None


def test_bpf_estimates_bounded_lru():
    for i in range(tiles_db._BPF_MAX_ENTRIES + 10):
        tiles_db._bpf_update(("cat", f"col{i}", 0), tile_bytes=100, features=1)
    assert len(tiles_db._BPF_ESTIMATES) == tiles_db._BPF_MAX_ENTRIES
    assert tiles_db._bpf_get(("cat", "col0", 0)) is None  # oldest evicted


@pytest.mark.asyncio
async def test_byte_budget_shrinks_limit_once_measured():
    """With a measured 100 B/feature and a 100 KB budget, the effective LIMIT
    drops from the 20000 bracket cap to 1000."""
    tiles_db._bpf_update(("cat", "col", 2), tile_bytes=100_000, features=1000)
    meta = _make_collection_meta(
        max_features_per_tile_by_zoom={0: 20000},
        tile_byte_budget=100_000,
    )
    params = await _capture_builder_params(meta, z="2")
    assert params.get("limit") == 1000


@pytest.mark.asyncio
async def test_byte_budget_without_measurement_uses_ladder():
    """Cold start: no estimate for the key → the bracket cap alone applies."""
    meta = _make_collection_meta(
        max_features_per_tile_by_zoom={0: 20000},
        tile_byte_budget=100_000,
    )
    params = await _capture_builder_params(meta, z="2")
    assert params.get("limit") == 20000


@pytest.mark.asyncio
async def test_byte_budget_never_raises_ladder_cap():
    """A generous budget must not lift the LIMIT above the bracket cap."""
    tiles_db._bpf_update(("cat", "col", 2), tile_bytes=100, features=100)  # 1 B/feat
    meta = _make_collection_meta(
        max_features_per_tile_by_zoom={0: 20000},
        tile_byte_budget=1_048_576,  # budget alone would allow ~1M rows
    )
    params = await _capture_builder_params(meta, z="2")
    assert params.get("limit") == 20000


@pytest.mark.asyncio
async def test_byte_budget_applies_when_ladder_opted_out():
    """{0: 0} opts out of the count ladder, but a measured byte budget still
    bounds the tile."""
    tiles_db._bpf_update(("cat", "col", 2), tile_bytes=100_000, features=1000)
    meta = _make_collection_meta(
        max_features_per_tile_by_zoom={0: 0},
        tile_byte_budget=100_000,
    )
    params = await _capture_builder_params(meta, z="2")
    assert params.get("limit") == 1000


@pytest.mark.asyncio
async def test_render_measures_bytes_per_feature():
    """A successful render seeds the estimator from (len(mvt), COUNT(*))."""
    meta = _make_collection_meta(tile_byte_budget=100_000)
    await _run_mvt_filtered(meta, z="2")  # harness row: (b"mvt", 3)
    assert tiles_db._bpf_get(("cat", "col", 2)) == pytest.approx(3 / 3)


@pytest.mark.asyncio
async def test_render_measurement_skipped_when_budget_disabled():
    meta = _make_collection_meta(tile_byte_budget=0)
    await _run_mvt_filtered(meta, z="2")
    assert tiles_db._bpf_get(("cat", "col", 2)) is None


@pytest.mark.asyncio
async def test_final_query_counts_features():
    """COUNT(*) must ride the ST_AsMVT aggregate for the estimator."""
    meta = _make_collection_meta()
    sql = await _run_mvt_filtered(meta, z="2")
    assert "COUNT(*)" in sql


def test_feature_rank_config_overridable():
    from dynastore.modules.tiles.tiles_config import TilesConfig
    cfg = TilesConfig(
        feature_rank_column="length_m",
        min_feature_rank_by_zoom={0: 20000.0, 6: 0.0},
    )
    assert cfg.feature_rank_column == "length_m"
    assert cfg.min_feature_rank_by_zoom == {0: 20000.0, 6: 0.0}


# ---------------------------------------------------------------------------
# TilesConfig: per-feature density CEILING (opt-in, default disabled) — the
# inverse of feature_rank_column/min_feature_rank_by_zoom above.
# ---------------------------------------------------------------------------


def test_feature_density_config_defaults_are_none():
    from dynastore.modules.tiles.tiles_config import TilesConfig
    cfg = TilesConfig()
    assert cfg.feature_density_column is None
    assert cfg.max_feature_density_by_zoom is None


def test_feature_density_config_overridable():
    from dynastore.modules.tiles.tiles_config import TilesConfig
    cfg = TilesConfig(
        feature_density_column="vertex_count",
        max_feature_density_by_zoom={0: 500.0, 6: 0.0},
    )
    assert cfg.feature_density_column == "vertex_count"
    assert cfg.max_feature_density_by_zoom == {0: 500.0, 6: 0.0}


def test_feature_density_config_opt_out():
    """{0: 0} is the documented opt-out: 0 resolves for every zoom and a
    0-valued bracket pushes no ceiling, mirroring max_features_per_tile_by_zoom."""
    from dynastore.modules.tiles.tiles_config import TilesConfig
    cfg = TilesConfig(max_feature_density_by_zoom={0: 0})
    assert cfg.max_feature_density_by_zoom == {0: 0}


# ---------------------------------------------------------------------------
# tiles_db: density ceiling pre-transform WHERE clause + composition with the
# rank filter.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_density_ceiling_pushes_pretransform_where():
    """feature_density_column + max_feature_density_by_zoom → indexed pre-transform WHERE."""
    meta = _make_collection_meta(
        feature_density_column="vertex_count",
        max_feature_density_by_zoom={0: 500.0, 6: 0.0},
    )
    params = await _capture_builder_params(meta, z="2")  # z2 ≥ key 0 → 500.0
    assert params.get("where") == '"vertex_count" <= :feat_density_max'
    assert params.get("raw_params", {}).get("feat_density_max") == 500.0


@pytest.mark.asyncio
async def test_density_ceiling_absent_without_column():
    """A max-density map alone (no density column) pushes no WHERE — needs the column."""
    meta = _make_collection_meta(
        feature_density_column=None,
        max_feature_density_by_zoom={0: 500.0},
    )
    params = await _capture_builder_params(meta, z="2")
    assert params.get("where") is None


@pytest.mark.asyncio
async def test_density_ceiling_zero_bracket_disables():
    """A 0 value in the resolved bracket disables the ceiling for that zoom
    and above — the per-zoom opt-out, mirroring max_features_per_tile_by_zoom."""
    meta = _make_collection_meta(
        feature_density_column="vertex_count",
        max_feature_density_by_zoom={0: 500.0, 6: 0},
    )
    params = await _capture_builder_params(meta, z="6")  # z6 ≥ key 6 → 0 → disabled
    assert params.get("where") is None


@pytest.mark.asyncio
async def test_rank_and_density_filters_compose():
    """Rank floor and density ceiling are independent and AND-combined into a
    single pre-transform WHERE, with both bind params present."""
    meta = _make_collection_meta(
        feature_rank_column="length_m",
        min_feature_rank_by_zoom={0: 20000.0},
        feature_density_column="vertex_count",
        max_feature_density_by_zoom={0: 500.0},
    )
    params = await _capture_builder_params(meta, z="2")
    assert params.get("where") == (
        '"length_m" >= :feat_rank_min AND "vertex_count" <= :feat_density_max'
    )
    raw_params = params.get("raw_params", {})
    assert raw_params.get("feat_rank_min") == 20000.0
    assert raw_params.get("feat_density_max") == 500.0


@pytest.mark.asyncio
async def test_no_rank_or_density_filters_pushes_no_where():
    """With neither filter configured, no where/raw_params keys reach the
    builder params — the pre-transform query is unchanged from before either
    filter existed."""
    meta = _make_collection_meta()
    params = await _capture_builder_params(meta, z="2")
    assert "where" not in params
    assert "raw_params" not in params
