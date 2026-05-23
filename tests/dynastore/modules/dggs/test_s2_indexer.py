"""Unit tests for modules.dggs.s2_indexer.

These tests require the s2sphere package (pip install s2sphere>=0.2.5).
They are skipped automatically when s2sphere is not installed.
"""

import pytest

s2sphere = pytest.importorskip("s2sphere", reason="s2sphere package not installed")


from dynastore.modules.dggs.s2_indexer import (
    S2_MAX_LEVEL,
    S2_MIN_LEVEL,
    bbox_to_cells,
    cell_int_to_str,
    cell_str_to_int,
    cell_to_center,
    cell_to_geojson_polygon,
    get_level,
    is_valid_cell,
    latlng_to_cell,
)


# FAO HQ in Rome (WGS-84)
FAO_LAT = 41.8823
FAO_LNG = 12.4824


def test_latlng_to_cell_returns_valid_token():
    token = latlng_to_cell(FAO_LAT, FAO_LNG, 10)
    assert isinstance(token, str)
    assert is_valid_cell(token)


def test_latlng_to_cell_level_roundtrip():
    for level in (0, 5, 10, 20, 30):
        token = latlng_to_cell(FAO_LAT, FAO_LNG, level)
        assert get_level(token) == level


def test_latlng_to_cell_invalid_level():
    with pytest.raises(ValueError, match="level"):
        latlng_to_cell(FAO_LAT, FAO_LNG, 31)
    with pytest.raises(ValueError, match="level"):
        latlng_to_cell(FAO_LAT, FAO_LNG, -1)


def test_cell_to_geojson_polygon_structure():
    token = latlng_to_cell(FAO_LAT, FAO_LNG, 10)
    polygon = cell_to_geojson_polygon(token)
    assert polygon["type"] == "Polygon"
    coords = polygon["coordinates"]
    assert isinstance(coords, list) and len(coords) == 1
    ring = coords[0]
    # S2 cell has 4 vertices + closing vertex = 5 points
    assert len(ring) == 5
    # Closed ring: first == last
    assert ring[0] == ring[-1]
    # GeoJSON order: [lng, lat]
    for point in ring:
        lng, lat = point
        assert -180 <= lng <= 180
        assert -90 <= lat <= 90


def test_cell_to_center_roundtrip():
    token = latlng_to_cell(FAO_LAT, FAO_LNG, 12)
    lat, lng = cell_to_center(token)
    reconstructed = latlng_to_cell(lat, lng, 12)
    assert reconstructed == token


def test_is_valid_cell():
    token = latlng_to_cell(0.0, 0.0, 5)
    assert is_valid_cell(token)
    assert not is_valid_cell("not-an-s2-token")
    assert not is_valid_cell("")


def test_bbox_to_cells_returns_nonempty_set():
    # Rough bbox around Rome
    cells = bbox_to_cells(xmin=12.0, ymin=41.5, xmax=13.0, ymax=42.5, level=10)
    assert isinstance(cells, set)
    assert len(cells) > 0
    for c in cells:
        assert is_valid_cell(c)
        assert get_level(c) == 10


def test_bbox_to_cells_coarser_level_fewer_cells():
    cells_fine = bbox_to_cells(0, 0, 1, 1, level=10)
    cells_coarse = bbox_to_cells(0, 0, 1, 1, level=7)
    assert len(cells_coarse) < len(cells_fine)




# ---------------------------------------------------------------------------
# int ↔ str conversion
# ---------------------------------------------------------------------------

def test_cell_str_to_int_roundtrip():
    token = latlng_to_cell(FAO_LAT, FAO_LNG, 10)
    int_val = cell_str_to_int(token)
    assert isinstance(int_val, int)
    assert int_val != 0
    assert cell_int_to_str(int_val) == token


def test_cell_str_to_int_invalid():
    with pytest.raises(ValueError):
        cell_str_to_int("not-valid-s2")


def test_constants():
    assert S2_MIN_LEVEL == 0
    assert S2_MAX_LEVEL == 30


# ---------------------------------------------------------------------------
# rect_bound_for_cell
# ---------------------------------------------------------------------------

def test_rect_bound_for_cell_normal():
    from dynastore.modules.dggs.s2_indexer import rect_bound_for_cell
    token = latlng_to_cell(FAO_LAT, FAO_LNG, 10)
    xmin, ymin, xmax, ymax = rect_bound_for_cell(token)
    assert xmin < xmax
    assert ymin < ymax
    assert -180 <= xmin <= 180
    assert -180 <= xmax <= 180
    assert -90 <= ymin <= 90
    assert -90 <= ymax <= 90
    # FAO cell must contain FAO coordinates
    assert xmin <= FAO_LNG <= xmax
    assert ymin <= FAO_LAT <= ymax


def test_rect_bound_all_cells_valid_bounds():
    """Spot-check that rect_bound_for_cell never returns xmin > xmax after antimeridian clamp."""
    from dynastore.modules.dggs.s2_indexer import rect_bound_for_cell
    test_points = [(0, 0), (0, 180), (0, -180), (90, 0), (-90, 0), (45, 170), (-45, -170)]
    for lat, lng in test_points:
        # Clamp lat to valid range
        lat = max(-89.9, min(89.9, lat))
        token = latlng_to_cell(lat, lng, 5)
        xmin, ymin, xmax, ymax = rect_bound_for_cell(token)
        assert xmin <= xmax, f"Invalid bbox for token {token}: xmin={xmin} xmax={xmax}"
