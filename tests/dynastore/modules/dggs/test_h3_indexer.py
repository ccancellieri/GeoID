"""Unit tests for modules.dggs.h3_indexer.

These tests require the h3 package (pip install h3>=4.1.0).
They are skipped automatically when h3 is not installed.
"""

import pytest

h3 = pytest.importorskip("h3", reason="h3 package not installed")


from dynastore.modules.dggs.h3_indexer import (
    bbox_to_cells,
    cell_to_center,
    cell_to_geojson_polygon,
    get_resolution,
    is_valid_cell,
    latlng_to_cell,
)


# FAO HQ in Rome (WGS-84)
FAO_LAT = 41.8823
FAO_LNG = 12.4824


def test_latlng_to_cell_returns_valid_index():
    cell = latlng_to_cell(FAO_LAT, FAO_LNG, 5)
    assert isinstance(cell, str)
    assert is_valid_cell(cell)


def test_latlng_to_cell_resolution_range():
    for res in (0, 5, 10, 15):
        cell = latlng_to_cell(FAO_LAT, FAO_LNG, res)
        assert get_resolution(cell) == res


def test_latlng_to_cell_invalid_resolution():
    with pytest.raises(ValueError, match="resolution"):
        latlng_to_cell(FAO_LAT, FAO_LNG, 16)
    with pytest.raises(ValueError, match="resolution"):
        latlng_to_cell(FAO_LAT, FAO_LNG, -1)


def test_cell_to_geojson_polygon_structure():
    cell = latlng_to_cell(FAO_LAT, FAO_LNG, 5)
    polygon = cell_to_geojson_polygon(cell)
    assert polygon["type"] == "Polygon"
    coords = polygon["coordinates"]
    assert isinstance(coords, list) and len(coords) == 1
    ring = coords[0]
    # H3 hexagon has 6 vertices + closing vertex = 7 points
    assert len(ring) == 7
    # Closed ring: first == last
    assert ring[0] == ring[-1]
    # GeoJSON order: [lng, lat]
    for point in ring:
        lng, lat = point
        assert -180 <= lng <= 180
        assert -90 <= lat <= 90


def test_cell_to_center_roundtrip():
    cell = latlng_to_cell(FAO_LAT, FAO_LNG, 7)
    lat, lng = cell_to_center(cell)
    # Centre should re-index to the same cell at the same resolution
    reconstructed = latlng_to_cell(lat, lng, 7)
    assert reconstructed == cell


def test_is_valid_cell():
    cell = latlng_to_cell(0.0, 0.0, 5)
    assert is_valid_cell(cell)
    assert not is_valid_cell("not-an-h3-cell")
    assert not is_valid_cell("")


def test_bbox_to_cells_returns_nonempty_set():
    # Rough bbox around Rome
    cells = bbox_to_cells(xmin=12.0, ymin=41.5, xmax=13.0, ymax=42.5, resolution=5)
    assert isinstance(cells, set)
    assert len(cells) > 0
    for c in cells:
        assert is_valid_cell(c)
        assert get_resolution(c) == 5


def test_bbox_to_cells_coarser_resolution_fewer_cells():
    cells_fine = bbox_to_cells(0, 0, 1, 1, resolution=6)
    cells_coarse = bbox_to_cells(0, 0, 1, 1, resolution=4)
    # Coarser resolution → fewer cells
    assert len(cells_coarse) < len(cells_fine)




# ---------------------------------------------------------------------------
# Sidecar int↔str conversion
# ---------------------------------------------------------------------------

def test_cell_str_to_int_roundtrip():
    from dynastore.modules.dggs.h3_indexer import cell_str_to_int, cell_int_to_str
    cell = latlng_to_cell(FAO_LAT, FAO_LNG, 5)
    int_val = cell_str_to_int(cell)
    assert isinstance(int_val, int)
    assert int_val > 0
    assert cell_int_to_str(int_val) == cell


def test_cell_str_to_int_invalid():
    from dynastore.modules.dggs.h3_indexer import cell_str_to_int
    with pytest.raises(ValueError):
        cell_str_to_int("not-valid")
