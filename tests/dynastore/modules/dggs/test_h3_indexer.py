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
    rect_bound_for_cell,
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


# ---------------------------------------------------------------------------
# Antimeridian handling
# ---------------------------------------------------------------------------

def test_rect_bound_for_cell_regular():
    """Test rect_bound_for_cell for a cell not crossing the antimeridian."""
    cell = latlng_to_cell(FAO_LAT, FAO_LNG, 5)
    xmin, ymin, xmax, ymax = rect_bound_for_cell(cell)
    
    assert -180 <= xmin <= 180
    assert -180 <= xmax <= 180
    assert -90 <= ymin <= 90
    assert -90 <= ymax <= 90
    assert xmin <= xmax
    assert ymin <= ymax
    
    assert xmin <= FAO_LNG <= xmax
    assert ymin <= FAO_LAT <= ymax


def test_rect_bound_for_cell_antimeridian():
    """Test that cells near the antimeridian are handled correctly."""
    cell_west = latlng_to_cell(0, 179.5, 5)
    xmin_w, ymin_w, xmax_w, ymax_w = rect_bound_for_cell(cell_west)
    
    assert xmin_w <= xmax_w, f"Invalid bbox: xmin={xmin_w} > xmax={xmax_w}"
    
    cell_east = latlng_to_cell(0, -179.5, 5)
    xmin_e, ymin_e, xmax_e, ymax_e = rect_bound_for_cell(cell_east)
    
    assert xmin_e <= xmax_e, f"Invalid bbox: xmin={xmin_e} > xmax={xmax_e}"


def test_rect_bound_all_cells_valid_bounds():
    """Spot-check that rect_bound_for_cell never returns xmin > xmax."""
    test_points = [
        (0, 0), (0, 180), (0, -180), (45, 170), (-45, -170),
        (0, 179.9), (0, -179.9), (30, 179), (-30, -179),
    ]
    for lat, lng in test_points:
        lat = max(-89.9, min(89.9, lat))
        cell = latlng_to_cell(lat, lng, 5)
        xmin, ymin, xmax, ymax = rect_bound_for_cell(cell)
        assert xmin <= xmax, f"Invalid bbox for cell {cell}: xmin={xmin} xmax={xmax}"
        assert ymin <= ymax, f"Invalid bbox for cell {cell}: ymin={ymin} ymax={ymax}"


def test_rect_bound_for_cell_invalid():
    """Test that rect_bound_for_cell raises for invalid cells."""
    with pytest.raises(ValueError, match="Invalid H3 cell"):
        rect_bound_for_cell("invalid-cell")
