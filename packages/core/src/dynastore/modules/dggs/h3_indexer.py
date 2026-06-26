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

"""H3 indexing utilities: coordinate → cell, cell → GeoJSON geometry."""

from typing import Any, Dict, List, Set, Tuple

H3_MIN_RESOLUTION = 0
H3_MAX_RESOLUTION = 15


def _require_h3():
    try:
        import h3
        return h3
    except ImportError as e:
        raise ImportError(
            "The 'h3' package is required for DGGS support. "
            "Install it with: pip install 'dynastore[extension_dggs]'"
        ) from e


def latlng_to_cell(lat: float, lng: float, resolution: int) -> str:
    """Convert a WGS-84 coordinate to an H3 cell index."""
    h3 = _require_h3()
    if not (H3_MIN_RESOLUTION <= resolution <= H3_MAX_RESOLUTION):
        raise ValueError(
            f"H3 resolution must be between {H3_MIN_RESOLUTION} and {H3_MAX_RESOLUTION}, got {resolution}"
        )
    return h3.latlng_to_cell(lat, lng, resolution)


def cell_to_geojson_polygon(cell: str) -> Dict[str, Any]:
    """Return a GeoJSON Polygon dict for the H3 cell boundary.

    H3 boundary vertices are (lat, lng); GeoJSON requires [lng, lat].
    The polygon is closed (first == last vertex).
    """
    h3 = _require_h3()
    boundary: List[Tuple[float, float]] = h3.cell_to_boundary(cell)
    coords = [[lng, lat] for lat, lng in boundary]
    coords.append(coords[0])
    return {"type": "Polygon", "coordinates": [coords]}


def cell_to_center(cell: str) -> Tuple[float, float]:
    """Return (lat, lng) of the H3 cell centre."""
    h3 = _require_h3()
    return h3.cell_to_latlng(cell)


def get_resolution(cell: str) -> int:
    """Return the resolution of an H3 cell."""
    h3 = _require_h3()
    return h3.get_resolution(cell)


def is_valid_cell(cell: str) -> bool:
    """Return True if *cell* is a valid H3 index."""
    h3 = _require_h3()
    return h3.is_valid_cell(cell)


def bbox_to_cells(
    xmin: float,
    ymin: float,
    xmax: float,
    ymax: float,
    resolution: int,
) -> Set[str]:
    """Return the set of H3 cells that cover the given WGS-84 bounding box."""
    h3 = _require_h3()
    # Build a GeoJSON-like polygon (coordinates in [lng, lat] order for h3)
    geo_polygon = {
        "type": "Polygon",
        "coordinates": [[
            [xmin, ymin],
            [xmax, ymin],
            [xmax, ymax],
            [xmin, ymax],
            [xmin, ymin],
        ]],
    }
    return set(h3.geo_to_cells(geo_polygon, resolution))


def cell_str_to_int(cell: str) -> int:
    """Convert an H3 hex-string cell ID to the BIGINT stored in the geometries sidecar.

    The geometries sidecar stores H3 indices as ``BIGINT`` (``int(cell, 16)``).
    Use this when building ``FilterCondition(EQ, h3_res{N}, cell_str_to_int(zone_id))``.
    """
    if not is_valid_cell(cell):
        raise ValueError(f"Not a valid H3 cell: {cell!r}")
    return int(cell, 16)


def cell_int_to_str(val: int) -> str:
    """Convert a BIGINT H3 value (as stored in the geometries sidecar) to a hex string cell ID.

    Reverse of :func:`cell_str_to_int`.  Useful when reading ``h3_res{N}`` columns back
    from the DB and needing the canonical cell ID string.
    """
    return format(val, "x")


def rect_bound_for_cell(cell: str) -> Tuple[float, float, float, float]:
    """Return (xmin, ymin, xmax, ymax) bounding box for an H3 cell.

    Correctly handles cells that cross the antimeridian by detecting when
    longitude values span both sides of ±180° and returning the full
    longitude range [-180, 180] in that case.

    Args:
        cell: H3 cell ID string.

    Returns:
        Tuple of (xmin, ymin, xmax, ymax) in WGS-84 coordinates.
    """
    if not is_valid_cell(cell):
        raise ValueError(f"Invalid H3 cell: {cell!r}")
    
    polygon = cell_to_geojson_polygon(cell)
    coords = polygon["coordinates"][0]
    lngs = [c[0] for c in coords[:-1]]
    lats = [c[1] for c in coords[:-1]]
    
    ymin, ymax = min(lats), max(lats)
    
    if _crosses_antimeridian(lngs):
        return -180.0, ymin, 180.0, ymax
    
    return min(lngs), ymin, max(lngs), ymax


def _crosses_antimeridian(lngs: List[float]) -> bool:
    """Detect if a list of longitudes crosses the antimeridian.

    A cell crosses the antimeridian if:
    1. It has vertices on both sides of ±180°
    2. The span would be > 180° if computed naively

    Args:
        lngs: List of longitude values (not including closing vertex).

    Returns:
        True if the cell crosses the antimeridian.
    """
    if not lngs:
        return False
    
    min_lng, max_lng = min(lngs), max(lngs)
    
    if max_lng - min_lng > 180:
        return True
    
    has_positive = any(lng > 0 for lng in lngs)
    has_negative = any(lng < 0 for lng in lngs)
    
    if not (has_positive and has_negative):
        return False
    
    if min_lng >= -180 and max_lng <= 180:
        near_positive = any(lng > 90 for lng in lngs)
        near_negative = any(lng < -90 for lng in lngs)
        return near_positive and near_negative
    
    return False
