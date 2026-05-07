#    Copyright 2025 FAO
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

"""H3 indexing utilities: coordinate → cell, cell → GeoJSON geometry."""

from typing import Any, Dict, List, Optional, Set, Tuple

H3_MIN_RESOLUTION = 0
H3_MAX_RESOLUTION = 15


def _require_h3():
    try:
        import h3
        return h3
    except ImportError:
        raise ImportError(
            "The 'h3' package is required for DGGS support. "
            "Install it with: pip install 'dynastore[extension_dggs]'"
        )


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


def parse_bbox(bbox_str: str) -> Optional[Tuple[float, float, float, float]]:
    """Parse a comma-separated bbox string into (xmin, ymin, xmax, ymax).

    Returns None if the string is empty or None.
    Raises ValueError on malformed input.
    """
    if not bbox_str:
        return None
    parts = [p.strip() for p in bbox_str.split(",")]
    if len(parts) != 4:
        raise ValueError(
            f"bbox must have exactly 4 comma-separated values (xmin,ymin,xmax,ymax), got {len(parts)}"
        )
    try:
        xmin, ymin, xmax, ymax = (float(p) for p in parts)
    except ValueError:
        raise ValueError(f"bbox values must be numeric, got: {bbox_str!r}")
    if xmin >= xmax or ymin >= ymax:
        raise ValueError(
            f"bbox is degenerate: xmin={xmin} >= xmax={xmax} or ymin={ymin} >= ymax={ymax}"
        )
    return xmin, ymin, xmax, ymax
