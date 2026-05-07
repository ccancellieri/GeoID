"""BigQuery row → Feature adapter and paged streamer."""

from __future__ import annotations

import re
from typing import Any, AsyncIterator, Dict, Optional

from dynastore.models.ogc import Feature


def row_to_feature(
    row: Dict[str, Any],
    *,
    id_column: str,
    geometry_column: Optional[str],
) -> Feature:
    if id_column not in row:
        raise KeyError(f"id column {id_column!r} not in row keys {list(row)}")
    feat_id = row[id_column]
    geometry: Optional[Dict[str, Any]] = None
    if geometry_column and geometry_column in row:
        raw = row[geometry_column]
        geometry = _coerce_geometry(raw)
    properties = {
        k: v
        for k, v in row.items()
        if k != id_column and k != geometry_column
    }
    return Feature(
        type="Feature",
        id=feat_id,
        geometry=geometry,
        properties=properties,
    )


def _coerce_geometry(raw: Any) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    if isinstance(raw, dict) and "type" in raw and "coordinates" in raw:
        return raw
    if isinstance(raw, str):
        return _wkt_to_geojson(raw)
    return None


def _wkt_to_geojson(wkt: str) -> Dict[str, Any]:
    """Minimal WKT → GeoJSON for POINT / LINESTRING / POLYGON."""
    wkt = wkt.strip()
    m = re.match(r"^POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)$", wkt, re.I)
    if m:
        return {
            "type": "Point",
            "coordinates": [float(m.group(1)), float(m.group(2))],
        }
    m = re.match(r"^LINESTRING\s*\((.+)\)$", wkt, re.I)
    if m:
        coords = [
            [float(p) for p in pair.strip().split()]
            for pair in m.group(1).split(",")
        ]
        return {"type": "LineString", "coordinates": coords}
    m = re.match(r"^POLYGON\s*\(\s*\((.+)\)\s*\)$", wkt, re.I)
    if m:
        ring = [
            [float(p) for p in pair.strip().split()]
            for pair in m.group(1).split(",")
        ]
        return {"type": "Polygon", "coordinates": [ring]}
    raise ValueError(f"Unsupported WKT geometry: {wkt!r}")


async def paged_feature_stream(
    execute_query,  # async (query: str, project_id: str) -> List[Dict[str, Any]]
    *,
    project_id: str,
    base_query: str,
    id_column: str,
    geometry_column: Optional[str],
    page_size: int = 1000,
    max_items: int = 100_000,
) -> AsyncIterator[Feature]:
    """Stream Features from BQ by paging LIMIT/OFFSET over ``base_query``."""
    emitted = 0
    offset = 0
    while emitted < max_items:
        page_limit = min(page_size, max_items - emitted)
        query = f"{base_query.rstrip().rstrip(';')} LIMIT {page_limit} OFFSET {offset}"
        rows = await execute_query(query, project_id)
        if not rows:
            return
        for row in rows:
            yield row_to_feature(
                row, id_column=id_column, geometry_column=geometry_column,
            )
            emitted += 1
            if emitted >= max_items:
                return
        if len(rows) < page_limit:
            return
        offset += page_limit
