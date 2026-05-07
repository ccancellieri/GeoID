"""EDR GeoJSON output writer for point (position) queries."""

from __future__ import annotations

import json
from typing import Any, Dict, Iterator, List, Optional


def write_position_geojson(
    lon: float,
    lat: float,
    datetime_val: Optional[str],
    band_names: List[str],
    values: Dict[int, Optional[float]],
) -> Iterator[bytes]:
    """Emit a GeoJSON FeatureCollection with a single point feature."""
    properties: Dict[str, Any] = {}
    if datetime_val:
        properties["datetime"] = datetime_val
    for idx, name in enumerate(band_names, start=1):
        properties[name] = values.get(idx)

    feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": properties,
    }
    doc = {"type": "FeatureCollection", "features": [feature]}
    yield json.dumps(doc).encode("utf-8")
