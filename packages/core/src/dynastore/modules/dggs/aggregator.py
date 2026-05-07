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

"""Aggregate PostGIS features into DGGS cells (H3 or S2) on-the-fly."""

from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

from dynastore.modules.dggs import h3_indexer, s2_indexer
from dynastore.modules.dggs.models import DGGSFeature, DGGSFeatureCollection, ZoneProperties


def _sidecar_key(dggs_id: str, resolution: int) -> str:
    """Return the geometry sidecar column name for the given DGGRS and resolution."""
    prefix = "h3" if dggs_id.upper() == "H3" else "s2"
    return f"{prefix}_res{resolution}"


def _cell_int_to_str(dggs_id: str, val: int) -> str:
    """Convert a BIGINT cell value from the sidecar to a canonical cell string."""
    if dggs_id.upper() == "H3":
        return h3_indexer.cell_int_to_str(val)
    return s2_indexer.cell_int_to_str(val)


def _latlng_to_cell(dggs_id: str, lat: float, lng: float, resolution: int) -> str:
    """Convert a centroid to a cell ID using the appropriate DGGRS indexer."""
    if dggs_id.upper() == "H3":
        return h3_indexer.latlng_to_cell(lat, lng, resolution)
    return s2_indexer.latlng_to_cell(lat, lng, resolution)


def _cell_to_geojson_polygon(dggs_id: str, cell: str) -> Dict[str, Any]:
    """Return the GeoJSON Polygon boundary for a cell."""
    if dggs_id.upper() == "H3":
        return h3_indexer.cell_to_geojson_polygon(cell)
    return s2_indexer.cell_to_geojson_polygon(cell)


def _cell_from_stored_index(feature: Any, resolution: int, dggs_id: str) -> Optional[str]:
    """Read a pre-computed cell ID from the geometry sidecar column ``{prefix}_res{resolution}``.

    The geometry sidecar stores H3 indices as BIGINT (``int(cell_hex, 16)``) and S2 indices
    as BIGINT (``CellId.id()``).  This function checks ``feature.properties`` for the
    appropriate sidecar key and converts the integer back to the canonical cell string.

    Returns None if the key is absent or the value is not a valid integer,
    triggering the fallback centroid-computation path.
    """
    key = _sidecar_key(dggs_id, resolution)
    props = _get_properties(feature)
    val = props.get(key)
    if val is None:
        return None
    try:
        return _cell_int_to_str(dggs_id, int(val))
    except (TypeError, ValueError):
        return None


def _extract_centroid(feature: Any) -> Optional[tuple]:
    """Extract (lat, lng) from a GeoJSON feature.

    Supports Point geometry and features that expose centroid via properties.
    Returns None if the centroid cannot be determined.
    """
    geom = None
    if hasattr(feature, "geometry"):
        geom = feature.geometry
    elif isinstance(feature, dict):
        geom = feature.get("geometry")

    if not geom:
        return None

    # Normalise Pydantic geometry to a plain dict
    if hasattr(geom, "model_dump"):
        geom = geom.model_dump(by_alias=True, exclude_none=True)
    elif hasattr(geom, "dict"):
        geom = geom.dict(by_alias=True, exclude_none=True)
    elif not isinstance(geom, dict):
        return None

    geom_type = geom.get("type")
    coords = geom.get("coordinates")

    if geom_type == "Point" and coords and len(coords) >= 2:
        # GeoJSON Point: [lng, lat]
        return coords[1], coords[0]

    if geom_type == "Polygon" and coords:
        ring = coords[0]
        if ring:
            lngs = [v[0] for v in ring]
            lats = [v[1] for v in ring]
            return sum(lats) / len(lats), sum(lngs) / len(lngs)

    if geom_type == "MultiPolygon" and coords:
        all_coords = [v for ring in coords[0] for v in ring]
        if all_coords:
            lngs = [v[0] for v in all_coords]
            lats = [v[1] for v in all_coords]
            return sum(lats) / len(lats), sum(lngs) / len(lngs)

    return None


def _get_properties(feature: Any) -> Dict[str, Any]:
    """Extract properties dict from a GeoJSON feature."""
    if hasattr(feature, "properties"):
        props = feature.properties
        if props is None:
            return {}
        if hasattr(props, "model_dump"):
            return props.model_dump(by_alias=True, exclude_none=True) or {}
        return dict(props) if props else {}
    if isinstance(feature, dict):
        return dict(feature.get("properties") or {})
    return {}


def aggregate_features(
    features: List[Any],
    resolution: int,
    parameter_names: Optional[Set[str]] = None,
    dggs_id: str = "H3",
) -> DGGSFeatureCollection:
    """Aggregate a list of GeoJSON features into DGGS cells at *resolution*.

    For each feature:
    1. Check for a pre-computed cell ID in the geometry sidecar (fast path)
    2. Fall back to extracting centroid and computing the cell on-the-fly
    3. Count features per cell and compute per-property numeric averages

    Args:
        features: List of GeoJSON feature objects (Pydantic models or dicts).
        resolution: DGGRS resolution/level (H3: 0-15, S2: 0-30).
        parameter_names: If set, only include these property names in aggregation.
        dggs_id: DGGRS identifier — ``"H3"`` or ``"S2"``.

    Returns:
        DGGSFeatureCollection with one feature per occupied cell.
    """
    cell_counts: Dict[str, int] = defaultdict(int)
    cell_sums: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    cell_numeric_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for feature in features:
        # Fast path: read pre-computed cell index from the geometry sidecar column.
        cell = _cell_from_stored_index(feature, resolution, dggs_id)
        if cell is None:
            # Slow path: extract centroid and compute cell on-the-fly.
            centroid = _extract_centroid(feature)
            if centroid is None:
                continue
            lat, lng = centroid
            try:
                cell = _latlng_to_cell(dggs_id, lat, lng, resolution)
            except Exception:
                continue

        cell_counts[cell] += 1
        props = _get_properties(feature)
        for key, value in props.items():
            if parameter_names and key not in parameter_names:
                continue
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                cell_sums[cell][key] += float(value)
                cell_numeric_counts[cell][key] += 1

    dggs_features: List[DGGSFeature] = []
    for cell, count in cell_counts.items():
        avgs: Dict[str, Any] = {}
        for key, total in cell_sums[cell].items():
            n = cell_numeric_counts[cell][key]
            avgs[key] = round(total / n, 6) if n > 0 else None

        dggs_features.append(
            DGGSFeature(
                id=cell,
                geometry=_cell_to_geojson_polygon(dggs_id, cell),
                properties=ZoneProperties(
                    **{"zone-id": cell, "resolution": resolution, "count": count, "values": avgs}
                ),
            )
        )

    return DGGSFeatureCollection(
        features=dggs_features,
        numberMatched=len(dggs_features),
        numberReturned=len(dggs_features),
        dggsId=dggs_id,
        zoneLevel=resolution,
    )
