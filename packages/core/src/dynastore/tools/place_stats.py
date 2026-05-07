"""
JSON-FG Place Statistics Computation Tool.

Computes statistics for JSON-FG 'place' geometries (Solid, Prism, 3D Curves)
that are impossible or inaccurate in standard WGS84 / 2D GeoJSON.

Supported geometry types:
 - Solid: 3D closed mesh (supports volume, surface area, centroid_3d)
 - Prism: Extruded polygon (supports volume, surface area, net_floor_area)
 - MultiSolid / MultiPrism: Collections of the above
 - 3D linestrings: vertical_gradient, z_range
 - Any geometry with Z: z_range, centroid_3d

Reference: OGC Features and Geometries JSON (JSON-FG) Part 1
"""

import logging
import math
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def compute_place_statistics(
    place_dict: Dict[str, Any],
    config: Any,  # PlaceStatisticsConfig
    validity: Any = None,  # TSTZRANGE or dict with start/end
) -> Dict[str, Any]:
    """
    Compute configured statistics for a JSON-FG 'place' geometry.

    Args:
        place_dict: The JSON-FG place geometry dict (e.g. {"type": "Prism", ...})
        config: PlaceStatisticsConfig with enabled flags per stat
        validity: Optional validity interval for temporal_duration computation

    Returns:
        Dictionary of computed statistics. Keys match PlaceStatisticsConfig fields.
        Empty dict if no stats could be derived (e.g. 2D-only geometry).
    """
    if not place_dict:
        return {}

    stats: Dict[str, Any] = {}
    geom_type = place_dict.get("type", "")

    try:
        if geom_type == "Prism":
            _compute_prism_stats(place_dict, config, stats)
        elif geom_type == "Solid":
            _compute_solid_stats(place_dict, config, stats)
        elif geom_type == "MultiPrism":
            _compute_multi_prism_stats(place_dict, config, stats)
        elif geom_type == "MultiSolid":
            _compute_multi_solid_stats(place_dict, config, stats)
        elif geom_type in ("LineString", "MultiLineString") and _has_z(place_dict):
            _compute_3d_curve_stats(place_dict, config, stats)
        elif geom_type == "Point" and _has_z(place_dict):
            _compute_point3d_stats(place_dict, config, stats)
        else:
            # GeoJSON-compatible 3D geometry — try z_range at minimum
            if config.z_range.enabled:
                zs = _collect_z_coords(place_dict)
                if zs:
                    stats["z_range"] = float(max(zs) - min(zs))

    except Exception as e:
        logger.warning(f"place_stats: Failed to compute stats for type={geom_type}: {e}")

    # Temporal duration (regardless of geom type)
    if config.temporal_duration.enabled and validity:
        duration = _compute_temporal_duration(validity)
        if duration is not None:
            stats["temporal_duration"] = duration

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# PRISM SUPPORT
# ─────────────────────────────────────────────────────────────────────────────

def _compute_prism_stats(place: Dict, cfg: Any, stats: Dict) -> None:
    """
    Prism: extruded polygon between two Z levels.
    JSON-FG: {"type":"Prism","base":{...polygon...},"lower":z1,"upper":z2}
    """
    from shapely.geometry import shape

    base = place.get("base")
    lower = place.get("lower")
    upper = place.get("upper")

    if not base:
        return

    try:
        shapely_base = shape(base)
        base_area = float(shapely_base.area)
        perimeter = float(shapely_base.length)
    except Exception as e:
        logger.debug(f"Prism base shape error: {e}")
        return

    height = None
    if lower is not None and upper is not None:
        try:
            height = float(upper) - float(lower)
        except (TypeError, ValueError):
            pass

    if cfg.volume.enabled and height is not None and height > 0:
        stats["volume"] = base_area * height

    if cfg.surface_area.enabled and height is not None:
        lateral = perimeter * abs(height)
        stats["surface_area"] = 2 * base_area + lateral

    if cfg.surface_to_volume_ratio.enabled and "volume" in stats and stats["volume"] > 0 and "surface_area" in stats:
        stats["surface_to_volume_ratio"] = stats["surface_area"] / stats["volume"]

    if cfg.net_floor_area.enabled:
        # For Prism: net floor area = base polygon area
        stats["net_floor_area"] = base_area

    if cfg.centroid_3d:
        c = shapely_base.centroid
        mid_z = ((float(lower) + float(upper)) / 2) if lower is not None and upper is not None else 0.0
        stats["centroid_3d"] = [float(c.x), float(c.y), mid_z]

    if cfg.z_range.enabled and lower is not None and upper is not None:
        stats["z_range"] = abs(float(upper) - float(lower))


def _compute_multi_prism_stats(place: Dict, cfg: Any, stats: Dict) -> None:
    """MultiPrism: {"type":"MultiPrism","prisms":[...]}"""
    sub_stats_list = []
    for prism in place.get("prisms", []):
        sub = {}
        _compute_prism_stats(prism, cfg, sub)
        if sub:
            sub_stats_list.append(sub)

    _aggregate_stats(sub_stats_list, cfg, stats)


# ─────────────────────────────────────────────────────────────────────────────
# SOLID SUPPORT
# ─────────────────────────────────────────────────────────────────────────────

def _compute_solid_stats(place: Dict, cfg: Any, stats: Dict) -> None:
    """
    Solid: 3D closed shell mesh.
    JSON-FG: {"type":"Solid","exterior":[[ring coords with Z],...]}

    Volume and surface area are computed via the signed-volume (divergence theorem)
    approach — works for any closed triangulated or polyhedral mesh.
    """
    shells = place.get("exterior", [])
    if not shells:
        return

    coords_flat = []
    faces = []
    for shell in shells:
        if isinstance(shell, list) and shell and isinstance(shell[0], list):
            for ring in shell:
                if len(ring) >= 3:
                    faces.append(ring)
                    coords_flat.extend(ring)

    if not faces:
        return

    all_z = [c[2] for c in coords_flat if len(c) >= 3]

    if cfg.z_range.enabled and all_z:
        stats["z_range"] = float(max(all_z) - min(all_z))

    if cfg.centroid_3d:
        if coords_flat:
            cx = sum(c[0] for c in coords_flat) / len(coords_flat)
            cy = sum(c[1] for c in coords_flat) / len(coords_flat)
            cz = sum(c[2] for c in coords_flat if len(c) >= 3) / max(len(all_z), 1)
            stats["centroid_3d"] = [cx, cy, cz]

    if cfg.surface_area.enabled or cfg.volume.enabled:
        total_volume = 0.0
        total_surface = 0.0
        for face in faces:
            # Triangulate face (fan from first vertex)
            v0 = face[0]
            for i in range(1, len(face) - 2):
                v1, v2 = face[i], face[i + 1]
                if len(v0) >= 3 and len(v1) >= 3 and len(v2) >= 3:
                    tri_area, signed_vol = _triangle_metrics(v0, v1, v2)
                    total_surface += tri_area
                    total_volume += signed_vol

        if cfg.volume.enabled:
            stats["volume"] = abs(total_volume) / 6.0
        if cfg.surface_area.enabled:
            stats["surface_area"] = total_surface
        if cfg.surface_to_volume_ratio.enabled and stats.get("volume", 0) > 0:
            stats["surface_to_volume_ratio"] = total_surface / stats["volume"]


def _compute_multi_solid_stats(place: Dict, cfg: Any, stats: Dict) -> None:
    """MultiSolid: {"type":"MultiSolid","solids":[...]}"""
    sub_stats_list = []
    for solid in place.get("solids", []):
        sub = {}
        _compute_solid_stats(solid, cfg, sub)
        if sub:
            sub_stats_list.append(sub)
    _aggregate_stats(sub_stats_list, cfg, stats)


# ─────────────────────────────────────────────────────────────────────────────
# 3D CURVE SUPPORT
# ─────────────────────────────────────────────────────────────────────────────

def _compute_3d_curve_stats(place: Dict, cfg: Any, stats: Dict) -> None:
    """3D LineString or MultiLineString: compute z_range and vertical_gradient."""
    zs = _collect_z_coords(place)
    if not zs:
        return

    if cfg.z_range.enabled:
        stats["z_range"] = float(max(zs) - min(zs))

    if cfg.vertical_gradient.enabled:
        coords = _collect_coords(place)
        if len(coords) >= 2:
            # Average slope: total vertical rise / total horizontal distance
            total_rise = sum(
                abs(coords[i][2] - coords[i - 1][2])
                for i in range(1, len(coords))
                if len(coords[i]) >= 3 and len(coords[i - 1]) >= 3
            )
            total_run = sum(
                math.hypot(
                    coords[i][0] - coords[i - 1][0],
                    coords[i][1] - coords[i - 1][1]
                )
                for i in range(1, len(coords))
            )
            if total_run > 0:
                stats["vertical_gradient"] = total_rise / total_run


def _compute_point3d_stats(place: Dict, cfg: Any, stats: Dict) -> None:
    """3D Point: centroid_3d and z_range = 0."""
    coords = place.get("coordinates", [])
    if len(coords) >= 3 and cfg.centroid_3d:
        stats["centroid_3d"] = [float(coords[0]), float(coords[1]), float(coords[2])]
    if cfg.z_range.enabled and len(coords) >= 3:
        stats["z_range"] = 0.0


# ─────────────────────────────────────────────────────────────────────────────
# TEMPORAL DURATION
# ─────────────────────────────────────────────────────────────────────────────

def _compute_temporal_duration(validity: Any) -> Optional[float]:
    """
    Compute duration in seconds from a validity interval.
    Supports:
    - dict with "lower" / "upper" ISO strings
    - tuple / list of two datetime objects
    - string like "[2020-01-01,2021-01-01)" (PostgreSQL TSTZRANGE literal)
    """
    try:
        from datetime import datetime, timezone

        def _parse(v):
            if isinstance(v, datetime):
                return v
            if isinstance(v, str):
                v = v.strip("[()] ")
                return datetime.fromisoformat(v.replace("Z", "+00:00"))
            return None

        if isinstance(validity, dict):
            start = _parse(validity.get("lower") or validity.get("start"))
            end = _parse(validity.get("upper") or validity.get("end"))
        elif isinstance(validity, (list, tuple)) and len(validity) == 2:
            start, end = _parse(validity[0]), _parse(validity[1])
        elif isinstance(validity, str):
            # PostgreSQL "[ts1,ts2)" format
            parts = validity.strip("[]()").split(",")
            if len(parts) == 2:
                start, end = _parse(parts[0].strip()), _parse(parts[1].strip())
            else:
                return None
        else:
            return None

        if start and end:
            return (end - start).total_seconds()
    except Exception as e:
        logger.debug(f"temporal_duration parse error: {e}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _triangle_metrics(v0, v1, v2):
    """Return (surface_area, signed_volume_contribution) for a 3D triangle."""
    ax, ay, az = float(v0[0]), float(v0[1]), float(v0[2])
    bx, by, bz = float(v1[0]), float(v1[1]), float(v1[2])
    cx, cy, cz = float(v2[0]), float(v2[1]), float(v2[2])

    # Cross product for area
    ux, uy, uz = bx - ax, by - ay, bz - az
    vx, vy, vz = cx - ax, cy - ay, cz - az
    nx = uy * vz - uz * vy
    ny = uz * vx - ux * vz
    nz = ux * vy - uy * vx
    area = math.sqrt(nx * nx + ny * ny + nz * nz) / 2.0

    # Signed volume (divergence theorem)
    signed_vol = (
        ax * (by * cz - bz * cy)
        + bx * (cy * az - cz * ay)
        + cx * (ay * bz - az * by)
    )
    return area, signed_vol


def _has_z(place: Dict) -> bool:
    """Check if place geometry has Z coordinates."""
    coords = _collect_coords(place)
    return any(len(c) >= 3 for c in coords)


def _collect_coords(place: Dict):
    """Flatten all coordinate arrays in a GeoJSON-like geometry dict."""
    geom_type = place.get("type", "")
    raw = place.get("coordinates")
    if raw is None:
        return []
    if geom_type == "Point":
        return [raw]
    if geom_type in ("LineString", "MultiPoint"):
        return raw
    if geom_type in ("MultiLineString", "Polygon"):
        return [c for ring in raw for c in ring]
    if geom_type == "MultiPolygon":
        return [c for poly in raw for ring in poly for c in ring]
    return []


def _collect_z_coords(place: Dict):
    return [c[2] for c in _collect_coords(place) if len(c) >= 3]


def _aggregate_stats(sub_list, cfg, stats):
    """Sum volumes, surface areas etc. across sub-geometries."""
    for key in ("volume", "surface_area", "net_floor_area", "z_range"):
        values = [s[key] for s in sub_list if key in s]
        if values:
            attr = getattr(cfg, key, None)
            if attr and attr.enabled:
                stats[key] = sum(values)

    if cfg.surface_to_volume_ratio.enabled and stats.get("volume", 0) > 0 and "surface_area" in stats:
        stats["surface_to_volume_ratio"] = stats["surface_area"] / stats["volume"]

    if cfg.centroid_3d:
        centroids = [s["centroid_3d"] for s in sub_list if "centroid_3d" in s]
        if centroids:
            stats["centroid_3d"] = [
                sum(c[i] for c in centroids) / len(centroids)
                for i in range(3)
            ]
