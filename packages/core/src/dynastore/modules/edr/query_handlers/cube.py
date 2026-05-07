"""EDR cube query handler: parse bbox and delegate to area extraction."""

from __future__ import annotations

from typing import Tuple


def parse_cube_bbox(bbox_str: str) -> Tuple[float, float, float, float]:
    """Parse EDR cube bbox param → (min_lon, min_lat, max_lon, max_lat).

    Accepts comma-separated values; optional 3D/4D extra values are ignored.
    """
    parts = [p.strip() for p in bbox_str.split(",")]
    if len(parts) < 4:
        raise ValueError(
            f"bbox must have at least 4 comma-separated values: {bbox_str!r}"
        )
    return float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3])
