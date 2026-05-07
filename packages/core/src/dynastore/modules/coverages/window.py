"""Resolve a SubsetRequest against a raster's georeference → pixel window.

Pure math: no rasterio. The caller adapts WindowBox to
rasterio.Window(col_off, row_off, width, height) when reading.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from dynastore.modules.coverages.subset import SubsetRequest


class WindowResolveError(ValueError):
    """Raised when a subset cannot be mapped onto the raster."""


@dataclass(frozen=True)
class RasterGeoRef:
    width: int
    height: int
    origin_x: float
    origin_y: float
    pixel_x: float
    pixel_y: float
    crs: str
    axis_order: Tuple[str, str]


@dataclass(frozen=True)
class WindowBox:
    col_off: int
    row_off: int
    width: int
    height: int


def resolve_window(req: SubsetRequest, ref: RasterGeoRef) -> WindowBox:
    import math
    if not req.axes:
        return WindowBox(0, 0, ref.width, ref.height)

    x_aliases, y_aliases = _axis_aliases(ref.axis_order)
    x_lo = x_hi = y_lo = y_hi = None
    for ar in req.axes:
        name = ar.axis.lower()
        if name in x_aliases:
            x_lo, x_hi = float(ar.low), float(ar.high)
        elif name in y_aliases:
            y_lo, y_hi = float(ar.low), float(ar.high)
        else:
            raise WindowResolveError(
                f"Unknown axis {ar.axis!r}; expected one of "
                f"{sorted(x_aliases | y_aliases)}",
            )

    if x_lo is None or x_hi is None:
        x_lo = ref.origin_x
        x_hi = ref.origin_x + ref.pixel_x * ref.width
    if y_lo is None or y_hi is None:
        top = ref.origin_y
        bot = ref.origin_y + ref.pixel_y * ref.height
        y_lo, y_hi = min(top, bot), max(top, bot)

    col_lo = int(math.floor((x_lo - ref.origin_x) / ref.pixel_x))
    col_hi = int(math.ceil((x_hi - ref.origin_x) / ref.pixel_x))
    row_lo = int(math.floor((ref.origin_y - y_hi) / -ref.pixel_y))
    row_hi = int(math.ceil((ref.origin_y - y_lo) / -ref.pixel_y))

    col_lo, col_hi = _clamp(col_lo, 0, ref.width), _clamp(col_hi, 0, ref.width)
    row_lo, row_hi = _clamp(row_lo, 0, ref.height), _clamp(row_hi, 0, ref.height)
    return WindowBox(col_lo, row_lo, max(0, col_hi - col_lo), max(0, row_hi - row_lo))


def _axis_aliases(axis_order: Tuple[str, str]):
    x_name, y_name = axis_order
    x_aliases = {x_name.lower(), "lon", "longitude", "x", "e", "east"}
    y_aliases = {y_name.lower(), "lat", "latitude", "y", "n", "north"}
    return x_aliases, y_aliases


def _clamp(v: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, v))
