"""EDR position query handler: extract pixel values at a WKT POINT."""

from __future__ import annotations

import re
from typing import Dict, Optional, Tuple


_POINT_RE = re.compile(
    r"^\s*POINT\s*\(\s*([+-]?\d+(?:\.\d+)?)\s+([+-]?\d+(?:\.\d+)?)\s*\)\s*$",
    re.IGNORECASE,
)


def parse_wkt_point(wkt: str) -> Tuple[float, float]:
    """Parse WKT POINT → (lon, lat). Pure Python, no GDAL."""
    m = _POINT_RE.match(wkt)
    if not m:
        raise ValueError(f"Invalid WKT POINT: {wkt!r}")
    return float(m.group(1)), float(m.group(2))


def extract_point_values(
    href: str,
    lon: float,
    lat: float,
) -> Dict[int, Optional[float]]:
    """Extract pixel values at (lon, lat) for all bands.

    Returns a dict mapping 1-based band index → float value (or None on error).
    Lazy-imports rasterio so callers without it can import this module.
    """
    from dynastore.modules.coverages.subset import AxisRange, SubsetRequest
    from dynastore.modules.coverages.window import RasterGeoRef, WindowBox, resolve_window
    from dynastore.modules.gdal.service import open_raster_vsi

    req = SubsetRequest(axes=[
        AxisRange("Lon", lon, lon),
        AxisRange("Lat", lat, lat),
    ])

    ds = open_raster_vsi(href)
    try:
        t = ds.transform
        ref = RasterGeoRef(
            width=ds.width,
            height=ds.height,
            origin_x=t.c,
            origin_y=t.f,
            pixel_x=t.a,
            pixel_y=t.e,
            crs=str(ds.crs),
            axis_order=("Lon", "Lat"),
        )
        box = resolve_window(req, ref)
        # A point collapses the window to 0x0 — expand to a single pixel.
        if box.width == 0 or box.height == 0:
            col = max(0, min(box.col_off, ref.width - 1))
            row = max(0, min(box.row_off, ref.height - 1))
            box = WindowBox(col, row, 1, 1)

        import rasterio.windows

        values: Dict[int, Optional[float]] = {}
        for band_idx in range(1, ds.count + 1):
            arr = ds.read(
                band_idx,
                window=rasterio.windows.Window(box.col_off, box.row_off, box.width, box.height),  # type: ignore
            )
            values[band_idx] = float(arr.flat[0]) if arr.size > 0 else None
        return values
    finally:
        ds.close()
