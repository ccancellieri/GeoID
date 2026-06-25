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
    z_bands: Optional[list[int]] = None,
) -> Dict[int, Optional[float]]:
    """Extract pixel values at (lon, lat) for specified bands.

    Args:
        href: Raster asset URI
        lon: Longitude coordinate
        lat: Latitude coordinate
        z_bands: Optional list of 1-based band indices to extract.
                 If None, extracts all bands.

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
        if box.width == 0 or box.height == 0:
            col = max(0, min(box.col_off, ref.width - 1))
            row = max(0, min(box.row_off, ref.height - 1))
            box = WindowBox(col, row, 1, 1)

        import rasterio.windows

        bands_to_read = z_bands if z_bands else list(range(1, ds.count + 1))

        values: Dict[int, Optional[float]] = {}
        for band_idx in bands_to_read:
            if band_idx < 1 or band_idx > ds.count:
                values[band_idx] = None
                continue
            arr = ds.read(
                band_idx,
                window=rasterio.windows.Window(box.col_off, box.row_off, box.width, box.height),
            )
            values[band_idx] = float(arr.flat[0]) if arr.size > 0 else None
        return values
    finally:
        ds.close()


def get_raster_crs(href: str) -> Optional[str]:
    """Get the CRS of a raster asset.

    Returns the CRS as a string (typically EPSG:XXXX or WKT).
    Returns None if CRS cannot be determined.
    """
    from dynastore.modules.gdal.service import open_raster_vsi

    ds = open_raster_vsi(href)
    try:
        return str(ds.crs) if ds.crs else None
    finally:
        ds.close()
