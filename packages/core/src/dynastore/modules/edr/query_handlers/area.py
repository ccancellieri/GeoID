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

"""EDR area query handler: extract coverage subset for a WKT POLYGON bbox."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


def parse_wkt_polygon_bbox(wkt: str) -> Tuple[float, float, float, float]:
    """Extract bounding box from WKT POLYGON → (min_lon, min_lat, max_lon, max_lat).

    Pure Python; handles 2D and 3D POLYGON / MULTIPOLYGON geometries.
    Parses each vertex as a whitespace-separated token tuple and takes only
    the first two ordinates (X=lon, Y=lat), ignoring any Z value.
    """
    inner = re.sub(r"[A-Za-z]+\s*", "", wkt)  # strip keyword prefixes
    tokens = re.findall(r"[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?", inner)
    if not tokens:
        raise ValueError(f"Cannot extract coordinates from WKT: {wkt!r}")

    # Determine ordinate count (2D vs 3D) by peeking at vertex token density.
    # Strategy: split by vertex separator (comma in WKT rings) and count
    # space-delimited numbers per vertex to detect Z ordinates.
    vertex_strs = re.split(r",", re.sub(r"[()A-Za-z]", " ", wkt))
    dim = 2
    for vs in vertex_strs:
        parts = vs.split()
        num_parts = sum(1 for p in parts if re.match(r"^[+-]?\d+(?:\.\d+)?", p))
        if num_parts >= 3:
            dim = 3
            break

    lons: List[float] = []
    lats: List[float] = []
    for i in range(0, len(tokens) - dim + 1, dim):
        lons.append(float(tokens[i]))
        lats.append(float(tokens[i + 1]))

    if not lons:
        raise ValueError(f"Cannot extract coordinates from WKT: {wkt!r}")
    return min(lons), min(lats), max(lons), max(lats)


def extract_area_values(
    href: str,
    bbox: Tuple[float, float, float, float],
    z_bands: Optional[List[int]] = None,
) -> Tuple[Any, Any, Dict[int, List]]:
    """Extract area data from a raster clipped to bbox.

    Args:
        href: Raster asset URI
        bbox: Bounding box as (min_lon, min_lat, max_lon, max_lat)
        z_bands: Optional list of 1-based band indices to extract.
                 If None, extracts all bands.

    Returns (RasterGeoRef, WindowBox, {band_index: [[row values], ...]}).
    Lazy-imports rasterio.
    """
    from dynastore.modules.coverages.subset import AxisRange, SubsetRequest
    from dynastore.modules.coverages.window import RasterGeoRef, resolve_window
    from dynastore.modules.coverages.reader import read_window_iter
    from dynastore.modules.gdal.service import open_raster_vsi

    min_lon, min_lat, max_lon, max_lat = bbox
    req = SubsetRequest(axes=[
        AxisRange("Lon", min_lon, max_lon),
        AxisRange("Lat", min_lat, max_lat),
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

        bands_to_read = z_bands if z_bands else list(range(1, ds.count + 1))

        band_arrays: Dict[int, List] = {}
        for band_idx in bands_to_read:
            if band_idx < 1 or band_idx > ds.count:
                band_arrays[band_idx] = []
                continue
            rows: List = []
            for chunk in read_window_iter(ds, box, band=band_idx):
                rows.extend(chunk.tolist())
            band_arrays[band_idx] = rows
        return ref, box, band_arrays
    finally:
        ds.close()
