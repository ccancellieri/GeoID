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

"""Windowed raster reader that yields blocks without loading the whole image.

Lazy-imports rasterio. Callers must supply an open rasterio dataset
(``open_raster_vsi`` from ``modules/gdal/service``).

``reader_for``/``open_coverage`` below are the format->reader registry: they
pick the opener for a *source* asset from its declared STAC media type
(``item["assets"][key]["type"]``), so a collection can mix e.g. a COG asset
in one item with a Zarr asset in another. This is distinct from the output
*writer* registry (``writers/__init__.py:writer_for``), which dispatches on
the client-requested response format instead.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Callable, Iterator, Optional, Tuple

from dynastore.modules.coverages.window import RasterGeoRef, WindowBox


class UnsupportedReaderMediaType(ValueError):
    """Raised when no reader is registered for a source asset's media type."""


def _open_via_gdal(href: str):
    from dynastore.modules.gdal.service import open_raster_vsi
    return open_raster_vsi(href)


# STAC media types that open through GDAL's VSI layer (rasterio). GDAL 3.13
# (pinned in module_gdal) natively drivers all three of these — COG/GeoTIFF,
# NetCDF, and Zarr — so one reader currently serves every registered format.
# "" covers items that don't declare a precise ``type`` (back-compat default).
# Adding a dedicated reader for a new format (e.g. a native xarray reader)
# only means adding an entry here; no call site changes.
_READER_FOR_MEDIA_TYPE: dict = {
    "": _open_via_gdal,
    "image/tiff; application=geotiff": _open_via_gdal,
    "image/tiff; application=geotiff; profile=cloud-optimized": _open_via_gdal,
    "image/tiff": _open_via_gdal,
    "image/geotiff": _open_via_gdal,
    "application/x-netcdf": _open_via_gdal,
    "application/netcdf": _open_via_gdal,
    "application/vnd+zarr": _open_via_gdal,
}


def reader_for(media_type: str) -> Callable[[str], Any]:
    """Return the opener registered for *media_type*.

    Raises :class:`UnsupportedReaderMediaType` for a declared media type
    with no registered reader, rather than silently trying to open it.
    """
    try:
        return _READER_FOR_MEDIA_TYPE[media_type]
    except KeyError:
        raise UnsupportedReaderMediaType(
            f"No reader registered for asset media type {media_type!r}."
        ) from None


@contextmanager
def open_coverage(href: str, media_type: str = ""):
    """Open *href* via the reader registered for *media_type*.

    Yields ``(ds, ref)`` — the opened dataset and its :class:`RasterGeoRef`
    — and closes the dataset on exit. Consolidates the open/georeference
    steps that used to be duplicated per output format in
    ``coverages_service.py``.
    """
    ds = reader_for(media_type)(href)
    try:
        t = ds.transform
        ref = RasterGeoRef(
            width=ds.width, height=ds.height,
            origin_x=t.c, origin_y=t.f,
            pixel_x=t.a, pixel_y=t.e,
            crs=str(ds.crs),
            axis_order=("Lon", "Lat"),
        )
        yield ds, ref
    finally:
        ds.close()


def read_window_iter(ds, box: WindowBox, band: int = 1, block: int = 512) -> Iterator:
    """Yield numpy arrays block-by-block covering ``box``."""
    import numpy as np
    from rasterio.windows import Window

    if box.width == 0 or box.height == 0:
        return

    for row in range(box.row_off, box.row_off + box.height, block):
        rh = min(block, box.row_off + box.height - row)
        for col in range(box.col_off, box.col_off + box.width, block):
            cw = min(block, box.col_off + box.width - col)
            arr = ds.read(band, window=Window(col, row, cw, rh))  # type: ignore
            yield np.asarray(arr)


def read_scaled(
    ds,
    box: WindowBox,
    band: int = 1,
    out_shape: Optional[Tuple[int, int]] = None,
) -> Any:
    """Read the raster window in a single call, optionally downsampling.

    ``out_shape`` is ``(out_height, out_width)`` in output pixels. When
    ``None`` the native resolution of ``box`` is used.  Uses rasterio's
    built-in ``out_shape`` resampling (Lanczos for downsampling, nearest
    otherwise) so the full down-sample is GPU/GDAL-accelerated and does not
    buffer the native-resolution pixels in Python.
    """
    import numpy as np
    from rasterio.enums import Resampling
    from rasterio.windows import Window

    win = Window(box.col_off, box.row_off, box.width, box.height)
    if out_shape is None:
        out_shape = (box.height, box.width)

    arr = ds.read(  # type: ignore[union-attr]
        band,
        window=win,
        out_shape=out_shape,
        resampling=Resampling.lanczos,
    )
    return np.asarray(arr)
