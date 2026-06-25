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

"""Render engine seam: COG → styled raster image bytes via rio-tiler.

Public surface:

* ``render_cog_tile`` — reads a WebMercatorQuad tile at (z, x, y).
* ``render_cog_map``  — reads an arbitrary bbox at a given width/height
  (Slice 2 addition for OGC API-Maps ``/map`` support).

Both functions apply the same colormap/format pipeline and are isolated
behind this module so that the colormap parser, cache-key logic, and STAC
contributor can be unit-tested **without** a real GDAL/rio-tiler
environment — they only import from ``.colormap`` and ``.config``, never
from ``.engine``.

Output format: ``"PNG"`` or ``"WEBP"`` (uppercase, as accepted by
``ImageData.render``).
"""

from __future__ import annotations

import logging
from typing import Dict, List, Literal, Optional, Tuple

logger = logging.getLogger(__name__)

# rio-tiler colormap type
RioColormap = Dict[int, Tuple[int, int, int, int]]

OutputFormat = Literal["PNG", "WEBP"]


def render_cog_tile(
    href: str,
    z: int,
    x: int,
    y: int,
    *,
    colormap: Optional[RioColormap] = None,
    output_format: OutputFormat = "PNG",
    band: int = 1,
) -> bytes:
    """Render a single raster tile from a COG asset href.

    Opens the COG at ``href`` via rio-tiler's ``COGReader``, reads the tile
    at ``(z, x, y)`` in WebMercatorQuad (the only TMS supported in Slice 1),
    applies ``colormap`` if supplied, and returns the rendered image bytes.

    Args:
        href: The COG asset URL (S3, GCS, or HTTPS; GDAL VSI is applied
            automatically by rio-tiler).
        z: Tile zoom level.
        x: Tile column index.
        y: Tile row index.
        colormap: Discrete colormap dict ``{pixel_value: (R, G, B, A)}``.
            Pass ``None`` to render the raw pixel values (grey-scale PNG).
        output_format: ``"PNG"`` (default) or ``"WEBP"``.
        band: Band index to read (1-based). Defaults to 1 (single-band).

    Returns:
        Raw image bytes in the requested format.

    Raises:
        ImportError: When ``rio-tiler`` is not installed.
        Exception: Any rio-tiler / GDAL error is propagated to the caller so
            the route handler can convert it to an appropriate HTTP 500 / 422.
    """
    try:
        from rio_tiler.io import COGReader  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "rio-tiler is required for COG rendering. "
            "Install the renders extension: `pip install dynastore-ext-renders`."
        ) from exc

    with COGReader(input=href) as cog:  # type: ignore[call-arg]  # rio-tiler attrs NOTHING default
        img = cog.tile(
            tile_x=x,
            tile_y=y,
            tile_z=z,
            indexes=(band,),
        )

    return img.render(
        img_format=output_format,
        colormap=colormap,
        add_mask=True,
    )


def render_cog_map(
    href: str,
    *,
    bbox: List[float],
    width: int,
    height: int,
    colormap: Optional[RioColormap] = None,
    output_format: OutputFormat = "PNG",
    band: int = 1,
) -> bytes:
    """Render a COG asset over an arbitrary geographic bounding box.

    Opens the COG at ``href`` via rio-tiler's ``COGReader``, reads the region
    defined by ``bbox`` (``[min_lon, min_lat, max_lon, max_lat]`` in
    WGS-84 / EPSG:4326) at the requested pixel dimensions, applies
    ``colormap`` if supplied, and returns the rendered image bytes.

    This is the Slice 2 companion to ``render_cog_tile``: the tile function
    handles WebMercatorQuad z/x/y requests; this function handles the
    OGC API-Maps ``/map?bbox=...&width=...&height=...`` form.

    Args:
        href: COG asset URL (S3, GCS, or HTTPS).
        bbox: ``[min_lon, min_lat, max_lon, max_lat]`` in EPSG:4326.
        width: Output image width in pixels.
        height: Output image height in pixels.
        colormap: Discrete colormap ``{pixel_value: (R, G, B, A)}``.
            Pass ``None`` to render raw values.
        output_format: ``"PNG"`` (default) or ``"WEBP"``.
        band: Band index to read (1-based). Defaults to 1.

    Returns:
        Raw image bytes in the requested format.

    Raises:
        ImportError: When ``rio-tiler`` is not installed.
        Exception: Any rio-tiler / GDAL error is propagated to the caller.
    """
    try:
        from rio_tiler.io import COGReader  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "rio-tiler is required for COG rendering. "
            "Install the renders extension: `pip install dynastore-ext-renders`."
        ) from exc

    min_lon, min_lat, max_lon, max_lat = bbox
    with COGReader(input=href) as cog:  # type: ignore[call-arg]
        img = cog.part(
            bbox=(min_lon, min_lat, max_lon, max_lat),
            indexes=(band,),
            width=width,
            height=height,
        )

    return img.render(
        img_format=output_format,
        colormap=colormap,
        add_mask=True,
    )
