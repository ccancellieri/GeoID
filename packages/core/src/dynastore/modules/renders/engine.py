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

Both functions support single-band rendering (``band``), multiband RGB
composites (``bands``, e.g. ``(3, 2, 1)``), band-math expressions
(``expression``, e.g. ``"(B1-B2)/(B1+B2)"``) via rio-tiler's native parser,
and per-band rescaling (``rescale``).  Priority when several are supplied:
``expression`` > ``bands`` > ``band``.

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
from typing import Dict, List, Literal, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

# rio-tiler colormap type
RioColormap = Dict[int, Tuple[int, int, int, int]]

OutputFormat = Literal["PNG", "WEBP"]

# Per-band rescale range: one (min, max) entry per output band.
RescaleRange = Sequence[Tuple[float, float]]


def _resolve_indexes(
    band: int,
    bands: Optional[Sequence[int]],
    expression: Optional[str],
) -> Tuple[Optional[Sequence[int]], Optional[str]]:
    """Resolve the effective (indexes, expression) pair for a COGReader call.

    Priority: expression > bands > band.  Returns ``(None, expression)`` when
    an expression is given so COGReader can apply it natively.  Returns
    ``(indexes, None)`` otherwise.
    """
    if expression:
        return None, expression
    if bands:
        return tuple(bands), None
    return (band,), None


def render_cog_tile(
    href: str,
    z: int,
    x: int,
    y: int,
    *,
    colormap: Optional[RioColormap] = None,
    output_format: OutputFormat = "PNG",
    band: int = 1,
    bands: Optional[Sequence[int]] = None,
    expression: Optional[str] = None,
    rescale: Optional[RescaleRange] = None,
) -> bytes:
    """Render a raster tile from a COG asset href.

    Opens the COG at ``href`` via rio-tiler's ``COGReader``, reads the tile
    at ``(z, x, y)`` in WebMercatorQuad, applies optional per-band rescaling
    and ``colormap``, and returns the rendered image bytes.

    Args:
        href: The COG asset URL (S3, GCS, or HTTPS; GDAL VSI is applied
            automatically by rio-tiler).
        z: Tile zoom level.
        x: Tile column index.
        y: Tile row index.
        colormap: Discrete colormap dict ``{pixel_value: (R, G, B, A)}``.
            Pass ``None`` to render the raw pixel values.
        output_format: ``"PNG"`` (default) or ``"WEBP"``.
        band: Single band index to read (1-based). Defaults to 1.  Ignored
            when ``bands`` or ``expression`` is supplied.
        bands: Sequence of band indices for multiband / RGB composite rendering,
            e.g. ``(3, 2, 1)`` for true-colour.  Passed as ``indexes=`` to
            COGReader.  Takes precedence over ``band``.
        expression: Band-math expression (e.g. ``"(B1-B2)/(B1+B2)"``).
            Passed directly to COGReader which uses numexpr when available.
            Takes precedence over ``bands`` and ``band``.
        rescale: Per-band rescale ranges as a list of ``(min, max)`` tuples,
            one per output band.  Applied via ``ImageData.rescale()`` before
            rendering so values are normalised to the uint8 range.

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

    indexes, expr = _resolve_indexes(band, bands, expression)

    with COGReader(input=href) as cog:  # type: ignore[call-arg]
        img = cog.tile(
            tile_x=x,
            tile_y=y,
            tile_z=z,
            indexes=indexes,
            expression=expr,
        )

    if rescale:
        img.rescale(in_range=list(rescale))

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
    bands: Optional[Sequence[int]] = None,
    expression: Optional[str] = None,
    rescale: Optional[RescaleRange] = None,
) -> bytes:
    """Render a COG asset over an arbitrary geographic bounding box.

    Opens the COG at ``href`` via rio-tiler's ``COGReader``, reads the region
    defined by ``bbox`` (``[min_lon, min_lat, max_lon, max_lat]`` in
    WGS-84 / EPSG:4326) at the requested pixel dimensions, applies optional
    per-band rescaling and ``colormap``, and returns the rendered image bytes.

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
        band: Single band index to read (1-based). Defaults to 1.  Ignored
            when ``bands`` or ``expression`` is supplied.
        bands: Sequence of band indices for multiband / RGB composite rendering.
            Passed as ``indexes=`` to COGReader.  Takes precedence over ``band``.
        expression: Band-math expression.  Passed directly to COGReader.
            Takes precedence over ``bands`` and ``band``.
        rescale: Per-band rescale ranges as a list of ``(min, max)`` tuples.
            Applied via ``ImageData.rescale()`` before rendering.

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

    indexes, expr = _resolve_indexes(band, bands, expression)
    min_lon, min_lat, max_lon, max_lat = bbox

    with COGReader(input=href) as cog:  # type: ignore[call-arg]
        img = cog.part(
            bbox=(min_lon, min_lat, max_lon, max_lat),
            indexes=indexes,
            expression=expr,
            width=width,
            height=height,
        )

    if rescale:
        img.rescale(in_range=list(rescale))

    return img.render(
        img_format=output_format,
        colormap=colormap,
        add_mask=True,
    )
