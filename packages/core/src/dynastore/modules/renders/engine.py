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
* ``render_cog_map`` — reads an arbitrary bbox at a given width/height
  (OGC API-Maps ``/map`` support).
* ``render_cog_terrain_rgb`` — single-band elevation COG → Terrain-RGB PNG
  tile, encoding elevation to the Mapbox ``raster-dem`` scheme (packed
  metres) so MapLibre can consume it as a ``raster-dem`` source.
* ``render_cog_hillshade`` — single-band elevation COG → shaded-relief PNG
  tile with optional hypsometric colormap overlay; azimuth and altitude
  control the illumination direction.

``render_cog_tile`` and ``render_cog_map`` support single-band rendering
(``band``), multiband RGB composites (``bands``, e.g. ``(3, 2, 1)``),
band-math expressions (``expression``, e.g. ``"(B1-B2)/(B1+B2)"``) via
rio-tiler's native parser, and per-band rescaling (``rescale``).  Priority
when several are supplied: ``expression`` > ``bands`` > ``band``.

All functions apply the same colormap/format pipeline and are isolated
behind this module so that the colormap parser, cache-key logic, and STAC
contributor can be unit-tested **without** a real GDAL/rio-tiler
environment — they only import from ``.colormap`` and ``.config``, never
from ``.engine``.

Output format: ``"PNG"`` or ``"WEBP"`` (uppercase, as accepted by
``ImageData.render``).

Terrain-RGB encoding (Mapbox scheme)
-------------------------------------
``elevation = -10000 + (R * 256 * 256 + G * 256 + B) * 0.1``

Inverting: given an elevation value *h* in metres the packed integer is::

    packed = round((h + 10000) / 0.1)
    R = packed >> 16
    G = (packed >> 8) & 0xFF
    B = packed & 0xFF

Values below -10 000 m clip to (0, 0, 0); values above ~1 677 721 m clip to
(255, 255, 255).  This matches the Mapbox Terrain-RGB v1 specification and is
understood natively by MapLibre GL v4+.
"""

from __future__ import annotations

import logging
from typing import List, Literal, Optional, Sequence, Tuple

import numpy as np

from dynastore.modules.renders.colormap import RGBA, RioColormap

logger = logging.getLogger(__name__)

OutputFormat = Literal["PNG", "WEBP"]

# Per-band rescale range: one (min, max) entry per output band.
RescaleRange = Sequence[Tuple[float, float]]

# Terrain-RGB encoding constants (Mapbox scheme, identical to Terrarium when
# the offset / scale differ, but we default to the Mapbox variant understood
# by MapLibre `raster-dem`).
_TERRAIN_RGB_OFFSET: float = 10000.0
_TERRAIN_RGB_SCALE: float = 0.1


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


def _elevation_to_terrain_rgb(elevation: np.ndarray) -> np.ndarray:
    """Convert a float32 elevation array (metres) to a (3, H, W) uint8 RGB array.

    Uses the Mapbox Terrain-RGB encoding::

        packed = round((elevation + 10_000) / 0.1)
        R = packed >> 16
        G = (packed >> 8) & 0xFF
        B = packed & 0xFF

    Out-of-range values are clipped to [0, 16 777 215] before packing.

    Args:
        elevation: 2-D float32 array of elevation values in metres.

    Returns:
        ``(3, H, W)`` uint8 array suitable for use as an RGB PNG tile.
    """
    packed = np.round((elevation.astype(np.float64) + _TERRAIN_RGB_OFFSET) / _TERRAIN_RGB_SCALE)
    packed = np.clip(packed, 0, 0xFF_FF_FF).astype(np.int32)
    r = ((packed >> 16) & 0xFF).astype(np.uint8)
    g = ((packed >> 8) & 0xFF).astype(np.uint8)
    b = (packed & 0xFF).astype(np.uint8)
    return np.stack([r, g, b], axis=0)


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
        colormap: Discrete colormap dict ``{pixel_value: (R, G, B, A)}`` or
            interval colormap ``[((lo, hi), (R, G, B, A)), ...]`` — both are
            accepted verbatim by rio-tiler.  Pass ``None`` to render the raw
            pixel values.
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
        colormap: Discrete colormap ``{pixel_value: (R, G, B, A)}`` or
            interval colormap ``[((lo, hi), (R, G, B, A)), ...]``.
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


def render_cog_terrain_rgb(
    href: str,
    z: int,
    x: int,
    y: int,
    *,
    band: int = 1,
) -> bytes:
    """Render a Terrain-RGB PNG tile from a single-band elevation COG.

    Encodes the elevation data using the Mapbox Terrain-RGB v1 scheme so
    MapLibre can consume the result as a ``raster-dem`` source for 3-D terrain
    rendering and vertical-exaggeration::

        elevation = -10 000 + (R * 65 536 + G * 256 + B) * 0.1

    The tile is always returned as PNG (the only format understood by MapLibre
    for ``raster-dem``).  NoData / masked pixels are encoded as (0, 0, 0) which
    maps to −10 000 m — below any real terrain — and MapLibre skips them.

    Args:
        href: COG asset URL (S3, GCS, or HTTPS).
        z: Tile zoom level.
        x: Tile column index.
        y: Tile row index.
        band: Elevation band index (1-based). Defaults to 1.

    Returns:
        PNG bytes of the Terrain-RGB tile.

    Raises:
        ImportError: When ``rio-tiler`` or ``numpy`` is not installed.
        Exception: Any rio-tiler / GDAL error is propagated to the caller.
    """
    try:
        from rio_tiler.io import COGReader  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "rio-tiler is required for Terrain-RGB rendering. "
            "Install the renders extension: `pip install dynastore-ext-renders`."
        ) from exc

    with COGReader(input=href) as cog:  # type: ignore[call-arg]
        img = cog.tile(
            tile_x=x,
            tile_y=y,
            tile_z=z,
            indexes=(band,),
        )

    # img.data is (1, H, W) float32 array of elevation in the COG's unit.
    # img.mask is (1, H, W) uint8 where 255 = valid, 0 = nodata.
    elev = img.data[0]  # (H, W)
    mask = img.mask[0] if img.mask is not None else np.full(elev.shape, 255, dtype=np.uint8)

    rgb = _elevation_to_terrain_rgb(elev)  # (3, H, W)

    # Apply mask: pixels that are nodata stay at (0, 0, 0) = −10 000 m.
    nodata = mask == 0
    rgb[:, nodata] = 0

    # Reuse the existing ImageData object by replacing its array in-place so
    # metadata (bounds, crs) is preserved without constructing a new instance.
    img.data = rgb  # type: ignore[assignment]
    img.mask = np.full(rgb.shape[1:], 255, dtype=np.uint8)  # type: ignore[assignment]
    return img.render(img_format="PNG", add_mask=False)


def render_cog_hillshade(
    href: str,
    z: int,
    x: int,
    y: int,
    *,
    band: int = 1,
    azimuth: float = 315.0,
    altitude: float = 45.0,
    colormap: Optional[RioColormap] = None,
) -> bytes:
    """Render a shaded-relief (hillshade) PNG tile from a single-band elevation COG.

    Computes the Lambertian hillshade via the Horn (1981) gradient method using
    NumPy, then optionally blends a hypsometric colormap overlay driven by the
    resolved SLD style (same style-resolution path as ``render_cog_tile``).

    The result is always a 4-channel RGBA PNG so the caller can composite it
    over a basemap at any opacity.

    Args:
        href: COG asset URL (S3, GCS, or HTTPS).
        z: Tile zoom level.
        x: Tile column index.
        y: Tile row index.
        band: Elevation band index (1-based). Defaults to 1.
        azimuth: Sun azimuth in degrees (0 = North, clockwise). Defaults to
            315° (north-west, the conventional cartographic default).
        altitude: Sun altitude above the horizon in degrees. Defaults to 45°.
        colormap: Optional discrete colormap dict ``{pixel_value: (R, G, B, A)}``
            for hypsometric tinting. When supplied, the hillshade intensity
            modulates the colormap colours; when ``None`` a greyscale hillshade
            is returned.

    Returns:
        RGBA PNG bytes of the hillshade tile.

    Raises:
        ImportError: When ``rio-tiler`` or ``numpy`` is not installed.
        Exception: Any rio-tiler / GDAL error is propagated to the caller.
    """
    try:
        from rio_tiler.io import COGReader  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "rio-tiler is required for hillshade rendering. "
            "Install the renders extension: `pip install dynastore-ext-renders`."
        ) from exc

    with COGReader(input=href) as cog:  # type: ignore[call-arg]
        img = cog.tile(
            tile_x=x,
            tile_y=y,
            tile_z=z,
            indexes=(band,),
        )

    elev = img.data[0].astype(np.float64)  # (H, W)
    mask = img.mask[0] if img.mask is not None else np.full(elev.shape, 255, dtype=np.uint8)

    shade = _compute_hillshade(elev, azimuth=azimuth, altitude=altitude)  # (H, W) float [0,1]

    if colormap:
        rgba = _apply_colormap_hillshade(elev, shade, colormap)
    else:
        # Greyscale hillshade: all three channels equal, full alpha for valid pixels.
        grey = (shade * 255).astype(np.uint8)
        rgba = np.stack([grey, grey, grey, np.where(mask > 0, 255, 0).astype(np.uint8)], axis=0)

    # Mask nodata pixels to fully transparent.
    rgba[3, mask == 0] = 0

    # Reuse the existing ImageData object to preserve bounds/crs metadata.
    img.data = rgba  # type: ignore[assignment]
    img.mask = np.full(rgba.shape[1:], 255, dtype=np.uint8)  # type: ignore[assignment]
    return img.render(img_format="PNG", add_mask=False)


# ---------------------------------------------------------------------------
# Private helpers — pure NumPy, importable without GDAL for unit testing.
# ---------------------------------------------------------------------------


def _compute_hillshade(
    elevation: np.ndarray,
    *,
    azimuth: float = 315.0,
    altitude: float = 45.0,
    z_factor: float = 1.0,
) -> np.ndarray:
    """Compute Lambertian hillshade intensity from a 2-D elevation array.

    Uses the Horn (1981) finite-difference gradient method, which is the same
    algorithm used by GDAL's ``gdaldem hillshade`` command.

    Args:
        elevation: 2-D float array of elevation values (any consistent unit).
        azimuth: Sun azimuth in degrees clockwise from North (default 315°).
        altitude: Sun altitude above the horizon in degrees (default 45°).
        z_factor: Vertical exaggeration applied to gradients (default 1.0).

    Returns:
        2-D float array in ``[0, 1]`` with the same shape as ``elevation``.
        Boundary rows/columns are filled with 0 (fully shadowed).
    """
    az_rad = np.deg2rad(360.0 - azimuth + 90.0)
    alt_rad = np.deg2rad(altitude)

    dx = np.zeros_like(elevation)
    dy = np.zeros_like(elevation)

    # Central difference on interior pixels (Horn / GDAL convention)
    dx[1:-1, 1:-1] = (
        (elevation[1:-1, 2:] - elevation[1:-1, :-2]) / 2.0
    ) * z_factor
    dy[1:-1, 1:-1] = (
        (elevation[:-2, 1:-1] - elevation[2:, 1:-1]) / 2.0
    ) * z_factor

    slope = np.arctan(np.sqrt(dx**2 + dy**2))
    aspect = np.arctan2(dy, -dx)

    shade = (
        np.sin(alt_rad) * np.cos(slope)
        + np.cos(alt_rad) * np.sin(slope) * np.cos(az_rad - aspect)
    )
    return np.clip(shade, 0.0, 1.0)


def _apply_colormap_hillshade(
    elevation: np.ndarray,
    shade: np.ndarray,
    colormap: RioColormap,
) -> np.ndarray:
    """Map elevation values through a discrete colormap and modulate by hillshade.

    Each pixel is assigned the colour for the nearest colormap entry whose key
    is ≤ the pixel's elevation value.  The RGB channels are then multiplied by
    the hillshade intensity so ridges stay bright and valleys darken.  The alpha
    channel comes from the colormap entry's alpha component.

    Args:
        elevation: 2-D float array of elevation values.
        shade: 2-D float array in ``[0, 1]`` (hillshade intensity).
        colormap: Discrete colormap ``{key: (R, G, B, A)}`` or interval
            colormap ``[((lo, hi), (R, G, B, A)), ...]``; either way each
            entry colours the values from its key/lower bound up to the next
            entry.

    Returns:
        ``(4, H, W)`` uint8 RGBA array.
    """
    h, w = elevation.shape
    out_r = np.zeros((h, w), dtype=np.uint8)
    out_g = np.zeros((h, w), dtype=np.uint8)
    out_b = np.zeros((h, w), dtype=np.uint8)
    out_a = np.zeros((h, w), dtype=np.uint8)

    if not colormap:
        grey = (shade * 255).astype(np.uint8)
        return np.stack([grey, grey, grey, np.full((h, w), 255, dtype=np.uint8)], axis=0)

    # Normalize both colormap shapes to sorted (lower_bound, rgba) pairs; the
    # loop below treats each lower bound as covering up to the next one.
    if isinstance(colormap, dict):
        pairs: List[Tuple[float, RGBA]] = sorted(colormap.items())
    else:
        pairs = sorted((bounds[0], rgba) for bounds, rgba in colormap)

    for i, (key, (r, g, b, a)) in enumerate(pairs):
        if i + 1 < len(pairs):
            mask = (elevation >= key) & (elevation < pairs[i + 1][0])
        else:
            mask = elevation >= key

        hs = shade[mask]
        out_r[mask] = np.clip(r * hs, 0, 255).astype(np.uint8)
        out_g[mask] = np.clip(g * hs, 0, 255).astype(np.uint8)
        out_b[mask] = np.clip(b * hs, 0, 255).astype(np.uint8)
        out_a[mask] = a

    # Pixels below the lowest key get the lowest colour with hillshade applied.
    lowest_key, (r0, g0, b0, a0) = pairs[0]
    below = elevation < lowest_key
    hs_below = shade[below]
    out_r[below] = np.clip(r0 * hs_below, 0, 255).astype(np.uint8)
    out_g[below] = np.clip(g0 * hs_below, 0, 255).astype(np.uint8)
    out_b[below] = np.clip(b0 * hs_below, 0, 255).astype(np.uint8)
    out_a[below] = a0

    return np.stack([out_r, out_g, out_b, out_a], axis=0)
