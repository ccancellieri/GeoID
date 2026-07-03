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

"""Optional fast path: assemble a vector ``/map`` from the cached MVT pyramid.

When the tiles module is loaded, on-demand caching is enabled for the
collection, and every MVT tile covering the request is already cached, a
default-style vector ``/map`` can be served by decoding those tiles instead of
running the PostGIS geometry fetch + simplification (the slow, occasionally
timing-out path). Tiles are decoded with GDAL's OGR MVT driver, which
georeferences a single tile blob to EPSG:3857 from its ``z/x/y``.

On ANY gate failure or cache miss the entry point returns ``None`` and the
caller renders from source exactly as before — the OGC API - Maps response
semantics (arbitrary bbox / width / height / CRS) are unchanged. The fast path
is therefore a pure, best-effort accelerator.

Scope of this first cut (deliberate, see the PR for rationale):
- WebMercatorQuad only — the GDAL MVT driver georeferences tiles to EPSG:3857.
- Default style only — a requested style falls back to the source render, which
  carries full feature attributes for symbolizer matching.
- No ``datetime`` / ``subset`` filters — the plain-collection cache key does not
  reflect those, so filtered requests fall back.
- All covering tiles must be present; partial coverage falls back rather than
  silently dropping features.
"""

import asyncio
import logging
import uuid
from typing import Any, Dict, List, Optional

from dynastore.modules.concurrency import run_in_thread
from dynastore.tools.discovery import get_protocol
from dynastore.extensions.maps.renderer import reproject_bbox_epsg

logger = logging.getLogger(__name__)

# Guarded import: the maps extension must load even when the tiles module is not
# installed. Any failure here disables the fast path (helper returns None).
_TILES_IMPORTS_OK = True
try:
    from dynastore.modules.tiles.tiles_module import TileStorageProtocol
    from dynastore.modules.tiles.tiles_config import cache_on_demand_enabled
    from dynastore.modules.tiles.tiles_engine import _resolve_tms
    from dynastore.modules.tiles.tile_cache_sync import _tiles_for_bbox
except Exception:  # pragma: no cover - only where the tiles module is absent
    _TILES_IMPORTS_OK = False

# The GDAL MVT driver georeferences a tile from z/x/y assuming WebMercatorQuad
# (EPSG:3857), so the fast path is limited to that TMS.
_MVT_TMS_ID = "WebMercatorQuad"
MVT_TILE_SRID = 3857
# Beyond this many covering tiles, per-tile fetch+decode stops beating a single
# PostGIS render — fall back instead.
_MAX_COVER_TILES = 64


async def try_mvt_cache_layers(
    *,
    catalog_id: str,
    collection_id: str,
    bbox: List[float],
    bbox_srid: int,
    width: int,
    height: int,
    max_tiles: int = _MAX_COVER_TILES,
) -> Optional[List[Dict[str, Any]]]:
    """Return ``[{"geom": <wkb>}, ...]`` in EPSG:3857 from cached MVT tiles.

    Returns ``None`` to mean "fall back to the source render" for any reason:
    tiles module absent, caching disabled, TMS unavailable, too many covering
    tiles, a covering tile missing, or a decode failure.
    """
    if not _TILES_IMPORTS_OK:
        return None
    provider = get_protocol(TileStorageProtocol)
    if provider is None:
        return None
    if not await _cache_enabled(catalog_id, collection_id):
        return None
    try:
        tms = await _resolve_tms(catalog_id, _MVT_TMS_ID, morecantile_compatible=True)
    except Exception as exc:  # morecantile missing, unknown TMS, etc.
        logger.debug("mvt fast path: TMS resolution failed: %s", exc)
        return None
    if tms is None or not hasattr(tms, "tiles"):
        return None

    cover = _covering_tiles(tms, bbox, bbox_srid, width, height, max_tiles)
    if not cover:
        return None

    try:
        blobs = await asyncio.gather(
            *[
                provider.get_tile(catalog_id, collection_id, _MVT_TMS_ID, z, x, y, "mvt")
                for (z, x, y) in cover
            ]
        )
    except Exception as exc:
        logger.debug("mvt fast path: tile fetch failed: %s", exc)
        return None
    if any(b is None for b in blobs):
        # Partial coverage would silently drop features — render from source.
        return None

    tiles = [(z, x, y, b) for (z, x, y), b in zip(cover, blobs)]
    try:
        layers = await run_in_thread(_decode_tiles_to_wkb, tiles)
    except Exception as exc:
        logger.warning("mvt fast path: decode failed, falling back: %s", exc)
        return None
    if layers:
        logger.info(
            "mvt fast path: served /map from cache catalog=%s collection=%s "
            "z=%s tiles=%d features=%d",
            catalog_id, collection_id, cover[0][0], len(cover), len(layers),
        )
    return layers or None


async def _cache_enabled(catalog_id: str, collection_id: str) -> bool:
    """Honour the operator's per-catalog/collection ``cache_on_demand`` intent."""
    return await cache_on_demand_enabled(catalog_id, collection_id)


def _covering_tiles(
    tms: Any,
    bbox: List[float],
    bbox_srid: int,
    width: int,
    height: int,
    max_tiles: int,
) -> List[tuple]:
    """Pick a zoom for the requested resolution and list covering ``(z, x, y)``.

    Returns ``[]`` when unusable: bad bbox, projection failure, or more than
    ``max_tiles`` covering tiles.
    """
    lonlat = reproject_bbox_epsg(bbox, bbox_srid, 4326)  # for tms.tiles()
    merc = reproject_bbox_epsg(bbox, bbox_srid, MVT_TILE_SRID)  # for pixel sizing
    if lonlat is None or merc is None:
        return []
    w, s, e, n = lonlat
    res = max(
        (merc[2] - merc[0]) / max(width, 1),
        (merc[3] - merc[1]) / max(height, 1),
    )
    if res <= 0:
        return []
    try:
        z = max(0, int(tms.zoom_for_res(res)))
    except Exception as exc:
        logger.debug("mvt fast path: tile coverage computation failed: %s", exc)
        return []
    cover = [
        (z, x, y)
        for (_tms_id, _z, x, y) in _tiles_for_bbox((w, s, e, n), _MVT_TMS_ID, z)
    ]
    if not cover or len(cover) > max_tiles:
        return []
    return cover


def _decode_tiles_to_wkb(tiles: List[tuple]) -> List[Dict[str, Any]]:
    """Decode cached MVT tile blobs into EPSG:3857 WKB geometries via GDAL.

    Each ``(z, x, y, blob)`` is written to ``/vsimem`` and opened with the OGR
    MVT driver, whose ``X``/``Y``/``Z`` open options georeference the tile-local
    coordinates to EPSG:3857 world coordinates.
    """
    from osgeo import gdal

    out: List[Dict[str, Any]] = []
    for i, (z, x, y, blob) in enumerate(tiles):
        if not blob:
            continue
        # /vsimem is process-global and this runs in a shared thread pool
        # (run_in_thread), so two concurrent requests decoding the same tile
        # at the same list index would otherwise collide on the same path.
        vpath = f"/vsimem/maps_mvt_{z}_{x}_{y}_{i}_{uuid.uuid4().hex}.pbf"
        gdal.FileFromMemBuffer(vpath, blob)
        ds = None
        try:
            ds = gdal.OpenEx(
                vpath,
                allowed_drivers=["MVT"],
                open_options=[f"X={x}", f"Y={y}", f"Z={z}"],
            )
            if ds is None:
                continue
            for li in range(ds.GetLayerCount()):
                lyr = ds.GetLayer(li)
                lyr.ResetReading()
                for feat in lyr:
                    g = feat.GetGeometryRef()
                    if g is not None:
                        out.append({"geom": bytes(g.ExportToWkb())})
        finally:
            ds = None
            gdal.Unlink(vpath)
    return out
