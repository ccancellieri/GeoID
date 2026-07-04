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

"""Default-style PNG rendering for VECTOR collections' ``/map/tiles/...`` route.

Registers into core's :class:`~dynastore.modules.tiles.tiles_source.TileSourceProtocol`
registry (format-gated on ``"png"``) so ``tiles_engine.build_render_context``
picks this source instead of ``PostgisTileSource`` when the caller asks for
``format="png"``. This is how a vector map-tile PNG gets rendered without the
tiles extension (or core) ever importing the maps extension: the maps
extension imports core and registers itself, core never imports maps.

Default style only (a fixed, un-symbolized fill/line/point render via
``render_map_image(..., style_record=None, ...)``) — named styles still fall
back to the source render's full feature-attribute path and are out of scope
here.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union

from dynastore.modules.concurrency import run_in_thread
from dynastore.tools.geospatial import SimplificationAlgorithm
from dynastore.modules.tiles.tiles_source import TileSourceProtocol
from dynastore.modules.tiles.tiles_models import TileMatrixSet

from . import maps_db
from .maps_tile_cache import _decode_tiles_to_wkb
from .renderer import render_map_image

logger = logging.getLogger(__name__)

# Guarded — mirrors ``maps_tile_cache``'s gate: the maps extension must still
# load when the tiles module isn't installed. When absent, the MVT-first probe
# is simply skipped and every render falls back to the PostGIS feature fetch.
_TILES_IMPORTS_OK = True
try:
    from dynastore.modules.tiles.tiles_module import TileStorageProtocol
except Exception:  # pragma: no cover - only where the tiles module is absent
    _TILES_IMPORTS_OK = False

_TILE_SIZE = 256
# The GDAL MVT driver (used to decode a cached tile below) georeferences a
# tile assuming WebMercatorQuad — matching ``maps_tile_cache``'s own scope
# note. The MVT-first probe is therefore gated on the requested TMS actually
# being WebMercatorQuad; any other TMS goes straight to the PostGIS fallback.
_WEBMERCATOR_TMS_ID = "WebMercatorQuad"
_WEBMERCATOR_SRID = 3857


def _tile_bbox(
    tms_def: Union[TileMatrixSet, Any], z: str, x: int, y: int
) -> Optional[List[float]]:
    """Return ``[minx, miny, maxx, maxy]`` for tile ``z/x/y`` in ``tms_def``'s
    native CRS.

    Mirrors ``tiles_db._calculate_tile_envelope_wkb``'s topLeft-origin,
    y-increases-downward convention (the OGC TMS default) rather than
    importing that core-private helper, so row 0 is north with no y-flip.
    """
    matrix = next((m for m in tms_def.tileMatrices if str(m.id) == str(z)), None)
    if matrix is None:
        return None
    origin = matrix.pointOfOrigin
    tile_span_x = matrix.tileWidth * matrix.cellSize
    tile_span_y = matrix.tileHeight * matrix.cellSize

    min_x = origin[0] + (x * tile_span_x)
    max_x = min_x + tile_span_x
    max_y = origin[1] - (y * tile_span_y)
    min_y = max_y - tile_span_y
    return [min_x, min_y, max_x, max_y]


class MapsPngTileSource(TileSourceProtocol):
    """Renders default-style PNG map tiles for VECTOR collections.

    ``supports`` narrows on ``format == "png"`` so ``PostgisTileSource``
    (mvt/pbf) keeps being selected for every existing caller; only a
    ``build_render_context(..., format="png")`` call routes here.

    ``render_tile`` tries the already-cached WebMercatorQuad MVT tile for the
    exact same coordinate first (decoded via the shared GDAL MVT decoder), and
    falls back to a direct PostGIS feature fetch otherwise. Either way the
    features are rendered through the same default-style GDAL renderer the
    vector ``/map`` mosaic endpoint uses.
    """

    def supports(self, driver: Any, format: str = "mvt") -> bool:
        return (
            format == "png"
            and getattr(driver, "_get_effective_driver_config", None) is not None
        )

    async def render_tile(
        self,
        conn: Any,
        *,
        resolved_collections: List[Dict[str, Any]],
        tms_def: Union[TileMatrixSet, Any],
        target_srid: int,
        z: str,
        x: int,
        y: int,
        format: str = "png",
        datetime_str: Optional[str] = None,
        cql_filter: Optional[str] = None,
        subset_params: Optional[Dict[str, Any]] = None,
        simplification: Optional[float] = None,
        simplification_algorithm: SimplificationAlgorithm = SimplificationAlgorithm.TOPOLOGY_PRESERVING,
    ) -> Optional[bytes]:
        if not resolved_collections:
            return None
        # build_render_context resolves this source's driver from the FIRST
        # collection id, and the vector map-tile route always calls it with a
        # single-element collection list — mirroring that call-site shape.
        meta = resolved_collections[0]
        catalog_id = meta.get("catalog_id")
        collection_id = meta.get("collection_id")
        if not catalog_id or not collection_id:
            return None

        tile_bbox = _tile_bbox(tms_def, z, x, y)
        if tile_bbox is None:
            logger.warning(
                "MapsPngTileSource: no matrix %r in supplied TMS for %s/%s",
                z, catalog_id, collection_id,
            )
            return None

        layers_data: Optional[List[Dict[str, Any]]] = None
        render_srid = target_srid
        tms_id = getattr(tms_def, "id", None)

        # MVT-first: reuse the cached WebMercatorQuad MVT tile for this exact
        # coordinate when present — the same plain collection_id cache-id the
        # vector MVT endpoint writes under (no params_hash), so this only ever
        # hits on an unfiltered, default-cache-key read.
        if (
            _TILES_IMPORTS_OK
            and tms_id == _WEBMERCATOR_TMS_ID
            and datetime_str is None
            and cql_filter is None
            and not subset_params
        ):
            from dynastore.tools.discovery import get_protocol

            provider = get_protocol(TileStorageProtocol)
            if provider is not None:
                z_int, x_int, y_int = int(z), int(x), int(y)
                try:
                    blob = await provider.get_tile(
                        catalog_id, collection_id, _WEBMERCATOR_TMS_ID,
                        z_int, x_int, y_int, "mvt",
                    )
                except Exception as exc:
                    logger.debug("MapsPngTileSource: MVT-cache probe failed: %s", exc)
                    blob = None
                if blob:
                    try:
                        decoded = await run_in_thread(
                            _decode_tiles_to_wkb, [(z_int, x_int, y_int, blob)]
                        )
                    except Exception as exc:
                        logger.warning(
                            "MapsPngTileSource: MVT decode failed, falling back: %s", exc
                        )
                        decoded = None
                    if decoded:
                        layers_data = decoded
                        render_srid = _WEBMERCATOR_SRID

        if layers_data is None:
            # #703: no connection is held across the CPU-bound render below —
            # this query runs on the connection the caller already acquired
            # per-request, released by the caller before/after this returns.
            try:
                layers_data = await maps_db.get_features_for_rendering(
                    conn=conn,
                    schema=catalog_id,
                    collections=[collection_id],
                    bbox=tile_bbox,
                    crs=f"EPSG:{target_srid}",
                    width=_TILE_SIZE,
                    height=_TILE_SIZE,
                    bbox_srid=target_srid,
                    datetime_str=datetime_str,
                    subset_params=subset_params,
                )
            except ValueError as exc:
                logger.warning(
                    "MapsPngTileSource: render skipped (query failed) %s/%s "
                    "z=%s x=%s y=%s: %s",
                    catalog_id, collection_id, z, x, y, exc,
                )
                return None
            # Features come back in the collection's native storage SRID
            # (get_features_for_rendering never reprojects the geometry
            # column itself), matching maps_service._resolve_target_srid's
            # convention for the same renderer call.
            render_srid = meta.get("source_srid") or target_srid

        if layers_data is None:
            return None
        if not layers_data:
            # Query ran and confirmed zero features — cacheable empty tile,
            # distinct from the `None` attempt-failure returns above.
            return b""

        return await run_in_thread(
            render_map_image,
            _TILE_SIZE,
            _TILE_SIZE,
            tile_bbox,
            f"EPSG:{target_srid}",
            render_srid,
            layers_data,
            None,  # style_record=None => default style
            True,
            None,
            target_srid,
        )
