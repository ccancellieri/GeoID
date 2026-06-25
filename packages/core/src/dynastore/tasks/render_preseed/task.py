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

"""``render_preseed`` task — bounded-zoom durable tile cache fill.

Drains a ``render_preseed`` obligation enqueued by
``modules/renders/preseed_sync.enqueue_render_preseed_task`` on
``AFTER_ASSET_CREATION``.  Dispatches to:

- **raster**: ``modules/renders/engine.render_cog_tile`` (single-band COG
  via rio-tiler); cache key via ``modules/renders/config.build_render_cache_key``
  (internal collection id + style_id, never external_id).
- **vector**: ``modules/tiles/tiles_db.get_features_as_mvt_filtered`` (PostGIS
  MVT); saved via ``TileStorageProtocol.save_tile``.

The zoom range ``[min_zoom, max_zoom]`` from the payload is enforced here
and logged at INFO level — both the seeded range and any skipped levels.
The full pyramid is never rendered eagerly; ``RenderPreseedConfig`` ensures
the obligation is bounded before it reaches the task.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from dynastore.tasks.protocols import TaskProtocol
from dynastore.modules.tasks.models import TaskPayload

from .models import RenderPreseedInputs

logger = logging.getLogger(__name__)


class RenderPreseedTask(TaskProtocol):
    """Drain a render-preseed obligation: fill the render cache for a bounded zoom range."""

    task_type = "render_preseed"
    priority = 30

    payload_model = RenderPreseedInputs

    def is_available(self) -> bool:
        return True

    async def run(self, payload: TaskPayload) -> Dict[str, Any]:  # type: ignore[override]
        inputs_raw = getattr(payload, "inputs", None) or {}
        if isinstance(inputs_raw, RenderPreseedInputs):
            inputs = inputs_raw
        else:
            inputs = RenderPreseedInputs.model_validate(inputs_raw)

        logger.info(
            "render_preseed: starting %s preseed for %s/%s zoom=%d..%d tms=%s style=%s",
            inputs.producer_kind,
            inputs.catalog_id,
            inputs.collection_id,
            inputs.min_zoom,
            inputs.max_zoom,
            inputs.tms_ids,
            inputs.style_id,
        )

        if inputs.min_zoom > inputs.max_zoom:
            logger.warning(
                "render_preseed: min_zoom (%d) > max_zoom (%d) for %s/%s — "
                "nothing to seed",
                inputs.min_zoom, inputs.max_zoom,
                inputs.catalog_id, inputs.collection_id,
            )
            return {"seeded": 0, "skipped": 0, "errors": 0, "zoom_range": []}

        if inputs.producer_kind == "raster":
            return await self._seed_raster(inputs)
        else:
            return await self._seed_vector(inputs)

    # ------------------------------------------------------------------
    # Raster path
    # ------------------------------------------------------------------

    async def _seed_raster(self, inputs: RenderPreseedInputs) -> Dict[str, Any]:
        """Render single-band COG tiles via rio-tiler and save to the render cache."""
        try:
            from dynastore.modules.renders.engine import render_cog_tile
        except ImportError as exc:
            raise RuntimeError(
                "render_preseed: rio-tiler not installed — raster preseed "
                "requires the renders extension. "
                f"Install 'dynastore-ext-renders'. Original error: {exc}"
            ) from exc

        from dynastore.modules.renders.config import (
            RenderCachingConfig,
            build_render_cache_key,
        )
        from dynastore.modules import get_protocol
        from dynastore.models.protocols import ConfigsProtocol
        from dynastore.modules.tiles.tiles_module import TileStorageProtocol
        from dynastore.modules.concurrency import run_in_thread

        cfg_svc = get_protocol(ConfigsProtocol)
        if cfg_svc is None:
            raise RuntimeError(
                "render_preseed: ConfigsProtocol unavailable — cannot load "
                "render cache config"
            )

        render_cfg: RenderCachingConfig = await cfg_svc.get_config(
            RenderCachingConfig, inputs.catalog_id, inputs.collection_id
        )
        if not isinstance(render_cfg, RenderCachingConfig):
            render_cfg = RenderCachingConfig()

        if not render_cfg.cache_enabled:
            logger.info(
                "render_preseed: cache_enabled=False for %s/%s — skipping raster preseed",
                inputs.catalog_id, inputs.collection_id,
            )
            return {"seeded": 0, "skipped": 0, "errors": 0, "reason": "cache_disabled"}

        provider = get_protocol(TileStorageProtocol)
        if provider is None:
            raise RuntimeError(
                "render_preseed: no TileStorageProtocol registered — "
                "cannot save rendered tiles"
            )

        # Resolve COG href: read the triggering asset's href from the first
        # item in the collection (same approach as RendersService._get_first_item).
        cog_href = await self._resolve_cog_href(
            inputs.catalog_id, inputs.collection_id
        )
        if not cog_href:
            logger.warning(
                "render_preseed: no COG href found for %s/%s — skipping raster preseed",
                inputs.catalog_id, inputs.collection_id,
            )
            return {"seeded": 0, "skipped": 0, "errors": 0, "reason": "no_cog_href"}

        # Colormap: load from styles service if available; fall back to None
        # (raw pixel values rendered as greyscale).
        colormap = await self._resolve_colormap(
            inputs.catalog_id, inputs.collection_id, inputs.style_id or "default"
        )

        zooms = list(range(inputs.min_zoom, inputs.max_zoom + 1))
        logger.info(
            "render_preseed: raster seeding zoom range %s for %s/%s",
            zooms, inputs.catalog_id, inputs.collection_id,
        )

        seeded = 0
        skipped = 0
        errors = 0

        try:
            import morecantile  # type: ignore[import]
        except ImportError as exc:
            raise RuntimeError(
                "render_preseed: morecantile not installed — required for tile "
                f"coordinate enumeration. Original error: {exc}"
            ) from exc

        style_id = inputs.style_id or "default"

        for tms_id in inputs.tms_ids:
            try:
                tms = morecantile.tms.get(tms_id)
            except Exception as exc:
                logger.warning(
                    "render_preseed: unknown TMS %r for %s/%s: %s — skipping TMS",
                    tms_id, inputs.catalog_id, inputs.collection_id, exc,
                )
                continue

            for z in zooms:
                # World-extent seed — callers who want a bbox-bounded seed
                # should configure RenderPreseedConfig.max_zoom conservatively
                # rather than adding a separate bbox parameter here.
                try:
                    tiles = list(tms.tiles(-180, -85.051129, 180, 85.051129, zooms=[z]))
                except Exception as exc:
                    logger.warning(
                        "render_preseed: tile enumeration failed z=%d tms=%r: %s",
                        z, tms_id, exc,
                    )
                    continue

                for tile in tiles:
                    cache_key = build_render_cache_key(
                        render_cfg.key_prefix,
                        inputs.collection_id,
                        style_id,
                        tms_id,
                        z,
                        tile.x,
                        tile.y,
                        "png",
                    )
                    try:
                        tile_bytes: bytes = await run_in_thread(
                            render_cog_tile,
                            cog_href,
                            z,
                            tile.x,
                            tile.y,
                            colormap=colormap,
                            output_format="PNG",
                        )
                        await provider.save_tile(
                            inputs.catalog_id,
                            cache_key,
                            tms_id,
                            z,
                            tile.x,
                            tile.y,
                            tile_bytes,
                            "png",
                        )
                        seeded += 1
                    except Exception as exc:
                        logger.warning(
                            "render_preseed: raster tile z=%d/%d/%d failed for "
                            "%s/%s: %s",
                            z, tile.x, tile.y,
                            inputs.catalog_id, inputs.collection_id, exc,
                        )
                        errors += 1

        logger.info(
            "render_preseed: raster done %s/%s zoom=%d..%d "
            "seeded=%d skipped=%d errors=%d",
            inputs.catalog_id, inputs.collection_id,
            inputs.min_zoom, inputs.max_zoom,
            seeded, skipped, errors,
        )
        return {
            "producer_kind": "raster",
            "seeded": seeded,
            "skipped": skipped,
            "errors": errors,
            "zoom_range": [inputs.min_zoom, inputs.max_zoom],
        }

    # ------------------------------------------------------------------
    # Vector path
    # ------------------------------------------------------------------

    async def _seed_vector(self, inputs: RenderPreseedInputs) -> Dict[str, Any]:
        """Generate MVT tiles via PostGIS and save via TileStorageProtocol."""
        from dynastore.modules import get_protocol
        from dynastore.modules.tiles.tiles_module import TileStorageProtocol
        from dynastore.modules.tiles import tiles_module, tiles_db
        from dynastore.modules.tiles.tms_definitions import BUILTIN_TILE_MATRIX_SETS
        from dynastore.modules.db_config.query_executor import managed_transaction
        from dynastore.tools.protocol_helpers import get_engine
        from dynastore.tools.geospatial import SimplificationAlgorithm

        try:
            import morecantile  # type: ignore[import]
        except ImportError as exc:
            raise RuntimeError(
                "render_preseed: morecantile not installed — required for MVT "
                f"tile enumeration. Original error: {exc}"
            ) from exc

        engine = get_engine()
        if engine is None:
            raise RuntimeError(
                "render_preseed: DB engine unavailable — cannot generate MVT tiles"
            )

        provider = get_protocol(TileStorageProtocol)
        if provider is None:
            raise RuntimeError(
                "render_preseed: no TileStorageProtocol registered — "
                "cannot save MVT tiles"
            )

        meta = await tiles_module.get_tile_resolution_params(
            inputs.catalog_id, inputs.collection_id
        )
        if not meta:
            logger.warning(
                "render_preseed: no tile resolution params for %s/%s — "
                "skipping vector preseed",
                inputs.catalog_id, inputs.collection_id,
            )
            return {
                "producer_kind": "vector",
                "seeded": 0,
                "skipped": 0,
                "errors": 0,
                "reason": "no_tile_meta",
            }

        zooms = list(range(inputs.min_zoom, inputs.max_zoom + 1))
        logger.info(
            "render_preseed: vector seeding zoom range %s for %s/%s",
            zooms, inputs.catalog_id, inputs.collection_id,
        )

        seeded = 0
        skipped = 0
        errors = 0

        for tms_id in inputs.tms_ids:
            tms_def = await tiles_module.get_custom_tms(
                catalog_id=inputs.catalog_id, tms_id=tms_id
            )
            if not tms_def:
                tms_def = BUILTIN_TILE_MATRIX_SETS.get(tms_id)
            if not tms_def:
                try:
                    tms_def = morecantile.tms.get(tms_id)
                except Exception as exc:
                    logger.warning(
                        "render_preseed: unknown TMS %r: %s — skipping",
                        tms_id, exc,
                    )
                    continue

            # Ensure morecantile-compatible object
            if tms_def and not hasattr(tms_def, "tiles"):
                try:
                    tms_dict = (
                        tms_def.model_dump(exclude_none=True)
                        if hasattr(tms_def, "model_dump")
                        else tms_def
                    )
                    tms_def = morecantile.TileMatrixSet.model_validate(tms_dict)
                except Exception as exc:
                    logger.warning(
                        "render_preseed: TMS %r conversion failed: %s — skipping",
                        tms_id, exc,
                    )
                    continue

            target_srid = 3857
            if hasattr(tms_def, "crs"):
                try:
                    target_srid = await tiles_module.resolve_srid(
                        engine, str(tms_def.crs), inputs.catalog_id
                    )
                except Exception as exc:
                    logger.warning(
                        "render_preseed: SRID resolution failed for %r: %s — "
                        "falling back to 3857",
                        tms_def.crs, exc,
                    )

            async with managed_transaction(engine) as conn:
                for z in zooms:
                    try:
                        tiles = list(
                            tms_def.tiles(
                                -180, -85.051129, 180, 85.051129, zooms=[z]
                            )
                        )
                    except Exception as exc:
                        logger.warning(
                            "render_preseed: tile enumeration failed z=%d tms=%r: %s",
                            z, tms_id, exc,
                        )
                        continue

                    for tile in tiles:
                        try:
                            mvt_data = await tiles_db.get_features_as_mvt_filtered(
                                conn=conn,
                                resolved_collections=[meta],
                                tms_def=tms_def,
                                target_srid=target_srid,
                                z=str(z),
                                x=tile.x,
                                y=tile.y,
                                simplification=None,
                                simplification_algorithm=SimplificationAlgorithm.TOPOLOGY_PRESERVING,
                            )
                            if mvt_data:
                                await provider.save_tile(
                                    catalog_id=inputs.catalog_id,
                                    collection_id=inputs.collection_id,
                                    tms_id=tms_id,
                                    z=z,
                                    x=tile.x,
                                    y=tile.y,
                                    data=mvt_data,
                                    format="mvt",
                                )
                                seeded += 1
                            else:
                                skipped += 1
                        except Exception as exc:
                            logger.warning(
                                "render_preseed: vector tile z=%d/%d/%d failed "
                                "for %s/%s: %s",
                                z, tile.x, tile.y,
                                inputs.catalog_id, inputs.collection_id, exc,
                            )
                            errors += 1

        logger.info(
            "render_preseed: vector done %s/%s zoom=%d..%d "
            "seeded=%d skipped=%d errors=%d",
            inputs.catalog_id, inputs.collection_id,
            inputs.min_zoom, inputs.max_zoom,
            seeded, skipped, errors,
        )
        return {
            "producer_kind": "vector",
            "seeded": seeded,
            "skipped": skipped,
            "errors": errors,
            "zoom_range": [inputs.min_zoom, inputs.max_zoom],
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _resolve_cog_href(
        self, catalog_id: str, collection_id: str
    ) -> Optional[str]:
        """Return the COG asset href from the collection's first item, or None."""
        try:
            from dynastore.models.protocols.item_query import ItemQueryProtocol
            from dynastore.modules import get_protocol
            from dynastore.models.query_builder import QueryRequest
            from dynastore.extensions.ogc_base import ogc_asset_href

            items_svc = get_protocol(ItemQueryProtocol)
            if items_svc is None:
                return None

            result = await items_svc.get_features(
                catalog_id,
                collection_id,
                QueryRequest(limit=1),
            )
            items: List[Any] = getattr(result, "features", None) or []
            if not items:
                return None

            try:
                return ogc_asset_href(items[0], error_detail="")
            except Exception:
                return None
        except Exception as exc:
            logger.debug(
                "render_preseed: COG href resolution failed for %s/%s: %s",
                catalog_id, collection_id, exc,
            )
            return None

    async def _resolve_colormap(
        self, catalog_id: str, collection_id: str, style_id: str
    ) -> Optional[Any]:
        """Return the colormap for ``style_id``, or ``None`` on any failure."""
        try:
            from dynastore.models.protocols import StylesProtocol  # type: ignore[attr-defined]
            from dynastore.modules import get_protocol
            from dynastore.modules.renders.colormap import parse_sld_colormap
            from dynastore.extensions.renders.renders_service import RendersService  # type: ignore[import]

            styles_svc = get_protocol(StylesProtocol)
            if styles_svc is None:
                return None

            style_obj = await styles_svc.get_style(catalog_id, collection_id, style_id)
            if not style_obj:
                return None

            renders_svc = get_protocol(RendersService)
            if renders_svc is None:
                return None

            sld_body = renders_svc._extract_sld_body(style_obj)  # type: ignore[union-attr]
            if not sld_body:
                return None

            return parse_sld_colormap(sld_body) or None
        except Exception as exc:
            logger.debug(
                "render_preseed: colormap resolution failed for %s/%s style=%r: %s",
                catalog_id, collection_id, style_id, exc,
            )
            return None
