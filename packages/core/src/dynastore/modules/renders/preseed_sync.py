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

"""Durable render-preseed enqueue helper and event subscriber.

``enqueue_render_preseed_task`` is called on the ``AFTER_ASSET_CREATION`` event
path and inserts a ``render_preseed`` task row into the unified task queue.
The task worker drains off the request path and fills the render cache for
the configured zoom range without blocking the ingestion write.

Design mirrors ``modules/tiles/tile_cache_sync.enqueue_tile_invalidation_task``:
- Capability-gated: skipped when ``RenderPreseedConfig.enabled`` is ``False``.
- Dedup key per (catalog, collection, producer_kind) prevents duplicate
  obligations when multiple assets are registered in quick succession.
- Never raises out — a pre-seed failure must not break asset creation.

``register_render_preseed_subscriber`` wires the subscriber to
``CatalogEventType.AFTER_ASSET_CREATION``.  It is called from
``CatalogModule.lifespan`` alongside the other event subscribers.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from dynastore.modules.catalog.event_service import CatalogEventType, async_event_listener

logger = logging.getLogger(__name__)


async def enqueue_render_preseed_task(
    catalog_id: str,
    collection_id: str,
    asset_id: str,
    *,
    producer_kind: str,
    engine: Any,
    schema: str,
    caller_id: Optional[str] = None,
) -> bool:
    """Enqueue a durable ``render_preseed`` obligation for one collection.

    Inserts a ``render_preseed`` task row using the same ``create_task`` path
    used by every other durable obligation (tiles_invalidate, index_propagation,
    etc.).  The task worker drains the row off the request path.

    ``producer_kind`` is ``"raster"`` or ``"vector"``.  The worker dispatches
    to ``render_cog_tile`` (raster) or the existing MVT path (vector).

    ``schema`` is the tenant physical PG schema (``catalog_id`` column value in
    the tasks table) — the same value passed to every ``create_task`` call.

    Dedup key: ``render-preseed:{catalog_id}:{collection_id}:{producer_kind}``
    — one pending obligation per (catalog, collection, kind) is enough; a new
    asset registration while a preseed is already queued is a no-op.

    Returns ``True`` when a new task row was inserted, ``False`` on a dedup hit
    or when the preseed is disabled / not applicable.  Never raises.
    """
    try:
        from dynastore.modules.renders.config import RenderPreseedConfig
        from dynastore.modules import get_protocol
        from dynastore.models.protocols import ConfigsProtocol

        cfg_svc = get_protocol(ConfigsProtocol)
        if cfg_svc is None:
            logger.debug(
                "render_preseed: ConfigsProtocol unavailable — skipping preseed "
                "for %s/%s (%s)", catalog_id, collection_id, producer_kind,
            )
            return False

        cfg: RenderPreseedConfig = await cfg_svc.get_config(
            RenderPreseedConfig, catalog_id, collection_id
        )
        if not isinstance(cfg, RenderPreseedConfig) or not cfg.enabled:
            logger.debug(
                "render_preseed: disabled for %s/%s — no obligation enqueued",
                catalog_id, collection_id,
            )
            return False

        if producer_kind == "raster" and not cfg.seed_raster:
            logger.debug(
                "render_preseed: seed_raster=False for %s/%s — skipping raster",
                catalog_id, collection_id,
            )
            return False
        if producer_kind == "vector" and not cfg.seed_vector:
            logger.debug(
                "render_preseed: seed_vector=False for %s/%s — skipping vector",
                catalog_id, collection_id,
            )
            return False

        effective_style_id = cfg.style_id or "default"

        logger.info(
            "render_preseed: enqueuing %s obligation for %s/%s "
            "zoom=%d..%d tms=%s style=%s",
            producer_kind, catalog_id, collection_id,
            cfg.min_zoom, cfg.max_zoom, cfg.tms_ids, effective_style_id,
        )

        from dynastore.modules.tasks import tasks_module
        from dynastore.modules.tasks.models import TaskCreate

        inputs: Dict[str, Any] = {
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "asset_id": asset_id,
            "producer_kind": producer_kind,
            "min_zoom": cfg.min_zoom,
            "max_zoom": cfg.max_zoom,
            "tms_ids": list(cfg.tms_ids),
            "style_id": effective_style_id,
        }

        dedup_key = (
            f"render-preseed:{catalog_id}:{collection_id}:{producer_kind}"
        )

        task = await tasks_module.create_task(
            engine,
            TaskCreate(
                task_type="render_preseed",
                type="task",
                caller_id=caller_id or "system:render_preseed",
                inputs=inputs,
                collection_id=collection_id,
                dedup_key=dedup_key,
            ),
            schema=schema,
            initial_status="PENDING",
        )

        if task is None:
            logger.debug(
                "render_preseed: obligation coalesced (dedup) for %s/%s (%s)",
                catalog_id, collection_id, producer_kind,
            )
            return False

        logger.info(
            "render_preseed: task %s enqueued for %s/%s (%s) zoom=%d..%d",
            task.task_id, catalog_id, collection_id, producer_kind,
            cfg.min_zoom, cfg.max_zoom,
        )
        return True

    except Exception as exc:  # noqa: BLE001 — never break asset creation
        logger.warning(
            "render_preseed: failed to enqueue obligation for %s/%s (%s): %s",
            catalog_id, collection_id, producer_kind, exc,
        )
        return False


class RenderPreseedSubscriber:
    """Async event subscriber that enqueues render-preseed obligations.

    Handles ``AFTER_ASSET_CREATION``.  Determines the producer kind from the
    asset role field (``data``/``coverage`` → raster; vector collections →
    vector).  Skips when the asset role is not pre-seedable.
    """

    @staticmethod
    async def on_asset_creation(
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        asset_id: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        **_kwargs: Any,
    ) -> None:
        if not catalog_id or not collection_id or not asset_id:
            return

        # Determine producer kind from the asset payload.
        # Assets with role "data" or "coverage" are raster COG candidates.
        # Assets with role "features" or no role on a vector collection are
        # vector candidates.  The task itself re-validates at run time.
        role: Optional[str] = None
        if isinstance(payload, dict):
            role = payload.get("role") or payload.get("asset_role")

        if role in ("data", "coverage", None):
            # Default: attempt raster; RenderPreseedConfig.seed_raster guards it.
            producer_kind = "raster"
        elif role == "features":
            producer_kind = "vector"
        else:
            # Non-pre-seedable role (thumbnail, metadata, etc.): skip.
            logger.debug(
                "render_preseed: asset role %r not pre-seedable for %s/%s/%s",
                role, catalog_id, collection_id, asset_id,
            )
            return

        # Resolve engine + schema needed by create_task.
        from dynastore.tools.protocol_helpers import get_engine
        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.modules import get_protocol
        from dynastore.models.driver_context import DriverContext

        engine = get_engine()
        if engine is None:
            logger.warning(
                "render_preseed: DB engine unavailable — skipping preseed for "
                "%s/%s/%s", catalog_id, collection_id, asset_id,
            )
            return

        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            logger.warning(
                "render_preseed: CatalogsProtocol unavailable — skipping preseed "
                "for %s/%s/%s", catalog_id, collection_id, asset_id,
            )
            return

        schema = await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=engine)
        )
        if schema is None:
            logger.warning(
                "render_preseed: cannot resolve schema for catalog %r — skipping",
                catalog_id,
            )
            return

        caller_id = (payload or {}).get("caller_id") if isinstance(payload, dict) else None

        await enqueue_render_preseed_task(
            catalog_id,
            collection_id,
            asset_id,
            producer_kind=producer_kind,
            engine=engine,
            schema=schema,
            caller_id=caller_id,
        )


def register_render_preseed_subscriber() -> None:
    """Wire ``RenderPreseedSubscriber`` to ``AFTER_ASSET_CREATION``.

    Called from ``CatalogModule.lifespan`` alongside other event subscribers.
    Idempotent at the registration site — duplicate registrations would cause
    duplicate dispatches but not data corruption (dedup_key prevents double-
    insert into the tasks table).
    """
    async_event_listener(CatalogEventType.AFTER_ASSET_CREATION)(
        RenderPreseedSubscriber.on_asset_creation
    )
    logger.info(
        "RenderPreseedSubscriber: registered on CatalogEventType.AFTER_ASSET_CREATION"
    )
