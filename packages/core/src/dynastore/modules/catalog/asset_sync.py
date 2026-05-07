"""
Asset entity sync — event-driven fan-out to AssetIndexer drivers.

``AssetEntitySyncSubscriber`` listens for ``CatalogEventType.ASSET_*`` events
emitted by ``AssetService`` and dispatches the row write/delete to every
driver registered under ``AssetRoutingConfig.operations[INDEX]`` (auto-
augmented with discoverable ``AssetIndexer`` implementors such as
``AssetElasticsearchDriver``).

This collapses the prior dual-write race where the ES driver received writes
from both the routing-config fan-out and a private listener block: the row
goes to the primary WRITE driver synchronously inside ``AssetService``; INDEX
fan-out happens via this subscriber, fed by the events outbox so failures
are replayable.
"""

import asyncio
import logging
from typing import Any, Dict, Optional

from dynastore.modules import get_protocol
from dynastore.modules.catalog.event_service import (
    CatalogEventType,
    async_event_listener,
)

logger = logging.getLogger(__name__)


class AssetEntitySyncSubscriber:
    """Async event subscribers that drive ``AssetIndexer`` fan-out."""

    @staticmethod
    async def on_asset_upsert(
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        asset_id: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        **_kwargs,
    ) -> None:
        if not catalog_id or not asset_id:
            return

        from dynastore.modules.storage.router import get_asset_index_drivers
        from dynastore.modules.storage.routing_config import FailurePolicy

        try:
            indexers = await get_asset_index_drivers(catalog_id, collection_id)
        except Exception as exc:
            logger.warning(
                "AssetEntitySync: index-driver resolution failed for %s/%s: %s",
                catalog_id, asset_id, exc,
            )
            return
        if not indexers:
            return

        doc = dict(payload) if isinstance(payload, dict) else {}
        doc.setdefault("asset_id", asset_id)
        doc.setdefault("catalog_id", catalog_id)
        if collection_id:
            doc.setdefault("collection_id", collection_id)

        results = await asyncio.gather(
            *(r.driver.index_asset(catalog_id, doc) for r in indexers),
            return_exceptions=True,
        )
        for r, result in zip(indexers, results):
            if isinstance(result, BaseException):
                level = (
                    logger.error if r.on_failure == FailurePolicy.FATAL
                    else logger.warning
                )
                level(
                    "AssetEntitySync: indexer '%s' index_asset failed for "
                    "%s/%s: %s", r.driver_ref, catalog_id, asset_id, result,
                )

    @staticmethod
    async def on_asset_delete(
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        asset_id: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        **_kwargs,
    ) -> None:
        if not asset_id:
            _val = (payload if isinstance(payload, dict) else {}).get("asset_id")
            asset_id = str(_val) if _val is not None else None
        if not catalog_id or not asset_id:
            return

        from dynastore.modules.storage.router import get_asset_index_drivers
        from dynastore.modules.storage.routing_config import FailurePolicy

        try:
            indexers = await get_asset_index_drivers(catalog_id, collection_id)
        except Exception as exc:
            logger.warning(
                "AssetEntitySync: index-driver resolution failed for %s/%s: %s",
                catalog_id, asset_id, exc,
            )
            return
        if not indexers:
            return

        results = await asyncio.gather(
            *(r.driver.delete_asset(catalog_id, asset_id) for r in indexers),
            return_exceptions=True,
        )
        for r, result in zip(indexers, results):
            if isinstance(result, BaseException):
                level = (
                    logger.error if r.on_failure == FailurePolicy.FATAL
                    else logger.warning
                )
                level(
                    "AssetEntitySync: indexer '%s' delete_asset failed for "
                    "%s/%s: %s", r.driver_ref, catalog_id, asset_id, result,
                )


def register_asset_entity_sync_subscriber() -> None:
    """Register ``AssetEntitySyncSubscriber`` on the global event bus.

    Wires ``CatalogEventType.ASSET_*`` to the upsert / delete handlers as
    async listeners (background dispatch, decoupled from the primary write).
    Idempotent at the registration site — duplicate registrations would
    cause duplicate dispatches but not data corruption.
    """
    async_event_listener(CatalogEventType.ASSET_CREATION)(
        AssetEntitySyncSubscriber.on_asset_upsert
    )
    async_event_listener(CatalogEventType.ASSET_UPDATE)(
        AssetEntitySyncSubscriber.on_asset_upsert
    )
    async_event_listener(CatalogEventType.ASSET_DELETION)(
        AssetEntitySyncSubscriber.on_asset_delete
    )
    async_event_listener(CatalogEventType.ASSET_HARD_DELETION)(
        AssetEntitySyncSubscriber.on_asset_delete
    )
    logger.info("AssetEntitySyncSubscriber: registered on CatalogEventType.ASSET_*")


class ItemReverseCascadeSubscriber:
    """Reverse cascade — delete items that reference a hard-deleted asset.

    Reads the ``propagate`` flag from the event payload (set by
    ``AssetService.delete_assets`` when its caller passes
    ``propagate=True``). If absent or False, the handler is a no-op —
    callers must opt in. Errors are logged and never raised so a
    bookkeeping cleanup never blocks asset deletion completion.

    The dependency is encoded in items' ``extra_metadata->'assets'``
    JSONB column, not in ``asset_references`` — the latter only carries
    collection-level back-links today. See
    ``ItemService.list_items_by_asset_id_query``.
    """

    @staticmethod
    async def on_asset_hard_delete(
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        asset_id: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        **_kwargs,
    ) -> None:
        del _kwargs
        if not catalog_id or not asset_id:
            return
        if not isinstance(payload, dict) or not payload.get("propagate"):
            return

        from dynastore.modules.catalog.catalog_service import CatalogService
        from dynastore.modules.db_config.query_executor import managed_transaction

        catalog_svc = get_protocol(CatalogService)
        if catalog_svc is None:
            logger.warning(
                "ItemReverseCascade: CatalogService unavailable for %s/%s",
                catalog_id, asset_id,
            )
            return

        target_collection = collection_id or payload.get("collection_id")
        if not target_collection:
            logger.debug(
                "ItemReverseCascade: skipping catalog-level asset %s/%s "
                "(no collection scope to walk)",
                catalog_id, asset_id,
            )
            return

        try:
            phys_schema = await catalog_svc.resolve_physical_schema(catalog_id)
        except Exception as exc:
            logger.warning(
                "ItemReverseCascade: schema resolve failed for %s: %s",
                catalog_id, exc,
            )
            return
        if not phys_schema:
            return

        try:
            list_q = catalog_svc._item_svc.list_items_by_asset_id_query
        except AttributeError:
            logger.warning(
                "ItemReverseCascade: ItemService missing list_items_by_asset_id_query"
            )
            return

        try:
            async with managed_transaction(catalog_svc.engine) as conn:
                rows = await list_q.execute(
                    conn,
                    catalog_id=phys_schema,
                    collection_id=target_collection,
                    asset_id=asset_id,
                )
        except Exception as exc:
            logger.warning(
                "ItemReverseCascade: list query failed for %s/%s/%s: %s",
                catalog_id, target_collection, asset_id, exc,
            )
            return

        if not rows:
            return

        deleted = 0
        for row in rows:
            external_id = row.get("external_id") if isinstance(row, dict) else None
            if not external_id:
                continue
            try:
                await catalog_svc.delete_item(
                    catalog_id, target_collection, external_id,
                )
                deleted += 1
            except Exception as exc:
                logger.warning(
                    "ItemReverseCascade: delete_item failed for "
                    "%s/%s/%s (asset %s): %s",
                    catalog_id, target_collection, external_id, asset_id, exc,
                )

        logger.info(
            "ItemReverseCascade: deleted %d/%d item(s) linked to asset %s/%s/%s",
            deleted, len(rows), catalog_id, target_collection, asset_id,
        )


def register_item_reverse_cascade_subscriber() -> None:
    """Register ``ItemReverseCascadeSubscriber`` on the global event bus.

    Wires ``ASSET_HARD_DELETION`` only — soft-deletes preserve the row
    so item links must remain queryable.
    """
    async_event_listener(CatalogEventType.ASSET_HARD_DELETION)(
        ItemReverseCascadeSubscriber.on_asset_hard_delete
    )
    logger.info(
        "ItemReverseCascadeSubscriber: registered on "
        "CatalogEventType.ASSET_HARD_DELETION"
    )
