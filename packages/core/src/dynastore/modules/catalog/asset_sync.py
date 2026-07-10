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

"""
Asset entity sync — event-driven fan-out to AssetIndexer drivers.

``AssetEntitySyncSubscriber`` listens for ``CatalogEventType.ASSET_*`` events
emitted by ``AssetService`` and dispatches the row write/delete to every
driver pinned as a secondary-index ``WRITE`` entry (``secondary_index=True``)
in ``AssetRoutingConfig.operations[WRITE]`` (auto-augmented with discoverable
``AssetIndexer`` implementors such as ``AssetElasticsearchDriver``).

This collapses the prior dual-write race where the ES driver received writes
from both the routing-config fan-out and a private listener block: the row
goes to the primary WRITE driver (``secondary_index=False``) synchronously
inside ``AssetService``; secondary-index fan-out happens via this subscriber,
fed by the events outbox so failures are replayable.

Failure propagation: this subscriber is invoked twice per event — once
immediately in-process (``EventService.emit``'s fast path) and once more
durably when ``EventDrainTask`` drains the corresponding ``tasks.events``
row and calls ``EventService.dispatch_to_listeners``. Every per-indexer
failure is logged; failures on entries whose ``on_failure`` policy is
``FATAL`` or ``OUTBOX`` additionally raise a single chained exception after
every entry has been attempted, so the durable leg's caller
(``EventDrainTask``) retries the row. ``WARN`` (logged) and ``IGNORE``
(silent, per its documented meaning) never trigger a retry. The immediate
leg swallows the same exception at the ``emit`` dispatch site (see
``event_service._invoke_listener_detached``) so it never surfaces as
unretrieved-task noise; the durable leg is the retry authority.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

from dynastore.modules import get_protocol
from dynastore.modules.catalog.event_service import (
    CatalogEventType,
    async_event_listener,
)

logger = logging.getLogger(__name__)


async def _fan_out_and_raise(
    *,
    action: str,
    catalog_id: str,
    asset_id: str,
    indexers: List[Any],
    calls: List[Any],
) -> None:
    """Await *calls* (one per entry in *indexers*) in parallel.

    Logs every per-indexer failure — ERROR for ``on_failure=FATAL``, WARNING
    for ``OUTBOX``/``WARN``, DEBUG for ``IGNORE`` (its documented meaning is
    silent skip, so it must not surface at WARNING). Once every entry has
    been attempted, raises a single ``RuntimeError`` — chained from the first
    such failure — summarizing every entry whose ``on_failure`` policy is
    ``FATAL`` or ``OUTBOX``, so the durable events-outbox row retries.
    ``WARN`` and ``IGNORE`` are tolerant: logged (or silently skipped for
    ``IGNORE``) but never trigger a retry.
    """
    from dynastore.modules.storage.routing_config import FailurePolicy

    results = await asyncio.gather(*calls, return_exceptions=True)

    first_failure: Optional[BaseException] = None
    retry_refs: List[str] = []
    for r, result in zip(indexers, results):
        if not isinstance(result, BaseException):
            continue
        if r.on_failure == FailurePolicy.FATAL:
            level = logger.error
        elif r.on_failure == FailurePolicy.IGNORE:
            level = logger.debug
        else:
            level = logger.warning
        level(
            "AssetEntitySync: indexer '%s' %s failed for %s/%s: %s",
            r.driver_ref, action, catalog_id, asset_id, result,
        )
        if r.on_failure in (FailurePolicy.FATAL, FailurePolicy.OUTBOX):
            retry_refs.append(r.driver_ref)
            if first_failure is None:
                first_failure = result

    if first_failure is not None:
        raise RuntimeError(
            f"AssetEntitySync: {action} failed for {catalog_id}/{asset_id} on "
            f"{len(retry_refs)} indexer(s) requiring retry: "
            f"{', '.join(retry_refs)}"
        ) from first_failure


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
            logger.debug(
                "AssetEntitySync: on_asset_upsert missing catalog_id/asset_id "
                "(catalog_id=%r, asset_id=%r) — dropping malformed event, no retry.",
                catalog_id, asset_id,
            )
            return

        from dynastore.modules.storage.router import get_asset_index_drivers

        try:
            indexers = await get_asset_index_drivers(catalog_id, collection_id)
        except Exception as exc:
            logger.warning(
                "AssetEntitySync: index-driver resolution failed for %s/%s: %s",
                catalog_id, asset_id, exc,
            )
            raise RuntimeError(
                f"AssetEntitySync: index-driver resolution failed for "
                f"{catalog_id}/{asset_id}"
            ) from exc
        if not indexers:
            return

        doc = dict(payload) if isinstance(payload, dict) else {}
        doc.setdefault("asset_id", asset_id)
        doc.setdefault("catalog_id", catalog_id)
        if collection_id:
            doc.setdefault("collection_id", collection_id)

        await _fan_out_and_raise(
            action="index_asset",
            catalog_id=catalog_id,
            asset_id=asset_id,
            indexers=indexers,
            calls=[r.driver.index_asset(catalog_id, doc) for r in indexers],
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
            logger.debug(
                "AssetEntitySync: on_asset_delete missing catalog_id/asset_id "
                "(catalog_id=%r, asset_id=%r) — dropping malformed event, no retry.",
                catalog_id, asset_id,
            )
            return

        from dynastore.modules.storage.router import get_asset_index_drivers

        try:
            indexers = await get_asset_index_drivers(catalog_id, collection_id)
        except Exception as exc:
            logger.warning(
                "AssetEntitySync: index-driver resolution failed for %s/%s: %s",
                catalog_id, asset_id, exc,
            )
            raise RuntimeError(
                f"AssetEntitySync: index-driver resolution failed for "
                f"{catalog_id}/{asset_id}"
            ) from exc
        if not indexers:
            return

        await _fan_out_and_raise(
            action="delete_asset",
            catalog_id=catalog_id,
            asset_id=asset_id,
            indexers=indexers,
            calls=[r.driver.delete_asset(catalog_id, asset_id) for r in indexers],
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


class ItemForwardCascadeSubscriber:
    """Forward cascade — soft-delete virtual assets owned by a soft-deleted item.

    Reads the ``asset_references`` table for ``CoreAssetReferenceType.ITEM``
    entries keyed by the deleted item's ID and issues a soft-delete for each
    matching virtual asset.

    Unlike ``ItemReverseCascadeSubscriber`` (which gates on ``payload['propagate']``
    to avoid dangerously cascading item deletions from an asset deletion), this
    cascade is UNCONDITIONAL: an item's own virtual assets must always track the
    item's lifecycle — there is no risk of unexpected data loss because virtual
    assets are synthesised from item data and carry no independent business value.
    """

    @staticmethod
    async def on_item_delete(
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        item_id: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        **_kwargs: Any,
    ) -> None:
        del _kwargs
        if not catalog_id or not item_id:
            return

        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.shared_models import CoreAssetReferenceType

        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            logger.warning(
                "ItemForwardCascade: CatalogsProtocol unavailable for %s/%s",
                catalog_id, item_id,
            )
            return

        try:
            asset_ids: List[str] = await catalogs.assets.list_assets_for_reference(
                catalog_id, CoreAssetReferenceType.ITEM, item_id
            )
        except Exception as exc:
            logger.warning(
                "ItemForwardCascade: list_assets_for_reference failed for %s/%s: %s",
                catalog_id, item_id, exc,
            )
            return

        if not asset_ids:
            return

        deleted = 0
        for asset_id in asset_ids:
            try:
                await catalogs.assets.delete_assets(
                    catalog_id,
                    asset_id=asset_id,
                    collection_id=collection_id,
                    hard=False,
                )
                deleted += 1
            except Exception as exc:
                logger.warning(
                    "ItemForwardCascade: delete_assets failed for %s/%s (item %s): %s",
                    catalog_id, asset_id, item_id, exc,
                )

        logger.info(
            "ItemForwardCascade: soft-deleted %d/%d virtual asset(s) for item %s/%s",
            deleted, len(asset_ids), catalog_id, item_id,
        )


def register_item_forward_cascade_subscriber() -> None:
    """Register ``ItemForwardCascadeSubscriber`` on the global event bus.

    Wires ``ITEM_DELETION`` only — soft delete is symmetric with the item's own
    soft-delete.  ``ITEM_HARD_DELETION`` is defined but never emitted; do not
    wire it here.
    """
    async_event_listener(CatalogEventType.ITEM_DELETION)(
        ItemForwardCascadeSubscriber.on_item_delete
    )
    logger.info(
        "ItemForwardCascadeSubscriber: registered on "
        "CatalogEventType.ITEM_DELETION"
    )
