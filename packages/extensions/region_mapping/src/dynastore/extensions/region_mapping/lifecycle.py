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

"""Referential-integrity cleanup for ``region_mapping.mappings`` (dynastore#443).

A claim row is a *weak reference*: it names a ``{src_catalog, src_collection}``
elsewhere in the platform, but nothing about that collection's own delete path
knows this extension exists, and it must stay that way -- a collection/catalog
delete can never be allowed to depend on, wait for, or be blocked by this
cleanup. It is therefore wired as an *async* listener on the shared event bus
(``dynastore.modules.catalog.event_service``): the delete emits its event and
returns; the listener runs afterwards, in a background task, retried by
``EventDrainTask`` on failure and dead-lettered after repeated failures --
never inside the delete's own transaction (compare ``register_event_listener``,
used elsewhere for genuinely coupled cascades, which runs synchronously
in-transaction).

Every read here goes through ``CatalogsProtocol``/whatever protocol the
platform exposes for this -- there is deliberately no PG-specific assumption:
region_mapping's own table is Postgres, but the *deleted* collection could be
served by any driver.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from dynastore.modules.catalog.event_service import CatalogEventType, async_event_listener
from dynastore.tools.protocol_helpers import get_engine

from . import registry_store as _store

logger = logging.getLogger(__name__)


async def _on_collection_gone(
    catalog_id: Optional[str] = None,
    collection_id: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    **_kwargs: Any,
) -> None:
    """Delete claims sourced from one hard-deleted collection.

    Best-effort: any exception is logged and swallowed rather than
    re-raised -- ``EventDrainTask`` already retries a failing listener on
    its own schedule, so escalating here would only turn a transient DB
    hiccup into a dead-lettered event for no benefit.
    """
    if not catalog_id or not collection_id:
        return
    engine = get_engine()
    if engine is None:
        return
    try:
        deleted = await _store.delete_claims_by_source_collection(
            engine, catalog_id, collection_id,
        )
    except Exception as exc:  # noqa: BLE001 -- best-effort, never re-raise
        logger.warning(
            "region_mapping: cleanup after collection %s/%s deletion failed "
            "(will retry on the next dispatch): %s", catalog_id, collection_id, exc,
        )
        return
    if deleted:
        logger.info(
            "region_mapping: removed %d dangling claim(s) for deleted "
            "collection %s/%s", deleted, catalog_id, collection_id,
        )


async def _on_catalog_gone(
    catalog_id: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    **_kwargs: Any,
) -> None:
    """Delete claims sourced from any collection in one hard-deleted catalog."""
    if not catalog_id:
        return
    engine = get_engine()
    if engine is None:
        return
    try:
        deleted = await _store.delete_claims_by_source_catalog(engine, catalog_id)
    except Exception as exc:  # noqa: BLE001 -- best-effort, never re-raise
        logger.warning(
            "region_mapping: cleanup after catalog %s deletion failed (will "
            "retry on the next dispatch): %s", catalog_id, exc,
        )
        return
    if deleted:
        logger.info(
            "region_mapping: removed %d dangling claim(s) for deleted "
            "catalog %s", deleted, catalog_id,
        )


def register_region_mapping_cleanup_subscriber() -> None:
    """Wire collection/catalog hard-deletion to the best-effort claim cleanup.

    Called from ``RegionMappingService.lifespan`` -- the collection/catalog
    delete path stays entirely unaware of region_mapping. Idempotent to call
    more than once: each call adds another dispatch of an idempotent DELETE.
    """
    async_event_listener(CatalogEventType.COLLECTION_HARD_DELETION)(_on_collection_gone)
    async_event_listener(CatalogEventType.CATALOG_HARD_DELETION)(_on_catalog_gone)
    logger.info(
        "region_mapping: registered cleanup listener on "
        "CatalogEventType.COLLECTION_HARD_DELETION / CATALOG_HARD_DELETION"
    )
