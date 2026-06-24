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

"""ES-vs-PostgreSQL item-index drift report (read-only diagnostic).

For each collection routed through the regular Elasticsearch driver, this
compares the authoritative PostgreSQL item count (the source-of-truth READ
driver, resolved via the ``GEOMETRY_EXACT`` hint) against the Elasticsearch
document count for the same collection. A non-zero ``drift`` means the
secondary index is missing (or holding extra) documents relative to PG —
typically un-drained async writes, replica lag, or documents the ES
``geo_shape`` mapper rejected at index time.

The report never mutates either store. Remediation for a real drift is the
existing reindex-from-PG task, dispatched via
``POST /admin/catalogs/{cat}/tasks`` with ``{"action": "reindex"}`` (or the
collection-scoped variant), which rebuilds the ES index from PG.

PG-only collections (no ES driver in their routing config) report
``es_active=False`` and are excluded from the drift totals so they do not
masquerade as a divergence.
"""
from __future__ import annotations

import logging
from typing import List, Optional

from dynastore.modules import get_protocol
from dynastore.models.protocols.catalogs import CatalogsProtocol

from .models import CollectionIndexDrift, IndexDriftReport

logger = logging.getLogger(__name__)


async def _collection_ids(catalog_id: str) -> List[str]:
    """Page through every collection id in the catalog."""
    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        raise RuntimeError("CatalogsProtocol not available in this process.")
    ids: List[str] = []
    offset, batch = 0, 100
    while True:
        page = await catalogs.list_collections(
            catalog_id, limit=batch, offset=offset, lang="*"
        )
        if not page:
            break
        for collection in page:
            cid = getattr(collection, "id", None)
            if cid:
                ids.append(cid)
        if len(page) < batch:
            break
        offset += batch
    return ids


async def _drift_for_collection(
    catalog_id: str, collection_id: str
) -> CollectionIndexDrift:
    """Compute PG-vs-ES counts for one collection (best-effort per store)."""
    from dynastore.modules.storage.router import get_items_search_driver
    from dynastore.modules.storage.hints import Hint
    from dynastore.modules.elasticsearch.bulk_reindex import (
        get_es_client,
        is_es_active_for,
    )
    from dynastore.modules.elasticsearch.client import get_index_prefix
    from dynastore.modules.elasticsearch.mappings import get_tenant_items_index
    from dynastore.modules.elasticsearch.items_es_ops import es_count_items

    es_active = await is_es_active_for(catalog_id, collection_id)

    # PG (source of truth) count via the GEOMETRY_EXACT read driver — this is
    # the authoritative store for a PG-primary collection and never the ES
    # index we are measuring.
    pg_count = 0
    try:
        resolved = await get_items_search_driver(
            catalog_id,
            collection_id,
            hints=frozenset({Hint.GEOMETRY_EXACT}),
        )
        pg_count = await resolved.driver.count_entities(catalog_id, collection_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "index-drift: PG count failed for %s/%s: %s",
            catalog_id, collection_id, exc,
        )

    # ES (secondary index) document count for the same collection.
    es_count = 0
    if es_active:
        try:
            es = get_es_client()
            index_name = get_tenant_items_index(get_index_prefix(), catalog_id)
            es_count = await es_count_items(
                es, index_name, collection=collection_id, routing=collection_id
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "index-drift: ES count failed for %s/%s: %s",
                catalog_id, collection_id, exc,
            )

    drift = pg_count - es_count
    return CollectionIndexDrift(
        collection_id=collection_id,
        es_active=es_active,
        pg_count=pg_count,
        es_count=es_count,
        drift=drift,
        # A PG-only collection is never expected to have ES docs, so it is
        # in sync by definition; otherwise sync means equal counts.
        in_sync=(not es_active) or (drift == 0),
    )


async def compute_index_drift(
    catalog_id: str,
    collection_id: Optional[str] = None,
) -> IndexDriftReport:
    """Build the ES-vs-PG drift report for a catalog (or a single collection).

    Totals are computed over ES-active collections only, so the
    ``total_drift`` is an apples-to-apples PG-vs-ES comparison.
    """
    cids = [collection_id] if collection_id else await _collection_ids(catalog_id)

    rows: List[CollectionIndexDrift] = []
    for cid in cids:
        rows.append(await _drift_for_collection(catalog_id, cid))

    active = [r for r in rows if r.es_active]
    total_pg = sum(r.pg_count for r in active)
    total_es = sum(r.es_count for r in active)
    return IndexDriftReport(
        catalog_id=catalog_id,
        collections=rows,
        total_pg=total_pg,
        total_es=total_es,
        total_drift=total_pg - total_es,
        in_sync=all(r.in_sync for r in rows),
    )
