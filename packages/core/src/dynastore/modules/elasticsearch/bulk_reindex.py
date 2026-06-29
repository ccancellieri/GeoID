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

"""Driver-level bulk reindex helpers for Elasticsearch.

These functions implement the actual bulk-index orchestration against the
Elasticsearch driver. They live at the **module** level (not in a task
package) so that any consumer — task, extension, or external integration —
can import and call them directly without going through the dispatcher.

The module-level placement matches the pattern established for
:mod:`dynastore.modules.elasticsearch.client`,
:mod:`dynastore.modules.elasticsearch.mappings`, and the rest of the ES
driver: drivers belong to ``modules``, tasks orchestrate them.

Hard runtime dep on ``opensearchpy`` is satisfied transitively via the
client/mappings imports — no extra import gating needed here.
"""
from __future__ import annotations

import json as _json
import logging
from typing import Any, Iterator, List, Optional

# Module-level imports give tests a stable patch target:
#   ``dynastore.modules.elasticsearch.bulk_reindex.<name>``.
# The router does not import from bulk_reindex so there is no cycle.
from dynastore.modules.storage.router import get_items_search_driver, get_write_drivers
from dynastore.modules.storage.hints import Hint
from dynastore.modules.elasticsearch.aliases import add_index_to_public_alias
from dynastore.modules.elasticsearch.mappings import get_tenant_items_index
from dynastore.modules.elasticsearch.client import get_index_prefix

logger = logging.getLogger(__name__)


def get_es_client():
    """Return the shared singleton AsyncElasticsearch client.

    Raises ``RuntimeError`` if the client is not initialized — caller is
    responsible for ensuring ElasticsearchModule's lifespan has started.
    """
    from dynastore.modules.elasticsearch.client import get_client

    es = get_client()
    if es is None:
        raise RuntimeError(
            "Elasticsearch client is not initialized. "
            "Ensure ElasticsearchModule is registered and its lifespan has started."
        )
    return es


async def is_es_active_for(catalog_id: str, collection_id: str) -> bool:
    """Whether the **regular** (public) items ES driver is routed for
    this collection — i.e. ``items_elasticsearch_driver`` is pinned in
    some operation of ``ItemsRoutingConfig``.

    **Privacy safety property** (#733): a collection routed *only*
    through ``items_elasticsearch_private_driver`` (i.e. items routing
    pins only the private driver — equivalent to the legacy
    ``is_private=True`` state, now expressed by the routing config
    itself) returns False here.  Callers that gate bulk reindex on this guard therefore
    cannot accidentally fan out private-collection items into the
    per-tenant *public* index ``{prefix}-{cat}-items`` — the private
    driver writes to ``{prefix}-{cat}-private-items`` via its own
    dispatcher path; the bulk reindex pipeline must skip those
    collections.

    Reads ``ItemsRoutingConfig`` via the ConfigsProtocol.  Returns
    False on any failure (missing protocol, missing config, malformed
    config) so callers can safely use this as a guard before
    performing ES operations.
    """
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig
    from dynastore.tools.discovery import get_protocol as _get_protocol

    configs = _get_protocol(ConfigsProtocol)
    if not configs:
        return False
    try:
        routing = await configs.get_config(
            ItemsRoutingConfig,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )
    except Exception as exc:  # noqa: BLE001
        # Routing config unavailable (missing config row, cold boot, test
        # environment without a live DB).  Safe to skip: the caller uses
        # False to gate bulk-reindex, so a config miss just leaves the
        # collection out of the run — it will be picked up on the next
        # scheduled sweep once the config is available.
        logger.debug(
            "is_es_active_for: could not resolve routing config for %s/%s: %s",
            catalog_id, collection_id, exc,
        )
        return False
    return any(
        entry.driver_ref == "items_elasticsearch_driver"
        for entries in routing.operations.values()
        for entry in entries
    )


def _select_writer(
    write_drivers: "List[Any]",
    reader_ref: str,
    driver_hint: Optional[str],
) -> "Any":
    """Select the WRITE driver to use as the reindex target.

    Selection order (first match wins):

    1. If ``driver_hint`` is given (from task inputs): pick the entry whose
       ``driver_ref`` equals the hint, provided it is not the reader.
    2. Otherwise: pick the first WRITE entry whose driver declares
       ``is_item_indexer = True`` (a secondary search-index), provided it is
       not the reader.

    A reader and writer sharing the same ``driver_ref`` would feed a driver
    back into itself — that is never the intent of a reindex, so the guard
    raises rather than silently no-ops.

    Args:
        write_drivers: List of ``ResolvedDriver`` from ``get_write_drivers``.
        reader_ref: ``driver_ref`` of the resolved reader (must not equal
            the selected writer's ref).
        driver_hint: Optional driver_ref override from task inputs.

    Returns:
        The selected ``ResolvedDriver`` entry.

    Raises:
        ValueError: If no suitable writer can be found, or the only candidate
            matches the reader (which would loop reads back to the source).
    """
    if driver_hint:
        for rd in write_drivers:
            if rd.driver_ref == driver_hint:
                if rd.driver_ref == reader_ref:
                    raise ValueError(
                        f"Reindex writer '{driver_hint}' (from task inputs) "
                        f"is the same driver as the reader ('{reader_ref}'). "
                        "The writer must be a different driver than the source."
                    )
                return rd
        raise ValueError(
            f"Reindex: driver_hint '{driver_hint}' not found in WRITE drivers "
            f"({[rd.driver_ref for rd in write_drivers]}). "
            "Verify ItemsRoutingConfig for this collection."
        )

    # Prefer secondary-index drivers (is_item_indexer) that differ from the reader.
    for rd in write_drivers:
        if rd.driver_ref != reader_ref and getattr(type(rd.driver), "is_item_indexer", False):
            return rd

    # No secondary-index driver found distinct from the reader.
    candidates = [rd.driver_ref for rd in write_drivers if rd.driver_ref != reader_ref]
    if not candidates:
        raise ValueError(
            f"Reindex: no writer found that is distinct from the reader "
            f"('{reader_ref}'). WRITE drivers: "
            f"{[rd.driver_ref for rd in write_drivers]}. "
            "Add a secondary-index driver (is_item_indexer=True) to the "
            "WRITE routing entries, or pass an explicit driver_hint."
        )
    raise ValueError(
        f"Reindex: no secondary-index (is_item_indexer) writer found distinct "
        f"from the reader ('{reader_ref}'). Non-reader WRITE drivers that were "
        f"found but lack is_item_indexer: {candidates}. "
        "Pass driver_hint to select one explicitly."
    )


# Target ceiling for a single ES _bulk request body (estimated serialized
# JSON size of all documents in one call).  OpenSearch's default
# http.max_content_length is 100 MB; this leaves a 12× safety margin so
# geometry-heavy docs (admin boundaries, networks) that can run 50–200 KB
# each don't produce 413 responses.  Large read pages are split into
# multiple sub-chunk writes — small docs accumulate more per call
# automatically; no operator tuning needed.
_MAX_BULK_BYTES: int = 8 * 1024 * 1024  # 8 MB


def _iter_byte_bounded_chunks(
    items: List[Any],
    max_bytes: int,
) -> Iterator[List[Any]]:
    """Yield sub-lists of items whose total estimated serialised JSON byte
    size does not exceed *max_bytes*.

    Each item's size is estimated via ``json.dumps`` with a plain encoder
    (sufficient for a byte-count approximation; exact ES encoding may
    differ slightly).  An item that individually exceeds *max_bytes* is
    yielded alone — the caller must accept an oversized single-doc request
    rather than infinite-loop or drop the item.
    """
    current_chunk: List[Any] = []
    current_bytes = 0
    for item in items:
        try:
            item_bytes = len(_json.dumps(item, ensure_ascii=False).encode("utf-8"))
        except (TypeError, ValueError):
            item_bytes = len(str(item).encode("utf-8"))
        if current_chunk and current_bytes + item_bytes > max_bytes:
            yield current_chunk
            current_chunk = []
            current_bytes = 0
        current_chunk.append(item)
        current_bytes += item_bytes
    if current_chunk:
        yield current_chunk


async def reindex_collection_into_index(
    catalog_id: str,
    collection_id: str,
    *,
    driver_hint: Optional[str] = None,
    reader_ref: Optional[str] = None,
    page_size: int = 2000,
) -> int:
    """Stream every item of a collection from the routing-resolved source-of-truth
    reader and bulk-write it via the routing-resolved secondary-index writer.

    Resolution strategy:

    - **Reader**: when ``reader_ref`` is given it is resolved directly from the
      driver registry (used by file-backed collections to name the file driver
      explicitly). Otherwise it is resolved via
      :func:`~dynastore.modules.storage.router.get_items_search_driver` with
      ``hints={Hint.GEOMETRY_EXACT}`` — the source-of-truth store for this
      collection, which is PostgreSQL for a PG-primary collection and the file
      driver (DuckDB) for a file-backed one, since that driver also advertises
      ``GEOMETRY_EXACT``. Either way the ES read path is bypassed — we must not
      read from the index we are rebuilding.
    - **Writer**: resolved via :func:`~dynastore.modules.storage.router.get_write_drivers`,
      then filtered to the first secondary-index entry (``is_item_indexer=True``) that
      differs from the reader.  When ``driver_hint`` is supplied, that ``driver_ref``
      is used directly instead.

    The reader and writer MUST resolve to different drivers. If the resolved writer
    equals the reader this function raises ``ValueError`` immediately — a reindex that
    reads and writes to the same driver is a no-op at best and a data hazard at worst.

    Chunks are sized to ``max(page_size, writer.preferred_chunk_size)`` when the writer
    declares a preference; otherwise ``page_size`` governs.

    Write failures propagate: :class:`~dynastore.modules.storage.errors.EsBulkWriteError`
    (and any other exception from ``write_entities``) are re-raised after logging the
    failure count so the task can surface them and apply its ``on_failure`` policy.
    The returned count reflects only successfully written documents.

    Alias enrolment: the writer's index is enrolled in the public alias once before
    streaming begins (idempotent; best-effort).

    Args:
        catalog_id: Catalog owning the collection.
        collection_id: Collection to reindex.
        driver_hint: Optional ``driver_ref`` override that selects the WRITE target
            directly (e.g. ``"items_elasticsearch_driver"``). Takes precedence over
            the secondary-index auto-select.
        reader_ref: Optional ``driver_ref`` override that selects the READ source
            directly (e.g. ``"items_duckdb_driver"`` for a file-backed collection).
            Takes precedence over the GEOMETRY_EXACT hint resolution.
        page_size: Items per read page (and write chunk, unless the writer declares
            a larger ``preferred_chunk_size``).

    Returns:
        Number of documents successfully written to the target index.

    Raises:
        ValueError: If routing cannot resolve a valid reader/writer pair.
        RuntimeError: If required protocols are unavailable.
        :class:`~dynastore.modules.storage.errors.EsBulkWriteError`: On ES bulk-write
            rejection (propagated from the writer driver).
    """
    # --- Resolve reader: source-of-truth READ driver. ---
    # An explicit reader_ref (file-backed collections) takes precedence; otherwise
    # the GEOMETRY_EXACT hint resolves the authoritative store (PG for PG-primary
    # collections, the file driver for file-backed ones), never the ES index we
    # are rebuilding.
    if reader_ref:
        from dynastore.modules.storage.driver_registry import DriverRegistry
        reader = DriverRegistry.get_collection(reader_ref)
        if reader is None:
            raise ValueError(
                f"reindex_collection_into_index: reader_ref '{reader_ref}' is not "
                f"a registered items driver for {catalog_id}/{collection_id}.",
            )
    else:
        reader_resolved = await get_items_search_driver(
            catalog_id,
            collection_id,
            hints=frozenset({Hint.GEOMETRY_EXACT}),
        )
        reader = reader_resolved.driver
        reader_ref = reader_resolved.driver_ref

    # --- Resolve writers: WRITE fan-out list (all configured WRITE drivers). ---
    write_drivers = await get_write_drivers(catalog_id, collection_id)

    # --- Select the target writer (must differ from the reader). ---
    writer_resolved = _select_writer(write_drivers, reader_ref, driver_hint)
    writer = writer_resolved.driver
    writer_ref = writer_resolved.driver_ref

    if not await is_es_active_for(catalog_id, collection_id):
        logger.debug(
            "Skipping collection %s/%s — the regular ES driver is not in the "
            "routing config for this collection.",
            catalog_id,
            collection_id,
        )
        return 0

    # --- Alias enrolment (idempotent). ---
    # Enrol the writer's index in the public alias so /search returns results
    # after the reindex completes.  Best-effort: a failure here is non-fatal
    # since the alias can be repaired independently.
    try:
        index_name = get_tenant_items_index(get_index_prefix(), catalog_id)
        await add_index_to_public_alias(index_name)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "reindex_collection_into_index: alias enrolment failed for %s/%s: %s. "
            "Continuing — alias can be repaired separately.",
            catalog_id, collection_id, exc,
        )

    # --- Determine chunk size from the writer's preference. ---
    writer_chunk = getattr(writer, "preferred_chunk_size", 0)
    chunk_size = max(page_size, writer_chunk) if writer_chunk > 0 else page_size

    logger.info(
        "reindex_collection_into_index: %s/%s  reader=%s  writer=%s  chunk_size=%d",
        catalog_id, collection_id, reader_ref, writer_ref, chunk_size,
    )

    total_written = 0
    offset = 0

    while True:
        # Collect a chunk from the reader.
        chunk: list = []
        async for feature in reader.read_entities(
            catalog_id,
            collection_id,
            limit=chunk_size,
            offset=offset,
        ):
            chunk.append(feature)

        if not chunk:
            break

        # Split the read page into byte-bounded sub-chunks before writing to ES.
        # Each sub-chunk produces one _bulk HTTP request; the byte ceiling
        # (_MAX_BULK_BYTES) prevents 413 on geometry-heavy collections while
        # still using large batches for lightweight docs.
        for sub_chunk in _iter_byte_bounded_chunks(chunk, max_bytes=_MAX_BULK_BYTES):
            try:
                written = await writer.write_entities(catalog_id, collection_id, sub_chunk)
                batch_count = len(written) if written is not None else len(sub_chunk)
            except Exception:
                logger.error(
                    "reindex_collection_into_index: write_entities raised for %s/%s "
                    "at offset %d (%d docs in sub-chunk); propagating error. "
                    "Total successfully written before failure: %d.",
                    catalog_id, collection_id, offset, len(sub_chunk), total_written,
                )
                raise
            total_written += batch_count

        if len(chunk) < chunk_size:
            break
        offset += chunk_size

    return total_written
