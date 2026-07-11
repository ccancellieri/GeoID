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

"""Stateless sweep for missed ``tasks.storage`` obligations (#2688 lane 1).

Item-tier async secondary-index writes are recorded as durable id-only
obligation rows in ``tasks.storage`` in a post-commit seam that runs after
the primary PG hub write already committed (``ItemService.upsert_bulk`` /
``ItemQueryMixin._enqueue_index_deletes``, see ``storage_emit.py``). If the
process dies inside that seam, the hub row exists with no corresponding
obligation and the derived store (e.g. Elasticsearch) silently diverges
forever — nothing else ever notices, because the obligation was never
recorded in the first place.

:func:`sweep_missing_obligations` closes that gap without a cursor table:
each run re-examines a bounded, recent time window of hub rows and
re-enqueues an id-only obligation for any row that has no matching
``tasks.storage`` row. ``grace`` covers the normal post-commit enqueue
seam so a write that simply hasn't landed its obligation yet is never
mistaken for a miss; ``lookback`` is a multiple of the calling job's
interval so consecutive runs overlap and a slow tick never opens a gap.

The window matches a hub row on either ``transaction_time`` (set on every
insert/update) or ``deleted_at`` (set on soft delete, which does NOT bump
``transaction_time`` — see ``ItemService``'s ``soft_delete_item_query`` /
``ItemQueryMixin._enqueue_index_deletes``) so an old row's later tombstone
is never invisible to the sweep just because the row itself is ancient.

A hub row counts as covered by either match:

- ``write_id`` — the row's own ``write_id`` column matches a write-id
  ledger row (``entity_id`` NULL). No further scoping: a ledger row is
  written once, atomically, for the exact write batch that produced this
  ``write_id``, so any match is definitionally current.
- ``entity_id`` — matches an id-only row (``write_id`` NULL) whose
  ``created_at`` is at or after the hub row's latest change. Id-only rows
  are ALSO produced by this sweep itself (the #3116 write-id-incapable
  fallback and every earlier sweep run write one per missing row), so an
  unscoped match would let a stale, already-drained sweep row from a prior
  tick permanently mask a *later* missed write of the same item — the
  ``created_at`` bound closes that hole.

Results are capped per (collection, driver) and ordered so the rows
closest to aging out of the lookback window are prioritized first — a
truncated tick still drains the ones that would otherwise be lost.

Idempotent by construction: a duplicate re-enqueue is never dropped by the
drain (there is no unique index on ``tasks.storage``), and the drain always
re-reads canonical PG state for an id-only row rather than replaying a
payload snapshot, so a duplicate just costs one wasted drain op — never a
correctness problem.

PG-only (mirrors the #3116 exclusion documented in ``storage_emit.py`` /
``modules/storage/README.md``): only collections whose resolved WRITE
primary is the PG items driver participate, because the hub table this
sweep reads only exists for that driver.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Covers the normal post-commit enqueue seam (primary write commits, then the
# outbox row lands moments later) so an in-flight write is never mistaken for
# a miss.
GRACE_SECONDS = 300

# Lookback is a multiple of the calling job's interval so two consecutive
# runs always overlap — a slow or skipped tick still gets covered by the
# next one. Duplicates across overlapping windows are harmless (see module
# docstring).
LOOKBACK_MULTIPLIER = 3

# Defensive cap on how many missing rows one (collection, driver) pair can
# surface per tick — keeps a single pathological collection from consuming
# the whole job's bounded dispatch window. A window this small (minutes)
# should never legitimately need more; any remainder is picked up on the
# next tick as long as it stays inside the lookback window.
_ROW_LIMIT_PER_TARGET = 1000

_PAGE_SIZE = 200
_MAX_CATALOGS = 10_000
_MAX_COLLECTIONS_PER_CATALOG = 10_000

# The only driver whose hub table this sweep knows how to read. Mirrors the
# #3116 write-id capability guard: in every shipped routing preset this is
# also the only driver implementing the write-id reader pair, but the check
# here is about hub-table *existence*, not write-id capability, so it is
# done directly against driver_ref rather than through
# ``storage_emit.driver_supports_write_id_reads``.
_ITEMS_POSTGRESQL_DRIVER_REF = "items_postgresql_driver"


async def sweep_missing_obligations(
    conn: Any,
    *,
    interval_seconds: int,
    grace_seconds: int = GRACE_SECONDS,
) -> int:
    """Re-enqueue ``tasks.storage`` obligations missing for recent hub writes.

    Runs entirely on the caller's open ``conn`` (one job transaction) — every
    catalog/collection enumerated and every SELECT/enqueue issued rides that
    same connection. A failure while sweeping one collection (missing
    column on a legacy hub table, unresolvable routing, ...) is isolated via
    a SAVEPOINT so it cannot poison the rest of the tick; that collection is
    simply retried on the next run.

    Returns the number of obligations re-enqueued.
    """
    now = datetime.now(timezone.utc)
    lookback_seconds = LOOKBACK_MULTIPLIER * interval_seconds
    window_start = now - timedelta(seconds=lookback_seconds)
    window_end = now - timedelta(seconds=grace_seconds)
    if window_start >= window_end:
        logger.warning(
            "obligation_sweep: grace_seconds (%d) >= lookback (%d) — empty "
            "window, skipping.",
            grace_seconds, lookback_seconds,
        )
        return 0

    from dynastore.models.driver_context import DriverContext
    from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
    from dynastore.models.protocols.visibility import visibility_bypass
    from dynastore.tools.discovery import get_protocol

    catalogs = get_protocol(CatalogsProtocol)
    configs = get_protocol(ConfigsProtocol)
    if catalogs is None or configs is None:
        logger.debug(
            "obligation_sweep: CatalogsProtocol/ConfigsProtocol unavailable "
            "— skipping this tick."
        )
        return 0

    ctx = DriverContext(db_resource=conn)
    collections_examined = 0
    targets_checked = 0
    total_enqueued = 0

    with visibility_bypass():
        catalog_offset = 0
        while catalog_offset < _MAX_CATALOGS:
            catalog_page = await catalogs.list_catalogs(
                limit=_PAGE_SIZE, offset=catalog_offset, ctx=ctx,
            )
            if not catalog_page:
                break
            for catalog in catalog_page:
                catalog_id = catalog.id
                collection_offset = 0
                while collection_offset < _MAX_COLLECTIONS_PER_CATALOG:
                    collection_page = await catalogs.list_collections(
                        catalog_id, limit=_PAGE_SIZE, offset=collection_offset,
                        ctx=ctx,
                    )
                    if not collection_page:
                        break
                    for collection in collection_page:
                        collections_examined += 1
                        checked, enqueued = await _sweep_collection(
                            conn,
                            catalogs=catalogs,
                            configs=configs,
                            catalog_id=catalog_id,
                            collection_id=collection.id,
                            ctx=ctx,
                            window_start=window_start,
                            window_end=window_end,
                        )
                        targets_checked += checked
                        total_enqueued += enqueued
                    if len(collection_page) < _PAGE_SIZE:
                        break
                    collection_offset += _PAGE_SIZE
            if len(catalog_page) < _PAGE_SIZE:
                break
            catalog_offset += _PAGE_SIZE

    logger.info(
        "obligation_sweep: examined %d collection(s), checked %d target(s), "
        "re-enqueued %d missing obligation(s) (window=[%s, %s]).",
        collections_examined, targets_checked, total_enqueued,
        window_start.isoformat(), window_end.isoformat(),
    )
    return total_enqueued


async def _sweep_collection(
    conn: Any,
    *,
    catalogs: Any,
    configs: Any,
    catalog_id: str,
    collection_id: str,
    ctx: Any,
    window_start: datetime,
    window_end: datetime,
) -> Tuple[int, int]:
    """Sweep one collection; returns ``(targets_checked, obligations_enqueued)``.

    Isolated in a SAVEPOINT so a per-collection failure never aborts the
    supervisor's job transaction for every other collection in the tick.
    The routing-config parse itself is guarded separately (below): a single
    stored config that fails Pydantic validation (e.g. a legacy field value
    a model shrink no longer accepts) must not abort the whole sweep either
    — it is logged and this collection is skipped for the tick.
    """
    from pydantic import ValidationError

    from dynastore.modules.db_config.query_executor import best_effort_savepoint
    from dynastore.modules.storage.routing_config import (
        ItemsRoutingConfig,
        Operation,
        index_entries,
    )

    try:
        routing = await configs.get_config(
            ItemsRoutingConfig,
            catalog_id=catalog_id,
            collection_id=collection_id,
            ctx=ctx,
        )
    except ValidationError as exc:
        failing_fields = ", ".join(
            ".".join(str(part) for part in error["loc"]) for error in exc.errors()
        )
        logger.error(
            "obligation_sweep: %s/%s has an invalid stored ItemsRoutingConfig "
            "(failing field(s): %s) — skipping this collection for this tick: %s",
            catalog_id, collection_id, failing_fields, exc,
        )
        return 0, 0
    ops_map = getattr(routing, "operations", {}) or {}
    async_entries = index_entries(ops_map)
    if not async_entries:
        return 0, 0

    write_entries = list(ops_map.get(Operation.WRITE, []))
    if not write_entries or write_entries[0].driver_ref != _ITEMS_POSTGRESQL_DRIVER_REF:
        logger.debug(
            "obligation_sweep: %s/%s resolved WRITE primary is not the PG "
            "items driver — hub table does not exist here (#3116), skipping.",
            catalog_id, collection_id,
        )
        return 0, 0

    checked = 0
    enqueued = 0
    async with best_effort_savepoint(conn) as outcome:
        checked, enqueued = await _check_and_enqueue(
            conn,
            catalogs=catalogs,
            configs=configs,
            catalog_id=catalog_id,
            collection_id=collection_id,
            ctx=ctx,
            async_entries=async_entries,
            window_start=window_start,
            window_end=window_end,
        )
    if outcome.error is not None:
        logger.warning(
            "obligation_sweep: %s/%s sweep failed (%s) — will retry next tick.",
            catalog_id, collection_id, outcome.error,
        )
        return 0, 0
    return checked, enqueued


async def _check_and_enqueue(
    conn: Any,
    *,
    catalogs: Any,
    configs: Any,
    catalog_id: str,
    collection_id: str,
    ctx: Any,
    async_entries: List[Any],
    window_start: datetime,
    window_end: datetime,
) -> Tuple[int, int]:
    """Query the PG hub table for each async target and re-enqueue misses."""
    from dynastore.models.protocols.indexing import OutboxRecord
    from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler
    from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
    from dynastore.modules.storage.driver_instance_id import compute_driver_instance_id
    from dynastore.modules.storage.storage_emit import enqueue_storage_op_id_only
    from dynastore.modules.tasks.tasks_module import get_task_schema
    from dynastore.tools.db import validate_sql_identifier
    from dynastore.tools.identifiers import generate_uuidv7

    driver_cfg = await configs.get_config(
        ItemsPostgresqlDriverConfig,
        catalog_id=catalog_id,
        collection_id=collection_id,
        ctx=ctx,
    )
    table = driver_cfg.physical_table
    if not table:
        return 0, 0
    table = validate_sql_identifier(table)

    schema = await catalogs.resolve_physical_schema(catalog_id, ctx=ctx)
    if not schema:
        return 0, 0
    schema = validate_sql_identifier(schema)

    task_schema = get_task_schema()
    validate_sql_identifier(task_schema)

    day_floor = (window_start - timedelta(days=1)).date()

    # "Latest change" for a hub row: deleted_at when tombstoned (soft delete
    # doesn't bump transaction_time), else transaction_time. Reused for both
    # the entity_id staleness bound and the truncation ORDER BY so the two
    # stay coherent.
    latest_change = 'GREATEST(h."transaction_time", COALESCE(h."deleted_at", h."transaction_time"))'

    sql = (
        f'SELECT h."geoid"::text AS geoid, h."deleted_at" AS deleted_at '
        f'FROM "{schema}"."{table}" h '
        f'WHERE ('
        f'      (h."transaction_time" >= :window_start AND h."transaction_time" < :window_end)'
        f'   OR (h."deleted_at" >= :window_start AND h."deleted_at" < :window_end)'
        f'  ) '
        f'  AND NOT EXISTS ('
        f'      SELECT 1 FROM {task_schema}.storage s '
        f'      WHERE s.catalog_id = :catalog_id '
        f'        AND s.driver_id = :driver_id '
        f'        AND s.collection_id = :collection_id '
        f'        AND s.day >= :day_floor '
        f'        AND ('
        f'              s.write_id = h."write_id"'
        f'           OR (s.entity_id = h."geoid"::text AND s.created_at >= {latest_change})'
        f'            )'
        f'  ) '
        f'ORDER BY {latest_change} ASC '
        f'LIMIT :row_limit'
    )

    records: List[OutboxRecord] = []
    checked = 0
    for entry in async_entries:
        checked += 1
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn,
            window_start=window_start,
            window_end=window_end,
            catalog_id=catalog_id,
            driver_id=entry.driver_ref,
            collection_id=collection_id,
            day_floor=day_floor,
            row_limit=_ROW_LIMIT_PER_TARGET,
        ) or []
        if not rows:
            continue
        driver_instance_id = compute_driver_instance_id(
            entry.driver_ref, catalog_id, collection_id,
        )
        for row in rows:
            geoid: Optional[str] = row.get("geoid")
            if not geoid:
                continue
            op = "delete" if row.get("deleted_at") is not None else "upsert"
            records.append(OutboxRecord(
                op_id=generate_uuidv7(),
                driver_id=entry.driver_ref,
                driver_instance_id=driver_instance_id,
                collection_id=collection_id,
                op=op,  # type: ignore[arg-type]
                item_id=geoid,
                idempotency_key=geoid,
            ))

    if not records:
        return checked, 0

    await enqueue_storage_op_id_only(conn, catalog_id=catalog_id, rows=records)
    logger.info(
        "obligation_sweep: %s/%s re-enqueued %d missing obligation(s) across "
        "%d target(s).",
        catalog_id, collection_id, len(records), checked,
    )
    return checked, len(records)
