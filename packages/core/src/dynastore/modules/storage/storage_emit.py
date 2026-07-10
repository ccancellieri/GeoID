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

# dynastore/modules/storage/storage_emit.py

"""Storage-plane write into the global ``tasks.storage`` table.

A single, config-free INSERT path.  Every item write (upsert or delete)
enqueues one or more rows into ``{task_schema}.storage`` on the caller's
open SQLAlchemy ``AsyncConnection`` (the co-transactional path) and then
inserts a dedup'd ``storage_drain`` PENDING task row on the same connection
so the drain is triggered co-transactionally with the work rows (no
permanent LISTEN required — the ``on_task_insert`` DB trigger emits
``NOTIFY new_task_queued`` automatically).

Atomicity guarantee: because both the storage rows and the drain trigger
ride the caller's transaction, a primary-write rollback leaves NO rows in
either ``tasks.storage`` or ``tasks.tasks``.

Tenancy is carried by the ``catalog_id`` column of ``tasks.storage`` (not
the table's physical location), so this module is schema-neutral from the
caller's point of view.

The ``task_schema`` is read from :func:`get_task_schema()` (backed by the
``DYNASTORE_TASK_SCHEMA`` env-var, default ``"tasks"``).  It is
schema-qualified in identifier position and validated before use.
"""
import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple

from dynastore.models.protocols.indexing import (
    OutboxRecord,
    WriteIdOutboxRecord,
)
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.identifiers import generate_uuidv7

logger = logging.getLogger(__name__)


def driver_supports_write_id_reads(driver: Any) -> bool:
    """True when ``driver`` can hydrate write-id ledger rows (#3116 guard).

    ``StorageDrainTask`` hydrates a write-id row from the collection's
    primary WRITE driver via ``read_indexable_write_batch`` or the
    ``read_active_rows_by_write_id`` / ``read_tombstoned_ids_by_write_id``
    chunk-read pair. A producer must only enqueue a write-id row when the
    primary exposes one of those — otherwise the row can never hydrate and
    would retry forever. Callers fall back to id-only rows (which re-read
    canonical PG state) or skip with a warning.
    """
    if driver is None:
        return False
    if getattr(driver, "read_indexable_write_batch", None) is not None:
        return True
    return (
        getattr(driver, "read_active_rows_by_write_id", None) is not None
        and getattr(driver, "read_tombstoned_ids_by_write_id", None) is not None
    )


async def _enqueue_drain_trigger(
    conn: Any,
    *,
    wedge_grace_seconds: Optional[float] = None,
    task_type: str = "storage_drain",
    dedup_key: str = "storage_drain",
) -> None:
    """Insert one global dedup'd PENDING drain task on ``conn``.

    Co-transactional: the drain row commits if and only if the caller's work
    rows commit. A single global dedup key ensures high write volume coalesces
    to one pending drain regardless of which tenant triggered the write.  The
    ``on_task_insert`` DB trigger fires ``NOTIFY new_task_queued`` on this
    INSERT, waking the dispatcher without requiring a new connection or LISTEN.

    Degrades gracefully when the tasks table does not exist (e.g. test
    environments that only provision storage): emits a DEBUG log and
    returns without raising. The storage rows are still committed; the
    drain will run on its next scheduled tick even without this NOTIFY trigger.

    ``task_type`` / ``dedup_key`` (#2732 step 4): both callers below keep the
    ``"storage_drain"`` default. ``StorageDrainTask._handoff_to_offload_job``
    passes ``task_type="storage_drain_offload", dedup_key="storage_drain_offload"``
    instead — its own dedup key so an offload handoff is never blocked by (or
    blocks) a live in-process ``storage_drain`` trigger.

    ``wedge_grace_seconds`` (#2715): shared by both callers — the hot
    co-transactional write path above (always ``None``) and the leader-side
    recovery tick (``dynastore.modules.tasks.drain_spawner``, which passes a
    configured float).

    * ``None`` (default): unchanged behaviour. ANY existing non-terminal
      (PENDING/ACTIVE/CREATED) row for ``dedup_key`` blocks a fresh enqueue,
      so heavy write traffic never piles up duplicate drains while one is
      healthily in flight.
    * A float: additionally tolerates a WEDGED existing row — a PENDING row
      older than ``wedge_grace_seconds`` (no dispatcher ever claimed it) or
      an ACTIVE row whose lease has already expired (``locked_until`` in the
      past — the owning worker died mid-run and the general stuck-task
      reaper has not yet caught up). Such a row can no longer make progress
      on its own, so it must not permanently silence the drain trigger for
      every future write/tick — this is the root cause behind #2715: a
      single crash-looping ``storage_drain`` task blocked every subsequent
      co-transactional enqueue via this exact guard. A LIVE PENDING row
      (still within its claim grace window) or a live ACTIVE row (lease not
      yet expired) still blocks, exactly as before — this only unblocks a
      demonstrably wedged row, never a healthy in-flight drain.
    """
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, best_effort_savepoint,
    )
    from dynastore.modules.tasks.tasks_module import get_task_schema

    task_schema = get_task_schema()
    validate_sql_identifier(task_schema)

    # When a grace window is supplied, a blocking row only counts as "live"
    # (still blocking) if it is a PENDING row within its claim grace window
    # or an ACTIVE row whose lease has not yet expired. Any other
    # non-terminal status (e.g. CREATED) is conservatively left blocking.
    wedge_tolerant_clause = (
        ""
        if wedge_grace_seconds is None
        else (
            " AND ("
            "     (status = 'PENDING' AND timestamp > now() - make_interval(secs => :wedge_grace_seconds))"
            "  OR (status = 'ACTIVE' AND locked_until > now())"
            "  OR status NOT IN ('PENDING', 'ACTIVE')"
            " )"
        )
    )

    # execution_mode uses the column-correct value 'ASYNCHRONOUS' (the column
    # DEFAULT and the value recognised by the dispatcher). The spec draft used
    # 'ASYNC' which is not a valid enum value in the tasks table.
    insert_sql = (
        f"INSERT INTO {task_schema}.tasks"
        f" (task_id, catalog_id, scope, task_type, type, execution_mode,"
        f"  inputs, timestamp, status, dedup_key)"
        f" SELECT :task_id, 'platform', 'platform', :task_type,"
        f"        'task', 'ASYNCHRONOUS', '{{}}'::jsonb, now(), 'PENDING',"
        f"        :dedup_key"
        f" WHERE NOT EXISTS ("
        f"     SELECT 1 FROM {task_schema}.tasks"
        # A distinct bind name from the SELECT target-list :dedup_key above —
        # asyncpg raises AmbiguousParameterError when the SAME named
        # parameter is reused across an untyped SELECT target position and a
        # column-comparison position, even though both bind the same Python
        # value here.
        f"     WHERE dedup_key = :dedup_key_filter"
        f"       AND catalog_id = 'platform'"
        # Full terminal set (matches the claim query in tasks_module). A
        # DISMISSED (terminal) drain task must NOT block a fresh enqueue —
        # otherwise the co-transactional NOTIFY trigger stays silenced until
        # manual cleanup. CREATED/PENDING/ACTIVE are non-terminal and DO block
        # (one live drain suffices) unless wedge_tolerant_clause narrows that
        # further to demonstrably-live rows only.
        f"       AND status NOT IN ('COMPLETED', 'FAILED', 'DISMISSED', 'DEAD_LETTER')"
        f"{wedge_tolerant_clause}"
        f" )"
    )
    params: Dict[str, Any] = {
        "task_id": generate_uuidv7(),
        "task_type": task_type,
        "dedup_key": dedup_key,
        "dedup_key_filter": dedup_key,
    }
    if wedge_grace_seconds is not None:
        params["wedge_grace_seconds"] = wedge_grace_seconds

    # Use a nested transaction (SAVEPOINT) so a missing-tasks-table error
    # does not abort the outer PG transaction carrying the storage rows. A
    # bare try/except does not help here: once asyncpg sees a statement error
    # the outer PG TX enters the aborted state and must be rolled back in its
    # entirety. best_effort_savepoint isolates the trigger INSERT so only it
    # is rolled back on failure, leaving the work rows intact — and falls
    # back to a direct (unguarded) attempt when ``conn`` doesn't support
    # nested transactions (production always uses SA AsyncConnection, so
    # that fallback is defensive only).
    async with best_effort_savepoint(conn) as outcome:
        await DQLQuery(insert_sql, result_handler=ResultHandler.NONE).execute(
            conn, **params
        )
    if outcome.error is not None:
        logger.debug(
            "storage_drain: drain trigger skipped — tasks table "
            "not available in schema %r (normal during staged rollout).",
            task_schema,
            exc_info=outcome.error,
        )


async def _enqueue_storage_id_only(
    conn: Any,
    *,
    catalog_id: str,
    rows: Sequence[OutboxRecord],
) -> None:
    """Insert id-only obligations into ``tasks.storage`` (#2494 P1).

    An id-only row carries ONLY the entity identifier — the drain
    (``StorageDrainTask``) re-reads canonical PG state for these rows at
    replay time instead of indexing a payload snapshot taken at enqueue
    time (see ``storage_drain_task.py``). ``tasks.storage`` has no payload
    column: a row is classified structurally — ``write_id`` set means a
    write-id batch reference, ``entity_id`` set means id-only.

    ``entity_kind`` defaults to ``'item'`` for the current items tier.
    # TODO(#1807 P1.3): branch on entity_kind for collection/catalog/asset tiers.
    """
    if not rows:
        return
    from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler
    from dynastore.modules.tasks.tasks_module import get_task_schema

    task_schema = get_task_schema()
    validate_sql_identifier(task_schema)
    insert_sql = (
        f"INSERT INTO {task_schema}.storage ("
        "    op_id, day, catalog_id, driver_id, collection_id,"
        "    entity_kind, entity_id, op, idempotency_key"
        ") VALUES ("
        "    :op_id, CURRENT_DATE, :catalog_id, :driver_id,"
        "    :collection_id, 'item', :entity_id,"
        "    :op, :idempotency_key"
        ")"
    )
    query = DQLQuery(insert_sql, result_handler=ResultHandler.NONE)
    for r in rows:
        await query.execute(
            conn,
            op_id=str(r.op_id),
            catalog_id=catalog_id,
            driver_id=r.driver_id,
            collection_id=r.collection_id,
            entity_id=r.item_id,
            op=r.op,
            idempotency_key=r.idempotency_key,
        )


async def _enqueue_storage_write_id(
    conn: Any,
    *,
    catalog_id: str,
    rows: Sequence[WriteIdOutboxRecord],
) -> None:
    """Insert write-id ledger rows into ``tasks.storage``.

    These rows represent a logical primary write batch for one secondary
    target, not a payload copy.  ``write_id`` is a first-class ledger column;
    ``entity_id`` stays NULL — the drain hydrates the batch from the primary
    driver by ``write_id``.
    """
    if not rows:
        return
    from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler
    from dynastore.modules.tasks.tasks_module import get_task_schema

    task_schema = get_task_schema()
    validate_sql_identifier(task_schema)
    insert_sql = (
        f"INSERT INTO {task_schema}.storage ("
        "    op_id, day, catalog_id, driver_id, collection_id,"
        "    entity_kind, entity_id, op, write_id, idempotency_key"
        ") VALUES ("
        "    :op_id, CURRENT_DATE, :catalog_id, :driver_id,"
        "    :collection_id, 'item', NULL,"
        "    :op, :write_id, :idempotency_key"
        ")"
    )
    query = DQLQuery(insert_sql, result_handler=ResultHandler.NONE)
    for r in rows:
        await query.execute(
            conn,
            op_id=str(r.op_id),
            catalog_id=catalog_id,
            driver_id=r.driver_id,
            collection_id=r.collection_id,
            op=r.op,
            write_id=r.write_id,
            idempotency_key=r.idempotency_key,
        )


def _coalesce_id_only_rows(rows: Sequence[OutboxRecord]) -> List[OutboxRecord]:
    """Coalesce id-only rows by ``(driver_id, collection_id, item_id)``.

    Last op wins within the batch: two upserts of the same id collapse to
    one row, and an upsert immediately followed by a delete of the same id
    (or vice versa) collapses to whichever came last — dict insertion order
    means "last" is "last in ``rows``". ``catalog_id`` is not part of the
    key: a single :func:`enqueue_storage_op_id_only` call always writes one
    catalog's rows, so it is already constant across the batch.
    """
    coalesced: Dict[Tuple[str, Optional[str], Optional[str]], OutboxRecord] = {}
    for r in rows:
        coalesced[(r.driver_id, r.collection_id, r.item_id)] = r
    return list(coalesced.values())


async def enqueue_storage_op_id_only(
    conn: Any,
    *,
    catalog_id: str,
    rows: Sequence[OutboxRecord],
) -> None:
    """Write id-only obligations into ``tasks.storage`` and enqueue the drain trigger.

    Used by the
    :class:`~dynastore.modules.storage.index_dispatcher.IndexDispatcher` for
    item-tier INDEX-lane entries — unconditionally, items INDEX
    materialization is storage-plane-always by design (#2494 WP-I). Each
    row carries only the entity id — the drain re-reads
    the canonical PG row for each id at replay time instead of replaying
    a payload snapshot, so the queued obligation can never go stale.

    Rows are coalesced within this call via :func:`_coalesce_id_only_rows`
    before the INSERT; cross-call duplicates are NOT deduplicated (there is
    no DB unique index on ``tasks.storage``) — the re-read-canonical-state
    drain design makes a duplicate a harmless repeat of the same read.

    Rides ``conn`` (the caller's open transaction) so a primary-write
    rollback leaves no rows in either ``tasks.storage`` or ``tasks.tasks``.
    """
    coalesced = _coalesce_id_only_rows(rows)
    if not coalesced:
        return
    await _enqueue_storage_id_only(conn, catalog_id=catalog_id, rows=coalesced)
    await _enqueue_drain_trigger(conn)


def _coalesce_write_id_rows(
    rows: Sequence[WriteIdOutboxRecord],
) -> List[WriteIdOutboxRecord]:
    """Coalesce write-id rows by ``(driver_id, collection_id, op, write_id)``."""
    coalesced: Dict[Tuple[str, Optional[str], str, str], WriteIdOutboxRecord] = {}
    for r in rows:
        coalesced[(r.driver_id, r.collection_id, r.op, r.write_id)] = r
    return list(coalesced.values())


async def enqueue_storage_op_write_id(
    conn: Any,
    *,
    catalog_id: str,
    rows: Sequence[WriteIdOutboxRecord],
) -> None:
    """Write lightweight write-id obligations into ``tasks.storage``.

    One row can represent a whole PG-primary write batch for one secondary
    target, collection and op.  The drain rehydrates by ``write_id`` from the
    primary driver, so no feature payloads are copied into ``tasks.storage``.
    """
    coalesced = _coalesce_write_id_rows(rows)
    if not coalesced:
        return
    await _enqueue_storage_write_id(conn, catalog_id=catalog_id, rows=coalesced)
    await _enqueue_drain_trigger(conn)
