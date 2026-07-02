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
import json
import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple

from dynastore.models.protocols.indexing import (
    STORAGE_PLANE_ID_ONLY_MARKER_KEY,
    OutboxRecord,
)
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.identifiers import generate_uuidv7

logger = logging.getLogger(__name__)


async def _enqueue_storage(
    conn: Any,
    *,
    catalog_id: str,
    rows: Sequence[OutboxRecord],
) -> None:
    """Insert outbox records into the global ``tasks.storage`` on ``conn``.

    Runs a schema-qualified parameterised ``INSERT`` per row on the caller's
    open SQLAlchemy transaction.  ``catalog_id`` is the tenant's logical
    identifier; it is bound as a column VALUE, never interpolated into SQL.
    ``day`` is ``CURRENT_DATE`` so the row lands in today's daily leaf (or
    the DEFAULT partition on a gap day).

    The ``entity_kind`` defaults to ``'item'`` for the current items tier.
    ``entity_id`` carries the item identifier (``r.item_id``).
    # TODO(#1807 P1.3): branch on entity_kind for collection/catalog/asset tiers.
    """
    if not rows:
        return
    from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler
    from dynastore.modules.tasks.tasks_module import get_task_schema

    task_schema = get_task_schema()
    # Defence-in-depth: the schema name comes from a trusted env default but is
    # placed in identifier position, so validate it like every other qualifier.
    validate_sql_identifier(task_schema)
    insert_sql = (
        f"INSERT INTO {task_schema}.storage ("
        "    op_id, day, catalog_id, driver_id, collection_id,"
        "    entity_kind, entity_id, op, op_payload, idempotency_key"
        ") VALUES ("
        "    :op_id, CURRENT_DATE, :catalog_id, :driver_id,"
        "    :collection_id, 'item', :entity_id,"
        "    :op, CAST(:op_payload AS jsonb), :idempotency_key"
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
            op_payload=json.dumps(r.payload),
            idempotency_key=r.idempotency_key,
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

    Same INSERT shape as :func:`_enqueue_storage`, except ``op_payload`` is
    always the explicit sentinel ``{STORAGE_PLANE_ID_ONLY_MARKER_KEY: true}``
    regardless of ``r.payload`` — the drain (``StorageDrainTask``) re-reads
    canonical PG state for these rows at replay time instead of indexing a
    payload snapshot taken at enqueue time (see ``storage_drain_task.py``).

    The marker is an explicit key, not a bare ``{}``: the ``tasks.storage``
    DDL default for ``op_payload`` is ALSO ``'{}'::jsonb``, so a genuinely
    empty payload (any producer that omits it) is indistinguishable from an
    id-only row on emptiness alone. The drain keys off the marker key, not
    payload emptiness (review finding, #2494).
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
        "    entity_kind, entity_id, op, op_payload, idempotency_key"
        ") VALUES ("
        "    :op_id, CURRENT_DATE, :catalog_id, :driver_id,"
        "    :collection_id, 'item', :entity_id,"
        "    :op, CAST(:op_payload AS jsonb), :idempotency_key"
        ")"
    )
    id_only_payload = json.dumps({STORAGE_PLANE_ID_ONLY_MARKER_KEY: True})
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
            op_payload=id_only_payload,
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

    The storage-plane counterpart of :func:`enqueue_storage_op` used by the
    :class:`~dynastore.modules.storage.index_dispatcher.IndexDispatcher` for
    ASYNC secondary-index ``WRITE`` entries when
    ``TasksPluginConfig.items_secondary_via_storage_plane`` is enabled
    (#2494 P1). Every row's ``op_payload`` is forced to the explicit
    ``{STORAGE_PLANE_ID_ONLY_MARKER_KEY: true}`` sentinel — the drain
    re-reads the canonical PG row for each id at replay time instead of
    replaying a payload snapshot, so the queued obligation can never go
    stale.

    Rows are coalesced within this call via :func:`_coalesce_id_only_rows`
    before the INSERT; cross-call duplicates are NOT deduplicated (there is
    no DB unique index on ``tasks.storage``) — the re-read-canonical-state
    drain design makes a duplicate a harmless repeat of the same read.

    Rides ``conn`` (the caller's open transaction) for the same atomicity
    guarantee as :func:`enqueue_storage_op`: a primary-write rollback
    leaves no rows in either ``tasks.storage`` or ``tasks.tasks``.
    """
    coalesced = _coalesce_id_only_rows(rows)
    if not coalesced:
        return
    await _enqueue_storage_id_only(conn, catalog_id=catalog_id, rows=coalesced)
    await _enqueue_drain_trigger(conn)


async def enqueue_storage_op(
    conn: Any,
    *,
    catalog_id: str,
    rows: Sequence[OutboxRecord],
) -> None:
    """Write storage rows into ``tasks.storage`` and enqueue the drain trigger.

    The single dispatch point for the storage-plane write, shared by the
    upsert seam (``ItemService.upsert_bulk``) and the delete twin
    (``ItemService._enqueue_index_deletes``). Both writes ride ``conn`` (the
    caller's open item-write transaction), so a failure rolls back the
    primary write atomically.

    ``catalog_id`` is the tenant's logical identifier; it is stored as a
    column value in ``tasks.storage``, providing tenancy without requiring a
    per-tenant table.

    A dedup'd ``storage_drain`` PENDING task row is also inserted on the same
    ``conn`` so the drain is triggered co-transactionally with the work rows.
    """
    if not rows:
        return
    await _enqueue_storage(conn, catalog_id=catalog_id, rows=rows)
    await _enqueue_drain_trigger(conn)
