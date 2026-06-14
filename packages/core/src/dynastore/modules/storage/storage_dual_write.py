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

# dynastore/modules/storage/storage_dual_write.py

"""Storage-plane dual-write into the global ``tasks.storage`` table.

The legacy ES-index outbox is a per-tenant ``{schema}.storage_outbox`` table
written co-transactionally with the item write — ``pg_outbox.enqueue_bulk`` on
the caller's open transaction (see ``ItemService.upsert_bulk`` and the delete
twin ``ItemService._enqueue_index_deletes``). The WorkClass migration (#1807)
introduces a single GLOBAL ``tasks.storage`` table where tenancy is carried
by a ``catalog_id`` column instead of the table's physical location.

During cutover the same outbox records are written to both tables, gated
per-plane by :class:`WorkClassConfig.emit_target_storage`:

* ``legacy`` (default) — only ``{schema}.storage_outbox`` (today's behaviour).
* ``both``   — both tables, on the same transaction.
* ``new``    — only ``tasks.storage``.

This module owns the new-table writer and the emit-target resolution so the
seam stays a thin one-call dispatch and the legacy/new split lives in one
place that Phase C can collapse. The new-table INSERT is schema-qualified to
the global tasks schema (``get_task_schema()``) so it lands correctly
regardless of the caller connection's tenant-pinned ``search_path``; it rides
the caller's transaction, so a failed enqueue rolls back the primary write —
the same atomicity guarantee the legacy outbox provides.
"""
import json
import logging
from typing import Any, Sequence, Tuple

from dynastore.models.protocols.indexing import OutboxRecord
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.identifiers import generate_uuidv7

logger = logging.getLogger(__name__)


async def _resolve_storage_emit_targets(configs: Any) -> Tuple[bool, bool]:
    """Return ``(write_legacy, write_new)`` for the storage plane.

    Fail-safe to ``(True, False)`` — legacy-only, exactly today's behaviour —
    on a missing ConfigsProtocol or ANY resolution error. The storage plane is
    the atomicity-critical seam in the item-write path: a config lookup must
    never break a write nor silently drop the legacy outbox row.
    """
    if configs is None:
        return True, False
    try:
        from dynastore.modules.tasks.workclass_config import WorkClassConfig

        cfg = await configs.get_config(WorkClassConfig)
        if isinstance(cfg, WorkClassConfig):
            return cfg.emit_storage_to_legacy, cfg.emit_storage_to_new
    except Exception:  # noqa: BLE001 — degrade to legacy-only, never fail the write
        logger.debug(
            "WorkClassConfig resolution failed; storage dual-write stays "
            "legacy-only for this write.",
            exc_info=True,
        )
    return True, False


async def _enqueue_storage(
    conn: Any,
    *,
    catalog_id: str,
    rows: Sequence[OutboxRecord],
) -> None:
    """Insert outbox records into the global ``tasks.storage`` on ``conn``.

    Runs a schema-qualified parameterised ``INSERT`` per row on the caller's
    open SQLAlchemy transaction (the co-transactional path), mirroring the
    legacy ``pg_outbox.enqueue_bulk`` caller-conn branch. ``catalog_id`` is
    the tenant's logical identifier (the value the legacy outbox uses as the
    catalog identity); it is bound as a column VALUE, never interpolated into
    SQL. ``day`` is ``CURRENT_DATE`` so the row lands in today's daily leaf
    (or the DEFAULT partition on a gap day).

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


async def _enqueue_drain_trigger(conn: Any) -> None:
    """Insert one global dedup'd ``storage_drain`` PENDING task on ``conn``.

    Co-transactional: the drain row commits if and only if the caller's work
    rows commit. A single global dedup key ensures high write volume coalesces
    to one pending drain regardless of which tenant triggered the write.  The
    ``on_task_insert`` DB trigger fires ``NOTIFY new_task_queued`` on this
    INSERT, waking the dispatcher without requiring a new connection or LISTEN.

    Degrades gracefully when the tasks table does not exist (e.g. test
    environments that only provision storage): emits a DEBUG log and
    returns without raising. The storage rows are still committed; the
    drain will run on its next scheduled tick even without this NOTIFY trigger.
    """
    from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler
    from dynastore.modules.tasks.tasks_module import get_task_schema

    task_schema = get_task_schema()
    validate_sql_identifier(task_schema)

    # execution_mode uses the column-correct value 'ASYNCHRONOUS' (the column
    # DEFAULT and the value recognised by the dispatcher). The spec draft used
    # 'ASYNC' which is not a valid enum value in the tasks table.
    insert_sql = (
        f"INSERT INTO {task_schema}.tasks"
        f" (task_id, schema_name, scope, task_type, type, execution_mode,"
        f"  inputs, timestamp, status, dedup_key)"
        f" SELECT :task_id, 'platform', 'platform', 'storage_drain',"
        f"        'task', 'ASYNCHRONOUS', '{{}}'::jsonb, now(), 'PENDING',"
        f"        'storage_drain'"
        f" WHERE NOT EXISTS ("
        f"     SELECT 1 FROM {task_schema}.tasks"
        f"     WHERE dedup_key = 'storage_drain'"
        f"       AND schema_name = 'platform'"
        # Full terminal set (matches the claim query in tasks_module). A
        # DISMISSED (terminal) drain task must NOT block a fresh enqueue —
        # otherwise the co-transactional NOTIFY trigger stays silenced until
        # manual cleanup. CREATED/PENDING/ACTIVE are non-terminal and DO block
        # (one live drain suffices).
        f"       AND status NOT IN ('COMPLETED', 'FAILED', 'DISMISSED', 'DEAD_LETTER')"
        f" )"
    )
    try:
        # Use a nested transaction (SAVEPOINT) so a missing-tasks-table error
        # does not abort the outer PG transaction carrying the storage rows.
        # A bare try/except does not help here: once asyncpg sees a statement
        # error the outer PG TX enters the aborted state and must be rolled
        # back in its entirety. The SAVEPOINT isolates the trigger INSERT so
        # only it is rolled back on failure, leaving the work rows intact.
        #
        # ``conn.begin_nested()`` is only available on a SQLAlchemy
        # AsyncConnection (not on an asyncpg connection or a raw SA
        # AsyncEngine). We probe for the attribute and fall back to a fire-
        # and-forget attempt if the caller's conn type does not support it.
        begin_nested = getattr(conn, "begin_nested", None)
        if begin_nested is not None:
            try:
                async with begin_nested():
                    await DQLQuery(insert_sql, result_handler=ResultHandler.NONE).execute(
                        conn, task_id=generate_uuidv7()
                    )
            except Exception:  # noqa: BLE001
                logger.debug(
                    "storage_drain: drain trigger skipped — tasks table "
                    "not available in schema %r (normal during staged rollout).",
                    task_schema,
                    exc_info=True,
                )
        else:
            # Conn type doesn't support nested transactions — attempt
            # the INSERT directly. If it fails the outer TX aborts;
            # production always uses SA AsyncConnection so this branch
            # is a defensive fallback only.
            await DQLQuery(insert_sql, result_handler=ResultHandler.NONE).execute(
                conn, task_id=generate_uuidv7()
            )
    except Exception:  # noqa: BLE001
        # Last-resort catch: savepoint setup itself failed.
        logger.debug(
            "storage_drain: drain trigger failed for schema %r.",
            task_schema,
            exc_info=True,
        )


async def dispatch_storage_dual_write(
    conn: Any,
    *,
    outbox: Any,
    catalog_id: str,
    rows: Sequence[OutboxRecord],
    configs: Any,
) -> None:
    """Write storage-outbox ``rows`` to the legacy and/or new table per config.

    The single dispatch point for the storage-plane dual-write, shared by the
    upsert seam (``ItemService.upsert_bulk``) and the delete twin
    (``ItemService._enqueue_index_deletes``). Both writes ride ``conn`` (the
    caller's open item-write transaction), so either failing rolls back the
    primary write. With the default config this calls only
    ``outbox.enqueue_bulk`` — byte-for-byte today's path.

    ``catalog_id`` is the tenant's logical identifier (the outbox convention);
    it is the legacy table's namespace AND the new table's ``catalog_id``.

    When ``write_new`` is true, a dedup'd ``storage_drain`` PENDING task
    row is also inserted on the same ``conn`` so the drain is triggered
    co-transactionally with the work rows (no permanent connection, no LISTEN
    required — the ``on_task_insert`` trigger emits NOTIFY automatically).
    """
    if not rows:
        return
    write_legacy, write_new = await _resolve_storage_emit_targets(configs)
    if write_legacy:
        await outbox.enqueue_bulk(conn, catalog_id=catalog_id, rows=rows)
    if write_new:
        await _enqueue_storage(conn, catalog_id=catalog_id, rows=rows)
        await _enqueue_drain_trigger(conn)
