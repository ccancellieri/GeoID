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
from typing import Any, Sequence

from dynastore.models.protocols.indexing import OutboxRecord
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
        f" (task_id, catalog_id, scope, task_type, type, execution_mode,"
        f"  inputs, timestamp, status, dedup_key)"
        f" SELECT :task_id, 'platform', 'platform', 'storage_drain',"
        f"        'task', 'ASYNCHRONOUS', '{{}}'::jsonb, now(), 'PENDING',"
        f"        'storage_drain'"
        f" WHERE NOT EXISTS ("
        f"     SELECT 1 FROM {task_schema}.tasks"
        f"     WHERE dedup_key = 'storage_drain'"
        f"       AND catalog_id = 'platform'"
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
