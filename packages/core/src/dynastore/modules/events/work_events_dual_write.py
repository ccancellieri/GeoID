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

# dynastore/modules/events/work_events_dual_write.py
"""Event-plane dual-write seam for the WorkClass durable-work unification.

``dispatch_event_dual_write`` is the single point of control for the event
emit path during the WorkClass cutover (#1807).  It sits between the public
``EventsModule.publish`` API and the two physical tables:

* **Legacy path** — ``events.events`` (the existing global outbox).  Active
  whenever ``WorkClassConfig.emit_events_to_legacy`` is True (the default).
  The legacy ``_publish_query`` write and the new ``tasks.work_events`` write
  share the same ``conn`` so they participate in the same transaction and roll
  back together on any failure.

* **New path** — ``tasks.work_events`` (the WorkClass global hot plane).
  Active whenever ``WorkClassConfig.emit_events_to_new`` is True.

The default ``WorkClassConfig`` has ``emit_target_events = EmitTarget.LEGACY``,
so ``emit_events_to_legacy=True`` and ``emit_events_to_new=False``.  This is
the fail-safe state: the legacy write runs unchanged, the new table is not
touched.

Fail-safe guarantee
-------------------
Any error resolving ``WorkClassConfig`` (missing ``ConfigsProtocol``, DB
error, import error) collapses to ``(write_legacy=True, write_new=False)``
inside a bare ``except``.  A config-resolution failure MUST NOT prevent the
legacy event from being emitted.

Scope normalisation
-------------------
``scope`` is stored as-is in the legacy ``events.events`` table (e.g.
``"PLATFORM"``).  The new ``tasks.work_events`` table has a PG CHECK
constraint requiring ``scope = lower(scope)``, so the value is lowercased
**only** for the work_events INSERT.

Phase-C deletion
----------------
This module is the entire event-plane dual-write seam.  When the migration
reaches the ``new``-only steady state and the legacy table is decommissioned,
remove this module and revert ``EventsModule.publish`` to call
``_publish_query`` directly.  There are no other callers.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.modules.db_config.query_executor import DQLQuery

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# work_events INSERT query. The {task_schema} qualifier is substituted once
# per process (the schema name is a process-constant env value) and the built
# DQLQuery is cached, mirroring the module-level ``_publish_query`` singleton
# on the legacy path — rebuilding per publish would add allocation + a
# TemplateQueryBuilder scan on the event hot path during cutover.
# ---------------------------------------------------------------------------

_WORK_EVENTS_INSERT_SQL = """
INSERT INTO {task_schema}.work_events (
    event_id,
    day,
    shard,
    schema_name,
    scope,
    event_type,
    payload
) VALUES (
    CAST(:event_id AS uuid),
    CURRENT_DATE,
    :shard,
    :schema_name,
    :scope,
    :event_type,
    CAST(:payload AS jsonb)
)
"""

# Cache of built INSERT queries keyed by task schema (one entry in practice).
_WORK_EVENTS_INSERT_QUERY_CACHE: Dict[str, "DQLQuery"] = {}


def _work_events_insert_query(task_schema: str) -> "DQLQuery":
    """Return a cached ``DQLQuery`` for the work_events INSERT in ``task_schema``.

    Built once per schema and reused, like the legacy ``_publish_query``
    singleton. ``task_schema`` lands in identifier position via ``.format``,
    so it is validated as an identifier for defence-in-depth (it comes from a
    trusted env default, but every other schema qualifier in the codebase is
    validated the same way).
    """
    query = _WORK_EVENTS_INSERT_QUERY_CACHE.get(task_schema)
    if query is None:
        from dynastore.modules.db_config.query_executor import (  # noqa: PLC0415
            DQLQuery,
            ResultHandler,
        )
        from dynastore.tools.db import validate_sql_identifier  # noqa: PLC0415

        validate_sql_identifier(task_schema)
        sql = _WORK_EVENTS_INSERT_SQL.format(task_schema=task_schema)
        query = DQLQuery(sql, result_handler=ResultHandler.NONE)
        _WORK_EVENTS_INSERT_QUERY_CACHE[task_schema] = query
    return query


async def _enqueue_event_drain_trigger(conn: Any) -> None:
    """Insert one global dedup'd ``work_event_drain`` PENDING task on ``conn``.

    Co-transactional twin of the storage plane's
    ``storage_dual_write._enqueue_drain_trigger``: the drain row commits if
    and only if the caller's event row commits.  A single global dedup key
    coalesces high event volume to one pending drain.  The ``on_task_insert``
    DB trigger fires ``NOTIFY new_task_queued`` on this INSERT, waking the
    dispatcher without a dedicated LISTEN connection.

    Degrades gracefully when the tasks table is absent (e.g. test environments
    that only provision ``work_events``): the INSERT is SAVEPOINT-isolated via
    ``conn.begin_nested()`` so a missing table cannot abort the outer PG
    transaction carrying the event row, and any failure is logged at DEBUG and
    swallowed.  The event row still commits; the drain runs on its next
    scheduled tick even without this NOTIFY.
    """
    from dynastore.modules.db_config.query_executor import (  # noqa: PLC0415
        DQLQuery,
        ResultHandler,
    )
    from dynastore.modules.tasks.tasks_module import get_task_schema  # noqa: PLC0415
    from dynastore.tools.db import validate_sql_identifier  # noqa: PLC0415
    from dynastore.tools.identifiers import generate_uuidv7  # noqa: PLC0415

    task_schema = get_task_schema()
    validate_sql_identifier(task_schema)

    insert_sql = (
        f"INSERT INTO {task_schema}.tasks"
        f" (task_id, schema_name, scope, task_type, type, execution_mode,"
        f"  inputs, timestamp, status, dedup_key)"
        f" SELECT :task_id, 'platform', 'platform', 'work_event_drain',"
        f"        'task', 'ASYNCHRONOUS', '{{}}'::jsonb, now(), 'PENDING',"
        f"        'work_event_drain'"
        f" WHERE NOT EXISTS ("
        f"     SELECT 1 FROM {task_schema}.tasks"
        f"     WHERE dedup_key = 'work_event_drain'"
        f"       AND schema_name = 'platform'"
        # Terminal set matches the dispatcher's claim query: a terminal-state
        # drain task (incl. DISMISSED) must NOT block a fresh enqueue, or the
        # co-transactional NOTIFY stays silenced until manual cleanup.
        f"       AND status NOT IN ('COMPLETED', 'FAILED', 'DISMISSED', 'DEAD_LETTER')"
        f" )"
    )
    try:
        begin_nested = getattr(conn, "begin_nested", None)
        if begin_nested is not None:
            try:
                async with begin_nested():
                    await DQLQuery(insert_sql, result_handler=ResultHandler.NONE).execute(
                        conn, task_id=generate_uuidv7()
                    )
            except Exception:  # noqa: BLE001
                logger.debug(
                    "work_event_drain: drain trigger skipped — tasks table not "
                    "available in schema %r (normal during staged rollout).",
                    task_schema,
                    exc_info=True,
                )
        else:
            await DQLQuery(insert_sql, result_handler=ResultHandler.NONE).execute(
                conn, task_id=generate_uuidv7()
            )
    except Exception:  # noqa: BLE001
        logger.debug(
            "work_event_drain: drain trigger failed for schema %r.",
            task_schema,
            exc_info=True,
        )


async def dispatch_event_dual_write(
    conn: Any,
    *,
    event_type: str,
    scope: str,
    schema_name: Optional[str],
    catalog_id: Optional[str],
    collection_id: Optional[str],
    identity_id: Optional[str],
    payload_str: str,
    shard: int,
    configs: Optional["ConfigsProtocol"],
) -> str:
    """Emit a domain event to the legacy and/or new work_events table.

    Both writes execute on ``conn`` so they are co-transactional with the
    caller's outer ``managed_transaction`` context.  Any exception propagates
    unmodified so the caller's transaction rolls back atomically.

    Parameters
    ----------
    conn:
        An open SQLAlchemy ``AsyncConnection`` from
        ``managed_transaction(engine)``.
    event_type:
        The event type label (e.g. ``"catalog_creation"``).
    scope:
        The event scope as the caller provides it (e.g. ``"PLATFORM"``).
        Stored as-is in ``events.events``; lowercased for ``work_events``.
    schema_name:
        Tenant schema name; ``None`` means platform-wide.
    catalog_id:
        Catalog identifier, or ``None``.
    collection_id:
        Collection identifier, or ``None``.
    identity_id:
        Identity identifier, or ``None``.
    payload_str:
        JSON-serialised event payload (already serialised by the caller).
    shard:
        Pre-computed shard value (``abs(hash(catalog_id or "PLATFORM")) % 16``).
    configs:
        Resolved ``ConfigsProtocol`` instance, or ``None``.  When ``None``
        or when resolution raises, the legacy write runs and the new write
        is skipped (fail-safe).

    Returns
    -------
    str
        The ``event_id`` returned by the legacy INSERT (a UUID string), or
        the Python-generated UUID when only the new table was written to.
    """
    # --- Resolve emit targets (fail-safe to legacy-only) ------------------
    write_legacy = True
    write_new = False

    try:
        if configs is not None:
            # Lazy import to avoid cross-module import cycles at load time.
            from dynastore.modules.tasks.workclass_config import WorkClassConfig  # noqa: PLC0415

            cfg: WorkClassConfig = await configs.get_config(WorkClassConfig)
            write_legacy = cfg.emit_events_to_legacy
            write_new = cfg.emit_events_to_new
    except Exception:
        # Any failure — missing protocol, DB error, config not found — falls
        # back to legacy-only.  This must NEVER prevent the legacy write.
        logger.debug(
            "work_events_dual_write: could not resolve WorkClassConfig; "
            "falling back to legacy-only emit.",
            exc_info=True,
        )
        write_legacy = True
        write_new = False

    # --- Legacy write: events.events --------------------------------------
    legacy_event_id: Optional[str] = None

    if write_legacy:
        from dynastore.modules.events.events_module import _publish_query  # noqa: PLC0415

        legacy_event_id = await _publish_query.execute(
            conn,
            event_type=event_type,
            scope=scope,
            schema_name=schema_name,
            catalog_id=catalog_id,
            collection_id=collection_id,
            identity_id=identity_id,
            payload=payload_str,
            shard=shard,
        )

    # --- New write: tasks.work_events -------------------------------------
    new_event_id: Optional[str] = None

    if write_new:
        from dynastore.modules.tasks.tasks_module import get_task_schema  # noqa: PLC0415
        from dynastore.tools.identifiers import generate_uuidv7  # noqa: PLC0415

        task_schema = get_task_schema()
        new_event_id = str(generate_uuidv7())
        scope_lower = (scope or "platform").lower()

        await _work_events_insert_query(task_schema).execute(
            conn,
            event_id=new_event_id,
            shard=shard,
            schema_name=schema_name,
            scope=scope_lower,
            event_type=event_type,
            payload=payload_str,
        )

        # Co-transactional drain trigger (Option A): enqueue one global dedup'd
        # ``work_event_drain`` PENDING task on the caller's own connection so
        # the drain is woken via the existing ``on_task_insert`` -> NOTIFY path
        # without holding a permanent LISTEN connection per tenant. Mirrors the
        # storage plane (``storage_dual_write._enqueue_drain_trigger``).
        await _enqueue_event_drain_trigger(conn)

    # Return the canonical event_id: legacy if present, otherwise new.
    if legacy_event_id is not None:
        return legacy_event_id
    if new_event_id is not None:
        return new_event_id
    # Both paths off is not a reachable steady-state config, but guard
    # defensively so callers always get a string back.
    from dynastore.tools.identifiers import generate_uuidv7  # noqa: PLC0415

    return str(generate_uuidv7())
