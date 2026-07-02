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

# dynastore/modules/tasks/events/events_emit.py
"""Event emit path — direct write to ``tasks.events``.

``emit_event_row`` is the single point of control for the event emit path.
It inserts the event row into the ``tasks.events`` partitioned table and
co-transactionally enqueues one dedup'd ``event_drain`` task on the same
connection so the ``EventDrainTask`` is woken via the existing
``on_task_insert`` NOTIFY path.

Scope normalisation
-------------------
``scope`` is stored lowercase in ``tasks.events`` (enforced by a PG CHECK
constraint).  The caller may pass the value in any case; it is lowercased
here before the INSERT.

Query caching
-------------
The ``DQLQuery`` for the tasks.events INSERT is built once per
``task_schema`` name and cached.  The schema name comes from
``get_task_schema()`` (a process-constant env value), so in practice one
cache entry covers the entire process lifetime.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from dynastore.modules.db_config.query_executor import DQLQuery

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# tasks.events INSERT query, cached per schema name.
# ---------------------------------------------------------------------------

_EVENTS_INSERT_SQL = """
INSERT INTO {task_schema}.events (
    event_id,
    day,
    shard,
    catalog_id,
    scope,
    event_type,
    payload
) VALUES (
    CAST(:event_id AS uuid),
    CURRENT_DATE,
    :shard,
    :catalog_id,
    :scope,
    :event_type,
    CAST(:payload AS jsonb)
)
"""

# Cache of built INSERT queries keyed by task schema (one entry in practice).
_EVENTS_INSERT_QUERY_CACHE: Dict[str, "DQLQuery"] = {}


def _events_insert_query(task_schema: str) -> "DQLQuery":
    """Return a cached ``DQLQuery`` for the tasks.events INSERT in ``task_schema``.

    Built once per schema and reused.  ``task_schema`` lands in identifier
    position via ``.format``; it is validated as a SQL identifier for
    defence-in-depth (the value comes from a trusted env default, but every
    other schema qualifier in the codebase is validated the same way).
    """
    query = _EVENTS_INSERT_QUERY_CACHE.get(task_schema)
    if query is None:
        from dynastore.modules.db_config.query_executor import (  # noqa: PLC0415
            DQLQuery,
            ResultHandler,
        )
        from dynastore.tools.db import validate_sql_identifier  # noqa: PLC0415

        validate_sql_identifier(task_schema)
        sql = _EVENTS_INSERT_SQL.format(task_schema=task_schema)
        query = DQLQuery(sql, result_handler=ResultHandler.NONE)
        _EVENTS_INSERT_QUERY_CACHE[task_schema] = query
    return query


async def _enqueue_event_drain_trigger(
    conn: Any, *, wedge_grace_seconds: Optional[float] = None,
) -> None:
    """Insert one global dedup'd ``event_drain`` PENDING task on ``conn``.

    Co-transactional: the drain row commits if and only if the caller's event
    row commits.  A single global dedup key coalesces high event volume to one
    pending drain.  The ``on_task_insert`` DB trigger fires
    ``NOTIFY new_task_queued`` on this INSERT, waking the dispatcher without a
    dedicated LISTEN connection.

    Degrades gracefully when the tasks table is absent (e.g. test environments
    that only provision ``tasks.events``): the INSERT is SAVEPOINT-isolated via
    :func:`best_effort_savepoint` so a missing table cannot abort the outer PG
    transaction carrying the event row, and any failure is logged at DEBUG and
    swallowed.  The event row still commits; the drain runs on its next
    scheduled tick even without this NOTIFY.

    ``wedge_grace_seconds`` (#2715): shared by both callers — the hot
    co-transactional write path above (always ``None``) and the leader-side
    recovery tick (``dynastore.modules.tasks.drain_spawner``, which passes a
    configured float). Mirrors ``storage_emit._enqueue_drain_trigger``'s
    parameter exactly:

    * ``None`` (default): unchanged behaviour — ANY existing non-terminal
      (PENDING/ACTIVE/CREATED) ``event_drain`` row blocks a fresh enqueue.
    * A float: additionally tolerates a WEDGED existing row — a PENDING row
      older than ``wedge_grace_seconds`` or an ACTIVE row whose lease
      (``locked_until``) has already expired. A live row still blocks,
      exactly as before.
    """
    from dynastore.modules.db_config.query_executor import (  # noqa: PLC0415
        DQLQuery,
        ResultHandler,
        best_effort_savepoint,
    )
    from dynastore.modules.tasks.tasks_module import get_task_schema  # noqa: PLC0415
    from dynastore.tools.caller import current_caller_id  # noqa: PLC0415
    from dynastore.tools.db import validate_sql_identifier  # noqa: PLC0415
    from dynastore.tools.identifiers import generate_uuidv7  # noqa: PLC0415

    task_schema = get_task_schema()
    validate_sql_identifier(task_schema)

    # Stamp the originating caller so the drain row records who triggered it —
    # the authenticated principal when this runs inside a request, the system
    # id otherwise. RunnerContext.caller_id is NOT NULL-able (min_length=1), so
    # a NULL here would crash dispatch before the drain ever runs.
    caller_id = current_caller_id()

    # See storage_emit._enqueue_drain_trigger's docstring for the identical
    # wedge-tolerance rationale.
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

    insert_sql = (
        f"INSERT INTO {task_schema}.tasks"
        f" (task_id, catalog_id, scope, caller_id, task_type, type,"
        f"  execution_mode, inputs, timestamp, status, dedup_key)"
        f" SELECT :task_id, 'platform', 'platform', :caller_id, 'event_drain',"
        f"        'task', 'ASYNCHRONOUS', '{{}}'::jsonb, now(), 'PENDING',"
        f"        'event_drain'"
        f" WHERE NOT EXISTS ("
        f"     SELECT 1 FROM {task_schema}.tasks"
        f"     WHERE dedup_key = 'event_drain'"
        f"       AND catalog_id = 'platform'"
        # Terminal set matches the dispatcher's claim query: a terminal-state
        # drain task (incl. DISMISSED) must NOT block a fresh enqueue, or the
        # co-transactional NOTIFY stays silenced until manual cleanup.
        f"       AND status NOT IN ('COMPLETED', 'FAILED', 'DISMISSED', 'DEAD_LETTER')"
        f"{wedge_tolerant_clause}"
        f" )"
    )
    params: Dict[str, Any] = {"task_id": generate_uuidv7(), "caller_id": caller_id}
    if wedge_grace_seconds is not None:
        params["wedge_grace_seconds"] = wedge_grace_seconds

    async with best_effort_savepoint(conn) as outcome:
        await DQLQuery(insert_sql, result_handler=ResultHandler.NONE).execute(
            conn, **params
        )
    if outcome.error is not None:
        logger.debug(
            "event_drain: drain trigger skipped — tasks table not "
            "available in schema %r (normal during staged rollout).",
            task_schema,
            exc_info=outcome.error,
        )


async def emit_event_row(
    conn: Any,
    *,
    event_type: str,
    scope: str,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    identity_id: Optional[str],
    payload_str: str,
    shard: int,
) -> str:
    """Emit a domain event row to ``tasks.events`` and enqueue the drain trigger.

    Both the event INSERT and the drain-trigger INSERT execute on ``conn`` so
    they are co-transactional with the caller's outer ``managed_transaction``
    context.  Any exception propagates unmodified so the caller's transaction
    rolls back atomically.

    Parameters
    ----------
    conn:
        An open SQLAlchemy ``AsyncConnection`` from
        ``managed_transaction(engine)``.
    event_type:
        The event type label (e.g. ``"catalog_creation"``).
    scope:
        The event scope as the caller provides it (e.g. ``"PLATFORM"``).
        Lowercased before the INSERT to satisfy the ``tasks.events`` CHECK
        constraint (``scope = lower(scope)``).
    catalog_id:
        Catalog internal id (after identity collapse, the physical PG schema
        name equals the catalog internal id); ``None`` means platform-wide.
    collection_id:
        Collection identifier, or ``None``; stored in ``payload`` if needed
        by listeners — the ``tasks.events`` schema does not have a dedicated
        column for it.
    identity_id:
        Identity identifier, or ``None``; same note as ``collection_id``.
    payload_str:
        JSON-serialised event payload (already serialised by the caller).
    shard:
        Pre-computed shard value (``abs(hash(catalog_id or "PLATFORM")) % 16``).

    Returns
    -------
    str
        The Python-generated UUIDv7 event_id inserted into ``tasks.events``.
    """
    from dynastore.modules.tasks.tasks_module import get_task_schema  # noqa: PLC0415
    from dynastore.tools.identifiers import generate_uuidv7  # noqa: PLC0415

    task_schema = get_task_schema()
    event_id = str(generate_uuidv7())
    scope_lower = (scope or "platform").lower()

    await _events_insert_query(task_schema).execute(
        conn,
        event_id=event_id,
        shard=shard,
        catalog_id=catalog_id,
        scope=scope_lower,
        event_type=event_type,
        payload=payload_str,
    )

    # Co-transactional drain trigger (Option A): enqueue one global dedup'd
    # ``event_drain`` PENDING task on the caller's own connection so the drain
    # is woken via the existing ``on_task_insert`` -> NOTIFY path without
    # holding a permanent LISTEN connection per tenant.
    await _enqueue_event_drain_trigger(conn)

    return event_id
