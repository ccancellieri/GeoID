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

"""Aggregate outbox-backlog signal for the serving-path async-writer dispatch.

The ``storage_drain`` / ``event_drain`` system tasks (the generic secondary-
write drainers, see ``dynastore.tasks.workclass_drain``) run in-process on the
claiming pod by default — light load must not pay for an external job hop.
When the aggregate backlog across BOTH global outbox tables (``tasks.storage``
and ``tasks.events``) grows large, that in-process drain starts competing with
serving requests for the same DB pool. This module answers one question —
"is the aggregate backlog high right now?" — so :func:`execution.offload_required`
can bias dispatch toward an external ``async_writer`` Cloud Run Job when one is
deployed (see #2622).

Design notes
------------

* **Bounded, never a full COUNT(*).** Each probe query is capped at
  ``threshold + 1`` rows via a ``LIMIT``-wrapped subquery, so cost never grows
  with an already-large backlog — the exact case this signal exists to detect.
* **Reuses the shared serving engine.** The probe rides the same pool the
  in-process drain and every other request already shares (via
  :func:`dynastore.tools.protocol_helpers.get_engine`) rather than opening a
  dedicated connection, so it adds no new connection class to budget (#2582).
* **Cached with a short, fixed TTL.** A dispatch-time DB round trip on every
  claim tick would itself become the kind of background-vs-serving contention
  #2582 warned about. The raw count is cached for
  :data:`_BACKLOG_PROBE_TTL_SECONDS`; the *threshold* it is compared against
  stays hot-reloadable via ``TasksPluginConfig`` so operators can retune
  sensitivity without a restart.
* **Fail-open to "not high".** Any error (missing engine, missing tasks
  table, transient pool exhaustion) degrades to ``False`` — the existing
  in-process background path — never raises. A degraded signal must never
  make the dispatcher refuse to drain.
"""
from __future__ import annotations

import logging

from dynastore.tools.cache import cached

logger = logging.getLogger(__name__)

# Fixed cache TTL for the raw backlog count. Deliberately not hot-reloadable —
# it is an implementation constant bounding DB round trips, not a tunable
# operators need to retune; the actual dispatch-relevant knob is
# ``TasksPluginConfig.async_writer_backlog_threshold``.
_BACKLOG_PROBE_TTL_SECONDS: float = 10.0

# Rows scanned per probe query, capped independently of the configured
# threshold so a single misconfigured (very high) threshold can never turn
# this into an unbounded scan.
_PROBE_SCAN_CAP: int = 50_000


async def _capped_ready_count(conn, *, task_schema: str, cap: int) -> int:
    """Sum of ready ``tasks.storage`` rows + pending ``tasks.events`` rows,
    each capped at ``cap`` via a ``LIMIT``-wrapped subquery so the cost of
    this probe never scales with an already-large backlog.
    """
    from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler

    storage_sql = (
        f"SELECT count(*) FROM ("
        f"    SELECT 1 FROM {task_schema}.storage"
        f"    WHERE status = 'ready' AND ready_at <= now()"
        f"    LIMIT :cap"
        f") capped"
    )
    events_sql = (
        f"SELECT count(*) FROM ("
        f"    SELECT 1 FROM {task_schema}.events"
        f"    WHERE status = 'PENDING' AND (locked_until IS NULL OR locked_until <= now())"
        f"    LIMIT :cap"
        f") capped"
    )
    storage_count = await DQLQuery(
        storage_sql, result_handler=ResultHandler.SCALAR,
    ).execute(conn, cap=cap)
    events_count = await DQLQuery(
        events_sql, result_handler=ResultHandler.SCALAR,
    ).execute(conn, cap=cap)
    return int(storage_count or 0) + int(events_count or 0)


async def count_pending_item_ops(
    conn, *, task_schema: str, catalog_id: str, collection_id: str,
) -> int:
    """Count outstanding item-tier ``tasks.storage`` ops for one collection.

    Scoped by ``(catalog_id, collection_id, entity_kind='item')`` with
    ``status IN ('ready', 'in_flight')`` — the two non-terminal states an
    id-only obligation (#2494) can be in before ``storage_drain`` converges
    it. Unlike :func:`_capped_ready_count` (an uncapped, cross-tenant probe),
    this is a genuine per-collection COUNT — but honestly: neither existing
    index covers this predicate. ``idx_storage_fairness`` is
    ``(catalog_id, ready_at) WHERE status = 'ready'`` (no ``collection_id`` /
    ``entity_kind``, and it excludes ``in_flight`` rows entirely);
    ``idx_storage_driver`` leads with ``driver_id``, which isn't part of this
    query at all. There is also no ``day`` filter, so every partition is
    visited. This is a genuine partition-wide sequential-ish scan, kept safe
    only by the caller bounding it with a server-side ``statement_timeout``
    (see ``reconciliation.reconcile_secondary_indexing``) rather than by any
    index. A covering index on ``(catalog_id, collection_id, entity_kind,
    status)`` would fix this properly; deliberately not added here (no
    runtime DDL) — left as a DBA follow-up.

    Note this counts obligations for the COLLECTION, not the calling task
    specifically: concurrent ingestions into the same collection inflate each
    other's ``queued`` count. Harmless for the read-side convergence check —
    it only ever delays a flip to "converged", never flips early.

    Shared by the ingestion completion path (#2897, "is the write I just
    finished still pending secondary indexing") and the task-status read path
    (convergence flip on GET), so both sides can never drift on the query.
    """
    from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler

    sql = (
        f"SELECT count(*) FROM {task_schema}.storage"
        f" WHERE catalog_id = :catalog_id"
        f"   AND collection_id = :collection_id"
        f"   AND entity_kind = 'item'"
        f"   AND status IN ('ready', 'in_flight')"
    )
    result = await DQLQuery(sql, result_handler=ResultHandler.SCALAR).execute(
        conn, catalog_id=catalog_id, collection_id=collection_id,
    )
    return int(result or 0)


@cached(maxsize=1, ttl=_BACKLOG_PROBE_TTL_SECONDS, distributed=False)
async def _cached_backlog_depth() -> int:
    """Process-local, short-TTL snapshot of the aggregate outbox backlog depth.

    Returns 0 (never raises) when the engine, the tasks schema, or the query
    itself is unavailable — the caller treats an unreadable signal as "no
    backlog opinion", which fails open to the existing in-process path.
    """
    try:
        from dynastore.modules.db_config.query_executor import managed_transaction
        from dynastore.modules.tasks.tasks_module import get_task_schema
        from dynastore.tools.db import validate_sql_identifier
        from dynastore.tools.protocol_helpers import get_engine

        engine = get_engine()
        if engine is None:
            return 0
        task_schema = get_task_schema()
        validate_sql_identifier(task_schema)
        async with managed_transaction(engine) as conn:
            return await _capped_ready_count(
                conn, task_schema=task_schema, cap=_PROBE_SCAN_CAP,
            )
    except Exception:  # noqa: BLE001 — probe is best-effort, never raise
        logger.debug(
            "async_writer_backlog: depth probe failed — treating as 0 "
            "(fail-open to the in-process drain path).",
            exc_info=True,
        )
        return 0


async def _resolve_threshold() -> int:
    """Read ``TasksPluginConfig.async_writer_backlog_threshold``, hot-reloaded.

    Fails open to the field default when the platform configs protocol is
    unavailable (early startup, tests) so the signal degrades to "not high"
    rather than raising.
    """
    try:
        from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
        from dynastore.modules.tasks.tasks_config import TasksPluginConfig
        from dynastore.tools.discovery import get_protocol

        config_mgr = get_protocol(PlatformConfigsProtocol)
        if config_mgr is None:
            return TasksPluginConfig.model_fields["async_writer_backlog_threshold"].default
        cfg = await config_mgr.get_config(TasksPluginConfig)
        if isinstance(cfg, TasksPluginConfig):
            return cfg.async_writer_backlog_threshold
    except Exception:  # noqa: BLE001 — config read is best-effort
        logger.debug(
            "async_writer_backlog: threshold config unavailable — using default.",
            exc_info=True,
        )
    from dynastore.modules.tasks.tasks_config import TasksPluginConfig
    return TasksPluginConfig.model_fields["async_writer_backlog_threshold"].default


async def backlog_is_high() -> bool:
    """True when the aggregate outbox backlog exceeds the configured threshold.

    Consulted by :func:`dynastore.modules.tasks.execution.offload_required`
    for the backlog-adaptive system tasks (``storage_drain`` / ``event_drain``)
    so serving-path secondary writes route to the offloaded ``async_writer``
    Cloud Run Job (when one is deployed) instead of the in-process
    ``BackgroundRunner`` once the backlog grows large. Fail-open to ``False``
    on any error — the in-process path remains the default.
    """
    try:
        depth = await _cached_backlog_depth()
        threshold = await _resolve_threshold()
        return depth > threshold
    except Exception:  # noqa: BLE001 — signal is best-effort, never raise
        logger.debug(
            "async_writer_backlog: backlog_is_high evaluation failed — "
            "defaulting to False.",
            exc_info=True,
        )
        return False
