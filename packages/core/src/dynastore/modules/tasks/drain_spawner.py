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

"""Leader-side RECOVERY tick for the ``event_drain`` / ``storage_drain``
system tasks (#2715).

Root cause
----------
Every event write (``events_emit.emit_event_row``) and every storage-plane
item write (``storage_emit.enqueue_storage_op`` /
``enqueue_storage_op_id_only``) already co-transactionally inserts one
dedup'd PENDING drain-trigger task row on the SAME connection as the work
row that triggered it — that mechanism already exists for BOTH drain types
and is NOT being reinvented here.

Live dev (2026-07-02) showed its fatal weakness: the trigger's dedup guard
(``WHERE NOT EXISTS (... status NOT IN <terminal set>)``) blocks a fresh
INSERT for as long as ANY non-terminal row exists — including a row that
can no longer make progress (a crash-looping task cycling
PENDING -> ACTIVE -> fail, or an ACTIVE row whose owner died and whose lease
has expired). Once that single row is wedged, every subsequent write's
co-transactional trigger silently no-ops (the guard correctly sees "a drain
already exists" and skips), and nothing else re-arms it — the outbox stays
unconsumed indefinitely, even after the wedged row eventually reaches
DEAD_LETTER, if no further write happens to retrigger the INSERT.

This module is the recovery path for exactly that gap: a leader-elected
periodic tick that, ONLY when the corresponding outbox actually has
undrained work, re-issues the SAME dedup'd INSERT the hot write path uses
(``_enqueue_event_drain_trigger`` / ``_enqueue_drain_trigger`` — reused
verbatim, not forked) with a wedge-tolerance window: a demonstrably wedged
existing row (stale PENDING, or ACTIVE with an expired lease) no longer
blocks the fresh INSERT, while a healthy in-flight drain still does.
"""

from __future__ import annotations

import logging
from typing import Any, Optional, Union

from dynastore.tools.background_service import (
    Leadership,
    PeriodicService,
    PodPolicy,
    ServiceContext,
)

logger = logging.getLogger(__name__)

# Mirrors StorageDrainTask._DEFAULT_LEASE_SECONDS / EventDrainTask's own
# lease default — an in_flight/PROCESSING row older than this has almost
# certainly lost its owner (the drain tasks themselves reclaim on the same
# threshold), so the backlog probe below treats it as still-undrained work.
_STALE_LEASE_SECONDS: int = 300


async def _storage_backlog_exists(engine: Any) -> bool:
    """Cheap ``EXISTS`` probe mirroring ``StorageDrainTask._claim_batch``'s
    claim predicate (ready rows, or in_flight rows whose lease expired).

    Fails open to ``False`` (no backlog opinion -> no spawn attempt this
    tick) on any error — a missing/unavailable ``tasks.storage`` table must
    never crash the leader tick.
    """
    try:
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler, managed_transaction,
        )
        from dynastore.modules.tasks.tasks_module import get_task_schema
        from dynastore.tools.db import validate_sql_identifier

        task_schema = get_task_schema()
        validate_sql_identifier(task_schema)
        sql = (
            f"SELECT EXISTS ("
            f"    SELECT 1 FROM {task_schema}.storage"
            f"    WHERE (status = 'ready' AND ready_at <= now())"
            f"       OR (status = 'in_flight'"
            f"           AND claimed_at < now() - make_interval(secs => :stale_lease))"
            f"    LIMIT 1"
            f")"
        )
        async with managed_transaction(engine) as conn:
            return bool(
                await DQLQuery(sql, result_handler=ResultHandler.SCALAR).execute(
                    conn, stale_lease=_STALE_LEASE_SECONDS,
                )
            )
    except Exception:  # noqa: BLE001 — probe is best-effort, never raise
        logger.debug(
            "drain_spawner: storage backlog probe failed — treating as "
            "no backlog.", exc_info=True,
        )
        return False


async def _events_backlog_exists(engine: Any) -> bool:
    """Cheap ``EXISTS`` probe mirroring ``EventDrainTask._claim_batch``'s
    claim predicate (PENDING/PROCESSING rows whose lock has elapsed).

    Fails open to ``False`` on any error, same rationale as
    :func:`_storage_backlog_exists`.
    """
    try:
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler, managed_transaction,
        )
        from dynastore.modules.tasks.tasks_module import get_task_schema
        from dynastore.tools.db import validate_sql_identifier

        task_schema = get_task_schema()
        validate_sql_identifier(task_schema)
        sql = (
            f"SELECT EXISTS ("
            f"    SELECT 1 FROM {task_schema}.events"
            f"    WHERE status IN ('PENDING', 'PROCESSING')"
            f"      AND (locked_until IS NULL OR locked_until <= now())"
            f"    LIMIT 1"
            f")"
        )
        async with managed_transaction(engine) as conn:
            return bool(
                await DQLQuery(sql, result_handler=ResultHandler.SCALAR).execute(conn)
            )
    except Exception:  # noqa: BLE001 — probe is best-effort, never raise
        logger.debug(
            "drain_spawner: events backlog probe failed — treating as "
            "no backlog.", exc_info=True,
        )
        return False


async def _spawn_storage_drain(ctx: ServiceContext, *, wedge_grace_seconds: float) -> None:
    if not await _storage_backlog_exists(ctx.engine):
        return
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.storage_emit import (
        _enqueue_drain_trigger as _enqueue_storage_drain_trigger,
    )

    async with managed_transaction(ctx.engine) as conn:
        await _enqueue_storage_drain_trigger(
            conn, wedge_grace_seconds=wedge_grace_seconds,
        )


async def _spawn_event_drain(ctx: ServiceContext, *, wedge_grace_seconds: float) -> None:
    if not await _events_backlog_exists(ctx.engine):
        return
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.tasks.events.events_emit import (
        _enqueue_event_drain_trigger,
    )

    async with managed_transaction(ctx.engine) as conn:
        await _enqueue_event_drain_trigger(
            conn, wedge_grace_seconds=wedge_grace_seconds,
        )


class DrainSpawnerService(PeriodicService):
    """Leader-elected recovery tick for wedged ``event_drain`` /
    ``storage_drain`` outboxes (#2715).

    LEADER_ONLY: one recovery attempt per drain type per cadence is enough
    platform-wide. Skips ephemeral Cloud Run Job pods (SKIP_EPHEMERAL) — a
    one-shot job has no business re-arming a platform-wide backstop.

    Each tick is a no-op unless the corresponding outbox table actually has
    undrained work (``_storage_backlog_exists`` / `_events_backlog_exists`),
    and even then the underlying dedup'd INSERT still no-ops whenever a
    healthy (non-wedged) drain is already PENDING/ACTIVE — see
    ``storage_emit._enqueue_drain_trigger`` / ``events_emit._enqueue_event_drain_trigger``.
    """

    name = "drain_spawner"
    leadership = Leadership.LEADER_ONLY
    pod_policy = PodPolicy.SKIP_EPHEMERAL
    lock_key: Optional[Union[int, str]] = "dynastore.drain_spawner"

    _DEFAULT_WEDGE_GRACE_SECONDS: float = 300.0

    def __init__(
        self,
        *,
        interval_s: float = 120.0,
        wedge_grace_seconds: float = _DEFAULT_WEDGE_GRACE_SECONDS,
    ) -> None:
        self.cadence_seconds = interval_s
        # Fallback used when TasksPluginConfig is unavailable (early startup,
        # tests without a running configs protocol) — tick() otherwise reads
        # the live, hot-reloadable value on every tick (mirrors
        # TaskRetentionService's ttl_days/dlq_max_days pattern: cadence is
        # fixed at construction, but the threshold values it acts on are
        # read live).
        self._wedge_grace_seconds_default = wedge_grace_seconds

    async def _resolve_wedge_grace_seconds(self) -> float:
        try:
            from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
            from dynastore.modules.tasks.tasks_config import TasksPluginConfig
            from dynastore.tools.discovery import get_protocol

            config_mgr = get_protocol(PlatformConfigsProtocol)
            if config_mgr is not None:
                cfg = await config_mgr.get_config(TasksPluginConfig)
                if isinstance(cfg, TasksPluginConfig):
                    return cfg.drain_recovery_wedge_grace_seconds
        except Exception:  # noqa: BLE001 — config read is best-effort
            logger.debug(
                "drain_spawner: drain_recovery_wedge_grace_seconds unavailable "
                "— using constructor default.", exc_info=True,
            )
        return self._wedge_grace_seconds_default

    async def tick(self, ctx: ServiceContext) -> None:
        wedge_grace_seconds = await self._resolve_wedge_grace_seconds()

        # Each drain type is independent: a failure recovering one must not
        # block the other from getting its recovery attempt this tick.
        try:
            await _spawn_event_drain(ctx, wedge_grace_seconds=wedge_grace_seconds)
        except Exception as exc:  # noqa: BLE001
            logger.warning("drain_spawner: event_drain recovery tick failed: %s", exc)

        try:
            await _spawn_storage_drain(ctx, wedge_grace_seconds=wedge_grace_seconds)
        except Exception as exc:  # noqa: BLE001
            logger.warning("drain_spawner: storage_drain recovery tick failed: %s", exc)
