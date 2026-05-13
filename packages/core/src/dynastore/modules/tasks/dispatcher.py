#    Copyright 2025 FAO
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

"""
tasks/dispatcher.py

Durable task dispatcher and Janitor for the DynaStore task system.

Uses a single global ``tasks.tasks`` table. No per-schema discovery is
needed: the ``claim_next()`` query filters by runner-aware capability map
types and returns the ``schema_name`` column so runners know which tenant
context to operate in.

The Dispatcher:
  - Waits on the signal-bus for ``new_task_queued`` events (from QueueListener).
  - Atomically claims a PENDING task via ``claim_next()`` (SKIP LOCKED).
  - Dispatches it to the appropriate runner (via runners.py).
  - On CancelledError, resets the task to PENDING so another instance can pick it up.

The Janitor:
  - Runs on the same event cycle as the Dispatcher (no separate thread needed).
  - After each Dispatcher wakeup, scans for ACTIVE tasks with expired
    visibility windows (dead runner, killed worker instance, etc.).
  - Resets tasks to PENDING (up to max_retries) or moves them to DEAD_LETTER.
  - Also cleans up orphaned tasks for deleted catalogs.

Both are stateless: any worker instance can resume any task after a crash.
"""

import asyncio
import hashlib
import json
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List


def _stable_advisory_lock_key(*parts: str) -> int:
    """Process-stable signed bigint for ``pg_try_advisory_xact_lock``.

    Python's builtin ``hash()`` is salted per-process (PEP 456) unless
    ``PYTHONHASHSEED`` is fixed — two pods hashing the same string will
    pick different lock keys, so any "single-leader across the deployment"
    guarantee that depends on it is silently broken. ``hashlib.blake2b``
    is deterministic across pods, processes, and Python versions.

    Returns a non-negative 63-bit int that fits PostgreSQL's signed
    bigint ``pg_try_advisory_xact_lock(bigint)`` signature.
    """
    h = hashlib.blake2b(
        b"\x00".join(p.encode("utf-8") for p in parts), digest_size=8,
    )
    return int.from_bytes(h.digest(), "big") & 0x7FFFFFFFFFFFFFFF

from sqlalchemy.ext.asyncio import AsyncEngine

from dynastore.modules.tasks.queue import NEW_TASK_QUEUED
from dynastore.modules.db_config.query_executor import DbResource
from dynastore.modules.db_config.exceptions import (
    DatabaseConnectionError,
    TableNotFoundError,
)
from dynastore.tools.async_utils import signal_bus

logger = logging.getLogger(__name__)


async def _load_oracle_inner_timeout() -> float:
    """Load oracle_inner_timeout_seconds from CachePluginConfig.

    Falls back to 0.5 s if ConfigsProtocol is unavailable or config load
    fails — consistent with CacheModule._load_cache_config.
    """
    try:
        from dynastore.modules.cache.cache_config import CachePluginConfig
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.frameworks.plugin import get_protocol
        configs_proto = get_protocol(ConfigsProtocol)
        cfg = await configs_proto.get_config(CachePluginConfig)
        if cfg:
            return cfg.oracle_inner_timeout_seconds
    except Exception:
        pass
    return 0.5


# ---------------------------------------------------------------------------
# Runner identity (owner_id for task claiming)
# ---------------------------------------------------------------------------

def _runner_id() -> str:
    """
    Returns a stable runner identity for this process.
    Prioritizes RUNNER_ID, then NAME, falling back to hostname + pid.
    """
    runner_id = os.getenv("RUNNER_ID", "")
    if runner_id:
        return runner_id

    name = os.getenv("NAME", "")
    if name:
        import socket
        return f"{name}@{socket.gethostname()}"

    import socket
    return f"{socket.gethostname()}:{os.getpid()}"


_RUNNER_ID = _runner_id()


# ---------------------------------------------------------------------------
# Service identity (logical service name for task service-affinity routing)
# ---------------------------------------------------------------------------
# Loaded from ${DYNASTORE_CONFIG_ROOT}/instance.json — see
# modules/db_config/instance.py.  When None (file missing or no service_name
# key), the dispatcher claims any task its CapabilityMap accepts (legacy
# behaviour, fully backward-compatible).

from dynastore.modules.db_config.instance import get_service_name as _get_service_name

_SERVICE_NAME: Optional[str] = _get_service_name()

# Back-off applied to a row when a worker's payload-aware ``can_claim``
# refuses it.  Keeps the same worker from immediately re-claiming on the
# next poll while leaving the row visible to any other worker (whose
# ``claim_batch`` filter is ``locked_until IS NULL OR locked_until <= NOW()``).
_CLAIM_REJECT_BACKOFF = timedelta(
    seconds=int(os.environ.get("DISPATCHER_CLAIM_REJECT_BACKOFF_SECONDS", "30")),
)
if _SERVICE_NAME:
    logger.info("Dispatcher: service_name=%r (from instance.json)", _SERVICE_NAME)
else:
    logger.info(
        "Dispatcher: no service_name configured — service-affinity routing inactive."
    )


# ---------------------------------------------------------------------------
# Batched heartbeat — one coroutine per dispatcher, one UPDATE per interval
# ---------------------------------------------------------------------------

class BatchedHeartbeat:
    """
    Refreshes ``locked_until`` for all tasks owned by this dispatcher in a
    single batched UPDATE via ``heartbeat_tasks()``, rather than one
    transaction per task.

    Usage::

        hb = BatchedHeartbeat(engine, visibility_timeout=timedelta(minutes=5))
        await hb.start()
        ...
        await hb.register(task_id, timestamp)
        try:
            result = await run_task(...)
        finally:
            await hb.unregister(task_id)
        ...
        await hb.stop()
    """

    def __init__(
        self,
        engine: DbResource,
        interval: timedelta = timedelta(seconds=30),
        visibility_timeout: timedelta = timedelta(minutes=5),
    ):
        self._engine = engine
        self._interval = interval.total_seconds()
        self._visibility_timeout = visibility_timeout
        # {task_id: timestamp}
        self._owned: Dict[str, datetime] = {}
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None

    async def register(self, task_id: str, timestamp: datetime) -> None:
        async with self._lock:
            self._owned[task_id] = timestamp

    async def unregister(self, task_id: str) -> None:
        async with self._lock:
            self._owned.pop(task_id, None)

    async def start(self) -> None:
        self._task = asyncio.create_task(self._beat_loop(), name="batched_heartbeat")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _beat_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self._flush()

    async def _flush(self) -> None:
        async with self._lock:
            snapshot = dict(self._owned)

        if not snapshot:
            return

        from dynastore.modules.tasks.tasks_module import heartbeat_tasks
        import uuid

        task_ids = [uuid.UUID(tid) for tid in snapshot.keys()]

        try:
            await heartbeat_tasks(self._engine, task_ids, self._visibility_timeout)
            logger.debug(
                f"BatchedHeartbeat: extended {len(task_ids)} task(s) "
                f"until +{self._visibility_timeout}."
            )
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.warning(f"BatchedHeartbeat: failed: {e}")


# ---------------------------------------------------------------------------
# Observability — structured task-terminal-state log (#504). Single emission
# site so a log-based metric pivots on `outcome` to compute success rate +
# latency percentiles per task_type without juggling multiple log shapes.
# Wrapped in try/except — telemetry must never break the drain.
# ---------------------------------------------------------------------------


def _log_task_terminal(
    task_type: str,
    task_id: Any,
    timestamp: datetime,
    *,
    outcome: str,
    error: Optional[str],
) -> None:
    try:
        drain_seconds = (datetime.now(timezone.utc) - timestamp).total_seconds()
        if outcome == "success":
            logger.info(
                "task_drained task_type=%s task_id=%s "
                "enqueue_to_drain_seconds=%.4f outcome=%s",
                task_type, task_id, drain_seconds, outcome,
            )
        else:
            logger.info(
                "task_failed task_type=%s task_id=%s "
                "enqueue_to_drain_seconds=%.4f outcome=%s error=%r",
                task_type, task_id, drain_seconds, outcome, error or "-",
            )
    except Exception:  # noqa: BLE001
        logger.debug("task_terminal log failed", exc_info=True)


# ---------------------------------------------------------------------------
# Reactive reaper — DLQ unclaimable rows when no live worker advertises the
# required capability (issue #502). Companion to ``TaskProtocol.can_claim``
# and ``TaskProtocol.required_capability``.
# ---------------------------------------------------------------------------


async def _maybe_dlq_unclaimable(
    engine: DbResource, row: Dict[str, Any], *, capability_id: str,
) -> bool:
    """If no live worker advertises ``capability_id``, move ``row`` to
    DEAD_LETTER with a clear ``error_message`` and return ``True``.

    Returns ``False`` if the row should stay PENDING (oracle says live,
    advisory lock contended, infra error). On any uncertainty we prefer
    "leave PENDING + WARN" over a false DLQ — the caller falls back to
    the standard ``reset_task_to_pending`` back-off.

    Single-actor guarded by a transaction-scoped ``pg_try_advisory_xact_lock``
    keyed by capability id: when N dispatchers see the same rejected row in
    parallel, at most one performs the UPDATE; the others skip and let the
    standard back-off path run.
    """
    from dynastore.modules.tasks.capability_oracle import (
        TASK_TYPE_CAPABILITY_INPUTS_KEY, is_capability_live,
    )
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, DDLQuery, ResultHandler, managed_transaction,
    )
    from dynastore.modules.tasks.tasks_module import get_task_schema

    task_id = row["task_id"]
    timestamp = row["timestamp"]

    try:
        timeout_s = await _load_oracle_inner_timeout()

        if await is_capability_live(capability_id):
            return False

        lock_key = _stable_advisory_lock_key(
            "dynastore.idx_reaper", capability_id,
        )
        schema = get_task_schema()
        error_message = (
            f"reaped: no live worker advertises capability {capability_id!r} "
            f"(check SCOPE/B6 — module not loaded in any reachable pool)"
        )
        async with managed_transaction(engine) as conn:
            got_lock = await DQLQuery(
                "SELECT pg_try_advisory_xact_lock(:k) AS got",
                result_handler=ResultHandler.ONE_DICT,
            ).execute(conn, k=lock_key)
            if not got_lock or not got_lock.get("got"):
                return False

            # Re-check liveness inside the locked transaction: another pod
            # may have appeared between the unlocked oracle call above and
            # the lock acquisition. Conservative double-check.
            #
            # Bounded: this call holds the DB connection + advisory xact
            # lock for its full duration. Under cache slowness (Valkey
            # cluster-mode timeout, network blip) an unbounded ``exists``
            # convoys every other dispatcher behind the same connection,
            # cascading into ``db_pool_acquire`` stalls (#629). The oracle
            # is already fail-open on error (returns True), so an
            # ``asyncio.TimeoutError`` here maps to the same "treat as
            # live, leave PENDING + WARN" outcome — never a false DLQ.
            try:
                live = await asyncio.wait_for(
                    is_capability_live(capability_id),
                    timeout=timeout_s,
                )
            except asyncio.TimeoutError:
                live = True
                logger.info(
                    "dispatcher: inner oracle timeout capability=%s "
                    "timeout_s=%.2f treating_as=live",
                    capability_id, timeout_s,
                )
            if live:
                return False

            # CAS guard: only DLQ rows still PENDING with retry_count=0. If
            # any dispatcher raced and claimed it, the UPDATE matches zero
            # rows and we fall through harmlessly.
            sql = f"""
                UPDATE "{schema}".tasks
                SET status        = 'DEAD_LETTER',
                    error_message = :err,
                    finished_at   = NOW(),
                    owner_id      = NULL,
                    locked_until  = NULL
                WHERE timestamp = :ts
                  AND task_id   = :tid
                  AND status    = 'PENDING'
                  AND retry_count = 0
                RETURNING task_id
            """
            updated = await DQLQuery(
                sql, result_handler=ResultHandler.ONE_DICT,
            ).execute(conn, err=error_message, ts=timestamp, tid=task_id)
            if updated:
                logger.warning(
                    "dispatcher: DLQ'd task %s (%s) — capability %r has no "
                    "live worker in the deployment",
                    task_id, row.get("task_type"), capability_id,
                )
                # Observability (#528): structured key=value INFO line —
                # log-based counter ready. A spike of >0 over a rolling
                # window means SCOPE drift; alerts can match this shape
                # directly without grepping the WARN above.
                logger.info(
                    "dispatcher_reactive_dlq_total task_type=%s capability=%s "
                    "reason=no_live_worker task_id=%s",
                    row.get("task_type") or "-", capability_id, task_id,
                )
                # Bulk fast-path (#529): once we have proven the capability
                # is dead AND the advisory lock is ours, sweep every other
                # PENDING/retry_count=0 row of the same task_type whose
                # JSONB capability field matches. Without this, a 500-row
                # backlog needs 500 separate dispatcher passes to drain.
                #
                # ``inputs_key`` is only interpolated from the hardcoded
                # mapping above — never user input — so direct format is
                # safe. Bound parameters carry the per-row values.
                task_type = row.get("task_type") or ""
                inputs_key = TASK_TYPE_CAPABILITY_INPUTS_KEY.get(task_type)
                if inputs_key:
                    bulk_sql = f"""
                        UPDATE "{schema}".tasks
                        SET status        = 'DEAD_LETTER',
                            error_message = :err,
                            finished_at   = NOW(),
                            owner_id      = NULL,
                            locked_until  = NULL
                        WHERE task_type    = :tt
                          AND status       = 'PENDING'
                          AND retry_count  = 0
                          AND inputs->>'{inputs_key}' = :cap
                        RETURNING task_id
                    """
                    sibling_rows = await DQLQuery(
                        bulk_sql, result_handler=ResultHandler.ALL_DICTS,
                    ).execute(
                        conn, err=error_message, tt=task_type,
                        cap=capability_id,
                    )
                    sibling_count = len(sibling_rows or [])
                    if sibling_count:
                        logger.warning(
                            "dispatcher: bulk-DLQ'd %d sibling task(s) of "
                            "type %s for dead capability %r",
                            sibling_count, task_type, capability_id,
                        )
                        logger.info(
                            "dispatcher_reactive_dlq_bulk_total "
                            "task_type=%s capability=%s "
                            "reason=no_live_worker count=%d",
                            task_type, capability_id, sibling_count,
                        )
                return True
            return False
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "dispatcher: reactive reaper failed for task %s (%s) — "
            "leaving PENDING: %s", task_id, row.get("task_type"), exc,
        )
        return False


# ---------------------------------------------------------------------------
# Janitor — recovery of stale ACTIVE tasks + orphan cleanup
# ---------------------------------------------------------------------------

async def _run_janitor(
    engine: DbResource,
    visibility_timeout: timedelta,
    orphan_grace_period: timedelta = timedelta(hours=1),
) -> None:
    """
    Global janitor:
    1. Finds ACTIVE tasks with expired locks and requeues or dead-letters them.
    2. Cleans up orphaned tasks for deleted catalogs.

    Uses pg_try_advisory_xact_lock to elect a single Janitor leader across
    all Cloud Run instances.
    """
    from dynastore.modules.tasks.tasks_module import find_stale_tasks, fail_task, cleanup_orphan_tasks
    from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler, managed_transaction

    _JANITOR_LOCK_KEY = _stable_advisory_lock_key("dynastore.janitor.global")

    async with managed_transaction(engine) as conn:
        try:
            lock_acquired = await DQLQuery(
                "SELECT pg_try_advisory_xact_lock(:lock_key);",
                result_handler=ResultHandler.SCALAR,
            ).execute(conn, lock_key=_JANITOR_LOCK_KEY)

            if not lock_acquired:
                return
        except Exception:
            return

    # Find stale tasks (expired locks) — global scan
    try:
        stale = await find_stale_tasks(engine, visibility_timeout)
        if stale:
            logger.info(f"Janitor: Found {len(stale)} stale ACTIVE task(s).")
            for row in stale:
                task_id = row["task_id"]
                retry_count = int(row.get("retry_count", 0))
                max_retries = int(row.get("max_retries", 3))
                should_retry = retry_count < max_retries
                await fail_task(
                    engine,
                    task_id=task_id,
                    timestamp=datetime.now(timezone.utc),
                    error_message=f"Janitor: visibility window expired (owner={row.get('owner_id')!r})",
                    retry=should_retry,
                )
                if not should_retry:
                    logger.error(
                        f"Janitor: Task {task_id} moved to DEAD_LETTER after "
                        f"{retry_count}/{max_retries} retries."
                    )
    except Exception as e:
        logger.warning(f"Janitor: Error scanning stale tasks: {e}")

    # Orphan cleanup
    try:
        orphaned = await cleanup_orphan_tasks(engine, orphan_grace_period)
        if orphaned:
            logger.info(f"Janitor: Dead-lettered {orphaned} orphaned task(s).")
    except Exception as e:
        logger.warning(f"Janitor: Error cleaning orphans: {e}")


# ---------------------------------------------------------------------------
# Dispatcher loop
# ---------------------------------------------------------------------------

async def run_dispatcher(
    engine: DbResource,
    schema: Optional[str],
    shutdown_event: asyncio.Event,
    visibility_timeout: timedelta = timedelta(minutes=5),
    signal_timeout: float = 35.0,
    batch_size: int = int(os.getenv("DISPATCHER_BATCH_SIZE", "10")),
) -> None:
    """
    Main dispatcher loop.

    Waits for a ``new_task_queued`` signal-bus event, atomically claims up to
    ``batch_size`` PENDING tasks from the global queue via ``claim_batch()``,
    dispatches them concurrently, and updates task state on completion or
    failure.

    Also runs the Janitor on every timeout-based wakeup.

    Args:
        engine:             Async database engine.
        schema:             Ignored (kept for backward compat). The global
                            ``tasks.tasks`` table is always used.
        shutdown_event:     Set this to stop the dispatcher cleanly.
        visibility_timeout: How long a claimed task stays ACTIVE before the
                            Janitor can reclaim it (heartbeat extends this).
        signal_timeout:     Max seconds to wait for a signal before running
                            the Janitor anyway (defensive polling).
        batch_size:         Max tasks to claim per batch (env: DISPATCHER_BATCH_SIZE).
    """
    from dynastore.modules.tasks.runners import capability_map
    from dynastore.modules.tasks.models import TaskExecutionMode, PermanentTaskFailure
    from dynastore.modules.tasks.tasks_module import (
        claim_batch, complete_task, fail_task, reset_task_to_pending,
    )
    from dynastore.modules.tasks.execution import execution_engine
    from dynastore.tasks import get_task_instance

    # Refresh capability map at startup
    await capability_map.refresh()

    logger.info(
        f"Dispatcher: Started (runner={_RUNNER_ID!r}, batch_size={batch_size}, "
        f"async_types={capability_map.async_types}, "
        f"sync_types={capability_map.sync_types})."
    )
    # In-process _run_janitor retired — stuck-task reaping is now handled by
    # the pg_cron job ``dynastore-task-reaper`` (registered in TasksModule
    # startup).  Single coordinated executor at the DB side; zero pod
    # connections, zero leader-election.  See
    # ``tasks_module.reap_stuck_tasks``.

    heartbeat = BatchedHeartbeat(engine, visibility_timeout=visibility_timeout)
    await heartbeat.start()

    async def _dispatch_one(row: Dict) -> None:
        """Dispatch a single claimed task with full error handling.

        Two completion paths:

        - **Synchronous runners** (``SyncRunner``) return the task result
          directly; dispatcher calls ``complete_task`` and unregisters the
          heartbeat.
        - **Background runners** (``BackgroundRunner`` in dispatcher-path
          mode) return :data:`DEFERRED_COMPLETION`: the dispatcher skips
          ``complete_task`` and hands off heartbeat ownership to the
          background coroutine, which updates the row (COMPLETED / FAILED)
          and unregisters itself when execution terminates.
        """
        from dynastore.modules.tasks.models import DEFERRED_COMPLETION

        task_id = row["task_id"]
        timestamp = row["timestamp"]
        deferred = False

        # Payload-aware claim predicate.  If the task class refuses this
        # specific row (e.g. ``IndexPropagationTask`` whose target indexer
        # is not registered in this process), release the claim back to
        # PENDING with a small back-off so another worker can pick it up
        # without this one hot-looping.  See #491.
        task_instance = get_task_instance(row["task_type"])
        if task_instance is not None:
            can_claim_fn = getattr(type(task_instance), "can_claim", None)
            if callable(can_claim_fn):
                try:
                    accepted = can_claim_fn(row)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "Dispatcher: can_claim raised for task %s (%s): %s — "
                        "falling through to runner so the failure surfaces.",
                        task_id, row["task_type"], exc,
                    )
                    accepted = True
                if not accepted:
                    # Reactive reaper (#502): if the task is capability-gated
                    # and no live worker advertises the required capability,
                    # the row is unclaimable across the entire deployment.
                    # DLQ it instead of leaving it PENDING forever. On any
                    # uncertainty (no capability declared, oracle says live,
                    # advisory lock contention) fall through to the standard
                    # back-off + reset-to-pending path.
                    from dynastore.modules.tasks.capability_oracle import (
                        resolve_required_capability,
                    )
                    cap_id = resolve_required_capability(task_instance, row)
                    if cap_id:
                        dlqed = await _maybe_dlq_unclaimable(
                            engine, row, capability_id=cap_id,
                        )
                        if dlqed:
                            return
                    # Observability (#504): claim-rejection rate per
                    # (task_type, capability) is the early-warning signal
                    # for SCOPE drift and module-deployment gaps. Structured
                    # key=value INFO line — log-based metric ready.
                    logger.info(
                        "task_claim_rejected task_type=%s capability=%s "
                        "task_id=%s — can_claim returned False on this worker",
                        row["task_type"], cap_id or "-", task_id,
                    )
                    await reset_task_to_pending(
                        engine, task_id, backoff=_CLAIM_REJECT_BACKOFF,
                    )
                    return

        await heartbeat.register(str(task_id), timestamp)
        try:
            result = await execution_engine.dispatch(
                row, engine=engine, heartbeat=heartbeat,
            )
            if result is DEFERRED_COMPLETION:
                # Background runner scheduled async work against the SAME
                # claimed row and will update complete_task / fail_task
                # itself.  Heartbeat ownership has been transferred — the
                # coroutine unregisters in its ``finally`` block.
                deferred = True
                logger.debug(
                    f"Dispatcher: task {task_id} deferred to background runner "
                    f"(heartbeat + completion ownership transferred)."
                )
                return

            await complete_task(engine, task_id, timestamp, outputs=result)
            # Observability (#504): single structured terminal-state line
            # (replaces the prior "completed successfully" INFO). Per-task_type
            # so the metric works for every TaskProtocol implementation.
            _log_task_terminal(
                row["task_type"], task_id, timestamp,
                outcome="success", error=None,
            )

        except asyncio.CancelledError:
            logger.warning(
                f"Dispatcher: Task {task_id} interrupted "
                f"(CancelledError) — resetting to PENDING."
            )
            await fail_task(
                engine, task_id, timestamp,
                "Runner interrupted (SIGTERM)",
                retry=True,
            )
            _log_task_terminal(
                row["task_type"], task_id, timestamp,
                outcome="cancelled", error="SIGTERM",
            )
            raise

        except PermanentTaskFailure as e:
            logger.error(
                f"Dispatcher: Task {task_id} permanently failed "
                f"(no retries): {e}"
            )
            await fail_task(
                engine, task_id, timestamp,
                str(e),
                retry=False,
            )
            _log_task_terminal(
                row["task_type"], task_id, timestamp,
                outcome="permanent_failure", error=str(e),
            )

        except Exception as e:
            import traceback
            logger.error(
                f"Dispatcher: Task {task_id} failed with error: "
                f"{e}\n{traceback.format_exc()}"
            )
            await fail_task(
                engine, task_id, timestamp,
                str(e),
                retry=True,
            )
            _log_task_terminal(
                row["task_type"], task_id, timestamp,
                outcome="transient_failure", error=str(e),
            )

        finally:
            # Skip unregister when the background runner took heartbeat
            # ownership — it will unregister in its own ``finally``.
            if not deferred:
                await heartbeat.unregister(str(task_id))

    while not shutdown_event.is_set():
        try:
            # Wait for a notification or periodic wakeup.  Periodic wakeup is
            # a defensive fallback — the pg_cron reaper issues ``pg_notify
            # 'new_task_queued'`` when it resets stuck rows, so we will be
            # woken promptly in steady state.
            got_signal = await signal_bus.wait_for(NEW_TASK_QUEUED, timeout=signal_timeout)

            # Claim and dispatch in batches until queue is empty
            while not shutdown_event.is_set():
                rows = await claim_batch(
                    engine,
                    async_task_types=capability_map.async_types,
                    sync_task_types=capability_map.sync_types,
                    visibility_timeout=visibility_timeout,
                    owner_id=_RUNNER_ID,
                    batch_size=batch_size,
                )
                if not rows:
                    break  # Queue empty for our capability set

                for row in rows:
                    logger.info(
                        f"Dispatcher: Claimed task {row['task_id']} ({row['task_type']}) "
                        f"schema={row.get('schema_name')!r} "
                        f"mode={row.get('execution_mode', 'ASYNC')}."
                    )

                # Dispatch batch concurrently
                results = await asyncio.gather(
                    *[_dispatch_one(row) for row in rows],
                    return_exceptions=True,
                )

                # Check for CancelledError — propagate shutdown
                for r in results:
                    if isinstance(r, asyncio.CancelledError):
                        raise r

        except asyncio.CancelledError:
            logger.info("Dispatcher: Cancelled — shutting down.")
            break
        except TableNotFoundError as e:
            # Transient: the global tasks table is gone (dev-compose db-reset
            # race, CASCADE DROP during manual cleanup, or cross-pod DDL
            # still in flight).  TasksModule.lifespan re-creates it under an
            # advisory lock at next startup; meanwhile a hot loop of ERROR
            # logs is noise, not signal.  Back off long enough for recovery
            # and keep quiet.
            if shutdown_event.is_set():
                break
            logger.warning(
                "Dispatcher: tasks table not yet available (%s) — backing off 10s. "
                "This is expected during startup / DB reset; becomes an error if "
                "it persists.", e,
            )
            await asyncio.sleep(10.0)
        except DatabaseConnectionError as e:
            # Transient asyncpg client-state errors — connection closed
            # mid-operation (#235) or "cannot switch to state" from
            # concurrent connection use (#239).  The dispatcher recovers
            # on the next loop with a fresh connection; demote to WARNING
            # so the underlying noise doesn't page operators.
            if shutdown_event.is_set():
                break
            logger.warning(
                "Dispatcher: transient PG connection issue (%s) — backing off 5s.", e,
            )
            await asyncio.sleep(5.0)
        except Exception as e:
            if shutdown_event.is_set():
                break
            logger.error(f"Dispatcher: Unexpected error: {e}", exc_info=True)
            await asyncio.sleep(2.0)

    await heartbeat.stop()
    logger.info("Dispatcher: Stopped.")
