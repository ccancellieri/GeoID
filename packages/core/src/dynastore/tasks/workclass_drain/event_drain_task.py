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

# dynastore/tasks/workclass_drain/event_drain_task.py

"""``EventDrainTask`` — control-plane-native drain for ``tasks.events``.

The event-plane counterpart of :class:`StorageDrainTask`.  Drains the GLOBAL
``tasks.events`` table for ALL tenants (tenancy is the ``schema_name``
column, not the physical table) and delivers each event to the in-process
async listeners registered on the resolved :class:`EventBusProtocol`.

Why a control-plane task
------------------------
Delivery runs on the generic task control plane so it shares routing, the
capability registry, heartbeat leasing, and the co-transactional NOTIFY wakeup
with every other durable task — and can be offloaded to a dedicated worker the
same way the index drain and gdal are.  Event *handlers* stay where they are:
this task resolves the process-local listener registry via
``get_protocol(EventBusProtocol)`` and calls
:meth:`EventBusProtocol.dispatch_to_listeners`, so no handler is rewritten or
relocated — the task simply runs in a worker scope that loads them.

Claim_version fencing (#1945)
-----------------------------
Every claim bumps ``claim_version = claim_version + 1`` on the row.  Terminal
writes (``mark_done`` / ``mark_retry``) are guarded by::

    AND owner_id = :owner_id AND claim_version = :claim_version

If a stalled drain worker reclaimed by another pod (bumping ``claim_version``
again) later tries to finalize the row, the CAS predicate matches 0 rows — the
stale write is a no-op and the live owner retains exclusive control.  This is
the same fence the index plane uses, expressed against the ``tasks.events``
column vocabulary (``owner_id`` / ``locked_until`` rather than ``claimed_by`` /
``claimed_at`` / ``ready_at``).

Lifecycle
---------
``tasks.events`` carries the event lifecycle vocabulary
(``PENDING`` / ``PROCESSING`` / ``DEAD_LETTER``) plus a terminal ``COMPLETED``
state.  Drained rows are NOT deleted on ack; whole-leaf ``DROP PARTITION``
retention reclaims them (preserving the failure-forensics window).
``locked_until`` serves double duty: the claim lease while ``PROCESSING`` and
the retry-not-before delay while ``PENDING``.

Drain loop
----------
``run(payload)`` loops ``drain_once()`` until it returns 0, then exits
(one-shot drain-to-empty, matching ``StorageDrainTask``).
The dispatcher re-enters via NOTIFY / periodic catch-up.

In-process wall-clock/row budget (#2887) — SERVING TIER ONLY
--------------------------------------------------------------
A ``dev-dynastore-async-writer`` execution OOM'd after a single
``EventDrainTask`` run stayed open for roughly four hours draining a large
``tasks.events`` backlog to empty. The actual tip-over was a *separate* bug
in ``main_task.py``'s ``report_failure()`` fallback, since fixed: it used to
re-bootstrap the entire module graph (every module plus a second
``BackgroundSupervisor``) purely to record one FAILED row, and that second
boot was the allocation that OOM'd an already memory-pressured process. With
that fixed, a long single Job execution is no longer the hazard it was, so
the async-writer Cloud Run Job still drains straight to empty, unbounded —
exactly like ``StorageDrainOffloadTask``.

The wall-clock/row budget below applies only when this task runs in-process
on the serving tier (no async-writer Job deployed, or the Job is busy and
the serving tier absorbed the work). If cumulative wall-clock elapsed or
cumulative claimed rows crosses
``TasksPluginConfig.event_drain_inprocess_max_seconds`` /
``event_drain_inprocess_max_rows`` with backlog rows still remaining,
``run()`` stops early and hands the remainder off to a fresh ``event_drain``
execution (see :meth:`EventDrainTask._handoff_to_offload_job`) instead of
holding the serving tier's request-handling capacity hostage indefinitely.
Mirrors ``StorageDrainTask``'s byte/wall-clock self-escape (#2732 step 4);
events carry no comparable per-row byte signal, so the companion budget
here is a row count rather than hydrated bytes. The signal distinguishing
"running as the Job" from "running in-process" is
``app_state.ephemeral_job`` (stamped exclusively by ``main_task.py``'s
Cloud Run Job entrypoint) — see :meth:`EventDrainTask.run` for the gate.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any, ClassVar, Dict, List, Optional, Set, Tuple
from uuid import uuid4

from dynastore.models.tasks import TaskPayload
from dynastore.tasks.report import TaskReport
from dynastore.tasks.workclass_drain import AsyncWriteDrainTaskProtocol
from dynastore.tools.db import validate_sql_identifier

logger = logging.getLogger(__name__)


# Per-attempt retry backoff in seconds (mirrors StorageDrainTask).
# Indexed by ``retry_count`` (0-based); the last entry caps the backoff at
# ~30 min.
_BACKOFF_SECONDS: List[int] = [1, 5, 30, 5 * 60, 30 * 60]

# Seconds before a PROCESSING row is considered stale (lease expired) and
# eligible for reclaim by any drain worker.
_DEFAULT_LEASE_SECONDS: int = 300

# Default claim batch size for one drain_once() pass.
_DEFAULT_BATCH_SIZE: int = 100

# Default max delivery attempts when the row carries no per-row ``max_retries``
# override. Mirrors the legacy events consumer's ``_MAX_RETRIES``.
_DEFAULT_MAX_RETRIES: int = 3

# Default in-process drain wall-clock budget (#2887), seconds. Matches
# TasksPluginConfig.event_drain_inprocess_max_seconds — see the module
# docstring's "In-process wall-clock/row budget" section.
_DEFAULT_INPROCESS_MAX_SECONDS: float = 5.0

# Default in-process drain row budget (#2887). Matches
# TasksPluginConfig.event_drain_inprocess_max_rows — the companion to
# _DEFAULT_INPROCESS_MAX_SECONDS for runs with many small events that never
# cross the wall-clock budget on their own.
_DEFAULT_INPROCESS_MAX_ROWS: int = 5_000


def _backoff(retry_count: int) -> int:
    """Return the backoff in seconds for the given zero-based retry count."""
    idx = min(retry_count, len(_BACKOFF_SECONDS) - 1)
    return _BACKOFF_SECONDS[idx]


# ---------------------------------------------------------------------------
# Webhook fan-out (#1807)
# ---------------------------------------------------------------------------

# Short-TTL process cache of the distinct event types that have at least one
# webhook subscription.  Lets the drain skip the per-event subscription lookup
# for types nobody subscribes to (the common case on a webhook-free install)
# without cross-pod cache invalidation — a new subscription takes effect within
# the TTL.
_SUBSCRIBED_TYPES_TTL_SECONDS: float = 60.0
_subscribed_types_cache: Dict[str, Any] = {"value": None, "expires_at": 0.0}


def _subscription_matches_event(
    sub: Dict[str, Any],
    event_catalog_id: Optional[str],
    event_collection_id: Optional[str],
) -> bool:
    """Return True when *sub*'s scope selects an event with these ids.

    A subscription's scope narrows which events of its type it wants:

    * ``PLATFORM`` — every event of the subscribed type, any catalog/collection.
    * ``CATALOG``  — only events whose ``catalog_id`` equals the subscription's.
    * ``COLLECTION`` — only events matching both ``catalog_id`` and
      ``collection_id``.

    The event's ``catalog_id`` / ``collection_id`` come from the (normalized)
    payload — ``tasks.events`` carries them in the JSONB body, not in dedicated
    columns.
    """
    scope = str(sub.get("scope") or "PLATFORM").upper()
    if scope == "PLATFORM":
        return True
    if scope == "CATALOG":
        sub_catalog = sub.get("catalog_id")
        return sub_catalog is not None and sub_catalog == event_catalog_id
    if scope == "COLLECTION":
        sub_catalog = sub.get("catalog_id")
        sub_collection = sub.get("collection_id")
        return (
            sub_catalog is not None
            and sub_catalog == event_catalog_id
            and sub_collection is not None
            and sub_collection == event_collection_id
        )
    return False


class EventDrainTask(AsyncWriteDrainTaskProtocol):
    """One-shot drain for the global ``tasks.events`` event outbox.

    Claims ready rows (and stale PROCESSING rows whose lease expired),
    delivers each to the in-process async listeners via the resolved
    ``EventBusProtocol``, and applies fenced terminal writes (done / retry /
    dead).  Drains to empty then exits; the dispatcher re-enters via NOTIFY.

    Routing: tier-agnostic (``affinity_tier = None``). Placement comes from
    the task routing config; with no override the default matrix routes a
    tier-less system task to the ``catalog`` tier — the service that
    co-locates the dispatcher and the in-process event listeners this drain
    delivers to (and where the legacy event consumer already runs). An
    operator can repoint it via routing config without a code change. Member
    of the async-write workclass (``AsyncWriteDrainTaskProtocol``) — on GCP
    this always offloads to the async-writer Cloud Run Job once one is
    deployed (#2732); with none deployed it keeps running here.
    """

    task_type: ClassVar[str] = "event_drain"
    priority: int = 100
    affinity_tier: ClassVar[Optional[str]] = None

    def __init__(
        self,
        app_state: object | None = None,
        *,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        lease_seconds: int = _DEFAULT_LEASE_SECONDS,
        inprocess_max_seconds: float = _DEFAULT_INPROCESS_MAX_SECONDS,
        inprocess_max_rows: int = _DEFAULT_INPROCESS_MAX_ROWS,
    ) -> None:
        self.app_state = app_state
        self.batch_size = batch_size
        self.lease_seconds = lease_seconds
        self.inprocess_max_seconds = inprocess_max_seconds
        self.inprocess_max_rows = inprocess_max_rows

    async def run(self, payload: TaskPayload) -> TaskReport:
        """Drain ``tasks.events``, then return.

        Loops ``drain_once()`` until it reports zero claimed rows (drain to
        empty) — the dispatcher re-enters via NOTIFY when new rows appear.

        In-process wall-clock/row budget (#2887) — SERVING TIER ONLY: when
        this execution is running in-process (no async-writer Cloud Run Job
        picked it up), the loop tracks cumulative claimed rows and elapsed
        wall-clock time. If either configured budget
        (``event_drain_inprocess_max_seconds`` / ``event_drain_inprocess_max_rows``)
        is crossed while rows still remain, the loop stops early and hands the
        remainder off to a fresh ``event_drain`` execution via
        :meth:`_handoff_to_offload_job`, rather than holding this in-process
        execution open indefinitely.

        The budget is SKIPPED ENTIRELY when this execution is the async-writer
        Cloud Run Job itself (``self.app_state.ephemeral_job`` — stamped by
        ``main_task.py`` for every Cloud Run Job entrypoint, never by the
        serving-tier app_state): the Job drains straight to empty, exactly
        like ``StorageDrainOffloadTask``. Applying the same short budget
        inside the Job would force it to self-escape into a fresh execution
        every few seconds, paying the ~40s app-bootstrap cost again on each
        hop (the exact tax the #2887 Gap-A ``report_failure`` fix removed)
        and could chain into a handoff that dead-ends against its own
        still-ACTIVE row (see :meth:`_handoff_to_offload_job`). Since Gap-A
        already removes the OOM mechanism a long single Job execution used to
        risk, letting the Job run unbounded is no longer the hazard it was.

        Returns a :class:`~dynastore.tasks.report.TaskReport` so the runner
        persists structured metrics alongside the human-facing message
        (#1807 P2).  ``drain_once`` retains its ``int`` return type so internal
        callers and existing tests are unaffected.
        """
        from sqlalchemy.ext.asyncio import create_async_engine
        from sqlalchemy.pool import NullPool

        from dynastore.modules.db_config.connection_poison_guard import (
            register_connection_poison_guard,
        )
        from dynastore.modules.db_config.db_config import DBConfig
        from dynastore.modules.db_config.db_timeout_config import (
            task_engine_connect_args,
        )
        from dynastore.modules.db_config.tools import normalize_db_url

        # ``normalize_db_url`` both swaps the prefix to ``postgresql+asyncpg://``
        # AND converts the libpq ``sslmode=`` query parameter to asyncpg's
        # ``ssl=``.  A bare prefix swap leaves ``sslmode=`` in the URL, which
        # makes asyncpg's ``connect()`` raise "unexpected keyword argument
        # 'sslmode'" against a Cloud SQL DSN — failing every drain unrecoverably
        # and leaving the events stuck.  Mirror the canonical engine build in
        # ``db_service`` rather than re-deriving the URL by hand.
        db_url = normalize_db_url(DBConfig.database_url, is_async=True)

        # One engine for the lifetime of this run — shared across all claim and
        # terminal-write statements so connection overhead is paid once.
        # server_settings carries the same lock_timeout /
        # idle_in_transaction_session_timeout the shared engine applies, so a
        # frozen connection here can't hold a lock indefinitely (#2749, #2832).
        engine = create_async_engine(
            db_url, poolclass=NullPool, connect_args=task_engine_connect_args(DBConfig)
        )
        register_connection_poison_guard(engine, service="event_drain_task")
        # Stable owner_id for the lifetime of this run — the claim stamp and the
        # CAS guard on terminal writes.
        owner_id = f"event_drain:{uuid4()}"
        # The in-process budget never applies inside the async-writer Cloud
        # Run Job (#2887) — see the docstring above. ``app_state`` is ``None``
        # for ad-hoc/test instantiation, which correctly defaults to the
        # budget being active (matches the serving-tier default).
        budget_enabled = not bool(getattr(self.app_state, "ephemeral_job", False))
        # Hot-reloaded in-process drain budget (#2887), resolved once per run
        # — skipped entirely inside the Job, mirroring
        # StorageDrainOffloadTask never resolving/checking its budget either.
        inprocess_max_seconds, inprocess_max_rows = (
            await self._resolve_inprocess_budget() if budget_enabled else (0.0, 0)
        )
        total = 0
        start_time = time.monotonic()
        try:
            while True:
                n = await self.drain_once(engine=engine, owner_id=owner_id)
                total += n
                if n == 0:
                    break

                if not budget_enabled:
                    continue

                elapsed = time.monotonic() - start_time
                over_seconds = (
                    inprocess_max_seconds > 0 and elapsed >= inprocess_max_seconds
                )
                over_rows = (
                    inprocess_max_rows > 0 and total >= inprocess_max_rows
                )
                if over_seconds or over_rows:
                    await self._handoff_to_offload_job(engine)
                    break
        finally:
            await engine.dispose()

        report = TaskReport.completed(
            message=f"event drain completed: {total} event(s) processed",
            metrics={"drained": total},
            correlation={"owner_id": owner_id},
        )

        # Best-effort structured log.  No catalog_id at this level (global
        # task), so we emit to the standard logger for observability.
        logger.info(
            "EventDrainTask finished",
            extra={"task_report": report.log_details()},
        )

        return report

    async def _resolve_inprocess_budget(self) -> Tuple[float, int]:
        """Resolve the ``TasksPluginConfig.event_drain_inprocess_max_seconds``
        / ``event_drain_inprocess_max_rows`` pair, hot-reloaded (#2887).

        Only ever called from the serving-tier (in-process) path of
        :meth:`run` — never inside the async-writer Cloud Run Job, which
        always drains to empty. Mirrors
        ``StorageDrainTask._resolve_inprocess_budget``'s fallback pattern:
        falls back to the instance defaults (constructor values, themselves
        matching the field defaults) when the platform configs protocol is
        unavailable — early startup, lightweight worker contexts, tests.
        """
        try:
            from dynastore.models.protocols.platform_configs import (
                PlatformConfigsProtocol,
            )
            from dynastore.modules.tasks.tasks_config import TasksPluginConfig
            from dynastore.tools.discovery import get_protocol

            config_mgr = get_protocol(PlatformConfigsProtocol)
            if config_mgr is not None:
                cfg = await config_mgr.get_config(TasksPluginConfig)
                if isinstance(cfg, TasksPluginConfig):
                    return (
                        float(cfg.event_drain_inprocess_max_seconds),
                        int(cfg.event_drain_inprocess_max_rows),
                    )
        except Exception:  # noqa: BLE001 — config read is best-effort
            logger.debug(
                "EventDrainTask: event_drain_inprocess_max_seconds/"
                "event_drain_inprocess_max_rows unavailable — falling back "
                "to the instance defaults (%.1f, %d).",
                self.inprocess_max_seconds,
                self.inprocess_max_rows,
                exc_info=True,
            )
        return self.inprocess_max_seconds, self.inprocess_max_rows

    async def _handoff_to_offload_job(self, engine: Any) -> None:
        """Enqueue a fresh ``event_drain`` execution for the remaining backlog
        (#2887).

        Called by ``run()`` once the in-process wall-clock/row drain budget is
        exhausted with backlog rows still remaining — serving tier only, the
        Job never calls this (see ``run()``'s docstring). Uses a dedup key
        distinct from the live co-transactional trigger's (``"event_drain"``)
        so the handoff is never blocked by the currently-running execution's
        own still-non-terminal row — unlike ``StorageDrainTask``, there is no
        separate ``event_drain_offload`` task type: ``EventDrainTask`` already
        carries the async-write workclass marker, so a fresh execution of the
        same task type is offloaded exactly like the one it replaces.

        Residual self-block (no async-writer Job deployed): when no
        offload-capable runner exists, this handoff's own row also runs
        in-process and can itself exceed the budget, calling this method
        again — that second INSERT uses the SAME static
        ``"event_drain_handoff"`` dedup key and is blocked by the first
        handoff's still-ACTIVE row (a deliberately static key: a per-hop
        key would let concurrent successors pile up instead). The insert is
        then a safe no-op logged below rather than silently swallowed;
        forward progress resumes once that row completes, or at worst on the
        next ``DrainSpawnerService`` recovery tick
        (``drain_spawn_interval_seconds``, default 120s).
        """
        from dynastore.modules.db_config.query_executor import managed_transaction
        from dynastore.modules.tasks.events.events_emit import (
            _enqueue_event_drain_trigger,
        )

        async with managed_transaction(engine) as conn:
            inserted = await _enqueue_event_drain_trigger(
                conn, dedup_key="event_drain_handoff",
            )
        if inserted:
            logger.info(
                "EventDrainTask: in-process drain budget exhausted with "
                "backlog remaining — handed off remainder to a fresh "
                "event_drain execution.",
            )
        else:
            logger.info(
                "EventDrainTask: handoff trigger was a no-op — a prior "
                "'event_drain_handoff' row is still non-terminal. Backlog "
                "remains; it resumes once that row completes or on the next "
                "DrainSpawnerService recovery tick.",
            )

    async def drain_once(self, *, engine: Any, owner_id: str) -> int:
        """Claim one batch, deliver, apply fenced outcomes; return rows handled.

        A successful delivery (all async listeners for the event awaited
        without raising, OR no listeners registered for the event_type) marks
        the row ``COMPLETED``.  A delivery exception funnels the row to retry
        (``PENDING`` with backoff, or ``DEAD_LETTER`` once attempts are
        exhausted).

        If no ``EventBusProtocol`` is resolvable in this process (the drain is
        running in a scope that did not load the event listeners), every
        claimed row is funnelled to retry — never dropped — so a capable pod
        can deliver it later.
        """
        from dynastore.modules.tasks.tasks_module import get_task_schema

        task_schema = get_task_schema()
        validate_sql_identifier(task_schema)

        rows = await self._claim_batch(
            engine=engine,
            task_schema=task_schema,
            owner_id=owner_id,
        )
        if not rows:
            return 0

        bus = self._resolve_event_bus()
        if bus is None:
            logger.warning(
                "EventDrainTask: no EventBusProtocol in this process — "
                "%d claimed event(s) queued for retry (wrong worker scope?).",
                len(rows),
            )
            for row in rows:
                await self._mark_retry(
                    engine=engine,
                    task_schema=task_schema,
                    row=row,
                    owner_id=owner_id,
                    error="EventBusProtocol unavailable in drain process",
                )
            return len(rows)

        for row in rows:
            event_type = row.get("event_type") or ""
            payload = self._coerce_payload(row.get("payload"))
            try:
                await bus.dispatch_to_listeners(event_type, payload)
            except Exception as exc:  # noqa: BLE001 — surface every handler failure
                logger.error(
                    "EventDrainTask: delivery failed for event_id=%s "
                    "type=%s: %s",
                    row.get("event_id"),
                    event_type,
                    exc,
                    exc_info=True,
                )
                await self._mark_retry(
                    engine=engine,
                    task_schema=task_schema,
                    row=row,
                    owner_id=owner_id,
                    error=str(exc),
                )
                continue

            # In-process delivery succeeded — fan out to external webhook
            # subscribers BEFORE the ack so a crash here redelivers the event
            # (the dedup_key coalesces to exactly one delivery task per
            # event/subscription). Best-effort: never blocks the ack.
            await self._fan_out_webhooks(engine=engine, row=row, payload=payload)

            await self._mark_done(
                engine=engine,
                task_schema=task_schema,
                row=row,
                owner_id=owner_id,
            )

        return len(rows)

    # ------------------------------------------------------------------
    # Event-bus resolution
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_event_bus() -> Optional[Any]:
        """Resolve the process-local ``EventBusProtocol`` (listener registry).

        Returns ``None`` when no event bus is registered in this process — the
        caller funnels claimed rows to retry rather than dropping them.  Not
        cached on the instance: the protocol registry is a process singleton
        and the lookup is cheap, while caching would pin a stale reference
        across a leader hand-off in long-lived runners.
        """
        try:
            from dynastore.models.protocols.event_bus import EventBusProtocol
            from dynastore.tools.discovery import get_protocol

            return get_protocol(EventBusProtocol)
        except Exception:  # noqa: BLE001 — absence is a valid, handled state
            logger.debug(
                "EventDrainTask: EventBusProtocol resolution raised; "
                "treating as unavailable.",
                exc_info=True,
            )
            return None

    @staticmethod
    def _coerce_payload(raw: Any) -> Dict[str, Any]:
        """Normalise a ``tasks.events`` payload cell to a dict.

        asyncpg may surface a JSONB column as either a decoded ``dict`` or a
        raw JSON ``str`` depending on the connection's codec; the legacy
        consumer handles both the same way.  An absent payload (``None``) is a
        legitimate no-args delivery and degrades silently to ``{}``.

        A *malformed* payload — a value that parses to a non-object, an
        un-decodable string, or an unexpected column type — cannot arise from
        the ``emit_event_row`` producer (which always stores a serialised dict),
        so it signals storage corruption or an out-of-band writer.  Those cases
        still degrade to ``{}`` to preserve liveness (delivering with empty
        args rather than poison-looping a row that will never decode), but emit
        a WARNING so the corrupt row is visible to operators instead of being
        silently marked COMPLETED.
        """
        if isinstance(raw, dict):
            return raw
        if raw is None:
            return {}
        if isinstance(raw, (str, bytes, bytearray)):
            try:
                parsed = json.loads(raw)
            except (ValueError, TypeError):
                logger.warning(
                    "EventDrainTask: tasks.events payload is not valid JSON "
                    "(%r…); delivering with empty args.",
                    str(raw)[:80],
                )
                return {}
            if isinstance(parsed, dict):
                return parsed
            logger.warning(
                "EventDrainTask: tasks.events payload decoded to a non-object "
                "(%s); delivering with empty args.",
                type(parsed).__name__,
            )
            return {}
        logger.warning(
            "EventDrainTask: tasks.events payload has unexpected type %s; "
            "delivering with empty args.",
            type(raw).__name__,
        )
        return {}

    # ------------------------------------------------------------------
    # Webhook fan-out (#1807)
    # ------------------------------------------------------------------

    async def _subscribed_event_types(self, engine: Any) -> Optional[Set[str]]:
        """Return the cached set of subscribed event types, refreshing on TTL.

        Returns ``None`` only when the set has never loaded AND a refresh just
        failed — the caller then falls through to a per-event lookup rather than
        dropping a webhook because of a transient cache error.
        """
        now = time.monotonic()
        cached = _subscribed_types_cache.get("value")
        if cached is not None and now < _subscribed_types_cache["expires_at"]:
            return cached
        try:
            from dynastore.modules.tasks.event_driver import (
                get_subscribed_event_types,
            )

            types = await get_subscribed_event_types(engine)
        except Exception:  # noqa: BLE001 — optimization only; never block drain
            logger.debug(
                "webhook fan-out: subscribed-type refresh failed; serving %s.",
                "stale set" if cached is not None else "no cached set",
                exc_info=True,
            )
            return cached
        _subscribed_types_cache["value"] = types
        _subscribed_types_cache["expires_at"] = now + _SUBSCRIBED_TYPES_TTL_SECONDS
        return types

    async def _fan_out_webhooks(
        self, *, engine: Any, row: Dict[str, Any], payload: Dict[str, Any]
    ) -> None:
        """Enqueue one ``webhook_delivery`` task per matching subscription.

        Best-effort: every failure is swallowed so a webhook problem never
        poisons the core event drain.  Dedup (``webhook:{event_id}:{sub_id}``)
        gives exactly-once delivery per (event, subscription) across redelivery
        and pods.
        """
        event_type = row.get("event_type") or ""
        if not event_type:
            return
        try:
            subscribed = await self._subscribed_event_types(engine)
            if subscribed is not None and event_type not in subscribed:
                return

            from dynastore.modules.tasks.event_driver import (
                get_subscriptions_for_event_type,
            )

            subs = await get_subscriptions_for_event_type(event_type, engine)
            if not subs:
                return

            from dynastore.tasks.webhook_delivery.task import (
                normalize_webhook_payload,
            )

            domain_payload = normalize_webhook_payload(payload)
            event_catalog_id = domain_payload.get("catalog_id")
            event_collection_id = domain_payload.get("collection_id")
            event_id = str(row.get("event_id"))

            from dynastore.models.tasks import TaskCreate, TaskScope
            from dynastore.modules.tasks.tasks_module import create_task

            for sub in subs:
                sub_dict = sub if isinstance(sub, dict) else sub.model_dump()
                if not _subscription_matches_event(
                    sub_dict, event_catalog_id, event_collection_id
                ):
                    continue
                subscription_id = str(sub_dict.get("subscription_id"))
                task_data = TaskCreate(
                    task_type="webhook_delivery",
                    caller_id="system:webhook_fanout",
                    scope=TaskScope.SYSTEM,
                    inputs={
                        "subscription_id": subscription_id,
                        "subscriber_name": sub_dict.get("subscriber_name"),
                        "event_type": event_type,
                        "event_id": event_id,
                        "payload": domain_payload,
                    },
                    dedup_key=f"webhook:{event_id}:{subscription_id}",
                )
                try:
                    await create_task(engine, task_data, schema="system")
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "webhook fan-out: failed to enqueue delivery for "
                        "subscription=%s event_id=%s; skipping (core event "
                        "delivery unaffected).",
                        subscription_id,
                        event_id,
                        exc_info=True,
                    )
        except Exception:  # noqa: BLE001 — never poison the drain
            logger.warning(
                "webhook fan-out: unexpected error for event_id=%s; core "
                "event delivery unaffected.",
                row.get("event_id"),
                exc_info=True,
            )

    # ------------------------------------------------------------------
    # Claim
    # ------------------------------------------------------------------

    async def _claim_batch(
        self,
        *,
        engine: Any,
        task_schema: str,
        owner_id: str,
    ) -> List[Dict[str, Any]]:
        """Claim a batch of ready / stale rows; return them as raw dicts.

        Eligible rows: ``PENDING`` whose ``locked_until`` has elapsed (fresh
        rows have ``locked_until IS NULL``; retried rows have a future delay),
        and stale ``PROCESSING`` rows whose lease (``locked_until``) expired.
        ``FOR UPDATE SKIP LOCKED`` lets multiple worker pods claim disjoint
        batches concurrently.  Bumps ``claim_version = claim_version + 1`` on
        every (re)claim — the fence preventing a stalled drain from finalising
        a row reclaimed by another worker.
        """
        from dynastore.modules.db_config.query_executor import (
            DQLQuery,
            ResultHandler,
            managed_transaction,
        )

        claim_sql = (
            f"WITH claimed AS ("
            f"    SELECT day, event_id"
            f"    FROM {task_schema}.events"
            f"    WHERE status IN ('PENDING', 'PROCESSING')"
            f"      AND (locked_until IS NULL OR locked_until <= now())"
            f"    ORDER BY created_at, event_id"
            f"    LIMIT :batch_size"
            f"    FOR UPDATE SKIP LOCKED"
            f")"
            f" UPDATE {task_schema}.events w"
            f" SET status = 'PROCESSING', owner_id = :owner_id,"
            f"     locked_until = now() + make_interval(secs => :lease_seconds),"
            f"     claim_version = w.claim_version + 1"
            f" FROM claimed"
            f" WHERE w.day = claimed.day AND w.event_id = claimed.event_id"
            f" RETURNING w.day, w.event_id, w.event_type, w.scope, w.catalog_id,"
            f"           w.payload, w.retry_count, w.max_retries, w.claim_version,"
            f"           w.owner_id"
        )

        async with managed_transaction(engine) as conn:
            rows = await DQLQuery(
                claim_sql,
                result_handler=ResultHandler.ALL_DICTS,
            ).execute(
                conn,
                lease_seconds=self.lease_seconds,
                batch_size=self.batch_size,
                owner_id=owner_id,
            )

        return rows or []

    # ------------------------------------------------------------------
    # Fenced terminal writes (CAS on owner_id + claim_version)
    # ------------------------------------------------------------------

    async def _mark_done(
        self,
        *,
        engine: Any,
        task_schema: str,
        row: Dict[str, Any],
        owner_id: str,
    ) -> None:
        """Mark a row COMPLETED; CAS on (owner_id, claim_version).

        If another worker reclaimed the row (bumping claim_version), this
        UPDATE matches 0 rows — the stale drain's finalization is a no-op.
        Rows are not deleted on ack; ``DROP PARTITION`` retention reclaims
        COMPLETED rows.
        """
        from dynastore.modules.db_config.query_executor import (
            DQLQuery,
            ResultHandler,
            managed_transaction,
        )

        sql = (
            f"UPDATE {task_schema}.events"
            f" SET status='COMPLETED', owner_id=NULL, locked_until=NULL,"
            f"     processed_at=now()"
            f" WHERE day=:day AND event_id=:event_id"
            f"   AND owner_id=:owner_id AND claim_version=:claim_version"
        )
        async with managed_transaction(engine) as conn:
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn,
                day=row["day"],
                event_id=str(row["event_id"]),
                owner_id=owner_id,
                claim_version=row["claim_version"],
            )

    async def _mark_retry(
        self,
        *,
        engine: Any,
        task_schema: str,
        row: Dict[str, Any],
        owner_id: str,
        error: str,
    ) -> None:
        """Retry with backoff, or DEAD_LETTER once attempts are exhausted.

        One fenced UPDATE handles both outcomes (mirrors the legacy events
        ``nack``): ``retry_count + 1 >= max_retries`` terminates the row as
        ``DEAD_LETTER`` (no further delay); otherwise it returns to ``PENDING``
        with ``locked_until`` pushed into the future by the backoff curve.  CAS
        on (owner_id, claim_version) — a stale claim misses and is a safe
        no-op.  ``max_retries`` falls back to the task default when the row
        carries no per-row override.
        """
        from dynastore.modules.db_config.query_executor import (
            DQLQuery,
            ResultHandler,
            managed_transaction,
        )

        retry_count = int(row.get("retry_count") or 0)
        max_retries = row.get("max_retries")
        if max_retries is None:
            max_retries = _DEFAULT_MAX_RETRIES
        backoff = _backoff(retry_count)

        sql = (
            f"UPDATE {task_schema}.events"
            f" SET status = CASE WHEN retry_count + 1 >= :max_retries"
            f"                    THEN 'DEAD_LETTER' ELSE 'PENDING' END,"
            f"     retry_count = retry_count + 1,"
            f"     error_message = :error,"
            f"     owner_id = NULL,"
            f"     locked_until = CASE WHEN retry_count + 1 >= :max_retries"
            f"                         THEN NULL"
            f"                         ELSE now() + make_interval(secs => :backoff_seconds)"
            f"                    END,"
            f"     processed_at = now()"
            f" WHERE day=:day AND event_id=:event_id"
            f"   AND owner_id=:owner_id AND claim_version=:claim_version"
        )
        async with managed_transaction(engine) as conn:
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn,
                day=row["day"],
                event_id=str(row["event_id"]),
                owner_id=owner_id,
                claim_version=row["claim_version"],
                max_retries=int(max_retries),
                backoff_seconds=backoff,
                error=error,
            )

        logger.debug(
            "EventDrainTask: retry event_id=%s retry_count+1=%d backoff=%ds "
            "max_retries=%d error=%r",
            row.get("event_id"),
            retry_count + 1,
            backoff,
            int(max_retries),
            error,
        )
