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

"""Unified primitive for long-lived background service loops.

These services are registered EXPLICITLY with BackgroundSupervisor and MUST
NEVER be discovered via get_protocols/structural isinstance. BackgroundService
is a plain typing.Protocol (not @runtime_checkable) to prevent accidental
structural matching — a service missing a field would silently vanish from the
election flow, which is the @runtime_checkable get_protocols trap that has
burned us before.

Runner-agnostic: no FastAPI/Starlette imports. Lives beside tools/async_utils.py
as a framework-free control point.
"""
from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Coroutine, Optional, Union, Protocol

from sqlalchemy.ext.asyncio import AsyncEngine

from dynastore.modules.concurrency import get_background_executor
from dynastore.modules.db_config.locking_tools import (
    pg_advisory_leadership,
    lease_leadership,
    probe_lock_connection_liveness,
)
from dynastore.tools.async_utils import run_leader_loop

logger = logging.getLogger(__name__)


class SupportsSubmit(Protocol):
    """Minimal executor capability the supervisor depends on.

    The supervisor needs exactly one thing from an executor: submit a coroutine
    as a tracked background task and hand back the asyncio.Task. Depending on
    this narrow Protocol rather than the concrete ``BackgroundExecutor`` keeps
    the supervisor decoupled from the executor implementation and lets tests
    inject a lightweight fake. The production ``BackgroundExecutor`` satisfies
    it structurally.
    """

    def submit(self, coro: Awaitable[Any], task_name: str = ...) -> "asyncio.Task[Any]": ...


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class Leadership(Enum):
    RUN_EVERYWHERE = "run_everywhere"
    """Every pod runs run() directly — no leadership election."""

    LEADER_ONLY = "leader_only"
    """Wrap run() in run_leader_loop + pg_advisory_leadership. Only the elected
    leader pod drives this service; followers skip until the lock is free."""


class PodPolicy(Enum):
    ALL = "all"
    """Start on every pod type."""

    SKIP_EPHEMERAL = "skip_ephemeral"
    """Do NOT start when ServiceContext.is_ephemeral is True (e.g. Cloud Run Jobs,
    one-shot migration containers)."""


# ---------------------------------------------------------------------------
# ServiceContext
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ServiceContext:
    """Runtime context threaded through every service's run() / tick() call.

    Attributes
    ----------
    engine:
        The DB resource (AsyncEngine in production; may be None or a sync
        engine in tests). BackgroundSupervisor uses isinstance(engine,
        AsyncEngine) to decide whether to attempt leadership election.
    shutdown:
        Event set by the caller (lifespan teardown) to signal all services
        to stop. Services MUST exit promptly when this is set.
    is_ephemeral:
        True for short-lived container variants (Cloud Run Jobs, migration
        runners). Services with PodPolicy.SKIP_EPHEMERAL are omitted.
    name:
        Host / service-instance name. Used in log messages and as a fallback
        component in advisory-lock key derivation.
    lock_connection:
        For LEADER_ONLY services backed by the advisory election backend, the
        dedicated AUTOCOMMIT connection holding the session advisory lock.
        Services may reuse this connection for DB work during the tick.
        ``None`` for RUN_EVERYWHERE services, when not the leader, or when
        the lease-table backend is active (``election_backend="lease"`` in
        ``LeadershipConfig``).  Services that need a DB connection and may
        receive ``None`` here should fall back to acquiring one via
        ``ctx.engine``.

        IMPORTANT: When non-None, this is an AUTOCOMMIT connection
        (isolation_level='AUTOCOMMIT').  When passing to
        :func:`~dynastore.modules.db_config.query_executor.managed_transaction`,
        the function automatically detects AUTOCOMMIT mode and uses ``begin()``
        instead of ``begin_nested()``. Do NOT attempt to create nested
        transactions manually on this connection — it will raise
        ``NoActiveSQLTransactionError``.
    """

    engine: Any
    shutdown: asyncio.Event
    is_ephemeral: bool
    name: str
    lock_connection: Any = None

    async def sleep(self, seconds: float) -> bool:
        """Sleep for up to *seconds*, interrupted early on shutdown.

        Returns
        -------
        True
            Shutdown was signalled during the wait — caller should stop.
        False
            Timeout elapsed normally — caller should continue.
        """
        try:
            await asyncio.wait_for(self.shutdown.wait(), timeout=seconds)
            return True  # shutdown signalled
        except asyncio.TimeoutError:
            return False  # normal timeout, keep running


# ---------------------------------------------------------------------------
# BackgroundService Protocol
# ---------------------------------------------------------------------------


class BackgroundService(Protocol):
    """Structural contract for a long-lived background service.

    Implement this Protocol (do NOT inherit it — structural matching is
    intentional) and register instances explicitly with BackgroundSupervisor.
    Never pass BackgroundService to get_protocols(); it is NOT @runtime_checkable.
    """

    name: str
    leadership: Leadership
    pod_policy: PodPolicy
    lock_key: Optional[Union[int, str]]
    """Advisory-lock key for LEADER_ONLY. None → derived from name + ctx.name."""

    async def run(self, ctx: ServiceContext) -> None:
        """Main service loop. Must return (not raise) when ctx.shutdown is set."""
        ...


# ---------------------------------------------------------------------------
# PeriodicService — convenience base for cadence-driven services
# ---------------------------------------------------------------------------


class PeriodicService(ABC):
    """Convenience base class for services that tick on a fixed cadence.

    NOT suitable for event-driven daemons (use BackgroundService directly for
    those). Subclasses set class attributes and implement tick().

    Execution model: tick() is called immediately on first start, then after
    each cadence_seconds sleep. The loop exits as soon as shutdown is signalled
    (ctx.sleep returns True), so services stop promptly without waiting for a
    full cadence period.
    """

    name: str
    leadership: Leadership = Leadership.RUN_EVERYWHERE
    pod_policy: PodPolicy = PodPolicy.ALL
    lock_key: Optional[Union[int, str]] = None
    cadence_seconds: float = 30.0
    tick_timeout: Optional[float] = None
    """Maximum time a tick may run before the advisory lock is released.

    When set, a tick exceeding this timeout is cancelled and leadership is
    resigned, preventing a slow tick from blocking election under pool
    contention or external API latency. Defaults to ``cadence_seconds`` to
    ensure the tick completes within one cadence window. Set to ``None`` or
    ``0`` to disable the timeout (not recommended for leader-elected services).
    """

    async def run(self, ctx: ServiceContext) -> None:
        await self._safe_tick(ctx)
        while not await ctx.sleep(self.cadence_seconds):
            await self._safe_tick(ctx)

    async def _safe_tick(self, ctx: ServiceContext) -> None:
        """Run one tick, surviving transient failures.

        A RUN_EVERYWHERE periodic loop that let an exception escape ``run()``
        would die silently (the background executor logs and drops it) and never
        tick again with no recovery. Catching here keeps the loop self-healing —
        the failure is logged and the next cadence retries. LEADER_ONLY periodic
        services do NOT use this path: the supervisor drives their tick through
        ``run_leader_loop``, which intentionally resigns leadership and retries
        on error so a poisoned leader hands the lock to another pod.

        The asymmetry is deliberate. RUN_EVERYWHERE self-heals in place because
        no shared resource is held across ticks. LEADER_ONLY MUST NOT retry in
        place: its tick holds the advisory-lock AUTOCOMMIT connection, so an
        in-place retry pins that connection and a pool slot through the backoff.
        A failing leader tick must raise so ``run_leader_loop`` resigns and the
        lock is freed for another pod — never add an inner retry loop there.
        """
        try:
            await self.tick(ctx)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("%s: tick failed; continuing on next cadence", self.name)

    @abstractmethod
    async def tick(self, ctx: ServiceContext) -> None:
        """Single unit of work. Called immediately, then every cadence_seconds."""
        ...


# ---------------------------------------------------------------------------
# BackgroundSupervisor
# ---------------------------------------------------------------------------

_REELECT_CADENCE_SECONDS = 5.0
"""How often a non-leader pod retries acquiring leadership (run_leader_loop outer loop)."""


class BackgroundSupervisor:
    """Starts and drains a registered set of BackgroundService instances.

    Usage
    -----
    supervisor = BackgroundSupervisor()
    supervisor.register(MyPeriodicService())
    supervisor.start(ctx)
    # … later, on shutdown …
    ctx.shutdown.set()
    await supervisor.stop()
    """

    def __init__(self, executor: Optional[SupportsSubmit] = None) -> None:
        self._executor: SupportsSubmit = executor or get_background_executor()
        self._services: list[BackgroundService] = []
        self._tasks: list[asyncio.Task[Any]] = []

    def register(self, service: BackgroundService) -> None:
        """Append *service* to the set managed by this supervisor."""
        self._services.append(service)

    def start(self, ctx: ServiceContext) -> None:
        """Submit all registered services as background tasks.

        Applies pod-policy filtering and leadership wrapping per service.
        Must be called from a running event loop (typically from lifespan).
        """
        for service in self._services:
            # --- Pod-policy gate ---
            if service.pod_policy is PodPolicy.SKIP_EPHEMERAL and ctx.is_ephemeral:
                logger.info(
                    "BackgroundSupervisor: skipped %s in ephemeral pod", service.name
                )
                continue

            # --- Effective leadership ---
            eff = service.leadership
            if eff is Leadership.LEADER_ONLY and not isinstance(ctx.engine, AsyncEngine):
                logger.info(
                    "BackgroundSupervisor: %s is LEADER_ONLY but engine is %s "
                    "(not AsyncEngine); downgrading to RUN_EVERYWHERE — "
                    "no concurrent writers in this deployment mode.",
                    service.name,
                    type(ctx.engine).__name__ if ctx.engine is not None else "None",
                )
                eff = Leadership.RUN_EVERYWHERE

            # --- Build coroutine per effective leadership ---
            if eff is Leadership.RUN_EVERYWHERE:
                coro = service.run(ctx)
            else:
                coro = self._leader_elected_coro(service, ctx)

            try:
                task = self._executor.submit(coro, task_name=f"service:{service.name}")
            except Exception:
                # One service failing to submit must not starve the rest. Close
                # the un-submitted coroutine so it doesn't emit a "never awaited"
                # warning, log, and carry on with the remaining services.
                logger.exception(
                    "BackgroundSupervisor: failed to start %s; skipping", service.name
                )
                coro.close()
                continue
            self._tasks.append(task)
            logger.info(
                "BackgroundSupervisor: started %s (leadership=%s, pod_policy=%s)",
                service.name,
                eff.value,
                service.pod_policy.value,
            )

    def _leader_elected_coro(
        self, service: BackgroundService, ctx: ServiceContext
    ) -> Coroutine[Any, Any, None]:
        """Wrap a LEADER_ONLY service in single-leader election.

        ``run_leader_loop`` repeatedly tries to acquire the per-service advisory
        lock; only the pod that wins runs the leader work. Followers retry on the
        loop's cadence until they win or shutdown is set.

        Connection cost — why periodic services elect *per tick*
        --------------------------------------------------------
        ``pg_advisory_leadership`` holds a dedicated pooled connection for the
        duration of the ``with`` block. If the leader work were the service's
        whole ``run()`` loop, that connection would stay checked out for the
        pod's entire lifetime — one pinned connection per LEADER_ONLY service.
        With several such services and a small pool (``DB_POOL_MIN_SIZE`` floors
        low and operators run it small), the leader pod would starve its own
        request traffic.

        So for a :class:`PeriodicService` the leader work is a *single* ``tick``:
        acquire → one tick → release → sleep one cadence → re-elect. The advisory
        connection is held only while a tick runs, never between ticks. This is
        exactly the proven reaper pattern (``run_leader_loop(on_leader=run_once)``)
        these daemons used before unification, and it keeps the pool budget safe.

        A non-periodic LEADER_ONLY service (none exist today) owns its own loop,
        so it necessarily holds leadership for its whole ``run()`` — the fallback
        path below, re-electing on ``_REELECT_CADENCE_SECONDS``.

        ``service`` and ``ctx`` are explicit parameters (not loop variables), so
        the inner closures capture exactly one service — no late-binding bug.
        """
        key: Union[int, str] = (
            service.lock_key
            if service.lock_key is not None
            else f"bg-service:{service.name}:{ctx.name}"
        )

        def acquire():
            # Select the election backend from live config.
            # "lease"    → lease_leadership: transaction-mode-pooler safe,
            #              CAS on configs.leader_lease, no pinned connection.
            # "advisory" → pg_advisory_leadership: session advisory lock on a
            #              dedicated AUTOCOMMIT connection (kept one release
            #              for rollback; not compatible with transaction-mode
            #              pooling).
            from dynastore.modules.db_config.connection_health_config import (
                _leadership_config,
            )
            if _leadership_config.election_backend == "lease":
                return lease_leadership(ctx.engine, key, name=service.name)
            return pg_advisory_leadership(ctx.engine, key, name=service.name)

        # One probe closure shared by both run_leader_loop call sites below.
        # Reads live config on each tick so operators can toggle or retune
        # the probe without restarting pods.
        async def _liveness_probe(lock_conn: Any) -> None:
            from dynastore.modules.db_config.connection_health_config import (
                ConnectionHealthConfig,
            )
            from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
            from dynastore.tools.discovery import get_protocol

            enabled = True
            timeout = 2.0
            try:
                svc = get_protocol(PlatformConfigsProtocol)
                if svc is not None:
                    raw_cfg = await svc.get_config(ConnectionHealthConfig)
                    # get_config returns PluginConfig (base type); cast to the
                    # concrete class so attribute access is type-checked.
                    cfg = raw_cfg if isinstance(raw_cfg, ConnectionHealthConfig) else ConnectionHealthConfig()
                    enabled = cfg.leader_liveness_probe_enabled
                    timeout = cfg.leader_liveness_probe_timeout_seconds
            except Exception as exc:
                # Config unavailable (DB down at startup, service not yet
                # registered): fall back to fail-safe defaults but make the
                # degraded read visible rather than masking it entirely.
                logger.debug("leader_probe_config_load_failed err=%s", exc)

            if enabled and lock_conn is not None:
                await probe_lock_connection_liveness(
                    lock_conn, timeout=timeout, name=service.name
                )

        if isinstance(service, PeriodicService):
            periodic = service
            # One-time visibility flag for the lease-TTL tick clamp below.
            _clamp_logged = {"done": False}

            async def on_leader_tick(lock_conn: Any) -> None:
                # One unit of work per election; the connection is released as
                # soon as this returns. run_leader_loop then sleeps the cadence
                # (lock NOT held) before re-electing, so this throttles to
                # cadence_seconds instead of hot-re-acquiring every tick.
                #
                # Pass the lock connection via ServiceContext so the tick can
                # reuse it for DB work, avoiding a second pool checkout.
                leader_ctx = ServiceContext(
                    engine=ctx.engine,
                    shutdown=ctx.shutdown,
                    is_ephemeral=ctx.is_ephemeral,
                    name=ctx.name,
                    lock_connection=lock_conn,
                )
                # Under the lease backend the lease is acquired with a TTL at
                # tick start and NOT renewed during the tick. A tick that runs
                # longer than the TTL would lose its lease mid-flight, opening a
                # two-leader window. Cap the tick at lease_ttl - skew_margin so
                # it can never outlive its lease; overlap is bounded to the skew
                # margin regardless of the service's cadence / tick_timeout. Read
                # the LIVE config here (not at registration) so the cap tracks
                # hot-reloaded values. Services needing a longer tenure must use
                # the renewal-heartbeat regime, not a longer cadence.
                from dynastore.modules.db_config.connection_health_config import (
                    _leadership_config,
                )
                cfg = _leadership_config
                if cfg.election_backend != "lease":
                    await periodic.tick(leader_ctx)
                    return
                configured = (
                    periodic.tick_timeout
                    if periodic.tick_timeout is not None
                    else periodic.cadence_seconds
                )
                cap = cfg.lease_ttl_seconds - cfg.lease_skew_margin_seconds
                # configured may be 0 (service disabled its timeout); the lease
                # backend still must bound the tick, so fall back to the cap.
                effective = min(configured, cap) if configured and configured > 0 else cap
                if configured and configured > cap and not _clamp_logged["done"]:
                    _clamp_logged["done"] = True
                    logger.info(
                        "%s: tick clamped to %.1fs (lease_ttl %.1fs - skew %.1fs) "
                        "below configured %.1fs to keep the tick within its lease.",
                        service.name, effective, cfg.lease_ttl_seconds,
                        cfg.lease_skew_margin_seconds, configured,
                    )
                await asyncio.wait_for(periodic.tick(leader_ctx), timeout=effective)

            return run_leader_loop(
                acquire_leadership=acquire,
                on_leader=on_leader_tick,
                name=service.name,
                cadence_seconds=periodic.cadence_seconds,
                is_shutdown=ctx.shutdown.is_set,
                shutdown_event=ctx.shutdown,
                tick_timeout=periodic.tick_timeout,
                pre_tick_probe=_liveness_probe,
            )

        async def on_leader_run(lock_conn: Any) -> None:
            # Non-periodic leader: it owns its loop, so leadership is held for
            # the full run() tenure (returns when shutdown is set).
            leader_ctx = ServiceContext(
                engine=ctx.engine,
                shutdown=ctx.shutdown,
                is_ephemeral=ctx.is_ephemeral,
                name=ctx.name,
                lock_connection=lock_conn,
            )
            await service.run(leader_ctx)

        return run_leader_loop(
            acquire_leadership=acquire,
            on_leader=on_leader_run,
            name=service.name,
            cadence_seconds=_REELECT_CADENCE_SECONDS,
            is_shutdown=ctx.shutdown.is_set,
            shutdown_event=ctx.shutdown,
            pre_tick_probe=_liveness_probe,
        )

    async def stop(self, *, timeout: float = 10.0) -> None:
        """Drain submitted service tasks up to *timeout* seconds.

        The primary stop signal is ctx.shutdown — callers must set it before
        calling stop(). This method waits for tasks to exit cleanly, then
        cancels any stragglers and logs their names.

        Never raises; failure to drain is logged but not propagated.
        """
        active = [t for t in self._tasks if not t.done()]
        if not active:
            return

        try:
            _, pending = await asyncio.wait(active, timeout=timeout)
        except Exception:
            logger.warning("BackgroundSupervisor.stop: wait raised unexpectedly", exc_info=True)
            return

        if pending:
            names = [t.get_name() for t in pending]
            logger.warning(
                "BackgroundSupervisor.stop: %d task(s) still running after %.1fs; "
                "cancelling: %s",
                len(pending),
                timeout,
                names,
            )
            for t in pending:
                t.cancel()
            try:
                await asyncio.gather(*pending, return_exceptions=True)
            except Exception:
                logger.warning(
                    "BackgroundSupervisor.stop: error during cancel gather", exc_info=True
                )
