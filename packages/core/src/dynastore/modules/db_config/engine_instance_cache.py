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

"""Engine instance cache — lazy lifecycle for platform engines (Cycle F.5).

Engines are platform-singleton resources (a PG asyncpg pool, an ES
client, a DuckDB process pool, an Iceberg catalog client).  This
module manages their RUNTIME instances:

- :class:`EngineInstanceProtocol` declares the engine-side lifecycle
  contract.  Concrete engine config classes implement
  ``async engine_init() -> Any`` to construct a runtime instance and
  ``async engine_release(instance) -> None`` to tear it down.

- :class:`EngineInstanceCache` lazy-instantiates engines on first
  request and applies the
  :class:`~dynastore.modules.db_config.engine_config.EngineLifecycleConfig`
  policy:

  * ``policy="global"`` (default) — keep the instance forever (cheap
    connection clients).
  * ``policy="ttl_lru"`` — evict idle instances after ``ttl_seconds``.
    Eviction calls ``engine_release(instance)`` so the engine can
    close pools / drain caches.

The cache key is the ``engine_ref`` string (resolved against the
:func:`~dynastore.modules.db_config.engine_registry.list_registered_engines`
view).  In Cycle F.1 single-instance-per-kind, every default
deployment has one ref per engine kind; F.4c will populate the cache
with operator-chosen ref names from stored configs.

F.6 wired the cache into ``DBConfigModule.lifespan`` and shipped
``engine_init`` / ``engine_release`` on each concrete engine config.
Until F.4c's ref-keyed driver-config storage lands, no driver consumes
the cache in production dispatch paths — admin tooling and tests
exercise the contract end-to-end via ``app_state.engine_cache``.

Because that dormancy ends once F.4c lands (#2913), the cache also
enforces a fleet-wide connection budget (#2963): every live entry's
``EngineConfig.connection_budget_units()`` (0 for engines that open no
real database connections; ``pool_size`` for ``PostgresqlEngineConfig``)
is summed against ``pool_budget``, and a ``get()`` that would push the
running total past it raises :class:`EngineCacheBudgetExceededError`
instead of silently instantiating another pool. Existing cached engines
are unaffected — only NEW instantiation is gated.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Dict, Optional, Protocol, runtime_checkable

from dynastore.modules.db_config.engine_config import (
    EngineConfig,
)
from dynastore.modules.db_config.exceptions import EngineCacheBudgetExceededError
from dynastore.tools.background_service import (
    Leadership,
    PeriodicService,
    PodPolicy,
    ServiceContext,
)

logger = logging.getLogger(__name__)

# Default fleet-wide connection budget (#2963) — the ceiling on the SUM of
# every live entry's ``connection_budget_units()``.  200 is a deliberately
# conservative slice of the ~1000-connection ceiling this codebase already
# measures for the shared AlloyDB instance (see
# ``ScalingPolicyConfig.db_max_connections`` / ``connection_headroom`` in
# ``modules/scaling/config.py``): it leaves ~800 connections of headroom for
# the shared serving engine (``db_service.py``) and every other tenant that
# was never routed through this cache.  Sizing THIS budget precisely against
# the shared engine's own live consumption is a coordination problem for
# whoever wires F.4c's live dispatch path (#2913) — until then the cache is
# dormant, so this default only needs to be safely conservative, not exact.
DEFAULT_ENGINE_CACHE_POOL_BUDGET: int = 200


@runtime_checkable
class EngineInstanceProtocol(Protocol):
    """Contract that engine config classes implement to participate in
    :class:`EngineInstanceCache` lifecycle management.

    Methods are async because constructing a runtime client (PG pool,
    ES connection, DuckDB process) is typically I/O-bound.

    Engines that have no special init / teardown (i.e. always-on
    state) can implement these as no-ops or simply not register with
    the cache at all (the cache only invokes them when wired).
    """

    async def engine_init(self) -> Any:
        """Construct and return a runtime instance for this engine.

        Called by the cache on first access for a given ``engine_ref``
        (under ``policy="global"``) or after every eviction (under
        ``policy="ttl_lru"``).  The returned object is opaque to the
        cache — it's whatever the engine considers a usable instance
        (e.g. an ``asyncpg.Pool``, an ES client, a DuckDB connection
        pool).
        """
        ...

    async def engine_release(self, instance: Any) -> None:
        """Tear down a runtime instance previously returned by
        :meth:`engine_init`.

        Called by the cache during eviction (TTL expiry) and on cache
        shutdown.  Idempotent: re-releasing an already-released
        instance MUST be a no-op (the cache does not track release
        state).
        """
        ...


def _connection_budget_units(engine: Any) -> int:
    """Resolve ``engine.connection_budget_units()`` defensively (#2963).

    Every concrete ``EngineConfig`` declares this method (base default 0,
    overridden by ``PostgresqlEngineConfig`` to return ``pool_size``), but
    ``EngineInstanceProtocol`` itself only requires ``engine_init`` /
    ``engine_release`` — a duck-typed test double or a future engine kind
    that skips the ``EngineConfig`` base still satisfies the protocol.  Such
    engines are treated as costing nothing against the budget rather than
    raising ``AttributeError`` here.
    """
    cost = getattr(engine, "connection_budget_units", None)
    if cost is None:
        return 0
    return int(cost())


class _Entry:
    """Internal cache entry — tracks instance + last-access time.

    Concurrency for a ref is serialised by ``EngineInstanceCache._ref_locks``
    (per-ref, in ``get()``); there is no per-entry lock. A concurrent
    ``evict`` releasing the old instance while ``get`` warms a fresh one is
    benign — they act on different instances, ``engine_release`` is idempotent,
    and ``_ref_locks`` already prevents double-init.
    """

    __slots__ = ("instance", "last_accessed", "budget_units")

    def __init__(self, instance: Any, *, now: float, budget_units: int) -> None:
        self.instance = instance
        self.last_accessed = now
        # Charged against ``EngineInstanceCache._allocated_units`` (#2963) —
        # stored per-entry so ``evict`` can hand the exact amount back.
        self.budget_units = budget_units


class EngineInstanceCache:
    """Lazy-instantiating engine cache with TTL eviction.

    Single instance per process — wired via ``DBConfigModule.lifespan``
    (Cycle F.6) and exposed on ``app_state.engine_cache``.  Operators do
    not configure the cache directly — its policy comes from each
    engine's :class:`EngineLifecycleConfig`.

    Thread / coroutine safety:
        All public methods are coroutine-safe.  Concurrent ``get()``
        calls for the same ref serialise on a per-ref lock so the
        first caller pays the ``engine_init`` cost and subsequent
        callers receive the same instance.

    Eviction:
        Background sweep runs at ``sweep_interval_seconds`` (default
        60s).  An entry whose engine has ``policy="ttl_lru"`` and
        whose ``last_accessed`` is older than ``ttl_seconds`` gets
        evicted (``engine_release()`` called, entry removed).
        ``policy="global"`` entries are never evicted.

    Resolution:
        ``get(engine_ref)`` looks up the engine config by ref and
        returns its runtime instance.  Unknown refs raise
        :class:`KeyError` — callers MUST surface as a config error
        rather than silently fall back.

    Budget:
        A running total of every live entry's ``connection_budget_units()``
        is tracked against ``pool_budget`` (default
        ``DEFAULT_ENGINE_CACHE_POOL_BUDGET``, #2963).  Instantiating a NEW
        engine that would push the total over budget raises
        :class:`~dynastore.modules.db_config.exceptions.EngineCacheBudgetExceededError`;
        already-cached engines keep serving unaffected, and evicting one
        frees its share for the next caller.
    """

    def __init__(
        self,
        *,
        engine_resolver: Callable[[str], Optional[EngineConfig]],
        engine_writer: Optional[Callable[[EngineConfig], None]] = None,
        sweep_interval_seconds: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
        pool_budget: int = DEFAULT_ENGINE_CACHE_POOL_BUDGET,
    ) -> None:
        """Construct the cache.

        :param engine_resolver: callable mapping ``engine_ref`` →
            :class:`EngineConfig` instance (or ``None`` for unknown).
            ``DBConfigModule.lifespan`` (F.6) wires this from a snapshot
            of platform engines (see ``engine_resolver.build_engine_snapshot``);
            tests inject a deterministic resolver directly.
        :param engine_writer: optional callable that writes a fresh
            :class:`EngineConfig` back into the resolver's snapshot under
            both ``class_key()`` and ``engine_class`` keys.  Required for
            :meth:`update_config` (#827 — apply-handler live reconfig);
            tests that only exercise ``get`` / ``evict`` may omit it.
        :param sweep_interval_seconds: how often the background TTL
            sweep runs.
        :param clock: monotonic-clock callable, injectable for tests.
        :param pool_budget: fleet-wide ceiling (#2963) on the SUM of every
            live entry's ``connection_budget_units()``.  A ``get()`` that
            would push the running total above this raises
            :class:`EngineCacheBudgetExceededError` instead of instantiating
            another engine.  Engines that report 0 units (the base
            ``EngineConfig`` default — anything that opens no real database
            connections) never count against it.  Defaults to
            ``DEFAULT_ENGINE_CACHE_POOL_BUDGET``.
        """
        self._resolver = engine_resolver
        self._writer = engine_writer
        self._sweep_interval = sweep_interval_seconds
        self._clock = clock
        self._pool_budget = pool_budget
        self._allocated_units = 0
        self._entries: Dict[str, _Entry] = {}
        self._ref_locks: Dict[str, asyncio.Lock] = {}
        self._closed = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get(self, engine_ref: str) -> Any:
        """Return the runtime instance for ``engine_ref``.

        First call for a ref: looks up the engine config, calls
        ``engine_init()``, caches.  Subsequent calls return the cached
        instance and refresh ``last_accessed``.

        Raises:
            KeyError: ``engine_ref`` not found in the resolver.
            RuntimeError: engine has ``enabled=False`` (returns 503
                semantics — caller decides how to surface).
            EngineCacheBudgetExceededError: instantiating this ref would
                push the cache's summed ``connection_budget_units()``
                past ``pool_budget`` (#2963).  Only fires on a genuinely
                NEW instantiation — already-cached refs keep serving from
                the fast path regardless of the current budget state.
        """
        if self._closed:
            raise RuntimeError("EngineInstanceCache is closed")

        engine = self._resolver(engine_ref)
        if engine is None:
            raise KeyError(
                f"engine_ref={engine_ref!r} not registered.  "
                f"Provision the engine at platform scope before "
                f"referencing it from a driver config."
            )
        if not engine.enabled:
            raise RuntimeError(
                f"engine_ref={engine_ref!r} is disabled "
                f"(``enabled=False``).  Re-enable at platform scope or "
                f"swap the driver's engine_ref before retrying."
            )
        if not isinstance(engine, EngineInstanceProtocol):
            raise TypeError(
                f"engine_ref={engine_ref!r} ({type(engine).__name__}) "
                f"does not implement EngineInstanceProtocol "
                f"(``async engine_init`` / ``async engine_release``).  "
                f"Engines without lifecycle methods cannot participate "
                f"in the instance cache."
            )

        # Fast path — already-warmed entry.
        entry = self._entries.get(engine_ref)
        if entry is not None:
            entry.last_accessed = self._clock()
            return entry.instance

        # Slow path — first access for this ref.  Per-ref lock keeps
        # concurrent callers from double-instantiating the engine.
        ref_lock = self._ref_locks.setdefault(engine_ref, asyncio.Lock())
        async with ref_lock:
            entry = self._entries.get(engine_ref)
            if entry is not None:
                entry.last_accessed = self._clock()
                return entry.instance
            budget_units = _connection_budget_units(engine)
            projected = self._allocated_units + budget_units
            if projected > self._pool_budget:
                raise EngineCacheBudgetExceededError(
                    f"engine_ref={engine_ref!r} would need "
                    f"{budget_units} more connection budget unit(s), "
                    f"bringing the fleet-wide total to {projected} — over "
                    f"pool_budget={self._pool_budget} "
                    f"(currently allocated={self._allocated_units}).  "
                    f"Evict an idle engine to free budget, or raise "
                    f"EngineInstanceCache's configured pool_budget.",
                    engine_ref=engine_ref,
                    requested_units=budget_units,
                    allocated_units=self._allocated_units,
                    pool_budget=self._pool_budget,
                )
            instance = await engine.engine_init()
            entry = _Entry(instance, now=self._clock(), budget_units=budget_units)
            self._entries[engine_ref] = entry
            self._allocated_units = projected
            return instance

    async def evict(self, engine_ref: str) -> bool:
        """Force-evict the cached instance for ``engine_ref``.

        Returns True if an entry was evicted, False if no entry was
        cached.  Used by maintenance / shutdown paths.  Frees the entry's
        ``budget_units`` back to the pool budget (#2963) regardless of
        whether ``engine_release`` below succeeds — the instance is gone
        from the cache either way, so its budget share must be too.
        """
        entry = self._entries.pop(engine_ref, None)
        if entry is None:
            return False
        self._allocated_units = max(0, self._allocated_units - entry.budget_units)
        engine = self._resolver(engine_ref)
        if engine is not None and isinstance(engine, EngineInstanceProtocol):
            try:
                await engine.engine_release(entry.instance)
            except Exception as exc:  # noqa: BLE001 — release best-effort
                logger.warning(
                    "EngineInstanceCache: engine_release(%r) raised %s; "
                    "instance dropped from cache anyway.",
                    engine_ref, exc,
                )
        return True

    async def update_config(self, config: EngineConfig) -> None:
        """Swap a fresh ``EngineConfig`` into the snapshot and evict its instance.

        Called by apply-handler callbacks (``register_apply_handler``) so a
        ``PUT /configs/plugins/<engine>_engine_config`` takes effect on the
        next ``get`` without a process restart.  Without this, the apply
        handler invalidates the cached runtime instance but the next ``get``
        re-instantiates against the stale boot-time config still sitting in
        the resolver snapshot (#827).

        Sequence:

          1. Writes ``config`` into the resolver snapshot under both
             ``class_key()`` and ``engine_class`` keys (via the writer
             supplied at construction).
          2. Evicts cached instances under both keys so the next ``get``
             re-runs ``engine_init()`` against the new config.

        Raises ``RuntimeError`` when no writer was supplied — that means
        the cache is wired for read-only mode and live reconfig is not
        supported (tests / admin tools).
        """
        if self._writer is None:
            raise RuntimeError(
                "EngineInstanceCache.update_config: no engine_writer "
                "supplied at construction; cannot push config into the "
                "snapshot.  Wire ``engine_writer=make_writer(snapshot)`` "
                "alongside ``engine_resolver=make_resolver(snapshot)``."
            )
        # Evict BEFORE writing the new config — ``evict`` re-resolves the
        # engine via the snapshot to call ``engine_release(instance)``,
        # and the instance was built by the OLD config's ``engine_init``;
        # if we wrote first, release would dispatch on the new (wrong)
        # config object.
        class_key = type(config).class_key()
        await self.evict(class_key)
        if config.engine_class and config.engine_class != class_key:
            await self.evict(config.engine_class)
        self._writer(config)

    async def sweep(self) -> int:
        """Run one TTL eviction pass.

        Returns the number of entries evicted.  Public for tests +
        admin tooling; the background task calls this every
        ``sweep_interval_seconds``.
        """
        now = self._clock()
        evicted = 0
        for engine_ref in list(self._entries):
            entry = self._entries.get(engine_ref)
            if entry is None:
                continue
            engine = self._resolver(engine_ref)
            if engine is None:
                # Engine deregistered — drop the orphan entry without
                # calling release (the engine class is gone).
                self._entries.pop(engine_ref, None)
                evicted += 1
                continue
            policy = engine.lifecycle.policy
            if policy != "ttl_lru":
                continue  # global / never-evict
            ttl = engine.lifecycle.ttl_seconds
            if ttl is None:
                continue  # validator should have caught this; defensive
            if (now - entry.last_accessed) >= ttl:
                if await self.evict(engine_ref):
                    evicted += 1
        return evicted

    async def close(self) -> None:
        """Release every cached instance.

        Idempotent.  Best-effort: ``engine_release`` failures are
        logged but do not propagate.  The periodic sweep service is
        stopped by ``BackgroundSupervisor`` before this is called —
        ``close()`` only handles instance teardown.
        """
        if self._closed:
            return
        self._closed = True
        for engine_ref in list(self._entries):
            await self.evict(engine_ref)


class EngineInstanceCacheSweepService(PeriodicService):
    """Periodic service that runs one TTL eviction pass on the engine cache.

    Replaces the hand-wired ``asyncio.create_task(_sweep_loop())`` that
    ``EngineInstanceCache.start_background_sweep()`` used to spawn. The
    ``BackgroundSupervisor`` now owns the loop lifecycle.

    Policy:
      - leadership = RUN_EVERYWHERE: eviction is idempotent so every pod
        can run it independently; no advisory-lock contention needed.
      - pod_policy = ALL: ephemeral Cloud Run Jobs also hold engine
        instances for their short lifetime and benefit from TTL eviction.
      - cadence_seconds = the cache's sweep_interval_seconds at
        construction time (default 60s, same as before).
    """

    name = "engine_instance_cache_sweep"
    leadership = Leadership.RUN_EVERYWHERE
    pod_policy = PodPolicy.ALL

    def __init__(self, cache: "EngineInstanceCache") -> None:
        self._cache = cache
        self.cadence_seconds: float = cache._sweep_interval

    async def tick(self, ctx: ServiceContext) -> None:
        """One TTL eviction pass, driven by ``BackgroundSupervisor`` on cadence."""
        try:
            await self._cache.sweep()
        except Exception as exc:  # noqa: BLE001 — sweep is best-effort
            logger.warning(
                "EngineInstanceCacheSweepService: sweep raised %s; cache continues.",
                exc,
            )


__all__ = [
    "EngineInstanceProtocol",
    "EngineInstanceCache",
    "EngineInstanceCacheSweepService",
    "DEFAULT_ENGINE_CACHE_POOL_BUDGET",
]
