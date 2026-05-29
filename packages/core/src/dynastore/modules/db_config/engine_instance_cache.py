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
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Dict, Optional, Protocol, runtime_checkable

from dynastore.modules.db_config.engine_config import (
    EngineConfig,
)

logger = logging.getLogger(__name__)


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


class _Entry:
    """Internal cache entry — tracks instance + last-access time."""

    __slots__ = ("instance", "last_accessed", "lock")

    def __init__(self, instance: Any, *, now: float) -> None:
        self.instance = instance
        self.last_accessed = now
        # Per-entry lock so concurrent ``engine_release`` calls during
        # eviction don't race against new ``get()`` calls re-warming the
        # entry.
        self.lock = asyncio.Lock()


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
    """

    def __init__(
        self,
        *,
        engine_resolver: Callable[[str], Optional[EngineConfig]],
        engine_writer: Optional[Callable[[EngineConfig], None]] = None,
        sweep_interval_seconds: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
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
        """
        self._resolver = engine_resolver
        self._writer = engine_writer
        self._sweep_interval = sweep_interval_seconds
        self._clock = clock
        self._entries: Dict[str, _Entry] = {}
        self._ref_locks: Dict[str, asyncio.Lock] = {}
        self._sweep_task: Optional[asyncio.Task] = None
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
            instance = await engine.engine_init()
            entry = _Entry(instance, now=self._clock())
            self._entries[engine_ref] = entry
            return instance

    async def evict(self, engine_ref: str) -> bool:
        """Force-evict the cached instance for ``engine_ref``.

        Returns True if an entry was evicted, False if no entry was
        cached.  Used by maintenance / shutdown paths.
        """
        entry = self._entries.pop(engine_ref, None)
        if entry is None:
            return False
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

    # ------------------------------------------------------------------
    # Background sweep lifecycle
    # ------------------------------------------------------------------

    def start_background_sweep(self) -> None:
        """Start the periodic TTL sweep task.

        Idempotent — calling twice does not create a second task.
        """
        if self._sweep_task is not None and not self._sweep_task.done():
            return
        self._sweep_task = asyncio.create_task(self._sweep_loop())

    async def _sweep_loop(self) -> None:
        try:
            while not self._closed:
                await asyncio.sleep(self._sweep_interval)
                if self._closed:
                    break
                try:
                    await self.sweep()
                except Exception as exc:  # noqa: BLE001 — sweep is best-effort
                    logger.warning(
                        "EngineInstanceCache: sweep raised %s; cache continues.",
                        exc,
                    )
        except asyncio.CancelledError:
            pass

    async def close(self) -> None:
        """Cancel the sweep + release every cached instance.

        Idempotent.  Best-effort: ``engine_release`` failures are
        logged but do not propagate.
        """
        if self._closed:
            return
        self._closed = True
        task = self._sweep_task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass  # expected — we just cancelled it
            except Exception:
                logger.warning(
                    "Engine instance-cache sweep task errored during close",
                    exc_info=True,
                )
        for engine_ref in list(self._entries):
            await self.evict(engine_ref)


__all__ = [
    "EngineInstanceProtocol",
    "EngineInstanceCache",
]
