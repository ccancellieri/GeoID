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

"""Engine snapshot resolver — sync ``engine_ref → EngineConfig`` lookup.

:class:`~dynastore.modules.db_config.engine_instance_cache.EngineInstanceCache`
takes a synchronous ``engine_resolver`` callable.
:class:`~dynastore.modules.db_config.platform_config_service.PlatformConfigService.get_config`
is async — so we build a *snapshot* of the platform-tier engine configs at
:meth:`DBConfigModule.lifespan` start and serve from that.

Snapshot semantics (Cycle F.6):

- Walks every concrete :class:`EngineConfig` subclass via
  :func:`~dynastore.modules.db_config.engine_registry.list_registered_engines`.
- Calls ``await pcfg.get_config(cls)`` once per kind; in the F.1
  single-instance-per-kind contract, that's all the engines that exist.
- Indexes the result both by ``class_key`` (e.g. ``postgresql_engine_config``)
  and by ``engine_class`` discriminator (e.g. ``postgresql_engine``) so
  default-deployment ref-naming conventions both resolve.
- F.4c will add per-scope ref-keyed storage (operator-chosen ref names like
  ``pg_main`` + ``pg_secondary``); this resolver is the seam where that
  widening happens.

Until F.4c lands no driver actually consumes the cache in production, but
the lifespan still constructs the cache so tests + admin tooling can
exercise the contract end-to-end.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Dict, Optional, TYPE_CHECKING

from dynastore.modules.db_config.engine_config import EngineConfig
from dynastore.modules.db_config.engine_registry import list_registered_engines

if TYPE_CHECKING:
    from dynastore.modules.db_config.platform_config_service import PlatformConfigService

logger = logging.getLogger(__name__)


# Default retry parameters for ``refresh_snapshot_until_ready``.  Tuned for the
# typical Cloud Run boot order: DBConfigModule lifespan (priority 0) starts
# before DBService (priority 10) installs the connection pool, so the very
# first snapshot attempt is guaranteed to fail with
# ``db_resource is None``.  The retry loop unblocks once DBService is up,
# usually within a couple of seconds; the upper bound covers slow cold-starts.
_DEFAULT_RETRY_MAX_ATTEMPTS = 30
_DEFAULT_RETRY_INITIAL_DELAY = 0.5
_DEFAULT_RETRY_MAX_DELAY = 5.0


# Stable substring of the ``ValueError`` raised by
# ``query_executor.managed_transaction`` when called before
# ``DBService`` installs the pool.  See #845 — keeping this match-by-message
# (rather than importing a narrow exception type) avoids a layering import
# from ``query_executor`` into the resolver, and the message is part of an
# internal API only this file consumes.
_POOL_NOT_READY_MARKER = "db_resource is None"


def _is_pool_not_ready(exc: BaseException) -> bool:
    """Recognise the benign boot-order race from ``managed_transaction``."""
    return isinstance(exc, ValueError) and _POOL_NOT_READY_MARKER in str(exc)


async def build_engine_snapshot(
    pcfg: "PlatformConfigService",
    *,
    into: Optional[Dict[str, EngineConfig]] = None,
) -> Dict[str, EngineConfig]:
    """Snapshot the platform-tier engine configs into a ``ref → instance`` map.

    For every concrete :class:`EngineConfig` subclass, fetch the stored
    configuration (or zero-arg default if none stored) and index it under
    BOTH the ``class_key()`` and the ``engine_class`` discriminator so a
    driver referencing either form resolves correctly.

    Errors fetching a single engine config are logged + skipped — the cache
    serves what it can; missing entries surface as ``KeyError`` from
    :meth:`EngineInstanceCache.get` at first dispatch.

    When ``into`` is provided, populates that dict in place and returns it;
    this lets a long-lived resolver closure observe successful entries from a
    later retry without rebuilding the closure.  See
    :func:`refresh_snapshot_until_ready`.
    """
    snapshot: Dict[str, EngineConfig] = into if into is not None else {}
    for class_key, cls in list_registered_engines().items():
        try:
            config = await pcfg.get_config(cls)
        except Exception as exc:  # noqa: BLE001 — best-effort snapshot
            # Distinguish the benign boot-order race (DBConfigModule priority 0
            # runs before DBService priority 10 installs the pool, so every
            # first-pass per-engine fetch raises ``db_resource is None``) from
            # a real config-load failure.  The race is recovered by
            # ``refresh_snapshot_until_ready`` retrying until the pool is up;
            # a successful retry then logs ``engine snapshot ready`` at INFO,
            # and total exhaustion logs ERROR.  Surfacing the race at WARNING
            # on every cold start makes those operationally-meaningful lines
            # impossible to find — see #845.
            if _is_pool_not_ready(exc):
                logger.debug(
                    "build_engine_snapshot: %s deferred — DB pool not yet up; "
                    "refresh task will retry (%s).",
                    class_key, exc,
                )
            else:
                logger.warning(
                    "build_engine_snapshot: failed to load %s (%s); skipping.",
                    class_key, exc,
                )
            continue
        if not isinstance(config, EngineConfig):
            logger.warning(
                "build_engine_snapshot: %s returned non-EngineConfig %r; "
                "skipping.",
                class_key, type(config).__name__,
            )
            continue
        snapshot[class_key] = config
        # Mirror under the engine_class discriminator (e.g.
        # ``postgresql_engine``) so drivers that reference the kind rather
        # than the class_key resolve to the same instance.
        if config.engine_class and config.engine_class != class_key:
            snapshot[config.engine_class] = config
    return snapshot


async def _snapshot_attempt(
    snapshot: Dict[str, EngineConfig], pcfg: "PlatformConfigService"
) -> int:
    """Run one ``build_engine_snapshot`` pass into ``snapshot``.

    Shared by :func:`refresh_snapshot_until_ready` (bounded retry loop with
    its own backoff + exhaustion logging) and
    :func:`try_refresh_snapshot_once` (single on-demand attempt for a
    caller that owns its own retry cadence). Returns the number of
    registered engine kinds currently present in ``snapshot``.
    """
    await build_engine_snapshot(pcfg, into=snapshot)
    return sum(
        1
        for class_key in list_registered_engines()
        if class_key in snapshot
    )


async def refresh_snapshot_until_ready(
    snapshot: Dict[str, EngineConfig],
    pcfg: "PlatformConfigService",
    *,
    max_attempts: int = _DEFAULT_RETRY_MAX_ATTEMPTS,
    initial_delay: float = _DEFAULT_RETRY_INITIAL_DELAY,
    max_delay: float = _DEFAULT_RETRY_MAX_DELAY,
) -> bool:
    """Retry ``build_engine_snapshot`` into ``snapshot`` until at least one
    entry is populated, with exponential backoff.

    Mutates ``snapshot`` in place so any existing
    :func:`make_resolver` closure observes the populated state without
    needing to be rebuilt.

    Runs the bounded ``max_attempts`` schedule first (exponential backoff
    from ``initial_delay`` up to ``max_delay`` — by default totalling
    ~132.5s). If the snapshot is still empty once that budget is spent,
    this does NOT give up: it logs a single ``ERROR`` (operational
    visibility — parity with #778's surfacing fix) and then degrades to
    an unbounded keep-alive phase, retrying every ``max_delay`` seconds
    until an entry loads or the caller cancels this coroutine (it is
    awaited-on-cancel at lifespan teardown — see
    :meth:`DBConfigModule._cancel_refresh_task`). A boot slow enough to
    outlast the bounded budget (#2908 — a connection pool that took
    ~156s to come up against a ~132.5s default budget) used to strand
    the snapshot empty for the rest of the process's life, with
    :class:`EngineInstanceCache` raising ``KeyError`` on every ref until
    a restart; now it just keeps trying.

    Returns ``True`` once the snapshot has at least one entry — from
    either phase. Only returns ``False`` in the degenerate case where
    :func:`list_registered_engines` is empty, i.e. there is nothing to
    ever wait for. Otherwise the only way out without an entry is
    cancellation, which raises ``asyncio.CancelledError`` rather than
    returning.
    """
    expected = len(list_registered_engines())
    delay = initial_delay
    attempt = 0
    for attempt in range(1, max_attempts + 1):
        loaded = await _snapshot_attempt(snapshot, pcfg)
        if loaded > 0:
            logger.info(
                "engine snapshot ready: attempt=%d loaded=%d/%d",
                attempt, loaded, expected,
            )
            return True
        if attempt < max_attempts:
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)

    logger.error(
        "engine snapshot: retry budget exhausted (attempts=%d, loaded=0/%d) — "
        "degrading to a keep-alive retry every %.1fs until the DB pool "
        "comes up or this task is cancelled at teardown. Likely root "
        "cause: DB pool has not become available yet.",
        max_attempts, expected, max_delay,
    )
    if expected == 0:
        # Nothing is ever going to load — an unbounded loop here would
        # spin forever with no way to reach loaded > 0.
        return False

    keepalive_started = time.monotonic()
    while True:
        await asyncio.sleep(max_delay)
        attempt += 1
        loaded = await _snapshot_attempt(snapshot, pcfg)
        if loaded > 0:
            logger.info(
                "engine snapshot ready: attempt=%d loaded=%d/%d "
                "(recovered %.1fs after the %d-attempt retry budget "
                "exhausted)",
                attempt, loaded, expected,
                time.monotonic() - keepalive_started, max_attempts,
            )
            return True


async def try_refresh_snapshot_once(
    snapshot: Dict[str, EngineConfig], pcfg: "PlatformConfigService"
) -> bool:
    """Single on-demand ``build_engine_snapshot`` attempt — #2857.

    For a caller that already owns its own retry loop and cadence (e.g.
    ``CacheModule``'s boot-upgrade recovery loop) and just wants "try to
    resolve the snapshot again right now" on each of its own attempts,
    without nesting :func:`refresh_snapshot_until_ready`'s independent
    backoff loop or triggering its terminal-exhaustion ``ERROR`` log on
    every single miss.

    Mutates ``snapshot`` in place, same as :func:`refresh_snapshot_until_ready`.
    Returns ``True`` when the snapshot has at least one entry after this
    attempt.
    """
    loaded = await _snapshot_attempt(snapshot, pcfg)
    return loaded > 0


def make_resolver(
    snapshot: Dict[str, EngineConfig],
) -> Callable[[str], Optional[EngineConfig]]:
    """Return a sync resolver closure over a snapshot dict.

    The returned callable matches the
    :class:`~dynastore.modules.db_config.engine_instance_cache.EngineInstanceCache`
    contract (``engine_ref → Optional[EngineConfig]``).  Treating unknown
    refs as ``None`` lets the cache surface a clear ``KeyError`` to callers
    rather than papering over typos with a default engine.

    The closure captures ``snapshot`` by reference, so mutations performed
    by :func:`refresh_snapshot_until_ready` are observed live.
    """

    def _resolve(engine_ref: str) -> Optional[EngineConfig]:
        return snapshot.get(engine_ref)

    return _resolve


def make_writer(
    snapshot: Dict[str, EngineConfig],
) -> Callable[[EngineConfig], None]:
    """Return a writer closure that swaps a fresh config into the snapshot.

    Symmetric to :func:`make_resolver`.  Writes ``config`` under BOTH
    ``class_key()`` and ``engine_class`` (when distinct) so any driver
    referencing either form sees the new instance on the next resolve.

    Used by :meth:`EngineInstanceCache.update_config` to push the fresh
    config received by a ``register_apply_handler`` callback back into the
    snapshot — without this, the apply handler invalidates the cached
    runtime instance but the next ``get`` re-instantiates against the
    stale boot-time config (#827).
    """

    def _write(config: EngineConfig) -> None:
        class_key = type(config).class_key()
        snapshot[class_key] = config
        if config.engine_class and config.engine_class != class_key:
            snapshot[config.engine_class] = config

    return _write


__all__ = [
    "build_engine_snapshot",
    "make_resolver",
    "make_writer",
    "refresh_snapshot_until_ready",
    "try_refresh_snapshot_once",
]
