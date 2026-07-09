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

"""Per-worker render admission gate (geoid#3155).

A single QGIS client issuing 6-8 concurrent MVT tile renders against heavy
geometry pinned ~3.1GiB RSS and OOM-killed a one-worker maps service every
~6 minutes — nothing in the render path bounded how much heap concurrent
renders could pin together. This gate caps concurrent tile/map renders on
THIS worker and sheds excess ones instead of admitting them into a doomed
allocation race:

1. **Cap.** A loop-local semaphore (:class:`~dynastore.tools.async_utils.
   LoopLocalSemaphore`) sized against the per-worker memory budget (see
   :func:`resolve_render_admission_cap`) bounds how many renders run at
   once.
2. **Brief bounded queueing.** A render that finds every slot taken waits
   up to ``queue_wait_seconds`` (a fixed constant, not a config knob — see
   the module-level constants below) rather than being admitted
   unconditionally; the caller's own request-time ceiling (e.g.
   ``TilesConfig.render_budget_seconds``, the 60s load-balancer timeout)
   stays authoritative.
3. **Shed, don't die.** ``acquire()`` raises :class:`RenderAdmissionRejected`
   — never blocks past the queue-wait timeout, never lets a render start
   while RSS is already at/above the memory-pressure threshold — so the
   caller can fail fast with 503 + ``Retry-After`` instead of letting one
   more render join the allocation race that killed the worker last time.
   On Cloud Run a shed response is the scale-out signal: the client's retry
   lands on a fresh instance with free RAM instead of piling onto this one.

Reuses the RSS/budget primitives ``tools/memory_watchdog.py`` already
exposes (``read_process_rss_bytes`` and ``resolve_watchdog_budget_mb``)
rather than re-deriving them — this module owns the render-admission
*policy* built on top of that pressure signal, not a second measurement of
it. Deliberately no async config-store round trip on the hot render-admission
path (every tile/map render calls ``acquire()``): the pressure ratio and
queue-wait bound are fixed constants, matching the codebase's "constant, not
knob" call for this exact gate.

No web-framework imports — this is a plain control-point primitive
(runner-agnostic, like ``tools/async_utils.py``); translating
``RenderAdmissionRejected`` into an HTTP 503 + ``Retry-After`` response is
the caller's job (see ``extensions/tiles/tiles_service.py``).
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Callable, Optional

from dynastore.tools.async_utils import LoopLocalSemaphore
from dynastore.tools.memory_watchdog import (
    read_process_rss_bytes,
    resolve_watchdog_budget_mb,
)

logger = logging.getLogger(__name__)

# Bounded queue wait for a render that finds every admission slot taken.
# Short enough that queueing plus a render still lands well inside the
# existing 55-60s request ceiling (TilesConfig.render_budget_seconds /
# live_tile_timeout_seconds); long enough to smooth a short burst instead of
# shedding on the very first collision. A fixed constant by design (#3155).
DEFAULT_QUEUE_WAIT_SECONDS = 10.0

# RSS/budget ratio at or above which the gate refuses to admit a new render,
# even with a free semaphore slot. Matches
# ``MemoryWatchdogConfig.critical_ratio``'s default (0.90): a render started
# this close to the budget is exactly the allocation race the watchdog's own
# critical-level warning already flags as high OOM risk. Kept as an
# independent constant (not read live from that config) so the render-
# admission hot path never depends on an async config-store round trip.
DEFAULT_PRESSURE_RATIO = 0.90

# Share of the per-worker memory budget concurrent renders may collectively
# pin, used only to size the semaphore cap (see resolve_render_admission_cap).
# Conservative — the rest of the budget covers the interpreter, connection
# pools, ORM/L1 caches, and everything else already resident in the process.
DEFAULT_RENDER_SHARE = 0.5

# Estimated peak heap one render can pin. Sized from the incident evidence
# (#3155): ~3.1GiB RSS pinned by 6-8 concurrent GAUL-level-1 MVT renders,
# ~400-500MiB/render at the high end for heavy geometry. Only used to derive
# the concurrency cap from the memory budget; actual per-render cost varies
# with geometry complexity (the per-tile feature/byte budget from #3155's
# point 2 is what bounds that directly — out of scope for this gate).
DEFAULT_RENDER_COST_MB = 400

# The cap never drops below this, even on a tiny/unresolvable budget (local
# dev, an undersized container) — a worker that can only ever admit one
# render at a time queues every single concurrent request needlessly.
_MIN_CONCURRENT = 2

# Cap used when no per-worker memory budget is resolvable at all (no RAM env
# / cgroup limit — e.g. local macOS dev): admit a small, fixed amount of
# concurrency rather than refusing to serve.
_FALLBACK_CONCURRENT = 4


def resolve_render_admission_cap(
    *,
    render_share: float = DEFAULT_RENDER_SHARE,
    render_cost_mb: int = DEFAULT_RENDER_COST_MB,
) -> int:
    """Derive the per-worker concurrent-render cap from the memory budget.

    Mirrors the ``tools/cache.py`` L1-byte-budget idiom (#3141): per-worker
    budget (container memory / GUNICORN_WORKERS, see
    ``memory_watchdog.resolve_watchdog_budget_mb``) times a conservative
    share reserved for renders, divided by an estimated per-render memory
    cost. Falls back to ``_FALLBACK_CONCURRENT`` when no budget is
    resolvable, floored at ``_MIN_CONCURRENT`` otherwise.
    """
    budget_mb = resolve_watchdog_budget_mb()
    if budget_mb is None:
        return _FALLBACK_CONCURRENT
    cap = int((budget_mb * render_share) // render_cost_mb)
    return max(_MIN_CONCURRENT, cap)


class RenderAdmissionRejected(Exception):
    """A render could not be admitted: queue-wait timeout or memory pressure.

    Carries ``retry_after_seconds`` so callers can build the same
    503 + ``Retry-After`` response this service's other capacity-shedding
    paths already use (DB pool saturation, render-budget abort).
    """

    def __init__(self, reason: str, *, retry_after_seconds: int = 5) -> None:
        self.reason = reason
        self.retry_after_seconds = retry_after_seconds
        super().__init__(f"render admission rejected: {reason}")


def _resolved_budget_bytes() -> Callable[[], Optional[int]]:
    """Resolve the per-worker budget ONCE and return a constant getter.

    The budget comes from process-start-time facts (RAM/GUNICORN_WORKERS env,
    else the cgroup limit file); re-resolving it on every ``acquire()`` would
    put an env parse — or worse, a blocking cgroup file read — on the hot
    render-admission path for a value that cannot change while the process
    lives.
    """
    budget_mb = resolve_watchdog_budget_mb()
    budget_bytes = None if budget_mb is None else budget_mb * 1024 * 1024
    return lambda: budget_bytes


class RenderAdmissionGate:
    """Per-worker cap on concurrent tile/map renders. See module docstring."""

    def __init__(
        self,
        *,
        max_concurrent: Optional[int] = None,
        queue_wait_seconds: float = DEFAULT_QUEUE_WAIT_SECONDS,
        pressure_ratio: float = DEFAULT_PRESSURE_RATIO,
        get_rss_bytes: Callable[[], Optional[int]] = read_process_rss_bytes,
        get_budget_bytes: Optional[Callable[[], Optional[int]]] = None,
    ) -> None:
        cap = (
            max_concurrent
            if max_concurrent is not None
            else resolve_render_admission_cap()
        )
        if cap <= 0:
            raise ValueError("max_concurrent must be positive")
        self.max_concurrent = cap
        self._semaphore = LoopLocalSemaphore(cap)
        self._queue_wait_seconds = queue_wait_seconds
        self._pressure_ratio = pressure_ratio
        self._get_rss_bytes = get_rss_bytes
        self._get_budget_bytes = get_budget_bytes or _resolved_budget_bytes()

    def _under_pressure(self) -> bool:
        """Cheap, allocation-free, non-blocking pressure check (one
        ``/proc/self/status`` read via ``get_rss_bytes``, no I/O beyond
        that). ``False`` whenever either signal is unavailable (e.g. local
        macOS dev has no per-worker budget) — the gate then falls back to
        pure concurrency capping."""
        limit_bytes = self._get_budget_bytes()
        if limit_bytes is None:
            return False
        rss_bytes = self._get_rss_bytes()
        if rss_bytes is None:
            return False
        return (rss_bytes / limit_bytes) >= self._pressure_ratio

    async def acquire(self) -> None:
        """Admit one render, or raise :class:`RenderAdmissionRejected`.

        Checks memory pressure BEFORE even attempting the semaphore — a
        worker already at/above the pressure threshold sheds immediately
        rather than spending the queue-wait budget on a render it was never
        going to start. On success, the caller owns exactly one matching
        :meth:`release` call.
        """
        if self._under_pressure():
            logger.warning(
                "render_admission: shedding — RSS pressure >= %.0f%% of "
                "budget before a slot was even attempted.",
                self._pressure_ratio * 100,
            )
            raise RenderAdmissionRejected("memory_pressure")

        acquired = await self._semaphore.acquire(timeout=self._queue_wait_seconds)
        if not acquired:
            logger.warning(
                "render_admission: shedding — %d-slot cap held for >%.0fs "
                "queue wait.",
                self.max_concurrent, self._queue_wait_seconds,
            )
            raise RenderAdmissionRejected("queue_timeout")

    def release(self) -> None:
        """Release a slot acquired via :meth:`acquire`."""
        self._semaphore.release()

    @asynccontextmanager
    async def admit(self) -> AsyncIterator[None]:
        """``async with gate.admit(): ...`` — the drop-in for a render call
        with no cross-task ownership handoff (see :meth:`acquire` /
        :meth:`release` directly when a render outlives this coroutine,
        e.g. a client-disconnect-shielded task)."""
        await self.acquire()
        try:
            yield
        finally:
            self.release()


__all__ = [
    "DEFAULT_PRESSURE_RATIO",
    "DEFAULT_QUEUE_WAIT_SECONDS",
    "DEFAULT_RENDER_COST_MB",
    "DEFAULT_RENDER_SHARE",
    "RenderAdmissionGate",
    "RenderAdmissionRejected",
    "resolve_render_admission_cap",
]
