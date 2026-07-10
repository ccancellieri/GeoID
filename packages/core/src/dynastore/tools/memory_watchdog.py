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

"""Proactive memory-pressure watchdog.

Cloud Run's out-of-memory killer sends ``SIGKILL`` straight to the
container, skipping ``SIGTERM`` entirely — the process gets no chance to
run any handler, log a line, or drain a request before it dies. That
makes an OOM kill genuinely invisible from inside the app: there is no
signal to catch, no exception to raise, no lifespan hook that ever runs
for it (see GeoID #2946).

What we *can* observe is the climb leading up to the kill. This module
polls the process's own resident set size (RSS) on a short cadence and
logs a structured warning/error once usage crosses configurable ratios
of a known memory budget. That turns a silent kill several minutes later
into a monitored log-based error well before it happens, without
requiring any new dependency — RSS is read straight from
``/proc/self/status``, which is always present in the Linux containers
this service runs in (returns ``None``, so the watchdog no-ops, on any
platform where that file does not exist, e.g. local macOS dev).

The per-worker memory budget is derived at service construction from the
deploy env — the container memory size (``RAM`` env var, else the cgroup
limit) divided by ``GUNICORN_WORKERS`` — because each gunicorn worker is a
separate process watching its own RSS. This needs no config-store round-trip,
so the budget is available from the very first ``tick()``. Cloud Run's sandbox
does not expose ``/sys/fs/cgroup`` at all (see ``modules/scaling/publisher.py``
for the confirmed detail), so cgroup auto-detection always returns ``None``
there — which is exactly why the budget is read from the deploy-injected
``RAM``/``GUNICORN_WORKERS`` env rather than the filesystem. The ratios in
``MemoryWatchdogConfig`` (``warn_ratio``/``critical_ratio``/``recycle_ratio``)
are thus percentages of each service's own RAM, with no hand-maintained
per-service MB to keep in sync with the deploy config. An operator may still
pin an explicit ``config.limit_mb`` per-worker override (read live every tick);
when neither the env nor cgroup yields a budget (e.g. local macOS dev), the
watchdog stays inert rather than guessing.

Lever B — readiness-shed + graceful self-recycle (geoid#2946, #2924)
----------------------------------------------------------------------
An OOM kill is a bare ``SIGKILL`` — no drain, no lifespan shutdown, DB
sessions and locks leaked (#2924), in-flight requests dropped (#2946). This
watchdog can pre-empt that: when RSS crosses ``recycle_ratio`` of the
budget, it sets the process-global draining flag
(``tools/serving_state.py``) and sends itself ``SIGTERM`` — which the
``DrainAwareUvicornWorker`` gunicorn worker class DOES turn into a bounded
drain (see ``scripts/gunicorn_worker.py`` / ``scripts/start.sh``), running
ASGI lifespan shutdown and releasing DB-side resources before the process
exits. The draining flag also makes ``/ready`` report unhealthy and, when
``readiness_shed_enabled`` is on, makes the app-level shed middleware
answer new requests 503 — Cloud Run itself has no readiness gate, so that
middleware is the only lever that actually steers new traffic away from a
draining worker before its SIGTERM lands.

Every self-recycle behavior is gated by its own ``Mutable`` config field —
default OFF everywhere — with a minimum uptime, a cooldown, and a
per-worker random jitter so a fleet of workers crossing the threshold
together does not recycle in lockstep (each worker recycles itself only,
never anything instance- or fleet-wide).
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import signal
import time
import tracemalloc
from typing import Callable, ClassVar, Optional, Tuple

from pydantic import Field, model_validator

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig
from dynastore.tools.background_service import (
    Leadership,
    PeriodicService,
    PodPolicy,
    ServiceContext,
)
from dynastore.tools.memory_units import (
    detect_cgroup_memory_limit_mb,
    detect_container_memory_mb,
    parse_memory_to_mb,
)
from dynastore.tools.serving_state import clear_draining, set_draining

logger = logging.getLogger(__name__)

_PROC_STATUS_PATH = "/proc/self/status"

# The process count sharing the container's memory; each worker's budget is
# the total divided by this. See ``resolve_watchdog_budget_mb``.
_WORKERS_ENV = "GUNICORN_WORKERS"

# Number of stack frames tracemalloc keeps per allocation for the diagnostic
# (geoid#3121). One frame only ever names the leaf, which for the spikes seen on
# dev is always ``json/decoder.py`` — true and useless. Reaching the code that
# asked for the decode costs four frames (raw_decode <- decode <- loads <-
# caller); six leaves room for one wrapper. tracemalloc interns tracebacks and
# stores one pointer per traced block, so depth costs a bounded number of
# traceback objects, not per-allocation memory.
_TRACEMALLOC_FRAMES = 6

# tracemalloc groups by leaf line under "lineno"; "traceback" keys each
# statistic by the whole retained frame chain, which is the point of keeping
# more than one frame.
_TRACEMALLOC_GROUP_BY = "traceback"


def _format_alloc_site(traceback: "tracemalloc.Traceback") -> str:
    """Render a traced allocation's frame chain, leaf first.

    ``str(Traceback)`` shows only the most recent frame, so a multi-frame
    snapshot would still read as a bare ``json/decoder.py:361``.
    """
    frames = [f"{frame.filename}:{frame.lineno}" for frame in reversed(traceback)]
    if not frames:
        return "<no traceback>"
    return "\n".join(f"       {frame}" for frame in frames)


def read_process_rss_bytes() -> Optional[int]:
    """Return this process's current resident set size (RSS) in bytes.

    Reads the ``VmRSS`` line from ``/proc/self/status`` (Linux-only).
    Returns ``None`` if the file does not exist or the line cannot be
    parsed, so callers on non-Linux platforms (or under restricted
    sandboxes) degrade to a no-op rather than raising.
    """
    try:
        with open(_PROC_STATUS_PATH, "r", encoding="ascii") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    # Format: "VmRSS:	   12345 kB\n"
                    parts = line.split()
                    if len(parts) >= 2:
                        return int(parts[1]) * 1024
                    return None
    except (OSError, ValueError):
        return None
    return None


def read_process_rss_breakdown_bytes() -> Optional[Tuple[int, int]]:
    """Return this process's ``(RssAnon, RssFile)`` in bytes.

    ``VmRSS`` — what :func:`read_process_rss_bytes` returns — is the sum of
    private anonymous pages, resident file-backed pages and shared memory.
    Only ``RssAnon`` is private to this worker and therefore additive across
    the gunicorn fleet; file-backed pages (shared-library text, mmap'd data)
    are counted in full by *every* process that maps them, so ``VmRSS`` can
    overstate a worker's own share of the container. How much it overstates is
    a property of the service, not a constant: dev catalog measured ``anon
    1976MiB, file 116MiB``, so there its ``VmRSS`` was very nearly all private.
    Log both rather than assuming either.

    Returns ``None`` when the fields are absent (Linux < 4.5, or non-Linux),
    so callers degrade to reporting ``VmRSS`` alone rather than raising.
    """
    anon: Optional[int] = None
    file_backed: Optional[int] = None
    try:
        with open(_PROC_STATUS_PATH, "r", encoding="ascii") as fh:
            for line in fh:
                if line.startswith("RssAnon:"):
                    anon = int(line.split()[1]) * 1024
                elif line.startswith("RssFile:"):
                    file_backed = int(line.split()[1]) * 1024
                if anon is not None and file_backed is not None:
                    return anon, file_backed
    except (OSError, IndexError, ValueError):
        return None
    return None


def _rss_breakdown_suffix() -> str:
    """Render ``" [anon NMiB, file NMiB]"`` for the watchdog log lines, or ``""``."""
    breakdown = read_process_rss_breakdown_bytes()
    if breakdown is None:
        return ""
    anon, file_backed = breakdown
    return " [anon {:.0f}MiB, file {:.0f}MiB]".format(
        anon / (1024 * 1024), file_backed / (1024 * 1024)
    )


class MemoryWatchdogService(PeriodicService):
    """Periodic service that logs a warning/error as RSS approaches a memory budget.

    Policy:
      - leadership = RUN_EVERYWHERE: each pod/worker only ever observes its
        own RSS, so there is nothing to elect — every instance runs this
        independently.
      - pod_policy = ALL: ephemeral one-shot containers can OOM too.
    """

    name = "memory_watchdog"
    leadership = Leadership.RUN_EVERYWHERE
    pod_policy = PodPolicy.ALL

    def __init__(
        self,
        *,
        limit_bytes: Optional[int] = None,
        warn_ratio: float = 0.80,
        critical_ratio: float = 0.90,
        cadence_seconds: float = 15.0,
        get_rss_bytes: Callable[[], Optional[int]] = read_process_rss_bytes,
    ) -> None:
        if limit_bytes is not None and limit_bytes <= 0:
            raise ValueError("limit_bytes must be positive")
        if not 0 < warn_ratio < critical_ratio <= 1:
            raise ValueError("require 0 < warn_ratio < critical_ratio <= 1")
        # Test/caller injection of an explicit budget (every tick()-focused
        # unit test below passes this). None means "derive it".
        self._explicit_limit_bytes = limit_bytes
        # Per-worker budget derived from the deploy env (RAM / GUNICORN_WORKERS),
        # resolved synchronously at startup with NO dependency on the DB-backed
        # config store. This is the key fix over the previous lazy first-tick
        # resolution: that ran before the store (or a failed early load) was
        # reachable and latched "no limit" permanently. The env budget is
        # available from process start, so the watchdog is armed from the first
        # tick. An explicit ``config.limit_mb`` (rare) still overrides it live,
        # read fresh in ``_effective_limit_bytes`` each tick.
        self._env_budget_bytes: Optional[int] = None
        if limit_bytes is None:
            budget_mb = resolve_watchdog_budget_mb()
            if budget_mb is not None:
                self._env_budget_bytes = budget_mb * 1024 * 1024
        self._warn_ratio = warn_ratio
        self._critical_ratio = critical_ratio
        self.cadence_seconds = cadence_seconds
        self._get_rss_bytes = get_rss_bytes
        # Only log the WARNING-level transition once per crossing (not every
        # tick) so a sustained plateau above warn_ratio doesn't spam the
        # logs; the CRITICAL transition always logs since it is the
        # actionable, monitored signal this service exists to raise.
        self._warned = False
        # Throttle the "no budget resolved" inert notice to once per crossing.
        self._inert_warned = False
        # Lever B (self-recycle) bookkeeping — see _maybe_self_recycle. The
        # recycle knobs are read fresh from config on every tick (they are the
        # hot-reload kill-switches).
        self._started_at = time.monotonic()
        self._last_recycle_attempt: Optional[float] = None
        # Diagnostic (geoid#3121) bookkeeping: throttle tracemalloc snapshots,
        # remember whether THIS service started tracing (so it can stop it
        # again when the flag is turned back off), and hold the previous
        # snapshot each growth report is diffed against (captured on the
        # arming tick, rolled forward on every report).
        self._last_diag_snapshot: Optional[float] = None
        self._diag_started = False
        self._diag_baseline: Optional[tracemalloc.Snapshot] = None

    def _effective_limit_bytes(
        self, config: "MemoryWatchdogConfig"
    ) -> Optional[int]:
        """The per-worker RSS budget in bytes, recomputed every tick.

        Priority: an explicit ``limit_bytes`` passed to the constructor (unit
        tests / callers) > an operator-set ``config.limit_mb`` (a per-worker
        override, read live so it can be tuned without a redeploy) > the
        env-derived per-worker budget resolved once at startup. ``None`` when
        none is available — the watchdog then stays inert.
        """
        if self._explicit_limit_bytes is not None:
            return self._explicit_limit_bytes
        if config.limit_mb is not None:
            return config.limit_mb * 1024 * 1024
        return self._env_budget_bytes

    async def tick(self, ctx: ServiceContext) -> None:
        config = await load_memory_watchdog_config()
        limit_bytes = self._effective_limit_bytes(config)

        # Apply the configured cadence live: PeriodicService.run() reads
        # self.cadence_seconds before every sleep, but the service is built
        # before the config store is reachable, so the constructor only ever
        # sees the code default (15s). An OOM spike that completes between two
        # 15s ticks is invisible to the diagnostic; operators need to be able
        # to shorten the sampling window through the config store without a
        # redeploy.
        if config.cadence_seconds > 0:
            self.cadence_seconds = config.cadence_seconds

        rss_bytes = self._get_rss_bytes()
        if rss_bytes is None or limit_bytes is None:
            if limit_bytes is None and not self._inert_warned:
                logger.warning(
                    "memory_watchdog: no per-worker memory budget resolved "
                    "(no limit_mb in config, and no RAM env / cgroup limit) on "
                    "%s — watchdog stays inert (no RSS budget to compare "
                    "against).",
                    ctx.name,
                )
                self._inert_warned = True
            return
        self._inert_warned = False
        ratio = rss_bytes / limit_bytes
        if ratio >= self._critical_ratio:
            logger.error(
                "memory_watchdog: RSS %.0fMiB%s is %.0f%% of the %.0fMiB budget "
                "(>= critical %.0f%%) on %s; instance is at high risk of an "
                "imminent OOM kill.",
                rss_bytes / (1024 * 1024),
                _rss_breakdown_suffix(),
                ratio * 100,
                limit_bytes / (1024 * 1024),
                self._critical_ratio * 100,
                ctx.name,
            )
            self._warned = True
        elif ratio >= self._warn_ratio:
            if not self._warned:
                logger.warning(
                    "memory_watchdog: RSS %.0fMiB%s is %.0f%% of the %.0fMiB budget "
                    "(>= warn %.0f%%) on %s.",
                    rss_bytes / (1024 * 1024),
                    _rss_breakdown_suffix(),
                    ratio * 100,
                    limit_bytes / (1024 * 1024),
                    self._warn_ratio * 100,
                    ctx.name,
                )
                self._warned = True
        else:
            self._warned = False

        self._maybe_emit_tracemalloc(ctx, config, ratio, rss_bytes, limit_bytes)
        await self._maybe_self_recycle(ctx, config, ratio, limit_bytes)

    def _maybe_emit_tracemalloc(
        self,
        ctx: ServiceContext,
        config: "MemoryWatchdogConfig",
        ratio: float,
        rss_bytes: int,
        limit_bytes: int,
    ) -> None:
        """Diagnostic (geoid#3121): log which allocation sites GREW as a worker
        climbs toward its budget.

        A kernel OOM kill is a bare SIGKILL between watchdog polls — no handler
        runs, so the allocation that spiked RSS is otherwise invisible. When
        ``diagnostic_tracemalloc_enabled`` is on, this lazily starts tracemalloc,
        captures a baseline snapshot on that same arming tick, and from the next
        qualifying tick on logs ``snapshot.compare_to(baseline, "traceback")`` —
        the sites whose footprint grew since the previous report — rather than
        an absolute top-N, which is dominated by legitimate steady-state
        allocations (SQLAlchemy metadata, client buffers) and never names the
        *growing* site. Grouping by traceback rather than leaf line keeps the
        calling frames: a spike inside ``json.loads`` is attributable to the
        code that asked for the decode, not to ``json/decoder.py``. The
        baseline rolls forward after every report, so
        one-time warm-up allocations appear once and disappear while a genuine
        leak dominates every subsequent report, with a per-interval growth rate.

        Every report also logs tracemalloc's own traced total next to RSS: a
        large RSS-vs-traced gap (or a report with no Python-level growth while
        RSS keeps climbing) means the growth is happening outside CPython's
        allocator — native/C-extension memory tracemalloc cannot see — and the
        next tool is a native profiler, not more Python-side reading.

        ``diagnostic_ratio`` sits deliberately below the OOM point so the
        snapshot's own allocation has headroom and the *climb* toward the kill
        is captured. Reports are throttled to
        ``diagnostic_min_interval_seconds``. Off by default — tracemalloc adds
        per-allocation overhead.

        The flag is read live here (per tick), not at service-build time: the
        watchdog is built before the platform config store is reachable (see
        ``build_memory_watchdog_service``), so a build-time read would always
        see the default (off) and the diagnostic could never be turned on
        through the config store. Starting tracemalloc on the first tick that
        sees the flag misses allocations made before that tick, but a recurring
        leak keeps allocating, so the culprit still surfaces on the climb.
        """
        if not config.diagnostic_tracemalloc_enabled:
            # Turned back off: release tracemalloc's per-allocation overhead if
            # this service was the one that started it.
            if self._diag_started and tracemalloc.is_tracing():
                tracemalloc.stop()
            self._diag_started = False
            self._diag_baseline = None
            return
        if ratio < config.diagnostic_ratio:
            return
        if not tracemalloc.is_tracing():
            # Lazy start: arm tracing and capture the baseline on THIS tick (a
            # snapshot taken the instant tracing starts is near-empty and
            # cheap), so the very next qualifying tick already emits a
            # meaningful growth diff — a worker OOM-killed shortly after
            # arming still leaves one report behind.
            tracemalloc.start(_TRACEMALLOC_FRAMES)
            self._diag_started = True
            self._diag_baseline = tracemalloc.take_snapshot()
            logger.warning(
                "memory_watchdog: diagnostic_tracemalloc_enabled — started "
                "tracemalloc (%d frames) and captured the baseline snapshot on "
                "%s; allocation-growth reports begin on the next tick above "
                "diagnostic_ratio.",
                _TRACEMALLOC_FRAMES,
                ctx.name,
            )
            return

        now = time.monotonic()
        if (
            self._last_diag_snapshot is not None
            and now - self._last_diag_snapshot
            < config.diagnostic_min_interval_seconds
        ):
            return
        interval = (
            now - self._last_diag_snapshot
            if self._last_diag_snapshot is not None
            else None
        )
        self._last_diag_snapshot = now

        snapshot = tracemalloc.take_snapshot()
        traced_bytes, _ = tracemalloc.get_traced_memory()
        top_n = max(1, config.diagnostic_top_n)

        if self._diag_baseline is None:
            # Tracing was already on before this service saw the flag (e.g. a
            # PYTHONTRACEMALLOC boot or another component armed it), so there
            # is no arming-tick baseline. Report the absolute top sites once —
            # this is the baseline report — and diff from here on.
            self._diag_baseline = snapshot
            stats = snapshot.statistics(_TRACEMALLOC_GROUP_BY)
            top = stats[:top_n]
            lines = "\n".join(
                f"  {i + 1}. {s.size / (1024 * 1024):.1f}MiB in {s.count} blocks:\n"
                f"{_format_alloc_site(s.traceback)}"
                for i, s in enumerate(top)
            )
            logger.error(
                "memory_watchdog[diagnostic]: RSS %.0fMiB (%.0f%% of %.0fMiB "
                "budget) on %s; tracemalloc traced total %.1fMiB — baseline "
                "report, top %d Python allocation sites by size (growth diffs "
                "follow from the next report):\n%s",
                rss_bytes / (1024 * 1024),
                ratio * 100,
                limit_bytes / (1024 * 1024),
                ctx.name,
                traced_bytes / (1024 * 1024),
                len(top),
                lines,
            )
            return

        diffs = snapshot.compare_to(self._diag_baseline, _TRACEMALLOC_GROUP_BY)
        self._diag_baseline = snapshot
        since = f"{interval:.0f}s ago" if interval is not None else "arming"
        grown = [d for d in diffs if d.size_diff > 0][:top_n]
        if not grown:
            logger.error(
                "memory_watchdog[diagnostic]: RSS %.0fMiB (%.0f%% of %.0fMiB "
                "budget) on %s; tracemalloc traced total %.1fMiB — NO "
                "Python-level allocation growth since the previous snapshot "
                "(%s). If RSS keeps climbing, the growth is outside CPython's "
                "allocator (native/C-extension memory tracemalloc cannot see).",
                rss_bytes / (1024 * 1024),
                ratio * 100,
                limit_bytes / (1024 * 1024),
                ctx.name,
                traced_bytes / (1024 * 1024),
                since,
            )
            return

        lines = "\n".join(
            f"  {i + 1}. +{d.size_diff / (1024 * 1024):.1f}MiB "
            f"(+{d.count_diff} blocks, total {d.size / (1024 * 1024):.1f}MiB):\n"
            f"{_format_alloc_site(d.traceback)}"
            for i, d in enumerate(grown)
        )
        logger.error(
            "memory_watchdog[diagnostic]: RSS %.0fMiB (%.0f%% of %.0fMiB budget) "
            "on %s; tracemalloc traced total %.1fMiB — top %d Python allocation "
            "sites by growth since the previous snapshot (%s):\n%s",
            rss_bytes / (1024 * 1024),
            ratio * 100,
            limit_bytes / (1024 * 1024),
            ctx.name,
            traced_bytes / (1024 * 1024),
            len(grown),
            since,
            lines,
        )

    async def _maybe_self_recycle(
        self,
        ctx: ServiceContext,
        config: "MemoryWatchdogConfig",
        ratio: float,
        limit_bytes: int,
    ) -> None:
        """Pre-empt an OOM kill by gracefully recycling THIS worker (Lever B).

        Guardrails, all non-negotiable: default-off (``self_recycle_enabled``
        is the live kill-switch); a minimum uptime so a just-booted worker is
        never recycled; a cooldown between attempts; and a per-worker random
        jitter, re-checking the ratio afterwards, so a fleet of workers
        crossing the threshold together does not all recycle in lockstep and
        a transient spike does not trigger an unnecessary recycle.
        """
        if not config.self_recycle_enabled or ratio < config.recycle_ratio:
            return

        now = time.monotonic()
        uptime = now - self._started_at
        if uptime < config.recycle_min_uptime_seconds:
            return
        if (
            self._last_recycle_attempt is not None
            and now - self._last_recycle_attempt < config.recycle_cooldown_seconds
        ):
            return

        self._last_recycle_attempt = now
        set_draining()

        jitter = random.uniform(0, config.recycle_jitter_seconds)
        if jitter > 0:
            await asyncio.sleep(jitter)

        # Re-check after the jitter delay: a transient spike that has
        # already subsided by now should not trigger a recycle.
        rss_bytes = self._get_rss_bytes()
        if rss_bytes is None:
            clear_draining()
            return
        recheck_ratio = rss_bytes / limit_bytes
        if recheck_ratio < config.recycle_ratio:
            logger.info(
                "memory_watchdog: self-recycle aborted after %.1fs jitter — "
                "RSS dropped to %.0f%% (< recycle threshold %.0f%%) on %s.",
                jitter, recheck_ratio * 100, config.recycle_ratio * 100, ctx.name,
            )
            clear_draining()
            return

        pid = os.getpid()
        logger.warning(
            "self-recycle: RSS %.0f%%>=%.0f%%%s, SIGTERM worker pid %d to "
            "drain before OOM (uptime=%.0fs) on %s.",
            recheck_ratio * 100, config.recycle_ratio * 100,
            _rss_breakdown_suffix(), pid, uptime, ctx.name,
        )
        os.kill(pid, signal.SIGTERM)


class MemoryWatchdogConfig(PluginConfig):
    """Configuration for the proactive memory-pressure watchdog (geoid#2946)."""

    _address: ClassVar[Tuple[str, ...]] = ("platform", "tools", "memory_watchdog")

    enabled: Mutable[bool] = Field(
        default=True,
        description=(
            "Master switch for the memory watchdog. Defaults to True on "
            "every environment: the watchdog only ever reads its own "
            "process's RSS and logs a warning/error as it climbs — no "
            "connections, no writes, no side effects — so there is no "
            "reason for it to be opt-in. Read at service-build time."
        ),
    )

    limit_mb: Mutable[Optional[int]] = Field(
        default=None,
        gt=0,
        description=(
            "Optional explicit per-worker memory budget (in MB) this process "
            "is watched against. Leave unset (the default): the budget is then "
            "derived automatically from the deploy env as the container memory "
            "(RAM env var, else cgroup limit) divided by GUNICORN_WORKERS, "
            "since each worker is its own process with its own RSS — so it "
            "tracks each service's RAM without a hand-maintained per-service "
            "number. Set this only to override that (e.g. an env where RAM is "
            "not injected and no cgroup limit is exposed). Read live every tick."
        ),
    )

    warn_ratio: Mutable[float] = Field(
        default=0.80,
        description="Ratio of limit_mb at which the watchdog logs a WARNING.",
    )

    critical_ratio: Mutable[float] = Field(
        default=0.90,
        description="Ratio of limit_mb at which the watchdog logs an ERROR.",
    )

    cadence_seconds: Mutable[float] = Field(
        default=15.0,
        gt=0,
        description="How often (seconds) the watchdog polls this process's RSS.",
    )

    readiness_shed_enabled: Mutable[bool] = Field(
        default=False,
        description=(
            "Kill-switch for the app-level request-shedding middleware "
            "(Lever B). While a worker is draining (see self_recycle_enabled) "
            "AND this is True, ordinary requests get 503 + Retry-After instead "
            "of reaching the app — the actual traffic-steering lever on Cloud "
            "Run, which exposes no readiness gate of its own. Defaults to "
            "False: enabling self-recycle without this only stops the worker "
            "from reporting itself /ready; new requests can still land on it "
            "during the drain window. Read live on every request while "
            "draining."
        ),
    )

    self_recycle_enabled: Mutable[bool] = Field(
        default=False,
        description=(
            "Kill-switch for graceful self-recycle (Lever B): when RSS "
            "crosses recycle_ratio, this worker sets the draining flag and "
            "sends itself SIGTERM (drained by DrainAwareUvicornWorker) instead "
            "of waiting for the platform's SIGKILL, which skips any drain "
            "entirely. Defaults to False. Read live on every tick."
        ),
    )

    recycle_ratio: Mutable[float] = Field(
        default=0.92,
        gt=0,
        lt=1,
        description=(
            "Ratio of limit_mb at which a self-recycle is triggered (subject "
            "to recycle_min_uptime_seconds and recycle_cooldown_seconds). MUST "
            "be strictly greater than critical_ratio and less than 1 (enforced "
            "by a validator) — recycling must never trigger before the "
            "critical warning it follows. Read live on every tick."
        ),
    )

    recycle_min_uptime_seconds: Mutable[float] = Field(
        default=120.0,
        ge=0,
        description=(
            "Minimum worker uptime before a self-recycle is even considered — "
            "guards against recycling a worker that has barely finished "
            "booting. Read live on every tick."
        ),
    )

    recycle_cooldown_seconds: Mutable[float] = Field(
        default=300.0,
        ge=0,
        description=(
            "Minimum time between two self-recycle attempts by this worker. "
            "Read live on every tick."
        ),
    )

    recycle_jitter_seconds: Mutable[float] = Field(
        default=10.0,
        ge=0,
        description=(
            "Upper bound of a random per-worker delay (seconds) applied "
            "between deciding to recycle and actually sending SIGTERM, so a "
            "fleet of workers crossing recycle_ratio together does not all "
            "recycle in lockstep. The ratio is re-checked after the delay; a "
            "transient spike that has already subsided aborts the recycle. "
            "Read live on every tick."
        ),
    )

    diagnostic_tracemalloc_enabled: Mutable[bool] = Field(
        default=False,
        description=(
            "Diagnostic (geoid#3121): when True, the watchdog lazily starts "
            "tracemalloc and logs the top Python allocation sites by size once "
            "RSS climbs past diagnostic_ratio, so a between-poll OOM spike "
            "leaves a breadcrumb naming the allocation instead of a bare "
            "SIGKILL. Off by default — tracemalloc adds per-allocation "
            "overhead — and meant to be switched on transiently in one "
            "environment to locate a leak/spike, then switched back off (which "
            "stops tracemalloc again — no restart needed either way). Read "
            "live every tick."
        ),
    )

    diagnostic_ratio: Mutable[float] = Field(
        default=0.60,
        gt=0,
        le=1,
        description=(
            "Ratio of the per-worker budget at which a diagnostic tracemalloc "
            "snapshot is logged (when diagnostic_tracemalloc_enabled). "
            "Deliberately well below the OOM point so the snapshot's own "
            "allocation has headroom and the climb toward the kill is "
            "captured. Read live every tick."
        ),
    )

    diagnostic_top_n: Mutable[int] = Field(
        default=12,
        gt=0,
        description=(
            "How many top allocation sites (by size) the diagnostic snapshot "
            "logs. Read live every tick."
        ),
    )

    diagnostic_min_interval_seconds: Mutable[float] = Field(
        default=6.0,
        ge=0,
        description=(
            "Minimum time between two diagnostic tracemalloc snapshots so a "
            "sustained climb does not log one every tick. Read live every tick."
        ),
    )

    @model_validator(mode="after")
    def _warn_below_critical(self) -> "MemoryWatchdogConfig":
        if not 0 < self.warn_ratio < self.critical_ratio <= 1:
            raise ValueError(
                "MemoryWatchdogConfig: require 0 < warn_ratio < critical_ratio <= 1 "
                f"(got warn_ratio={self.warn_ratio}, critical_ratio={self.critical_ratio})"
            )
        return self

    @model_validator(mode="after")
    def _recycle_ratio_above_critical(self) -> "MemoryWatchdogConfig":
        if not self.critical_ratio < self.recycle_ratio < 1:
            raise ValueError(
                "MemoryWatchdogConfig: require critical_ratio < recycle_ratio < 1 "
                f"(got critical_ratio={self.critical_ratio}, recycle_ratio={self.recycle_ratio})"
            )
        return self






def resolve_watchdog_budget_mb() -> Optional[int]:
    """Per-worker RSS budget (MB): total container memory / gunicorn workers.

    Each gunicorn worker is its own process watching its own RSS, so the budget
    a single worker is measured against is the container's memory divided by the
    worker count (``GUNICORN_WORKERS`` env, default 1). Derived entirely from
    process env — no dependency on the DB-backed config store — so the budget is
    available from the very first tick, even before that store is reachable.
    Returns ``None`` when no total-memory source is available.
    """
    total_mb = detect_container_memory_mb()
    if total_mb is None:
        return None
    try:
        workers = int(os.environ.get(_WORKERS_ENV, "1") or "1")
    except ValueError:
        workers = 1
    workers = max(1, workers)
    return max(1, total_mb // workers)


async def load_memory_watchdog_config() -> MemoryWatchdogConfig:
    """Load ``MemoryWatchdogConfig`` from the platform config store.

    Falls back to the default instance if the store is unavailable or the
    config has not been set (mirrors ``load_zombie_session_reaper_config``).
    """
    try:
        from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        config_mgr = get_protocol(PlatformConfigsProtocol)
        if config_mgr is not None:
            cfg = await config_mgr.get_config(MemoryWatchdogConfig)
            if isinstance(cfg, MemoryWatchdogConfig):
                return cfg
    except Exception as exc:
        logger.warning(
            "memory_watchdog: failed to load MemoryWatchdogConfig (%s) — "
            "using defaults.", exc,
        )
    return MemoryWatchdogConfig()


async def build_memory_watchdog_service(
    config: Optional[MemoryWatchdogConfig] = None,
) -> Optional[MemoryWatchdogService]:
    """Build a :class:`MemoryWatchdogService` from :class:`MemoryWatchdogConfig`.

    Loads the config from the platform config store when *config* is not
    supplied. Returns ``None`` (watchdog inert) only when disabled via
    config — never merely because a memory budget could not yet be
    resolved. The per-worker budget is derived from the process env
    (RAM / GUNICORN_WORKERS, else cgroup) inside the service constructor,
    which needs no config store, so an explicit ``config.limit_mb`` override
    is read live on every ``tick()`` rather than captured here — this runs at
    lifespan start, before the store is reachable.
    """
    if config is None:
        config = await load_memory_watchdog_config()
    if not config.enabled:
        logger.debug("memory_watchdog: disabled via config — skipping.")
        return None

    # The tracemalloc diagnostic is armed lazily from the tick path, not here:
    # this runs at lifespan start, before the platform config store is
    # reachable, so ``config`` is always the code defaults at build time and any
    # store-set flag would be missed. See ``_maybe_emit_tracemalloc``.
    return MemoryWatchdogService(
        warn_ratio=config.warn_ratio,
        critical_ratio=config.critical_ratio,
        cadence_seconds=config.cadence_seconds,
    )


__all__ = [
    "MemoryWatchdogConfig",
    "MemoryWatchdogService",
    "build_memory_watchdog_service",
    "detect_cgroup_memory_limit_mb",
    "load_memory_watchdog_config",
    "parse_memory_to_mb",
    "read_process_rss_bytes",
    "read_process_rss_breakdown_bytes",
    "resolve_watchdog_budget_mb",
]
