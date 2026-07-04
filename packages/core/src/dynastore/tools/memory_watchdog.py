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

The memory budget itself (``MEMORY_WATCHDOG_LIMIT_MB``) is deploy-time
configuration — typically the container's memory limit divided by the
number of gunicorn workers sharing it, since each worker is its own
process with its own RSS. Unset by default: the watchdog only starts if
a limit is explicitly configured, so existing deployments are unaffected
until an operator opts in.
"""

from __future__ import annotations

import logging
import os
from typing import Callable, Optional

from dynastore.tools.background_service import (
    Leadership,
    PeriodicService,
    PodPolicy,
    ServiceContext,
)

logger = logging.getLogger(__name__)

_PROC_STATUS_PATH = "/proc/self/status"


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
        limit_bytes: int,
        warn_ratio: float = 0.80,
        critical_ratio: float = 0.90,
        cadence_seconds: float = 15.0,
        get_rss_bytes: Callable[[], Optional[int]] = read_process_rss_bytes,
    ) -> None:
        if limit_bytes <= 0:
            raise ValueError("limit_bytes must be positive")
        if not 0 < warn_ratio < critical_ratio <= 1:
            raise ValueError("require 0 < warn_ratio < critical_ratio <= 1")
        self._limit_bytes = limit_bytes
        self._warn_ratio = warn_ratio
        self._critical_ratio = critical_ratio
        self.cadence_seconds = cadence_seconds
        self._get_rss_bytes = get_rss_bytes
        # Only log the WARNING-level transition once per crossing (not every
        # tick) so a sustained plateau above warn_ratio doesn't spam the
        # logs; the CRITICAL transition always logs since it is the
        # actionable, monitored signal this service exists to raise.
        self._warned = False

    async def tick(self, ctx: ServiceContext) -> None:
        rss_bytes = self._get_rss_bytes()
        if rss_bytes is None:
            return
        ratio = rss_bytes / self._limit_bytes
        if ratio >= self._critical_ratio:
            logger.error(
                "memory_watchdog: RSS %.0fMiB is %.0f%% of the %.0fMiB budget "
                "(>= critical %.0f%%) on %s; instance is at high risk of an "
                "imminent OOM kill.",
                rss_bytes / (1024 * 1024),
                ratio * 100,
                self._limit_bytes / (1024 * 1024),
                self._critical_ratio * 100,
                ctx.name,
            )
            self._warned = True
        elif ratio >= self._warn_ratio:
            if not self._warned:
                logger.warning(
                    "memory_watchdog: RSS %.0fMiB is %.0f%% of the %.0fMiB budget "
                    "(>= warn %.0f%%) on %s.",
                    rss_bytes / (1024 * 1024),
                    ratio * 100,
                    self._limit_bytes / (1024 * 1024),
                    self._warn_ratio * 100,
                    ctx.name,
                )
                self._warned = True
        else:
            self._warned = False


def build_memory_watchdog_service_from_env() -> Optional[MemoryWatchdogService]:
    """Build a :class:`MemoryWatchdogService` from ``MEMORY_WATCHDOG_*`` env vars.

    Returns ``None`` (watchdog disabled) unless ``MEMORY_WATCHDOG_LIMIT_MB``
    is set to a positive number — this keeps every existing deployment
    behavior-unchanged until an operator opts in with the per-worker memory
    budget for that deployment.
    """
    raw_limit = os.environ.get("MEMORY_WATCHDOG_LIMIT_MB")
    if not raw_limit:
        return None
    try:
        limit_mb = float(raw_limit)
    except ValueError:
        logger.warning(
            "memory_watchdog: MEMORY_WATCHDOG_LIMIT_MB=%r is not a number; "
            "watchdog disabled.",
            raw_limit,
        )
        return None
    if limit_mb <= 0:
        logger.warning(
            "memory_watchdog: MEMORY_WATCHDOG_LIMIT_MB=%r must be positive; "
            "watchdog disabled.",
            raw_limit,
        )
        return None

    def _float_env(name: str, default: float) -> float:
        raw = os.environ.get(name)
        if not raw:
            return default
        try:
            return float(raw)
        except ValueError:
            logger.warning(
                "memory_watchdog: %s=%r is not a number; using default %.2f.",
                name,
                raw,
                default,
            )
            return default

    return MemoryWatchdogService(
        limit_bytes=int(limit_mb * 1024 * 1024),
        warn_ratio=_float_env("MEMORY_WATCHDOG_WARN_RATIO", 0.80),
        critical_ratio=_float_env("MEMORY_WATCHDOG_CRITICAL_RATIO", 0.90),
        cadence_seconds=_float_env("MEMORY_WATCHDOG_INTERVAL_SECONDS", 15.0),
    )


__all__ = [
    "MemoryWatchdogService",
    "build_memory_watchdog_service_from_env",
    "read_process_rss_bytes",
]
