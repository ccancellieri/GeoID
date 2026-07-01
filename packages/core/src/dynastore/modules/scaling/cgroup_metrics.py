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

"""Cloud-neutral cgroup v2 self-report of THIS pod's own CPU/memory
utilization — no cloud SDK, no API token, no network call.

Works identically on Cloud Run gen2, GKE, or any other cgroup-v2 host,
because it reads the same kernel-exposed accounting files every one of them
mounts at ``/sys/fs/cgroup/`` — there is nothing platform-specific to
abstract behind a protocol. Fail-soft throughout: a missing file (cgroup v1,
a sandboxed/rootless environment, a non-Linux host), a parse error, or an
unbounded memory limit all degrade to ``None`` rather than raising — the
caller (``ScalingSignalPublisher``) simply contributes no signal for that
metric this tick.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

_CGROUP_ROOT = Path("/sys/fs/cgroup")
_CPU_STAT = _CGROUP_ROOT / "cpu.stat"
_CPU_MAX = _CGROUP_ROOT / "cpu.max"
_MEMORY_CURRENT = _CGROUP_ROOT / "memory.current"
_MEMORY_MAX = _CGROUP_ROOT / "memory.max"


def _read_cpu_usage_usec() -> Optional[int]:
    """Cumulative CPU time (microseconds) this cgroup has consumed, from
    cgroup v2's ``cpu.stat`` ``usage_usec`` line. ``None`` on cgroup v1 (no
    ``cpu.stat`` at this path), a missing/unreadable file, or a malformed
    line — never raises.
    """
    try:
        for line in _CPU_STAT.read_text().splitlines():
            if line.startswith("usage_usec "):
                return int(line.split()[1])
    except (OSError, ValueError, IndexError):
        return None
    return None


def _read_allotted_cores() -> float:
    """This cgroup's CPU quota in whole-core units, from ``cpu.max``
    (``"<quota> <period>"`` in microseconds, or ``"max <period>"`` when the
    cgroup is unthrottled). An unthrottled cgroup is a legitimate, common
    configuration — not a failure — so it falls back to the process's CPU
    affinity mask (or ``os.cpu_count()``) rather than returning ``None``.
    """
    try:
        raw = _CPU_MAX.read_text().split()
        quota, period = raw[0], raw[1]
        if quota != "max":
            return max(int(quota) / int(period), 0.001)
    except (OSError, ValueError, IndexError):
        pass
    try:
        return float(len(os.sched_getaffinity(0)))
    except (AttributeError, OSError):
        return float(os.cpu_count() or 1)


def _read_memory_current() -> Optional[int]:
    try:
        return int(_MEMORY_CURRENT.read_text().strip())
    except (OSError, ValueError):
        return None


def _read_memory_max() -> Optional[int]:
    """This cgroup's memory limit in bytes, or ``None`` when unbounded
    (``cpu.max``-style ``"max"`` sentinel) or unreadable — an unbounded
    limit makes a utilization RATIO meaningless, so it is treated the same
    as "unavailable" rather than as some arbitrary denominator.
    """
    try:
        raw = _MEMORY_MAX.read_text().strip()
        if raw == "max":
            return None
        return int(raw)
    except (OSError, ValueError):
        return None


class CgroupMetricsReader:
    """Self-report reader for this pod's own cgroup, held by whatever
    publishes signals (``ScalingSignalPublisher``) across its whole
    lifetime so consecutive CPU reads share a baseline.

    CPU utilization needs two samples (a cumulative counter, not an
    instantaneous rate), so :meth:`read_cpu_utilization` returns ``None`` on
    its first call for a fresh instance — exactly one missed tick at
    process start, then a real reading every tick after. Memory utilization
    is a single stateless ratio, valid from the first call.
    """

    def __init__(self) -> None:
        self._prev_usage_usec: Optional[int] = None
        self._prev_ts: Optional[float] = None

    def read_cpu_utilization(self) -> Optional[float]:
        """CPU time consumed since the previous call, as a fraction of the
        wall-clock time elapsed times this cgroup's allotted core count.
        Normalized to ``[0, 1]``. ``None`` when cgroup v2 isn't available,
        this is the first sample, or the usage counter went backwards (the
        cgroup was recreated between reads — treat as "no data" rather than
        a nonsensical negative rate).
        """
        usage = _read_cpu_usage_usec()
        now = time.monotonic()
        if usage is None:
            return None

        prev_usage, prev_ts = self._prev_usage_usec, self._prev_ts
        self._prev_usage_usec, self._prev_ts = usage, now
        if prev_usage is None or prev_ts is None:
            return None  # first sample for this reader — no baseline yet

        elapsed_usec = (now - prev_ts) * 1_000_000.0
        delta_usage_usec = usage - prev_usage
        if elapsed_usec <= 0 or delta_usage_usec < 0:
            return None

        cores = _read_allotted_cores()
        utilization = delta_usage_usec / (elapsed_usec * cores)
        return max(0.0, min(1.0, utilization))

    def read_memory_utilization(self) -> Optional[float]:
        """``memory.current / memory.max``, normalized to ``[0, 1]``.
        ``None`` when either file is unreadable or the cgroup has no memory
        limit set.
        """
        current = _read_memory_current()
        limit = _read_memory_max()
        if current is None or limit is None or limit <= 0:
            return None
        return max(0.0, min(1.0, current / limit))


# ---------------------------------------------------------------------------
# One-shot diagnostic probe — "does cgroup self-report even work here?"
# ---------------------------------------------------------------------------
# Separate from CgroupMetricsReader: a probe is a single point-in-time
# snapshot for a human to read in the logs (raw file contents / failure
# reasons, not just the derived numbers), taken once at startup rather than
# on every publish tick.


def _read_raw(path: Path) -> Tuple[Optional[str], Optional[str]]:
    """Returns ``(contents, None)`` on success or ``(None, reason)`` on any
    read failure — never raises."""
    try:
        return path.read_text().strip(), None
    except OSError as exc:
        return None, str(exc)


def probe_cgroup() -> Dict[str, Any]:
    """One-shot diagnostic snapshot: raw contents (or the exact failure
    reason) for every file this module reads, plus what a fresh reader could
    derive from them right now. Read-only — never touches a
    ``CgroupMetricsReader`` instance's CPU baseline, so calling this does not
    disturb the publisher's own delta sampling. Never raises.

    ``cpu_usage_usec`` is the raw counter, not a utilization ratio — a
    single point-in-time probe has no second sample to diff against (see
    ``CgroupMetricsReader.read_cpu_utilization``'s docstring). ``usable`` is
    ``True`` when at least one of the two metrics could be read at all.
    """
    cpu_stat_raw, cpu_stat_error = _read_raw(_CPU_STAT)
    cpu_max_raw, cpu_max_error = _read_raw(_CPU_MAX)
    memory_current_raw, memory_current_error = _read_raw(_MEMORY_CURRENT)
    memory_max_raw, memory_max_error = _read_raw(_MEMORY_MAX)

    cpu_usage_usec = _read_cpu_usage_usec()
    allotted_cores = _read_allotted_cores()
    memory_utilization = CgroupMetricsReader().read_memory_utilization()

    return {
        "cgroup_version": "v2" if cpu_stat_raw is not None else "unknown (no cpu.stat found)",
        "cpu_stat_path": str(_CPU_STAT),
        "cpu_stat_raw": cpu_stat_raw,
        "cpu_stat_error": cpu_stat_error,
        "cpu_max_path": str(_CPU_MAX),
        "cpu_max_raw": cpu_max_raw,
        "cpu_max_error": cpu_max_error,
        "memory_current_path": str(_MEMORY_CURRENT),
        "memory_current_raw": memory_current_raw,
        "memory_current_error": memory_current_error,
        "memory_max_path": str(_MEMORY_MAX),
        "memory_max_raw": memory_max_raw,
        "memory_max_error": memory_max_error,
        "cpu_usage_usec": cpu_usage_usec,
        "allotted_cores": allotted_cores,
        "memory_utilization": memory_utilization,
        "usable": cpu_usage_usec is not None or memory_utilization is not None,
    }


def format_cgroup_probe(diag: Dict[str, Any]) -> str:
    """Render :func:`probe_cgroup`'s snapshot as one greppable log line
    (stable ``cgroup_probe`` prefix) — the primary validation signal for
    "does this work on the real Cloud Run gen2 service."
    """
    return (
        "cgroup_probe usable=%s cgroup_version=%s "
        "cpu_stat=%r cpu_stat_error=%s cpu_max=%r cpu_max_error=%s "
        "memory_current=%r memory_current_error=%s memory_max=%r memory_max_error=%s "
        "cpu_usage_usec=%s allotted_cores=%s memory_utilization=%s"
    ) % (
        diag["usable"],
        diag["cgroup_version"],
        diag["cpu_stat_raw"],
        diag["cpu_stat_error"],
        diag["cpu_max_raw"],
        diag["cpu_max_error"],
        diag["memory_current_raw"],
        diag["memory_current_error"],
        diag["memory_max_raw"],
        diag["memory_max_error"],
        diag["cpu_usage_usec"],
        diag["allotted_cores"],
        diag["memory_utilization"],
    )
