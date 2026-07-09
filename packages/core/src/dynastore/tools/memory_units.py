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

"""Memory and CPU quantity parsing, and detection of what this host actually has.

Import discipline: this module MUST stay stdlib-only. It is imported by
``scripts/worker_sizing.py`` before gunicorn starts — earlier than any config
store, database or plugin registry exists — so worker sizing still works on a
container whose application package cannot be imported.

Detection order for both memory and CPU is: the deploy-injected env var first
(``RAM`` / ``CPU``, which is how the dynastore deploy sizes a Cloud Run
service), then the cgroup limit (``--memory`` / ``--cpus``), then — for CPU —
the affinity mask (``--cpuset-cpus``), then the host's own capacity. Cloud Run's
sandbox does not expose a readable ``/sys/fs/cgroup``, which is why the env var
leads; an on-prem Docker host injects no env var, so its real limits are read
from the cgroup, and a container with no limit at all falls through to the host
capacity rather than reporting nothing.
"""
from __future__ import annotations

import os
from typing import Optional

_CGROUP_V2_MEMORY_MAX_PATH = "/sys/fs/cgroup/memory.max"
_CGROUP_V1_MEMORY_LIMIT_PATH = "/sys/fs/cgroup/memory/memory.limit_in_bytes"

# CPU quota, the `docker run --cpus` / Kubernetes cpu-limit knob. v2 packs
# "<quota> <period>" into one file; v1 splits them across two.
_CGROUP_V2_CPU_MAX_PATH = "/sys/fs/cgroup/cpu.max"
_CGROUP_V1_CPU_QUOTA_PATH = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"
_CGROUP_V1_CPU_PERIOD_PATH = "/sys/fs/cgroup/cpu/cpu.cfs_period_us"

# Deploy-injected env vars describing the container's slice of the cluster.
_RAM_ENV = "RAM"
_CPU_ENV = "CPU"

# Kubernetes/Cloud-Run memory-quantity suffix factors (binary vs decimal).
_MEM_SUFFIX_FACTORS = {
    "k": 1000,
    "ki": 1024,
    "m": 1000**2,
    "mi": 1024**2,
    "g": 1000**3,
    "gi": 1024**3,
    "t": 1000**4,
    "ti": 1024**4,
}

# Cgroup v1 reports "no limit" as a huge sentinel (commonly
# 9223372036854771712, close to but not exactly int64-max, since the kernel
# rounds it to a page boundary). Anything at or above this threshold is
# treated as unlimited rather than a real memory budget — no real container
# or host has anywhere near this much RAM.
_UNLIMITED_THRESHOLD_BYTES = 1 << 62

# Share of each worker's memory budget the in-process L1 cache may occupy.
#
# Single source of truth on purpose. Two layers consume it and must agree, or a
# worker is sized for one cache and runs another:
#   * scripts/worker_sizing.py, before gunicorn forks, subtracts this share
#     when deciding how many workers fit;
#   * modules/cache CachePluginConfig.l1_memory_percent defaults to it, and
#     tools/cache.py caps L1 at this percentage of the watchdog's per-worker
#     budget at runtime.
# Raising it in only one place under-reserves the app's own baseline and the
# kernel OOM-kills the worker (GeoID #3121).
DEFAULT_L1_MEMORY_PERCENT = 10.0


def parse_memory_to_mb(raw: Optional[str]) -> Optional[int]:
    """Parse a Kubernetes/Cloud-Run memory quantity into whole MB.

    Accepts values like ``"8Gi"``, ``"2G"``, ``"512Mi"`` or a bare byte count.
    Binary suffixes (``Ki``/``Mi``/``Gi``/``Ti``) use 1024; decimal suffixes
    (``K``/``M``/``G``/``T``) use 1000, per the Kubernetes quantity spec.
    Returns ``None`` for empty or unparseable input.
    """
    if not raw:
        return None
    text = raw.strip()
    if not text:
        return None
    lowered = text.lower()
    # Longest suffix first so "gi" matches before "g".
    for suffix in ("ki", "mi", "gi", "ti", "k", "m", "g", "t"):
        if lowered.endswith(suffix):
            number = text[: -len(suffix)].strip()
            factor = _MEM_SUFFIX_FACTORS[suffix]
            break
    else:
        number, factor = text, 1
    try:
        value_bytes = float(number) * factor
    except ValueError:
        return None
    if value_bytes <= 0:
        return None
    return int(value_bytes) // (1024 * 1024)


def parse_cpu_to_cores(raw: Optional[str]) -> Optional[float]:
    """Parse a Kubernetes/Cloud-Run CPU quantity into cores.

    Accepts millicores (``"4000m"`` → 4.0) or whole/fractional cores
    (``"2"`` → 2.0). Returns ``None`` for empty or unparseable input.
    """
    if not raw:
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        if text.lower().endswith("m"):
            return float(text[:-1]) / 1000.0
        return float(text)
    except ValueError:
        return None


def _read_cgroup_text(path: str) -> Optional[str]:
    """Contents of one cgroup file, stripped, or ``None`` if it cannot be read.

    Absent on macOS and on any non-Linux host, hence the broad ``OSError``.
    """
    try:
        with open(path, "r", encoding="ascii") as fh:
            return fh.read().strip()
    except OSError:
        return None


def _read_cgroup_memory_limit_bytes(path: str) -> Optional[int]:
    """Read and parse one cgroup memory-limit file.

    Returns ``None`` if the file is absent, unreadable, reports the cgroup
    v2 "max" sentinel, or reports a value at/above ``_UNLIMITED_THRESHOLD_BYTES``
    (the cgroup v1 "no limit" sentinel).
    """
    raw = _read_cgroup_text(path)
    if raw is None or raw == "max":
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    if value <= 0 or value >= _UNLIMITED_THRESHOLD_BYTES:
        return None
    return value


def detect_cgroup_memory_limit_mb() -> Optional[int]:
    """Best-effort auto-detection of the container's memory budget, in MB.

    Tries cgroup v2 (``memory.max``) first, then cgroup v1
    (``memory.limit_in_bytes``). Returns ``None`` when neither file exists
    or both report "no limit" — e.g. local dev on macOS, or a container run
    without a memory limit.
    """
    limit_bytes = _read_cgroup_memory_limit_bytes(_CGROUP_V2_MEMORY_MAX_PATH)
    if limit_bytes is None:
        limit_bytes = _read_cgroup_memory_limit_bytes(_CGROUP_V1_MEMORY_LIMIT_PATH)
    if limit_bytes is None:
        return None
    return limit_bytes // (1024 * 1024)


def detect_host_memory_mb() -> Optional[int]:
    """Total physical memory of the host, in MB, or ``None`` if unavailable.

    The last resort for an unconstrained on-prem Docker host: no ``RAM`` env
    var and no cgroup limit, so the container may use whatever the host has.
    """
    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
    except (AttributeError, OSError, ValueError):
        return None
    if pages <= 0 or page_size <= 0:
        return None
    return (pages * page_size) // (1024 * 1024)


def detect_container_memory_mb() -> Optional[int]:
    """Memory this container was *given*, in MB: ``RAM`` env, then cgroup.

    Deliberately does not fall back to the host's capacity: callers that must
    not guess (the memory watchdog stays inert rather than police a budget it
    invented) rely on ``None`` here. Use :func:`detect_total_memory_mb` when a
    best-effort number is better than none.
    """
    return parse_memory_to_mb(os.environ.get(_RAM_ENV)) or detect_cgroup_memory_limit_mb()


def detect_total_memory_mb() -> Optional[int]:
    """Memory usable by this process, in MB: ``RAM`` env, cgroup, then the host.

    The host fallback covers an on-prem Docker run with no memory limit set,
    where the container may use whatever the machine has.
    """
    return detect_container_memory_mb() or detect_host_memory_mb()


def _quota_to_cores(quota: float, period: float) -> Optional[float]:
    """Cores implied by a cgroup CPU quota/period pair, or ``None`` if unlimited.

    A negative quota is the cgroup v1 "no limit" sentinel; a non-positive period
    is nonsense the kernel should never write.
    """
    if quota <= 0 or period <= 0:
        return None
    return quota / period


def _read_cgroup_v2_cpu_quota_cores() -> Optional[float]:
    """Cores from cgroup v2 ``cpu.max``, which holds ``"<quota> <period>"``.

    The quota field is the literal ``max`` when no limit is set.
    """
    raw = _read_cgroup_text(_CGROUP_V2_CPU_MAX_PATH)
    if not raw:
        return None
    fields = raw.split()
    if len(fields) != 2 or fields[0] == "max":
        return None
    try:
        return _quota_to_cores(float(fields[0]), float(fields[1]))
    except ValueError:
        return None


def _read_cgroup_v1_cpu_quota_cores() -> Optional[float]:
    """Cores from cgroup v1 ``cpu.cfs_quota_us`` / ``cpu.cfs_period_us``.

    A quota of ``-1`` means no limit.
    """
    quota_raw = _read_cgroup_text(_CGROUP_V1_CPU_QUOTA_PATH)
    period_raw = _read_cgroup_text(_CGROUP_V1_CPU_PERIOD_PATH)
    if quota_raw is None or period_raw is None:
        return None
    try:
        return _quota_to_cores(float(quota_raw), float(period_raw))
    except ValueError:
        return None


def detect_cgroup_cpu_quota_cores() -> Optional[float]:
    """Fractional cores this container may burn, from its cgroup CPU quota.

    This is what ``docker run --cpus=N`` and a Kubernetes CPU *limit* set. It is
    the number that matters on an on-prem Docker host, where nothing injects the
    ``CPU`` env var: ``os.cpu_count()`` reports every core on the machine, so a
    container limited to 2 cores on a 32-core host would otherwise be sized for
    32 workers and OOM long before the CPU throttle ever bit.

    Returns ``None`` when no quota is set (the container may use the whole host)
    or when the cgroup files are absent, as on macOS.
    """
    quota = _read_cgroup_v2_cpu_quota_cores()
    if quota is None:
        quota = _read_cgroup_v1_cpu_quota_cores()
    return quota


def detect_affinity_cpu_cores() -> Optional[int]:
    """Cores this process is pinned to, from its CPU affinity mask.

    ``docker run --cpuset-cpus="0,1"`` restricts which cores a container runs on
    without setting any quota, so :func:`detect_cgroup_cpu_quota_cores` sees
    nothing. Linux-only; absent on macOS.
    """
    try:
        return len(os.sched_getaffinity(0)) or None
    except (AttributeError, OSError):
        return None


def detect_cpu_cores() -> Optional[float]:
    """CPU available to this process, in cores, most specific source first.

    ``CPU`` env (injected by the cluster deploy) beats the container's cgroup
    quota (``--cpus``), which beats its affinity mask (``--cpuset-cpus``), which
    beats the host's core count. Each step down is a weaker claim about what this
    container may actually use, and the host count is the only one that can
    over-report by an order of magnitude.
    """
    from_env = parse_cpu_to_cores(os.environ.get(_CPU_ENV))
    if from_env is not None and from_env > 0:
        return from_env

    from_quota = detect_cgroup_cpu_quota_cores()
    if from_quota is not None and from_quota > 0:
        return from_quota

    from_affinity = detect_affinity_cpu_cores()
    if from_affinity:
        return float(from_affinity)

    count = os.cpu_count()
    return float(count) if count else None


__all__ = [
    "DEFAULT_L1_MEMORY_PERCENT",
    "detect_affinity_cpu_cores",
    "detect_cgroup_cpu_quota_cores",
    "detect_cgroup_memory_limit_mb",
    "detect_container_memory_mb",
    "detect_cpu_cores",
    "detect_host_memory_mb",
    "detect_total_memory_mb",
    "parse_cpu_to_cores",
    "parse_memory_to_mb",
]
