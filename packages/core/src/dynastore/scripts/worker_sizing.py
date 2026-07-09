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

"""Derive the gunicorn worker count from the memory and CPU actually available.

``start.sh`` calls this before exec'ing gunicorn. Setting ``GUNICORN_WORKERS``
explicitly always wins; leaving it unset lets a service track whatever RAM/CPU
its environment gives it, so overriding ``RAM`` per environment no longer
silently shrinks each worker's share.

Import discipline: stdlib plus :mod:`dynastore.tools.memory_units` only. This
runs before gunicorn, so it must not import the application package.

Why the worker count is a memory question
-----------------------------------------
Each gunicorn worker is a separate process with its own heap, so the memory a
single worker may use is ``total_memory / workers``. Two things have to fit in
that share: the process baseline (interpreter, imported extension modules,
pools, per-worker caches) and the in-process L1 cache, whose budget is itself a
percentage of the per-worker share (``CachePluginConfig.l1_memory_percent``).

That makes the constraint circular — the L1 budget depends on the worker count,
which depends on how much room L1 needs. Writing ``B`` for the per-worker share
and ``p`` for the L1 fraction::

    B >= base_mb + p * B
    B * (1 - p) >= base_mb
    (usable_mb / workers) * (1 - p) >= base_mb
    workers <= usable_mb * (1 - p) / base_mb

so the circularity cancels and the memory cap is a closed form. ``p`` is fixed
when the container starts, while ``l1_memory_percent`` is live-patchable: raising
it at runtime grows the L1 budget inside a worker count chosen for the old value,
eating the baseline's headroom rather than adding workers. Restart the service
after raising it substantially.

The CPU cap is
one worker per core: these are async uvicorn workers, so extra processes past
the core count buy concurrency the event loop already provides, while each one
costs a full baseline.

Sizing is against the worker's *peak* baseline, not its idle one. A worker that
idles at 800MB and peaks at 2GB while serving needs 2GB, or the kernel OOM-kills
it — the peak is the number that has to fit.
"""
from __future__ import annotations

import os
from typing import Optional

from dynastore.tools.memory_units import (
    DEFAULT_L1_MEMORY_PERCENT,
    detect_cpu_cores,
    detect_total_memory_mb,
)

# Env overrides. All optional: the defaults below size a service correctly from
# RAM/CPU alone.
_WORKERS_ENV = "GUNICORN_WORKERS"
_BASE_MB_ENV = "WORKER_BASE_MB"
_RESERVE_MB_ENV = "WORKER_RESERVE_MB"
_L1_PERCENT_ENV = "L1_MEMORY_PERCENT"
_MAX_WORKERS_ENV = "GUNICORN_WORKERS_MAX"

# Peak private (RssAnon) memory one worker occupies, in MB. Measured on dev
# catalog 2026-07-09: a worker reporting "RSS 2106MiB [anon 1976MiB, file
# 116MiB]" against a 1907MiB share, then OOM-killed. Services with a lighter
# footprint should lower this per service rather than raising their RAM.
_DEFAULT_BASE_MB = 2048

# Held back for the gunicorn master process and the OS before dividing the
# rest among workers.
_DEFAULT_RESERVE_MB = 512

# Share reserved for the in-process L1 cache. Shared with the cache module so
# the two cannot drift; see DEFAULT_L1_MEMORY_PERCENT. Overriding the env var
# without also PATCHing CachePluginConfig.l1_memory_percent breaks the
# assumption this sizing rests on.
_DEFAULT_L1_PERCENT = DEFAULT_L1_MEMORY_PERCENT

# A service with a lot of RAM still gains little from very many async workers,
# and every worker multiplies DB pool connections (MAX_SCALE x GUNICORN_WORKERS
# x DB_POOL_MAX_SIZE must stay under the server's connection limit).
_DEFAULT_MAX_WORKERS = 8


def _env_float(name: str, default: float) -> float:
    try:
        value = float(os.environ.get(name, "") or default)
    except ValueError:
        return default
    return value if value > 0 else default


def _env_int(name: str, default: int) -> int:
    try:
        value = int(float(os.environ.get(name, "") or default))
    except ValueError:
        return default
    return value if value > 0 else default


def explicit_worker_count() -> Optional[int]:
    """The operator's ``GUNICORN_WORKERS`` override, or ``None`` when unset."""
    raw = os.environ.get(_WORKERS_ENV, "").strip()
    if not raw:
        return None
    try:
        workers = int(raw)
    except ValueError:
        return None
    return workers if workers >= 1 else None


def resolve_worker_count() -> int:
    """Workers this container should run: explicit override, else derived.

    Falls back to a single worker when neither the deploy env, the cgroup nor
    the host reveals how much memory is available — one worker is the only
    count guaranteed to fit in an unknown budget.
    """
    override = explicit_worker_count()
    if override is not None:
        return override

    total_mb = detect_total_memory_mb()
    if total_mb is None:
        return 1

    base_mb = _env_int(_BASE_MB_ENV, _DEFAULT_BASE_MB)
    reserve_mb = _env_int(_RESERVE_MB_ENV, _DEFAULT_RESERVE_MB)
    l1_fraction = _env_float(_L1_PERCENT_ENV, _DEFAULT_L1_PERCENT) / 100.0
    max_workers = _env_int(_MAX_WORKERS_ENV, _DEFAULT_MAX_WORKERS)

    # A pathological l1_memory_percent would divide by zero or go negative.
    headroom = 1.0 - l1_fraction
    if headroom <= 0:
        return 1

    usable_mb = total_mb - reserve_mb
    if usable_mb <= 0:
        return 1

    memory_cap = int((usable_mb * headroom) // base_mb)

    cores = detect_cpu_cores()
    cpu_cap = max(1, int(cores)) if cores else max_workers

    return max(1, min(memory_cap, cpu_cap, max_workers))


def main() -> None:
    """Print the worker count. ``start.sh`` reads this from stdout."""
    print(resolve_worker_count())


if __name__ == "__main__":
    main()
