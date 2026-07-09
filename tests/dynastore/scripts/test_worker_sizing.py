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

"""Unit tests for deriving the gunicorn worker count (GeoID #3121)."""
from __future__ import annotations

import pytest

from dynastore.scripts.worker_sizing import (
    _DEFAULT_L1_PERCENT,
    explicit_worker_count,
    resolve_worker_count,
)
from dynastore.tools.memory_units import DEFAULT_L1_MEMORY_PERCENT


@pytest.fixture(autouse=True)
def _clear_sizing_env(monkeypatch):
    """Sizing reads only env; unset everything so a test sees what it sets."""
    for name in (
        "GUNICORN_WORKERS",
        "RAM",
        "CPU",
        "WORKER_BASE_MB",
        "WORKER_RESERVE_MB",
        "L1_MEMORY_PERCENT",
        "GUNICORN_WORKERS_MAX",
    ):
        monkeypatch.delenv(name, raising=False)
    # Never let the host's real capacity leak into a test's arithmetic.
    monkeypatch.setattr(
        "dynastore.scripts.worker_sizing.detect_total_memory_mb", lambda: None
    )
    monkeypatch.setattr("dynastore.scripts.worker_sizing.detect_cpu_cores", lambda: 8.0)


def _size(monkeypatch, *, total_mb, cores=8.0, **env):
    monkeypatch.setattr(
        "dynastore.scripts.worker_sizing.detect_total_memory_mb", lambda: total_mb
    )
    monkeypatch.setattr(
        "dynastore.scripts.worker_sizing.detect_cpu_cores", lambda: cores
    )
    for k, v in env.items():
        monkeypatch.setenv(k, str(v))
    return resolve_worker_count()


# ---------------------------------------------------------------------------
# Explicit override always wins
# ---------------------------------------------------------------------------


def test_explicit_override_wins_over_derivation(monkeypatch) -> None:
    assert _size(monkeypatch, total_mb=8192, GUNICORN_WORKERS=6) == 6


def test_explicit_override_ignored_when_not_a_positive_int(monkeypatch) -> None:
    monkeypatch.setenv("GUNICORN_WORKERS", "not-a-number")
    assert explicit_worker_count() is None
    monkeypatch.setenv("GUNICORN_WORKERS", "0")
    assert explicit_worker_count() is None


def test_explicit_override_absent_when_unset() -> None:
    assert explicit_worker_count() is None


# ---------------------------------------------------------------------------
# Memory cap: workers <= usable * (1 - l1) / base
# ---------------------------------------------------------------------------


def test_memory_cap_reserves_headroom_for_l1_cache(monkeypatch) -> None:
    # usable = 8192-512 = 7680; 7680*0.9 = 6912; 6912/2048 = 3.375 -> 3
    assert _size(monkeypatch, total_mb=8192, WORKER_BASE_MB=2048) == 3


def test_larger_l1_percent_yields_fewer_workers(monkeypatch) -> None:
    # usable 7680; *0.5 = 3840; /2048 = 1.875 -> 1
    assert _size(monkeypatch, total_mb=8192, WORKER_BASE_MB=2048, L1_MEMORY_PERCENT=50) == 1


def test_lighter_baseline_yields_more_workers(monkeypatch) -> None:
    # usable 7680; *0.9 = 6912; /512 = 13.5 -> capped by cores (8)
    assert _size(monkeypatch, total_mb=8192, WORKER_BASE_MB=512) == 8


def test_reserve_is_subtracted_before_dividing(monkeypatch) -> None:
    # usable 8192-4096 = 4096; *0.9 = 3686; /2048 = 1.8 -> 1
    assert _size(monkeypatch, total_mb=8192, WORKER_BASE_MB=2048, WORKER_RESERVE_MB=4096) == 1


def test_small_container_still_gets_one_worker(monkeypatch) -> None:
    assert _size(monkeypatch, total_mb=1024, WORKER_BASE_MB=2048) == 1


def test_reserve_larger_than_total_still_gets_one_worker(monkeypatch) -> None:
    assert _size(monkeypatch, total_mb=256, WORKER_BASE_MB=2048, WORKER_RESERVE_MB=512) == 1


# ---------------------------------------------------------------------------
# CPU cap and hard ceiling
# ---------------------------------------------------------------------------


def test_cpu_cap_limits_a_memory_rich_container(monkeypatch) -> None:
    # Memory alone would allow many; two cores means two workers.
    assert _size(monkeypatch, total_mb=65536, cores=2.0, WORKER_BASE_MB=512) == 2


def test_fractional_cpu_floors_to_at_least_one_worker(monkeypatch) -> None:
    assert _size(monkeypatch, total_mb=65536, cores=0.5, WORKER_BASE_MB=512) == 1


def test_hard_ceiling_caps_a_large_machine(monkeypatch) -> None:
    assert (
        _size(monkeypatch, total_mb=65536, cores=64.0, WORKER_BASE_MB=512, GUNICORN_WORKERS_MAX=4)
        == 4
    )


# ---------------------------------------------------------------------------
# Degenerate inputs
# ---------------------------------------------------------------------------


def test_falls_back_to_one_worker_when_memory_is_undetectable(monkeypatch) -> None:
    assert _size(monkeypatch, total_mb=None) == 1


def test_l1_percent_of_100_cannot_divide_by_zero(monkeypatch) -> None:
    assert _size(monkeypatch, total_mb=8192, L1_MEMORY_PERCENT=100) == 1


def test_unparseable_knobs_fall_back_to_defaults(monkeypatch) -> None:
    # usable 7680; *0.9 = 6912; /2048 (default base) = 3
    assert _size(monkeypatch, total_mb=8192, WORKER_BASE_MB="junk", L1_MEMORY_PERCENT="junk") == 3


# ---------------------------------------------------------------------------
# The L1 share sizing reserves must be the share the cache actually takes
# ---------------------------------------------------------------------------


def test_l1_share_is_the_one_the_cache_module_uses() -> None:
    """Sizing reserves room for L1; the cache fills it. One constant, or OOM.

    Three sites read ``DEFAULT_L1_MEMORY_PERCENT``: sizing before gunicorn forks,
    the cache module's built-in default before config load, and the config field
    that overrides it. If a future change gives any of them its own literal, a
    worker gets sized for a 10% cache and then runs a larger one (GeoID #3121,
    #3130).
    """
    from dynastore.modules.cache.cache_config import CachePluginConfig
    from dynastore.tools.cache import _DEFAULT_L1_MEMORY_PERCENT

    assert _DEFAULT_L1_PERCENT == DEFAULT_L1_MEMORY_PERCENT
    assert _DEFAULT_L1_MEMORY_PERCENT == DEFAULT_L1_MEMORY_PERCENT
    field = CachePluginConfig.model_fields["l1_memory_percent"]
    assert field.default == DEFAULT_L1_MEMORY_PERCENT
