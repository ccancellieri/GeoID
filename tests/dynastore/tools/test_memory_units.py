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

"""Detecting the CPU a container actually has (GeoID #3121).

On-prem runs under plain Docker, where nothing injects the ``CPU`` env var the
cluster deploy sets. The container's real limit lives in its cgroup, and
``os.cpu_count()`` cannot see it — it reports every core on the host.
"""
from __future__ import annotations

import pytest

from dynastore.tools.memory_units import (
    detect_affinity_cpu_cores,
    detect_cgroup_cpu_quota_cores,
    detect_cpu_cores,
)

_V2 = "dynastore.tools.memory_units._CGROUP_V2_CPU_MAX_PATH"
_V1_QUOTA = "dynastore.tools.memory_units._CGROUP_V1_CPU_QUOTA_PATH"
_V1_PERIOD = "dynastore.tools.memory_units._CGROUP_V1_CPU_PERIOD_PATH"


@pytest.fixture
def no_cgroup(tmp_path, monkeypatch):
    """Point every cgroup CPU path at a file that does not exist."""
    missing = str(tmp_path / "does-not-exist")
    for target in (_V2, _V1_QUOTA, _V1_PERIOD):
        monkeypatch.setattr(target, missing)
    return monkeypatch


def _write_v2(tmp_path, monkeypatch, contents: str) -> None:
    path = tmp_path / "cpu.max"
    path.write_text(contents)
    monkeypatch.setattr(_V2, str(path))


def _write_v1(tmp_path, monkeypatch, quota: str, period: str) -> None:
    quota_file = tmp_path / "cpu.cfs_quota_us"
    period_file = tmp_path / "cpu.cfs_period_us"
    quota_file.write_text(quota)
    period_file.write_text(period)
    monkeypatch.setattr(_V1_QUOTA, str(quota_file))
    monkeypatch.setattr(_V1_PERIOD, str(period_file))


# ---------------------------------------------------------------------------
# cgroup v2: cpu.max holds "<quota> <period>"
# ---------------------------------------------------------------------------


def test_v2_quota_is_a_ratio_of_quota_to_period(tmp_path, no_cgroup) -> None:
    # `docker run --cpus=2` on a cgroup v2 host.
    _write_v2(tmp_path, no_cgroup, "200000 100000\n")
    assert detect_cgroup_cpu_quota_cores() == 2.0


def test_v2_quota_may_be_fractional(tmp_path, no_cgroup) -> None:
    # `--cpus=1.5`; the sizing caller floors this, it must not round up here.
    _write_v2(tmp_path, no_cgroup, "150000 100000\n")
    assert detect_cgroup_cpu_quota_cores() == 1.5


def test_v2_max_sentinel_means_no_quota(tmp_path, no_cgroup) -> None:
    _write_v2(tmp_path, no_cgroup, "max 100000\n")
    assert detect_cgroup_cpu_quota_cores() is None


def test_v2_malformed_contents_are_ignored(tmp_path, no_cgroup) -> None:
    _write_v2(tmp_path, no_cgroup, "not a quota\n")
    assert detect_cgroup_cpu_quota_cores() is None


def test_v2_zero_period_does_not_divide_by_zero(tmp_path, no_cgroup) -> None:
    _write_v2(tmp_path, no_cgroup, "100000 0\n")
    assert detect_cgroup_cpu_quota_cores() is None


# ---------------------------------------------------------------------------
# cgroup v1: quota and period in separate files
# ---------------------------------------------------------------------------


def test_v1_is_used_when_v2_is_absent(tmp_path, no_cgroup) -> None:
    _write_v1(tmp_path, no_cgroup, "400000\n", "100000\n")
    assert detect_cgroup_cpu_quota_cores() == 4.0


def test_v1_negative_quota_means_no_limit(tmp_path, no_cgroup) -> None:
    _write_v1(tmp_path, no_cgroup, "-1\n", "100000\n")
    assert detect_cgroup_cpu_quota_cores() is None


def test_no_cgroup_files_at_all_yields_none(no_cgroup) -> None:
    # The macOS case: /sys/fs/cgroup does not exist.
    assert detect_cgroup_cpu_quota_cores() is None


# ---------------------------------------------------------------------------
# detect_cpu_cores: env > quota > affinity > host
# ---------------------------------------------------------------------------


def test_env_cpu_wins_over_the_cgroup_quota(tmp_path, no_cgroup) -> None:
    _write_v2(tmp_path, no_cgroup, "800000 100000\n")
    no_cgroup.setenv("CPU", "2000m")
    assert detect_cpu_cores() == 2.0


def test_cgroup_quota_wins_over_the_host_core_count(tmp_path, no_cgroup) -> None:
    """The on-prem Docker case this exists for.

    Without reading the quota, a 2-core container on a 32-core host reports 32
    and gets sized for far more workers than its memory can hold.
    """
    no_cgroup.delenv("CPU", raising=False)
    _write_v2(tmp_path, no_cgroup, "200000 100000\n")
    no_cgroup.setattr("os.cpu_count", lambda: 32)
    assert detect_cpu_cores() == 2.0


def test_affinity_is_used_when_no_quota_is_set(no_cgroup) -> None:
    # `docker run --cpuset-cpus="0,1"` pins cores without setting a quota.
    no_cgroup.delenv("CPU", raising=False)
    no_cgroup.setattr(
        "dynastore.tools.memory_units.detect_affinity_cpu_cores", lambda: 2
    )
    no_cgroup.setattr("os.cpu_count", lambda: 32)
    assert detect_cpu_cores() == 2.0


def test_falls_back_to_the_host_core_count(no_cgroup) -> None:
    no_cgroup.delenv("CPU", raising=False)
    no_cgroup.setattr(
        "dynastore.tools.memory_units.detect_affinity_cpu_cores", lambda: None
    )
    no_cgroup.setattr("os.cpu_count", lambda: 8)
    assert detect_cpu_cores() == 8.0


def test_returns_none_when_nothing_reveals_the_cpu(no_cgroup) -> None:
    no_cgroup.delenv("CPU", raising=False)
    no_cgroup.setattr(
        "dynastore.tools.memory_units.detect_affinity_cpu_cores", lambda: None
    )
    no_cgroup.setattr("os.cpu_count", lambda: None)
    assert detect_cpu_cores() is None


def test_affinity_reports_a_positive_count_or_none() -> None:
    """Linux returns the pinned-core count; macOS has no sched_getaffinity."""
    cores = detect_affinity_cpu_cores()
    assert cores is None or cores > 0
