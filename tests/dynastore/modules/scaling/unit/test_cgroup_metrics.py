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

"""Unit tests for the cgroup v2 self-report reader.

Builds a tmp fake ``/sys/fs/cgroup``-shaped directory and monkeypatches the
module's path constants at it — the same monkeypatch-module-globals idiom
``test_db_contention_monitor.py`` uses. Never touches the real
``/sys/fs/cgroup`` on the machine running these tests.
"""

from __future__ import annotations

from types import SimpleNamespace

import dynastore.modules.scaling.cgroup_metrics as mod
from dynastore.modules.scaling.cgroup_metrics import (
    CgroupMetricsReader,
    format_cgroup_probe,
    probe_cgroup,
)


def _write(path, content: str) -> None:
    path.write_text(content)


class TestCpuUtilization:
    def test_first_sample_returns_none(self, tmp_path, monkeypatch):
        cpu_stat = tmp_path / "cpu.stat"
        _write(cpu_stat, "usage_usec 1000000\nnr_periods 0\n")
        monkeypatch.setattr(mod, "_CPU_STAT", cpu_stat)

        reader = CgroupMetricsReader()
        assert reader.read_cpu_utilization() is None

    def test_second_sample_computes_delta_normalized_by_cores(self, tmp_path, monkeypatch):
        cpu_stat = tmp_path / "cpu.stat"
        cpu_max = tmp_path / "cpu.max"
        _write(cpu_stat, "usage_usec 1000000\n")
        _write(cpu_max, "200000 100000\n")  # quota=200000us, period=100000us -> 2 cores
        monkeypatch.setattr(mod, "_CPU_STAT", cpu_stat)
        monkeypatch.setattr(mod, "_CPU_MAX", cpu_max)
        monkeypatch.setattr(mod.time, "monotonic", lambda: 0.0)

        reader = CgroupMetricsReader()
        assert reader.read_cpu_utilization() is None  # baseline sample

        # 1 real second elapsed, 1 full core-second (1_000_000us) consumed
        # across a 2-core allotment -> 50% utilization.
        _write(cpu_stat, "usage_usec 2000000\n")
        monkeypatch.setattr(mod.time, "monotonic", lambda: 1.0)
        assert reader.read_cpu_utilization() == 0.5

    def test_missing_cpu_stat_is_none_cgroup_v1_or_sandboxed(self, tmp_path, monkeypatch):
        monkeypatch.setattr(mod, "_CPU_STAT", tmp_path / "does-not-exist" / "cpu.stat")

        reader = CgroupMetricsReader()
        assert reader.read_cpu_utilization() is None

    def test_malformed_cpu_stat_is_none(self, tmp_path, monkeypatch):
        cpu_stat = tmp_path / "cpu.stat"
        _write(cpu_stat, "garbage, no usage_usec line here\n")
        monkeypatch.setattr(mod, "_CPU_STAT", cpu_stat)

        assert CgroupMetricsReader().read_cpu_utilization() is None

    def test_counter_reset_between_reads_returns_none(self, tmp_path, monkeypatch):
        cpu_stat = tmp_path / "cpu.stat"
        _write(cpu_stat, "usage_usec 5000000\n")
        monkeypatch.setattr(mod, "_CPU_STAT", cpu_stat)
        monkeypatch.setattr(mod, "_CPU_MAX", tmp_path / "cpu.max")  # missing -> fallback cores
        monkeypatch.setattr(mod.time, "monotonic", lambda: 0.0)

        reader = CgroupMetricsReader()
        reader.read_cpu_utilization()  # baseline

        # Cgroup recreated -> counter goes backwards.
        _write(cpu_stat, "usage_usec 100\n")
        monkeypatch.setattr(mod.time, "monotonic", lambda: 1.0)
        assert reader.read_cpu_utilization() is None

    def test_utilization_clamped_to_one_when_over_allotment(self, tmp_path, monkeypatch):
        cpu_stat = tmp_path / "cpu.stat"
        cpu_max = tmp_path / "cpu.max"
        _write(cpu_stat, "usage_usec 0\n")
        _write(cpu_max, "100000 100000\n")  # 1 core
        monkeypatch.setattr(mod, "_CPU_STAT", cpu_stat)
        monkeypatch.setattr(mod, "_CPU_MAX", cpu_max)
        monkeypatch.setattr(mod.time, "monotonic", lambda: 0.0)

        reader = CgroupMetricsReader()
        reader.read_cpu_utilization()

        # 2 full core-seconds of usage in 1 wall second on a 1-core budget
        # would be 200% — must clamp to 1.0, never raise/overshoot.
        _write(cpu_stat, "usage_usec 2000000\n")
        monkeypatch.setattr(mod.time, "monotonic", lambda: 1.0)
        assert reader.read_cpu_utilization() == 1.0

    def test_unthrottled_cpu_max_falls_back_to_cpu_count(self, tmp_path, monkeypatch):
        cpu_stat = tmp_path / "cpu.stat"
        cpu_max = tmp_path / "cpu.max"
        _write(cpu_stat, "usage_usec 0\n")
        _write(cpu_max, "max 100000\n")
        monkeypatch.setattr(mod, "_CPU_STAT", cpu_stat)
        monkeypatch.setattr(mod, "_CPU_MAX", cpu_max)
        # A namespace with no ``sched_getaffinity`` forces the AttributeError
        # fallback path regardless of the host OS (Linux has the real
        # function; macOS never did) without mutating the real ``os`` module.
        monkeypatch.setattr(mod, "os", SimpleNamespace(cpu_count=lambda: 4))
        monkeypatch.setattr(mod.time, "monotonic", lambda: 0.0)

        reader = CgroupMetricsReader()
        reader.read_cpu_utilization()

        _write(cpu_stat, "usage_usec 1000000\n")  # 1 core-second in 1s / 4 cores = 0.25
        monkeypatch.setattr(mod.time, "monotonic", lambda: 1.0)
        assert reader.read_cpu_utilization() == 0.25


class TestMemoryUtilization:
    def test_reads_ratio(self, tmp_path, monkeypatch):
        current = tmp_path / "memory.current"
        limit = tmp_path / "memory.max"
        _write(current, "50000000\n")
        _write(limit, "100000000\n")
        monkeypatch.setattr(mod, "_MEMORY_CURRENT", current)
        monkeypatch.setattr(mod, "_MEMORY_MAX", limit)

        assert CgroupMetricsReader().read_memory_utilization() == 0.5

    def test_unbounded_limit_returns_none(self, tmp_path, monkeypatch):
        current = tmp_path / "memory.current"
        limit = tmp_path / "memory.max"
        _write(current, "50000000\n")
        _write(limit, "max\n")
        monkeypatch.setattr(mod, "_MEMORY_CURRENT", current)
        monkeypatch.setattr(mod, "_MEMORY_MAX", limit)

        assert CgroupMetricsReader().read_memory_utilization() is None

    def test_missing_files_return_none(self, tmp_path, monkeypatch):
        monkeypatch.setattr(mod, "_MEMORY_CURRENT", tmp_path / "nope" / "memory.current")
        monkeypatch.setattr(mod, "_MEMORY_MAX", tmp_path / "nope" / "memory.max")

        assert CgroupMetricsReader().read_memory_utilization() is None

    def test_ratio_clamped_to_one(self, tmp_path, monkeypatch):
        current = tmp_path / "memory.current"
        limit = tmp_path / "memory.max"
        _write(current, "150000000\n")  # transient overshoot past the limit
        _write(limit, "100000000\n")
        monkeypatch.setattr(mod, "_MEMORY_CURRENT", current)
        monkeypatch.setattr(mod, "_MEMORY_MAX", limit)

        assert CgroupMetricsReader().read_memory_utilization() == 1.0


class TestProbeCgroup:
    def test_usable_true_with_full_fake_cgroup(self, tmp_path, monkeypatch):
        cpu_stat = tmp_path / "cpu.stat"
        cpu_max = tmp_path / "cpu.max"
        mem_current = tmp_path / "memory.current"
        mem_max = tmp_path / "memory.max"
        _write(cpu_stat, "usage_usec 42\n")
        _write(cpu_max, "max 100000\n")
        _write(mem_current, "1000\n")
        _write(mem_max, "2000\n")
        monkeypatch.setattr(mod, "_CPU_STAT", cpu_stat)
        monkeypatch.setattr(mod, "_CPU_MAX", cpu_max)
        monkeypatch.setattr(mod, "_MEMORY_CURRENT", mem_current)
        monkeypatch.setattr(mod, "_MEMORY_MAX", mem_max)

        diag = probe_cgroup()

        assert diag["usable"] is True
        assert diag["cgroup_version"] == "v2"
        assert diag["cpu_usage_usec"] == 42
        assert diag["memory_utilization"] == 0.5
        assert diag["cpu_stat_error"] is None

        line = format_cgroup_probe(diag)
        assert line.startswith("cgroup_probe ")
        assert "usable=True" in line

    def test_usable_false_when_nothing_readable(self, tmp_path, monkeypatch):
        missing = tmp_path / "does-not-exist"
        for name in ("_CPU_STAT", "_CPU_MAX", "_MEMORY_CURRENT", "_MEMORY_MAX"):
            monkeypatch.setattr(mod, name, missing / name)

        diag = probe_cgroup()

        assert diag["usable"] is False
        assert diag["cgroup_version"].startswith("unknown")
        assert diag["cpu_stat_error"] is not None

        line = format_cgroup_probe(diag)
        assert "usable=False" in line
