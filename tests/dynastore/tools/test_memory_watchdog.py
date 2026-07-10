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

"""Unit tests for the proactive memory-pressure watchdog (GeoID #2946)."""
from __future__ import annotations

import asyncio
import logging
import os
import re
import signal
import time

import pytest

from dynastore.tools.background_service import BackgroundSupervisor, ServiceContext
from dynastore.tools.memory_watchdog import (
    MemoryWatchdogConfig,
    MemoryWatchdogService,
    _rss_breakdown_suffix,
    build_memory_watchdog_service,
    detect_cgroup_memory_limit_mb,
    parse_memory_to_mb,
    read_process_rss_breakdown_bytes,
    read_process_rss_bytes,
    resolve_watchdog_budget_mb,
)


@pytest.fixture(autouse=True)
def _clear_budget_env(monkeypatch):
    """Isolate the env-derived budget: unset RAM / GUNICORN_WORKERS so a test
    only sees what it explicitly sets. Individual tests re-set them as needed."""
    monkeypatch.delenv("RAM", raising=False)
    monkeypatch.delenv("GUNICORN_WORKERS", raising=False)


def _make_ctx(name: str = "test-host") -> ServiceContext:
    return ServiceContext(
        engine=None,
        shutdown=asyncio.Event(),
        is_ephemeral=False,
        name=name,
    )


@pytest.fixture(autouse=True)
def _reset_draining_flag():
    """Safety net: the draining flag is process-global module state (by
    design — see tools/serving_state.py), so make sure no test leaks it into
    the next one regardless of test ordering."""
    from dynastore.tools.serving_state import clear_draining
    clear_draining()
    yield
    clear_draining()


# ---------------------------------------------------------------------------
# read_process_rss_bytes
# ---------------------------------------------------------------------------


def test_read_process_rss_bytes_parses_proc_status(tmp_path, monkeypatch) -> None:
    status_file = tmp_path / "status"
    status_file.write_text(
        "Name:\tpython\nVmPeak:\t   20000 kB\nVmRSS:\t   12345 kB\nThreads:\t2\n"
    )
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._PROC_STATUS_PATH", str(status_file)
    )
    assert read_process_rss_bytes() == 12345 * 1024


def test_read_process_rss_bytes_returns_none_when_file_missing(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._PROC_STATUS_PATH",
        str(tmp_path / "does-not-exist"),
    )
    assert read_process_rss_bytes() is None


def test_read_process_rss_bytes_returns_none_when_line_missing(tmp_path, monkeypatch) -> None:
    status_file = tmp_path / "status"
    status_file.write_text("Name:\tpython\nThreads:\t2\n")
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._PROC_STATUS_PATH", str(status_file)
    )
    assert read_process_rss_bytes() is None


# ---------------------------------------------------------------------------
# read_process_rss_breakdown_bytes
# ---------------------------------------------------------------------------


def _write_status(tmp_path, monkeypatch, text: str) -> None:
    status_file = tmp_path / "status"
    status_file.write_text(text)
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._PROC_STATUS_PATH", str(status_file)
    )


def test_read_process_rss_breakdown_parses_anon_and_file(tmp_path, monkeypatch) -> None:
    _write_status(
        tmp_path,
        monkeypatch,
        "VmRSS:\t 2000 kB\nRssAnon:\t  500 kB\nRssFile:\t 1500 kB\nThreads:\t2\n",
    )
    assert read_process_rss_breakdown_bytes() == (500 * 1024, 1500 * 1024)


def test_read_process_rss_breakdown_is_none_when_fields_absent(tmp_path, monkeypatch) -> None:
    # Linux < 4.5 exposes VmRSS but neither RssAnon nor RssFile.
    _write_status(tmp_path, monkeypatch, "VmRSS:\t 2000 kB\nThreads:\t2\n")
    assert read_process_rss_breakdown_bytes() is None


def test_read_process_rss_breakdown_is_none_when_file_missing(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._PROC_STATUS_PATH",
        str(tmp_path / "does-not-exist"),
    )
    assert read_process_rss_breakdown_bytes() is None


def test_rss_breakdown_suffix_renders_both_components(tmp_path, monkeypatch) -> None:
    _write_status(
        tmp_path,
        monkeypatch,
        "RssAnon:\t 1048576 kB\nRssFile:\t 2097152 kB\n",
    )
    assert _rss_breakdown_suffix() == " [anon 1024MiB, file 2048MiB]"


def test_rss_breakdown_suffix_is_empty_when_unavailable(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._PROC_STATUS_PATH",
        str(tmp_path / "does-not-exist"),
    )
    assert _rss_breakdown_suffix() == ""


def test_read_process_rss_bytes_returns_none_on_malformed_line(tmp_path, monkeypatch) -> None:
    status_file = tmp_path / "status"
    status_file.write_text("VmRSS:\tnot-a-number kB\n")
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._PROC_STATUS_PATH", str(status_file)
    )
    assert read_process_rss_bytes() is None


# ---------------------------------------------------------------------------
# MemoryWatchdogService construction validation
# ---------------------------------------------------------------------------


def test_service_rejects_non_positive_limit() -> None:
    with pytest.raises(ValueError):
        MemoryWatchdogService(limit_bytes=0)


@pytest.mark.parametrize("warn_ratio,critical_ratio", [(0.9, 0.8), (0.0, 0.5), (0.5, 1.5)])
def test_service_rejects_invalid_ratios(warn_ratio: float, critical_ratio: float) -> None:
    with pytest.raises(ValueError):
        MemoryWatchdogService(
            limit_bytes=1024,
            warn_ratio=warn_ratio,
            critical_ratio=critical_ratio,
        )


# ---------------------------------------------------------------------------
# MemoryWatchdogService.tick — the actual monitored-error signal
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tick_logs_error_at_or_above_critical_ratio(caplog) -> None:
    limit = 1000
    svc = MemoryWatchdogService(
        limit_bytes=limit,
        warn_ratio=0.8,
        critical_ratio=0.9,
        get_rss_bytes=lambda: 950,  # 95% of budget
    )
    with caplog.at_level(logging.ERROR, logger="dynastore.tools.memory_watchdog"):
        await svc.tick(_make_ctx())
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1
    assert "imminent OOM kill" in errors[0].getMessage()


@pytest.mark.asyncio
async def test_tick_logs_warning_between_warn_and_critical(caplog) -> None:
    limit = 1000
    svc = MemoryWatchdogService(
        limit_bytes=limit,
        warn_ratio=0.8,
        critical_ratio=0.9,
        get_rss_bytes=lambda: 850,  # 85% of budget
    )
    with caplog.at_level(logging.WARNING, logger="dynastore.tools.memory_watchdog"):
        await svc.tick(_make_ctx())
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(warnings) == 1
    assert not errors


@pytest.mark.asyncio
async def test_tick_is_silent_below_warn_ratio(caplog) -> None:
    svc = MemoryWatchdogService(
        limit_bytes=1000,
        warn_ratio=0.8,
        critical_ratio=0.9,
        get_rss_bytes=lambda: 100,  # 10% of budget
    )
    with caplog.at_level(logging.WARNING, logger="dynastore.tools.memory_watchdog"):
        await svc.tick(_make_ctx())
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


@pytest.mark.asyncio
async def test_tick_warning_does_not_repeat_on_sustained_plateau(caplog) -> None:
    """A tick that stays in the warn band across ticks logs only once, not every tick."""
    rss = {"value": 850}
    svc = MemoryWatchdogService(
        limit_bytes=1000,
        warn_ratio=0.8,
        critical_ratio=0.9,
        get_rss_bytes=lambda: rss["value"],
    )
    ctx = _make_ctx()
    with caplog.at_level(logging.WARNING, logger="dynastore.tools.memory_watchdog"):
        await svc.tick(ctx)
        await svc.tick(ctx)
        await svc.tick(ctx)
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1


@pytest.mark.asyncio
async def test_tick_warning_refires_after_dropping_back_below_warn_ratio(caplog) -> None:
    rss = {"value": 850}
    svc = MemoryWatchdogService(
        limit_bytes=1000,
        warn_ratio=0.8,
        critical_ratio=0.9,
        get_rss_bytes=lambda: rss["value"],
    )
    ctx = _make_ctx()
    with caplog.at_level(logging.WARNING, logger="dynastore.tools.memory_watchdog"):
        await svc.tick(ctx)  # warn fires
        rss["value"] = 100  # drop back down
        await svc.tick(ctx)  # quiet
        rss["value"] = 850  # climb again
        await svc.tick(ctx)  # warn fires again
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 2


@pytest.mark.asyncio
async def test_tick_noop_when_rss_unavailable(caplog) -> None:
    svc = MemoryWatchdogService(
        limit_bytes=1000,
        get_rss_bytes=lambda: None,
    )
    with caplog.at_level(logging.WARNING, logger="dynastore.tools.memory_watchdog"):
        await svc.tick(_make_ctx())
    assert not caplog.records


# ---------------------------------------------------------------------------
# MemoryWatchdogConfig
# ---------------------------------------------------------------------------


def test_config_enabled_defaults_to_true() -> None:
    assert MemoryWatchdogConfig().enabled is True


def test_config_limit_mb_defaults_to_none() -> None:
    assert MemoryWatchdogConfig().limit_mb is None


def test_config_rejects_warn_ratio_not_below_critical_ratio() -> None:
    with pytest.raises(ValueError):
        MemoryWatchdogConfig(warn_ratio=0.9, critical_ratio=0.8)


# ---------------------------------------------------------------------------
# detect_cgroup_memory_limit_mb
# ---------------------------------------------------------------------------


def test_detect_cgroup_v2_limit(tmp_path, monkeypatch) -> None:
    v2_file = tmp_path / "memory.max"
    v2_file.write_text("536870912\n")  # 512 MiB
    monkeypatch.setattr(
        "dynastore.tools.memory_units._CGROUP_V2_MEMORY_MAX_PATH", str(v2_file)
    )
    assert detect_cgroup_memory_limit_mb() == 512


def test_detect_cgroup_v1_fallback_when_v2_absent(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "dynastore.tools.memory_units._CGROUP_V2_MEMORY_MAX_PATH",
        str(tmp_path / "does-not-exist"),
    )
    v1_file = tmp_path / "memory.limit_in_bytes"
    v1_file.write_text("268435456\n")  # 256 MiB
    monkeypatch.setattr(
        "dynastore.tools.memory_units._CGROUP_V1_MEMORY_LIMIT_PATH", str(v1_file)
    )
    assert detect_cgroup_memory_limit_mb() == 256


def test_detect_cgroup_v2_max_sentinel_is_unlimited(tmp_path, monkeypatch) -> None:
    v2_file = tmp_path / "memory.max"
    v2_file.write_text("max\n")
    monkeypatch.setattr(
        "dynastore.tools.memory_units._CGROUP_V2_MEMORY_MAX_PATH", str(v2_file)
    )
    monkeypatch.setattr(
        "dynastore.tools.memory_units._CGROUP_V1_MEMORY_LIMIT_PATH",
        str(tmp_path / "does-not-exist"),
    )
    assert detect_cgroup_memory_limit_mb() is None


def test_detect_cgroup_v1_huge_sentinel_is_unlimited(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "dynastore.tools.memory_units._CGROUP_V2_MEMORY_MAX_PATH",
        str(tmp_path / "does-not-exist"),
    )
    v1_file = tmp_path / "memory.limit_in_bytes"
    v1_file.write_text("9223372036854771712\n")
    monkeypatch.setattr(
        "dynastore.tools.memory_units._CGROUP_V1_MEMORY_LIMIT_PATH", str(v1_file)
    )
    assert detect_cgroup_memory_limit_mb() is None


def test_detect_cgroup_none_when_neither_file_present(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "dynastore.tools.memory_units._CGROUP_V2_MEMORY_MAX_PATH",
        str(tmp_path / "does-not-exist-v2"),
    )
    monkeypatch.setattr(
        "dynastore.tools.memory_units._CGROUP_V1_MEMORY_LIMIT_PATH",
        str(tmp_path / "does-not-exist-v1"),
    )
    assert detect_cgroup_memory_limit_mb() is None


# ---------------------------------------------------------------------------
# build_memory_watchdog_service
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_build_service_disabled_via_config() -> None:
    svc = await build_memory_watchdog_service(MemoryWatchdogConfig(enabled=False))
    assert svc is None


@pytest.mark.asyncio
async def test_build_service_never_none_merely_for_unresolved_limit(monkeypatch, tmp_path) -> None:
    """The service is always built when enabled, even if no budget can be
    resolved yet (no RAM env, no cgroup) — it stays inert until a budget
    appears (an operator limit_mb, read live) rather than refusing to start."""
    monkeypatch.setattr(
        "dynastore.tools.memory_units._CGROUP_V2_MEMORY_MAX_PATH",
        str(tmp_path / "does-not-exist-v2"),
    )
    monkeypatch.setattr(
        "dynastore.tools.memory_units._CGROUP_V1_MEMORY_LIMIT_PATH",
        str(tmp_path / "does-not-exist-v1"),
    )
    svc = await build_memory_watchdog_service(MemoryWatchdogConfig())
    assert svc is not None
    assert svc._env_budget_bytes is None


@pytest.mark.asyncio
async def test_build_service_derives_per_worker_budget_from_ram_env(monkeypatch) -> None:
    """The budget is derived at construction from RAM / GUNICORN_WORKERS —
    no tick, no config store, no cgroup needed."""
    monkeypatch.setenv("RAM", "8Gi")
    monkeypatch.setenv("GUNICORN_WORKERS", "4")
    svc = await build_memory_watchdog_service(MemoryWatchdogConfig())
    assert svc is not None
    assert svc._env_budget_bytes == 2048 * 1024 * 1024  # 8Gi / 4


@pytest.mark.asyncio
async def test_build_service_auto_detects_budget_from_cgroup_at_init(monkeypatch, tmp_path) -> None:
    """With no RAM env, the budget falls back to the cgroup limit (÷ workers,
    default 1), resolved at construction."""
    v2_file = tmp_path / "memory.max"
    v2_file.write_text("536870912\n")  # 512 MiB
    monkeypatch.setattr(
        "dynastore.tools.memory_units._CGROUP_V2_MEMORY_MAX_PATH", str(v2_file)
    )

    svc = await build_memory_watchdog_service(MemoryWatchdogConfig())

    assert svc is not None
    assert svc._env_budget_bytes == 512 * 1024 * 1024
    assert svc._warn_ratio == pytest.approx(0.80)
    assert svc._critical_ratio == pytest.approx(0.90)
    assert svc.cadence_seconds == pytest.approx(15.0)


# ---------------------------------------------------------------------------
# parse_memory_to_mb / resolve_watchdog_budget_mb
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("8Gi", 8192),
        ("2Gi", 2048),
        ("512Mi", 512),
        ("8G", 7629),          # 8 * 1000^3 bytes in whole MiB
        ("1073741824", 1024),  # bare bytes = 1 GiB
        ("", None),
        ("garbage", None),
        (None, None),
    ],
)
def test_parse_memory_to_mb(raw, expected) -> None:
    assert parse_memory_to_mb(raw) == expected


def test_resolve_budget_divides_ram_by_workers(monkeypatch) -> None:
    monkeypatch.setenv("RAM", "8Gi")
    monkeypatch.setenv("GUNICORN_WORKERS", "5")
    assert resolve_watchdog_budget_mb() == 8192 // 5  # 1638


def test_resolve_budget_defaults_workers_to_one(monkeypatch) -> None:
    monkeypatch.setenv("RAM", "4Gi")
    # GUNICORN_WORKERS unset by the autouse fixture -> workers = 1
    assert resolve_watchdog_budget_mb() == 4096


def test_resolve_budget_none_when_no_source(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        "dynastore.tools.memory_units._CGROUP_V2_MEMORY_MAX_PATH",
        str(tmp_path / "nope-v2"),
    )
    monkeypatch.setattr(
        "dynastore.tools.memory_units._CGROUP_V1_MEMORY_LIMIT_PATH",
        str(tmp_path / "nope-v1"),
    )
    assert resolve_watchdog_budget_mb() is None


# ---------------------------------------------------------------------------
# Effective budget resolution per tick (env budget vs live config override) —
# no first-tick latch: a limit_mb that appears later IS picked up.
# ---------------------------------------------------------------------------


def _fake_config(cfg: MemoryWatchdogConfig):
    """Build an async ``load_memory_watchdog_config`` replacement returning *cfg*."""
    async def _load() -> MemoryWatchdogConfig:
        return cfg
    return _load


def test_effective_limit_prefers_config_over_env_budget(monkeypatch) -> None:
    """A live config.limit_mb (per-worker override) wins over the env budget."""
    monkeypatch.setenv("RAM", "8Gi")
    monkeypatch.setenv("GUNICORN_WORKERS", "4")  # env budget = 2048 MiB
    svc = MemoryWatchdogService(get_rss_bytes=lambda: 100)
    assert svc._env_budget_bytes == 2048 * 1024 * 1024
    assert svc._effective_limit_bytes(MemoryWatchdogConfig(limit_mb=1024)) == 1024 * 1024 * 1024
    # With no override, the env budget is used.
    assert svc._effective_limit_bytes(MemoryWatchdogConfig()) == 2048 * 1024 * 1024


@pytest.mark.asyncio
async def test_tick_uses_env_budget_when_config_limit_unset(monkeypatch, caplog) -> None:
    monkeypatch.setenv("RAM", "1Gi")  # workers=1 -> budget 1024 MiB
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog.load_memory_watchdog_config",
        _fake_config(MemoryWatchdogConfig(warn_ratio=0.5, critical_ratio=0.6, recycle_ratio=0.7)),
    )
    # RSS = 700 MiB -> 68% of the 1024 MiB env budget: above critical (60%).
    svc = MemoryWatchdogService(
        warn_ratio=0.5, critical_ratio=0.6, get_rss_bytes=lambda: 700 * 1024 * 1024,
    )
    assert svc._env_budget_bytes == 1024 * 1024 * 1024
    with caplog.at_level(logging.ERROR, logger="dynastore.tools.memory_watchdog"):
        await svc.tick(_make_ctx())
    assert any("critical" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_tick_config_limit_picked_up_live_no_latch(monkeypatch, tmp_path) -> None:
    """Regression for the early-boot latch: a service built with NO budget
    (no RAM env, no cgroup) must pick up a config.limit_mb that only becomes
    loadable on a LATER tick — the old design latched 'inert' on the first
    tick and never recovered."""
    monkeypatch.setattr(
        "dynastore.tools.memory_units._CGROUP_V2_MEMORY_MAX_PATH",
        str(tmp_path / "nope-v2"),
    )
    monkeypatch.setattr(
        "dynastore.tools.memory_units._CGROUP_V1_MEMORY_LIMIT_PATH",
        str(tmp_path / "nope-v1"),
    )
    # First tick: config store 'unreachable' -> default config (no limit_mb).
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog.load_memory_watchdog_config",
        _fake_config(MemoryWatchdogConfig()),
    )
    svc = MemoryWatchdogService(get_rss_bytes=lambda: 100)
    assert svc._env_budget_bytes is None
    await svc.tick(_make_ctx())  # inert, no crash

    # A later tick: config now carries an explicit per-worker limit_mb.
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog.load_memory_watchdog_config",
        _fake_config(MemoryWatchdogConfig(limit_mb=512)),
    )
    assert svc._effective_limit_bytes(MemoryWatchdogConfig(limit_mb=512)) == 512 * 1024 * 1024


@pytest.mark.asyncio
async def test_tick_applies_config_cadence_live(monkeypatch) -> None:
    """A cadence_seconds set through the config store takes effect on the next
    tick. PeriodicService.run() reads self.cadence_seconds before every sleep,
    but the service is always BUILT with the code default (the config store is
    unreachable at lifespan start), so shortening the sampling window — e.g. to
    catch an OOM spike that completes between two 15s ticks — must work
    without a redeploy."""
    monkeypatch.setenv("RAM", "1Gi")
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog.load_memory_watchdog_config",
        _fake_config(MemoryWatchdogConfig(cadence_seconds=2.0)),
    )
    svc = MemoryWatchdogService(get_rss_bytes=lambda: 100)
    assert svc.cadence_seconds == pytest.approx(15.0)
    await svc.tick(_make_ctx())
    assert svc.cadence_seconds == pytest.approx(2.0)


@pytest.mark.asyncio
async def test_tick_inert_and_warns_once_when_no_budget(monkeypatch, tmp_path, caplog) -> None:
    monkeypatch.setattr(
        "dynastore.tools.memory_units._CGROUP_V2_MEMORY_MAX_PATH",
        str(tmp_path / "does-not-exist-v2"),
    )
    monkeypatch.setattr(
        "dynastore.tools.memory_units._CGROUP_V1_MEMORY_LIMIT_PATH",
        str(tmp_path / "does-not-exist-v1"),
    )
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog.load_memory_watchdog_config",
        _fake_config(MemoryWatchdogConfig()),
    )

    svc = MemoryWatchdogService(get_rss_bytes=lambda: 100)
    with caplog.at_level(logging.WARNING, logger="dynastore.tools.memory_watchdog"):
        await svc.tick(_make_ctx())
        await svc.tick(_make_ctx())

    assert svc._env_budget_bytes is None
    inert = [r for r in caplog.records if "stays inert" in r.getMessage()]
    assert len(inert) == 1  # throttled to once


# ---------------------------------------------------------------------------
# Fix 2 (Lever B) — readiness-shed + graceful self-recycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_config_recycle_ratio_must_exceed_critical_ratio() -> None:
    with pytest.raises(ValueError):
        MemoryWatchdogConfig(critical_ratio=0.90, recycle_ratio=0.90)
    with pytest.raises(ValueError):
        MemoryWatchdogConfig(critical_ratio=0.90, recycle_ratio=0.85)


def test_config_recycle_defaults_are_safe() -> None:
    cfg = MemoryWatchdogConfig()
    assert cfg.readiness_shed_enabled is False
    assert cfg.self_recycle_enabled is False
    assert cfg.recycle_ratio == pytest.approx(0.92)
    assert cfg.recycle_min_uptime_seconds == pytest.approx(120.0)
    assert cfg.recycle_cooldown_seconds == pytest.approx(300.0)
    assert cfg.recycle_jitter_seconds == pytest.approx(10.0)


@pytest.mark.asyncio
async def test_self_recycle_fires_sigterm_at_ratio(monkeypatch) -> None:
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog.load_memory_watchdog_config",
        _fake_config(
            MemoryWatchdogConfig(
                self_recycle_enabled=True,
                recycle_ratio=0.92,
                recycle_min_uptime_seconds=0.0,
                recycle_cooldown_seconds=300.0,
                recycle_jitter_seconds=0.0,
            )
        ),
    )
    killed: list = []
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog.os.kill",
        lambda pid, sig: killed.append((pid, sig)),
    )

    svc = MemoryWatchdogService(limit_bytes=1000, get_rss_bytes=lambda: 950)  # 95%
    svc._started_at -= 1000  # already well past min uptime

    from dynastore.tools.serving_state import is_draining, clear_draining
    clear_draining()
    await svc.tick(_make_ctx())

    assert killed == [(os.getpid(), signal.SIGTERM)]
    assert is_draining() is True
    clear_draining()


@pytest.mark.asyncio
async def test_self_recycle_does_not_fire_when_disabled(monkeypatch) -> None:
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog.load_memory_watchdog_config",
        _fake_config(MemoryWatchdogConfig(self_recycle_enabled=False)),
    )
    killed: list = []
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog.os.kill",
        lambda pid, sig: killed.append((pid, sig)),
    )

    svc = MemoryWatchdogService(limit_bytes=1000, get_rss_bytes=lambda: 999)
    await svc.tick(_make_ctx())
    assert killed == []


@pytest.mark.asyncio
async def test_self_recycle_does_not_fire_under_min_uptime(monkeypatch) -> None:
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog.load_memory_watchdog_config",
        _fake_config(
            MemoryWatchdogConfig(
                self_recycle_enabled=True,
                recycle_ratio=0.92,
                recycle_min_uptime_seconds=120.0,
                recycle_jitter_seconds=0.0,
            )
        ),
    )
    killed: list = []
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog.os.kill",
        lambda pid, sig: killed.append((pid, sig)),
    )

    svc = MemoryWatchdogService(limit_bytes=1000, get_rss_bytes=lambda: 999)
    # Freshly started — uptime is ~0, well below the 120s minimum.
    await svc.tick(_make_ctx())
    assert killed == []


@pytest.mark.asyncio
async def test_self_recycle_does_not_fire_within_cooldown(monkeypatch) -> None:
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog.load_memory_watchdog_config",
        _fake_config(
            MemoryWatchdogConfig(
                self_recycle_enabled=True,
                recycle_ratio=0.92,
                recycle_min_uptime_seconds=0.0,
                recycle_cooldown_seconds=300.0,
                recycle_jitter_seconds=0.0,
            )
        ),
    )
    killed: list = []
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog.os.kill",
        lambda pid, sig: killed.append((pid, sig)),
    )

    from dynastore.tools.serving_state import clear_draining

    svc = MemoryWatchdogService(limit_bytes=1000, get_rss_bytes=lambda: 999)
    svc._started_at -= 1000
    clear_draining()
    await svc.tick(_make_ctx())
    assert len(killed) == 1

    # Immediately-following tick is inside the cooldown window — must not fire again.
    await svc.tick(_make_ctx())
    assert len(killed) == 1
    clear_draining()


@pytest.mark.asyncio
async def test_self_recycle_aborts_when_jitter_recheck_drops_below_threshold(monkeypatch) -> None:
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog.load_memory_watchdog_config",
        _fake_config(
            MemoryWatchdogConfig(
                self_recycle_enabled=True,
                recycle_ratio=0.92,
                recycle_min_uptime_seconds=0.0,
                recycle_cooldown_seconds=300.0,
                recycle_jitter_seconds=0.0,  # no real sleep needed to exercise the recheck
            )
        ),
    )
    killed: list = []
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog.os.kill",
        lambda pid, sig: killed.append((pid, sig)),
    )

    from dynastore.tools.serving_state import is_draining, clear_draining

    # tick() reads RSS once (for the warn/critical branch, and the ratio it
    # passes into _maybe_self_recycle); _maybe_self_recycle then reads it a
    # second time for the post-jitter recheck. Return the high value on the
    # first read (95% — triggers the recycle decision) and a low value from
    # then on, so the recheck sees the spike has already subsided.
    calls = {"n": 0}

    def _stateful():
        calls["n"] += 1
        return 950 if calls["n"] == 1 else 100

    svc = MemoryWatchdogService(limit_bytes=1000, get_rss_bytes=_stateful)
    svc._started_at -= 1000
    clear_draining()

    await svc.tick(_make_ctx())

    assert killed == []
    assert is_draining() is False


# ---------------------------------------------------------------------------
# End-to-end wiring through the real BackgroundSupervisor
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_service_runs_under_real_supervisor_and_stops_cleanly(caplog) -> None:
    """Registered with a real BackgroundSupervisor, it ticks and drains on stop."""
    svc = MemoryWatchdogService(
        limit_bytes=1000,
        warn_ratio=0.8,
        critical_ratio=0.9,
        cadence_seconds=0.01,
        get_rss_bytes=lambda: 950,
    )
    ctx = ServiceContext(
        engine=None,
        shutdown=asyncio.Event(),
        is_ephemeral=False,
        name="supervisor-test",
    )
    supervisor = BackgroundSupervisor()
    supervisor.register(svc)

    with caplog.at_level(logging.ERROR, logger="dynastore.tools.memory_watchdog"):
        supervisor.start(ctx)
        await asyncio.sleep(0.05)
        ctx.shutdown.set()
        await supervisor.stop(timeout=2.0)

    assert any(r.levelno >= logging.ERROR for r in caplog.records)


# ---------------------------------------------------------------------------
# Diagnostic tracemalloc snapshot (geoid#3121)
# ---------------------------------------------------------------------------


def _patch_config(monkeypatch, config: MemoryWatchdogConfig) -> None:
    import dynastore.tools.memory_watchdog as mw

    async def _fake_load() -> MemoryWatchdogConfig:
        return config

    monkeypatch.setattr(mw, "load_memory_watchdog_config", _fake_load)


@pytest.fixture
def _stop_tracemalloc():
    """Leave tracemalloc off after any diagnostic test (it is process-global)."""
    import tracemalloc

    yield
    if tracemalloc.is_tracing():
        tracemalloc.stop()


@pytest.mark.asyncio
async def test_diagnostic_off_by_default_takes_no_snapshot(
    monkeypatch, caplog, _stop_tracemalloc
) -> None:
    """With diagnostics disabled (the default), no snapshot log is emitted even
    while RSS is high enough to have crossed diagnostic_ratio."""
    import tracemalloc

    tracemalloc.start(1)
    _patch_config(monkeypatch, MemoryWatchdogConfig())  # diagnostic off by default
    svc = MemoryWatchdogService(
        limit_bytes=1000,
        warn_ratio=0.8,
        critical_ratio=0.9,
        get_rss_bytes=lambda: 700,  # 70% — above default diagnostic_ratio 0.60
    )
    with caplog.at_level(logging.ERROR, logger="dynastore.tools.memory_watchdog"):
        await svc.tick(_make_ctx())
    assert not any("diagnostic" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_diagnostic_logs_top_allocations_above_ratio(
    monkeypatch, caplog, _stop_tracemalloc
) -> None:
    """Tracing already on but no arming-tick baseline (e.g. a PYTHONTRACEMALLOC
    boot): the first qualifying tick logs the absolute top sites once, as the
    baseline report; growth diffs follow from the next report."""
    import tracemalloc

    tracemalloc.start(1)  # pre-armed so this tick exercises the snapshot path
    # Give the snapshot something concrete to rank at the top.
    big = [bytearray(1024 * 512) for _ in range(8)]  # ~4MiB
    _patch_config(
        monkeypatch,
        MemoryWatchdogConfig(
            diagnostic_tracemalloc_enabled=True,
            diagnostic_ratio=0.50,
            diagnostic_min_interval_seconds=0.0,
            self_recycle_enabled=False,
        ),
    )
    svc = MemoryWatchdogService(
        limit_bytes=1000,
        warn_ratio=0.8,
        critical_ratio=0.9,
        get_rss_bytes=lambda: 700,  # 70% — above diagnostic_ratio
    )
    with caplog.at_level(logging.ERROR, logger="dynastore.tools.memory_watchdog"):
        await svc.tick(_make_ctx())
    diag = [r for r in caplog.records if "diagnostic" in r.getMessage()]
    assert len(diag) == 1
    assert "baseline report" in diag[0].getMessage()
    assert "allocation sites" in diag[0].getMessage()
    assert len(big) == 8  # keep the allocation alive until after the snapshot


@pytest.mark.asyncio
async def test_diagnostic_throttles_repeated_snapshots(
    monkeypatch, caplog, _stop_tracemalloc
) -> None:
    """A sustained climb does not log a snapshot every tick — throttled by
    diagnostic_min_interval_seconds."""
    import tracemalloc

    tracemalloc.start(1)
    _patch_config(
        monkeypatch,
        MemoryWatchdogConfig(
            diagnostic_tracemalloc_enabled=True,
            diagnostic_ratio=0.50,
            diagnostic_min_interval_seconds=60.0,  # long enough to suppress tick 2/3
            self_recycle_enabled=False,
        ),
    )
    svc = MemoryWatchdogService(
        limit_bytes=1000,
        warn_ratio=0.8,
        critical_ratio=0.9,
        get_rss_bytes=lambda: 700,
    )
    ctx = _make_ctx()
    with caplog.at_level(logging.ERROR, logger="dynastore.tools.memory_watchdog"):
        await svc.tick(ctx)
        await svc.tick(ctx)
        await svc.tick(ctx)
    diag = [r for r in caplog.records if "diagnostic" in r.getMessage()]
    assert len(diag) == 1


@pytest.mark.asyncio
async def test_diagnostic_lazy_starts_tracing_then_snapshots(
    monkeypatch, caplog, _stop_tracemalloc
) -> None:
    """Enabled while tracemalloc is off: the first qualifying tick lazily arms
    tracing AND captures the baseline snapshot, and a later tick emits the
    growth report diffed against it. The flag is read live, so the diagnostic
    can be switched on through the config store with no restart."""
    import tracemalloc

    if tracemalloc.is_tracing():
        tracemalloc.stop()
    _patch_config(
        monkeypatch,
        MemoryWatchdogConfig(
            diagnostic_tracemalloc_enabled=True,
            diagnostic_ratio=0.50,
            diagnostic_min_interval_seconds=0.0,
            self_recycle_enabled=False,
        ),
    )
    svc = MemoryWatchdogService(
        limit_bytes=1000,
        warn_ratio=0.8,
        critical_ratio=0.9,
        get_rss_bytes=lambda: 700,
    )
    ctx = _make_ctx()
    with caplog.at_level(logging.WARNING, logger="dynastore.tools.memory_watchdog"):
        await svc.tick(ctx)  # arms tracing + captures the baseline
        assert tracemalloc.is_tracing()
        assert svc._diag_baseline is not None
        assert any("started tracemalloc" in r.getMessage() for r in caplog.records)
        assert not any("[diagnostic]" in r.getMessage() for r in caplog.records)
        await svc.tick(ctx)  # now reports growth against the baseline
    diag = [r for r in caplog.records if "[diagnostic]" in r.getMessage()]
    assert len(diag) == 1
    assert "previous snapshot" in diag[0].getMessage()


@pytest.mark.asyncio
async def test_diagnostic_reports_growth_since_previous_snapshot(
    monkeypatch, caplog, _stop_tracemalloc
) -> None:
    """The report names sites that GREW between snapshots (compare_to against
    the rolled-forward baseline), with an explicit +size diff — not the absolute
    top-N, which steady-state allocations would dominate."""
    import tracemalloc

    if tracemalloc.is_tracing():
        tracemalloc.stop()
    _patch_config(
        monkeypatch,
        MemoryWatchdogConfig(
            diagnostic_tracemalloc_enabled=True,
            diagnostic_ratio=0.50,
            diagnostic_min_interval_seconds=0.0,
            self_recycle_enabled=False,
        ),
    )
    svc = MemoryWatchdogService(
        limit_bytes=1000,
        warn_ratio=0.8,
        critical_ratio=0.9,
        get_rss_bytes=lambda: 700,
    )
    ctx = _make_ctx()
    with caplog.at_level(logging.ERROR, logger="dynastore.tools.memory_watchdog"):
        await svc.tick(ctx)  # arming tick: baseline only, no report
        # Allocate AFTER the baseline so this site shows up as growth.
        big = [bytearray(1024 * 512) for _ in range(8)]  # ~4MiB
        await svc.tick(ctx)
    diag = [r for r in caplog.records if "[diagnostic]" in r.getMessage()]
    assert len(diag) == 1
    message = diag[0].getMessage()
    assert "by growth since the previous snapshot" in message
    assert "+" in message  # sites are reported as +N.NMiB deltas
    assert "test_memory_watchdog" in message  # this file is the growing site
    assert len(big) == 8  # keep the allocation alive until after the snapshot


@pytest.mark.asyncio
async def test_diagnostic_no_python_growth_points_at_native_memory(
    monkeypatch, caplog, _stop_tracemalloc
) -> None:
    """When the diff comes back empty while RSS is high, the report says so
    explicitly and points at native/C-extension memory — the decisive signal
    that tracemalloc is the wrong tool and a native profiler is next."""
    import dynastore.tools.memory_watchdog as mw

    class _FakeSnapshot:
        def compare_to(self, _baseline, _key):
            return []

        def statistics(self, _key):
            return []

    class _FakeTracemalloc:
        @staticmethod
        def is_tracing() -> bool:
            return True

        @staticmethod
        def take_snapshot() -> "_FakeSnapshot":
            return _FakeSnapshot()

        @staticmethod
        def get_traced_memory():
            return (42 * 1024 * 1024, 64 * 1024 * 1024)

    monkeypatch.setattr(mw, "tracemalloc", _FakeTracemalloc())
    _patch_config(
        monkeypatch,
        MemoryWatchdogConfig(
            diagnostic_tracemalloc_enabled=True,
            diagnostic_ratio=0.50,
            diagnostic_min_interval_seconds=0.0,
            self_recycle_enabled=False,
        ),
    )
    # Real bytes scale (unlike most tests above, which use abstract small
    # numbers): 42MiB traced against 700MiB RSS is the minority-traced case
    # the native disclaimer is for.
    svc = MemoryWatchdogService(
        limit_bytes=1000 * 1024 * 1024,
        warn_ratio=0.8,
        critical_ratio=0.9,
        get_rss_bytes=lambda: 700 * 1024 * 1024,
    )
    svc._diag_baseline = _FakeSnapshot()  # type: ignore[assignment]  # as if armed earlier
    with caplog.at_level(logging.ERROR, logger="dynastore.tools.memory_watchdog"):
        await svc.tick(_make_ctx())
    diag = [r for r in caplog.records if "[diagnostic]" in r.getMessage()]
    assert len(diag) == 1
    message = diag[0].getMessage()
    assert "NO Python-level allocation growth" in message
    assert "% traced" in message
    assert "native/C-extension" in message
    assert "GEOS/GDAL" in message


@pytest.mark.asyncio
async def test_build_does_not_start_tracemalloc(
    monkeypatch, _stop_tracemalloc
) -> None:
    """The diagnostic is armed lazily from the tick path, never at build time:
    the platform config store is unreachable when the service is built, so a
    build-time read of the flag always sees the default. build_memory_watchdog_service
    therefore leaves tracing off and keeps the plain cadence even with the flag set."""
    import tracemalloc

    if tracemalloc.is_tracing():
        tracemalloc.stop()
    _patch_config(
        monkeypatch,
        MemoryWatchdogConfig(
            diagnostic_tracemalloc_enabled=True,
            cadence_seconds=15.0,
        ),
    )
    svc = await build_memory_watchdog_service()
    assert svc is not None
    assert not tracemalloc.is_tracing()
    assert svc.cadence_seconds == pytest.approx(15.0)


@pytest.mark.asyncio
async def test_diagnostic_disable_stops_tracing_it_started(
    monkeypatch, _stop_tracemalloc
) -> None:
    """Flipping the flag back off releases tracemalloc if this service started
    it — so an operator can turn the diagnostic off live and reclaim the
    per-allocation overhead without recycling the process."""
    import tracemalloc

    if tracemalloc.is_tracing():
        tracemalloc.stop()
    _patch_config(
        monkeypatch,
        MemoryWatchdogConfig(
            diagnostic_tracemalloc_enabled=True,
            diagnostic_ratio=0.50,
            diagnostic_min_interval_seconds=0.0,
            self_recycle_enabled=False,
        ),
    )
    svc = MemoryWatchdogService(
        limit_bytes=1000,
        warn_ratio=0.8,
        critical_ratio=0.9,
        get_rss_bytes=lambda: 700,
    )
    ctx = _make_ctx()
    await svc.tick(ctx)  # lazy-starts tracing
    assert tracemalloc.is_tracing()
    _patch_config(monkeypatch, MemoryWatchdogConfig())  # diagnostic now off
    await svc.tick(ctx)
    assert not tracemalloc.is_tracing()


def _decode_json_payload(raw: str):
    """Stand-in for the real callers of ``json.loads`` (cache decode, HTTP
    bodies): the frame the diagnostic has to name."""
    import json

    return json.loads(raw)


def test_tracemalloc_keeps_enough_frames_to_reach_the_caller() -> None:
    """``raw_decode`` <- ``decode`` <- ``loads`` <- caller is four frames deep,
    so anything below that can only ever name ``json/decoder.py``."""
    from dynastore.tools.memory_watchdog import _TRACEMALLOC_FRAMES

    assert _TRACEMALLOC_FRAMES >= 4


@pytest.mark.asyncio
async def test_growth_report_names_the_caller_not_just_the_leaf_frame(
    monkeypatch, caplog, _stop_tracemalloc
) -> None:
    """A spike inside ``json.loads`` must be attributed to the code that asked
    for the decode. Retaining one frame reports ``json/decoder.py`` — true, and
    useless for finding the culprit (geoid#3121)."""
    import json
    import tracemalloc

    assert not tracemalloc.is_tracing()
    raw = json.dumps([{"k": i, "v": "x" * 64} for i in range(20000)])

    _patch_config(
        monkeypatch,
        MemoryWatchdogConfig(
            diagnostic_tracemalloc_enabled=True,
            diagnostic_ratio=0.50,
            diagnostic_min_interval_seconds=0.0,
            self_recycle_enabled=False,
            readiness_shed_enabled=False,
        ),
    )
    svc = MemoryWatchdogService(
        limit_bytes=1000,
        warn_ratio=0.8,
        critical_ratio=0.9,
        get_rss_bytes=lambda: 700,  # 70% — above diagnostic_ratio, below warn
    )

    await svc.tick(_make_ctx())  # arming tick: starts tracing, takes the baseline
    decoded = _decode_json_payload(raw)  # the spike the next report must explain
    caplog.clear()
    with caplog.at_level(logging.ERROR, logger="dynastore.tools.memory_watchdog"):
        await svc.tick(_make_ctx())

    reports = [
        r.getMessage()
        for r in caplog.records
        if "memory_watchdog[diagnostic]" in r.getMessage()
    ]
    assert len(reports) == 1

    # Each ranked entry starts with "  N. +X.YMiB ...". Isolate the entry that
    # the decode dominates; the caller must be inside *that* entry, not merely
    # somewhere else in the report.
    entries = re.split(r"\n\s+\d+\.\s", reports[0])
    decode_entries = [e for e in entries if "decoder.py" in e]
    assert decode_entries, "expected the json decode to dominate the growth"
    assert "test_memory_watchdog.py" in decode_entries[0], (
        "the decode entry must name the caller of json.loads, not only its leaf "
        f"frame. Entry was:\n{decode_entries[0]}"
    )
    assert len(decoded) == 20000  # keep the allocation alive past the snapshot


# ---------------------------------------------------------------------------
# Diagnostic coupled to threshold crossings (geoid#3191)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_crossing_warn_forces_diagnostic_below_diagnostic_ratio(
    monkeypatch, caplog, _stop_tracemalloc
) -> None:
    """A tick that crosses INTO the warn band forces the diagnostic snapshot
    on that same tick even though ``ratio`` has not reached ``diagnostic_ratio``
    yet — a fast spike that crosses warn and kills the worker within one
    cadence period must not wait for a later poll that happens to also clear
    diagnostic_ratio."""
    import tracemalloc

    if tracemalloc.is_tracing():
        tracemalloc.stop()
    tracemalloc.start(1)
    _patch_config(
        monkeypatch,
        MemoryWatchdogConfig(
            diagnostic_tracemalloc_enabled=True,
            diagnostic_ratio=0.95,  # above warn_ratio: never reached in this test
            diagnostic_min_interval_seconds=0.0,
            self_recycle_enabled=False,
        ),
    )
    rss = {"value": 500}
    svc = MemoryWatchdogService(
        limit_bytes=1000,
        warn_ratio=0.8,
        critical_ratio=0.9,
        get_rss_bytes=lambda: rss["value"],
    )
    svc._diag_baseline = tracemalloc.take_snapshot()  # already armed
    ctx = _make_ctx()

    with caplog.at_level(logging.WARNING, logger="dynastore.tools.memory_watchdog"):
        await svc.tick(ctx)  # ratio 0.5 — below warn, no diagnostic
        caplog.clear()
        rss["value"] = 850  # crosses warn (0.8) — ratio 0.85, still < diagnostic_ratio 0.95
        await svc.tick(ctx)

    diag = [r for r in caplog.records if "[diagnostic]" in r.getMessage()]
    assert len(diag) == 1


@pytest.mark.asyncio
async def test_crossing_inside_min_interval_does_not_force_second_snapshot(
    monkeypatch, caplog, _stop_tracemalloc
) -> None:
    """Crossing bypasses the diagnostic_ratio floor, but must NOT bypass the
    min-interval throttle: a crossing that lands inside the window since the
    previous snapshot does not force a second one."""
    import tracemalloc

    if tracemalloc.is_tracing():
        tracemalloc.stop()
    tracemalloc.start(1)
    _patch_config(
        monkeypatch,
        MemoryWatchdogConfig(
            diagnostic_tracemalloc_enabled=True,
            diagnostic_ratio=0.95,  # above both warn and critical in this test
            diagnostic_min_interval_seconds=60.0,
            self_recycle_enabled=False,
        ),
    )
    rss = {"value": 500}
    svc = MemoryWatchdogService(
        limit_bytes=1000,
        warn_ratio=0.8,
        critical_ratio=0.9,
        get_rss_bytes=lambda: rss["value"],
    )
    svc._diag_baseline = tracemalloc.take_snapshot()
    svc._last_diag_snapshot = time.monotonic()  # a snapshot "just" landed
    ctx = _make_ctx()

    with caplog.at_level(logging.WARNING, logger="dynastore.tools.memory_watchdog"):
        rss["value"] = 850  # crosses warn — still below diagnostic_ratio 0.95
        await svc.tick(ctx)

    diag = [r for r in caplog.records if "[diagnostic]" in r.getMessage()]
    assert diag == []


@pytest.mark.asyncio
async def test_crossing_tick_logs_diagnostic_before_bare_rss_line(
    monkeypatch, caplog, _stop_tracemalloc
) -> None:
    """On a crossing tick the diagnostic must land in the log BEFORE the bare
    RSS warn line: if the spike kills the worker mid-tick, the last thing
    flushed has to be the report that names the allocator, not the line that
    only restates the number."""
    import tracemalloc

    if tracemalloc.is_tracing():
        tracemalloc.stop()
    tracemalloc.start(1)
    _patch_config(
        monkeypatch,
        MemoryWatchdogConfig(
            diagnostic_tracemalloc_enabled=True,
            diagnostic_ratio=0.95,
            diagnostic_min_interval_seconds=0.0,
            self_recycle_enabled=False,
        ),
    )
    rss = {"value": 500}
    svc = MemoryWatchdogService(
        limit_bytes=1000,
        warn_ratio=0.8,
        critical_ratio=0.9,
        get_rss_bytes=lambda: rss["value"],
    )
    svc._diag_baseline = tracemalloc.take_snapshot()
    ctx = _make_ctx()

    with caplog.at_level(logging.WARNING, logger="dynastore.tools.memory_watchdog"):
        await svc.tick(ctx)  # ratio 0.5 — below warn
        caplog.clear()
        rss["value"] = 850  # crosses warn
        await svc.tick(ctx)

    messages = [r.getMessage() for r in caplog.records]
    diag_idx = [i for i, m in enumerate(messages) if "[diagnostic]" in m]
    warn_idx = [i for i, m in enumerate(messages) if "of the" in m and "budget" in m]
    assert diag_idx and warn_idx
    assert diag_idx[0] < warn_idx[0]


@pytest.mark.asyncio
async def test_diagnostic_report_flags_native_growth_when_traced_is_minority(
    monkeypatch, caplog, _stop_tracemalloc
) -> None:
    """The report always states the traced/RSS ratio, and when tracemalloc's
    traced total covers only a small fraction of RSS, explicitly says the
    untraced growth is native (GEOS/GDAL/driver-level) — the decisive signal
    that no Python allocation site can name it."""
    import tracemalloc

    if tracemalloc.is_tracing():
        tracemalloc.stop()
    _patch_config(
        monkeypatch,
        MemoryWatchdogConfig(
            diagnostic_tracemalloc_enabled=True,
            diagnostic_ratio=0.50,
            diagnostic_min_interval_seconds=0.0,
            self_recycle_enabled=False,
        ),
    )
    # Real bytes scale: RSS ~700MiB against a handful of KB of tracemalloc-
    # traced Python allocations — the overwhelming majority is untraced.
    svc = MemoryWatchdogService(
        limit_bytes=1000 * 1024 * 1024,
        warn_ratio=0.8,
        critical_ratio=0.9,
        get_rss_bytes=lambda: 700 * 1024 * 1024,
    )
    ctx = _make_ctx()
    with caplog.at_level(logging.ERROR, logger="dynastore.tools.memory_watchdog"):
        await svc.tick(ctx)  # arming tick
        small = [bytearray(1024) for _ in range(4)]  # a few KB — negligible vs RSS
        await svc.tick(ctx)

    diag = [r for r in caplog.records if "[diagnostic]" in r.getMessage()]
    assert len(diag) == 1
    message = diag[0].getMessage()
    assert "% traced" in message
    assert "native/C-extension" in message
    assert "GEOS/GDAL" in message
    assert len(small) == 4


@pytest.mark.asyncio
async def test_diagnostic_report_omits_native_statement_when_traced_dominates(
    monkeypatch, caplog, _stop_tracemalloc
) -> None:
    """When tracemalloc's traced heap covers most of RSS, the report still
    states the ratio but does not claim the growth is native — the numbers
    genuinely are explained by Python-level allocations."""
    import tracemalloc

    if tracemalloc.is_tracing():
        tracemalloc.stop()
    _patch_config(
        monkeypatch,
        MemoryWatchdogConfig(
            diagnostic_tracemalloc_enabled=True,
            diagnostic_ratio=0.50,
            diagnostic_min_interval_seconds=0.0,
            self_recycle_enabled=False,
        ),
    )
    svc = MemoryWatchdogService(
        limit_bytes=4 * 1024 * 1024,
        warn_ratio=0.8,
        critical_ratio=0.9,
        get_rss_bytes=lambda: 3 * 1024 * 1024,  # 75% — below warn, above diagnostic_ratio
    )
    ctx = _make_ctx()
    with caplog.at_level(logging.ERROR, logger="dynastore.tools.memory_watchdog"):
        await svc.tick(ctx)  # arms tracing — no traced allocation yet
        big = [bytearray(1024 * 1024) for _ in range(2)]  # ~2MiB, traced after arming
        await svc.tick(ctx)

    diag = [r for r in caplog.records if "[diagnostic]" in r.getMessage()]
    assert len(diag) == 1
    message = diag[0].getMessage()
    assert "% traced" in message
    assert "native/C-extension" not in message
    assert len(big) == 2
