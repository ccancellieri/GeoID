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
import signal

import pytest

from dynastore.tools.background_service import BackgroundSupervisor, ServiceContext
from dynastore.tools.memory_watchdog import (
    MemoryWatchdogConfig,
    MemoryWatchdogService,
    build_memory_watchdog_service,
    detect_cgroup_memory_limit_mb,
    parse_memory_to_mb,
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
        "dynastore.tools.memory_watchdog._CGROUP_V2_MEMORY_MAX_PATH", str(v2_file)
    )
    assert detect_cgroup_memory_limit_mb() == 512


def test_detect_cgroup_v1_fallback_when_v2_absent(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._CGROUP_V2_MEMORY_MAX_PATH",
        str(tmp_path / "does-not-exist"),
    )
    v1_file = tmp_path / "memory.limit_in_bytes"
    v1_file.write_text("268435456\n")  # 256 MiB
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._CGROUP_V1_MEMORY_LIMIT_PATH", str(v1_file)
    )
    assert detect_cgroup_memory_limit_mb() == 256


def test_detect_cgroup_v2_max_sentinel_is_unlimited(tmp_path, monkeypatch) -> None:
    v2_file = tmp_path / "memory.max"
    v2_file.write_text("max\n")
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._CGROUP_V2_MEMORY_MAX_PATH", str(v2_file)
    )
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._CGROUP_V1_MEMORY_LIMIT_PATH",
        str(tmp_path / "does-not-exist"),
    )
    assert detect_cgroup_memory_limit_mb() is None


def test_detect_cgroup_v1_huge_sentinel_is_unlimited(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._CGROUP_V2_MEMORY_MAX_PATH",
        str(tmp_path / "does-not-exist"),
    )
    v1_file = tmp_path / "memory.limit_in_bytes"
    v1_file.write_text("9223372036854771712\n")
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._CGROUP_V1_MEMORY_LIMIT_PATH", str(v1_file)
    )
    assert detect_cgroup_memory_limit_mb() is None


def test_detect_cgroup_none_when_neither_file_present(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._CGROUP_V2_MEMORY_MAX_PATH",
        str(tmp_path / "does-not-exist-v2"),
    )
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._CGROUP_V1_MEMORY_LIMIT_PATH",
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
        "dynastore.tools.memory_watchdog._CGROUP_V2_MEMORY_MAX_PATH",
        str(tmp_path / "does-not-exist-v2"),
    )
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._CGROUP_V1_MEMORY_LIMIT_PATH",
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
        "dynastore.tools.memory_watchdog._CGROUP_V2_MEMORY_MAX_PATH", str(v2_file)
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
        "dynastore.tools.memory_watchdog._CGROUP_V2_MEMORY_MAX_PATH",
        str(tmp_path / "nope-v2"),
    )
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._CGROUP_V1_MEMORY_LIMIT_PATH",
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
        "dynastore.tools.memory_watchdog._CGROUP_V2_MEMORY_MAX_PATH",
        str(tmp_path / "nope-v2"),
    )
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._CGROUP_V1_MEMORY_LIMIT_PATH",
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
async def test_tick_inert_and_warns_once_when_no_budget(monkeypatch, tmp_path, caplog) -> None:
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._CGROUP_V2_MEMORY_MAX_PATH",
        str(tmp_path / "does-not-exist-v2"),
    )
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._CGROUP_V1_MEMORY_LIMIT_PATH",
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
    """When enabled and RSS is above diagnostic_ratio, a tracemalloc snapshot of
    the top allocation sites is logged (naming the between-poll spike)."""
    import tracemalloc

    tracemalloc.start(1)  # build_memory_watchdog_service does this in production
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
    assert "top" in diag[0].getMessage()
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
async def test_diagnostic_warns_once_when_tracing_not_started(
    monkeypatch, caplog, _stop_tracemalloc
) -> None:
    """Enabled but tracemalloc was never started: warn once, take no snapshot."""
    import tracemalloc

    if tracemalloc.is_tracing():
        tracemalloc.stop()
    _patch_config(
        monkeypatch,
        MemoryWatchdogConfig(
            diagnostic_tracemalloc_enabled=True,
            diagnostic_ratio=0.50,
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
        await svc.tick(ctx)
        await svc.tick(ctx)
    not_tracing = [r for r in caplog.records if "not tracing" in r.getMessage()]
    assert len(not_tracing) == 1


@pytest.mark.asyncio
async def test_build_starts_tracemalloc_and_uses_diagnostic_cadence(
    monkeypatch, _stop_tracemalloc
) -> None:
    """build_memory_watchdog_service starts tracemalloc and swaps in the faster
    diagnostic cadence when the diagnostic flag is on."""
    import tracemalloc

    if tracemalloc.is_tracing():
        tracemalloc.stop()
    _patch_config(
        monkeypatch,
        MemoryWatchdogConfig(
            diagnostic_tracemalloc_enabled=True,
            diagnostic_cadence_seconds=2.5,
            cadence_seconds=15.0,
        ),
    )
    svc = await build_memory_watchdog_service()
    assert svc is not None
    assert tracemalloc.is_tracing()
    assert svc.cadence_seconds == 2.5
