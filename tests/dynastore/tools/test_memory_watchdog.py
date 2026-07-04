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

import pytest

from dynastore.tools.background_service import BackgroundSupervisor, ServiceContext
from dynastore.tools.memory_watchdog import (
    MemoryWatchdogService,
    build_memory_watchdog_service_from_env,
    read_process_rss_bytes,
)


def _make_ctx(name: str = "test-host") -> ServiceContext:
    return ServiceContext(
        engine=None,
        shutdown=asyncio.Event(),
        is_ephemeral=False,
        name=name,
    )


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
# build_memory_watchdog_service_from_env
# ---------------------------------------------------------------------------


def test_build_from_env_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("MEMORY_WATCHDOG_LIMIT_MB", raising=False)
    assert build_memory_watchdog_service_from_env() is None


def test_build_from_env_disabled_on_non_numeric_limit(monkeypatch) -> None:
    monkeypatch.setenv("MEMORY_WATCHDOG_LIMIT_MB", "not-a-number")
    assert build_memory_watchdog_service_from_env() is None


def test_build_from_env_disabled_on_non_positive_limit(monkeypatch) -> None:
    monkeypatch.setenv("MEMORY_WATCHDOG_LIMIT_MB", "0")
    assert build_memory_watchdog_service_from_env() is None


def test_build_from_env_builds_service_with_configured_values(monkeypatch) -> None:
    monkeypatch.setenv("MEMORY_WATCHDOG_LIMIT_MB", "512")
    monkeypatch.setenv("MEMORY_WATCHDOG_WARN_RATIO", "0.7")
    monkeypatch.setenv("MEMORY_WATCHDOG_CRITICAL_RATIO", "0.95")
    monkeypatch.setenv("MEMORY_WATCHDOG_INTERVAL_SECONDS", "5")

    svc = build_memory_watchdog_service_from_env()

    assert svc is not None
    assert svc._limit_bytes == 512 * 1024 * 1024
    assert svc._warn_ratio == pytest.approx(0.7)
    assert svc._critical_ratio == pytest.approx(0.95)
    assert svc.cadence_seconds == pytest.approx(5.0)


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


def test_build_from_env_uses_defaults_for_unset_ratios(monkeypatch) -> None:
    monkeypatch.setenv("MEMORY_WATCHDOG_LIMIT_MB", "1024")
    monkeypatch.delenv("MEMORY_WATCHDOG_WARN_RATIO", raising=False)
    monkeypatch.delenv("MEMORY_WATCHDOG_CRITICAL_RATIO", raising=False)
    monkeypatch.delenv("MEMORY_WATCHDOG_INTERVAL_SECONDS", raising=False)

    svc = build_memory_watchdog_service_from_env()

    assert svc is not None
    assert svc._warn_ratio == pytest.approx(0.80)
    assert svc._critical_ratio == pytest.approx(0.90)
    assert svc.cadence_seconds == pytest.approx(15.0)
