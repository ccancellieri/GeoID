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
    read_process_rss_bytes,
)


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
    """Fix 1: the service is always built when enabled, regardless of
    whether a memory budget can be resolved yet — resolution is deferred to
    the first tick() (see the lazy-resolution tests below), so a per-env
    limit_mb committed to the config store shortly after boot still takes
    effect instead of being permanently locked out by an unreachable config
    store at lifespan-start time."""
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
    assert svc._limit_bytes is None


@pytest.mark.asyncio
async def test_build_service_uses_explicit_limit_mb_over_cgroup(monkeypatch, tmp_path) -> None:
    """An explicit config.limit_mb is available synchronously at build time
    (the caller already supplied a resolved config), so it is wired straight
    into the service — no tick() required, and cgroup detection is never
    even consulted."""
    v2_file = tmp_path / "memory.max"
    v2_file.write_text("536870912\n")  # 512 MiB — must be ignored, explicit limit wins
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._CGROUP_V2_MEMORY_MAX_PATH", str(v2_file)
    )

    svc = await build_memory_watchdog_service(
        MemoryWatchdogConfig(
            limit_mb=1024, warn_ratio=0.7, critical_ratio=0.95, cadence_seconds=5.0,
            recycle_ratio=0.99,  # must exceed critical_ratio=0.95
        )
    )

    assert svc is not None
    assert svc._limit_bytes == 1024 * 1024 * 1024
    assert svc._limit_resolved is True
    assert svc._warn_ratio == pytest.approx(0.7)
    assert svc._critical_ratio == pytest.approx(0.95)
    assert svc.cadence_seconds == pytest.approx(5.0)


@pytest.mark.asyncio
async def test_build_service_auto_detects_limit_from_cgroup_on_first_tick(monkeypatch, tmp_path) -> None:
    """No explicit limit_mb: the budget is unresolved right after build()
    (Fix 1) and only becomes available once the service's first tick() runs
    cgroup auto-detection."""
    v2_file = tmp_path / "memory.max"
    v2_file.write_text("536870912\n")  # 512 MiB
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._CGROUP_V2_MEMORY_MAX_PATH", str(v2_file)
    )

    svc = await build_memory_watchdog_service(MemoryWatchdogConfig())

    assert svc is not None
    assert svc._limit_bytes is None
    assert svc._warn_ratio == pytest.approx(0.80)
    assert svc._critical_ratio == pytest.approx(0.90)
    assert svc.cadence_seconds == pytest.approx(15.0)

    await svc.tick(_make_ctx())
    assert svc._limit_bytes == 512 * 1024 * 1024


# ---------------------------------------------------------------------------
# Fix 1 — lazy limit resolution on first tick()
# ---------------------------------------------------------------------------


def _fake_config(cfg: MemoryWatchdogConfig):
    """Build an async ``load_memory_watchdog_config`` replacement returning *cfg*."""
    async def _load() -> MemoryWatchdogConfig:
        return cfg
    return _load


@pytest.mark.asyncio
async def test_tick_resolves_limit_from_live_config_over_cgroup(monkeypatch, tmp_path) -> None:
    """A service built with no limit yet (config store unreachable at boot)
    picks up a live config.limit_mb on its first tick, even when a cgroup
    limit is also present — config wins."""
    v2_file = tmp_path / "memory.max"
    v2_file.write_text("536870912\n")  # 512 MiB — must be ignored
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._CGROUP_V2_MEMORY_MAX_PATH", str(v2_file)
    )
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog.load_memory_watchdog_config",
        _fake_config(MemoryWatchdogConfig(limit_mb=256)),
    )

    svc = MemoryWatchdogService(get_rss_bytes=lambda: 100)
    assert svc._limit_bytes is None

    await svc.tick(_make_ctx())
    assert svc._limit_bytes == 256 * 1024 * 1024


@pytest.mark.asyncio
async def test_tick_falls_back_to_cgroup_when_config_limit_unset(monkeypatch, tmp_path) -> None:
    v2_file = tmp_path / "memory.max"
    v2_file.write_text("536870912\n")  # 512 MiB
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog._CGROUP_V2_MEMORY_MAX_PATH", str(v2_file)
    )
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog.load_memory_watchdog_config",
        _fake_config(MemoryWatchdogConfig()),
    )

    svc = MemoryWatchdogService(get_rss_bytes=lambda: 100)
    await svc.tick(_make_ctx())
    assert svc._limit_bytes == 512 * 1024 * 1024


@pytest.mark.asyncio
async def test_tick_stays_inert_when_neither_config_nor_cgroup_resolves(monkeypatch, tmp_path, caplog) -> None:
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

    assert svc._limit_bytes is None
    assert svc._limit_resolved is True
    assert any("stays inert" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_tick_resolves_limit_only_once_then_caches(monkeypatch, tmp_path) -> None:
    """A limit_mb committed to config AFTER the first tick must not retroactively
    change the resolved (and already cached) limit — resolution happens exactly
    once, per the design (avoids re-detecting cgroup / re-reading config every
    15s for a value that only matters once at startup)."""
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
    await svc.tick(_make_ctx())
    assert svc._limit_bytes is None

    # A later config commits limit_mb — but the first tick already cached
    # "inert", so a second tick must not pick it up.
    monkeypatch.setattr(
        "dynastore.tools.memory_watchdog.load_memory_watchdog_config",
        _fake_config(MemoryWatchdogConfig(limit_mb=999)),
    )
    await svc.tick(_make_ctx())
    assert svc._limit_bytes is None


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
