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

"""Regression test for #3002 — cold-boot reconciliation off the startup path.

``run_cold_boot`` used to be awaited synchronously in ``main.py``'s lifespan,
before ``yield`` — so an app with many catalogs to reconcile could grind
through the entire Cloud Run startup-probe window before ever becoming ready
to serve a request. This test proves the pipeline now runs as a background
task submitted through ``BackgroundSupervisor`` and does not gate the app
becoming ready: startup must complete (and ``/health`` must be servable)
while reconciliation is still in flight, and the deferred task must still
run to completion afterward under its own single-flight (advisory-lock)
safety.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import logging
import threading
from types import SimpleNamespace

from fastapi.testclient import TestClient
import pytest

import dynastore.modules.presets.cold_boot as cold_boot_module
from dynastore.main import app, _ColdBootMemoryProbe, _ColdBootReconciliationService
from dynastore.tools.background_service import Leadership, ServiceContext
from dynastore.tools.memory_watchdog import MemoryWatchdogConfig


def test_cold_boot_reconciliation_is_delayed_one_shot():
    assert _ColdBootReconciliationService.leadership is Leadership.RUN_EVERYWHERE
    assert _ColdBootReconciliationService.lock_key is None
    assert _ColdBootReconciliationService.initial_delay_seconds > 0


def _lease_leadership_stub(is_leader: bool, calls: list[tuple[tuple, dict]]):
    @asynccontextmanager
    async def _lease(*args, **kwargs):
        calls.append((args, kwargs))
        yield is_leader, None

    return _lease


def _service_context(engine) -> ServiceContext:
    return ServiceContext(
        engine=engine,
        shutdown=asyncio.Event(),
        is_ephemeral=False,
        name="test",
    )


@pytest.mark.asyncio
async def test_cold_boot_reconciliation_runs_once_when_lease_acquired(monkeypatch):
    lease_calls = []
    run_calls = []
    engine = object()

    async def _fake_run_cold_boot(run_engine, *, probe=None):
        run_calls.append((run_engine, probe))

    monkeypatch.setattr(cold_boot_module, "run_cold_boot", _fake_run_cold_boot)
    monkeypatch.setattr(
        "dynastore.modules.db_config.locking_tools.lease_leadership",
        _lease_leadership_stub(True, lease_calls),
    )

    service = _ColdBootReconciliationService(engine=engine)
    await service.run(_service_context(engine))

    assert len(lease_calls) == 1
    assert lease_calls[0][0][0] is engine
    assert lease_calls[0][0][1] == "dynastore.cold_boot_reconciliation"
    assert len(run_calls) == 1
    assert run_calls[0][0] is engine
    assert isinstance(run_calls[0][1], _ColdBootMemoryProbe)


@pytest.mark.asyncio
async def test_cold_boot_reconciliation_skips_when_lease_not_acquired(monkeypatch):
    lease_calls = []
    run_calls = []
    engine = object()

    async def _fake_run_cold_boot(run_engine, *, probe=None):
        run_calls.append((run_engine, probe))

    monkeypatch.setattr(cold_boot_module, "run_cold_boot", _fake_run_cold_boot)
    monkeypatch.setattr(
        "dynastore.modules.db_config.locking_tools.lease_leadership",
        _lease_leadership_stub(False, lease_calls),
    )

    service = _ColdBootReconciliationService(engine=engine)
    await service.run(_service_context(engine))

    assert len(lease_calls) == 1
    assert run_calls == []


def test_cold_boot_reconciliation_runs_after_startup_not_before(monkeypatch):
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    lease_calls = []

    async def _blocking_run_cold_boot(engine, *, probe=None):
        assert probe is not None
        started.set()
        # Wait on a threading.Event via a worker thread so this coroutine
        # occupies the background task without ever touching the DB —
        # isolates the test to the scheduling behavior being verified.
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, release.wait, 10)
        finished.set()

    monkeypatch.setattr(cold_boot_module, "run_cold_boot", _blocking_run_cold_boot)
    monkeypatch.setattr(
        "dynastore.modules.db_config.locking_tools.lease_leadership",
        _lease_leadership_stub(True, lease_calls),
    )
    monkeypatch.setattr(_ColdBootReconciliationService, "initial_delay_seconds", 0.0)

    # TestClient's context manager drives the ASGI lifespan startup to
    # completion before returning — if it returns, the app is ready to
    # serve regardless of reconciliation's progress. Release and drain the
    # background task INSIDE the `with` block, before shutdown's bounded
    # supervisor drain (see main.py's lifespan `finally`) gets a chance to
    # cancel a still-blocked task.
    with TestClient(app) as client:
        resp = client.get("/health")
        assert resp.status_code == 200

        assert started.wait(5), (
            "cold-boot reconciliation was never scheduled in the background"
        )
        assert not finished.is_set(), (
            "cold-boot reconciliation finished before the app became "
            "ready to serve — it is back on the startup critical path"
        )

        release.set()
        assert finished.wait(5), "reconciliation never completed after being released"
        assert len(lease_calls) == 1


@pytest.mark.asyncio
async def test_cold_boot_memory_probe_logs_when_watchdog_diagnostic_enabled(
    monkeypatch, caplog
):
    """The main-process probe turns memory_watchdog_config into cold-boot RSS logs."""
    cfg = MemoryWatchdogConfig(
        diagnostic_tracemalloc_enabled=True,
        diagnostic_ratio=0.50,
    )

    async def _fake_config():
        return cfg

    rss_values = iter([60 * 1024 * 1024, 90 * 1024 * 1024])
    monkeypatch.setattr("dynastore.main.load_memory_watchdog_config", _fake_config)
    monkeypatch.setattr(
        "dynastore.main.read_process_rss_bytes",
        lambda: next(rss_values),
    )
    monkeypatch.setattr("dynastore.main.resolve_watchdog_budget_mb", lambda: 100)

    contributor = SimpleNamespace(name="demo_data", priority=10)
    probe = _ColdBootMemoryProbe()

    with caplog.at_level(logging.WARNING, logger="dynastore.main"):
        await probe("before", contributor, None, None)
        await probe("after", contributor, 1.25, None)

    assert any(
        "cold_boot[diagnostic]" in rec.getMessage()
        and "demo_data" in rec.getMessage()
        and "delta=30MiB" in rec.getMessage()
        for rec in caplog.records
    )
