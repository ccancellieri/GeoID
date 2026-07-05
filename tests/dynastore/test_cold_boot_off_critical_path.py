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
import threading

from fastapi.testclient import TestClient

import dynastore.modules.presets.cold_boot as cold_boot_module
from dynastore.main import app


def test_cold_boot_reconciliation_runs_after_startup_not_before(monkeypatch):
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    async def _blocking_run_cold_boot(engine):
        started.set()
        # Wait on a threading.Event via a worker thread so this coroutine
        # occupies the background task without ever touching the DB —
        # isolates the test to the scheduling behavior being verified.
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, release.wait, 10)
        finished.set()

    monkeypatch.setattr(cold_boot_module, "run_cold_boot", _blocking_run_cold_boot)

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
