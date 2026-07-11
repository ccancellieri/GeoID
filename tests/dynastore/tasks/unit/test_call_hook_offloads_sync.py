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

"""Regression tests for ``call_hook`` event-loop offload.

A dev-environment ``catalog_provision`` run (~7 min) had its lease reclaimed
by ``tasks.reap_stuck_tasks`` mid-run: ``call_hook`` invoked a sync
provisioner hook inline on the event loop, and ``CatalogProvisionTask``
shares that loop with ``BatchedHeartbeat._beat_loop``. A blocking sync hook
therefore starved the heartbeat until the lease lapsed. ``call_hook`` now
routes sync hooks through ``run_in_thread`` so they never block the loop.
"""
from __future__ import annotations

import asyncio
import threading

import pytest

from dynastore.tasks._helpers import call_hook


@pytest.mark.asyncio
async def test_sync_hook_runs_off_event_loop():
    """A blocking sync hook must not freeze the event loop.

    While the hook is running, a concurrently-scheduled coroutine must still
    be able to make progress and complete.
    """
    hook_thread_name: list[str] = []
    concurrent_progress: list[str] = []

    def blocking_hook(**kwargs):
        hook_thread_name.append(threading.current_thread().name)
        # Simulate a blocking sync SDK call.
        import time

        time.sleep(0.2)
        return "sync-result"

    async def concurrent_ticker():
        # If the loop were blocked by blocking_hook, this would never
        # observe more than a single tick until the hook finished.
        for i in range(5):
            await asyncio.sleep(0.02)
            concurrent_progress.append(str(i))

    ticker_task = asyncio.create_task(concurrent_ticker())
    result = await call_hook(blocking_hook, catalog_id="c1")
    await ticker_task

    assert result == "sync-result"
    assert hook_thread_name[0] != threading.current_thread().name
    # The ticker must have made progress *during* the blocking hook, not
    # merely after it — proof the loop stayed responsive.
    assert len(concurrent_progress) == 5


@pytest.mark.asyncio
async def test_async_hook_is_awaited_on_the_event_loop():
    """A coroutine-function hook keeps running inline (no thread offload)."""
    called_thread_name: list[str] = []

    async def async_hook(**kwargs):
        called_thread_name.append(threading.current_thread().name)
        return "async-result"

    result = await call_hook(async_hook, catalog_id="c1")

    assert result == "async-result"
    assert called_thread_name[0] == threading.current_thread().name


@pytest.mark.asyncio
async def test_sync_hook_return_value_is_preserved():
    def sync_hook(**kwargs):
        return {"key": "value"}

    result = await call_hook(sync_hook)

    assert result == {"key": "value"}


@pytest.mark.asyncio
async def test_sync_hook_kwargs_forwarded():
    seen = {}

    def sync_hook(**kwargs):
        seen.update(kwargs)
        return None

    await call_hook(sync_hook, external_id="ext-1", scope="catalog")

    assert seen == {"external_id": "ext-1", "scope": "catalog"}
