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

"""Unit tests for ``EventService``'s dual async-listener dispatch (#2494).

``EventService.emit`` schedules every async listener twice: once
immediately in-process (fire-and-forget, via ``run_in_background``) and
once more durably when ``EventDrainTask`` later drains the event's
``tasks.events`` row and calls ``dispatch_to_listeners``. Now that
listeners (e.g. ``AssetEntitySyncSubscriber``) are allowed to raise so the
durable leg retries, the immediate leg must not turn that same exception
into an asyncio "Task exception was never retrieved" warning — nothing
awaits that background task's result. ``dispatch_to_listeners`` itself must
keep propagating listener exceptions unchanged; that is the drain's retry
signal.
"""
from __future__ import annotations

import pytest

from dynastore.modules import concurrency
from dynastore.modules.catalog.event_service import EventService

_EVENT_NAME = "test.event_service_immediate_dispatch_noise"


@pytest.mark.asyncio
async def test_immediate_dispatch_leg_swallows_listener_exception():
    """A listener exception on the immediate leg must not leave the
    scheduled background task carrying an unretrieved exception."""
    svc = EventService()
    calls: list = []

    async def failing_listener(**kwargs):
        calls.append(kwargs)
        raise RuntimeError("boom")

    svc._async_listeners[_EVENT_NAME].append(failing_listener)
    try:
        await svc.emit(_EVENT_NAME, catalog_id="c1", asset_id="a1")

        task = next(
            t for t in concurrency._background_tasks
            if t.get_name() == f"event_listener:{_EVENT_NAME}" and not t.done()
        )
        await task  # let the wrapped coroutine run to completion

        assert task.exception() is None, (
            "the immediate-dispatch task must not carry the listener's "
            "exception — it should have been caught and logged"
        )
        assert calls == [{"catalog_id": "c1", "asset_id": "a1"}]
    finally:
        svc._async_listeners[_EVENT_NAME].remove(failing_listener)


@pytest.mark.asyncio
async def test_dispatch_to_listeners_still_propagates_exceptions():
    """The durable leg (``dispatch_to_listeners``, called by
    ``EventDrainTask``) must keep raising — that is the drain's retry
    signal and must not be affected by the immediate-leg fix."""
    svc = EventService()

    async def failing_listener(**kwargs):
        raise RuntimeError("boom")

    svc._async_listeners[_EVENT_NAME].append(failing_listener)
    try:
        with pytest.raises(RuntimeError, match="boom"):
            await svc.dispatch_to_listeners(
                _EVENT_NAME,
                {"args": [], "kwargs": {"catalog_id": "c1", "asset_id": "a1"}},
            )
    finally:
        svc._async_listeners[_EVENT_NAME].remove(failing_listener)
