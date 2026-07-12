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

"""Regression coverage for #3027 (stale ``RequestVisibility`` in background work).

``asyncio.create_task()`` copies the *current* contextvars snapshot at
creation time. A task spawned mid-request, while ``IamMiddleware`` has a
``RequestVisibility`` published, therefore keeps that snapshot for its whole
lifetime — independent of the originating request's own
``reset_request_visibility()`` in its ``finally`` block, because the reset
only mutates the *request's* context, not the already-detached copy the
spawned task owns.

The first test below proves the raw mechanism with nothing but
``asyncio``/``contextvars`` (no DynaStore background-task machinery
involved). The second test exercises the platform's actual fire-and-forget
entry point, :func:`dynastore.modules.concurrency.run_in_background`, and
locks in the fix: background work must start from a clean slate (``None``,
the documented "IAM off / background" state), never from a leftover
request-scoped snapshot.
"""
from __future__ import annotations

import asyncio

import pytest

from dynastore.modules.concurrency import run_in_background
from dynastore.models.protocols.visibility import (
    RequestVisibility,
    get_request_visibility,
    reset_request_visibility,
    set_request_visibility,
)


@pytest.mark.asyncio
async def test_raw_create_task_keeps_stale_visibility_after_request_reset():
    """Demonstrates the underlying contextvars mechanism the issue describes.

    A plain ``asyncio.create_task()`` spawned while a caller snapshot is
    published still observes that snapshot after the "request" resets its
    own context — the spawned task's context is an independent copy.
    """
    seen: list = []
    started = asyncio.Event()
    release = asyncio.Event()

    async def background_job() -> None:
        started.set()
        await release.wait()
        seen.append(get_request_visibility())

    visibility = RequestVisibility(principals=("role:tenant-x",))
    token = set_request_visibility(visibility)
    task = asyncio.create_task(background_job())
    await started.wait()

    # Simulate IamMiddleware.dispatch()'s finally: the request's own context
    # is clean again ...
    reset_request_visibility(token)
    assert get_request_visibility() is None

    # ... but the already-spawned task never saw that reset.
    release.set()
    await task
    assert seen == [visibility]


@pytest.mark.asyncio
async def test_run_in_background_clears_stale_visibility_for_spawned_task():
    """``run_in_background`` is the platform's shared fire-and-forget entry
    point (``BackgroundExecutor.submit`` and every direct caller funnel
    through it). It must not let a task inherit whatever ``RequestVisibility``
    happened to be ambient at spawn time — background work has no live
    request to answer to and must run in the same unfiltered state as
    CLI/out-of-process callers (``get_request_visibility() is None``).
    """
    seen: list = []
    release = asyncio.Event()

    async def job() -> None:
        await release.wait()
        seen.append(get_request_visibility())

    visibility = RequestVisibility(principals=("role:tenant-x",))
    token = set_request_visibility(visibility)
    task = run_in_background(job(), name="test_visibility_isolation")
    reset_request_visibility(token)

    release.set()
    await task

    assert seen == [None]
