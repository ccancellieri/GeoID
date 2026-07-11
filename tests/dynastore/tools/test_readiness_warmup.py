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

"""Unit tests for the process-global cache warm-up readiness gate
(geoid#3207)."""
from __future__ import annotations

import asyncio

import pytest

from dynastore.tools.readiness_warmup import is_warm, reset_for_testing, run_warmup


@pytest.fixture(autouse=True)
def _reset():
    reset_for_testing()
    yield
    reset_for_testing()


def test_starts_warm_when_nothing_registered() -> None:
    """No extension has opted in yet -- must never gate readiness."""
    assert is_warm() is True


@pytest.mark.asyncio
async def test_not_warm_until_warmup_runs_then_warm_after() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def _slow_warmup() -> None:
        started.set()
        await release.wait()

    task = asyncio.create_task(run_warmup("slow", _slow_warmup(), timeout=5.0))
    await started.wait()

    assert is_warm() is False

    release.set()
    await task

    assert is_warm() is True


@pytest.mark.asyncio
async def test_warmup_failure_still_reaches_warm_and_logs_a_warning(caplog) -> None:
    """Best-effort: a broken warm-up must not wedge the instance un-ready
    forever -- it logs and still flips is_warm() to True."""

    async def _broken_warmup() -> None:
        raise RuntimeError("boom")

    with caplog.at_level("WARNING"):
        await run_warmup("broken", _broken_warmup(), timeout=5.0)

    assert is_warm() is True
    assert any(
        "broken" in record.message and "failed" in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_warmup_timeout_still_reaches_warm_and_logs_a_warning(caplog) -> None:
    """A warm-up that never finishes within its budget still resolves --
    is_warm() flips to True and the timeout is logged, never blocking
    readiness forever."""

    async def _hangs_forever() -> None:
        await asyncio.Event().wait()

    with caplog.at_level("WARNING"):
        await run_warmup("hangs", _hangs_forever(), timeout=0.01)

    assert is_warm() is True
    assert any(
        "hangs" in record.message and "did not finish" in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_warmup_success_reaches_warm_without_warning(caplog) -> None:
    async def _fast_warmup() -> None:
        return None

    with caplog.at_level("WARNING"):
        await run_warmup("fast", _fast_warmup(), timeout=5.0)

    assert is_warm() is True
    assert not caplog.records
