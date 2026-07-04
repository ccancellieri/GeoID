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

"""Circuit-breaker recovery loop (#2741).

``ValkeyCacheBackend._record_failure`` unregisters itself from the
CacheManager after 3 consecutive failures, but nothing used to re-register
it — a mid-life trip degraded a pod to L1-only cache (and IAM denylist
checks failing open) for the rest of its process lifetime. These tests pin
the recovery loop CacheModule now owns: ``_on_backend_trip`` schedules a
single guarded re-probe task, it keeps retrying with backoff until
``_current_backend`` is populated again, and it is cancelled on shutdown.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dynastore.modules.cache import cache_module as cm


def _reset_state() -> None:
    cm._current_backend = None
    cm._recovery_task = None


@pytest.fixture(autouse=True)
def _isolate() -> Any:
    _reset_state()
    yield
    _reset_state()


async def _drain_recovery_task() -> None:
    """Await the scheduled recovery task to completion (or a bounded wait)."""
    task = cm._recovery_task
    if task is not None:
        for _ in range(500):
            if task.done():
                return
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_on_backend_trip_schedules_a_task() -> None:
    sentinel = object()

    async def _slow_recover(*_a: Any, **_kw: Any) -> None:
        await asyncio.sleep(1)

    with patch.object(cm, "_recover_after_circuit_trip", _slow_recover):
        cm._on_backend_trip(sentinel)
        assert cm._recovery_task is not None
        first_task = cm._recovery_task

        # A second trip while the loop is still in flight must not
        # schedule a duplicate.
        cm._on_backend_trip(sentinel)
        assert cm._recovery_task is first_task

        first_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first_task


@pytest.mark.asyncio
async def test_recovery_loop_retries_until_backend_recovers(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Backoff loop keeps probing and stops as soon as a backend is live."""
    tripped_backend = object()
    attempts = {"n": 0}

    async def _fake_apply_handler(*_a: Any, **_kw: Any) -> None:
        attempts["n"] += 1
        if attempts["n"] >= 3:
            cm._current_backend = MagicMock(name="recovered-backend")

    with (
        patch.object(cm, "_CB_RECOVERY_INITIAL_DELAY", 0.01),
        patch.object(cm, "_CB_RECOVERY_MAX_DELAY", 0.02),
        patch.object(cm, "_on_valkey_engine_config_change", _fake_apply_handler),
        caplog.at_level("INFO"),
    ):
        cm._on_backend_trip(tripped_backend)
        await _drain_recovery_task()

    assert attempts["n"] == 3
    assert cm._current_backend is not None
    assert cm._recovery_task is None  # cleared on completion
    assert any(
        "circuit breaker recovered" in r.getMessage() for r in caplog.records
    )


@pytest.mark.asyncio
async def test_recovery_loop_stops_when_superseded_by_concurrent_reconnect() -> None:
    """If a live PATCH reconnect races the recovery loop and wins, the loop
    must not keep hammering Valkey — and must not overwrite the winner."""
    tripped_backend = object()
    winner = MagicMock(name="config-patch-backend")
    apply_calls = {"n": 0}

    async def _fake_apply_handler(*_a: Any, **_kw: Any) -> None:
        apply_calls["n"] += 1

    with (
        patch.object(cm, "_CB_RECOVERY_INITIAL_DELAY", 0.01),
        patch.object(cm, "_CB_RECOVERY_MAX_DELAY", 0.02),
        patch.object(cm, "_on_valkey_engine_config_change", _fake_apply_handler),
    ):
        cm._on_backend_trip(tripped_backend)
        # Racing config-PATCH reconnect installs a different backend while
        # the recovery loop is still sleeping its first backoff window.
        cm._current_backend = winner
        await _drain_recovery_task()

    assert cm._current_backend is winner
    assert apply_calls["n"] == 0, "loop must bail before re-probing once superseded"


@pytest.mark.asyncio
async def test_recovery_loop_guard_tracks_observed_backend_not_the_original_trip() -> None:
    """#2741 finding 2 — the ``guard_current`` passed on each attempt must be
    whatever this loop itself last observed ``_current_backend`` to be, not
    permanently pinned to the original ``tripped_backend``.

    A failed reconnect attempt leaves ``_current_backend`` at ``None``
    (torn down, nothing rebuilt); the *next* attempt's guard must reflect
    that (``None``), not the stale original backend — otherwise every
    retry after the first would spuriously see a "guard mismatch" inside
    the lock and the loop would never actually recover.
    """
    tripped_backend = object()
    guards_seen: list[Any] = []

    async def _fake_apply_handler(*_a: Any, **_kw: Any) -> None:
        guards_seen.append(_kw.get("guard_current"))
        if len(guards_seen) >= 3:
            cm._current_backend = MagicMock(name="recovered-backend")
        else:
            # Mirrors the real handler: teardown always sets
            # _current_backend to None before a rebuild is attempted, even
            # when the rebuild itself then fails.
            cm._current_backend = None

    cm._current_backend = tripped_backend  # the trip hasn't been torn down yet
    with (
        patch.object(cm, "_CB_RECOVERY_INITIAL_DELAY", 0.01),
        patch.object(cm, "_CB_RECOVERY_MAX_DELAY", 0.02),
        patch.object(cm, "_on_valkey_engine_config_change", _fake_apply_handler),
    ):
        cm._on_backend_trip(tripped_backend)
        await _drain_recovery_task()

    assert guards_seen == [tripped_backend, None, None]
    assert cm._current_backend is not None


@pytest.mark.asyncio
async def test_cancel_recovery_task_logs_real_error_not_swallowed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A genuine error raised while the task unwinds from cancellation must
    be logged, not silently eaten alongside the expected CancelledError."""
    tripped_backend = object()

    async def _buggy_recover(*_a: Any, **_kw: Any) -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            raise RuntimeError("recovery cleanup blew up") from None

    with (
        caplog.at_level("ERROR"),
        patch.object(cm, "_recover_after_circuit_trip", _buggy_recover),
    ):
        cm._on_backend_trip(tripped_backend)
        task = cm._recovery_task
        assert task is not None and not task.done()
        # Let the task actually start (reach its ``await asyncio.sleep``)
        # before cancelling — cancelling before the coroutine ever runs
        # short-circuits straight to CancelledError without executing any
        # of its body, which would trivially (and misleadingly) pass here.
        await asyncio.sleep(0)

        await cm._cancel_recovery_task()

    assert cm._recovery_task is None
    assert any(
        "circuit-breaker recovery task errored during cancellation" in r.getMessage()
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_cancel_recovery_task_stops_the_loop() -> None:
    tripped_backend = object()

    async def _never_recovers(*_a: Any, **_kw: Any) -> None:
        return None  # _current_backend stays None -> loop keeps sleeping

    with (
        patch.object(cm, "_CB_RECOVERY_INITIAL_DELAY", 0.05),
        patch.object(cm, "_CB_RECOVERY_MAX_DELAY", 0.05),
        patch.object(cm, "_on_valkey_engine_config_change", _never_recovers),
    ):
        cm._on_backend_trip(tripped_backend)
        task = cm._recovery_task
        assert task is not None and not task.done()

        await cm._cancel_recovery_task()

        assert task.cancelled() or task.done()
        assert cm._recovery_task is None
