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

"""Unit tests for BackgroundService primitives — no DB, no real engine."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, Optional, Union
from unittest.mock import MagicMock, patch

import pytest

from dynastore.tools.background_service import (
    BackgroundService,
    BackgroundSupervisor,
    Leadership,
    PeriodicService,
    PodPolicy,
    ServiceContext,
)


# ---------------------------------------------------------------------------
# Helpers / fakes
# ---------------------------------------------------------------------------


def _make_ctx(
    *,
    engine: Any = None,
    is_ephemeral: bool = False,
    name: str = "test-host",
) -> ServiceContext:
    return ServiceContext(
        engine=engine,
        shutdown=asyncio.Event(),
        is_ephemeral=is_ephemeral,
        name=name,
    )


class _FakeAsyncEngine:
    """Minimal stand-in that satisfies isinstance(..., AsyncEngine) via patch."""


class _TrackingExecutor:
    """Executor stub that records submitted task names and actually runs them."""

    def __init__(self) -> None:
        self.submitted: list[str] = []
        self._tasks: list[asyncio.Task[Any]] = []

    def submit(self, coro: Any, task_name: str = "background_task") -> asyncio.Task[Any]:
        self.submitted.append(task_name)
        task = asyncio.create_task(coro, name=task_name)
        self._tasks.append(task)
        return task

    async def gather(self) -> None:
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)


def _make_service(
    *,
    name: str = "test-svc",
    leadership: Leadership = Leadership.RUN_EVERYWHERE,
    pod_policy: PodPolicy = PodPolicy.ALL,
    lock_key: Optional[Union[int, str]] = None,
    run_fn: Any = None,
) -> BackgroundService:
    """Build a minimal BackgroundService structural instance."""

    class _Svc:
        def __init__(self) -> None:
            self.name = name
            self.leadership = leadership
            self.pod_policy = pod_policy
            self.lock_key = lock_key

        async def run(self, ctx: ServiceContext) -> None:
            if run_fn is not None:
                await run_fn(ctx)

    return _Svc()  # type: ignore[return-value]


# Fake advisory-lock context managers


@asynccontextmanager
async def _fake_leader_acquirer():
    """Always yields True (this pod is the leader)."""
    yield True


@asynccontextmanager
async def _fake_non_leader_acquirer():
    """Always yields False (another pod holds the lock)."""
    yield False


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_leader_only_wraps_in_leader_loop_non_leader() -> None:
    """LEADER_ONLY with a non-leader acquirer: run() is never called."""
    tick_count = {"n": 0}

    async def _run(ctx: ServiceContext) -> None:
        tick_count["n"] += 1
        # Exit immediately if called
        ctx.shutdown.set()

    ctx = _make_ctx()
    executor = _TrackingExecutor()
    supervisor = BackgroundSupervisor(executor=executor)

    # Pretend engine is an AsyncEngine so leadership election is attempted
    from sqlalchemy.ext.asyncio import AsyncEngine as _AsyncEngine

    with patch(
        "dynastore.tools.background_service.pg_advisory_leadership",
        side_effect=lambda *a, **kw: _fake_non_leader_acquirer(),
    ):
        with patch(
            "dynastore.tools.background_service.isinstance",
            side_effect=lambda obj, cls: cls is _AsyncEngine,
        ):
            # Provide a fake AsyncEngine object so the isinstance check passes
            ctx = _make_ctx(engine=MagicMock(spec=_AsyncEngine))
            supervisor.register(_make_service(
                name="leader-svc",
                leadership=Leadership.LEADER_ONLY,
                run_fn=_run,
            ))

            # Set shutdown after a brief moment so the outer leader loop exits
            async def _set_shutdown():
                await asyncio.sleep(0.05)
                ctx.shutdown.set()

            asyncio.create_task(_set_shutdown())
            supervisor.start(ctx)
            await asyncio.sleep(0.15)
            await supervisor.stop(timeout=1.0)

    # Non-leader: run() should NOT have been called
    assert tick_count["n"] == 0


@pytest.mark.asyncio
async def test_leader_only_wraps_in_leader_loop_is_leader() -> None:
    """LEADER_ONLY with a leader acquirer: run() is called at least once."""
    tick_count = {"n": 0}

    async def _run(ctx: ServiceContext) -> None:
        tick_count["n"] += 1
        # Signal shutdown so we exit promptly after first tick
        ctx.shutdown.set()

    from sqlalchemy.ext.asyncio import AsyncEngine as _AsyncEngine

    ctx = _make_ctx(engine=MagicMock(spec=_AsyncEngine))
    executor = _TrackingExecutor()
    supervisor = BackgroundSupervisor(executor=executor)
    supervisor.register(_make_service(
        name="leader-svc",
        leadership=Leadership.LEADER_ONLY,
        run_fn=_run,
    ))

    with patch(
        "dynastore.tools.background_service.pg_advisory_leadership",
        side_effect=lambda *a, **kw: _fake_leader_acquirer(),
    ):
        supervisor.start(ctx)
        try:
            await asyncio.wait_for(executor._tasks[0], timeout=2.0)
        except asyncio.TimeoutError:
            ctx.shutdown.set()

    assert tick_count["n"] >= 1


@pytest.mark.asyncio
async def test_run_everywhere_skips_election() -> None:
    """RUN_EVERYWHERE: pg_advisory_leadership is NEVER called."""
    ran = {"yes": False}

    async def _run(ctx: ServiceContext) -> None:
        ran["yes"] = True
        ctx.shutdown.set()

    ctx = _make_ctx()
    executor = _TrackingExecutor()
    supervisor = BackgroundSupervisor(executor=executor)
    supervisor.register(_make_service(
        name="everywhere-svc",
        leadership=Leadership.RUN_EVERYWHERE,
        run_fn=_run,
    ))

    mock_pg = MagicMock()

    with patch(
        "dynastore.tools.background_service.pg_advisory_leadership",
        mock_pg,
    ):
        supervisor.start(ctx)
        if executor._tasks:
            try:
                await asyncio.wait_for(executor._tasks[0], timeout=2.0)
            except asyncio.TimeoutError:
                ctx.shutdown.set()

    mock_pg.assert_not_called()
    assert ran["yes"] is True


@pytest.mark.asyncio
async def test_leader_only_downgrades_on_non_async_engine() -> None:
    """LEADER_ONLY with a non-AsyncEngine: auto-downgrade to RUN_EVERYWHERE."""
    ran = {"yes": False}

    async def _run(ctx: ServiceContext) -> None:
        ran["yes"] = True
        ctx.shutdown.set()

    # Plain sentinel — NOT an AsyncEngine
    ctx = _make_ctx(engine=object())
    executor = _TrackingExecutor()
    supervisor = BackgroundSupervisor(executor=executor)
    supervisor.register(_make_service(
        name="downgrade-svc",
        leadership=Leadership.LEADER_ONLY,
        run_fn=_run,
    ))

    mock_pg = MagicMock()

    with patch(
        "dynastore.tools.background_service.pg_advisory_leadership",
        mock_pg,
    ):
        supervisor.start(ctx)
        if executor._tasks:
            try:
                await asyncio.wait_for(executor._tasks[0], timeout=2.0)
            except asyncio.TimeoutError:
                ctx.shutdown.set()

    mock_pg.assert_not_called()
    assert ran["yes"] is True


@pytest.mark.asyncio
async def test_skip_ephemeral_not_submitted_when_ephemeral() -> None:
    """SKIP_EPHEMERAL service is omitted from submission when is_ephemeral=True."""
    ctx_ephemeral = _make_ctx(is_ephemeral=True)
    executor = _TrackingExecutor()
    supervisor = BackgroundSupervisor(executor=executor)
    supervisor.register(_make_service(
        name="ephemeral-svc",
        pod_policy=PodPolicy.SKIP_EPHEMERAL,
    ))
    supervisor.start(ctx_ephemeral)

    assert "service:ephemeral-svc" not in executor.submitted


@pytest.mark.asyncio
async def test_skip_ephemeral_is_submitted_when_not_ephemeral() -> None:
    """SKIP_EPHEMERAL service IS submitted when is_ephemeral=False."""
    ran = {"yes": False}

    async def _run(ctx: ServiceContext) -> None:
        ran["yes"] = True
        ctx.shutdown.set()

    ctx = _make_ctx(is_ephemeral=False)
    executor = _TrackingExecutor()
    supervisor = BackgroundSupervisor(executor=executor)
    supervisor.register(_make_service(
        name="ephemeral-svc",
        pod_policy=PodPolicy.SKIP_EPHEMERAL,
        run_fn=_run,
    ))
    supervisor.start(ctx)

    assert "service:ephemeral-svc" in executor.submitted
    if executor._tasks:
        try:
            await asyncio.wait_for(executor._tasks[0], timeout=2.0)
        except asyncio.TimeoutError:
            ctx.shutdown.set()
    assert ran["yes"] is True


@pytest.mark.asyncio
async def test_periodic_ticks_then_stops_on_shutdown() -> None:
    """PeriodicService ticks immediately, ticks again, stops when shutdown fires."""
    tick_count = {"n": 0}

    class _FastPeriodic(PeriodicService):
        name = "fast-periodic"
        cadence_seconds = 0.01

        async def tick(self, ctx: ServiceContext) -> None:
            tick_count["n"] += 1

    ctx = _make_ctx()
    svc = _FastPeriodic()

    # Run for enough ticks, then stop
    async def _driver() -> None:
        await asyncio.wait_for(svc.run(ctx), timeout=0.5)

    # Let it tick a couple of times, then signal shutdown
    async def _stopper() -> None:
        await asyncio.sleep(0.05)
        ctx.shutdown.set()

    await asyncio.gather(_driver(), _stopper(), return_exceptions=True)

    assert tick_count["n"] >= 2, f"Expected >=2 ticks, got {tick_count['n']}"


@pytest.mark.asyncio
async def test_context_sleep_returns_true_on_shutdown() -> None:
    """ServiceContext.sleep returns True when shutdown fires during the wait."""
    ctx = _make_ctx()
    # Set shutdown before sleeping — should return True immediately
    ctx.shutdown.set()
    result = await ctx.sleep(10.0)
    assert result is True


@pytest.mark.asyncio
async def test_context_sleep_returns_false_on_timeout() -> None:
    """ServiceContext.sleep returns False when it times out normally."""
    ctx = _make_ctx()
    result = await ctx.sleep(0.01)
    assert result is False


@pytest.mark.asyncio
async def test_supervisor_stop_drains_tasks() -> None:
    """Supervisor.stop() returns within timeout once services exit on shutdown."""
    exited = {"n": 0}

    async def _run(ctx: ServiceContext) -> None:
        # Wait for shutdown, then exit cleanly
        await ctx.shutdown.wait()
        exited["n"] += 1

    ctx = _make_ctx()
    executor = _TrackingExecutor()
    supervisor = BackgroundSupervisor(executor=executor)
    supervisor.register(_make_service(name="svc-a", run_fn=_run))
    supervisor.register(_make_service(name="svc-b", run_fn=_run))

    supervisor.start(ctx)
    assert len(executor._tasks) == 2

    # Signal shutdown then drain
    ctx.shutdown.set()
    await supervisor.stop(timeout=2.0)

    assert all(t.done() for t in executor._tasks)
    assert exited["n"] == 2


@pytest.mark.asyncio
async def test_stop_cancels_straggler_after_timeout() -> None:
    """A service that ignores shutdown is cancelled once stop()'s timeout elapses."""
    started = asyncio.Event()
    cancelled = {"yes": False}

    async def _run(ctx: ServiceContext) -> None:
        started.set()
        try:
            await asyncio.Event().wait()  # never set — ignores ctx.shutdown
        except asyncio.CancelledError:
            cancelled["yes"] = True
            raise

    ctx = _make_ctx()
    executor = _TrackingExecutor()
    supervisor = BackgroundSupervisor(executor=executor)
    supervisor.register(_make_service(name="straggler", run_fn=_run))
    supervisor.start(ctx)
    await asyncio.wait_for(started.wait(), timeout=1.0)

    ctx.shutdown.set()
    # The service will not exit on its own; stop() must cancel it after timeout.
    await supervisor.stop(timeout=0.1)

    assert executor._tasks[0].done()
    assert cancelled["yes"] is True


@pytest.mark.asyncio
async def test_skip_ephemeral_evaluated_before_leadership() -> None:
    """SKIP_EPHEMERAL + LEADER_ONLY in an ephemeral pod is skipped BEFORE any
    leadership election is attempted (gate ordering: pod-policy then election)."""
    ctx = _make_ctx(is_ephemeral=True, engine=MagicMock())
    executor = _TrackingExecutor()
    supervisor = BackgroundSupervisor(executor=executor)
    supervisor.register(_make_service(
        name="leader-ephemeral",
        leadership=Leadership.LEADER_ONLY,
        pod_policy=PodPolicy.SKIP_EPHEMERAL,
    ))

    mock_pg = MagicMock()
    with patch("dynastore.tools.background_service.pg_advisory_leadership", mock_pg):
        supervisor.start(ctx)

    assert "service:leader-ephemeral" not in executor.submitted
    mock_pg.assert_not_called()


@pytest.mark.asyncio
async def test_periodic_run_survives_tick_exception() -> None:
    """A RUN_EVERYWHERE PeriodicService whose tick raises does NOT die — the loop
    logs and continues, so a transient failure can't silently stop it forever."""
    calls = {"n": 0}

    class _FlakyPeriodic(PeriodicService):
        name = "flaky-periodic"
        cadence_seconds = 0.01

        async def tick(self, ctx: ServiceContext) -> None:
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("transient")
            if calls["n"] >= 3:
                ctx.shutdown.set()

    ctx = _make_ctx()
    svc = _FlakyPeriodic()
    await asyncio.wait_for(svc.run(ctx), timeout=1.0)

    assert calls["n"] >= 3  # survived the first-tick exception and kept ticking


@pytest.mark.asyncio
async def test_start_continues_when_one_submit_fails() -> None:
    """If one service's submit raises, start() logs and still starts the rest
    (one failing service must not starve the others)."""

    class _FlakyExecutor:
        def __init__(self) -> None:
            self.submitted: list[str] = []
            self._tasks: list[asyncio.Task[Any]] = []

        def submit(self, coro: Any, task_name: str = "bg") -> asyncio.Task[Any]:
            if "boom" in task_name:
                raise RuntimeError("submit failed")
            self.submitted.append(task_name)
            t = asyncio.create_task(coro, name=task_name)
            self._tasks.append(t)
            return t

    ran = {"ok": False}

    async def _ok(ctx: ServiceContext) -> None:
        ran["ok"] = True
        ctx.shutdown.set()

    ctx = _make_ctx()
    executor = _FlakyExecutor()
    supervisor = BackgroundSupervisor(executor=executor)
    supervisor.register(_make_service(name="boom"))            # submit() raises
    supervisor.register(_make_service(name="good", run_fn=_ok))

    supervisor.start(ctx)  # must NOT raise despite 'boom' failing

    assert "service:good" in executor.submitted
    if executor._tasks:
        await asyncio.wait_for(executor._tasks[0], timeout=1.0)
    assert ran["ok"] is True
