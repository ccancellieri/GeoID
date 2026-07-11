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
import importlib
from contextlib import asynccontextmanager
from typing import Any, Optional, Union
from unittest.mock import MagicMock, patch

import pytest

from dynastore.tools.background_service import (
    BackgroundService,
    BackgroundSupervisor,
    Leadership,
    LeaseRenewalMode,
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


# Fake leader-election context managers


@asynccontextmanager
async def _fake_leader_acquirer():
    """Always yields (True, None) (this pod is the leader, no connection)."""
    yield (True, None)


@asynccontextmanager
async def _fake_non_leader_acquirer():
    """Always yields (False, None) (another pod holds the lease)."""
    yield (False, None)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_initial_delay_defers_run_without_blocking_supervisor_start() -> None:
    """A service-level initial delay postpones first work, not task submission."""
    ran = asyncio.Event()

    async def _run(ctx: ServiceContext) -> None:
        ran.set()
        ctx.shutdown.set()

    ctx = _make_ctx()
    executor = _TrackingExecutor()
    supervisor = BackgroundSupervisor(executor=executor)
    service = _make_service(name="delayed-svc", run_fn=_run)
    service.initial_delay_seconds = 0.05  # type: ignore[attr-defined]
    supervisor.register(service)

    supervisor.start(ctx)

    assert executor.submitted == ["service:delayed-svc"]
    await asyncio.sleep(0.01)
    assert not ran.is_set()

    await asyncio.wait_for(ran.wait(), timeout=0.3)
    await supervisor.stop(timeout=1.0)


@pytest.mark.asyncio
async def test_initial_delay_exits_without_running_after_shutdown() -> None:
    """Shutdown during the delay window drains without calling service.run()."""
    ran = asyncio.Event()

    async def _run(ctx: ServiceContext) -> None:
        ran.set()

    ctx = _make_ctx()
    ctx.shutdown.set()
    executor = _TrackingExecutor()
    supervisor = BackgroundSupervisor(executor=executor)
    service = _make_service(name="delayed-shutdown-svc", run_fn=_run)
    service.initial_delay_seconds = 60.0  # type: ignore[attr-defined]
    supervisor.register(service)

    supervisor.start(ctx)
    await executor.gather()
    await supervisor.stop(timeout=1.0)

    assert not ran.is_set()


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
        "dynastore.tools.background_service.lease_leadership",
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
        "dynastore.tools.background_service.lease_leadership",
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
    """RUN_EVERYWHERE: lease_leadership is NEVER called."""
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
        "dynastore.tools.background_service.lease_leadership",
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
        "dynastore.tools.background_service.lease_leadership",
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
    with patch("dynastore.tools.background_service.lease_leadership", mock_pg):
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


@pytest.mark.asyncio
async def test_leader_tick_timeout_releases_lock() -> None:
    """A LEADER_ONLY PeriodicService with tick_timeout releases leadership
    when the tick exceeds the timeout, preventing indefinite lock hold."""
    from sqlalchemy.ext.asyncio import AsyncEngine as _AsyncEngine

    class _SlowPeriodic(PeriodicService):
        name = "slow-leader"
        leadership = Leadership.LEADER_ONLY
        cadence_seconds = 30.0
        tick_timeout = 0.1  # 100ms timeout

        async def tick(self, ctx: ServiceContext) -> None:
            await asyncio.sleep(10.0)  # Would hold lock for 10s without timeout

    ctx = _make_ctx(engine=MagicMock(spec=_AsyncEngine))
    executor = _TrackingExecutor()
    supervisor = BackgroundSupervisor(executor=executor)
    supervisor.register(_SlowPeriodic())

    lock_released_at = {"time": None}
    start_time = {"time": None}

    @asynccontextmanager
    async def _fake_leader_acquirer_with_tracking():
        if start_time["time"] is None:
            start_time["time"] = asyncio.get_event_loop().time()
        yield (True, MagicMock())
        lock_released_at["time"] = asyncio.get_event_loop().time()

    original_isinstance = isinstance

    def _patched_isinstance(obj, cls):
        if cls is _AsyncEngine:
            return True
        return original_isinstance(obj, cls)

    with patch(
        "dynastore.tools.background_service.lease_leadership",
        side_effect=lambda *a, **kw: _fake_leader_acquirer_with_tracking(),
    ):
        with patch(
            "dynastore.tools.background_service.isinstance",
            side_effect=_patched_isinstance,
        ):
            supervisor.start(ctx)
            # Wait for the tick to timeout (should be ~0.1s, not 10s)
            await asyncio.sleep(0.3)
            ctx.shutdown.set()
            await supervisor.stop(timeout=1.0)

    # The lock should have been released within 0.5s of acquisition (0.1s timeout + margin)
    assert lock_released_at["time"] is not None, "Lock should have been released"
    assert start_time["time"] is not None, "Start time should be set"
    elapsed = lock_released_at["time"] - start_time["time"]
    assert elapsed < 0.5, f"Lock held for {elapsed:.2f}s, expected <0.5s (timeout should fire)"


@pytest.mark.asyncio
async def test_leader_elected_coro_passes_pre_tick_probe_for_periodic() -> None:
    """_leader_elected_coro must wire a non-None pre_tick_probe into run_leader_loop
    for LEADER_ONLY PeriodicService instances."""
    from sqlalchemy.ext.asyncio import AsyncEngine as _AsyncEngine

    captured: dict[str, Any] = {}

    # The real run_leader_loop is an async function; replacing it with a MagicMock
    # whose side_effect is an async coroutine ensures _leader_elected_coro's
    # "return run_leader_loop(...)" returns an awaitable and captures kwargs.
    async def _capture_and_exit(**kwargs: Any) -> None:
        captured.update(kwargs)

    class _LeaderPeriodic(PeriodicService):
        name = "leader-periodic"
        leadership = Leadership.LEADER_ONLY
        cadence_seconds = 30.0

        async def tick(self, ctx: ServiceContext) -> None:
            pass

    ctx = _make_ctx(engine=MagicMock(spec=_AsyncEngine))
    supervisor = BackgroundSupervisor()

    with patch(
        "dynastore.tools.background_service.run_leader_loop",
        MagicMock(side_effect=_capture_and_exit),
    ):
        coro = supervisor._leader_elected_coro(_LeaderPeriodic(), ctx)
        await coro

    assert "pre_tick_probe" in captured, "pre_tick_probe must be passed to run_leader_loop"
    assert captured["pre_tick_probe"] is not None, "pre_tick_probe must be non-None"


# ---------------------------------------------------------------------------
# LeaseRenewalMode — opt-in continuous-tenure heartbeat regime (#2597)
# ---------------------------------------------------------------------------


def test_periodic_service_default_lease_renewal_mode_is_per_tick() -> None:
    """Every existing PeriodicService keeps the default PER_TICK regime
    without any change to its class body."""

    class _PlainPeriodic(PeriodicService):
        name = "plain-periodic"

        async def tick(self, ctx: ServiceContext) -> None:
            pass

    assert _PlainPeriodic.lease_renewal_mode is LeaseRenewalMode.PER_TICK


@pytest.mark.asyncio
async def test_heartbeat_mode_dispatches_to_heartbeat_loop() -> None:
    """A HEARTBEAT-mode PeriodicService is wrapped by
    run_lease_leadership_heartbeat_loop, not run_leader_loop."""

    class _HeartbeatPeriodic(PeriodicService):
        name = "heartbeat-periodic"
        leadership = Leadership.LEADER_ONLY
        lease_renewal_mode = LeaseRenewalMode.HEARTBEAT
        cadence_seconds = 30.0

        async def tick(self, ctx: ServiceContext) -> None:
            pass

    ctx = _make_ctx(engine=MagicMock())
    supervisor = BackgroundSupervisor()

    captured: dict[str, Any] = {}

    async def _fake_heartbeat_loop(*args: Any, **kwargs: Any) -> None:
        captured["args"] = args
        captured["kwargs"] = kwargs

    with patch(
        "dynastore.tools.background_service.run_lease_leadership_heartbeat_loop",
        side_effect=_fake_heartbeat_loop,
    ) as mock_heartbeat_loop, patch(
        "dynastore.tools.background_service.run_leader_loop",
    ) as mock_per_tick_loop:
        coro = supervisor._leader_elected_coro(_HeartbeatPeriodic(), ctx)
        await coro

    mock_heartbeat_loop.assert_called_once()
    mock_per_tick_loop.assert_not_called()
    assert captured["kwargs"]["name"] == "heartbeat-periodic"
    assert captured["kwargs"]["cadence_seconds"] == 30.0
    # #2959: followers must retry on the service's own cadence, not the 5s
    # default baked into run_lease_leadership_heartbeat_loop.
    assert captured["kwargs"]["reelect_cadence_seconds"] == 30.0


@pytest.mark.asyncio
async def test_heartbeat_mode_ticks_repeatedly_without_reacquiring() -> None:
    """End-to-end (fake lease primitives): a HEARTBEAT-mode service ticks
    several times on cadence while the lease acquire CM is entered exactly
    once, proving the continuous-tenure model doesn't re-elect per tick."""
    acquire_count = {"n": 0}
    tick_count = {"n": 0}
    lost = asyncio.Event()

    @asynccontextmanager
    async def _fake_heartbeat_acquire(engine: Any, key: Any, *, name: str = "leader"):
        acquire_count["n"] += 1
        yield (True, lost)

    class _HeartbeatPeriodic(PeriodicService):
        name = "heartbeat-e2e"
        leadership = Leadership.LEADER_ONLY
        lease_renewal_mode = LeaseRenewalMode.HEARTBEAT
        cadence_seconds = 0.01

        async def tick(self, ctx: ServiceContext) -> None:
            tick_count["n"] += 1
            if tick_count["n"] >= 3:
                ctx.shutdown.set()

    ctx = _make_ctx(engine=MagicMock())
    supervisor = BackgroundSupervisor()

    with patch(
        "dynastore.modules.db_config.locking_tools.lease_leadership_with_heartbeat",
        _fake_heartbeat_acquire,
    ):
        coro = supervisor._leader_elected_coro(_HeartbeatPeriodic(), ctx)
        await asyncio.wait_for(coro, timeout=2.0)

    assert acquire_count["n"] == 1, "lease must be acquired ONCE, not per tick"
    assert tick_count["n"] == 3


@pytest.mark.asyncio
async def test_heartbeat_mode_slow_tick_is_clamped_by_lease_ttl(monkeypatch) -> None:
    """A HEARTBEAT-mode tick that runs longer than the lease TTL is cancelled
    by the same TTL-skew clamp the per-tick regime applies (#2900).

    Without this clamp, a tick that resumes after a throttling freeze longer
    than the lease TTL could keep running after a successor has already
    taken over — split-brain with no forced cutoff. cadence_seconds is set
    much larger than the TTL-derived cap so the clamp firing is attributable
    to the lease TTL, not to a short cadence.
    """
    import dynastore.modules.db_config.connection_health_config as chc

    monkeypatch.setattr(chc._leadership_config, "lease_ttl_seconds", 0.2)
    monkeypatch.setattr(chc._leadership_config, "lease_skew_margin_seconds", 0.1)
    # effective cap = 0.2 - 0.1 = 0.1s

    lost = asyncio.Event()

    @asynccontextmanager
    async def _fake_heartbeat_acquire(engine: Any, key: Any, *, name: str = "leader"):
        yield (True, lost)

    tick_started = asyncio.Event()
    tick_finished = {"done": False}

    class _SlowHeartbeatPeriodic(PeriodicService):
        name = "slow-heartbeat"
        leadership = Leadership.LEADER_ONLY
        lease_renewal_mode = LeaseRenewalMode.HEARTBEAT
        cadence_seconds = 10.0  # far above the 0.1s TTL-derived cap

        async def tick(self, ctx: ServiceContext) -> None:
            tick_started.set()
            await asyncio.sleep(5.0)  # far longer than the cap
            tick_finished["done"] = True  # must never be reached

    ctx = _make_ctx(engine=MagicMock())
    supervisor = BackgroundSupervisor()

    with patch(
        "dynastore.modules.db_config.locking_tools.lease_leadership_with_heartbeat",
        _fake_heartbeat_acquire,
    ):
        coro = supervisor._leader_elected_coro(_SlowHeartbeatPeriodic(), ctx)
        task = asyncio.create_task(coro)
        await asyncio.wait_for(tick_started.wait(), timeout=1.0)
        # Give the clamp (~0.1s) time to fire, well short of the tick's own
        # 5s sleep and the 10s cadence.
        await asyncio.sleep(0.4)
        ctx.shutdown.set()
        await asyncio.wait_for(task, timeout=2.0)

    assert tick_finished["done"] is False, (
        "the slow tick must have been cancelled by the lease-TTL clamp, "
        "not run to completion"
    )


@pytest.mark.asyncio
async def test_heartbeat_mode_tick_timeout_is_honored(monkeypatch) -> None:
    """A HEARTBEAT-mode service's own ``tick_timeout`` is honored, not
    silently ignored (#2900) — it must cancel a slow tick even when the
    lease-TTL cap alone would allow much more time."""
    import dynastore.modules.db_config.connection_health_config as chc

    monkeypatch.setattr(chc._leadership_config, "lease_ttl_seconds", 30.0)
    monkeypatch.setattr(chc._leadership_config, "lease_skew_margin_seconds", 5.0)
    # cap = 25s, far above tick_timeout below.

    lost = asyncio.Event()

    @asynccontextmanager
    async def _fake_heartbeat_acquire(engine: Any, key: Any, *, name: str = "leader"):
        yield (True, lost)

    tick_started = asyncio.Event()
    tick_finished = {"done": False}

    class _TimeoutHeartbeatPeriodic(PeriodicService):
        name = "timeout-heartbeat"
        leadership = Leadership.LEADER_ONLY
        lease_renewal_mode = LeaseRenewalMode.HEARTBEAT
        cadence_seconds = 30.0
        tick_timeout = 0.1  # far below the 25s cap

        async def tick(self, ctx: ServiceContext) -> None:
            tick_started.set()
            await asyncio.sleep(5.0)  # far longer than tick_timeout
            tick_finished["done"] = True  # must never be reached

    ctx = _make_ctx(engine=MagicMock())
    supervisor = BackgroundSupervisor()

    with patch(
        "dynastore.modules.db_config.locking_tools.lease_leadership_with_heartbeat",
        _fake_heartbeat_acquire,
    ):
        coro = supervisor._leader_elected_coro(_TimeoutHeartbeatPeriodic(), ctx)
        task = asyncio.create_task(coro)
        await asyncio.wait_for(tick_started.wait(), timeout=1.0)
        # Give tick_timeout (0.1s) time to fire.
        await asyncio.sleep(0.4)
        ctx.shutdown.set()
        await asyncio.wait_for(task, timeout=2.0)

    assert tick_finished["done"] is False, (
        "tick_timeout must be honored in HEARTBEAT mode, not silently ignored"
    )


# ---------------------------------------------------------------------------
# Regression: every class registered with BackgroundSupervisor.register() in
# production code must satisfy the full BackgroundService protocol (#3257)
# ---------------------------------------------------------------------------

# One entry per class passed to a `*_supervisor.register(...)` call anywhere
# in the codebase. BackgroundService is intentionally NOT @runtime_checkable
# (see its docstring) — a missing member is invisible to isinstance() and,
# before this test, was only caught by pyright at the register() call site.
# #3257 was exactly that: three implementors silently dropped
# lease_renewal_mode and pytest stayed green. Keep this list in sync with
# new registrations — add an entry whenever a new class is passed to
# BackgroundSupervisor.register().
_REGISTERED_BACKGROUND_SERVICES: list[tuple[str, str]] = [
    ("dynastore.modules.tasks.dispatcher", "DispatcherService"),
    ("dynastore.modules.tasks.tasks_module", "StuckPendingWarnerService"),
    ("dynastore.modules.tasks.tasks_module", "ProactiveSweepService"),
    ("dynastore.modules.tasks.tasks_module", "TaskRetentionService"),
    ("dynastore.modules.tasks.drain_spawner", "DrainSpawnerService"),
    ("dynastore.modules.tasks.capability_publisher", "CapabilityPublisherService"),
    ("dynastore.modules.tasks.registry.publisher", "RegistryHeartbeatService"),
    ("dynastore.modules.catalog.soft_delete_reaper", "SoftDeleteReaper"),
    ("dynastore.modules.catalog.maintenance_supervisor", "MaintenanceSupervisor"),
    ("dynastore.modules.catalog.log_drainer", "LogDrainer"),
    ("dynastore.modules.catalog.lifecycle_reaper", "LifecycleReaper"),
    ("dynastore.modules.db.db_contention_monitor", "DbContentionMonitor"),
    ("dynastore.modules.db.instance_liveness", "InstanceLivenessHeartbeat"),
    ("dynastore.modules.db.zombie_session_reaper", "ZombieSessionReaper"),
    ("dynastore.modules.scaling.publisher", "ScalingSignalPublisher"),
    ("dynastore.modules.gcp.liveness_reconciler", "GcpLivenessReconciler"),
    ("dynastore.modules.gcp.liveness_reconciler", "GcpLivenessBackstop"),
    ("dynastore.modules.gcp.scaling_reconciler", "GcpScalingReconciler"),
    ("dynastore.modules.scaling.monitoring_signal_provider", "MonitoringSignalProvider"),
    ("dynastore.modules.db_config.engine_instance_cache", "EngineInstanceCacheSweepService"),
    ("dynastore.modules.db_config.config_reload_service", "ConfigReloadService"),
    ("dynastore.modules.db_config.notification_hub", "NotificationHubService"),
    ("dynastore.modules.iam.compiled_rule_cache", "IamRuleCacheRefreshService"),
    ("dynastore.modules.iam.module", "IdentityProviderReconcileService"),
    ("dynastore.tools.memory_watchdog", "MemoryWatchdogService"),
    ("dynastore.main", "_ColdBootReconciliationService"),
]


@pytest.mark.parametrize(
    "module_path, class_name",
    _REGISTERED_BACKGROUND_SERVICES,
    ids=[name for _, name in _REGISTERED_BACKGROUND_SERVICES],
)
def test_registered_service_satisfies_background_service_protocol(
    module_path: str, class_name: str
) -> None:
    """Every registered BackgroundService implementor declares all protocol
    members at the class level, with the right type for each.

    Checked at the class (not instance) level: every implementor in this
    codebase declares name/leadership/pod_policy/lock_key/lease_renewal_mode
    as class attributes (directly, or inherited from PeriodicService), so a
    missing member is a class-level gap, not something that only appears
    after __init__ runs.
    """
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)

    assert isinstance(getattr(cls, "name", None), str), (
        f"{class_name}.name must be a str"
    )
    assert isinstance(getattr(cls, "leadership", None), Leadership), (
        f"{class_name}.leadership must be a Leadership"
    )
    assert isinstance(getattr(cls, "pod_policy", None), PodPolicy), (
        f"{class_name}.pod_policy must be a PodPolicy"
    )
    assert hasattr(cls, "lock_key"), f"{class_name} must declare lock_key"
    lock_key = getattr(cls, "lock_key")
    assert lock_key is None or isinstance(lock_key, (int, str)), (
        f"{class_name}.lock_key must be None, int, or str"
    )
    assert isinstance(getattr(cls, "lease_renewal_mode", None), LeaseRenewalMode), (
        f"{class_name}.lease_renewal_mode must be a LeaseRenewalMode — regression "
        "guard for #3257 (a bespoke, non-PeriodicService implementor must "
        "declare this explicitly; PeriodicService subclasses inherit PER_TICK "
        "automatically)"
    )
    assert asyncio.iscoroutinefunction(getattr(cls, "run", None)), (
        f"{class_name}.run must be an async method"
    )
