"""Regression test: the durable outbox consumer must drain with scope="ALL".

The publish path writes lowercase EventScope values ("platform", "catalog",
"collection", "item", "asset") to the ``scope`` column.  The claim query in
events_module.py is case-sensitive and supports the wholesale sentinel "ALL"
(matches every row regardless of scope).  Passing "PLATFORM" as the drain
scope means the consumer only sees rows whose scope is literally "PLATFORM" —
which is never written — so every row stays PENDING forever.

These tests pin the invariant across three layers:
  1. ``_consume_shard`` forwards whatever scope it receives to the driver.
  2. ``_run_consume_loop`` default scope is "ALL" (not "PLATFORM").
  3. ``start_consumer`` default scope reaches ``_run_consume_loop`` as "ALL".
"""

from __future__ import annotations

import asyncio
from typing import List

from dynastore.modules.catalog.event_service import EventService


class _ShutdownAfter:
    """Signals shutdown after exactly N iterations (allows N loop body executions)."""

    def __init__(self, after_iters: int) -> None:
        self._remaining = after_iters

    def is_set(self) -> bool:
        if self._remaining <= 0:
            return True
        self._remaining -= 1
        return False


def _service() -> EventService:
    svc = EventService.__new__(EventService)
    svc._consumer_running = False
    svc._consumer_task = None
    return svc


class _RecordingDriver:
    """Fake EventDriverProtocol that records the scope passed to consume_batch."""

    def __init__(self) -> None:
        self.recorded_scopes: List[str] = []

    async def consume_batch(
        self, *, scope: str, batch_size: int = 100, shard: int = 0
    ) -> list:
        self.recorded_scopes.append(scope)
        return []  # empty batch triggers wait_for_events path

    async def wait_for_events(self, timeout: float = 10.0) -> None:
        pass  # return immediately so the test doesn't sleep

    async def ack(self, event_ids: list) -> None:
        pass

    async def nack(self, event_id: str, error: str) -> None:
        pass


# ---------------------------------------------------------------------------
# Layer 1: _consume_shard forwards scope unchanged to driver.consume_batch
# ---------------------------------------------------------------------------

async def test_consume_shard_forwards_all_scope_to_driver(monkeypatch):
    """_consume_shard must pass scope="ALL" through to driver.consume_batch.

    This tests the bottom-of-stack forwarding.  When called with scope="ALL"
    the driver must receive "ALL" — not any transformation of it.
    """
    svc = _service()
    driver = _RecordingDriver()

    monkeypatch.setattr(
        "dynastore.modules.catalog.event_service.get_protocol",
        lambda _: driver,
    )

    # Allow one iteration before shutdown so consume_batch is called once.
    await svc._consume_shard(
        shard_id=0,
        shutdown_event=_ShutdownAfter(after_iters=1),
        scope="ALL",
    )

    assert driver.recorded_scopes == ["ALL"], (
        f"_consume_shard must forward scope='ALL' to driver.consume_batch unchanged. "
        f"Got {driver.recorded_scopes!r}"
    )


# ---------------------------------------------------------------------------
# Layer 2: _run_consume_loop default scope is "ALL"
# ---------------------------------------------------------------------------

async def test_run_consume_loop_default_scope_is_all(monkeypatch):
    """_run_consume_loop's default scope parameter must be "ALL".

    This is the primary regression guard.  With scope="PLATFORM" (old default)
    the claim query ``AND (:scope = 'ALL' OR scope = :scope)`` evaluates to
    ``'platform' = 'PLATFORM'`` = FALSE for every row, so the consumer returns
    zero events forever.

    This test would FAIL against the old default of "PLATFORM".
    """
    svc = _service()

    # Capture scopes forwarded to shards without running 16 real tasks.
    captured: List[str] = []

    async def _fake_consume_shard(shard_id, shutdown_event, *, scope):
        captured.append(scope)

    monkeypatch.setattr(svc, "_consume_shard", _fake_consume_shard)

    # Call _run_consume_loop WITHOUT specifying scope to exercise the default.
    await svc._run_consume_loop(_ShutdownAfter(after_iters=0))

    assert len(captured) == 16, f"Expected 16 shard tasks, got {len(captured)}"
    assert all(s == "ALL" for s in captured), (
        f"_run_consume_loop default scope must be 'ALL' (wholesale drain sentinel). "
        f"Got scopes={captured!r}. "
        f"With 'PLATFORM' the consumer matches zero rows because publish writes "
        f"lowercase EventScope values ('platform', 'catalog', etc.)."
    )


# ---------------------------------------------------------------------------
# Layer 3: start_consumer default scope flows through to _run_consume_loop
# ---------------------------------------------------------------------------

async def test_start_consumer_default_scope_reaches_run_consume_loop(monkeypatch):
    """start_consumer default scope must arrive at _run_consume_loop as "ALL".

    catalog_module.py calls ``self.event_service.start_consumer(shutdown)``
    with no explicit scope.  This test ensures the default propagates correctly
    through the leader-elected background task.
    """
    svc = _service()

    captured_scopes: List[str] = []

    async def _fake_run_loop(shutdown_event, *, scope, channels=None):
        captured_scopes.append(scope)

    monkeypatch.setattr(svc, "_run_consume_loop", _fake_run_loop)

    # Stub get_background_executor to run the coroutine inline in the test loop.
    class _InlineExecutor:
        def submit(self, coro, *, task_name=""):
            # Schedule the coroutine on the current event loop and return a task.
            return asyncio.ensure_future(coro)

    monkeypatch.setattr(
        "dynastore.modules.catalog.event_service.get_background_executor",
        lambda: _InlineExecutor(),
    )

    # Stub run_leader_loop (imported locally in start_consumer) to call on_leader once.
    async def _fake_run_leader_loop(
        *, acquire_leadership, on_leader, name, cadence_seconds, is_shutdown
    ):
        await on_leader()

    monkeypatch.setattr(
        "dynastore.tools.async_utils.run_leader_loop",
        _fake_run_leader_loop,
    )

    # Also stub get_protocol so acquire_consumer_lock doesn't fail.
    class _FakeEventDriver:
        async def acquire_consumer_lock(self, key):
            from contextlib import asynccontextmanager

            @asynccontextmanager
            async def _cm():
                yield True

            return _cm()

    monkeypatch.setattr(
        "dynastore.modules.catalog.event_service.get_protocol",
        lambda _: _FakeEventDriver(),
    )

    shutdown = _ShutdownAfter(after_iters=0)
    await svc.start_consumer(shutdown)

    # Give the background task a chance to execute.
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert captured_scopes == ["ALL"], (
        f"start_consumer default scope must be 'ALL'. "
        f"Got {captured_scopes!r}. "
        f"With 'PLATFORM' no lowercase-scoped events would ever be drained."
    )
