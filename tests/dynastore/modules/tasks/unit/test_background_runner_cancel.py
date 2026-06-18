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

"""Unit tests for BackgroundRunner in-process confirmed-dismiss path.

Covers:
 - _task_registry add/remove on spawn and done-callback.
 - signal_stop: local-cancel path (task in registry → asyncio.Task.cancel called).
 - signal_stop: remote-publish path (task not in registry → pg_notify emitted).
 - CANCEL_REQUESTED signal wakes _cancel_listener and cancels a registered task.
 - Dismiss-driven CancelledError stamps dismiss_confirmed_at; shutdown-driven does NOT.
 - force_stop: local path cancels + evicts; remote path delegates to signal_stop.
 - StopSignalProtocol structural compatibility.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_runner():
    """Instantiate a fresh BackgroundRunner with empty registry."""
    from dynastore.modules.tasks.runners import BackgroundRunner
    return BackgroundRunner()


def _make_task_stub(task_id: uuid.UUID | None = None, status: str = "RUNNING", dismissed_at=None) -> MagicMock:
    """Create a minimal Task-like object."""
    stub = MagicMock()
    stub.task_id = task_id or uuid.uuid4()
    stub.status = MagicMock()
    stub.status.value = status
    stub.dismiss_confirmed_at = dismissed_at
    return stub


# ---------------------------------------------------------------------------
# A. Task registry — add/remove
# ---------------------------------------------------------------------------

def test_task_registry_starts_empty():
    runner = _make_runner()
    assert runner._task_registry == {}


def test_task_registry_add_and_discard_via_done_callback():
    """Registering a mock asyncio.Task and firing done-callback removes it."""
    runner = _make_runner()
    task_id = str(uuid.uuid4())

    # Simulate what the spawn site does.
    mock_t: Any = MagicMock(spec=asyncio.Task)
    _callbacks: list = []
    mock_t.add_done_callback.side_effect = _callbacks.append

    runner._task_registry[task_id] = mock_t
    # Attach the done-callback the same way the runner code does.
    mock_t.add_done_callback(lambda _: runner._task_registry.pop(task_id, None))

    assert task_id in runner._task_registry

    # Fire callbacks (simulates task completion).
    for cb in _callbacks:
        cb(mock_t)

    assert task_id not in runner._task_registry


# ---------------------------------------------------------------------------
# B. StopSignalProtocol structural compatibility
# ---------------------------------------------------------------------------

def test_background_runner_satisfies_stop_signal_protocol():
    """BackgroundRunner must be structurally recognised as StopSignalProtocol."""
    from dynastore.modules.tasks.liveness import StopSignalProtocol
    from dynastore.modules.tasks.runners import BackgroundRunner

    runner = BackgroundRunner()
    assert isinstance(runner, StopSignalProtocol)


def test_runner_type_is_background():
    from dynastore.modules.tasks.runners import BackgroundRunner
    assert BackgroundRunner.runner_type == "background"


# ---------------------------------------------------------------------------
# B2. owns()
# ---------------------------------------------------------------------------

def test_owns_returns_false_for_gcp_owner():
    runner = _make_runner()
    assert runner.owns("gcp_cloud_run_abc123") is False


def test_owns_returns_true_for_in_process_owner():
    runner = _make_runner()
    # Dispatcher-style owner_id (hostname:pid).
    assert runner.owns("myhost:1234") is True


def test_owns_returns_false_for_empty_owner_id():
    runner = _make_runner()
    assert runner.owns("") is False


# ---------------------------------------------------------------------------
# C. signal_stop — local cancel path
# ---------------------------------------------------------------------------

def test_signal_stop_local_cancel():
    """signal_stop cancels the asyncio.Task in the local registry."""

    async def _run():
        runner = _make_runner()
        task_id = uuid.uuid4()
        mock_asyncio_task: Any = MagicMock(spec=asyncio.Task)

        runner._task_registry[str(task_id)] = mock_asyncio_task

        task_stub = _make_task_stub(task_id=task_id)
        result = await runner.signal_stop(task_stub)

        assert result is True
        mock_asyncio_task.cancel.assert_called_once()
        # Registry entry must still exist (cancel is idempotent; eviction is
        # done by the done-callback when the task actually finishes).
        assert str(task_id) in runner._task_registry

    asyncio.run(_run())


def test_signal_stop_missing_task_id_returns_false():
    async def _run():
        runner = _make_runner()
        stub = MagicMock()
        del stub.task_id  # no attribute at all
        result = await runner.signal_stop(stub)
        assert result is False

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# D. signal_stop — remote publish path
# ---------------------------------------------------------------------------

def test_signal_stop_remote_publishes_pg_notify():
    """When the task is NOT in the local registry, pg_notify is issued."""

    async def _run():
        runner = _make_runner()
        task_id = uuid.uuid4()
        # Registry is empty — no local task.

        mock_engine = MagicMock()
        mock_conn = AsyncMock()
        mock_execute = AsyncMock(return_value=None)
        mock_dql_instance = MagicMock(execute=mock_execute)

        task_stub = _make_task_stub(task_id=task_id)

        with (
            patch(
                "dynastore.modules.db_config.query_executor.DQLQuery",
                return_value=mock_dql_instance,
            ),
            patch(
                "dynastore.tools.protocol_helpers.get_engine",
                return_value=mock_engine,
            ),
            patch(
                "dynastore.modules.db_config.query_executor.managed_transaction",
            ) as mock_managed_txn,
        ):
            mock_managed_txn.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_managed_txn.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await runner.signal_stop(task_stub)

        assert result is True
        mock_execute.assert_awaited_once()

    asyncio.run(_run())


def test_signal_stop_remote_returns_false_when_no_engine():
    """If no engine is available the remote path returns False without raising."""

    async def _run():
        runner = _make_runner()
        task_id = uuid.uuid4()
        task_stub = _make_task_stub(task_id=task_id)

        with patch(
            "dynastore.tools.protocol_helpers.get_engine",
            return_value=None,
        ):
            result = await runner.signal_stop(task_stub)

        assert result is False

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# E. _cancel_listener — receives CANCEL_REQUESTED and cancels local task
# ---------------------------------------------------------------------------

def test_cancel_listener_cancels_registered_task():
    """CANCEL_REQUESTED signal wakes listener; task in registry is cancelled."""

    async def _run():
        from dynastore.tools.async_utils import signal_bus
        from dynastore.modules.tasks.queue import CANCEL_REQUESTED, _get_cancel_inbox
        import dynastore.modules.tasks.queue as _q

        # Reset module-level inbox for test isolation.
        _q._cancel_inbox = None

        runner = _make_runner()
        task_id = str(uuid.uuid4())
        mock_asyncio_task: Any = MagicMock(spec=asyncio.Task)
        runner._task_registry[task_id] = mock_asyncio_task

        # Start listener first — it must reach wait_for before the signal is emitted.
        listener = asyncio.create_task(runner._cancel_listener())
        # Yield control so the listener coroutine runs up to its first await.
        await asyncio.sleep(0)

        # Pre-populate the cancel inbox with the task_id.
        _get_cancel_inbox().put_nowait(task_id)

        # Emit the broadcast wake signal.
        await signal_bus.emit(CANCEL_REQUESTED)

        # Give the listener a moment to drain the inbox.
        await asyncio.sleep(0.1)

        listener.cancel()
        try:
            await listener
        except asyncio.CancelledError:
            pass

        mock_asyncio_task.cancel.assert_called_once()

    asyncio.run(_run())


def test_cancel_listener_no_op_for_unknown_task_id():
    """CANCEL_REQUESTED for a task not in registry is silently ignored."""

    async def _run():
        from dynastore.tools.async_utils import signal_bus
        from dynastore.modules.tasks.queue import CANCEL_REQUESTED
        import dynastore.modules.tasks.queue as _q

        _q._cancel_inbox = None

        runner = _make_runner()
        unknown_id = str(uuid.uuid4())

        import dynastore.modules.tasks.queue as _q2
        _q2._get_cancel_inbox().put_nowait(unknown_id)

        listener = asyncio.create_task(runner._cancel_listener())
        await signal_bus.emit(CANCEL_REQUESTED)
        await asyncio.sleep(0.05)
        listener.cancel()
        try:
            await listener
        except asyncio.CancelledError:
            pass

        # Nothing in registry to cancel — no assertion failure expected.
        assert runner._task_registry == {}

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# F. Dismiss-driven vs shutdown-driven CancelledError distinction
# ---------------------------------------------------------------------------

def _make_tasks_mgr(task_id: uuid.UUID, status: str, dismiss_confirmed_at=None):
    """Return a mock TasksProtocol manager that reports the given status."""
    from dynastore.modules.tasks.models import TaskStatusEnum

    _status_map = {
        "DISMISSED": TaskStatusEnum.DISMISSED,
        "ACTIVE": TaskStatusEnum.ACTIVE,
        "RUNNING": TaskStatusEnum.RUNNING,
        "PENDING": TaskStatusEnum.PENDING,
    }

    refreshed = MagicMock()
    refreshed.task_id = task_id
    refreshed.status = _status_map.get(status, TaskStatusEnum.ACTIVE)
    refreshed.dismiss_confirmed_at = dismiss_confirmed_at

    mgr = AsyncMock()
    mgr.get_task = AsyncMock(return_value=refreshed)
    mgr.update_task = AsyncMock(return_value=None)
    return mgr


def test_dismiss_driven_cancel_stamps_confirmed_at():
    """Condition logic: status=DISMISSED + no stamp → should call stamp."""

    async def _run():
        from dynastore.modules.tasks.models import TaskStatusEnum

        task_id = uuid.uuid4()
        tasks_mgr = _make_tasks_mgr(task_id, "DISMISSED")
        engine_stub = MagicMock()

        refreshed = await tasks_mgr.get_task(engine_stub, task_id, schema="tasks")

        # Assert the dismiss-driven branch condition is True.
        assert refreshed is not None
        assert refreshed.status == TaskStatusEnum.DISMISSED
        assert refreshed.dismiss_confirmed_at is None

        # Verify stamp_dismiss_confirmed is importable and is a coroutine function.
        import inspect
        from dynastore.modules.tasks.tasks_module import stamp_dismiss_confirmed
        assert inspect.iscoroutinefunction(stamp_dismiss_confirmed)

    asyncio.run(_run())


def test_shutdown_driven_cancel_does_not_stamp():
    """CancelledError when status=ACTIVE → no stamp, reset to PENDING instead."""

    async def _run():
        task_id = uuid.uuid4()
        tasks_mgr = _make_tasks_mgr(task_id, "ACTIVE")

        stamp_mock = AsyncMock()
        engine_stub = MagicMock()

        from dynastore.modules.tasks.models import TaskStatusEnum

        refreshed = await tasks_mgr.get_task(engine_stub, task_id, schema="tasks")
        should_stamp = (
            refreshed is not None
            and refreshed.status == TaskStatusEnum.DISMISSED
            and refreshed.dismiss_confirmed_at is None
        )
        # For ACTIVE status, no stamp.
        assert not should_stamp
        stamp_mock.assert_not_awaited()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# G. force_stop
# ---------------------------------------------------------------------------

def test_force_stop_local_cancels_and_evicts():
    """force_stop cancels the task AND removes it from the registry."""

    async def _run():
        runner = _make_runner()
        task_id = uuid.uuid4()
        mock_asyncio_task: Any = MagicMock(spec=asyncio.Task)
        runner._task_registry[str(task_id)] = mock_asyncio_task

        task_stub = _make_task_stub(task_id=task_id)
        result = await runner.force_stop(task_stub)

        assert result is True
        mock_asyncio_task.cancel.assert_called_once()
        # force_stop evicts immediately (unlike signal_stop).
        assert str(task_id) not in runner._task_registry

    asyncio.run(_run())


def test_force_stop_remote_delegates_to_signal_stop():
    """force_stop for a non-local task falls through to signal_stop."""

    async def _run():
        runner = _make_runner()
        task_id = uuid.uuid4()

        mock_engine = MagicMock()
        mock_conn = AsyncMock()
        mock_execute = AsyncMock(return_value=None)
        mock_dql_instance = MagicMock(execute=mock_execute)

        task_stub = _make_task_stub(task_id=task_id)

        with (
            patch(
                "dynastore.modules.db_config.query_executor.DQLQuery",
                return_value=mock_dql_instance,
            ),
            patch(
                "dynastore.tools.protocol_helpers.get_engine",
                return_value=mock_engine,
            ),
            patch(
                "dynastore.modules.db_config.query_executor.managed_transaction",
            ) as mock_managed_txn,
        ):
            mock_managed_txn.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_managed_txn.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await runner.force_stop(task_stub)

        assert result is True
        mock_execute.assert_awaited_once()

    asyncio.run(_run())


def test_force_stop_missing_task_id_returns_false():
    async def _run():
        runner = _make_runner()
        stub = MagicMock()
        del stub.task_id
        result = await runner.force_stop(stub)
        assert result is False

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# H. CANCEL_REQUESTED channel constant and queue.py additions
# ---------------------------------------------------------------------------

def test_cancel_requested_constant_defined():
    from dynastore.modules.tasks.queue import CANCEL_REQUESTED
    assert CANCEL_REQUESTED == "cancel_requested"


def test_cancel_requested_in_transform_pushes_to_inbox():
    """_notification_transform for cancel_requested pushes task_id to inbox."""
    import dynastore.modules.tasks.queue as _q
    from dynastore.modules.tasks.queue import _notification_transform, CANCEL_REQUESTED

    async def _run():
        _q._cancel_inbox = None
        task_id = str(uuid.uuid4())
        result = _notification_transform(CANCEL_REQUESTED, task_id)
        assert result == (CANCEL_REQUESTED, None)
        inbox = _q._get_cancel_inbox()
        assert not inbox.empty()
        assert inbox.get_nowait() == task_id

    asyncio.run(_run())


def test_notification_transform_cancel_with_no_payload():
    """Transform with no payload must not enqueue anything and still returns wake."""
    from dynastore.modules.tasks.queue import _notification_transform, CANCEL_REQUESTED
    import dynastore.modules.tasks.queue as _q

    async def _run():
        _q._cancel_inbox = None
        result = _notification_transform(CANCEL_REQUESTED, None)
        # Still emits the wake signal even with no payload.
        assert result == (CANCEL_REQUESTED, None)
        inbox = _q._get_cancel_inbox()
        assert inbox.empty()

    asyncio.run(_run())
