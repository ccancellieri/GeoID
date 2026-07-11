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

"""Unit tests asserting the dispatcher's inline terminal writes bind a live
completion timestamp to ``finished_at`` — not the claimed row's original
enqueue ``timestamp`` (the partition/creation time captured at claim). Binding
the stale enqueue time corrupts ``finished_at``-driven consumers: retention's
purge windows (``purge_completed_tasks`` / ``purge_dead_letter_tasks``) and
the DLQ requeue since-filter, all of which assume ``finished_at`` reflects
when the row actually reached its terminal state.
"""

from __future__ import annotations

import asyncio
import uuid as _uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.tasks import dispatcher as dispatcher_mod
from dynastore.modules.tasks import execution as execution_mod
from dynastore.modules.tasks.models import PermanentTaskFailure

# An old enqueue timestamp captured at claim time — if a terminal write ever
# binds this to finished_at, purge/requeue windows computed off finished_at
# would be wrong (a just-completed row would look weeks old).
_STALE_ENQUEUE_TS = datetime(2020, 1, 1, tzinfo=timezone.utc)


def _claimed_row(task_id: Any, task_type: str = "noop_task") -> Dict[str, Any]:
    return {
        "task_id": task_id,
        "task_type": task_type,
        "catalog_id": "tasks",
        "scope": None,
        "caller_id": "system",
        "inputs": {},
        "collection_id": None,
        "execution_mode": "ASYNCHRONOUS",
        "retry_count": 0,
        "max_retries": 3,
        "dedup_key": None,
        "timestamp": _STALE_ENQUEUE_TS,
        "owner_id": dispatcher_mod._RUNNER_ID,
    }


async def _run_one_batch(
    row: Dict[str, Any],
    *,
    dispatch_side_effect: Optional[BaseException] = None,
    dispatch_return_value: Any = "ok",
) -> Dict[str, AsyncMock]:
    shutdown = asyncio.Event()
    call_count = {"n": 0}

    async def fake_claim_batch(*_a, **_kw):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return [row]
        shutdown.set()
        return []

    async def fake_wait_for(*_a, **_kw):
        return True

    heartbeat = AsyncMock()
    complete_mock = AsyncMock(return_value=True)
    fail_mock = AsyncMock(return_value=True)
    dead_letter_mock = AsyncMock(return_value=True)

    default_terminal = execution_mod.RoutingTerminal(
        on_success=None, on_failure=None, on_timeout=None, timeout_seconds=None,
    )

    if dispatch_side_effect is not None:
        dispatch_mock = AsyncMock(side_effect=dispatch_side_effect)
    else:
        dispatch_mock = AsyncMock(return_value=dispatch_return_value)

    with (
        patch.object(dispatcher_mod, "BatchedHeartbeat", return_value=heartbeat),
        patch.object(dispatcher_mod.signal_bus, "wait_for", side_effect=fake_wait_for),
        patch("dynastore.modules.tasks.tasks_module.claim_batch", side_effect=fake_claim_batch),
        patch("dynastore.modules.tasks.tasks_module.complete_task", complete_mock),
        patch("dynastore.modules.tasks.tasks_module.fail_task", fail_mock),
        patch("dynastore.modules.tasks.tasks_module.dead_letter_task", dead_letter_mock),
        patch("dynastore.modules.tasks.tasks_module.reset_task_to_pending", AsyncMock()),
        patch("dynastore.modules.tasks.runners.capability_map") as cap_map,
        patch("dynastore.tasks.get_task_instance", return_value=None),
        patch.object(
            execution_mod, "resolve_routing_terminal",
            AsyncMock(return_value=default_terminal),
        ),
        patch.object(execution_mod, "apply_terminal_action", AsyncMock()),
        patch.object(execution_mod.execution_engine, "dispatch", dispatch_mock),
    ):
        cap_map.refresh = AsyncMock()
        cap_map.async_types = [row["task_type"]]
        cap_map.sync_types = []

        await asyncio.wait_for(
            dispatcher_mod.run_dispatcher(
                engine=object(),
                schema=None,
                shutdown_event=shutdown,
                signal_timeout=0.01,
            ),
            timeout=5.0,
        )

    return {"complete": complete_mock, "fail": fail_mock, "dead_letter": dead_letter_mock}


def _assert_live_now(finished_at: datetime) -> None:
    assert finished_at != _STALE_ENQUEUE_TS, (
        "terminal write must not bind the claimed row's original enqueue "
        "timestamp to finished_at."
    )
    assert (datetime.now(timezone.utc) - finished_at).total_seconds() < 10


@pytest.mark.asyncio
async def test_dispatcher_complete_task_binds_live_now():
    task_id = _uuid.uuid4()
    row = _claimed_row(task_id)

    mocks = await _run_one_batch(row, dispatch_return_value={"ok": True})

    mocks["complete"].assert_awaited_once()
    _assert_live_now(mocks["complete"].await_args.args[2])


@pytest.mark.asyncio
async def test_dispatcher_fail_task_binds_live_now_on_exception():
    task_id = _uuid.uuid4()
    row = _claimed_row(task_id)

    mocks = await _run_one_batch(row, dispatch_side_effect=RuntimeError("boom"))

    mocks["fail"].assert_awaited_once()
    _assert_live_now(mocks["fail"].await_args.args[2])


@pytest.mark.asyncio
async def test_dispatcher_fail_task_binds_live_now_on_permanent_failure():
    task_id = _uuid.uuid4()
    row = _claimed_row(task_id)

    mocks = await _run_one_batch(
        row, dispatch_side_effect=PermanentTaskFailure("no retries"),
    )

    mocks["fail"].assert_awaited_once()
    _assert_live_now(mocks["fail"].await_args.args[2])


def test_dispatcher_terminal_writes_bind_live_now_grep_guard():
    """Source-level guard covering all 5 inline terminal-write call sites in
    ``run_dispatcher``: each must pass ``datetime.now(timezone.utc)``, not
    the claimed row's ``timestamp`` local, as the finished_at-binding arg."""
    import inspect

    source = inspect.getsource(dispatcher_mod.run_dispatcher)
    lines = source.splitlines()
    call_sites = [
        i for i, ln in enumerate(lines)
        if any(
            f"await {fn}(" in ln
            for fn in ("complete_task", "fail_task", "dead_letter_task")
        )
    ]
    assert len(call_sites) == 5, (
        f"expected 5 terminal-write call sites in run_dispatcher, found "
        f"{len(call_sites)}: {[lines[i] for i in call_sites]}"
    )
    for idx in call_sites:
        window = "\n".join(lines[idx:idx + 3])
        assert "datetime.now(timezone.utc)" in window, (
            f"run_dispatcher:{idx + 1}: terminal write must bind a live "
            f"completion timestamp, not the claimed row's stale enqueue "
            f"`timestamp`. Window:\n{window}"
        )
        assert "engine, task_id, timestamp," not in window, (
            f"run_dispatcher:{idx + 1}: terminal write is still binding the "
            f"claimed row's stale enqueue `timestamp` to finished_at. "
            f"Window:\n{window}"
        )
