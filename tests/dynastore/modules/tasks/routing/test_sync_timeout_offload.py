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

"""Inline-sync timeout offload (#2221).

When a process runs in-process via ``Prefer: respond-sync`` and exceeds its
routing deadline, the sync runner degrades gracefully: it dead-letters the
audit task and applies the configured ``on_timeout`` action. When that action
is ROUTE (e.g. re-dispatch gdal to its Cloud Run target), the offload task is
enqueued and returned so the HTTP layer answers 201 + Location instead of
blocking the request. With any other ``on_timeout`` policy the timeout surfaces
as an error.

Covers:
1. ``apply_terminal_action`` returns the routed Task on ROUTE, ``None`` otherwise.
2. ``SyncRunner.run`` timeout + ``on_timeout=route`` → dead-letters and returns
   the offload Task (→ 201 path).
3. ``SyncRunner.run`` timeout + ``on_timeout=dead_letter`` (no offload) → raises.
4. ``SyncRunner.run`` fast path (no timeout) still returns the inline result.

All DB-free: DB helpers, routing resolver and ``apply_terminal_action`` are
mocked. Run with ``--noconftest`` to skip the live-database conftest.
"""

from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest


# ---------------------------------------------------------------------------
# Group 1 — apply_terminal_action return value
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_terminal_action_returns_routed_task_on_route(monkeypatch):
    from dynastore.modules.tasks import execution as exec_mod
    from dynastore.modules.tasks import tasks_module
    from dynastore.modules.tasks.routing.model import Action, ActionVerb

    monkeypatch.setattr(exec_mod, "_drain_provisioning_checklist", AsyncMock())
    monkeypatch.setattr(exec_mod, "_read_task_status", AsyncMock(return_value="DEAD_LETTER"))
    routed = SimpleNamespace(task_id=uuid.uuid4())
    monkeypatch.setattr(tasks_module, "create_task", AsyncMock(return_value=routed))

    result = await exec_mod.apply_terminal_action(
        MagicMock(),
        task_id=uuid.uuid4(),
        task_type="gdal",
        inputs={"asset_id": "a1"},
        caller_id="u",
        collection_id=None,
        schema="tasks",
        scope=None,
        outcome="timeout",
        action=Action(action=ActionVerb.ROUTE, process="gdal"),
    )
    assert result is routed


@pytest.mark.asyncio
async def test_apply_terminal_action_returns_none_when_not_route(monkeypatch):
    from dynastore.modules.tasks import execution as exec_mod
    from dynastore.modules.tasks.routing.model import Action, ActionVerb

    monkeypatch.setattr(exec_mod, "_drain_provisioning_checklist", AsyncMock())

    result = await exec_mod.apply_terminal_action(
        MagicMock(),
        task_id=uuid.uuid4(),
        task_type="gdal",
        inputs={},
        caller_id="u",
        collection_id=None,
        schema="tasks",
        scope=None,
        outcome="timeout",
        action=Action(action=ActionVerb.DEAD_LETTER),
    )
    assert result is None


# ---------------------------------------------------------------------------
# Group 2 — SyncRunner.run timeout offload
# ---------------------------------------------------------------------------


def _sync_context():
    return SimpleNamespace(
        engine=MagicMock(),
        task_type="gdal",
        caller_id="user@example.com",
        inputs={"asset_id": "a1"},
        collection_id="col",
        db_schema="tasks",
        dedup_key=None,
        asset=None,
    )


def _routing_terminal(*, timeout_seconds, on_timeout):
    from dynastore.modules.tasks.routing.model import Action, ActionVerb

    return SimpleNamespace(
        timeout_seconds=timeout_seconds,
        on_success=Action(action=ActionVerb.REPORT),
        on_failure=Action(action=ActionVerb.DEAD_LETTER),
        on_timeout=on_timeout,
    )


def _patch_sync_deps(monkeypatch, *, terminal, task_run_coro, apply_ta_return,
                     job_task_id):
    """Patch the helpers SyncRunner.run imports lazily (at their source modules)
    plus the TasksProtocol resolve. Returns the mock namespace."""
    from dynastore.modules.tasks import runners as runners_mod
    from dynastore.modules.tasks import execution as exec_mod
    from dynastore.modules.tasks import tasks_module

    # TasksProtocol manager (resolve(TasksProtocol) inside run()).
    tasks_mgr = MagicMock()
    tasks_mgr.create_task = AsyncMock(
        return_value=SimpleNamespace(task_id=job_task_id)
    )
    tasks_mgr.update_task = AsyncMock(return_value=True)
    monkeypatch.setattr(
        "dynastore.tools.protocol_helpers.resolve",
        lambda _proto: tasks_mgr,
    )

    # In-process task instance lookup + payload hydration.
    task_instance = MagicMock()
    task_instance.run = MagicMock(side_effect=task_run_coro)
    monkeypatch.setattr(runners_mod, "get_task_instance", lambda _t: task_instance)
    monkeypatch.setattr(
        "dynastore.tasks.hydrate_task_payload",
        lambda inst, raw: SimpleNamespace(inputs=raw.get("inputs", {})),
    )

    # Routing terminal + terminal action + dead-letter.
    monkeypatch.setattr(exec_mod, "resolve_routing_terminal", AsyncMock(return_value=terminal))
    apply_ta = AsyncMock(return_value=apply_ta_return)
    monkeypatch.setattr(exec_mod, "apply_terminal_action", apply_ta)
    dead_letter = AsyncMock(return_value=True)
    monkeypatch.setattr(tasks_module, "dead_letter_task", dead_letter)
    # normalize_task_result only matters on the success path.
    monkeypatch.setattr(
        "dynastore.tasks.report.normalize_task_result",
        lambda result: (result if isinstance(result, dict) else {}, None),
    )

    return SimpleNamespace(
        tasks_mgr=tasks_mgr,
        task_instance=task_instance,
        apply_ta=apply_ta,
        dead_letter=dead_letter,
    )


@pytest.mark.asyncio
async def test_sync_timeout_with_route_returns_offload_task(monkeypatch):
    """Slow sync run + on_timeout=route → dead-letter + return the offload Task."""
    from dynastore.modules.tasks.routing.model import Action, ActionVerb

    offload = SimpleNamespace(task_id=uuid.uuid4())
    terminal = _routing_terminal(
        timeout_seconds=0.01,
        on_timeout=Action(action=ActionVerb.ROUTE, process="gdal"),
    )

    async def _slow_run(_payload):
        await asyncio.sleep(10)
        return {}

    mocks = _patch_sync_deps(
        monkeypatch, terminal=terminal, task_run_coro=_slow_run,
        apply_ta_return=offload, job_task_id=uuid.uuid4(),
    )

    from dynastore.modules.tasks.runners import SyncRunner

    result = await SyncRunner().run(_sync_context())

    assert result is offload
    mocks.dead_letter.assert_awaited_once()
    mocks.apply_ta.assert_awaited_once()
    _, kwargs = mocks.apply_ta.await_args
    assert kwargs["outcome"] == "timeout"
    assert kwargs["action"].action is ActionVerb.ROUTE


@pytest.mark.asyncio
async def test_sync_timeout_without_offload_raises(monkeypatch):
    """Slow sync run + on_timeout=dead_letter (no offload) → TimeoutError."""
    from dynastore.modules.tasks.routing.model import Action, ActionVerb

    terminal = _routing_terminal(
        timeout_seconds=0.01,
        on_timeout=Action(action=ActionVerb.DEAD_LETTER),
    )

    async def _slow_run(_payload):
        await asyncio.sleep(10)
        return {}

    mocks = _patch_sync_deps(
        monkeypatch, terminal=terminal, task_run_coro=_slow_run,
        apply_ta_return=None, job_task_id=uuid.uuid4(),
    )

    from dynastore.modules.tasks.runners import SyncRunner

    with pytest.raises(TimeoutError):
        await SyncRunner().run(_sync_context())
    mocks.dead_letter.assert_awaited_once()


@pytest.mark.asyncio
async def test_sync_fast_path_returns_inline_result(monkeypatch):
    """Fast run under the deadline → inline result returned, no offload."""
    from dynastore.modules.tasks.routing.model import Action, ActionVerb

    terminal = _routing_terminal(
        timeout_seconds=5.0,
        on_timeout=Action(action=ActionVerb.ROUTE, process="gdal"),
    )

    async def _fast_run(_payload):
        return {"bands": 3}

    mocks = _patch_sync_deps(
        monkeypatch, terminal=terminal, task_run_coro=_fast_run,
        apply_ta_return=None, job_task_id=uuid.uuid4(),
    )

    from dynastore.modules.tasks.runners import SyncRunner

    result = await SyncRunner().run(_sync_context())

    assert result == {"bands": 3}
    mocks.dead_letter.assert_not_awaited()
    mocks.apply_ta.assert_not_awaited()
