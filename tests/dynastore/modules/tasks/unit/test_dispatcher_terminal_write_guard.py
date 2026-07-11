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

"""Unit tests for the dispatcher's inline terminal-write owner_id race guard:

``complete_task`` / ``fail_task`` / ``dead_letter_task`` calls made directly
from ``run_dispatcher``'s ``_dispatch_one`` closure must guard on
``owner_id=_RUNNER_ID`` — a stale dispatcher instance (e.g. one whose
in-flight claim was reclaimed by the pg_cron reaper and re-claimed by a
fresh worker) must not clobber the fresh attempt's row. A ``False``
guarded-write return must log a clear warning, not be silently dropped.
"""

from __future__ import annotations

import asyncio
import logging
import uuid as _uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.tasks import dispatcher as dispatcher_mod
from dynastore.modules.tasks import execution as execution_mod
from dynastore.modules.tasks.models import PermanentTaskFailure


# A stale enqueue timestamp: if a terminal write ever binds this to
# finished_at, purge/requeue windows computed off finished_at would be wrong.
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
    complete_return_value: bool = True,
    fail_return_value: bool = True,
) -> Dict[str, AsyncMock]:
    """Drive ``run_dispatcher`` through exactly one ``claim_batch`` cycle
    carrying ``row``, then shut it down. Returns the terminal-write mocks
    used so callers can assert on the calls made against them.
    """
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
    complete_mock = AsyncMock(return_value=complete_return_value)
    fail_mock = AsyncMock(return_value=fail_return_value)
    dead_letter_mock = AsyncMock(return_value=True)
    apply_terminal_action_mock = AsyncMock()

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
        patch.object(execution_mod, "apply_terminal_action", apply_terminal_action_mock),
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

    return {
        "complete": complete_mock,
        "fail": fail_mock,
        "dead_letter": dead_letter_mock,
        "apply_terminal_action": apply_terminal_action_mock,
    }


@pytest.mark.asyncio
async def test_dispatcher_complete_task_passes_owner_guard():
    task_id = _uuid.uuid4()
    row = _claimed_row(task_id)

    mocks = await _run_one_batch(row, dispatch_return_value={"ok": True})

    mocks["complete"].assert_awaited_once()
    call = mocks["complete"].await_args
    assert call.args[1] == task_id
    assert call.kwargs.get("owner_id") == dispatcher_mod._RUNNER_ID


@pytest.mark.asyncio
async def test_dispatcher_lost_terminal_write_race_logs_warning(caplog):
    task_id = _uuid.uuid4()
    row = _claimed_row(task_id)

    caplog.set_level(logging.WARNING)
    await _run_one_batch(
        row, dispatch_return_value={"ok": True}, complete_return_value=False,
    )

    assert any(
        "lost terminal-write race" in r.message
        and r.name == "dynastore.modules.tasks.dispatcher"
        for r in caplog.records
    ), (
        f"expected a lost-race warning; got: "
        f"{[(r.levelname, r.name, r.getMessage()) for r in caplog.records]}"
    )


@pytest.mark.asyncio
async def test_dispatcher_fail_task_passes_owner_guard_on_exception():
    task_id = _uuid.uuid4()
    row = _claimed_row(task_id)

    mocks = await _run_one_batch(row, dispatch_side_effect=RuntimeError("boom"))

    mocks["fail"].assert_awaited_once()
    call = mocks["fail"].await_args
    assert call.args[1] == task_id
    assert call.kwargs.get("owner_id") == dispatcher_mod._RUNNER_ID
    assert call.kwargs.get("retry") is True


@pytest.mark.asyncio
async def test_dispatcher_fail_task_passes_owner_guard_on_permanent_failure():
    task_id = _uuid.uuid4()
    row = _claimed_row(task_id)

    mocks = await _run_one_batch(
        row, dispatch_side_effect=PermanentTaskFailure("no retries"),
    )

    mocks["fail"].assert_awaited_once()
    call = mocks["fail"].await_args
    assert call.kwargs.get("owner_id") == dispatcher_mod._RUNNER_ID
    assert call.kwargs.get("retry") is False


# ---------------------------------------------------------------------------
# #3264 — follow-on ROUTE actions must be gated on winning the terminal
# write's owner_id CAS, not fired unconditionally after a lost race.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatcher_success_race_lost_skips_follow_on_action():
    """A stale dispatcher attempt whose ``complete_task`` lost the owner_id
    race (row already reclaimed and completed by a fresh attempt) must NOT
    call ``apply_terminal_action`` — the winning attempt already fired its
    own follow-on off the fresh row; firing it again here would enqueue a
    duplicate ROUTE task."""
    task_id = _uuid.uuid4()
    row = _claimed_row(task_id)

    mocks = await _run_one_batch(
        row, dispatch_return_value={"ok": True}, complete_return_value=False,
    )

    mocks["apply_terminal_action"].assert_not_awaited()


@pytest.mark.asyncio
async def test_dispatcher_success_race_won_fires_follow_on_action():
    """The attempt that actually wins the ``complete_task`` CAS must still
    fire its follow-on action exactly once."""
    task_id = _uuid.uuid4()
    row = _claimed_row(task_id)

    mocks = await _run_one_batch(
        row, dispatch_return_value={"ok": True}, complete_return_value=True,
    )

    mocks["apply_terminal_action"].assert_awaited_once()
    assert mocks["apply_terminal_action"].await_args.kwargs.get("outcome") == "success"


@pytest.mark.asyncio
async def test_dispatcher_transient_failure_race_lost_skips_follow_on_action():
    """Same guard on the failure path: a stale attempt whose ``fail_task``
    lost the owner_id race must not fire ``apply_terminal_action`` either,
    even though ``apply_terminal_action`` itself re-reads ground truth for
    failure/timeout outcomes — the ground-truth status may already be
    DEAD_LETTER/FAILED from the winning attempt, which would otherwise
    fire a second duplicate ROUTE for this same outcome."""
    task_id = _uuid.uuid4()
    row = _claimed_row(task_id)

    mocks = await _run_one_batch(
        row, dispatch_side_effect=RuntimeError("boom"), fail_return_value=False,
    )

    mocks["apply_terminal_action"].assert_not_awaited()


def test_dispatcher_terminal_writes_pass_owner_guard_grep_guard():
    """Source-level guard covering all 5 inline terminal-write call sites in
    ``run_dispatcher`` (the nested ``_dispatch_one`` closure isn't
    independently addressable via ``inspect``, so this scans the whole
    function's source)."""
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
        window = "\n".join(lines[idx:idx + 6])
        assert "owner_id=_RUNNER_ID" in window, (
            f"run_dispatcher:{idx + 1}: terminal write missing the owner_id "
            f"race guard. Window:\n{window}"
        )


def test_dispatcher_follow_on_actions_gated_on_terminal_write_result():
    """Source-level guard (#3264): every ``apply_terminal_action`` call in
    ``run_dispatcher`` must be nested under an ``if`` guarded on the boolean
    the preceding terminal write returned — a stale attempt that lost the
    owner_id race must not fire a duplicate follow-on ROUTE task on top of
    the winning attempt's own call."""
    import inspect

    source = inspect.getsource(dispatcher_mod.run_dispatcher)
    lines = source.splitlines()
    call_sites = [
        i for i, ln in enumerate(lines) if "await apply_terminal_action(" in ln
    ]
    assert len(call_sites) == 4, (
        f"expected 4 apply_terminal_action call sites in run_dispatcher, "
        f"found {len(call_sites)}: {[lines[i] for i in call_sites]}"
    )
    for idx in call_sites:
        call_indent = len(lines[idx]) - len(lines[idx].lstrip())
        guarded = False
        for back in range(idx - 1, -1, -1):
            stripped = lines[back].strip()
            if not stripped:
                continue
            indent = len(lines[back]) - len(lines[back].lstrip())
            if indent < call_indent:
                guarded = stripped.startswith("if ")
                break
        assert guarded, (
            f"run_dispatcher:{idx + 1}: apply_terminal_action call is not "
            f"gated on the preceding terminal write's CAS result. "
            f"Line: {lines[idx]!r}"
        )
