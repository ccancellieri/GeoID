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

"""Unit tests for ``ExecutionEngine.run_ephemeral``'s terminal-write owner_id
race guard: ``complete_task`` / ``fail_task`` must guard on the
``ephemeral-{task_id}`` owner_id stamped by ``claim_by_id`` at the top of the
method, so a stale run cannot clobber a row a Cloud Run offload re-owned via
``claim_for_dispatch`` in the interim. A ``False`` guarded-write return must
log a clear warning, not be silently dropped.
"""

from __future__ import annotations

import logging
import uuid as _uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tasks.execution import ExecutionEngine
from dynastore.modules.tasks.models import PermanentTaskFailure


def _fake_row(task_id) -> dict:
    return {
        "task_id": task_id,
        "task_type": "t",
        "timestamp": datetime(2020, 1, 1, tzinfo=timezone.utc),
    }


@pytest.mark.asyncio
async def test_run_ephemeral_complete_task_passes_owner_guard():
    task_id = _uuid.uuid4()
    fake_task = MagicMock()
    fake_task.task_id = task_id
    row = _fake_row(task_id)

    engine = ExecutionEngine()
    complete_mock = AsyncMock(return_value=True)

    with (
        patch("dynastore.modules.tasks.tasks_module.create_task", AsyncMock(return_value=fake_task)),
        patch("dynastore.modules.tasks.tasks_module.claim_by_id", AsyncMock(return_value=row)),
        patch("dynastore.modules.tasks.tasks_module.complete_task", complete_mock),
        patch.object(engine, "dispatch", AsyncMock(return_value={"ok": True})),
    ):
        result = await engine.run_ephemeral(MagicMock(), "tasks", engine=MagicMock())

    assert result == {"ok": True}
    complete_mock.assert_awaited_once()
    call = complete_mock.await_args
    assert call.args[1] == task_id
    assert call.kwargs.get("owner_id") == f"ephemeral-{task_id}"


@pytest.mark.asyncio
async def test_run_ephemeral_lost_terminal_write_race_logs_warning(caplog):
    task_id = _uuid.uuid4()
    fake_task = MagicMock()
    fake_task.task_id = task_id
    row = _fake_row(task_id)

    engine = ExecutionEngine()

    caplog.set_level(logging.WARNING)
    with (
        patch("dynastore.modules.tasks.tasks_module.create_task", AsyncMock(return_value=fake_task)),
        patch("dynastore.modules.tasks.tasks_module.claim_by_id", AsyncMock(return_value=row)),
        patch("dynastore.modules.tasks.tasks_module.complete_task", AsyncMock(return_value=False)),
        patch.object(engine, "dispatch", AsyncMock(return_value={"ok": True})),
    ):
        await engine.run_ephemeral(MagicMock(), "tasks", engine=MagicMock())

    assert any(
        "lost terminal-write race" in r.message
        and r.name == "dynastore.modules.tasks.execution"
        for r in caplog.records
    ), (
        f"expected a lost-race warning; got: "
        f"{[(r.levelname, r.name, r.getMessage()) for r in caplog.records]}"
    )


@pytest.mark.asyncio
async def test_run_ephemeral_fail_task_passes_owner_guard_on_exception():
    task_id = _uuid.uuid4()
    fake_task = MagicMock()
    fake_task.task_id = task_id
    row = _fake_row(task_id)

    engine = ExecutionEngine()
    fail_mock = AsyncMock(return_value=True)

    with (
        patch("dynastore.modules.tasks.tasks_module.create_task", AsyncMock(return_value=fake_task)),
        patch("dynastore.modules.tasks.tasks_module.claim_by_id", AsyncMock(return_value=row)),
        patch("dynastore.modules.tasks.tasks_module.fail_task", fail_mock),
        patch.object(engine, "dispatch", AsyncMock(side_effect=RuntimeError("boom"))),
    ):
        with pytest.raises(RuntimeError):
            await engine.run_ephemeral(MagicMock(), "tasks", engine=MagicMock())

    fail_mock.assert_awaited_once()
    call = fail_mock.await_args
    assert call.args[1] == task_id
    assert call.kwargs.get("owner_id") == f"ephemeral-{task_id}"
    assert call.kwargs.get("retry") is True


@pytest.mark.asyncio
async def test_run_ephemeral_fail_task_passes_owner_guard_on_permanent_failure():
    task_id = _uuid.uuid4()
    fake_task = MagicMock()
    fake_task.task_id = task_id
    row = _fake_row(task_id)

    engine = ExecutionEngine()
    fail_mock = AsyncMock(return_value=True)

    with (
        patch("dynastore.modules.tasks.tasks_module.create_task", AsyncMock(return_value=fake_task)),
        patch("dynastore.modules.tasks.tasks_module.claim_by_id", AsyncMock(return_value=row)),
        patch("dynastore.modules.tasks.tasks_module.fail_task", fail_mock),
        patch.object(engine, "dispatch", AsyncMock(side_effect=PermanentTaskFailure("nope"))),
    ):
        with pytest.raises(PermanentTaskFailure):
            await engine.run_ephemeral(MagicMock(), "tasks", engine=MagicMock())

    fail_mock.assert_awaited_once()
    call = fail_mock.await_args
    assert call.kwargs.get("owner_id") == f"ephemeral-{task_id}"
    assert call.kwargs.get("retry") is False


def test_run_ephemeral_terminal_writes_pass_owner_guard_grep_guard():
    """Source-level guard: every complete_task / fail_task call in
    ``run_ephemeral`` must pass ``owner_id=owner_id`` — the
    ``ephemeral-{task_id}`` identity stamped by ``claim_by_id``."""
    import inspect

    source = inspect.getsource(ExecutionEngine.run_ephemeral)
    lines = source.splitlines()
    call_sites = [
        i for i, ln in enumerate(lines)
        if any(f"await {fn}(" in ln for fn in ("complete_task", "fail_task"))
    ]
    assert len(call_sites) == 3, (
        f"expected 3 terminal-write call sites in run_ephemeral, found "
        f"{len(call_sites)}: {[lines[i] for i in call_sites]}"
    )
    for idx in call_sites:
        window = "\n".join(lines[idx:idx + 4])
        assert "owner_id=owner_id" in window, (
            f"run_ephemeral:{idx + 1}: terminal write missing the owner_id "
            f"race guard. Window:\n{window}"
        )
