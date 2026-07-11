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

"""``ExecutionEngine.run_ephemeral``'s success path must bind a live
completion timestamp to ``finished_at`` on ``complete_task`` — not the
claimed row's original enqueue ``timestamp`` captured at ``claim_by_id``
time. The failure paths already passed ``datetime.now(timezone.utc)``.
"""

from __future__ import annotations

import uuid as _uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tasks.execution import ExecutionEngine

_STALE_ENQUEUE_TS = datetime(2020, 1, 1, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_run_ephemeral_complete_task_binds_live_now():
    task_id = _uuid.uuid4()
    fake_task = MagicMock()
    fake_task.task_id = task_id
    row = {"task_id": task_id, "task_type": "t", "timestamp": _STALE_ENQUEUE_TS}

    engine = ExecutionEngine()
    complete_mock = AsyncMock(return_value=True)

    with (
        patch("dynastore.modules.tasks.tasks_module.create_task", AsyncMock(return_value=fake_task)),
        patch("dynastore.modules.tasks.tasks_module.claim_by_id", AsyncMock(return_value=row)),
        patch("dynastore.modules.tasks.tasks_module.complete_task", complete_mock),
        patch.object(engine, "dispatch", AsyncMock(return_value={"ok": True})),
    ):
        await engine.run_ephemeral(MagicMock(), "tasks", engine=MagicMock())

    complete_mock.assert_awaited_once()
    finished_at = complete_mock.await_args.args[2]
    assert finished_at != _STALE_ENQUEUE_TS, (
        "complete_task must not bind the claimed row's original enqueue "
        "timestamp to finished_at."
    )
    assert (datetime.now(timezone.utc) - finished_at).total_seconds() < 10


def test_run_ephemeral_complete_task_binds_live_now_grep_guard():
    import inspect

    source = inspect.getsource(ExecutionEngine.run_ephemeral)
    lines = source.splitlines()
    complete_idx = next(
        i for i, ln in enumerate(lines) if "await complete_task(" in ln
    )
    window = "\n".join(lines[complete_idx:complete_idx + 3])
    assert 'row["timestamp"]' not in window, (
        "run_ephemeral: complete_task must not bind the claimed row's "
        f"stale enqueue timestamp to finished_at. Window:\n{window}"
    )
    assert "datetime.now(timezone.utc)" in window, (
        f"run_ephemeral: complete_task must bind a live completion "
        f"timestamp. Window:\n{window}"
    )
