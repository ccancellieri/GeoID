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

"""``report_failure()`` must not re-bootstrap the full module graph (#2887).

A ``dev-dynastore-async-writer`` execution OOM'd after a long-running
``EventDrainTask`` run. Part of the tip-over: ``report_failure()`` — the
outer ``__main__`` fallback, reached when the in-lifecycle failure path could
not run or record the row itself — used to call ``bootstrap_task_env()`` and
enter a second full ``modules.lifespan()``, instantiating every module
(Tiles, MovingFeatures, Stats, ConnectedSystems, ...), a fresh DB engine/pool,
a fresh ES client, and a second ``BackgroundSupervisor`` with roughly a dozen
asyncio loops — purely to flip one task row to FAILED. On a memory-pressured
process that second full boot was the allocation that OOM'd it.

The fix: ``report_failure()`` opens its own bare, ``NullPool``-backed async
engine (the same minimal-footprint pattern the drain tasks already use to
build their own engines) and writes the FAILED status directly via
``update_task`` — no module bootstrap, no lifespan, no background loops.

These tests pin both a source-level guarantee (the expensive calls are gone
for good) and the observable behaviour (the row still gets marked FAILED, and
a DB failure inside the minimal path degrades gracefully instead of raising).
"""

from __future__ import annotations

import inspect
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _report_failure_src() -> str:
    from dynastore.main_task import report_failure
    return inspect.getsource(report_failure)


# ---------------------------------------------------------------------------
# Source-level guard: the expensive full-graph bootstrap must never reappear.
# ---------------------------------------------------------------------------


def test_report_failure_source_never_bootstraps_full_module_graph():
    src = _report_failure_src()
    assert "bootstrap_task_env" not in src, (
        "report_failure() must not call bootstrap_task_env() — that "
        "instantiates every registered module, not just the DB path"
    )
    assert "modules.lifespan" not in src, (
        "report_failure() must not enter modules.lifespan() — that starts "
        "every module's async lifespan, including the BackgroundSupervisor "
        "loops, purely to write one FAILED row"
    )


def test_report_failure_source_uses_bare_engine_and_update_task():
    src = _report_failure_src()
    assert "create_task_engine" in src
    assert "update_task(" in src


# ---------------------------------------------------------------------------
# Behavioural: the write still happens, the engine is disposed, and neither
# the module bootstrap nor the lifespan context manager is ever touched.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_report_failure_writes_failed_without_touching_module_graph():
    from dynastore.main_task import report_failure
    from dynastore.modules.tasks.models import TaskStatusEnum

    fake_engine = MagicMock()
    fake_engine.dispose = AsyncMock()
    update_task_mock = AsyncMock(return_value=None)
    bootstrap_mock = MagicMock()
    lifespan_mock = MagicMock()

    task_id = str(uuid.uuid4())

    with (
        patch(
            "dynastore.modules.db_config.db_timeout_config.create_task_engine",
            return_value=fake_engine,
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.update_task",
            new=update_task_mock,
        ),
        # Neither of these is imported by report_failure any more; patching
        # them here still proves the negative — if a future change re-adds
        # either call it will show up as an unexpected invocation.
        patch("dynastore.tasks.bootstrap.bootstrap_task_env", new=bootstrap_mock),
        patch("dynastore.modules.lifespan", new=lifespan_mock),
    ):
        await report_failure(task_id, "platform", "boom")

    update_task_mock.assert_awaited_once()
    called_task_id, update_data = update_task_mock.await_args.args[1:3]
    assert called_task_id == uuid.UUID(task_id)
    assert update_data.status == TaskStatusEnum.FAILED
    assert "boom" in update_data.error_message

    bootstrap_mock.assert_not_called()
    lifespan_mock.assert_not_called()
    fake_engine.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_report_failure_swallows_db_errors_from_the_minimal_path():
    """A DB failure inside the minimal path must degrade gracefully — this
    fallback is already running from the outermost exception handler and
    must never itself raise."""
    from dynastore.main_task import report_failure

    fake_engine = MagicMock()
    fake_engine.dispose = AsyncMock()
    update_task_mock = AsyncMock(side_effect=RuntimeError("db unreachable"))

    with (
        patch(
            "dynastore.modules.db_config.db_timeout_config.create_task_engine",
            return_value=fake_engine,
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.update_task",
            new=update_task_mock,
        ),
    ):
        await report_failure(str(uuid.uuid4()), "platform", "boom")  # must not raise

    fake_engine.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_report_failure_is_a_noop_without_task_id_or_schema():
    from dynastore.main_task import report_failure

    with patch("sqlalchemy.ext.asyncio.create_async_engine") as create_engine_mock:
        await report_failure("", "platform", "boom")
        await report_failure("some-task-id", "", "boom")

    create_engine_mock.assert_not_called()
