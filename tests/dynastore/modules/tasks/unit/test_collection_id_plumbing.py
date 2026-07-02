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

"""Contract tests for ``collection_id`` plumbing through ExecutionEngine + runners.

Locks the behaviour added alongside the per-tenant tasks-table cleanup:

- ``RunnerContext.collection_id`` is a first-class field and defaults to None.
- ``ExecutionEngine.execute(..., collection_id=...)`` forwards into
  ``RunnerContext.collection_id``.
- The 3 in-runner ``TaskCreate(...)`` sites (``SyncRunner``,
  ``BackgroundRunner``, ``WorkerQueueRunner``) persist ``collection_id`` from
  the context onto the task row — closing the gap where OGC-Process-routed
  work resolved ``/collections/{collection_id}`` and lost it before INSERT.

These run pure-unit (no DB) by mocking ``create_task``.
"""

from __future__ import annotations

from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.engine import Engine as _SAEngine

from dynastore.modules.tasks.execution import ExecutionEngine
from dynastore.modules.tasks.models import RunnerContext, TaskExecutionMode


def _fake_engine() -> MagicMock:
    return MagicMock(spec=_SAEngine)


def test_runner_context_collection_id_defaults_to_none() -> None:
    ctx = RunnerContext(
        engine=_fake_engine(),
        task_type="t",
        caller_id="u",
        inputs={},
        extra_context={},
    )
    assert ctx.collection_id is None


def test_runner_context_collection_id_is_carried() -> None:
    ctx = RunnerContext(
        engine=_fake_engine(),
        task_type="t",
        caller_id="u",
        inputs={},
        collection_id="my_collection",
        extra_context={},
    )
    assert ctx.collection_id == "my_collection"


@pytest.mark.asyncio
async def test_execution_engine_forwards_collection_id_into_context() -> None:
    """``ExecutionEngine.execute(collection_id=X)`` lands on
    ``RunnerContext.collection_id``.
    """
    engine = ExecutionEngine()

    captured: Dict[str, Any] = {}

    class _StubRunner:
        async def run(self, ctx: RunnerContext) -> Any:
            captured["collection_id"] = ctx.collection_id
            captured["caller_id"] = ctx.caller_id
            captured["db_schema"] = ctx.db_schema
            return "ok"

    with patch.object(
        engine, "get_runners_for", return_value=[_StubRunner()],
    ):
        result = await engine.execute(
            task_type="t",
            inputs={},
            engine=_fake_engine(),
            mode=TaskExecutionMode.ASYNCHRONOUS,
            caller_id="alice",
            db_schema="s_2ka8fbc3",
            collection_id="stations",
        )

    assert result == "ok"
    assert captured["collection_id"] == "stations"
    assert captured["caller_id"] == "alice"
    assert captured["db_schema"] == "s_2ka8fbc3"


def test_runner_taskcreate_sites_pass_collection_id_grep_guard() -> None:
    """Source-level guard: every in-runner ``TaskCreate(...)`` site reads
    ``context.collection_id``.

    The three production sites are ``SyncRunner.run``, ``BackgroundRunner.run``
    (direct-invocation branch) and ``WorkerQueueRunner.run``. A pure-unit
    mock-based check is brittle here because each runner imports its deps
    lazily inside the method (``from dynastore.tools.protocol_helpers
    import resolve``), so module-level ``patch`` cannot intercept. Instead
    we read the source and assert the literal occurs alongside every
    ``TaskCreate(`` constructor. Any future regression that drops the
    field from a runner is caught at unit time without booting a DB.
    """
    import inspect

    from dynastore.modules.tasks import runners as runners_module

    source = inspect.getsource(runners_module)

    # Locate every ``TaskCreate(`` constructor in the runners module and
    # demand that within ~14 lines downstream the ``collection_id=`` arg
    # appears. That window matches the longest current constructor (caller_id
    # + task_type + inputs + collection_id + dedup_key + max_retries) plus
    # cushion for future additions.
    lines = source.splitlines()
    constructor_lines = [
        i for i, ln in enumerate(lines) if "TaskCreate(" in ln
    ]
    assert constructor_lines, "No TaskCreate(...) sites found in runners.py"

    for idx in constructor_lines:
        window = "\n".join(lines[idx:idx + 14])
        assert "collection_id=context.collection_id" in window, (
            f"runners.py:{idx + 1}: TaskCreate(...) missing "
            f"`collection_id=context.collection_id` — attribution would be "
            f"lost for collection-scoped work. Window:\n{window}"
        )


def test_runner_task_run_scope_sites_declare_own_catalog_grep_guard() -> None:
    """Source-level guard (#2716): every ``task_run_scope(...)`` call site
    in ``runners.py`` declares ``catalog=context.db_schema``.

    Without the catalog, ``IndexDispatcher``'s in-run write-absorption gate
    (#2716) falls back to "no restriction known" and a task run's ASYNC
    secondary writes are absorbed inline regardless of which catalog they
    target — the exact foreign-backlog-absorption bug this issue fixes. A
    mock-based check is brittle here for the same reason as the
    ``collection_id`` guard above (lazy imports inside each runner method),
    so this reads the source directly.
    """
    import inspect

    from dynastore.modules.tasks import runners as runners_module

    source = inspect.getsource(runners_module)
    lines = source.splitlines()
    scope_lines = [i for i, ln in enumerate(lines) if "task_run_scope(" in ln]
    assert scope_lines, "No task_run_scope(...) sites found in runners.py"

    for idx in scope_lines:
        assert "catalog=context.db_schema" in lines[idx], (
            f"runners.py:{idx + 1}: task_run_scope(...) missing "
            f"`catalog=context.db_schema` — in-run index-dispatch absorption "
            f"would be unscoped for this runner. Line: {lines[idx]!r}"
        )


def test_main_task_task_run_scope_site_declares_own_catalog_grep_guard() -> None:
    """Source-level guard (#2716): the ``task_run_scope(...)`` call site in
    ``main_task.py`` — the Cloud Run Job entrypoint — declares
    ``catalog=schema``.

    ``schema`` is the task's own catalog id (GcpJobRunner passes it via
    ``--schema``, sourced from the task row's ``catalog_id``). Without it,
    ``current_task_catalog()`` stays ``None`` for every Cloud Run Job
    execution and ``IndexDispatcher``'s in-run write-absorption gate falls
    back to "no restriction known", leaving the job process — the one that
    actually OOM'd — able to absorb a foreign catalog's outbox backlog
    inline. Mirrors the runners.py guard above; kept separate because
    main_task.py binds a plain local (``schema``), not ``context.db_schema``.
    """
    import inspect

    from dynastore import main_task as main_task_module

    source = inspect.getsource(main_task_module)
    lines = source.splitlines()
    scope_lines = [i for i, ln in enumerate(lines) if "task_run_scope(" in ln]
    assert scope_lines, "No task_run_scope(...) sites found in main_task.py"

    for idx in scope_lines:
        assert "catalog=schema" in lines[idx], (
            f"main_task.py:{idx + 1}: task_run_scope(...) missing "
            f"`catalog=schema` — in-run index-dispatch absorption would be "
            f"unscoped for the Cloud Run Job entrypoint. Line: {lines[idx]!r}"
        )
