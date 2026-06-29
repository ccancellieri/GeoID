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

"""Unit tests for per-execution override threading through the OGC Processes path.

Verifies:
  (a) An ExecuteRequest with execution_overrides.timeout_seconds causes
      __execution_overrides__ to be injected into the inputs dict passed to
      execution_engine.execute(), matching the /task spawn-handler contract.
  (b) Omitting execution_overrides leaves inputs unchanged (no __execution_overrides__
      key, backward-compatible behaviour).
  (c) The ingestion process input validation (_validate_process_inputs) ignores
      __execution_overrides__ because the key lives outside the declared
      execution_request.inputs dict — no 422 is raised.
  (d) ExecuteRequest.execution_overrides round-trips correctly and is excluded
      from the task inputs dict when popped before execute().
  (e) GcpJobRunner REST path applies execution_overrides.max_retries to TaskCreate
      when provided, and falls back to the job-config ceiling otherwise.
"""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.models.tasks import TaskExecutionOverrides
from dynastore.modules.processes.models import ExecuteRequest
from dynastore.modules.tasks.execution import _EXECUTION_OVERRIDES_KEY


# ---------------------------------------------------------------------------
# (a) & (b) — execute_process injects __execution_overrides__ into dumped
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_process_injects_execution_overrides_key():
    """When execution_overrides is set on ExecuteRequest, execute_process must
    inject __execution_overrides__ into the inputs dict forwarded to
    execution_engine.execute(), mirroring the /task spawn-handler contract."""
    from dynastore.modules.processes import processes_module

    ovr = TaskExecutionOverrides(timeout_seconds=7200, max_retries=0)
    exec_req = ExecuteRequest(
        inputs={"catalog_id": "c_test", "collection_id": "col1"},
        execution_overrides=ovr,
    )

    captured: dict = {}

    async def _fake_execute(task_type, inputs, *, engine, mode, caller_id,
                            db_schema, collection_id, background_tasks,
                            dedup_key, execution_overrides, **kw):
        captured["inputs"] = dict(inputs)
        captured["execution_overrides"] = execution_overrides
        return object()

    with patch.object(
        processes_module.execution_engine, "execute", side_effect=_fake_execute
    ), patch.object(
        processes_module, "_is_invocable_process", return_value=True
    ), patch.object(
        processes_module, "get_protocols",
        return_value=[_make_registry(exec_req)],
    ), patch.object(
        processes_module, "_validate_process_inputs"
    ), patch.object(
        processes_module, "_resolve_execution_mode", return_value=MagicMock(value="async-execute")
    ), patch(
        "dynastore.modules.tasks.execution.offload_required", AsyncMock(return_value=False)
    ), patch(
        "dynastore.modules.tasks.dispatcher._SERVICE_NAME", "catalog"
    ), patch(
        "dynastore.modules.processes.processes_module.get_protocol", return_value=None
    ):
        await processes_module.execute_process(
            process_id="ingestion",
            execution_request=exec_req,
            engine=MagicMock(),
        )

    assert _EXECUTION_OVERRIDES_KEY in captured["inputs"], (
        f"__execution_overrides__ not found in inputs; got keys: {list(captured['inputs'].keys())}"
    )
    assert captured["inputs"][_EXECUTION_OVERRIDES_KEY] == {
        "timeout_seconds": 7200,
        "max_retries": 0,
    }
    # execution_overrides must NOT appear as a sibling key in task inputs
    assert "execution_overrides" not in captured["inputs"]
    # The same object must be passed directly to execution_engine.execute()
    assert captured["execution_overrides"] is ovr


@pytest.mark.asyncio
async def test_execute_process_no_overrides_key_when_omitted():
    """When execution_overrides is absent, __execution_overrides__ must NOT
    appear in the inputs dict and execution_overrides kwarg must be None."""
    from dynastore.modules.processes import processes_module

    exec_req = ExecuteRequest(
        inputs={"catalog_id": "c_test", "collection_id": "col1"},
    )

    captured: dict = {}

    async def _fake_execute(task_type, inputs, *, engine, mode, caller_id,
                            db_schema, collection_id, background_tasks,
                            dedup_key, execution_overrides, **kw):
        captured["inputs"] = dict(inputs)
        captured["execution_overrides"] = execution_overrides
        return object()

    with patch.object(
        processes_module.execution_engine, "execute", side_effect=_fake_execute
    ), patch.object(
        processes_module, "_is_invocable_process", return_value=True
    ), patch.object(
        processes_module, "get_protocols",
        return_value=[_make_registry(exec_req)],
    ), patch.object(
        processes_module, "_validate_process_inputs"
    ), patch.object(
        processes_module, "_resolve_execution_mode", return_value=MagicMock(value="async-execute")
    ), patch(
        "dynastore.modules.tasks.execution.offload_required", AsyncMock(return_value=False)
    ), patch(
        "dynastore.modules.tasks.dispatcher._SERVICE_NAME", "catalog"
    ), patch(
        "dynastore.modules.processes.processes_module.get_protocol", return_value=None
    ):
        await processes_module.execute_process(
            process_id="ingestion",
            execution_request=exec_req,
            engine=MagicMock(),
        )

    assert _EXECUTION_OVERRIDES_KEY not in captured["inputs"], (
        "__execution_overrides__ must NOT appear in inputs when execution_overrides is absent"
    )
    assert "execution_overrides" not in captured["inputs"]
    assert captured["execution_overrides"] is None


# ---------------------------------------------------------------------------
# (c) — ingestion input validation tolerates __execution_overrides__ in inputs
# ---------------------------------------------------------------------------


def test_validate_process_inputs_ignores_reserved_key():
    """_validate_process_inputs only checks keys declared in process.inputs.
    The __execution_overrides__ key lives outside execution_request.inputs so
    it never reaches the JSON schema validator — no 422 is raised."""
    from dynastore.modules.processes.processes_module import _validate_process_inputs
    from dynastore.modules.processes.models import Process, ProcessScope, ProcessInput

    # Minimal process with one declared input
    process = Process(
        id="ingestion",
        title="Ingestion",
        version="1.0.0",
        scopes=[ProcessScope.COLLECTION],
        inputs={
            "source": ProcessInput(
                title="Source URL",
                **{"schema": {"type": "string"}},
            )
        },
        outputs={},
    )

    # ExecuteRequest whose .inputs has the declared key PLUS the reserved key
    # (simulates what the dispatcher pops before handing off — but even if it
    # appeared here the validator must not reject it since it isn't declared).
    exec_req = ExecuteRequest(
        inputs={
            "source": "https://example.com/data.geojson",
            _EXECUTION_OVERRIDES_KEY: {"timeout_seconds": 3600},
        },
    )

    # Must not raise
    _validate_process_inputs(process, exec_req)


# ---------------------------------------------------------------------------
# (d) — ExecuteRequest.execution_overrides field round-trip
# ---------------------------------------------------------------------------


def test_execute_request_accepts_execution_overrides():
    ovr = TaskExecutionOverrides(timeout_seconds=3600, max_retries=1)
    req = ExecuteRequest(
        inputs={"source": "s3://bucket/file.geojson"},
        execution_overrides=ovr,
    )
    assert req.execution_overrides is ovr
    assert req.execution_overrides.timeout_seconds == 3600
    assert req.execution_overrides.max_retries == 1


def test_execute_request_execution_overrides_defaults_none():
    req = ExecuteRequest(inputs={"source": "s3://bucket/file.geojson"})
    assert req.execution_overrides is None


def test_execute_request_model_dump_excludes_overrides_from_inner_inputs():
    """execution_overrides is a top-level field on ExecuteRequest, not inside
    .inputs — so model_dump()['inputs'] must never contain it."""
    ovr = TaskExecutionOverrides(timeout_seconds=7200)
    req = ExecuteRequest(
        inputs={"source": "https://example.com/data.csv"},
        execution_overrides=ovr,
    )
    dumped = req.model_dump()
    assert _EXECUTION_OVERRIDES_KEY not in dumped["inputs"]
    assert "execution_overrides" not in dumped["inputs"]


# ---------------------------------------------------------------------------
# (e) — GcpJobRunner REST path honours max_retries from execution_overrides
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gcp_runner_rest_path_applies_override_max_retries():
    """When context.execution_overrides.max_retries=0, TaskCreate must be called
    with max_retries=0 (not the job-config default of 3)."""
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner
    from dynastore.modules.tasks.models import RunnerContext
    from sqlalchemy.engine import Engine as _SAEngine

    runner = GcpJobRunner()
    ovr = TaskExecutionOverrides(timeout_seconds=7200, max_retries=0)

    ctx = RunnerContext(
        engine=MagicMock(spec=_SAEngine),
        task_type="ingestion",
        caller_id="user@example.com",
        inputs={},
        db_schema="s_test",
        extra_context={},
        execution_overrides=ovr,
    )

    created_tasks: list = []

    async def _fake_create_task(engine, task_create, *, schema, initial_status, owner_id, locked_until):
        created_tasks.append(task_create)
        fake = MagicMock()
        fake.task_id = __import__("uuid").uuid4()
        return fake

    with patch(
        "dynastore.modules.gcp.tools.jobs.load_job_config",
        AsyncMock(return_value={"ingestion": "dynastore-ingestion-job"}),
    ), patch(
        "dynastore.modules.tasks.tasks_module.create_task", side_effect=_fake_create_task
    ), patch(
        "dynastore.modules.gcp.tools.jobs.run_cloud_run_job_async", AsyncMock(return_value=None)
    ), patch(
        "dynastore.modules.gcp.tools.jobs.try_load_process_definition", return_value=None
    ), patch(
        "dynastore.modules.gcp.tools.jobs.get_job_max_retries", return_value=3
    ):
        await runner.run(ctx)

    assert created_tasks, "create_task was not called"
    assert created_tasks[0].max_retries == 0, (
        f"Expected max_retries=0 from override but got {created_tasks[0].max_retries}"
    )


@pytest.mark.asyncio
async def test_gcp_runner_rest_path_falls_back_to_job_max_retries_when_no_override():
    """When execution_overrides.max_retries is None, GcpJobRunner falls back
    to the job-config max_retries value (3 in this fixture)."""
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner
    from dynastore.modules.tasks.models import RunnerContext
    from sqlalchemy.engine import Engine as _SAEngine

    runner = GcpJobRunner()
    ovr = TaskExecutionOverrides(timeout_seconds=7200)  # max_retries=None

    ctx = RunnerContext(
        engine=MagicMock(spec=_SAEngine),
        task_type="ingestion",
        caller_id="user@example.com",
        inputs={},
        db_schema="s_test",
        extra_context={},
        execution_overrides=ovr,
    )

    created_tasks: list = []

    async def _fake_create_task(engine, task_create, *, schema, initial_status, owner_id, locked_until):
        created_tasks.append(task_create)
        fake = MagicMock()
        fake.task_id = __import__("uuid").uuid4()
        return fake

    with patch(
        "dynastore.modules.gcp.tools.jobs.load_job_config",
        AsyncMock(return_value={"ingestion": "dynastore-ingestion-job"}),
    ), patch(
        "dynastore.modules.tasks.tasks_module.create_task", side_effect=_fake_create_task
    ), patch(
        "dynastore.modules.gcp.tools.jobs.run_cloud_run_job_async", AsyncMock(return_value=None)
    ), patch(
        "dynastore.modules.gcp.tools.jobs.try_load_process_definition", return_value=None
    ), patch(
        "dynastore.modules.gcp.tools.jobs.get_job_max_retries", return_value=3
    ):
        await runner.run(ctx)

    assert created_tasks, "create_task was not called"
    assert created_tasks[0].max_retries == 3, (
        f"Expected fallback max_retries=3 but got {created_tasks[0].max_retries}"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_registry(exec_req: ExecuteRequest):
    """Return a minimal async process registry stub that returns a synthetic process."""
    from dynastore.modules.processes.models import Process, ProcessScope, JobControlOptions

    process = Process(
        id="ingestion",
        title="Ingestion",
        version="1.0.0",
        scopes=[ProcessScope.COLLECTION],
        jobControlOptions=[JobControlOptions.ASYNC_EXECUTE],
        inputs={},
        outputs={},
    )

    registry = MagicMock()
    registry.get_process = AsyncMock(return_value=process)
    return registry
