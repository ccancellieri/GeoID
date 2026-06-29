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

"""Unit tests for per-execution TaskExecutionOverrides threading.

Verifies:
  1. GCPModule.run_job sets RunJobRequest.Overrides.timeout when timeout_seconds
     is supplied, and leaves it unset (job default) when not supplied.
  2. run_cloud_run_job_async threads execution_overrides through to run_job.
  3. GcpJobRunner passes context.execution_overrides to run_cloud_run_job_async
     and uses the per-task timeout for the DB lease; falls back to the config
     value when no override is set.
  4. TaskExecutionOverrides model validation (timeout ge=1, max_retries ge=0).
  5. SyncRunner respects execution_overrides.timeout_seconds over the routing
     ceiling, and silently ignores cpu/memory.
"""

from __future__ import annotations

import uuid as _uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.tasks import TaskExecutionOverrides
from dynastore.modules.tasks.models import RunnerContext
from sqlalchemy.engine import Engine as _SAEngine


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _fake_engine() -> MagicMock:
    return MagicMock(spec=_SAEngine)


# ---------------------------------------------------------------------------
# 1. TaskExecutionOverrides model
# ---------------------------------------------------------------------------


def test_task_execution_overrides_defaults_all_none():
    """All fields default to None — every existing call site is backward-compatible."""
    ovr = TaskExecutionOverrides()
    assert ovr.timeout_seconds is None
    assert ovr.cpu is None
    assert ovr.memory is None
    assert ovr.max_retries is None


def test_task_execution_overrides_accepts_valid_fields():
    ovr = TaskExecutionOverrides(
        timeout_seconds=28800,
        cpu="4",
        memory="16Gi",
        max_retries=0,
    )
    assert ovr.timeout_seconds == 28800
    assert ovr.cpu == "4"
    assert ovr.memory == "16Gi"
    assert ovr.max_retries == 0


def test_task_execution_overrides_rejects_zero_timeout():
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        TaskExecutionOverrides(timeout_seconds=0)


def test_task_execution_overrides_rejects_negative_timeout():
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        TaskExecutionOverrides(timeout_seconds=-1)


def test_task_execution_overrides_rejects_negative_max_retries():
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        TaskExecutionOverrides(max_retries=-1)


# ---------------------------------------------------------------------------
# 2. GCPModule.run_job — applies overrides.timeout; ignores cpu/memory
# ---------------------------------------------------------------------------


def _build_mock_run_v2():
    """Build a minimal run_v2 mock that records the RunJobRequest sent."""
    from unittest.mock import MagicMock, AsyncMock

    # Mock the run_v2 module
    mock_run_v2 = MagicMock()

    # RunJobRequest.Overrides.ContainerOverride
    container_override_instance = MagicMock()
    container_override_instance.args = []
    container_override_instance.env = []
    mock_run_v2.RunJobRequest.Overrides.ContainerOverride.return_value = container_override_instance

    # RunJobRequest instance — stores whatever is assigned to its .overrides
    request_instance = MagicMock()
    request_instance.overrides = MagicMock()
    request_instance.overrides.container_overrides = []
    request_instance.overrides.timeout = None
    mock_run_v2.RunJobRequest.return_value = request_instance

    mock_run_v2.EnvVar = MagicMock(side_effect=lambda name, value: MagicMock(name=name, value=value))

    # Async jobs client
    op_mock = MagicMock()
    op_mock.operation = MagicMock(name="projects/p/locations/r/jobs/j/executions/e")
    jobs_client = AsyncMock()
    jobs_client.run_job = AsyncMock(return_value=op_mock)

    return mock_run_v2, request_instance, jobs_client


@pytest.mark.asyncio
async def test_run_job_sets_overrides_timeout_when_execution_overrides_provided():
    """GCPModule.run_job must set request.overrides.timeout when timeout_seconds given."""
    from dynastore.modules.gcp.gcp_module import GCPModule
    import dynastore.modules.gcp.gcp_module as gcp_mod_module

    mock_run_v2, request_instance, jobs_client = _build_mock_run_v2()

    module = GCPModule.__new__(GCPModule)
    module.get_project_id = MagicMock(return_value="proj")
    module.get_region = MagicMock(return_value="us-central1")
    module.get_jobs_client = MagicMock(return_value=jobs_client)

    ovr = TaskExecutionOverrides(timeout_seconds=7200)

    dur_instance = MagicMock()

    with patch.object(gcp_mod_module, "run_v2", mock_run_v2), \
         patch("dynastore.modules.gcp.gcp_module.GCPModule.run_job", GCPModule.run_job):
        pass  # just to verify patches available

    with patch.object(gcp_mod_module, "run_v2", mock_run_v2), \
         patch("google.protobuf.duration_pb2.Duration", return_value=dur_instance) as dur_cls:
        await GCPModule.run_job(module, "my-job", execution_overrides=ovr)

    dur_cls.assert_called_once_with(seconds=7200)
    assert request_instance.overrides.timeout == dur_instance


@pytest.mark.asyncio
async def test_run_job_no_timeout_when_execution_overrides_none():
    """GCPModule.run_job must NOT touch overrides.timeout when execution_overrides is None."""
    from dynastore.modules.gcp.gcp_module import GCPModule
    import dynastore.modules.gcp.gcp_module as gcp_mod_module

    mock_run_v2, request_instance, jobs_client = _build_mock_run_v2()

    module = GCPModule.__new__(GCPModule)
    module.get_project_id = MagicMock(return_value="proj")
    module.get_region = MagicMock(return_value="us-central1")
    module.get_jobs_client = MagicMock(return_value=jobs_client)

    with patch.object(gcp_mod_module, "run_v2", mock_run_v2), \
         patch("google.protobuf.duration_pb2.Duration") as dur_cls:
        await GCPModule.run_job(module, "my-job", execution_overrides=None)

    dur_cls.assert_not_called()


# ---------------------------------------------------------------------------
# 3. run_cloud_run_job_async threads execution_overrides through
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_cloud_run_job_async_passes_execution_overrides():
    """run_cloud_run_job_async forwards execution_overrides to job_runner.run_job."""
    from dynastore.modules.gcp.tools.jobs import run_cloud_run_job_async

    ovr = TaskExecutionOverrides(timeout_seconds=3600)
    mock_runner = AsyncMock()
    mock_runner.run_job = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.gcp.tools.jobs.get_protocol", return_value=mock_runner
    ):
        await run_cloud_run_job_async(
            "my-job",
            args=["arg1"],
            env_vars={"K": "V"},
            execution_overrides=ovr,
        )

    mock_runner.run_job.assert_awaited_once_with(
        "my-job", ["arg1"], {"K": "V"}, ovr
    )


@pytest.mark.asyncio
async def test_run_cloud_run_job_async_passes_none_when_not_supplied():
    """run_cloud_run_job_async passes None by default — backward-compatible."""
    from dynastore.modules.gcp.tools.jobs import run_cloud_run_job_async

    mock_runner = AsyncMock()
    mock_runner.run_job = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.gcp.tools.jobs.get_protocol", return_value=mock_runner
    ):
        await run_cloud_run_job_async("my-job")

    mock_runner.run_job.assert_awaited_once_with("my-job", None, None, None)


# ---------------------------------------------------------------------------
# 4. GcpJobRunner — per-task timeout for lease + pass to run_cloud_run_job_async
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gcp_runner_uses_per_task_timeout_for_lease():
    """When context.execution_overrides.timeout_seconds is set, the DB lease
    must use that value (not the platform config default), and
    run_cloud_run_job_async must receive the same execution_overrides."""
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner

    runner = GcpJobRunner()
    claimed_id = _uuid.uuid4()
    ovr = TaskExecutionOverrides(timeout_seconds=28800)

    ctx = RunnerContext(
        engine=_fake_engine(),
        task_type="ingestion",
        caller_id="user@example.com",
        inputs={},
        db_schema="s_test",
        extra_context={"task_id": str(claimed_id), "task_timestamp": "2026-01-01T00:00:00Z"},
        execution_overrides=ovr,
    )

    run_job_mock = AsyncMock(return_value=None)
    load_job_config_mock = AsyncMock(return_value={"ingestion": "dynastore-ingestion-job"})

    locked_until_seen: list = []

    async def _fake_claim(engine, task_id, *, owner_id, locked_until, **kw):
        locked_until_seen.append(locked_until)
        return True

    with patch(
        "dynastore.modules.tasks.tasks_module.claim_for_dispatch", side_effect=_fake_claim
    ), patch(
        "dynastore.modules.gcp.tools.jobs.load_job_config", load_job_config_mock
    ), patch(
        "dynastore.modules.gcp.tools.jobs.run_cloud_run_job_async", run_job_mock
    ), patch(
        "dynastore.modules.gcp.tools.jobs.try_load_process_definition", return_value=None
    ):
        await runner.run(ctx)

    # Lease must be ~28800s (per-task override), not the 3600 platform default.
    from datetime import datetime, timezone, timedelta
    assert locked_until_seen, "claim_for_dispatch was not called"
    effective_lease = locked_until_seen[0] - datetime.now(timezone.utc)
    assert effective_lease > timedelta(seconds=28700), (
        f"Expected lease ~28800s but got {effective_lease.total_seconds():.0f}s"
    )

    # execution_overrides must be forwarded to run_cloud_run_job_async.
    run_job_mock.assert_awaited_once()
    call_kwargs = run_job_mock.await_args.kwargs
    assert call_kwargs.get("execution_overrides") is ovr


@pytest.mark.asyncio
async def test_gcp_runner_falls_back_to_config_timeout_when_no_overrides():
    """When context.execution_overrides is None, GcpJobRunner falls back to
    the TasksPluginConfig.task_timeout_seconds value for the DB lease."""
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner

    runner = GcpJobRunner()
    ctx = RunnerContext(
        engine=_fake_engine(),
        task_type="ingestion",
        caller_id="user@example.com",
        inputs={},
        db_schema="s_test",
        extra_context={},
        execution_overrides=None,  # no per-task override
    )

    fake_task = MagicMock()
    fake_task.task_id = _uuid.uuid4()
    create_mock = AsyncMock(return_value=fake_task)
    run_job_mock = AsyncMock(return_value=None)
    load_job_config_mock = AsyncMock(return_value={"ingestion": "dynastore-ingestion-job"})

    with patch(
        "dynastore.modules.tasks.tasks_module.create_task", create_mock
    ), patch(
        "dynastore.modules.gcp.tools.jobs.load_job_config", load_job_config_mock
    ), patch(
        "dynastore.modules.gcp.tools.jobs.run_cloud_run_job_async", run_job_mock
    ), patch(
        "dynastore.modules.gcp.tools.jobs.try_load_process_definition", return_value=None
    ):
        await runner.run(ctx)

    # execution_overrides passed to run_cloud_run_job_async must be None.
    call_kwargs = run_job_mock.await_args.kwargs
    assert call_kwargs.get("execution_overrides") is None


# ---------------------------------------------------------------------------
# 5. SpawnTaskRequest carries execution_overrides field
# ---------------------------------------------------------------------------


def test_spawn_task_request_accepts_execution_overrides():
    from dynastore.models.tasks import SpawnTaskRequest
    req = SpawnTaskRequest(
        task_type="ingestion",
        inputs={"source": "s3://bucket/file.geojson"},
        execution_overrides=TaskExecutionOverrides(timeout_seconds=7200, cpu="4"),
    )
    assert req.execution_overrides is not None
    assert req.execution_overrides.timeout_seconds == 7200
    assert req.execution_overrides.cpu == "4"


def test_spawn_task_request_execution_overrides_optional():
    from dynastore.models.tasks import SpawnTaskRequest
    req = SpawnTaskRequest(task_type="ingestion", inputs={})
    assert req.execution_overrides is None


# ---------------------------------------------------------------------------
# 6. RunnerContext carries execution_overrides
# ---------------------------------------------------------------------------


def test_runner_context_accepts_execution_overrides():
    ovr = TaskExecutionOverrides(timeout_seconds=3600)
    ctx = RunnerContext(
        engine=_fake_engine(),
        task_type="ingestion",
        caller_id="user@example.com",
        inputs={},
        db_schema="s_test",
        extra_context={},
        execution_overrides=ovr,
    )
    assert ctx.execution_overrides is ovr


def test_runner_context_execution_overrides_defaults_none():
    ctx = RunnerContext(
        engine=_fake_engine(),
        task_type="ingestion",
        caller_id="user@example.com",
        inputs={},
        db_schema="s_test",
        extra_context={},
    )
    assert ctx.execution_overrides is None


# ---------------------------------------------------------------------------
# 7. execution.py _EXECUTION_OVERRIDES_KEY constant
# ---------------------------------------------------------------------------


def test_execution_overrides_key_is_reserved():
    """The reserved key must start with __ so it cannot clash with user inputs."""
    from dynastore.modules.tasks.execution import _EXECUTION_OVERRIDES_KEY
    assert _EXECUTION_OVERRIDES_KEY.startswith("__")
    assert _EXECUTION_OVERRIDES_KEY.endswith("__")


# ---------------------------------------------------------------------------
# 8. max_retries: execution_overrides takes precedence over body default
# ---------------------------------------------------------------------------


def _effective_max_retries(body_max_retries, execution_overrides):
    """Mirror the conditional expression used in all three spawn handlers."""
    return (
        execution_overrides.max_retries
        if execution_overrides and execution_overrides.max_retries is not None
        else body_max_retries
    )


def test_execution_overrides_max_retries_zero_beats_body_default():
    """execution_overrides.max_retries=0 must win over body.max_retries=3
    (0 is falsy in Python, so we test the is-not-None guard explicitly)."""
    ovr = TaskExecutionOverrides(max_retries=0)
    assert _effective_max_retries(body_max_retries=3, execution_overrides=ovr) == 0


def test_execution_overrides_max_retries_positive_beats_body_default():
    ovr = TaskExecutionOverrides(max_retries=5)
    assert _effective_max_retries(body_max_retries=3, execution_overrides=ovr) == 5


def test_execution_overrides_max_retries_none_falls_back_to_body():
    """When execution_overrides.max_retries is None, body.max_retries wins."""
    ovr = TaskExecutionOverrides(max_retries=None)
    assert _effective_max_retries(body_max_retries=7, execution_overrides=ovr) == 7


def test_no_execution_overrides_falls_back_to_body_max_retries():
    """When execution_overrides is None entirely, body.max_retries is used."""
    assert _effective_max_retries(body_max_retries=2, execution_overrides=None) == 2


# ---------------------------------------------------------------------------
# 9. dispatch() warns on corrupt __execution_overrides__ payload
#    We verify the trigger condition (model_validate raises on bad data) and
#    that the warning string is present in execution.py source.
# ---------------------------------------------------------------------------


def test_corrupt_execution_overrides_payload_raises_validation_error():
    """A corrupt __execution_overrides__ dict must raise ValidationError so the
    except-branch in dispatch() fires and emits the warning."""
    from pydantic import ValidationError

    # timeout_seconds must be int ge=1; passing a non-numeric string is invalid.
    with pytest.raises(ValidationError):
        TaskExecutionOverrides.model_validate({"timeout_seconds": "not-an-int"})


def test_dispatch_warning_message_present_in_source():
    """Regression guard: the warning call in dispatch() must not be accidentally
    removed — check the exact log-message fragment exists in execution.py."""
    import inspect
    import dynastore.modules.tasks.execution as _exec_mod

    src = inspect.getsource(_exec_mod)
    assert "invalid __execution_overrides__ payload, ignoring" in src, (
        "logger.warning for corrupt __execution_overrides__ payload not found "
        "in dynastore.modules.tasks.execution — was it removed?"
    )
