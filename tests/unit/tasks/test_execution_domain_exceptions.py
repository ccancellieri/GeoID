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

"""Unit tests: ExecutionEngine raises domain exceptions, not HTTPException.

Regression guard for issue #1969: execution.py must be framework-free.
Tests confirm that update_job / start_job / dismiss_job raise
JobLockedError / JobStateConflictError respectively — never fastapi.HTTPException.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from dynastore.modules.tasks.exceptions import JobLockedError, JobStateConflictError
from dynastore.models.tasks import TaskStatusEnum


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_JOB_ID = uuid4()
_DB_SCHEMA = "tasks"
_ENGINE = MagicMock()


def _make_job(status: TaskStatusEnum) -> MagicMock:
    job = MagicMock()
    job.task_id = _JOB_ID
    job.task_type = "test.task"
    job.inputs = {}
    job.caller_id = "system"
    job.status = status
    return job


def _make_tasks_mgr(job: MagicMock) -> MagicMock:
    mgr = MagicMock()
    mgr.get_task = AsyncMock(return_value=job)
    mgr.update_task = AsyncMock(return_value=None)
    return mgr


# ---------------------------------------------------------------------------
# Tests: update_job raises JobLockedError when job is not CREATED
# ---------------------------------------------------------------------------


class TestUpdateJobLockedError:
    """update_job must raise JobLockedError (HTTP 423) when job is locked."""

    @pytest.mark.parametrize(
        "status",
        [
            TaskStatusEnum.PENDING,
            TaskStatusEnum.RUNNING,
            TaskStatusEnum.COMPLETED,
            TaskStatusEnum.FAILED,
        ],
    )
    def test_raises_job_locked_error_not_http_exception(
        self, status: TaskStatusEnum
    ) -> None:
        from dynastore.modules.tasks.execution import ExecutionEngine

        engine = ExecutionEngine()
        job = _make_job(status)
        tasks_mgr = _make_tasks_mgr(job)

        async def _run() -> None:
            with _patch_tasks_protocol(tasks_mgr):
                await engine.update_job(
                    _JOB_ID,
                    {"new": "input"},
                    engine=_ENGINE,
                    db_schema=_DB_SCHEMA,
                )

        with pytest.raises(JobLockedError) as exc_info:
            asyncio.run(_run())

        assert str(_JOB_ID) in str(exc_info.value)
        assert status.value in str(exc_info.value)

    def test_does_not_raise_http_exception(self) -> None:
        """Confirm the raised exception is NOT fastapi.HTTPException."""
        from dynastore.modules.tasks.execution import ExecutionEngine

        engine = ExecutionEngine()
        job = _make_job(TaskStatusEnum.RUNNING)
        tasks_mgr = _make_tasks_mgr(job)

        async def _run() -> None:
            with _patch_tasks_protocol(tasks_mgr):
                await engine.update_job(
                    _JOB_ID, {}, engine=_ENGINE, db_schema=_DB_SCHEMA
                )

        with pytest.raises(Exception) as exc_info:
            asyncio.run(_run())

        exc = exc_info.value
        assert isinstance(exc, JobLockedError)
        # Must not be an HTTPException — the extension boundary handles that
        assert exc.__class__.__name__ != "HTTPException"
        assert "fastapi" not in exc.__class__.__module__


# ---------------------------------------------------------------------------
# Tests: start_job raises JobStateConflictError when job is not CREATED
# ---------------------------------------------------------------------------


class TestStartJobStateConflictError:
    """start_job must raise JobStateConflictError (HTTP 409) when job is not CREATED."""

    @pytest.mark.parametrize(
        "status",
        [
            TaskStatusEnum.PENDING,
            TaskStatusEnum.RUNNING,
            TaskStatusEnum.COMPLETED,
            TaskStatusEnum.FAILED,
        ],
    )
    def test_raises_job_state_conflict_error(self, status: TaskStatusEnum) -> None:
        from dynastore.modules.tasks.execution import ExecutionEngine

        engine = ExecutionEngine()
        job = _make_job(status)
        tasks_mgr = _make_tasks_mgr(job)

        async def _run() -> None:
            with _patch_tasks_protocol(tasks_mgr):
                await engine.start_job(_JOB_ID, engine=_ENGINE, db_schema=_DB_SCHEMA)

        with pytest.raises(JobStateConflictError) as exc_info:
            asyncio.run(_run())

        assert str(_JOB_ID) in str(exc_info.value)
        assert status.value in str(exc_info.value)

    def test_does_not_raise_http_exception(self) -> None:
        from dynastore.modules.tasks.execution import ExecutionEngine

        engine = ExecutionEngine()
        job = _make_job(TaskStatusEnum.COMPLETED)
        tasks_mgr = _make_tasks_mgr(job)

        async def _run() -> None:
            with _patch_tasks_protocol(tasks_mgr):
                await engine.start_job(_JOB_ID, engine=_ENGINE, db_schema=_DB_SCHEMA)

        with pytest.raises(Exception) as exc_info:
            asyncio.run(_run())

        exc = exc_info.value
        assert isinstance(exc, JobStateConflictError)
        assert exc.__class__.__name__ != "HTTPException"
        assert "fastapi" not in exc.__class__.__module__


# ---------------------------------------------------------------------------
# Tests: dismiss_job raises JobStateConflictError when job is terminal
# ---------------------------------------------------------------------------


class TestDismissJobStateConflictError:
    """dismiss_job must raise JobStateConflictError (HTTP 409) when job is terminal."""

    @pytest.mark.parametrize(
        "status",
        [
            TaskStatusEnum.COMPLETED,
            TaskStatusEnum.FAILED,
            TaskStatusEnum.DISMISSED,
            TaskStatusEnum.DEAD_LETTER,
        ],
    )
    def test_raises_job_state_conflict_error_for_terminal(
        self, status: TaskStatusEnum
    ) -> None:
        from dynastore.modules.tasks.execution import ExecutionEngine

        engine = ExecutionEngine()
        job = _make_job(status)
        tasks_mgr = _make_tasks_mgr(job)

        async def _run() -> None:
            with _patch_tasks_protocol(tasks_mgr):
                await engine.dismiss_job(_JOB_ID, engine=_ENGINE, db_schema=_DB_SCHEMA)

        with pytest.raises(JobStateConflictError) as exc_info:
            asyncio.run(_run())

        assert str(_JOB_ID) in str(exc_info.value)
        assert status.value in str(exc_info.value)

    def test_does_not_raise_http_exception(self) -> None:
        from dynastore.modules.tasks.execution import ExecutionEngine

        engine = ExecutionEngine()
        job = _make_job(TaskStatusEnum.COMPLETED)
        tasks_mgr = _make_tasks_mgr(job)

        async def _run() -> None:
            with _patch_tasks_protocol(tasks_mgr):
                await engine.dismiss_job(_JOB_ID, engine=_ENGINE, db_schema=_DB_SCHEMA)

        with pytest.raises(Exception) as exc_info:
            asyncio.run(_run())

        exc = exc_info.value
        assert isinstance(exc, JobStateConflictError)
        assert exc.__class__.__name__ != "HTTPException"
        assert "fastapi" not in exc.__class__.__module__


# ---------------------------------------------------------------------------
# Tests: exception hierarchy — must NOT subclass HTTPException
# ---------------------------------------------------------------------------


class TestExceptionHierarchy:
    """Domain exceptions must not inherit from HTTPException or any fastapi type."""

    def test_job_locked_error_not_subclass_of_http_exception(self) -> None:
        try:
            from fastapi import HTTPException

            assert not issubclass(JobLockedError, HTTPException)
        except ImportError:
            pass  # fastapi not installed — trivially passes

    def test_job_state_conflict_error_not_subclass_of_http_exception(self) -> None:
        try:
            from fastapi import HTTPException

            assert not issubclass(JobStateConflictError, HTTPException)
        except ImportError:
            pass

    def test_job_locked_error_is_plain_exception(self) -> None:
        exc = JobLockedError("test locked message")
        assert isinstance(exc, Exception)
        assert str(exc) == "test locked message"

    def test_job_state_conflict_error_is_plain_exception(self) -> None:
        exc = JobStateConflictError("test conflict message")
        assert isinstance(exc, Exception)
        assert str(exc) == "test conflict message"


# ---------------------------------------------------------------------------
# Tests: handler mapping (registry maps to correct HTTP status)
# ---------------------------------------------------------------------------


class TestExceptionHandlerMapping:
    """Extension-boundary handlers must map to the correct HTTP status codes."""

    def test_job_locked_maps_to_423(self) -> None:
        from dynastore.extensions.tools.exception_handlers import (
            JobLockedExceptionHandler,
        )

        handler = JobLockedExceptionHandler()
        exc = JobLockedError("job is locked")
        assert handler.can_handle(exc)
        result = handler.handle(exc)
        assert result is not None
        assert result.status_code == 423
        assert "job is locked" in str(result.detail)

    def test_job_state_conflict_maps_to_409(self) -> None:
        from dynastore.extensions.tools.exception_handlers import (
            JobStateConflictExceptionHandler,
        )

        handler = JobStateConflictExceptionHandler()
        exc = JobStateConflictError("job cannot be started")
        assert handler.can_handle(exc)
        result = handler.handle(exc)
        assert result is not None
        assert result.status_code == 409
        assert "job cannot be started" in str(result.detail)

    def test_job_locked_handler_rejects_other_exceptions(self) -> None:
        from dynastore.extensions.tools.exception_handlers import (
            JobLockedExceptionHandler,
        )

        handler = JobLockedExceptionHandler()
        assert not handler.can_handle(ValueError("not locked"))
        assert not handler.can_handle(RuntimeError("not locked"))

    def test_job_state_conflict_handler_rejects_other_exceptions(self) -> None:
        from dynastore.extensions.tools.exception_handlers import (
            JobStateConflictExceptionHandler,
        )

        handler = JobStateConflictExceptionHandler()
        assert not handler.can_handle(ValueError("not a conflict"))
        assert not handler.can_handle(RuntimeError("not a conflict"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _patch_tasks_protocol(tasks_mgr: MagicMock):
    """Patch resolve(TasksProtocol) to return a fake tasks manager."""
    from unittest.mock import patch

    return patch(
        "dynastore.tools.protocol_helpers.resolve",
        return_value=tasks_mgr,
    )
