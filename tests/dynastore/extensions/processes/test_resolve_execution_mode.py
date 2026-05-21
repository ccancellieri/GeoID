"""Unit tests for ``_resolve_execution_mode`` capability-awareness.

Regression: mode selection used to check "does *any* runner exist for this
mode" (``runners.get_runners(mode)``) rather than "can *some* runner actually
run *this process* in this mode". On a service whose in-process worker can't
claim ``gdal`` (no osgeo) it would still pick SYNCHRONOUS because a SyncRunner
exists, then fail late at execute time. The fix routes the decision through
``execution_engine.get_runners_for(process.id, mode, ...)`` (``can_handle``)
so a deployment without an in-process GDAL runtime resolves ``gdal`` to the
async Cloud Run job runner instead.
"""

from __future__ import annotations

import pytest

from dynastore.modules.processes import models, processes_module
from dynastore.modules.tasks.models import TaskExecutionMode

SYNC = models.JobControlOptions.SYNC_EXECUTE
ASYNC = models.JobControlOptions.ASYNC_EXECUTE


def _process(job_control):
    return models.Process(
        id="gdal",
        title="GDAL Info",
        version="1.0.0",
        scopes=[models.ProcessScope.ASSET],
        jobControlOptions=job_control,
        inputs={},
        outputs={},
    )


def _patch_capability(monkeypatch, capable_modes):
    """Make ``get_runners_for`` report capability only for ``capable_modes``."""

    def fake_get_runners_for(task_type, mode, *, has_request_context=False):
        return ["runner"] if mode in capable_modes else []

    monkeypatch.setattr(
        processes_module.execution_engine,
        "get_runners_for",
        fake_get_runners_for,
    )


def test_preferred_mode_used_when_a_runner_can_handle_it(monkeypatch):
    _patch_capability(
        monkeypatch,
        {TaskExecutionMode.SYNCHRONOUS, TaskExecutionMode.ASYNCHRONOUS},
    )
    proc = _process([SYNC, ASYNC])
    assert (
        processes_module._resolve_execution_mode(proc, SYNC)
        == TaskExecutionMode.SYNCHRONOUS
    )
    assert (
        processes_module._resolve_execution_mode(proc, ASYNC)
        == TaskExecutionMode.ASYNCHRONOUS
    )


def test_falls_through_to_capable_mode_when_preferred_uncapable(monkeypatch):
    # SYNC is preferred AND declared, but no runner can handle the process in
    # SYNC here (e.g. catalog API without osgeo). Must fall through to ASYNC.
    _patch_capability(monkeypatch, {TaskExecutionMode.ASYNCHRONOUS})
    proc = _process([SYNC, ASYNC])
    assert (
        processes_module._resolve_execution_mode(proc, SYNC)
        == TaskExecutionMode.ASYNCHRONOUS
    )


def test_sync_chosen_where_in_process_runtime_present(monkeypatch):
    # Worker that carries the runtime in-process (e.g. maps with GDAL): the
    # process resolves to SYNCHRONOUS even with no preference.
    _patch_capability(monkeypatch, {TaskExecutionMode.SYNCHRONOUS})
    proc = _process([SYNC, ASYNC])
    assert (
        processes_module._resolve_execution_mode(proc, None)
        == TaskExecutionMode.SYNCHRONOUS
    )


def test_raises_when_no_runner_can_handle_any_mode(monkeypatch):
    _patch_capability(monkeypatch, set())
    proc = _process([SYNC, ASYNC])
    with pytest.raises(NotImplementedError):
        processes_module._resolve_execution_mode(proc, None)


def test_request_context_is_threaded_into_capability_check(monkeypatch):
    seen = {}

    def fake_get_runners_for(task_type, mode, *, has_request_context=False):
        seen["has_request_context"] = has_request_context
        return ["runner"]

    monkeypatch.setattr(
        processes_module.execution_engine,
        "get_runners_for",
        fake_get_runners_for,
    )
    proc = _process([ASYNC])
    processes_module._resolve_execution_mode(proc, None, has_request_context=True)
    assert seen["has_request_context"] is True
