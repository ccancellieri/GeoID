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

"""Tests for ``_resolve_execution_mode`` covering the maps-service sync path.

Scenario: the maps service ships osgeo + worker_task_gdal + processes.  When a
client sends ``Prefer: respond-sync`` the engine must resolve SYNCHRONOUS if a
SyncRunner can handle the process, and fall back to ASYNCHRONOUS when no sync
runner / task instance is available (e.g. a catalog service without osgeo).

These complement the existing ``test_resolve_execution_mode.py`` tests, which
cover the general capability-awareness regression; these tests are specific to
the SYNC_EXECUTE → ASYNC_EXECUTE fall-through with a process advertising both
options — the core routing path for the maps-service gdal sync feature.
"""

from __future__ import annotations

import pytest

from dynastore.modules.processes import models, processes_module
from dynastore.modules.tasks.models import TaskExecutionMode

SYNC = models.JobControlOptions.SYNC_EXECUTE
ASYNC = models.JobControlOptions.ASYNC_EXECUTE


def _both_options_process(process_id: str = "gdal") -> models.Process:
    """A process that declares both SYNC_EXECUTE and ASYNC_EXECUTE."""
    return models.Process(
        id=process_id,
        title="GDAL Info",
        version="1.0.0",
        scopes=[models.ProcessScope.CATALOG, models.ProcessScope.COLLECTION],
        jobControlOptions=[SYNC, ASYNC],
        inputs={},
        outputs={},
    )


def _patch_runners_for(monkeypatch, sync_capable: bool, async_capable: bool) -> None:
    """Patch ``execution_engine.get_runners_for`` to control which modes are capable."""
    capable: set[TaskExecutionMode] = set()
    if sync_capable:
        capable.add(TaskExecutionMode.SYNCHRONOUS)
    if async_capable:
        capable.add(TaskExecutionMode.ASYNCHRONOUS)

    def fake_get_runners_for(task_type, mode, *, has_request_context=False):
        return ["runner"] if mode in capable else []

    monkeypatch.setattr(
        processes_module.execution_engine,
        "get_runners_for",
        fake_get_runners_for,
    )


# ---------------------------------------------------------------------------
# Maps service: in-process SyncRunner can handle the process
# ---------------------------------------------------------------------------


def test_sync_preferred_resolves_synchronous_when_sync_runner_present(monkeypatch):
    """Maps service with osgeo: Prefer: respond-sync → SYNCHRONOUS."""
    _patch_runners_for(monkeypatch, sync_capable=True, async_capable=True)
    proc = _both_options_process()
    result = processes_module._resolve_execution_mode(proc, SYNC)
    assert result == TaskExecutionMode.SYNCHRONOUS


def test_no_preference_resolves_synchronous_when_only_sync_available(monkeypatch):
    """When only SyncRunner can handle (no async runner), SYNCHRONOUS is chosen."""
    _patch_runners_for(monkeypatch, sync_capable=True, async_capable=False)
    proc = _both_options_process()
    result = processes_module._resolve_execution_mode(proc, None)
    assert result == TaskExecutionMode.SYNCHRONOUS


# ---------------------------------------------------------------------------
# Catalog service: no SyncRunner → falls back to async
# ---------------------------------------------------------------------------


def test_sync_preferred_falls_back_to_async_when_no_sync_runner(monkeypatch):
    """Catalog service without osgeo: SYNC preferred but incapable → falls back ASYNC."""
    _patch_runners_for(monkeypatch, sync_capable=False, async_capable=True)
    proc = _both_options_process()
    result = processes_module._resolve_execution_mode(proc, SYNC)
    assert result == TaskExecutionMode.ASYNCHRONOUS


def test_async_preferred_resolves_asynchronous_regardless_of_sync(monkeypatch):
    """Prefer: respond-async always lands on ASYNC when a runner can handle it."""
    _patch_runners_for(monkeypatch, sync_capable=True, async_capable=True)
    proc = _both_options_process()
    result = processes_module._resolve_execution_mode(proc, ASYNC)
    assert result == TaskExecutionMode.ASYNCHRONOUS


def test_raises_when_neither_mode_has_a_capable_runner(monkeypatch):
    """Service with no capable runner for any mode raises NotImplementedError."""
    _patch_runners_for(monkeypatch, sync_capable=False, async_capable=False)
    proc = _both_options_process()
    with pytest.raises(NotImplementedError, match="gdal"):
        processes_module._resolve_execution_mode(proc, SYNC)
