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

"""Unit tests for the liveness-aware orphan task dir reaper.

Pure Python — no osgeo / GDAL import required.  TasksProtocol and
TempDirProtocol are both monkeypatched so these tests run in the plain venv.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.tasks import TaskStatusEnum
from dynastore.models.protocols.temp_dir import TASK_DIR_PREFIX
from dynastore.tasks.ingestion.temp_reaper import (
    _LOOKUP_ERROR,
    _TERMINAL,
    reap_orphan_task_dirs,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ENGINE = object()  # dummy engine; never actually called (get_task is mocked)

_TASK_ID = str(uuid.uuid4())
_SCHEMA = "c_test_catalog"


def _make_dir(tmp_path, task_id=_TASK_ID, schema=_SCHEMA, write_sidecar=True):
    """Create a ``dynastore_task_*`` directory under *tmp_path* with an .owner sidecar."""
    d = tmp_path / f"{TASK_DIR_PREFIX}{task_id}_"
    d.mkdir()
    if write_sidecar:
        (d / ".owner").write_text(json.dumps({"task_id": task_id, "schema": schema}))
    return d


def _task(status: TaskStatusEnum):
    return SimpleNamespace(status=status)


def _mock_tasks_mgr(return_value):
    """Return a mock TasksProtocol whose get_task is an AsyncMock returning *return_value*."""
    mgr = MagicMock()
    mgr.get_task = AsyncMock(return_value=return_value)
    return mgr


# ---------------------------------------------------------------------------
# Terminal status: dir must be removed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("status", [
    TaskStatusEnum.COMPLETED,
    TaskStatusEnum.FAILED,
    TaskStatusEnum.DISMISSED,
    TaskStatusEnum.DEAD_LETTER,
])
async def test_reaps_terminal_task(tmp_path, monkeypatch, status):
    d = _make_dir(tmp_path)
    # Force the dir to look old so the age guard passes.
    os.utime(str(d), (0, 0))

    mgr = _mock_tasks_mgr(_task(status))
    with patch("dynastore.tools.protocol_helpers.resolve", return_value=mgr):
        count = await reap_orphan_task_dirs(
            _ENGINE, tmpdir=str(tmp_path), min_age_seconds=0
        )

    assert count == 1, f"expected 1 reclaim for status {status}"
    assert not d.exists(), f"dir should be removed for terminal status {status}"


# ---------------------------------------------------------------------------
# Alive task: dir must be kept
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("status", [
    TaskStatusEnum.ACTIVE,
    TaskStatusEnum.RUNNING,
    TaskStatusEnum.PENDING,
    TaskStatusEnum.CREATED,
])
async def test_keeps_alive_task(tmp_path, monkeypatch, status):
    d = _make_dir(tmp_path)
    os.utime(str(d), (0, 0))

    mgr = _mock_tasks_mgr(_task(status))
    with patch("dynastore.tools.protocol_helpers.resolve", return_value=mgr):
        count = await reap_orphan_task_dirs(
            _ENGINE, tmpdir=str(tmp_path), min_age_seconds=0
        )

    assert count == 0, f"should not reap alive status {status}"
    assert d.exists(), f"dir must survive for alive status {status}"


# ---------------------------------------------------------------------------
# Task gone (get_task returns None): dir must be removed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reaps_gone_task(tmp_path):
    d = _make_dir(tmp_path)
    os.utime(str(d), (0, 0))

    mgr = _mock_tasks_mgr(None)  # task not found in DB
    with patch("dynastore.tools.protocol_helpers.resolve", return_value=mgr):
        count = await reap_orphan_task_dirs(
            _ENGINE, tmpdir=str(tmp_path), min_age_seconds=0
        )

    assert count == 1
    assert not d.exists()


# ---------------------------------------------------------------------------
# No .owner sidecar: dir must be kept
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_keeps_dir_without_sidecar(tmp_path):
    d = _make_dir(tmp_path, write_sidecar=False)
    os.utime(str(d), (0, 0))

    mgr = _mock_tasks_mgr(None)
    with patch("dynastore.tools.protocol_helpers.resolve", return_value=mgr):
        count = await reap_orphan_task_dirs(
            _ENGINE, tmpdir=str(tmp_path), min_age_seconds=0
        )

    assert count == 0
    assert d.exists(), "dir without .owner sidecar must be kept"


# ---------------------------------------------------------------------------
# Invalid/unreadable .owner JSON: dir must be kept
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_keeps_dir_with_invalid_sidecar(tmp_path):
    d = _make_dir(tmp_path, write_sidecar=False)
    (d / ".owner").write_text("NOT_JSON{{{{")
    os.utime(str(d), (0, 0))

    mgr = _mock_tasks_mgr(None)
    with patch("dynastore.tools.protocol_helpers.resolve", return_value=mgr):
        count = await reap_orphan_task_dirs(
            _ENGINE, tmpdir=str(tmp_path), min_age_seconds=0
        )

    assert count == 0
    assert d.exists(), "dir with invalid .owner JSON must be kept"


# ---------------------------------------------------------------------------
# Young dir (mtime too recent): must be skipped even if owner is terminal
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_skips_young_dir(tmp_path):
    d = _make_dir(tmp_path)
    # Dir mtime is right now; min_age_seconds is large — should be skipped.

    mgr = _mock_tasks_mgr(_task(TaskStatusEnum.COMPLETED))
    with patch("dynastore.tools.protocol_helpers.resolve", return_value=mgr):
        count = await reap_orphan_task_dirs(
            _ENGINE, tmpdir=str(tmp_path), min_age_seconds=3600
        )

    assert count == 0
    assert d.exists(), "young dir must not be reaped even if owner is terminal"


# ---------------------------------------------------------------------------
# DB lookup error: dir must be kept (fail-safe)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_keeps_dir_on_lookup_error(tmp_path):
    d = _make_dir(tmp_path)
    os.utime(str(d), (0, 0))

    # resolve() raises so _lookup_task returns _LOOKUP_ERROR
    with patch(
        "dynastore.tools.protocol_helpers.resolve",
        side_effect=RuntimeError("no db"),
    ):
        # Also patch get_task_by_id_unscoped to raise (unscoped fallback path)
        with patch(
            "dynastore.modules.tasks.tasks_module.get_task_by_id_unscoped",
            new=AsyncMock(side_effect=RuntimeError("no db")),
        ):
            count = await reap_orphan_task_dirs(
                _ENGINE, tmpdir=str(tmp_path), min_age_seconds=0
            )

    assert count == 0
    assert d.exists(), "dir must be kept when task liveness cannot be determined"


# ---------------------------------------------------------------------------
# TempDirProtocol: TASK_DIR_PREFIX constant is non-empty and stable
# ---------------------------------------------------------------------------

def test_task_dir_prefix_is_non_empty():
    assert TASK_DIR_PREFIX, "TASK_DIR_PREFIX must be a non-empty string"


# ---------------------------------------------------------------------------
# _TERMINAL set contains exactly the four expected statuses
# ---------------------------------------------------------------------------

def test_terminal_set_contents():
    assert _TERMINAL == frozenset({
        TaskStatusEnum.COMPLETED,
        TaskStatusEnum.FAILED,
        TaskStatusEnum.DISMISSED,
        TaskStatusEnum.DEAD_LETTER,
    })
