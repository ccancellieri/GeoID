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

"""``GET /jobs`` (and its catalog/collection-scoped siblings) never healed a
stale ``running`` row before serializing it — only the single-job GET path
did, via ``reconcile_task_liveness``. A job discovered through the
documented list-jobs path could therefore report stale ``running`` forever
between periodic reconciler passes (#2806).

These tests drive the shared helper ``_reconcile_lapsed_list_tasks`` that all
three list routes now call between ``tasks_module.list_tasks`` and building
``StatusInfo``, covering:

- a lapsed (``ACTIVE`` + expired lease) row gets probed and comes back healed
- a healthy row (fresh lease, or already terminal) is never probed
- the per-request probe cap is respected even with many lapsed rows
- a probe exception never fails the list — the original row is kept
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

import dynastore.extensions.processes.processes_service as svc
from dynastore.modules.tasks.models import Task, TaskStatusEnum


def _lapsed_task(job_id=None, *, seconds_past: int = 30) -> Task:
    return Task(
        task_id=job_id or uuid.uuid4(),
        task_type="ingest",
        type="process",
        status=TaskStatusEnum.ACTIVE,
        catalog_id="s_cat",
        owner_id="gcp_cloud_run_abc123",
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=seconds_past),
    )


def _fresh_task(job_id=None) -> Task:
    return Task(
        task_id=job_id or uuid.uuid4(),
        task_type="ingest",
        type="process",
        status=TaskStatusEnum.ACTIVE,
        catalog_id="s_cat",
        owner_id="gcp_cloud_run_abc123",
        locked_until=datetime.now(timezone.utc) + timedelta(seconds=300),
    )


def _terminal_task(job_id=None) -> Task:
    return Task(
        task_id=job_id or uuid.uuid4(),
        task_type="ingest",
        type="process",
        status=TaskStatusEnum.COMPLETED,
        catalog_id="s_cat",
    )


@pytest.mark.asyncio
async def test_lapsed_row_is_probed_and_healed(monkeypatch):
    job_id = uuid.uuid4()
    lapsed = _lapsed_task(job_id)
    healed = Task(
        task_id=job_id,
        task_type="ingest",
        type="process",
        status=TaskStatusEnum.FAILED,
        catalog_id="s_cat",
        error_message="Reconciled: remote execution terminated without reporting status.",
    )

    reconcile_mock = AsyncMock(return_value=healed)
    monkeypatch.setattr(svc, "reconcile_task_liveness", reconcile_mock)

    result = await svc._reconcile_lapsed_list_tasks(MagicMock(), [lapsed], schema="s_cat")

    reconcile_mock.assert_awaited_once()
    assert reconcile_mock.await_args.kwargs.get("schema") == "s_cat"
    assert result[0].status == TaskStatusEnum.FAILED


@pytest.mark.asyncio
async def test_healthy_rows_are_never_probed(monkeypatch):
    fresh = _fresh_task()
    terminal = _terminal_task()

    reconcile_mock = AsyncMock(side_effect=AssertionError("must not be called"))
    monkeypatch.setattr(svc, "reconcile_task_liveness", reconcile_mock)

    result = await svc._reconcile_lapsed_list_tasks(
        MagicMock(), [fresh, terminal], schema="s_cat"
    )

    reconcile_mock.assert_not_awaited()
    assert result == [fresh, terminal]


@pytest.mark.asyncio
async def test_probe_cap_respected_with_many_lapsed_rows(monkeypatch):
    lapsed_rows = [_lapsed_task() for _ in range(8)]

    async def fake_reconcile(conn, task, *, schema):
        return task

    reconcile_mock = AsyncMock(side_effect=fake_reconcile)
    monkeypatch.setattr(svc, "reconcile_task_liveness", reconcile_mock)

    result = await svc._reconcile_lapsed_list_tasks(MagicMock(), lapsed_rows, schema="s_cat")

    assert reconcile_mock.await_count == svc._LIST_RECONCILE_MAX_PROBES
    assert len(result) == len(lapsed_rows)


@pytest.mark.asyncio
async def test_probe_exception_does_not_fail_the_list(monkeypatch):
    lapsed = _lapsed_task()

    reconcile_mock = AsyncMock(side_effect=RuntimeError("executions API unreachable"))
    monkeypatch.setattr(svc, "reconcile_task_liveness", reconcile_mock)

    result = await svc._reconcile_lapsed_list_tasks(MagicMock(), [lapsed], schema="s_cat")

    reconcile_mock.assert_awaited_once()
    # Best-effort: the original (unreconciled) row is returned, not raised.
    assert result[0] is lapsed
    assert result[0].status == TaskStatusEnum.ACTIVE
