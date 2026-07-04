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

"""``GET /jobs/{id}`` triggers an on-demand liveness reconcile before
serializing the response.

A Cloud Run Job container's SIGTERM-kill on ``taskTimeout`` leaves a row
``ACTIVE`` with a lapsing lease and no terminal write; without the on-demand
trigger, a poll of this route would report ``running`` forever until the next
periodic ``GcpLivenessReconciler`` pass. This test drives ``_get_job_internal``
(the shared helper behind the catalog- and collection-scoped job-status
routes) with a fake probe that reports ``TERMINAL_FAILED`` and confirms the
row comes back FAILED with an explanatory message — proving the wiring from
route → ``reconcile_task_liveness`` → owner-guarded ``fail_task`` actually
fires on a single-job GET.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

import dynastore.extensions.processes.processes_service as svc
from dynastore.models.tasks import Task, TaskStatusEnum
from dynastore.modules.processes import models as processes_models
from dynastore.modules.tasks import reconciliation as reconciliation_module
from dynastore.modules.tasks.liveness import LivenessVerdict


def _stale_active_job(job_id) -> Task:
    return Task(
        task_id=job_id,
        task_type="ingest",
        type="process",
        status=TaskStatusEnum.ACTIVE,
        catalog_id="s_cat",
        owner_id="gcp_cloud_run_abc123",
        locked_until=datetime.now(timezone.utc) - timedelta(seconds=30),
    )


class _FakeTerminalFailedProbe:
    runner_type = "gcp_cloud_run"

    def owns(self, owner_id):
        return True

    async def probe_liveness(self, task):
        return LivenessVerdict.TERMINAL_FAILED


@pytest.mark.asyncio
async def test_get_job_internal_reconciles_stale_active_row_to_failed(monkeypatch):
    job_id = uuid.uuid4()
    stale = _stale_active_job(job_id)

    reconciled = Task(
        task_id=job_id,
        task_type="ingest",
        type="process",
        status=TaskStatusEnum.FAILED,
        catalog_id="s_cat",
        error_message=(
            "Reconciled: remote execution terminated without reporting "
            "status (probe verdict=terminal_failed)."
        ),
    )

    async def fake_schema(catalog_id, conn):
        return "s_cat"

    async def fake_uncached(conn, jid):
        assert jid == job_id
        return stale

    fail_task_mock = AsyncMock(return_value=True)
    get_task_mock = AsyncMock(return_value=reconciled)

    monkeypatch.setattr(svc, "_resolve_catalog_schema", fake_schema)
    monkeypatch.setattr(svc.tasks_module, "get_task_by_id_unscoped", fake_uncached)
    monkeypatch.setattr(svc.tasks_module, "fail_task", fail_task_mock)
    monkeypatch.setattr(svc.tasks_module, "get_task", get_task_mock)
    monkeypatch.setattr(
        reconciliation_module, "resolve_probe",
        lambda owner_id: _FakeTerminalFailedProbe(),
    )

    task = await svc._get_job_internal(job_id, "cat", MagicMock())

    # The owner-guarded writer fired with the exact owner_id the probe saw.
    fail_task_mock.assert_awaited_once()
    assert fail_task_mock.await_args.kwargs.get("owner_id") == "gcp_cloud_run_abc123"
    assert fail_task_mock.await_args.kwargs.get("retry") is True

    # The route returns the freshly re-fetched (FAILED) row, not the stale one.
    assert task.status == TaskStatusEnum.FAILED

    status_info = processes_models.task_to_status_info(task)
    assert status_info.status == "failed"
    assert status_info.message is not None
    assert "Reconciled" in status_info.message


@pytest.mark.asyncio
async def test_get_job_internal_leaves_fresh_lease_running(monkeypatch):
    """Control case: a lease that has NOT lapsed must not be probed at all —
    the row is returned exactly as read, still ``running``."""
    job_id = uuid.uuid4()
    fresh = Task(
        task_id=job_id,
        task_type="ingest",
        type="process",
        status=TaskStatusEnum.ACTIVE,
        catalog_id="s_cat",
        owner_id="gcp_cloud_run_abc123",
        locked_until=datetime.now(timezone.utc) + timedelta(seconds=300),
        # Container already up and executing (#2893: ACTIVE with a NULL
        # started_at means still cold-starting -> "accepted", not "running").
        started_at=datetime.now(timezone.utc) - timedelta(seconds=60),
    )

    async def fake_schema(catalog_id, conn):
        return "s_cat"

    async def fake_uncached(conn, jid):
        return fresh

    probe_called = {"n": 0}

    class _CountingProbe:
        runner_type = "gcp_cloud_run"

        def owns(self, owner_id):
            return True

        async def probe_liveness(self, task):
            probe_called["n"] += 1
            return LivenessVerdict.ALIVE

    monkeypatch.setattr(svc, "_resolve_catalog_schema", fake_schema)
    monkeypatch.setattr(svc.tasks_module, "get_task_by_id_unscoped", fake_uncached)
    monkeypatch.setattr(
        reconciliation_module, "resolve_probe",
        lambda owner_id: _CountingProbe(),
    )

    task = await svc._get_job_internal(job_id, "cat", MagicMock())

    assert probe_called["n"] == 0, "a fresh (non-lapsed) lease must never reach the probe"
    status_info = processes_models.task_to_status_info(task)
    assert status_info.status == "running"
