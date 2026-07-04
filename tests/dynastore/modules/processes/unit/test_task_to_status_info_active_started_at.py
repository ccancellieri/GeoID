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

"""Regression coverage for GeoID #2893: an ACTIVE REMOTE task whose container
is still cold-starting (``started_at IS NULL``) must report OGC status
"accepted", not "running" — the API must not tell a caller a job is running
before its container has actually started.
"""
from __future__ import annotations

from datetime import datetime, timezone

from dynastore.models.tasks import Task, TaskStatusEnum
from dynastore.modules.processes.models import task_to_status_info


def test_active_with_no_started_at_reports_accepted():
    """A REMOTE task born ACTIVE at dispatch (owner/lock stamped, container
    not yet booted) has started_at NULL — must map to 'accepted'."""
    task = Task(task_type="ingest", status=TaskStatusEnum.ACTIVE, started_at=None)

    info = task_to_status_info(task)

    assert info.status == "accepted"


def test_active_with_started_at_reports_running():
    """Once claim_for_execution stamps started_at, the same ACTIVE status
    must map to 'running'."""
    task = Task(
        task_type="ingest",
        status=TaskStatusEnum.ACTIVE,
        started_at=datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc),
    )

    info = task_to_status_info(task)

    assert info.status == "running"


def test_running_legacy_alias_unaffected_by_started_at():
    """The RUNNING legacy status alias always means genuinely running —
    unlike ACTIVE it does not gate on started_at."""
    task = Task(task_type="ingest", status=TaskStatusEnum.RUNNING, started_at=None)

    info = task_to_status_info(task)

    assert info.status == "running"


def test_active_started_at_still_surfaced_on_status_info():
    """The top-level started/updated fields must reflect the live task even
    while status reports 'accepted' — only the OGC status label is gated."""
    task = Task(task_type="ingest", status=TaskStatusEnum.ACTIVE, started_at=None)

    info = task_to_status_info(task)

    assert info.started is None
    assert info.status == "accepted"
