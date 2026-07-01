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

"""``_handle_job_results`` §7.13 results-document projection (issue #2588).

``dwh_join``'s ``message`` alongside the declared output is a released,
frozen customer contract and must never change. New, non-frozen processes
(``joins_export``) opt in to a strict results document that carries only
declared output ids, via ``_STRICT_RESULTS_PROCESSES``.
"""

from __future__ import annotations

import uuid

import dynastore.extensions.processes.processes_service as svc
from dynastore.models.tasks import Task, TaskStatusEnum


def _completed_task(task_type: str, outputs: dict) -> Task:
    return Task(
        task_id=uuid.uuid4(),
        task_type=task_type,
        type="process",
        status=TaskStatusEnum.COMPLETED,
        catalog_id="s_cat",
        outputs=outputs,
    )


def test_dwh_join_keeps_message_alongside_declared_output():
    """Frozen customer contract: dwh_join's results document is untouched."""
    task = _completed_task(
        "dwh_join",
        {"message": "https://signed.example/x.gpkg",
         "result": {"href": "https://signed.example/x.gpkg", "type": "application/geopackage+sqlite3"}},
    )
    result = svc._handle_job_results(task, task.task_id)
    assert result["message"] == "https://signed.example/x.gpkg"
    assert result["result"]["href"] == "https://signed.example/x.gpkg"


def test_joins_export_projects_to_declared_outputs_only(monkeypatch):
    """joins_export opts into the strict §7.13 shape: no `message` member."""
    task = _completed_task(
        "joins_export",
        {"message": "https://signed.example/gaul.geojson",
         "result": {"href": "https://signed.example/gaul.geojson", "type": "application/geo+json"}},
    )

    class _FakeDefinition:
        outputs = {"result": object()}

    class _FakeCfg:
        definition = _FakeDefinition()

    monkeypatch.setattr(svc, "get_task_config", lambda task_type: _FakeCfg())

    result = svc._handle_job_results(task, task.task_id)
    assert "message" not in result
    assert result == {"result": {"href": "https://signed.example/gaul.geojson", "type": "application/geo+json"}}


def test_joins_export_falls_back_to_dropping_message_without_registry(monkeypatch):
    """When the task registry hasn't discovered `joins_export` (e.g. a remote
    runner context), the projection still drops `message` rather than serving
    an unfiltered document."""
    task = _completed_task(
        "joins_export",
        {"message": "https://signed.example/gaul.geojson",
         "result": {"href": "https://signed.example/gaul.geojson", "type": "application/geo+json"}},
    )

    monkeypatch.setattr(svc, "get_task_config", lambda task_type: None)

    result = svc._handle_job_results(task, task.task_id)
    assert "message" not in result
    assert result["result"]["href"] == "https://signed.example/gaul.geojson"


def test_other_processes_are_unaffected_by_default():
    """A process not in `_STRICT_RESULTS_PROCESSES` keeps the legacy shape,
    same as before this projection was added."""
    task = _completed_task(
        "export_features",
        {"message": "https://signed.example/y.gpkg",
         "result": {"href": "https://signed.example/y.gpkg", "type": "application/geopackage+sqlite3"}},
    )
    result = svc._handle_job_results(task, task.task_id)
    assert result["message"] == "https://signed.example/y.gpkg"
