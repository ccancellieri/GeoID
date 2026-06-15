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

"""The persisted ``type`` column is a denormalised cache of ``task_kind``.

``resolve_task_type_kind`` is the single helper both DB write paths
(``create_task`` and ``enqueue``) call, so a row's ``type`` reflects whether
its task ships an OGC Process definition — regardless of which runner created
it. This guards the regression where the OGC Processes execution path left the
``type`` column at its ``"task"`` default, mislabelling genuine processes such
as ``gdal`` and ``ingestion`` as system tasks.

Tests are tolerant of the loaded SCOPE: a task absent from the test
environment skips its specific sub-assertion; the universal invariant always
runs."""
from __future__ import annotations

from dynastore.tasks import (
    _DYNASTORE_TASKS,
    discover_tasks,
    resolve_task_type_kind,
    task_kind,
)

# Tasks that ship an OGC Process definition -> must classify as "process".
_PROCESS_TASK_KEYS = ("gdal", "ingestion", "tiles_invalidate")
# System tasks fired by events/listeners -> must classify as "task".
_SYSTEM_TASK_KEYS = ("gcp_provision_catalog",)


def test_resolve_matches_task_kind_for_every_registered_task():
    discover_tasks()
    assert _DYNASTORE_TASKS, "no tasks discovered"
    for key, cfg in _DYNASTORE_TASKS.items():
        resolved = resolve_task_type_kind(key)
        assert resolved == task_kind(cfg), key
        assert resolved in ("process", "task"), key


def test_processes_resolve_to_process_overriding_task_default():
    """The reported bug: gdal/ingestion were stored as ``type="task"``.

    Even when the caller-supplied default is ``"task"`` (the historical value
    every OGC-Processes runner left in place), a task that ships a Process
    definition must resolve to ``"process"``.
    """
    discover_tasks()
    for key in _PROCESS_TASK_KEYS:
        cfg = _DYNASTORE_TASKS.get(key)
        if cfg is None:
            continue  # not loaded in this SCOPE
        assert task_kind(cfg) == "process", key
        assert resolve_task_type_kind(key, "task") == "process", key


def test_system_tasks_resolve_to_task_overriding_process_default():
    """A system task stays ``"task"`` even if a caller mistakenly hints
    ``"process"`` — the registry is the single source of truth."""
    discover_tasks()
    for key in _SYSTEM_TASK_KEYS:
        cfg = _DYNASTORE_TASKS.get(key)
        if cfg is None:
            continue  # not loaded in this SCOPE
        assert task_kind(cfg) == "task", key
        assert resolve_task_type_kind(key, "process") == "task", key


def test_unknown_task_type_falls_back_to_default():
    # An unregistered task_type (e.g. a remote runner context where discovery
    # has not populated the registry) must not be silently relabelled.
    assert resolve_task_type_kind("definitely-not-a-real-task", "task") == "task"
    assert resolve_task_type_kind("definitely-not-a-real-task", "process") == "process"
