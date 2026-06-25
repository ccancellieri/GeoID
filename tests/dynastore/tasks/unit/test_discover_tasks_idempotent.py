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

"""Guard: discover_tasks() is idempotent per process.

A full scan re-imports every plugin module (~12-35ms each). Two bootstraps
already call discover_tasks() at startup, and before #2341's call-site guard
every POST .../processes/{id}/execution triggered another scan (~97 redundant
scans were measured under burst). The function now short-circuits on a
non-empty registry; the clear-then-rediscover idiom and an explicit
``force=True`` reload still re-run a full scan.

Tests:
- registry populated         → no entry-point scan happens
- registry populated + force → a scan happens
- registry empty             → a scan happens
"""
from __future__ import annotations

from typing import Type, cast
from unittest.mock import patch

import pytest

import dynastore.tasks as tasks
from dynastore.tasks import discover_tasks, _DYNASTORE_TASKS, TaskConfig
from dynastore.tasks.protocols import TaskProtocol


@pytest.fixture
def restore_registry():
    """Snapshot and restore the module-level registry around each test so the
    idempotency probing here never leaks into the rest of the suite."""
    snapshot = dict(_DYNASTORE_TASKS)
    try:
        yield
    finally:
        _DYNASTORE_TASKS.clear()
        _DYNASTORE_TASKS.update(snapshot)


def _dummy_config() -> TaskConfig:
    return TaskConfig(
        cls=cast(Type[TaskProtocol], object),
        type="task",
        module_name="dummy",
        name="dummy_task",
        definition=None,
    )


def test_discover_skips_scan_when_registry_populated(restore_registry) -> None:
    """A non-empty registry must short-circuit before the entry-point scan."""
    _DYNASTORE_TASKS.clear()
    _DYNASTORE_TASKS["dummy_task"] = _dummy_config()

    with patch(
        "dynastore.tools.discovery.discover_and_load_plugins"
    ) as mock_scan:
        discover_tasks()

    mock_scan.assert_not_called()


def test_discover_force_scans_even_when_populated(restore_registry) -> None:
    """force=True bypasses the guard so an explicit reload re-scans."""
    _DYNASTORE_TASKS.clear()
    _DYNASTORE_TASKS["dummy_task"] = _dummy_config()

    with (
        patch(
            "dynastore.tools.discovery.discover_and_load_plugins",
            return_value={},
        ) as mock_scan,
        patch.object(tasks, "_register_definition_only_placeholders"),
    ):
        discover_tasks(force=True)

    mock_scan.assert_called_once()


def test_discover_scans_when_registry_empty(restore_registry) -> None:
    """An empty registry must run the scan — preserves clear-then-rediscover."""
    _DYNASTORE_TASKS.clear()

    with (
        patch(
            "dynastore.tools.discovery.discover_and_load_plugins",
            return_value={},
        ) as mock_scan,
        patch.object(tasks, "_register_definition_only_placeholders"),
    ):
        discover_tasks()

    mock_scan.assert_called_once()
