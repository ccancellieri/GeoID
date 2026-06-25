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

"""Guard: list_processes() must not call discover_tasks() when the registry
is already populated.

Under stress (many concurrent POST .../processes/{id}/execution requests) the
old unconditional discover_tasks() call was measured at ~97 redundant
entry-point scans per burst.  The guard added in #2341 short-circuits to a
no-op when get_loaded_task_types() is non-empty.

Tests:
- registry populated  → discover_tasks is NOT called
- registry empty      → discover_tasks IS called
"""
from __future__ import annotations

from unittest.mock import patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_module() -> object:
    """Import TasksModule lazily so the test stays isolated from DB deps."""
    from dynastore.modules.tasks.tasks_module import TasksModule
    return TasksModule()


# ---------------------------------------------------------------------------
# Registry populated → discover_tasks must NOT be called
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_processes_skips_discover_when_registry_populated() -> None:
    """When get_loaded_task_types() returns a non-empty list, discover_tasks
    must not be called — the registry is already warmed up."""
    module = _make_module()

    with (
        patch(
            "dynastore.tasks.get_loaded_task_types",
            return_value=["some_task"],
        ),
        patch(
            "dynastore.tasks.discover_tasks",
        ) as mock_discover,
        patch(
            "dynastore.modules.gcp.tools.jobs.try_load_process_definition",
            return_value=None,
        ),
    ):
        await module.list_processes()

    mock_discover.assert_not_called()


# ---------------------------------------------------------------------------
# Registry empty → discover_tasks must BE called
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_processes_calls_discover_when_registry_empty() -> None:
    """When get_loaded_task_types() returns an empty list, discover_tasks
    must be called so the registry is populated on first use."""
    module = _make_module()

    with (
        patch(
            "dynastore.tasks.get_loaded_task_types",
            return_value=[],
        ),
        patch(
            "dynastore.tasks.discover_tasks",
        ) as mock_discover,
        patch(
            "dynastore.modules.gcp.tools.jobs.try_load_process_definition",
            return_value=None,
        ),
    ):
        await module.list_processes()

    mock_discover.assert_called_once()
