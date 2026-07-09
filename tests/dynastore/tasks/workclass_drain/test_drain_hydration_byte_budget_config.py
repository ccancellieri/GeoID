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

"""Regression: hydrated-payload dispatch must be byte-budget-bounded (#2723).

``storage_drain_batch_size`` (#2726) bounds ROW COUNT claimed per cycle, but
row count says nothing about payload weight: id-only rows (#2494 P1) re-read
canonical PG state and rebuild a full document per row, and MB-scale
geometries (e.g. GAUL polygons) could still OOM a container well before
``storage_drain_batch_size`` rows accumulated. The claim size is now
complemented by ``TasksPluginConfig.storage_drain_hydration_byte_budget``
(hot-reloaded, default 16 MiB), which bounds how much hydrated payload is
held before an ``index_bulk`` dispatch, independent of row count.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.tasks.tasks_config import TasksPluginConfig
from dynastore.tasks.workclass_drain.storage_drain_task import (
    _DEFAULT_HYDRATION_BYTE_BUDGET,
    StorageDrainTask,
)


def test_default_hydration_byte_budget_matches_config_field_default():
    field_default = TasksPluginConfig.model_fields[
        "storage_drain_hydration_byte_budget"
    ].default
    assert field_default == 16 * 1024 * 1024
    assert _DEFAULT_HYDRATION_BYTE_BUDGET == field_default
    assert StorageDrainTask().hydration_byte_budget == field_default


@pytest.mark.asyncio
async def test_resolve_hydration_byte_budget_reads_hot_config():
    class _FakeConfigMgr:
        async def get_config(self, cls):
            assert cls is TasksPluginConfig
            return TasksPluginConfig(storage_drain_hydration_byte_budget=65_536)

    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=_FakeConfigMgr()
    ):
        assert await StorageDrainTask()._resolve_hydration_byte_budget() == 65_536


@pytest.mark.asyncio
async def test_resolve_hydration_byte_budget_falls_back_without_protocol():
    with patch("dynastore.tools.discovery.get_protocol", return_value=None):
        task = StorageDrainTask(hydration_byte_budget=999)
        assert await task._resolve_hydration_byte_budget() == 999


@pytest.mark.asyncio
async def test_drain_once_threads_hydration_byte_budget_to_processing():
    """``drain_once``'s ``hydration_byte_budget`` argument reaches
    ``_process_driver_rows`` as the effective budget (``None`` falls back to
    the instance default)."""
    task = StorageDrainTask(hydration_byte_budget=777)
    captured = {}

    async def _fake_claim_batch(**kwargs):
        return [
            {
                "day": "2026-01-01", "op_id": "11111111-1111-1111-1111-111111111111",
                "driver_id": "es_driver", "catalog_id": "cat", "collection_id": "coll",
                "op": "upsert", "entity_id": "e1",
                "idempotency_key": "ik", "attempts": 0, "claim_version": 1,
                "claimed_by": "owner",
            },
        ]

    async def _fake_resolve_indexer(driver_id):
        return AsyncMock()

    async def _fake_process_driver_rows(**kwargs):
        captured.update(kwargs)
        return {"indexed": 0, "auto_done": 0, "retried": 0}

    with (
        patch.object(task, "_claim_batch", side_effect=_fake_claim_batch),
        patch.object(task, "_resolve_indexer", side_effect=_fake_resolve_indexer),
        patch.object(task, "_process_driver_rows", side_effect=_fake_process_driver_rows),
        patch(
            "dynastore.modules.tasks.tasks_module.get_task_schema",
            return_value="tasks",
        ),
    ):
        await task.drain_once(engine=AsyncMock(), owner_id="t", hydration_byte_budget=321)
        assert captured["byte_budget"] == 321

        captured.clear()
        await task.drain_once(engine=AsyncMock(), owner_id="t")
        assert captured["byte_budget"] == task.hydration_byte_budget
