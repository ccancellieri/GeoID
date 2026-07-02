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

"""Regression: the storage drain claim size must be config-bounded.

Id-only rows (#2494 P1) hydrate to full canonical documents for the WHOLE
claimed batch before the bulk dispatch, so the claim size bounds the run's
peak memory. The previous fixed 1500 OOM-killed both serving workers and
the 2Gi async_writer job on MB-scale geometries (#2723) — the claim size
is now ``TasksPluginConfig.storage_drain_batch_size`` (hot-reloaded,
default 100).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.tasks.tasks_config import TasksPluginConfig
from dynastore.tasks.workclass_drain.storage_drain_task import (
    _DEFAULT_BATCH_SIZE,
    StorageDrainTask,
)


def test_default_batch_size_matches_config_field_default():
    field_default = TasksPluginConfig.model_fields["storage_drain_batch_size"].default
    assert field_default == 100
    assert _DEFAULT_BATCH_SIZE == field_default
    assert StorageDrainTask().batch_size == field_default


@pytest.mark.asyncio
async def test_resolve_batch_size_reads_hot_config():
    class _FakeConfigMgr:
        async def get_config(self, cls):
            assert cls is TasksPluginConfig
            return TasksPluginConfig(storage_drain_batch_size=7)

    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=_FakeConfigMgr()
    ):
        assert await StorageDrainTask()._resolve_batch_size() == 7


@pytest.mark.asyncio
async def test_resolve_batch_size_falls_back_without_protocol():
    with patch("dynastore.tools.discovery.get_protocol", return_value=None):
        task = StorageDrainTask(batch_size=42)
        assert await task._resolve_batch_size() == 42


@pytest.mark.asyncio
async def test_drain_once_threads_batch_size_to_claim():
    task = StorageDrainTask()
    captured = {}

    async def _fake_claim_batch(**kwargs):
        captured.update(kwargs)
        return []

    with (
        patch.object(task, "_claim_batch", side_effect=_fake_claim_batch),
        patch(
            "dynastore.modules.tasks.tasks_module.get_task_schema",
            return_value="tasks",
        ),
    ):
        n = await task.drain_once(engine=AsyncMock(), owner_id="t", batch_size=9)
        assert n == 0
        assert captured["batch_size"] == 9

        captured.clear()
        n = await task.drain_once(engine=AsyncMock(), owner_id="t")
        assert n == 0
        assert captured["batch_size"] == task.batch_size
