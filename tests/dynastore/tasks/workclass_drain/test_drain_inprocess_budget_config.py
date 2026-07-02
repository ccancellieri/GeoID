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

"""Regression: the in-process drain budget must be config-bounded (#2732 step 4).

``StorageDrainTask`` always starts in-process; once its cumulative hydrated
byte total or wall-clock elapsed crosses
``TasksPluginConfig.storage_drain_inprocess_max_bytes`` /
``storage_drain_inprocess_max_seconds`` (hot-reloaded) with backlog rows
still remaining, it hands the remainder off to ``storage_drain_offload``
instead of continuing to hold the catalog API pod's request-serving
capacity. Mirrors the ``_resolve_batch_size`` / ``_resolve_hydration_byte_budget``
hot-reload test pattern.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from dynastore.modules.tasks.tasks_config import TasksPluginConfig
from dynastore.tasks.workclass_drain.storage_drain_task import (
    _DEFAULT_INPROCESS_MAX_BYTES,
    _DEFAULT_INPROCESS_MAX_SECONDS,
    StorageDrainTask,
)


def test_default_inprocess_budget_matches_config_field_defaults():
    bytes_default = TasksPluginConfig.model_fields[
        "storage_drain_inprocess_max_bytes"
    ].default
    seconds_default = TasksPluginConfig.model_fields[
        "storage_drain_inprocess_max_seconds"
    ].default
    assert bytes_default == 32 * 1024 * 1024
    assert seconds_default == 5.0
    assert _DEFAULT_INPROCESS_MAX_BYTES == bytes_default
    assert _DEFAULT_INPROCESS_MAX_SECONDS == seconds_default
    task = StorageDrainTask()
    assert task.inprocess_max_bytes == bytes_default
    assert task.inprocess_max_seconds == seconds_default


@pytest.mark.asyncio
async def test_resolve_inprocess_budget_reads_hot_config():
    class _FakeConfigMgr:
        async def get_config(self, cls):
            assert cls is TasksPluginConfig
            return TasksPluginConfig(
                storage_drain_inprocess_max_bytes=123,
                storage_drain_inprocess_max_seconds=1.5,
            )

    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=_FakeConfigMgr()
    ):
        assert await StorageDrainTask()._resolve_inprocess_budget() == (123, 1.5)


@pytest.mark.asyncio
async def test_resolve_inprocess_budget_falls_back_without_protocol():
    with patch("dynastore.tools.discovery.get_protocol", return_value=None):
        task = StorageDrainTask(inprocess_max_bytes=999, inprocess_max_seconds=2.0)
        assert await task._resolve_inprocess_budget() == (999, 2.0)


@pytest.mark.asyncio
async def test_resolve_inprocess_budget_falls_back_on_config_read_error():
    class _BoomConfigMgr:
        async def get_config(self, cls):
            raise RuntimeError("platform configs unavailable")

    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=_BoomConfigMgr()
    ):
        task = StorageDrainTask(inprocess_max_bytes=42, inprocess_max_seconds=3.0)
        assert await task._resolve_inprocess_budget() == (42, 3.0)
