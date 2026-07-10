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

"""Unit tests for :func:`_maybe_apply_ingest_backpressure` (#2494).

Cooperative backpressure fires unconditionally whenever the aggregate
outbox backlog is high — items INDEX materialization is
storage-plane-always by design (#2494 WP-I), so there is no flag gate any
more; only the backlog check itself governs whether the sleep fires.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _fake_config_mgr(cfg: object) -> MagicMock:
    mgr = MagicMock()
    mgr.get_config = AsyncMock(return_value=cfg)
    return mgr


@pytest.mark.asyncio
async def test_backpressure_noop_when_backlog_not_high():
    from dynastore.modules.tasks.tasks_config import TasksPluginConfig
    from dynastore.tasks.ingestion.main_ingestion import _maybe_apply_ingest_backpressure

    cfg = TasksPluginConfig()
    mgr = _fake_config_mgr(cfg)

    with (
        patch(
            "dynastore.tools.discovery.get_protocol", return_value=mgr,
        ),
        patch(
            "dynastore.modules.tasks.async_writer_backlog.backlog_is_high",
            new=AsyncMock(return_value=False),
        ) as backlog_mock,
        patch("asyncio.sleep", new=AsyncMock()) as sleep_mock,
    ):
        await _maybe_apply_ingest_backpressure()

    backlog_mock.assert_called_once()
    sleep_mock.assert_not_called()


@pytest.mark.asyncio
async def test_backpressure_sleeps_when_backlog_high():
    from dynastore.modules.tasks.tasks_config import TasksPluginConfig
    from dynastore.tasks.ingestion.main_ingestion import _maybe_apply_ingest_backpressure

    cfg = TasksPluginConfig(ingest_backpressure_sleep_seconds=3.5)
    mgr = _fake_config_mgr(cfg)

    with (
        patch(
            "dynastore.tools.discovery.get_protocol", return_value=mgr,
        ),
        patch(
            "dynastore.modules.tasks.async_writer_backlog.backlog_is_high",
            new=AsyncMock(return_value=True),
        ) as backlog_mock,
        patch("asyncio.sleep", new=AsyncMock()) as sleep_mock,
    ):
        await _maybe_apply_ingest_backpressure()

    backlog_mock.assert_called_once()
    sleep_mock.assert_called_once_with(3.5)


@pytest.mark.asyncio
async def test_backpressure_noop_when_no_configs_protocol():
    """No platform configs registered — fails open (no sleep, no raise)."""
    from dynastore.tasks.ingestion.main_ingestion import _maybe_apply_ingest_backpressure

    with (
        patch("dynastore.tools.discovery.get_protocol", return_value=None),
        patch("asyncio.sleep", new=AsyncMock()) as sleep_mock,
    ):
        await _maybe_apply_ingest_backpressure()

    sleep_mock.assert_not_called()


@pytest.mark.asyncio
async def test_backpressure_never_raises_on_probe_failure():
    """A config/backlog probe error must never propagate into ingestion."""
    from dynastore.tasks.ingestion.main_ingestion import _maybe_apply_ingest_backpressure

    with patch(
        "dynastore.tools.discovery.get_protocol",
        side_effect=RuntimeError("boom"),
    ):
        await _maybe_apply_ingest_backpressure()  # must not raise
