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

"""Unit tests for the #2622 aggregate outbox-backlog signal.

Covers: the capped-count query shape, fail-open behaviour when the engine or
config protocol is unavailable, and the threshold comparison in
``backlog_is_high``. The signal must NEVER raise — every failure path degrades
to "not high" (the existing in-process drain path stays available).
"""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from dynastore.modules.tasks import async_writer_backlog as backlog


@pytest.fixture(autouse=True)
def _clear_backlog_cache():
    """Ensure the short-TTL cache never bleeds state across tests."""
    yield
    if hasattr(backlog._cached_backlog_depth, "cache_clear"):
        backlog._cached_backlog_depth.cache_clear()


# ---------------------------------------------------------------------------
# _capped_ready_count
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_capped_ready_count_sums_storage_and_events(monkeypatch):
    calls = []

    class _FakeQuery:
        def __init__(self, sql, result_handler=None):
            self.sql = sql

        async def execute(self, conn, **kwargs):
            calls.append((self.sql, kwargs))
            if ".storage" in self.sql:
                return 30
            return 12

    monkeypatch.setattr(
        "dynastore.modules.db_config.query_executor.DQLQuery", _FakeQuery,
    )

    total = await backlog._capped_ready_count(
        conn=object(), task_schema="tasks", cap=5000,
    )
    assert total == 42
    assert len(calls) == 2
    # Both queries are capped (bounded scan), never a bare COUNT(*).
    assert all("LIMIT :cap" in sql for sql, _ in calls)
    assert all(kwargs.get("cap") == 5000 for _, kwargs in calls)


# ---------------------------------------------------------------------------
# _cached_backlog_depth — fail-open behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cached_backlog_depth_zero_when_no_engine(monkeypatch):
    monkeypatch.setattr(
        "dynastore.tools.protocol_helpers.get_engine", lambda *a, **k: None,
    )
    assert await backlog._cached_backlog_depth() == 0


@pytest.mark.asyncio
async def test_cached_backlog_depth_fails_open_on_exception(monkeypatch):
    def _boom(*a, **k):
        raise RuntimeError("pool exhausted")

    monkeypatch.setattr("dynastore.tools.protocol_helpers.get_engine", _boom)
    assert await backlog._cached_backlog_depth() == 0


# ---------------------------------------------------------------------------
# backlog_is_high — threshold comparison + fail-open
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backlog_is_high_true_above_threshold(monkeypatch):
    monkeypatch.setattr(backlog, "_cached_backlog_depth", AsyncMock(return_value=2500))
    monkeypatch.setattr(backlog, "_resolve_threshold", AsyncMock(return_value=2000))
    assert await backlog.backlog_is_high() is True


@pytest.mark.asyncio
async def test_backlog_is_high_false_at_threshold(monkeypatch):
    """Exactly at the threshold is NOT high — strict greater-than."""
    monkeypatch.setattr(backlog, "_cached_backlog_depth", AsyncMock(return_value=2000))
    monkeypatch.setattr(backlog, "_resolve_threshold", AsyncMock(return_value=2000))
    assert await backlog.backlog_is_high() is False


@pytest.mark.asyncio
async def test_backlog_is_high_false_below_threshold(monkeypatch):
    monkeypatch.setattr(backlog, "_cached_backlog_depth", AsyncMock(return_value=10))
    monkeypatch.setattr(backlog, "_resolve_threshold", AsyncMock(return_value=2000))
    assert await backlog.backlog_is_high() is False


@pytest.mark.asyncio
async def test_backlog_is_high_fails_open_on_depth_probe_error(monkeypatch):
    async def _boom():
        raise RuntimeError("boom")

    monkeypatch.setattr(backlog, "_cached_backlog_depth", _boom)
    monkeypatch.setattr(backlog, "_resolve_threshold", AsyncMock(return_value=2000))
    assert await backlog.backlog_is_high() is False


@pytest.mark.asyncio
async def test_backlog_is_high_fails_open_on_threshold_resolution_error(monkeypatch):
    async def _boom():
        raise RuntimeError("boom")

    monkeypatch.setattr(backlog, "_cached_backlog_depth", AsyncMock(return_value=999999))
    monkeypatch.setattr(backlog, "_resolve_threshold", _boom)
    assert await backlog.backlog_is_high() is False


# ---------------------------------------------------------------------------
# _resolve_threshold — config fail-open
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_threshold_falls_back_to_default_without_config_protocol(monkeypatch):
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol", lambda *a, **k: None,
    )
    from dynastore.modules.tasks.tasks_config import TasksPluginConfig

    default = TasksPluginConfig.model_fields["async_writer_backlog_threshold"].default
    assert await backlog._resolve_threshold() == default
