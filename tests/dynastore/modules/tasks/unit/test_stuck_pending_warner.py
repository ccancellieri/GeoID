"""Unit tests for ``tasks_module._warn_stuck_pending_tasks``.

The warner is a periodic read-only scan that logs WARN lines for tasks that
have been PENDING with retry_count=0 for too long — typically caused by a
TaskRoutingConfig typo or a service that should claim them but isn't deployed.

We mock the SQL execution so the test doesn't need a real PG. The contract:
- when ``shutdown_event`` fires immediately, the loop returns cleanly.
- when the query returns rows, each row produces one WARN log line.
- when the query raises, the warner logs and continues (doesn't crash).
"""
from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.tasks.tasks_module import (
    _emit_stuck_pending_logs,
    _resolve_row_capability,
    _safe_is_live,
    _stuck_pending_hint,
    _warn_stuck_pending_tasks,
)


@pytest.mark.asyncio
async def test_warner_exits_immediately_on_shutdown(caplog):
    """If shutdown_event is already set, the loop returns without scanning."""
    shutdown = asyncio.Event()
    shutdown.set()
    caplog.set_level(logging.WARNING)
    with patch.object(
        __import__("dynastore.modules.tasks.tasks_module", fromlist=["DQLQuery"]),
        "DQLQuery",
    ):
        await asyncio.wait_for(
            _warn_stuck_pending_tasks(
                engine=object(), schema="tasks", shutdown_event=shutdown,
                interval_s=0.01,
            ),
            timeout=1.0,
        )


@pytest.mark.asyncio
async def test_warner_logs_one_line_per_stuck_row(caplog):
    """Each stuck row from the query produces one WARN log line."""
    shutdown = asyncio.Event()
    fake_rows = [
        {"task_id": "abc-1", "task_type": "elasticsearch_index",
         "schema_name": "s_x", "age_s": 999.0},
        {"task_id": "abc-2", "task_type": "tile_preseed",
         "schema_name": "s_y", "age_s": 1234.5},
    ]

    async def _fake_execute(*args, **kwargs):
        # First call returns rows then sets shutdown so the loop exits next pass.
        shutdown.set()
        return fake_rows

    fake_query = AsyncMock()
    fake_query.execute = _fake_execute

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _fake_managed_transaction(_engine):
        yield object()

    caplog.set_level(logging.WARNING)
    with patch(
        "dynastore.modules.tasks.tasks_module.DQLQuery",
        return_value=fake_query,
    ), patch(
        "dynastore.modules.tasks.tasks_module.managed_transaction",
        _fake_managed_transaction,
    ):
        await asyncio.wait_for(
            _warn_stuck_pending_tasks(
                engine=object(), schema="tasks", shutdown_event=shutdown,
                interval_s=0.01, min_age_s=10.0,
            ),
            timeout=2.0,
        )

    warn_lines = [r.message for r in caplog.records if "stuck-pending: task" in r.message]
    assert len(warn_lines) == 2
    assert any("abc-1" in m and "elasticsearch_index" in m for m in warn_lines)
    assert any("abc-2" in m and "tile_preseed" in m for m in warn_lines)


@pytest.mark.asyncio
async def test_warner_swallows_query_errors(caplog):
    """A failing query is logged but doesn't crash the loop."""
    shutdown = asyncio.Event()

    call_count = {"n": 0}

    async def _failing_execute(*args, **kwargs):
        call_count["n"] += 1
        shutdown.set()  # exit after this scan
        raise RuntimeError("PG died")

    fake_query = AsyncMock()
    fake_query.execute = _failing_execute

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _fake_managed_transaction(_engine):
        yield object()

    caplog.set_level(logging.WARNING)
    with patch(
        "dynastore.modules.tasks.tasks_module.DQLQuery",
        return_value=fake_query,
    ), patch(
        "dynastore.modules.tasks.tasks_module.managed_transaction",
        _fake_managed_transaction,
    ):
        await asyncio.wait_for(
            _warn_stuck_pending_tasks(
                engine=object(), schema="tasks", shutdown_event=shutdown,
                interval_s=0.01,
            ),
            timeout=2.0,
        )

    assert call_count["n"] == 1
    # The error message is logged at WARNING level — the loop survived.
    assert any(
        "stuck-pending warner: scan failed" in r.message for r in caplog.records
    )


# --- _stuck_pending_hint -----------------------------------------------------

def test_hint_falls_back_to_routing_check_when_no_capability():
    msg = _stuck_pending_hint("tile_preseed", None, None)
    assert "TaskRoutingConfig.routing['tile_preseed']" in msg


def test_hint_surfaces_dead_capability_reaper_signal():
    msg = _stuck_pending_hint(
        "index_propagation", "collection_elasticsearch_driver", False,
    )
    assert "live=false" in msg
    assert "reactive reaper" in msg
    assert "DLQ" in msg
    assert "'collection_elasticsearch_driver'" in msg


def test_hint_surfaces_transient_starvation_when_capability_alive():
    msg = _stuck_pending_hint(
        "index_propagation", "collection_elasticsearch_driver", True,
    )
    assert "live=true" in msg
    assert "transient pool starvation" in msg


# --- _resolve_row_capability + _safe_is_live + _emit_stuck_pending_logs ---


def test_resolve_row_capability_returns_none_when_task_unknown():
    with patch("dynastore.tasks.get_task_instance", return_value=None):
        cache: dict = {}
        cap = _resolve_row_capability(
            {"task_type": "no_such_task", "inputs": None}, cache,
        )
    assert cap is None
    assert "no_such_task" in cache  # memoized as None


def test_resolve_row_capability_parses_json_string_inputs():
    class _FakeTask:
        @classmethod
        def required_capability(cls, payload):
            return payload["inputs"]["indexer_id"]

    with patch("dynastore.tasks.get_task_instance", return_value=_FakeTask()):
        cap = _resolve_row_capability(
            {"task_type": "index_propagation",
             "inputs": '{"indexer_id": "collection_elasticsearch_driver"}'},
            {},
        )
    assert cap == "collection_elasticsearch_driver"


def test_resolve_row_capability_uses_task_instance_cache():
    """Many rows with the same task_type → ONE get_task_instance call."""
    class _FakeTask:
        @classmethod
        def required_capability(cls, payload):
            return payload["inputs"]["indexer_id"]

    call_count = {"n": 0}

    def _stub_get(task_type):
        call_count["n"] += 1
        return _FakeTask()

    with patch("dynastore.tasks.get_task_instance", side_effect=_stub_get):
        cache: dict = {}
        for _ in range(5):
            _resolve_row_capability(
                {"task_type": "index_propagation",
                 "inputs": {"indexer_id": "d1"}},
                cache,
            )
    assert call_count["n"] == 1


@pytest.mark.asyncio
async def test_safe_is_live_returns_none_on_cache_failure():
    with patch(
        "dynastore.modules.tasks.capability_oracle.is_capability_live",
        new=AsyncMock(side_effect=RuntimeError("cache down")),
    ):
        live = await _safe_is_live("x")
    assert live is None


@pytest.mark.asyncio
async def test_emit_coalesces_oracle_calls_per_distinct_capability(caplog):
    """30 rows sharing a dead indexer_id → ONE oracle call, 30 WARN lines."""
    class _FakeTask:
        @classmethod
        def required_capability(cls, payload):
            return payload["inputs"]["indexer_id"]

    rows = [
        {"task_id": f"r-{i}", "task_type": "index_propagation",
         "schema_name": "s", "age_s": 999.0,
         "inputs": {"indexer_id": "dead_driver"}}
        for i in range(30)
    ]

    oracle = AsyncMock(return_value=False)
    caplog.set_level(logging.WARNING)
    with patch("dynastore.tasks.get_task_instance", return_value=_FakeTask()), \
         patch(
             "dynastore.modules.tasks.capability_oracle.is_capability_live",
             new=oracle,
         ):
        await _emit_stuck_pending_logs(rows)

    assert oracle.await_count == 1
    warn_lines = [r.message for r in caplog.records if "stuck-pending:" in r.message]
    assert len(warn_lines) == 30
    assert all("live=false" in m for m in warn_lines)
