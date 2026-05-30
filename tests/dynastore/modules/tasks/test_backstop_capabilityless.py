"""A PENDING row of an unclaimable task with required_capability=None IS
dead-lettered by the backstop (the #1647-class escape the capability reaper skips)."""
from __future__ import annotations

import pytest

from dynastore.modules.tasks import dispatcher


@pytest.mark.asyncio
async def test_capabilityless_unclaimable_rows_are_dlqd(monkeypatch):
    captured = {}

    class _FakeConn:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

    import contextlib

    @contextlib.asynccontextmanager
    async def _fake_managed_transaction(_engine):
        yield object()

    class _FakeQuery:
        def __init__(self, sql, result_handler=None):
            captured["sql"] = sql
        async def execute(self, _conn, **params):
            captured["params"] = params
            return [{"task_id": "a"}, {"task_id": "b"}, {"task_id": "c"}]  # 3 rows

    import dynastore.modules.db_config.query_executor as qe
    monkeypatch.setattr(qe, "managed_transaction", _fake_managed_transaction)
    monkeypatch.setattr(qe, "DQLQuery", _FakeQuery)

    async def _unclaimable(_engine, *, ttl_grace_seconds):
        return ["cascade_cleanup"]
    monkeypatch.setattr(dispatcher, "_find_unclaimable_task_types", _unclaimable)

    n = await dispatcher.sweep_unclaimable_rows(object(), schema="system", ttl_grace_seconds=90, min_age_s=300)
    assert n == 3
    assert "dead_letter" in captured["sql"].lower()
    assert "make_interval" in captured["sql"]
    assert captured["params"].get("min_age_s") == 300
    assert "cascade_cleanup" in str(captured["params"].get("task_types"))


@pytest.mark.asyncio
async def test_no_unclaimable_types_is_a_noop(monkeypatch):
    async def _none(_engine, *, ttl_grace_seconds):
        return []
    monkeypatch.setattr(dispatcher, "_find_unclaimable_task_types", _none)

    n = await dispatcher.sweep_unclaimable_rows(
        object(), schema="system", ttl_grace_seconds=90, min_age_s=300
    )
    assert n == 0
