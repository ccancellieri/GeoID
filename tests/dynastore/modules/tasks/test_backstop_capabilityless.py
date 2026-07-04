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

"""A PENDING row of an unclaimable task with required_capability=None IS
dead-lettered by the backstop (the #1647-class escape the capability reaper skips)."""
from __future__ import annotations

import pytest

from dynastore.modules.tasks import dispatcher


@pytest.mark.asyncio
async def test_capabilityless_unclaimable_rows_are_dlqd(monkeypatch):
    captured = {}

    class _FakeQuery:
        def __init__(self, sql, result_handler=None):
            captured["sql"] = sql
        async def execute(self, _conn, **params):
            captured["params"] = params
            return [{"task_id": "a"}, {"task_id": "b"}, {"task_id": "c"}]  # 3 rows

    import dynastore.modules.db_config.query_executor as qe
    monkeypatch.setattr(qe, "DQLQuery", _FakeQuery)

    async def _unclaimable(_engine, *, ttl_grace_seconds, conn=None):
        return ["cascade_cleanup"]
    monkeypatch.setattr(dispatcher, "_find_unclaimable_task_types", _unclaimable)

    n = await dispatcher.sweep_unclaimable_rows(
        object(), schema="system", ttl_grace_seconds=90, min_age_s=300, conn=object(),
    )
    assert n == 3
    assert "dead_letter" in captured["sql"].lower()
    assert "make_interval" in captured["sql"]
    assert captured["params"].get("min_age_s") == 300
    assert "cascade_cleanup" in str(captured["params"].get("task_types"))


@pytest.mark.asyncio
async def test_no_unclaimable_types_is_a_noop(monkeypatch):
    async def _none(_engine, *, ttl_grace_seconds, conn=None):
        return []
    monkeypatch.setattr(dispatcher, "_find_unclaimable_task_types", _none)

    n = await dispatcher.sweep_unclaimable_rows(
        object(), schema="system", ttl_grace_seconds=90, min_age_s=300
    )
    assert n == 0


@pytest.mark.asyncio
async def test_sweep_requires_conn_when_rows_are_unclaimable(monkeypatch):
    """The DLQ UPDATE step requires a pre-acquired, gated conn -- the sole
    caller (the mandatory backstop pass) always supplies one already routed
    through background_managed_transaction. Guards against reintroducing an
    ungated pool acquire here (#2900)."""
    async def _unclaimable(_engine, *, ttl_grace_seconds, conn=None):
        return ["cascade_cleanup"]
    monkeypatch.setattr(dispatcher, "_find_unclaimable_task_types", _unclaimable)

    with pytest.raises(AssertionError):
        await dispatcher.sweep_unclaimable_rows(
            object(), schema="system", ttl_grace_seconds=90, min_age_s=300,
        )
