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

"""``CREATE OR REPLACE FUNCTION`` DDL must carry NO existence gate (#3306).

A pg_proc name check in front of ``CREATE OR REPLACE FUNCTION`` freezes the
function body at first creation: the name exists, the executor skips the
DDL, and body fixes never reach a long-lived database (the #3298 reaper
predicate widening was inert on dev while fresh CI databases got the new
body). ``OR REPLACE`` is itself the idempotency mechanism, so these
statements must execute on every startup — supersedes the explicit
``check_query`` gates originally added for #3120/#3117.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tasks import tasks_module


@pytest.mark.asyncio
async def test_ensure_task_storage_function_ddl_has_no_existence_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = tasks_module.get_task_schema()
    conn = AsyncMock()

    # Isolate the function DDLQuery(...) calls under test from every other
    # provisioning step in ensure_task_storage_exists.
    monkeypatch.setattr(tasks_module, "ensure_schema_exists", AsyncMock())

    fake_batch = MagicMock()
    fake_batch.execute = AsyncMock()
    monkeypatch.setattr(tasks_module, "_build_tasks_ddl_batch", lambda schema: fake_batch)
    monkeypatch.setattr(tasks_module, "_ensure_tasks_default_partition", AsyncMock())

    from dynastore.modules.db_config import maintenance_tools

    monkeypatch.setattr(maintenance_tools, "ensure_future_partitions", AsyncMock())

    calls: list[tuple[str, object]] = []

    def _ddl_factory(sql, check_query=None, **kw):
        calls.append((sql, check_query))
        inst = MagicMock()
        inst.execute = AsyncMock()
        return inst

    with patch("dynastore.modules.tasks.tasks_module.DDLQuery", side_effect=_ddl_factory):
        await tasks_module.ensure_task_storage_exists(conn, schema)

    func_ddl_calls = [
        (sql, cq) for sql, cq in calls if "CREATE OR REPLACE FUNCTION" in sql
    ]
    # reaper + retention + partcreate
    assert len(func_ddl_calls) == 3
    assert {name for name in ("reap_stuck_tasks", "maintain_partitions", "create_partitions")} == {
        name
        for sql, _ in func_ddl_calls
        for name in ("reap_stuck_tasks", "maintain_partitions", "create_partitions")
        if name in sql
    }
    for sql, check_query in func_ddl_calls:
        assert check_query is None, (
            f"function DDL must not be gated by an existence check (#3306): {sql[:80]!r}"
        )


@pytest.mark.asyncio
async def test_ensure_workclass_storage_function_ddl_has_no_existence_gate() -> None:
    from dynastore.modules.tasks import workclass_ddl

    conn = AsyncMock()
    calls: list[tuple[str, object]] = []

    def _ddl_factory(sql, check_query=None, **kw):
        calls.append((sql, check_query))
        inst = MagicMock()
        inst.execute = AsyncMock()
        return inst

    with (
        patch("dynastore.modules.tasks.workclass_ddl.DDLQuery", side_effect=_ddl_factory),
        patch("dynastore.modules.tasks.workclass_ddl.DQLQuery") as dql,
    ):
        dql.return_value.execute = AsyncMock()
        await workclass_ddl.ensure_workclass_storage_exists(conn, "tasks")

    func_ddl_calls = [
        (sql, cq) for sql, cq in calls if "CREATE OR REPLACE FUNCTION" in sql
    ]
    # events + storage, create-ahead + retention each
    assert len(func_ddl_calls) == 4
    for sql, check_query in func_ddl_calls:
        assert check_query is None, (
            f"function DDL must not be gated by an existence check (#3306): {sql[:80]!r}"
        )
