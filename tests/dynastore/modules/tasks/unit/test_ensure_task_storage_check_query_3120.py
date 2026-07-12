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

"""``ensure_task_storage_exists`` must pass an explicit ``check_query`` for
the two global maintenance-helper functions (#3120).

``GLOBAL_TASKS_RETENTION_FUNC_DDL`` / ``GLOBAL_TASKS_PARTCREATE_FUNC_DDL``
name their function with ``{schema}`` embedded inside a quoted identifier
(e.g. ``"{schema}"."maintain_partitions_{schema}_tasks"``) -- the exact
pattern that made the auto-inferred existence check silently truncate the
templated name and stay ``False`` after startup DDL hit a verified peer
race (#3117). Passing an explicit ``check_query`` here makes the
duplicate-object peer-race recovery self-documenting instead of relying on
that inference.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.workclass_ddl import (
    partition_create_ahead_function_name,
    partition_retention_function_name,
)


@pytest.mark.asyncio
async def test_ensure_task_storage_exists_passes_explicit_check_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = tasks_module.get_task_schema()
    conn = AsyncMock()

    # Isolate the two DDLQuery(...) calls under test from every other
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

    with (
        patch("dynastore.modules.tasks.tasks_module.DDLQuery", side_effect=_ddl_factory),
        patch(
            "dynastore.modules.tasks.tasks_module.check_function_exists",
            new=AsyncMock(return_value=True),
        ) as mocked_check,
    ):
        await tasks_module.ensure_task_storage_exists(conn, schema)

        func_ddl_calls = [
            (sql, cq) for sql, cq in calls if "CREATE OR REPLACE FUNCTION" in sql
        ]
        # reaper (no embedded-{schema} name) + retention + partcreate
        assert len(func_ddl_calls) == 3

        retention_check = next(
            cq for sql, cq in func_ddl_calls if "maintain_partitions" in sql
        )
        partcreate_check = next(
            cq for sql, cq in func_ddl_calls if "create_partitions" in sql
        )
        assert retention_check is not None
        assert partcreate_check is not None

        await retention_check(conn)
        await partcreate_check(conn)

    seen_names = {call.args[1] for call in mocked_check.await_args_list}
    assert seen_names == {
        partition_retention_function_name(table="tasks", schema=schema),
        partition_create_ahead_function_name(table="tasks", schema=schema),
    }
