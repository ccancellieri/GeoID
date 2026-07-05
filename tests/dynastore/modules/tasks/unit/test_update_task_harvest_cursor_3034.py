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

"""Unit coverage for ``tasks_module.update_task_harvest_cursor`` (#3034).

Verifies the SQL shape without touching a real database, mirroring the
``DQLQuery``-capture pattern used for ``update_task_ingestion_offset``
(#2820): the write must ``jsonb_set`` the whole ``inputs.resume`` object in
one call (``create_missing=true``) using ``CAST(:param AS jsonb)`` rather
than a ``::`` cast -- the #3032 lesson (a bind parameter immediately
followed by a Postgres ``::`` cast is sent to the server with the bind
marker unsubstituted and fails with a syntax error on every call).
"""
from __future__ import annotations

import json
import uuid
from unittest.mock import patch

import pytest

from dynastore.modules.tasks import tasks_module


class _FakeQuery:
    """Captures the SQL template + execute() params, mirroring the real
    ``DQLQuery`` interface just enough for ``update_task_harvest_cursor``."""

    last_sql: str = ""
    last_params: dict = {}
    rowcount: int = 1

    def __init__(self, sql, *args, **kwargs):
        type(self).last_sql = sql

    async def execute(self, conn, **kwargs):
        type(self).last_params = kwargs
        return type(self).rowcount


class _FakeTxn:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *_):
        return False


@pytest.mark.asyncio
async def test_update_task_harvest_cursor_uses_jsonb_set_in_place():
    task_id = uuid.uuid4()
    _FakeQuery.rowcount = 1

    with (
        patch("dynastore.modules.tasks.tasks_module.DQLQuery", _FakeQuery),
        patch(
            "dynastore.modules.tasks.tasks_module.managed_transaction",
            return_value=_FakeTxn(),
        ),
    ):
        result = await tasks_module.update_task_harvest_cursor(
            object(), task_id, "c2", "https://src/items?page=2", False,
        )

    assert result is True
    sql = _FakeQuery.last_sql
    assert "jsonb_set" in sql
    assert "{inputs,resume}" in sql
    assert "WHERE task_id = :task_id" in sql
    # CAST(:param AS type), never the bind-then-`::`-cast shape #3032 fixed
    # (a literal ``'{}'::jsonb`` default is fine -- it is not a bind param).
    assert "CAST(:resume_json AS jsonb)" in sql
    assert ":resume_json::" not in sql
    # No catalog_id filter -- scoped by task_id only, matching
    # update_task_ingestion_offset / complete_task / fail_task.
    assert "catalog_id" not in sql

    params = _FakeQuery.last_params
    assert params["task_id"] == task_id
    assert json.loads(params["resume_json"]) == {
        "collection_id": "c2",
        "items_href": "https://src/items?page=2",
        "done": False,
    }


@pytest.mark.asyncio
async def test_update_task_harvest_cursor_serializes_none_fields_as_json_null():
    """``done=True`` clears items_href -- must serialize as JSON null, not
    be dropped (a resumed walk relies on reading it back explicitly)."""
    _FakeQuery.rowcount = 1

    with (
        patch("dynastore.modules.tasks.tasks_module.DQLQuery", _FakeQuery),
        patch(
            "dynastore.modules.tasks.tasks_module.managed_transaction",
            return_value=_FakeTxn(),
        ),
    ):
        await tasks_module.update_task_harvest_cursor(
            object(), uuid.uuid4(), "c2", None, True,
        )

    payload = json.loads(_FakeQuery.last_params["resume_json"])
    assert payload == {"collection_id": "c2", "items_href": None, "done": True}


@pytest.mark.asyncio
async def test_update_task_harvest_cursor_returns_false_when_no_row_matched():
    _FakeQuery.rowcount = 0

    with (
        patch("dynastore.modules.tasks.tasks_module.DQLQuery", _FakeQuery),
        patch(
            "dynastore.modules.tasks.tasks_module.managed_transaction",
            return_value=_FakeTxn(),
        ),
    ):
        result = await tasks_module.update_task_harvest_cursor(
            object(), uuid.uuid4(), "c1", "href", False,
        )

    assert result is False
