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

"""Unit coverage for ``tasks_module.update_task_ingestion_offset`` (#2820).

Verifies the SQL shape without touching a real database, mirroring the
``DQLQuery``-capture pattern used in ``test_proactive_sweep.py``: the write
must ``jsonb_set`` the ``inputs.ingestion_request.offset`` path in place
(``create_missing=true``) rather than replacing the whole ``inputs`` column,
so a concurrent read of any other ``inputs`` key is never clobbered.
"""
from __future__ import annotations

import uuid
from unittest.mock import patch

import pytest

from dynastore.modules.tasks import tasks_module


class _FakeQuery:
    """Captures the SQL template + execute() params, mirroring the real
    ``DQLQuery`` interface just enough for ``update_task_ingestion_offset``."""

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
async def test_update_task_ingestion_offset_uses_jsonb_set_in_place():
    task_id = uuid.uuid4()
    _FakeQuery.rowcount = 1

    with (
        patch("dynastore.modules.tasks.tasks_module.DQLQuery", _FakeQuery),
        patch(
            "dynastore.modules.tasks.tasks_module.managed_transaction",
            return_value=_FakeTxn(),
        ),
    ):
        result = await tasks_module.update_task_ingestion_offset(
            object(), task_id, 1234,
        )

    assert result is True
    sql = _FakeQuery.last_sql
    assert "jsonb_set" in sql
    assert "{ingestion_request,offset}" in sql
    assert "WHERE task_id = :task_id" in sql
    # No catalog_id filter -- scoped by task_id only, matching
    # complete_task/fail_task (the ingestion loop has no cheap access to the
    # catalog_id column value at this call site).
    assert "catalog_id" not in sql

    params = _FakeQuery.last_params
    assert params["task_id"] == task_id
    assert params["offset_value"] == 1234


@pytest.mark.asyncio
async def test_update_task_ingestion_offset_returns_false_when_no_row_matched():
    _FakeQuery.rowcount = 0

    with (
        patch("dynastore.modules.tasks.tasks_module.DQLQuery", _FakeQuery),
        patch(
            "dynastore.modules.tasks.tasks_module.managed_transaction",
            return_value=_FakeTxn(),
        ),
    ):
        result = await tasks_module.update_task_ingestion_offset(
            object(), uuid.uuid4(), 5,
        )

    assert result is False
