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

"""Unit tests for ``enqueue_storage_op_write_id`` — the write-id ledger row
is a first-class ``write_id`` column, not a JSON payload; ``tasks.storage``
carries no payload column at all (#3116 async write-id outbox slice)."""

from __future__ import annotations
from uuid import uuid4

import pytest


@pytest.mark.asyncio
async def test_write_id_rows_store_column_not_payload(monkeypatch):
    from dynastore.models.protocols.indexing import WriteIdOutboxRecord
    from dynastore.modules.storage import storage_emit

    captured: list[dict] = []

    class _FakeResultHandler:
        NONE = object()

    class _FakeDQLQuery:
        def __init__(self, sql, *, result_handler):
            self.sql = sql
            self.result_handler = result_handler

        async def execute(self, conn, **params):
            captured.append({"sql": self.sql, "params": params})

    monkeypatch.setattr(
        "dynastore.modules.db_config.query_executor.DQLQuery", _FakeDQLQuery,
    )
    monkeypatch.setattr(
        "dynastore.modules.db_config.query_executor.ResultHandler",
        _FakeResultHandler,
    )
    monkeypatch.setattr(
        "dynastore.modules.tasks.tasks_module.get_task_schema", lambda: "tasks",
    )
    monkeypatch.setattr(storage_emit, "_enqueue_drain_trigger", _noop_trigger)

    row = WriteIdOutboxRecord(
        op_id=uuid4(),
        driver_id="items_elasticsearch_driver",
        driver_instance_id="di",
        collection_id="col1",
        op="upsert",
        write_id="w-123",
        idempotency_key="ik",
    )

    await storage_emit.enqueue_storage_op_write_id(
        object(), catalog_id="cat1", rows=[row],
    )

    assert len(captured) == 1
    params = captured[0]["params"]
    assert params["catalog_id"] == "cat1"
    assert params["driver_id"] == "items_elasticsearch_driver"
    assert params["collection_id"] == "col1"
    assert params["op"] == "upsert"
    assert params["write_id"] == "w-123"
    assert "entity_id" not in params
    assert "op_payload" not in params, "tasks.storage carries no payload column"
    assert "write_id" in captured[0]["sql"]


@pytest.mark.asyncio
async def test_write_id_rows_coalesce_by_target_collection_op_and_write_id(monkeypatch):
    from dynastore.models.protocols.indexing import WriteIdOutboxRecord
    from dynastore.modules.storage import storage_emit

    captured: list[dict] = []

    class _FakeResultHandler:
        NONE = object()

    class _FakeDQLQuery:
        def __init__(self, sql, *, result_handler):
            pass

        async def execute(self, conn, **params):
            captured.append(params)

    monkeypatch.setattr(
        "dynastore.modules.db_config.query_executor.DQLQuery", _FakeDQLQuery,
    )
    monkeypatch.setattr(
        "dynastore.modules.db_config.query_executor.ResultHandler",
        _FakeResultHandler,
    )
    monkeypatch.setattr(
        "dynastore.modules.tasks.tasks_module.get_task_schema", lambda: "tasks",
    )
    monkeypatch.setattr(storage_emit, "_enqueue_drain_trigger", _noop_trigger)

    rows = [
        WriteIdOutboxRecord(
            op_id=uuid4(),
            driver_id="items_elasticsearch_driver",
            driver_instance_id="di",
            collection_id="col1",
            op="upsert",
            write_id="w-123",
            idempotency_key=f"ik-{i}",
        )
        for i in range(2)
    ]

    await storage_emit.enqueue_storage_op_write_id(
        object(), catalog_id="cat1", rows=rows,
    )

    assert len(captured) == 1
    assert captured[0]["idempotency_key"] == "ik-1"
    assert captured[0]["write_id"] == "w-123"


async def _noop_trigger(*args, **kwargs):
    return None
