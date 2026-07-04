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

"""Real-DB regression: ``update_task_ingestion_offset`` must actually execute
against Postgres (refs #2820).

The unit suite (``test_update_task_ingestion_offset_2820.py``) mocks
``DQLQuery`` entirely and only asserts on the SQL string, so it cannot catch a
statement that is syntactically invalid Postgres. The original SQL cast the
bind parameter as ``to_jsonb(:offset_value::bigint)`` -- a named bind parameter
immediately followed by a Postgres ``::`` cast -- which psycopg2 sends to the
server with the bind marker unsubstituted, raising
``psycopg2.errors.SyntaxError: syntax error at or near ":"`` on every call.
This silently disabled ingestion-cursor checkpointing in production: every
retry after a Cloud Run Job timeout resumed from the original request offset
instead of the last committed one, looping forever on a large ingestion.

These tests pin the live-Postgres behavior: the offset must actually commit
and be independently readable, and unrelated ``inputs`` keys must survive.
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from sqlalchemy import text

from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.models import TaskCreate

from tests.dynastore.test_utils import generate_test_id


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.timeout(120),
]


@pytest_asyncio.fixture(loop_scope="function")
async def _task_factory(task_app_state):
    engine = task_app_state.engine
    task_schema = tasks_module.get_task_schema()
    schema_name = f"test_ingest_offset_{generate_test_id(8)}"
    created: list[uuid.UUID] = []

    async def _create() -> uuid.UUID:
        task = await tasks_module.create_task(
            engine,
            TaskCreate(
                task_type=f"ingest_offset_{generate_test_id(6)}",
                caller_id="update-task-ingestion-offset-test",
                inputs={"ingestion_request": {"offset": 0}, "other_key": "keep-me"},
            ),
            schema=schema_name,
        )
        assert task is not None, "row must be created"
        created.append(task.task_id)
        return task.task_id

    yield _create, engine

    if created:
        async with engine.connect() as conn:
            await conn.execute(
                text(f'DELETE FROM "{task_schema}".tasks WHERE task_id = ANY(:ids)'),
                {"ids": created},
            )
            await conn.commit()


async def _read_inputs(engine, task_id) -> dict:
    task_schema = tasks_module.get_task_schema()
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(f'SELECT inputs FROM "{task_schema}".tasks WHERE task_id = :tid'),
                {"tid": task_id},
            )
        ).fetchone()
    assert row is not None
    return row[0]


async def test_update_task_ingestion_offset_commits_against_real_postgres(
    _task_factory,
):
    """The write must not raise and must actually reach the server -- this is
    the exact call the ingestion loop makes after every batch commit."""
    create, engine = _task_factory
    task_id = await create()

    result = await tasks_module.update_task_ingestion_offset(engine, task_id, 3_103_100)

    assert result is True
    inputs = await _read_inputs(engine, task_id)
    assert inputs["ingestion_request"]["offset"] == 3_103_100
    assert inputs["other_key"] == "keep-me", (
        "jsonb_set must patch the offset key in place, not clobber sibling "
        "inputs keys"
    )


async def test_update_task_ingestion_offset_returns_false_for_unknown_task(
    _task_factory,
):
    create, engine = _task_factory
    await create()

    result = await tasks_module.update_task_ingestion_offset(
        engine, uuid.uuid4(), 42
    )
    assert result is False
