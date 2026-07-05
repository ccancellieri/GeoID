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

"""Real-DB regression: ``update_task_harvest_cursor`` must actually execute
against Postgres (#3034).

Mirrors ``test_update_task_ingestion_offset_persists_2820.py``: the unit
suite (``test_update_task_harvest_cursor_3034.py``) mocks ``DQLQuery``
entirely and only asserts on the SQL string, so it cannot catch a statement
that is syntactically invalid Postgres -- exactly the class of bug #3032
found in the ingestion cursor's ``::`` cast. These tests pin the
live-Postgres behavior: the resume cursor must actually commit and be
independently readable, and unrelated ``inputs`` keys must survive the
``jsonb_set``.
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
    schema_name = f"test_harvest_cursor_{generate_test_id(8)}"
    created: list[uuid.UUID] = []

    async def _create() -> uuid.UUID:
        task = await tasks_module.create_task(
            engine,
            TaskCreate(
                task_type=f"stac_harvest_{generate_test_id(6)}",
                caller_id="update-task-harvest-cursor-test",
                # Mirrors the real column shape: stac_harvest is always
                # submitted via execute_process, so the process inputs
                # (and their eventual "resume" key) sit one level under the
                # ExecuteRequest wrapper's own "inputs" key.
                inputs={
                    "inputs": {"catalog_url": "https://src", "other_key": "keep-me"},
                    "title": "harvest",
                },
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


async def test_update_task_harvest_cursor_commits_against_real_postgres(_task_factory):
    """The write must not raise and must actually reach the server -- this is
    the exact call the harvest loop makes after every batch commit."""
    create, engine = _task_factory
    task_id = await create()

    result = await tasks_module.update_task_harvest_cursor(
        engine, task_id, "c2", "https://src/collections/c2/items?page=3", False,
    )

    assert result is True
    inputs = await _read_inputs(engine, task_id)
    assert inputs["inputs"]["resume"] == {
        "collection_id": "c2",
        "items_href": "https://src/collections/c2/items?page=3",
        "done": False,
    }
    # jsonb_set must patch inputs.resume in place, not clobber sibling keys.
    assert inputs["inputs"]["other_key"] == "keep-me"
    assert inputs["inputs"]["catalog_url"] == "https://src"


async def test_update_task_harvest_cursor_overwrites_previous_resume_value(_task_factory):
    """A second checkpoint call must replace the whole resume object, not merge
    with the stale one (a stale items_href must not survive a done=True stamp)."""
    create, engine = _task_factory
    task_id = await create()

    await tasks_module.update_task_harvest_cursor(
        engine, task_id, "c2", "https://src/collections/c2/items?page=3", False,
    )
    result = await tasks_module.update_task_harvest_cursor(
        engine, task_id, "c2", None, True,
    )

    assert result is True
    inputs = await _read_inputs(engine, task_id)
    assert inputs["inputs"]["resume"] == {
        "collection_id": "c2", "items_href": None, "done": True,
    }


async def test_update_task_harvest_cursor_returns_false_for_unknown_task(_task_factory):
    create, engine = _task_factory
    await create()

    result = await tasks_module.update_task_harvest_cursor(
        engine, uuid.uuid4(), "c1", "href", False,
    )
    assert result is False
