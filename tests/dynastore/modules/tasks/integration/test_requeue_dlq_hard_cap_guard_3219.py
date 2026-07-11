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

"""Real-DB regression: ``requeue_dead_letter_task(reset_retries=False)`` must
refuse rather than create an unclaimable zombie row (#3219).

``claim_batch`` only claims rows with ``retry_count < hard_cap``. Before this
fix, requeuing a DEAD_LETTER/FAILED task with ``reset_retries=False`` while
its ``retry_count`` was already at/above that cap flipped it to
``status='PENDING'`` anyway — a row no dispatcher will ever claim and the
reaper (ACTIVE rows only) will never touch again. This pins the refusal and
confirms the below-cap path still works exactly as before.
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from sqlalchemy import text

from dynastore.modules.tasks import maintenance, tasks_module
from dynastore.modules.tasks.exceptions import UnclaimableRequeueError
from dynastore.modules.tasks.models import TaskCreate

from tests.dynastore.test_utils import generate_test_id


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.timeout(120),
]


@pytest_asyncio.fixture(loop_scope="function")
async def _dlq_task_factory(task_app_state):
    engine = task_app_state.engine
    task_schema = tasks_module.get_task_schema()
    schema_name = f"test_dlq_cap_{generate_test_id(8)}"
    created: list[uuid.UUID] = []

    async def _create(retry_count: int) -> uuid.UUID:
        task = await tasks_module.create_task(
            engine,
            TaskCreate(
                task_type=f"dlq_cap_{generate_test_id(6)}",
                caller_id="requeue-hard-cap-test",
            ),
            schema=schema_name,
        )
        assert task is not None, "row must be created"
        created.append(task.task_id)
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    f'UPDATE "{task_schema}".tasks '
                    "SET status = 'DEAD_LETTER', retry_count = :retry_count, "
                    "error_message = 'boom: exhausted retries' "
                    "WHERE task_id = :tid"
                ),
                {"retry_count": retry_count, "tid": task.task_id},
            )
        return task.task_id

    yield _create, engine

    if created:
        async with engine.connect() as conn:
            await conn.execute(
                text(f'DELETE FROM "{task_schema}".tasks WHERE task_id = ANY(:ids)'),
                {"ids": created},
            )
            await conn.commit()


async def _read_row(engine, task_id):
    task_schema = tasks_module.get_task_schema()
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    f'SELECT status, retry_count FROM "{task_schema}".tasks '
                    "WHERE task_id = :tid"
                ),
                {"tid": task_id},
            )
        ).fetchone()
    assert row is not None
    return row[0], row[1]


async def test_requeue_at_cap_with_reset_retries_false_raises_and_creates_no_row(
    _dlq_task_factory,
):
    """A DEAD_LETTER row whose retry_count already reached the hard cap must
    be refused, not silently turned into an unclaimable PENDING row."""
    create, engine = _dlq_task_factory
    hard_cap = tasks_module.get_hard_retry_cap()
    task_id = await create(retry_count=hard_cap)

    with pytest.raises(UnclaimableRequeueError):
        await maintenance.requeue_dead_letter_task(
            engine, str(task_id), reset_retries=False
        )

    status, retry_count = await _read_row(engine, task_id)
    assert status == "DEAD_LETTER", (
        "refused requeue must leave the row exactly where it was — no "
        "unclaimable PENDING row"
    )
    assert retry_count == hard_cap


async def test_requeue_below_cap_with_reset_retries_false_still_works(
    _dlq_task_factory,
):
    """Below the cap, reset_retries=False must behave exactly as before:
    requeue to PENDING, keep the retry_count."""
    create, engine = _dlq_task_factory
    hard_cap = tasks_module.get_hard_retry_cap()
    task_id = await create(retry_count=hard_cap - 1)

    result = await maintenance.requeue_dead_letter_task(
        engine, str(task_id), reset_retries=False
    )

    assert result is True
    status, retry_count = await _read_row(engine, task_id)
    assert status == "PENDING"
    assert retry_count == hard_cap - 1, "reset_retries=False must keep the count"


async def test_requeue_at_cap_with_reset_retries_true_still_succeeds(
    _dlq_task_factory,
):
    """The operator's escape hatch mentioned in the error message must
    actually work: reset_retries=True always resets the count to 0, so the
    hard-cap guard never applies to it."""
    create, engine = _dlq_task_factory
    hard_cap = tasks_module.get_hard_retry_cap()
    task_id = await create(retry_count=hard_cap)

    result = await maintenance.requeue_dead_letter_task(
        engine, str(task_id), reset_retries=True
    )

    assert result is True
    status, retry_count = await _read_row(engine, task_id)
    assert status == "PENDING"
    assert retry_count == 0
