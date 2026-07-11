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

"""Real-DB regression: ``claim_batch`` must surface a claimed row's prior
failure reason as a task event before it clears ``error_message`` (#3225).

Follow-up to the merged fix that nulls ``error_message`` on every claim (see
``test_claim_batch_clears_stale_error_message.py``): that fix is correct but
trades away the only place the prior failure was visible while a retrying
task is ACTIVE. This pins that the history now survives in the event stream
instead — and that first-time claims (no prior error) don't pay for an emit
they don't need.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

import pytest
import pytest_asyncio
from sqlalchemy import text

from dynastore.modules.catalog.event_service import event_service
from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.models import TaskCreate

from tests.dynastore.test_utils import generate_test_id


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.timeout(120),
]


@pytest_asyncio.fixture(loop_scope="function")
async def _pending_task_factory(task_app_state):
    engine = task_app_state.engine
    task_schema = tasks_module.get_task_schema()
    schema_name = f"test_claim_evt_{generate_test_id(8)}"
    created: list[uuid.UUID] = []

    async def _create(*, prior_error: str | None, retry_count: int = 0) -> tuple[uuid.UUID, str]:
        task_type = f"claim_evt_{generate_test_id(6)}"
        task = await tasks_module.create_task(
            engine,
            TaskCreate(task_type=task_type, caller_id="claim-batch-event-test"),
            schema=schema_name,
        )
        assert task is not None, "row must be created"
        created.append(task.task_id)
        if prior_error is not None or retry_count:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        f'UPDATE "{task_schema}".tasks '
                        "SET error_message = :err, retry_count = :retry_count "
                        "WHERE task_id = :tid"
                    ),
                    {"err": prior_error, "retry_count": retry_count, "tid": task.task_id},
                )
        return task.task_id, task_type

    yield _create, engine

    if created:
        async with engine.connect() as conn:
            await conn.execute(
                text(f'DELETE FROM "{task_schema}".tasks WHERE task_id = ANY(:ids)'),
                {"ids": created},
            )
            await conn.commit()


@pytest_asyncio.fixture(loop_scope="function")
async def _captured_retried_events():
    """Registers a temporary sync listener on ``task.retried`` and yields the
    list of kwargs each emission was called with, cleaning up afterwards so
    listener state doesn't leak into other tests."""
    captured: list[dict] = []

    async def _listener(*_args, **kwargs):
        captured.append(kwargs)

    event_service.register("task.retried", _listener)
    try:
        yield captured
    finally:
        event_service.unregister("task.retried", _listener)


async def test_claim_batch_emits_prior_error_as_task_event(
    _pending_task_factory, _captured_retried_events
):
    create, engine = _pending_task_factory
    task_id, task_type = await create(
        prior_error="boom: connection reset", retry_count=1
    )

    rows = await tasks_module.claim_batch(
        engine,
        async_task_types=[task_type],
        sync_task_types=[],
        visibility_timeout=timedelta(minutes=5),
        owner_id="claim-batch-event-test-owner",
        batch_size=10,
    )

    assert [r["task_id"] for r in rows] == [task_id]
    assert "prior_error_message" not in rows[0], (
        "the internal-only RETURNING alias must not leak into the claimed row"
    )

    assert len(_captured_retried_events) == 1, (
        "a row carrying a prior error_message must emit exactly one "
        "task.retried event"
    )
    event = _captured_retried_events[0]
    assert event["task_id"] == str(task_id)
    assert event["prior_error_message"] == "boom: connection reset"


async def test_claim_batch_first_time_claim_emits_no_event(
    _pending_task_factory, _captured_retried_events
):
    create, engine = _pending_task_factory
    task_id, task_type = await create(prior_error=None)

    rows = await tasks_module.claim_batch(
        engine,
        async_task_types=[task_type],
        sync_task_types=[],
        visibility_timeout=timedelta(minutes=5),
        owner_id="claim-batch-event-test-owner",
        batch_size=10,
    )

    assert [r["task_id"] for r in rows] == [task_id]
    assert _captured_retried_events == [], (
        "a first-time claim with no prior error must not emit task.retried"
    )
