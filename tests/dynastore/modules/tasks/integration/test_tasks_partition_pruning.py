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

"""Real-DB integration coverage for #3218 — terminal task writes skip
partition pruning.

``tasks`` is RANGE-partitioned on ``timestamp`` (the row's creation time).
The unit counterpart (``unit/test_tasks_partition_pruning.py``) pins the SQL
shape; this module proves the live Postgres behaviour:

1. A row created "now" lives in a genuine monthly leaf partition (not the
   DEFAULT catch-all), and every terminal write / heartbeat still completes
   it correctly when given the row's real creation timestamp.
2. A row relocated into the DEFAULT partition (an out-of-range timestamp)
   is NOT a regression — the equality predicate still matches it.
3. A WRONG creation timestamp does not silently no-op: the row is left
   untouched and a distinguishable ERROR is logged, separate from (and not
   confused with) a genuine owner_id race loss.

This is a new, dedicated test module — the existing ``modules/tasks`` test
files are owned by a concurrent test-suite pass and are not touched here.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import text

from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.models import TaskCreate

from tests.dynastore.test_utils import generate_test_id


pytestmark = [pytest.mark.asyncio]

_LOGGER_NAME = "dynastore.modules.tasks.tasks_module"


async def _row(engine, task_id):
    task_schema = tasks_module.get_task_schema()
    async with engine.connect() as conn:
        return (
            await conn.execute(
                text(
                    f'SELECT status, owner_id, timestamp, locked_until '
                    f'FROM "{task_schema}".tasks WHERE task_id = :tid'
                ),
                {"tid": task_id},
            )
        ).one()


async def _partition_of(engine, task_id) -> str:
    """Physical partition table backing this row (``tableoid::regclass``)."""
    task_schema = tasks_module.get_task_schema()
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT tableoid::regclass::text FROM "
                    f'"{task_schema}".tasks WHERE task_id = :tid'
                ),
                {"tid": task_id},
            )
        ).fetchone()
    assert row is not None, f"row {task_id} not found"
    return row[0]


async def _relocate_to_default_partition(engine, task_id, out_of_range_ts: datetime) -> None:
    """Force ``task_id``'s row into the DEFAULT partition by updating its
    partition-key column through the parent table. Postgres re-routes a
    partition-key UPDATE to the correct partition automatically — this
    reproduces exactly what an out-of-range write lands as at INSERT time,
    without any manual row migration (out of scope for #3218)."""
    task_schema = tasks_module.get_task_schema()
    async with engine.connect() as conn:
        await conn.execute(
            text(f'UPDATE "{task_schema}".tasks SET timestamp = :ts WHERE task_id = :tid'),
            {"ts": out_of_range_ts, "tid": task_id},
        )
        await conn.commit()


@pytest_asyncio.fixture(loop_scope="function")
async def _pruning_row_factory(task_app_state):
    """Yields ``(create, schema, engine)``; force-deletes everything created."""
    engine = task_app_state.engine
    task_schema = tasks_module.get_task_schema()
    schema_name = f"test_pruning_{generate_test_id(8)}"
    created: list[uuid.UUID] = []

    async def _create(**kwargs):
        kwargs.setdefault("owner_id", f"pruning-{generate_test_id(6)}")
        kwargs.setdefault("locked_until", datetime.now(timezone.utc) + timedelta(minutes=5))
        task = await tasks_module.create_task(
            engine,
            TaskCreate(
                task_type=f"pruning_test_{generate_test_id(6)}",
                caller_id="partition-pruning-test",
            ),
            schema=schema_name,
            initial_status="ACTIVE",
            **kwargs,
        )
        assert task is not None, "born-claimed row must be created"
        created.append(task.task_id)
        return task

    yield _create, schema_name, engine

    if created:
        async with engine.connect() as conn:
            await conn.execute(
                text(f'DELETE FROM "{task_schema}".tasks WHERE task_id = ANY(:ids)'),
                {"ids": created},
            )
            await conn.commit()


# --- (b) a genuine leaf-partition row completes/fails/heartbeats correctly -


async def test_task_created_now_lives_in_a_genuine_leaf_partition(_pruning_row_factory):
    """Sanity check the test setup itself: a task created 'now' must NOT
    land in the DEFAULT partition, otherwise the leaf-partition assertions
    below would pass vacuously."""
    create, _, engine = _pruning_row_factory
    task = await create()
    task_schema = tasks_module.get_task_schema()

    partition = await _partition_of(engine, task.task_id)
    assert partition != f"{task_schema}.tasks_default"
    assert partition.startswith(f"{task_schema}.tasks_")


async def test_complete_task_with_real_created_at_in_leaf_partition(_pruning_row_factory):
    create, _, engine = _pruning_row_factory
    task = await create()

    ok = await tasks_module.complete_task(
        engine, task.task_id, datetime.now(timezone.utc),
        outputs={"ok": True}, created_at=task.timestamp,
    )
    assert ok is True
    row = await _row(engine, task.task_id)
    assert row.status == "COMPLETED"


async def test_fail_task_with_real_created_at_in_leaf_partition(_pruning_row_factory):
    create, _, engine = _pruning_row_factory
    task = await create()

    ok = await tasks_module.fail_task(
        engine, task.task_id, datetime.now(timezone.utc),
        "boom", retry=False, created_at=task.timestamp,
    )
    assert ok is True
    row = await _row(engine, task.task_id)
    assert row.status == "FAILED"


async def test_dead_letter_task_with_real_created_at_in_leaf_partition(_pruning_row_factory):
    create, _, engine = _pruning_row_factory
    task = await create()

    ok = await tasks_module.dead_letter_task(
        engine, task.task_id, datetime.now(timezone.utc),
        "timed out", created_at=task.timestamp,
    )
    assert ok is True
    row = await _row(engine, task.task_id)
    assert row.status == "DEAD_LETTER"


async def test_heartbeat_task_if_active_with_real_created_at_in_leaf_partition(_pruning_row_factory):
    create, _, engine = _pruning_row_factory
    task = await create()
    before = datetime.now(timezone.utc)

    ok = await tasks_module.heartbeat_task_if_active(
        engine, task.task_id, timedelta(seconds=300), created_at=task.timestamp,
    )
    assert ok is True
    row = await _row(engine, task.task_id)
    assert row.locked_until > before


async def test_heartbeat_tasks_batch_with_real_created_at_pairs(_pruning_row_factory):
    """Two rows in the same batch, each with its own creation timestamp."""
    create, _, engine = _pruning_row_factory
    task_a = await create()
    task_b = await create()
    before = datetime.now(timezone.utc)

    await tasks_module.heartbeat_tasks(
        engine,
        [(task_a.task_id, task_a.timestamp), (task_b.task_id, task_b.timestamp)],
        timedelta(seconds=300),
    )

    row_a = await _row(engine, task_a.task_id)
    row_b = await _row(engine, task_b.task_id)
    assert row_a.locked_until > before
    assert row_b.locked_until > before


# --- (c) a DEFAULT-partition row is not a regression ------------------------


async def test_complete_task_still_matches_a_row_relocated_to_tasks_default(_pruning_row_factory):
    create, _, engine = _pruning_row_factory
    task = await create()
    out_of_range = datetime.now(timezone.utc).replace(
        year=datetime.now(timezone.utc).year - 5
    )
    await _relocate_to_default_partition(engine, task.task_id, out_of_range)
    task_schema = tasks_module.get_task_schema()
    assert await _partition_of(engine, task.task_id) == f"{task_schema}.tasks_default", (
        "test setup must actually relocate the row into tasks_default"
    )

    ok = await tasks_module.complete_task(
        engine, task.task_id, datetime.now(timezone.utc),
        outputs={"ok": True}, created_at=out_of_range,
    )
    assert ok is True, "the equality predicate on timestamp must still match a DEFAULT-partition row"
    row = await _row(engine, task.task_id)
    assert row.status == "COMPLETED"


async def test_fail_task_still_matches_a_row_relocated_to_tasks_default(_pruning_row_factory):
    create, _, engine = _pruning_row_factory
    task = await create()
    out_of_range = datetime.now(timezone.utc).replace(
        year=datetime.now(timezone.utc).year - 5
    )
    await _relocate_to_default_partition(engine, task.task_id, out_of_range)

    ok = await tasks_module.fail_task(
        engine, task.task_id, datetime.now(timezone.utc),
        "boom", retry=False, created_at=out_of_range,
    )
    assert ok is True
    row = await _row(engine, task.task_id)
    assert row.status == "FAILED"


async def test_heartbeat_task_if_active_still_matches_a_row_relocated_to_tasks_default(
    _pruning_row_factory,
):
    create, _, engine = _pruning_row_factory
    task = await create()
    out_of_range = datetime.now(timezone.utc).replace(
        year=datetime.now(timezone.utc).year - 5
    )
    await _relocate_to_default_partition(engine, task.task_id, out_of_range)
    before = datetime.now(timezone.utc)

    ok = await tasks_module.heartbeat_task_if_active(
        engine, task.task_id, timedelta(seconds=300), created_at=out_of_range,
    )
    assert ok is True
    row = await _row(engine, task.task_id)
    assert row.locked_until > before


# --- (d) a WRONG created_at must not silently no-op --------------------------


async def test_complete_task_wrong_created_at_leaves_row_untouched_and_logs_loudly(
    _pruning_row_factory, caplog,
):
    create, _, engine = _pruning_row_factory
    task = await create()
    wrong_ts = task.timestamp - timedelta(days=400)  # a different partition entirely

    with caplog.at_level(logging.ERROR, logger=_LOGGER_NAME):
        ok = await tasks_module.complete_task(
            engine, task.task_id, datetime.now(timezone.utc),
            outputs={"ok": True}, created_at=wrong_ts,
        )

    assert ok is False, "a wrong partition key must not match the row"
    row = await _row(engine, task.task_id)
    assert row.status == "ACTIVE", "the row must be left byte-for-byte untouched, no partial write"
    assert any("WRONG partition key" in rec.message for rec in caplog.records), (
        "a wrong created_at must surface as a distinguishable ERROR, not a silent no-op"
    )


async def test_fail_task_wrong_created_at_leaves_row_untouched_and_logs_loudly(
    _pruning_row_factory, caplog,
):
    create, _, engine = _pruning_row_factory
    task = await create()
    wrong_ts = task.timestamp - timedelta(days=400)

    with caplog.at_level(logging.ERROR, logger=_LOGGER_NAME):
        ok = await tasks_module.fail_task(
            engine, task.task_id, datetime.now(timezone.utc),
            "boom", retry=False, created_at=wrong_ts,
        )

    assert ok is False
    row = await _row(engine, task.task_id)
    assert row.status == "ACTIVE"
    assert any("WRONG partition key" in rec.message for rec in caplog.records)


async def test_heartbeat_task_if_active_wrong_created_at_leaves_row_untouched_and_logs_loudly(
    _pruning_row_factory, caplog,
):
    create, _, engine = _pruning_row_factory
    task = await create()
    wrong_ts = task.timestamp - timedelta(days=400)

    with caplog.at_level(logging.ERROR, logger=_LOGGER_NAME):
        ok = await tasks_module.heartbeat_task_if_active(
            engine, task.task_id, timedelta(seconds=300), created_at=wrong_ts,
        )

    assert ok is False
    assert any("WRONG partition key" in rec.message for rec in caplog.records)


async def test_wrong_created_at_is_distinguishable_from_a_genuine_owner_id_race_loss(
    _pruning_row_factory, caplog,
):
    """The two 0-row causes must be tellable apart: a genuine owner_id race
    loss (row exists, CORRECT creation timestamp, owner_id mismatch) must
    NOT log the wrong-partition-key ERROR — only a real timestamp mismatch
    does. Otherwise operators can't distinguish 'benign race, retry will
    happen' from 'caller bug, every retry will also silently no-op'."""
    create, _, engine = _pruning_row_factory
    task = await create()

    with caplog.at_level(logging.ERROR, logger=_LOGGER_NAME):
        ok = await tasks_module.complete_task(
            engine, task.task_id, datetime.now(timezone.utc),
            outputs={"ok": True},
            created_at=task.timestamp,  # correct
            owner_id="a-different-owner",  # wrong — genuine race loss
        )

    assert ok is False, "owner_id mismatch must still lose the race"
    assert not any("WRONG partition key" in rec.message for rec in caplog.records), (
        "a genuine owner_id race loss must not be misreported as a wrong "
        "partition key — the row's real timestamp matched what was passed"
    )
    row = await _row(engine, task.task_id)
    assert row.status == "ACTIVE"
