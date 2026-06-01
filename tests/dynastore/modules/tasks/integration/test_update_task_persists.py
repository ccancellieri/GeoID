"""Real-DB regression: ``update_task`` must COMMIT its write (refs #1676).

``update_task`` receives a ``DbResource`` that, at every ``BackgroundRunner`` /
``GcpJobRunner`` terminal call site, is a bare ``AsyncEngine`` (``context.engine``).
``DQLQuery.execute`` on a bare engine routes through the executor's pool-return
path, which rolls back any open transaction before handing the connection back
to the pool. Without an explicit ``managed_transaction`` the UPDATE's
``RETURNING`` row is therefore *read* (so a ``Task`` is returned with the new
status) while the status flip itself is silently discarded — a COMPLETED task
quietly reverts to its prior status, and the dispatcher / reconciler observe a
task that never finished.

The unit suites mock the engine and cannot see this; these tests pin the live
Postgres durability: the new value is visible from a SEPARATE pooled connection
after ``update_task`` returns. Before the fix they go RED (the read sees the
pre-update value); after it they go GREEN.
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from sqlalchemy import text

from dynastore.models.tasks import TaskStatusEnum
from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.models import TaskCreate, TaskUpdate

from tests.dynastore.test_utils import generate_test_id


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.timeout(120),
]


@pytest_asyncio.fixture(loop_scope="function")
async def _task_factory(task_app_state):
    """Yields ``(create, schema_name, engine)`` and force-deletes its rows.

    Rows are born through the production ``create_task`` path so they match
    what the runners actually flip in production.
    """
    engine = task_app_state.engine
    task_schema = tasks_module.get_task_schema()
    schema_name = f"test_update_task_{generate_test_id(8)}"
    created: list[uuid.UUID] = []

    async def _create(initial_status: str = "PENDING") -> uuid.UUID:
        task = await tasks_module.create_task(
            engine,
            TaskCreate(
                task_type=f"update_persist_{generate_test_id(6)}",
                caller_id="update-task-persist-test",
            ),
            schema=schema_name,
            initial_status=initial_status,
        )
        assert task is not None, "row must be created"
        created.append(task.task_id)
        return task.task_id

    yield _create, schema_name, engine

    if created:
        async with engine.connect() as conn:
            await conn.execute(
                text(f'DELETE FROM "{task_schema}".tasks WHERE task_id = ANY(:ids)'),
                {"ids": created},
            )
            await conn.commit()


async def _read_status(engine, task_id) -> str | None:
    task_schema = tasks_module.get_task_schema()
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(f'SELECT status FROM "{task_schema}".tasks WHERE task_id = :tid'),
                {"tid": task_id},
            )
        ).fetchone()
    return row[0] if row else None


async def test_update_task_with_engine_commits_status_flip(_task_factory):
    """``update_task(engine, ...)`` — the exact form every runner uses — must
    persist the status flip. A SEPARATE pooled connection must observe
    COMPLETED, proving the write committed rather than being rolled back on
    pool return."""
    create, schema_name, engine = _task_factory
    task_id = await create(initial_status="PENDING")

    returned = await tasks_module.update_task(
        engine,
        task_id,
        TaskUpdate(status=TaskStatusEnum.COMPLETED),
        schema=schema_name,
    )
    # RETURNING always reflected the flip, even with the bug.
    assert returned is not None
    assert returned.status == TaskStatusEnum.COMPLETED

    # Load-bearing: a fresh connection sees the committed value.
    persisted = await _read_status(engine, task_id)
    assert persisted == "COMPLETED", (
        "update_task(engine, ...) did not COMMIT — the status flip was rolled "
        "back on pool return (regression: missing managed_transaction)."
    )


async def test_update_task_with_engine_commits_outputs(_task_factory):
    """A non-status field (``outputs``, which also exercises the JSON-encoder
    branch) must likewise commit through ``managed_transaction``."""
    create, schema_name, engine = _task_factory
    task_id = await create(initial_status="ACTIVE")

    payload = {"reconciled": True, "rows": 7}
    returned = await tasks_module.update_task(
        engine,
        task_id,
        TaskUpdate(outputs=payload),
        schema=schema_name,
    )
    assert returned is not None

    task_schema = tasks_module.get_task_schema()
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    f'SELECT outputs, status FROM "{task_schema}".tasks '
                    "WHERE task_id = :tid"
                ),
                {"tid": task_id},
            )
        ).fetchone()
    assert row is not None
    assert row[0] == payload, "outputs write must commit, not roll back"
    assert row[1] == "ACTIVE", "outputs-only update must not touch status"


async def test_update_task_noop_when_no_fields_set(_task_factory):
    """An empty ``TaskUpdate`` short-circuits to a read and leaves the row
    untouched — the managed_transaction wrap must not introduce a spurious
    write or flip the status."""
    create, schema_name, engine = _task_factory
    task_id = await create(initial_status="PENDING")

    returned = await tasks_module.update_task(
        engine, task_id, TaskUpdate(), schema=schema_name
    )
    assert returned is not None
    assert returned.status == TaskStatusEnum.PENDING
    assert await _read_status(engine, task_id) == "PENDING"
