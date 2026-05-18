"""Real-PG integration coverage for ``sweep_dead_capability_rows``
(GeoID #524 PR B).

The unit suite mocks ``managed_transaction`` and ``pg_try_advisory_xact_lock``
so two concurrent calls cannot truly race. This test runs both calls
against the same Postgres instance the dispatcher uses in production and
asserts the load-bearing invariant:

  Exactly ONE of two concurrent ``sweep_dead_capability_rows(cap, t)``
  calls bulk-DLQs the rows; the other observes the advisory xact lock
  held by its peer, returns 0, and leaves the rows untouched.

Without this, PR C's removal of the reactive branch would delete the
only path we ever exercised against real PG locking semantics.

The capability oracle is monkey-patched to ``not live`` so the SQL
branch under test actually fires; the oracle itself is a cache concern
covered by ``capability_oracle`` unit tests.
"""
from __future__ import annotations

import asyncio
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import text

from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.dispatcher import sweep_dead_capability_rows
from dynastore.modules.tasks.models import TaskCreate

from tests.dynastore.test_utils import generate_test_id


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.timeout(120),
    pytest.mark.enable_modules(
        "db_config", "db", "catalog", "stats", "iam", "stac",
        "collection_postgresql", "catalog_postgresql", "tasks",
    ),
]


_TASK_TYPE = "index_propagation"  # listed in TASK_TYPE_CAPABILITY_INPUTS_KEY
_INPUTS_KEY = "indexer_id"


@pytest_asyncio.fixture(loop_scope="function")
async def _pending_rows(task_app_state, monkeypatch):
    """Plant N PENDING/retry_count=0 rows targeting one dead capability,
    aged past the production minimum. Forces the oracle to ``not live``.
    Cleans up the rows it created on teardown.

    Suppresses competing service tasks (dispatcher / queue_listener /
    proactive sweep) launched by ``TasksModule.lifespan``. Without this,
    the dispatcher CLAIMs a planted PENDING row, the patched oracle
    reports ``not live``, ``reset_task_to_pending`` bumps ``retry_count``
    to 1, and the bulk DLQ SQL (``WHERE retry_count = 0``) skips the
    row — leaving one row in PENDING and failing the "all rows
    DEAD_LETTER" assertion. The load-bearing invariant ("two concurrent
    ``sweep_dead_capability_rows`` calls serialise to one winner via
    ``pg_try_advisory_xact_lock``") is purely a property of the sweep
    path; isolating it from the live dispatcher keeps the test
    deterministic.
    """
    from dynastore.modules.concurrency import _background_tasks
    from dynastore.modules.tasks import capability_oracle

    cap_id = f"dead-cap-{uuid.uuid4().hex[:10]}"
    engine = task_app_state.engine
    task_schema = tasks_module.get_task_schema()

    # Cancel competing background services. ``BackgroundExecutor.submit``
    # names tasks ``<executor>:service:<name>``; match by substring so we
    # remain robust to the executor prefix.
    _COMPETING = (
        "service:dispatcher",
        "service:queue_listener",
        "service:proactive_capability_sweep",
    )
    competing = [
        t for t in list(_background_tasks)
        if not t.done() and any(s in (t.get_name() or "") for s in _COMPETING)
    ]
    for t in competing:
        t.cancel()
    if competing:
        await asyncio.gather(*competing, return_exceptions=True)

    async def _fake_is_live(_cap_id: str) -> bool:  # noqa: D401
        return False

    monkeypatch.setattr(
        capability_oracle, "is_capability_live", _fake_is_live,
    )
    # Also patch the symbol imported into dispatcher at function-call
    # time. ``sweep_dead_capability_rows`` does ``from ... import
    # is_capability_live`` inside the function so the patch above on the
    # source module already takes effect — no separate patch needed.

    n_rows = 5
    inserted: list[uuid.UUID] = []
    test_schema_name = f"test_sweep_524_{generate_test_id(6)}"
    for _ in range(n_rows):
        task = await tasks_module.create_task(
            engine,
            TaskCreate(
                task_type=_TASK_TYPE,
                caller_id="proactive-sweep-test",
                inputs={_INPUTS_KEY: cap_id},
            ),
            schema=test_schema_name,
            initial_status="PENDING",
        )
        assert task is not None
        inserted.append(task.task_id)

    # Age the rows past the sweep's min_age_s (the helper itself uses
    # ``timestamp < NOW() - make_interval``; pull timestamps backwards
    # so the WHERE-clause finds them).
    async with engine.begin() as conn:
        await conn.execute(
            text(
                f'UPDATE "{task_schema}".tasks '
                "SET timestamp = NOW() - INTERVAL '10 minutes' "
                "WHERE task_id = ANY(:ids)"
            ),
            {"ids": inserted},
        )

    yield cap_id, inserted, engine

    async with engine.begin() as conn:
        await conn.execute(
            text(f'DELETE FROM "{task_schema}".tasks WHERE task_id = ANY(:ids)'),
            {"ids": inserted},
        )


async def test_concurrent_sweep_advisory_lock_serialises_one_winner(_pending_rows):
    """Two concurrent ``sweep_dead_capability_rows`` calls against the
    same ``(cap, task_type)`` must produce exactly one nonzero result.

    This is the dedicated PG advisory-lock coverage promised by PR A's
    critical analysis — without it PR C cannot safely delete the
    reactive branch (the only other path that touches this lock).
    """
    cap_id, inserted, engine = _pending_rows
    task_schema = tasks_module.get_task_schema()

    results = await asyncio.gather(
        sweep_dead_capability_rows(engine, cap_id, task_type=_TASK_TYPE),
        sweep_dead_capability_rows(engine, cap_id, task_type=_TASK_TYPE),
    )

    nonzero = [r for r in results if r > 0]
    zero = [r for r in results if r == 0]
    # Load-bearing invariant: advisory lock serialises the pair — exactly
    # one call observes the lock as free, runs the bulk UPDATE, and
    # returns the count; the other observes contention and returns 0.
    assert len(nonzero) == 1 and len(zero) == 1, (
        f"advisory lock must serialise to one winner + one loser; got {results}"
    )
    # With the dispatcher / queue-listener / proactive-sweep service
    # tasks cancelled in the fixture, the winning sweep is the only
    # path touching the planted rows — so it MUST DLQ every one of
    # them in a single bulk UPDATE.
    assert nonzero[0] == len(inserted), (
        f"winner must DLQ all {len(inserted)} planted rows in one bulk "
        f"UPDATE; got {nonzero[0]}"
    )

    # Verify in PG: all rows are DEAD_LETTER + carry the dead-cap error_message.
    async with engine.connect() as conn:
        result = await conn.execute(
            text(
                f'SELECT status, error_message FROM "{task_schema}".tasks '
                "WHERE task_id = ANY(:ids)"
            ),
            {"ids": inserted},
        )
        rows = result.fetchall()
    assert len(rows) == len(inserted)
    for row in rows:
        assert row[0] == "DEAD_LETTER", f"row not DLQed: {row}"
        assert row[1] is not None and cap_id in row[1], (
            f"error_message must mention the dead capability: {row[1]!r}"
        )
