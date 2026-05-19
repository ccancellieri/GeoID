import asyncio
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.pool import StaticPool, NullPool
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DDLQuery,
    managed_transaction,
    ResultHandler,
)
from dynastore.modules.db_config.locking_tools import check_schema_exists, check_table_exists


@pytest_asyncio.fixture
async def shared_engine(db_url):
    """
    Creates an engine with a StaticPool to simulate a shared physical connection
    across all sessions and tasks. This is the hardest case for concurrency.
    """
    url = db_url.replace("postgresql://", "postgresql+asyncpg://")
    engine = create_async_engine(
        url,
        poolclass=StaticPool,
        connect_args={"server_settings": {"application_name": "concurrency_test"}},
        pool_reset_on_return="rollback",  # Ensure clean state for asyncpg
    )
    yield engine
    await engine.dispose()


@pytest.mark.asyncio
async def test_reentrancy_same_task(shared_engine):
    """Verifies that a single task can acquire the lock multiple times (reentrancy)."""
    async with managed_transaction(shared_engine) as conn:
        # First query
        res1 = await DQLQuery("SELECT 1", result_handler=ResultHandler.SCALAR).execute(
            conn
        )
        assert res1 == 1

        # Nested transaction (should be reentrant)
        async with managed_transaction(conn) as nested_conn:
            res2 = await DQLQuery(
                "SELECT 2", result_handler=ResultHandler.SCALAR
            ).execute(nested_conn)
            assert res2 == 2

            # Deeply nested manual lock
            res3 = await DQLQuery(
                "SELECT 3", result_handler=ResultHandler.SCALAR
            ).execute(nested_conn)
            assert res3 == 3


@pytest.mark.asyncio
async def test_concurrent_tasks_shared_engine(shared_engine):
    """
    Verifies that multiple concurrent tasks using the same Engine (shared StaticPool connection)
    are properly serialized and do not cause 'another operation in progress' errors.
    """

    async def run_query(task_id, delay=0.1):
        # We use a query that takes some time to execute
        sql = f"SELECT pg_sleep({delay}), {task_id}"
        async with managed_transaction(shared_engine) as conn:
            res = await DQLQuery(sql, result_handler=ResultHandler.ONE).execute(conn)
            return res[1]

    # Spawn 5 concurrent tasks
    tasks = [run_query(i) for i in range(5)]
    results = await asyncio.gather(*tasks)

    assert len(results) == 5
    assert set(results) == {0, 1, 2, 3, 4}


@pytest.mark.asyncio
async def test_absolute_root_convergence(shared_engine):
    """
    Verifies that different wrappers for the same physical connection converge on the same lock.
    """
    async with shared_engine.connect() as conn1:
        # Get raw connection to verify resolution
        # Create a session from the same connection
        session = AsyncSession(bind=conn1)

        # Attempting to lock the session should be REENTRANT because it's the same task
        # and we've already locked conn1. If convergence works, this won't block.
        async with asyncio.timeout(2):  # 2 second timeout to detect deadlock
            res = await DQLQuery(
                "SELECT 'convergence'", result_handler=ResultHandler.SCALAR
            ).execute(session)
            assert res == "convergence"


@pytest.mark.asyncio
async def test_concurrent_ddl_and_dml(shared_engine):
    """
    Verifies that DDL (which takes locks) and DML can run concurrently
    without causing asyncpg InterfaceErrors.
    """
    import uuid
    table_name = f"test_concurrency_{uuid.uuid4().hex[:8]}"

    await DDLQuery(
        f"CREATE TABLE IF NOT EXISTS {table_name} (id serial primary key, val text)"
    ).execute(shared_engine)

    async def do_ddl():
        await DDLQuery(f"COMMENT ON TABLE {table_name} IS 'testing'").execute(
            shared_engine
        )
        return "ddl_ok"

    async def do_dml(val):
        await DQLQuery(
            f"INSERT INTO {table_name} (val) VALUES (:val)",
            result_handler=ResultHandler.NONE,
        ).execute(shared_engine, val=val)
        return "dml_ok"

    # Run many mixed operations
    ops = []
    for i in range(10):
        ops.append(do_ddl())
        ops.append(do_dml(f"val_{i}"))

    results = await asyncio.gather(*ops)
    assert len(results) == 20
    assert "ddl_ok" in results
    assert "dml_ok" in results

    # Cleanup
    await DDLQuery(f"DROP TABLE {table_name}").execute(shared_engine)


@pytest.mark.asyncio
async def test_managed_transaction_serialization(shared_engine):
    """
    Explicitly test that managed_transaction prevents overlapping 'begin()' calls
    on a shared engine with StaticPool.
    """

    async def concurrent_begin(task_id):
        # managed_transaction should serialize the start of the transaction
        async with managed_transaction(shared_engine) as conn:
            # We are now in a transaction
            await asyncio.sleep(0.05)
            await DQLQuery("SELECT 1", result_handler=ResultHandler.SCALAR).execute(
                conn
            )
        return task_id

    # If serialization fails, one of these will hit InterfaceError during BEGIN
    tasks = [concurrent_begin(i) for i in range(10)]
    results = await asyncio.gather(*tasks)
    assert len(results) == 10


@pytest_asyncio.fixture
async def null_pool_engine(db_url):
    """
    Engine with NullPool: each acquire() creates a fresh connection, simulating
    two separate processes (e.g., api + worker containers) using the same DB.
    """
    url = db_url.replace("postgresql://", "postgresql+asyncpg://")
    engine = create_async_engine(url, poolclass=NullPool)
    yield engine
    await engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_schema_table_creation_no_race(null_pool_engine):
    """
    Regression test for the DDLExecutor advisory-lock race condition.

    Scenario: two containers start simultaneously and both try to initialize
    the same schema + table via DDLQuery.  With the old 'skip-on-lock-fail'
    behaviour the second container would:
      1. skip schema creation (another worker holds the advisory lock)
      2. immediately attempt CREATE TABLE
      3. fail with "schema does not exist" (first container hasn't committed yet)

    With the fix the second container blocks on pg_advisory_xact_lock, waits
    for the first to commit, re-checks existence, and proceeds cleanly.
    """
    schema_name = "test_ddl_race_fix"
    table_name = "items"

    # Ensure clean state
    async with managed_transaction(null_pool_engine) as conn:
        await DDLQuery(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE').execute(conn)

    async def init_worker():
        """Mimics one container's module-initialization sequence."""
        async with managed_transaction(null_pool_engine) as conn:
            # Step 1 — ensure schema (may be contested by the other worker)
            await DDLQuery(
                f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"',
                check_query=f"SELECT 1 FROM pg_namespace WHERE nspname = '{schema_name}'",
            ).execute(conn)
            # Tiny yield to maximise interleaving probability
            await asyncio.sleep(0)
            # Step 2 — create table (requires schema to be committed/visible)
            await DDLQuery(
                f"""CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
                    id   serial PRIMARY KEY,
                    val  text
                )""",
                check_query=(
                    f"SELECT 1 FROM pg_tables "
                    f"WHERE schemaname = '{schema_name}' AND tablename = '{table_name}'"
                ),
            ).execute(conn)

    # Two workers racing — must both complete without exceptions
    await asyncio.gather(init_worker(), init_worker())

    # Verify the schema and table actually exist
    async with managed_transaction(null_pool_engine) as conn:
        assert await check_schema_exists(conn, schema_name), "schema missing after concurrent init"
        assert await check_table_exists(conn, table_name, schema=schema_name), "table missing after concurrent init"

    # Cleanup
    async with managed_transaction(null_pool_engine) as conn:
        await DDLQuery(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE').execute(conn)


@pytest.mark.asyncio
async def test_cancellation_mid_query_leaves_pool_clean(shared_engine, caplog):
    """#640: a CancelledError fired mid-fetch must drain ROLLBACK on the
    wire before the connection returns to the pool. StaticPool reuses the
    same physical asyncpg wire, so the next acquire would trip the
    invalidate-recover path (#619) if state 15 (IN_QUERY) leaked.

    Asserts:
      1. Cancelling a long-running query inside ``managed_transaction``
         propagates ``CancelledError`` to the caller.
      2. A subsequent acquire on the same engine runs a trivial query
         to completion — no state-15 trip, no pool-hygiene WARN.
      3. The ``managed_transaction_cancel_drain`` WARN fires exactly
         once with the cancelled wire_id.
    """
    import logging

    caplog.set_level(logging.WARNING)
    # Ensure the query_executor logger propagates to caplog under xdist.
    qe_logger = logging.getLogger("dynastore.modules.db_config.query_executor")
    _prev_propagate = qe_logger.propagate
    qe_logger.propagate = True
    qe_logger.setLevel(logging.WARNING)

    async def _long_query_body():
        async with managed_transaction(shared_engine) as conn:
            await DQLQuery(
                "SELECT pg_sleep(10)", result_handler=ResultHandler.SCALAR,
            ).execute(conn)

    try:
        task = asyncio.create_task(_long_query_body())
        await asyncio.sleep(0.2)  # let pg_sleep start on the wire
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        qe_logger.propagate = _prev_propagate

    drain_warnings = [
        r for r in caplog.records
        if "managed_transaction_cancel_drain" in r.getMessage()
    ]
    assert len(drain_warnings) == 1, (
        f"expected exactly one cancel-drain WARN, got {len(drain_warnings)}: "
        f"{[r.getMessage() for r in drain_warnings]}"
    )
    assert "wire_id=" in drain_warnings[0].getMessage()
    assert "exc=" in drain_warnings[0].getMessage()

    pool_hygiene_before = sum(
        1 for r in caplog.records
        if "pool-hygiene" in r.getMessage()
    )

    async with managed_transaction(shared_engine) as conn:
        result = await DQLQuery(
            "SELECT 1", result_handler=ResultHandler.SCALAR,
        ).execute(conn)
    assert result == 1

    pool_hygiene_after = sum(
        1 for r in caplog.records
        if "pool-hygiene" in r.getMessage()
    )
    assert pool_hygiene_after == pool_hygiene_before, (
        "next acquire tripped pool-hygiene invalidate-recover — drain failed to clean the wire"
    )
