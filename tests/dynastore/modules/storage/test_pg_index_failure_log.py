"""PgIndexFailureLog tests — record + list_failures with filters
and pagination against the per-tenant index_failure_log table."""
from __future__ import annotations

from uuid import uuid4

import pytest


@pytest.mark.asyncio
async def test_pg_failure_log_record_persists(async_conn, async_schema):
    from dynastore.modules.storage.outbox_ddl import ensure_index_failure_log_asyncpg
    from dynastore.modules.storage.pg_index_failure_log import PgIndexFailureLog

    await ensure_index_failure_log_asyncpg(async_conn, async_schema)
    log = PgIndexFailureLog(single_conn=async_conn)

    op_id = uuid4()
    await log.record(
        async_conn,
        catalog_id=async_schema, collection_id="cc",
        driver_instance_id="x", driver_id="d",
        op_id=op_id, item_id="i1", op="upsert",
        attempts=3, error_class="TransientIndexerError",
        error_message="blip", status="retrying",
    )

    row = await async_conn.fetchrow(
        "SELECT collection_id, status, attempts FROM index_failure_log "
        "WHERE op_id=$1", op_id,
    )
    assert row["collection_id"] == "cc"
    assert row["status"] == "retrying"
    assert row["attempts"] == 3


@pytest.mark.asyncio
async def test_pg_failure_log_record_with_correlation_id(async_conn, async_schema):
    from dynastore.modules.storage.outbox_ddl import ensure_index_failure_log_asyncpg
    from dynastore.modules.storage.pg_index_failure_log import PgIndexFailureLog

    await ensure_index_failure_log_asyncpg(async_conn, async_schema)
    log = PgIndexFailureLog(single_conn=async_conn)

    op_id = uuid4()
    await log.record(
        async_conn,
        catalog_id=async_schema, collection_id="cc",
        driver_instance_id="x", driver_id="d",
        op_id=op_id, item_id="i1", op="upsert",
        attempts=5, error_class="X", error_message="m",
        status="failed", correlation_id="trace-abc-123",
    )

    row = await async_conn.fetchrow(
        "SELECT correlation_id FROM index_failure_log WHERE op_id=$1", op_id,
    )
    assert row["correlation_id"] == "trace-abc-123"


@pytest.mark.asyncio
async def test_pg_failure_log_list_filters_collection(async_conn, async_schema):
    from dynastore.modules.storage.outbox_ddl import ensure_index_failure_log_asyncpg
    from dynastore.modules.storage.pg_index_failure_log import PgIndexFailureLog

    await ensure_index_failure_log_asyncpg(async_conn, async_schema)
    log = PgIndexFailureLog(single_conn=async_conn)

    for i in range(15):
        await log.record(
            async_conn,
            catalog_id=async_schema,
            collection_id="A" if i % 2 == 0 else "B",
            driver_instance_id="x", driver_id="d",
            op_id=uuid4(), item_id=str(i), op="upsert",
            attempts=1, error_class="X", error_message="m",
            status="failed",
        )

    rows = await log.list_failures(catalog_id=async_schema, collection_id="A", limit=100)
    assert len(rows) == 8  # 0,2,4,6,8,10,12,14
    assert all(r.collection_id == "A" for r in rows)


@pytest.mark.asyncio
async def test_pg_failure_log_list_paginates(async_conn, async_schema):
    from dynastore.modules.storage.outbox_ddl import ensure_index_failure_log_asyncpg
    from dynastore.modules.storage.pg_index_failure_log import PgIndexFailureLog

    await ensure_index_failure_log_asyncpg(async_conn, async_schema)
    log = PgIndexFailureLog(single_conn=async_conn)

    for i in range(15):
        await log.record(
            async_conn,
            catalog_id=async_schema, collection_id="cc",
            driver_instance_id="x", driver_id="d",
            op_id=uuid4(), item_id=str(i), op="upsert",
            attempts=1, error_class="X", error_message="m",
            status="failed",
        )

    page1 = await log.list_failures(catalog_id=async_schema, limit=5, offset=0)
    page2 = await log.list_failures(catalog_id=async_schema, limit=5, offset=5)
    page3 = await log.list_failures(catalog_id=async_schema, limit=5, offset=10)
    assert len(page1) == 5 and len(page2) == 5 and len(page3) == 5
    seen = {r.failure_id for r in page1} | {r.failure_id for r in page2} | {r.failure_id for r in page3}
    assert len(seen) == 15  # disjoint pages


@pytest.mark.asyncio
async def test_pg_failure_log_list_filters_driver_status(async_conn, async_schema):
    from dynastore.modules.storage.outbox_ddl import ensure_index_failure_log_asyncpg
    from dynastore.modules.storage.pg_index_failure_log import PgIndexFailureLog

    await ensure_index_failure_log_asyncpg(async_conn, async_schema)
    log = PgIndexFailureLog(single_conn=async_conn)

    statuses = ["retrying", "retrying", "failed", "failed", "failed"]
    drivers = ["d1", "d2", "d1", "d2", "d2"]
    for s, d in zip(statuses, drivers):
        await log.record(
            async_conn,
            catalog_id=async_schema, collection_id="cc",
            driver_instance_id="x", driver_id=d,
            op_id=uuid4(), item_id="i", op="upsert",
            attempts=1, error_class="X", error_message="m",
            status=s,
        )

    only_failed = await log.list_failures(catalog_id=async_schema, status="failed")
    assert len(only_failed) == 3
    only_d2 = await log.list_failures(catalog_id=async_schema, driver_id="d2")
    assert len(only_d2) == 3
    only_d2_failed = await log.list_failures(catalog_id=async_schema, driver_id="d2", status="failed")
    assert len(only_d2_failed) == 2
