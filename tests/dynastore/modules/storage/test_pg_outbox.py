"""Tests for ``storage_outbox`` + ``index_failure_log`` DDL.

Each test runs against a freshly created throwaway PG schema so the
DDL is exercised against an empty namespace. The schema is dropped on
teardown via the writer fixture, which is also the connection that owns
``CREATE SCHEMA`` and so retains drop authority.

The DDL helpers under test are the ``*_asyncpg`` entry points — these
take a raw ``asyncpg.Connection`` and execute the rendered DDL directly
so LISTEN / NOTIFY can be exercised on the same physical session. The
production catalog-bootstrap path uses
:func:`dynastore.modules.storage.outbox_ddl.ensure_storage_outbox`
(without the ``_asyncpg`` suffix) which routes through ``DDLQuery``.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from dynastore.tools.identifiers import generate_id_hex


async def _create_fresh_schema(*conns: object) -> str:
    """Create a per-test schema and point every supplied connection at it.

    Works on raw ``asyncpg.Connection`` only — every fixture in this file
    yields one. Returns the schema name so tests can pass it explicitly to
    the ``*_asyncpg`` DDL helpers and build NOTIFY channels that match
    what ``current_schema()`` will see inside the DDL function.
    """
    schema = f"outbox_t_{generate_id_hex()[:10]}"
    # The first connection owns the schema. Authority to drop it stays
    # with whichever session is the original creator.
    await conns[0].execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')  # type: ignore[attr-defined]
    for c in conns:
        await c.execute(f'SET search_path TO "{schema}"')  # type: ignore[attr-defined]
    return schema


async def _drop_schema(conn: object, schema: str) -> None:
    """Best-effort schema teardown."""
    try:
        # Reset search_path first so the DROP doesn't reference the schema
        # we're about to remove via current_schema().
        await conn.execute("RESET search_path")  # type: ignore[attr-defined]
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')  # type: ignore[attr-defined]
    except Exception:
        pass


@pytest.mark.asyncio
async def test_storage_outbox_table_exists(async_conn):
    from dynastore.modules.storage.outbox_ddl import ensure_storage_outbox_asyncpg

    schema = await _create_fresh_schema(async_conn)
    try:
        await ensure_storage_outbox_asyncpg(async_conn, schema)
        row = await async_conn.fetchrow(  # type: ignore[attr-defined]
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_name='storage_outbox' AND table_schema=$1",
            schema,
        )
        assert row is not None
    finally:
        await _drop_schema(async_conn, schema)


@pytest.mark.asyncio
async def test_drain_index_is_partial(async_conn):
    from dynastore.modules.storage.outbox_ddl import ensure_storage_outbox_asyncpg

    schema = await _create_fresh_schema(async_conn)
    try:
        await ensure_storage_outbox_asyncpg(async_conn, schema)
        rows = await async_conn.fetch(  # type: ignore[attr-defined]
            "SELECT indexdef FROM pg_indexes "
            "WHERE indexname='storage_outbox_drain_idx' AND schemaname=$1",
            schema,
        )
        assert rows and "WHERE" in rows[0]["indexdef"]
    finally:
        await _drop_schema(async_conn, schema)


@pytest.mark.asyncio
async def test_ensure_storage_outbox_idempotent(async_conn):
    from dynastore.modules.storage.outbox_ddl import ensure_storage_outbox_asyncpg

    schema = await _create_fresh_schema(async_conn)
    try:
        await ensure_storage_outbox_asyncpg(async_conn, schema)
        await ensure_storage_outbox_asyncpg(async_conn, schema)
        row = await async_conn.fetchrow(  # type: ignore[attr-defined]
            "SELECT count(*) AS n FROM information_schema.tables "
            "WHERE table_name='storage_outbox' AND table_schema=$1",
            schema,
        )
        assert row["n"] == 1
    finally:
        await _drop_schema(async_conn, schema)


@pytest.mark.asyncio
async def test_index_failure_log_table_exists(async_conn):
    from dynastore.modules.storage.outbox_ddl import ensure_index_failure_log_asyncpg

    schema = await _create_fresh_schema(async_conn)
    try:
        await ensure_index_failure_log_asyncpg(async_conn, schema)
        row = await async_conn.fetchrow(  # type: ignore[attr-defined]
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_name='index_failure_log' AND table_schema=$1",
            schema,
        )
        assert row is not None
    finally:
        await _drop_schema(async_conn, schema)


@pytest.mark.asyncio
async def test_pg_outbox_enqueue_bulk_persists(async_conn, async_schema):
    """COPY-based bulk enqueue persists rows.

    The ``async_schema`` fixture has already set ``search_path`` on
    ``async_conn`` so bare ``storage_outbox`` lands in the right namespace.
    """
    from dynastore.modules.storage.pg_outbox import PgOutboxStore
    from dynastore.modules.storage.outbox_ddl import ensure_storage_outbox_asyncpg
    from dynastore.models.protocols.indexing import OutboxRecord

    await ensure_storage_outbox_asyncpg(async_conn, async_schema)
    store = PgOutboxStore(pool=None, single_conn=async_conn)
    rows = [
        OutboxRecord(
            op_id=uuid4(), driver_id="d", driver_instance_id="di",
            collection_id="cc", op="upsert", item_id=f"i{i}",
            payload={"i": i}, idempotency_key=f"i{i}",
        )
        for i in range(2000)
    ]
    await store.enqueue_bulk(async_conn, catalog_id=async_schema, rows=rows)
    n = await async_conn.fetchval("SELECT count(*) FROM storage_outbox")  # type: ignore[attr-defined]
    assert n == 2000


@pytest.mark.asyncio
async def test_pg_outbox_claim_batch_skip_locked(
    async_conn, second_async_conn, async_schema,
):
    """Two concurrent claimers each get a disjoint subset.

    The fixture sets ``search_path`` only on ``async_conn``; we replicate
    that on ``second_async_conn`` here because it's a wholly independent
    asyncpg session.
    """
    from dynastore.modules.storage.pg_outbox import PgOutboxStore
    from dynastore.modules.storage.outbox_ddl import ensure_storage_outbox_asyncpg
    from dynastore.models.protocols.indexing import OutboxRecord

    await ensure_storage_outbox_asyncpg(async_conn, async_schema)
    await second_async_conn.execute(  # type: ignore[attr-defined]
        f'SET search_path TO "{async_schema}"',
    )

    store = PgOutboxStore(pool=None, single_conn=async_conn)
    await store.enqueue_bulk(
        async_conn, catalog_id=async_schema,
        rows=[
            OutboxRecord(
                op_id=uuid4(), driver_id="d", driver_instance_id="di",
                collection_id="cc", op="upsert", item_id=str(i),
                payload={}, idempotency_key=str(i),
            )
            for i in range(10)
        ],
    )

    store2 = PgOutboxStore(pool=None, single_conn=second_async_conn)
    async with async_conn.transaction(), second_async_conn.transaction():  # type: ignore[attr-defined]
        b1 = await store.claim_batch(
            driver_id="d", catalog_id=async_schema,
            batch_size=5, claimed_by="w1",
        )
        b2 = await store2.claim_batch(
            driver_id="d", catalog_id=async_schema,
            batch_size=5, claimed_by="w2",
        )
    assert len(b1) == 5 and len(b2) == 5
    assert {r.op_id for r in b1}.isdisjoint({r.op_id for r in b2})


@pytest.mark.asyncio
async def test_pg_outbox_mark_done_retry_failed(async_conn, async_schema):
    """``mark_done`` / ``mark_retry`` / ``mark_failed`` update status."""
    from dynastore.modules.storage.pg_outbox import PgOutboxStore
    from dynastore.modules.storage.outbox_ddl import ensure_storage_outbox_asyncpg
    from dynastore.models.protocols.indexing import OutboxRecord

    await ensure_storage_outbox_asyncpg(async_conn, async_schema)
    store = PgOutboxStore(pool=None, single_conn=async_conn)
    op_ids = [uuid4() for _ in range(3)]
    await store.enqueue_bulk(
        async_conn, catalog_id=async_schema,
        rows=[
            OutboxRecord(
                op_id=op_ids[i], driver_id="d", driver_instance_id="di",
                collection_id="cc", op="upsert", item_id=str(i),
                payload={}, idempotency_key=str(i),
            )
            for i in range(3)
        ],
    )

    await store.mark_done(catalog_id=async_schema, op_ids=[op_ids[0]])
    await store.mark_retry(
        catalog_id=async_schema, op_ids=[op_ids[1]],
        error="transient", attempts_seen=0,
    )
    await store.mark_failed(
        catalog_id=async_schema, op_ids=[op_ids[2]], error="poison",
    )

    statuses = {
        r["op_id"]: r["status"]
        for r in await async_conn.fetch(  # type: ignore[attr-defined]
            "SELECT op_id, status FROM storage_outbox",
        )
    }
    assert statuses[op_ids[0]] == "done"
    assert statuses[op_ids[1]] == "ready"
    assert statuses[op_ids[2]] == "failed"


@pytest.mark.asyncio
async def test_pg_outbox_mark_retry_bumps_attempts_and_delays_ready_at(
    async_conn, async_schema,
):
    """``mark_retry`` increments ``attempts`` and pushes ``ready_at`` out."""
    from dynastore.modules.storage.pg_outbox import PgOutboxStore
    from dynastore.modules.storage.outbox_ddl import ensure_storage_outbox_asyncpg
    from dynastore.models.protocols.indexing import OutboxRecord

    await ensure_storage_outbox_asyncpg(async_conn, async_schema)
    store = PgOutboxStore(pool=None, single_conn=async_conn)
    op_id = uuid4()
    await store.enqueue_bulk(
        async_conn, catalog_id=async_schema,
        rows=[
            OutboxRecord(
                op_id=op_id, driver_id="d", driver_instance_id="di",
                collection_id="cc", op="upsert", item_id="i",
                payload={}, idempotency_key="i",
            ),
        ],
    )
    await store.mark_retry(
        catalog_id=async_schema, op_ids=[op_id],
        error="net blip", attempts_seen=0,
    )

    row = await async_conn.fetchrow(  # type: ignore[attr-defined]
        "SELECT status, attempts, last_error, ready_at > now() AS delayed "
        "FROM storage_outbox WHERE op_id = $1",
        op_id,
    )
    assert row["status"] == "ready"
    assert row["attempts"] == 1
    assert row["last_error"] == "net blip"
    assert row["delayed"] is True


@pytest.mark.asyncio
async def test_insert_emits_notify(async_conn, second_async_conn):
    """AFTER INSERT trigger must NOTIFY on outbox_<driver_id>_<schema>."""
    from dynastore.modules.storage.outbox_ddl import ensure_storage_outbox_asyncpg

    schema = await _create_fresh_schema(async_conn, second_async_conn)
    try:
        await ensure_storage_outbox_asyncpg(async_conn, schema)
        channel = f"outbox_d_{schema}"

        received = asyncio.Event()
        payload_holder: dict[str, str] = {}

        async def _on_notify(conn, pid, ch, payload):  # noqa: ANN001
            payload_holder["p"] = payload
            received.set()

        await second_async_conn.add_listener(channel, _on_notify)  # type: ignore[attr-defined]
        try:
            await async_conn.execute(  # type: ignore[attr-defined]
                "INSERT INTO storage_outbox "
                "(op_id, driver_id, driver_instance_id, collection_id, op, "
                " payload, idempotency_key) "
                "VALUES ($1, 'd', 'di', 'col', 'upsert', '{}'::jsonb, 'k')",
                uuid4(),
            )
            await asyncio.wait_for(received.wait(), timeout=2.0)
        finally:
            await second_async_conn.remove_listener(channel, _on_notify)  # type: ignore[attr-defined]
        assert "driver_id" in payload_holder["p"]
    finally:
        await _drop_schema(async_conn, schema)
