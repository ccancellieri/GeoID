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

"""End-to-end atomicity tests for ``PgOutboxStore.enqueue_bulk`` against a
real SQLAlchemy ``AsyncConnection``.

These tests verify the co-transactional path (``conn is not None``) that
was broken before the fix: the old code called
``conn.copy_records_to_table(...)`` on a SQLAlchemy ``AsyncConnection``,
which has no such method → ``AttributeError`` at runtime for any collection
with an ASYNC+OUTBOX secondary-index routing entry.

The fix replaces that branch with a schema-qualified ``DQLQuery`` INSERT
executed on the caller's SA connection inside the caller's transaction.

Two properties are proven here against live PG:

1. **Persistence** — rows inserted via ``managed_transaction`` + commit
   are readable afterwards.

2. **Atomicity** — when the outer transaction is rolled back (simulating
   a primary-write failure that raises after the enqueue), the outbox
   rows are rolled back too and are absent from the table.

Schema isolation: each test creates a throwaway PG schema (mirroring the
pattern in ``test_pg_outbox.py``) and drops it on teardown. The
``sa_engine`` fixture below uses the same ``DATABASE_URL`` environment
variable as every other live-PG test in this suite; tests that require
a live PG automatically skip when the DB is unreachable — the
``pytest.importorskip`` / ``asyncpg.connect`` call in the conftest
fixture surfaces the skip naturally.
"""

from __future__ import annotations

import os
from typing import AsyncIterator
from uuid import uuid4

import pytest
import pytest_asyncio

from dynastore.tools.identifiers import generate_id_hex


def _sa_db_url() -> str:
    """SA-compatible asyncpg URL (``postgresql+asyncpg://...``)."""
    url = os.getenv(
        "DATABASE_URL",
        "postgresql://testuser:testpassword@localhost:54320/gis_dev",
    )
    if not url.startswith("postgresql+asyncpg://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


@pytest_asyncio.fixture
async def sa_engine():
    """Minimal SQLAlchemy ``AsyncEngine`` for outbox SA-conn tests.

    Uses ``NullPool`` so connections are not retained between tests
    (matches the session-level ``db_reset_session`` engine pattern
    in ``tests/dynastore/conftest.py``).

    Skips the whole test if the DB is unreachable or SQLAlchemy /
    asyncpg are not installed.
    """
    sqlalchemy_async = pytest.importorskip(
        "sqlalchemy.ext.asyncio",
        reason="sqlalchemy[asyncio] not installed",
    )
    create_async_engine = sqlalchemy_async.create_async_engine

    pytest.importorskip("asyncpg", reason="asyncpg not installed")

    from sqlalchemy.pool import NullPool

    url = _sa_db_url()
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        # Probe connectivity; raises if DB is unreachable → pytest skip.
        async with engine.connect() as probe:
            await probe.close()
    except Exception as exc:  # noqa: BLE001
        await engine.dispose()
        pytest.skip(f"Live PG unavailable ({exc!s}); skipping SA-conn outbox tests.")

    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def sa_schema(sa_engine) -> AsyncIterator[str]:  # noqa: ANN001
    """Per-test PG schema, created and torn down via the SA engine.

    Mirrors the ``async_schema`` fixture in conftest.py but uses
    SQLAlchemy rather than raw asyncpg so the DDL runs through the same
    stack as production code.
    """
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )

    schema = f"outbox_sa_{generate_id_hex()[:10]}"
    async with managed_transaction(sa_engine) as conn:
        await DQLQuery(
            f'CREATE SCHEMA IF NOT EXISTS "{schema}"',
            result_handler=ResultHandler.NONE,
        ).execute(conn)

    # Bootstrap storage_outbox DDL in the new schema.
    from dynastore.modules.storage.outbox_ddl import ensure_storage_outbox
    await ensure_storage_outbox(sa_engine, schema)

    try:
        yield schema
    finally:
        async with managed_transaction(sa_engine) as conn:
            try:
                await DQLQuery(
                    f'DROP SCHEMA IF EXISTS "{schema}" CASCADE',
                    result_handler=ResultHandler.NONE,
                ).execute(conn)
            except Exception:  # noqa: BLE001 — best-effort teardown
                pass


# ---------------------------------------------------------------------------
# Helper: count rows in the outbox via a fresh SA connection (reads committed
# data so we can assert across transaction boundaries).
# ---------------------------------------------------------------------------


async def _count_rows(engine, schema: str) -> int:
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )

    async with managed_transaction(engine) as conn:
        count = await DQLQuery(
            f'SELECT count(*) FROM "{schema}".storage_outbox',
            result_handler=ResultHandler.SCALAR,
        ).execute(conn)
        return count or 0


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enqueue_bulk_sa_conn_persists_rows(sa_engine, sa_schema):
    """``enqueue_bulk`` with a SA ``AsyncConnection`` inserts rows durably.

    Before the fix this raised ``AttributeError: 'AsyncConnection' object
    has no attribute 'copy_records_to_table'``.
    """
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.pg_outbox import PgOutboxStore
    from dynastore.models.protocols.indexing import OutboxRecord

    # Store is constructed with a dummy pool sentinel; only the SA conn path
    # is exercised here (pool is never acquired in the conn-is-not-None branch).
    store = PgOutboxStore(pool=object(), single_conn=None)

    rows = [
        OutboxRecord(
            op_id=uuid4(),
            driver_id="d",
            driver_instance_id="di",
            collection_id="cc",
            op="upsert",
            item_id=f"item_{i}",
            payload={"idx": i},
            idempotency_key=f"ik_{i}",
        )
        for i in range(5)
    ]

    async with managed_transaction(sa_engine) as conn:
        await store.enqueue_bulk(conn, catalog_id=sa_schema, rows=rows)
        # Still inside the transaction — count from the same connection.
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler,
        )
        in_tx_count = await DQLQuery(
            f'SELECT count(*) FROM "{sa_schema}".storage_outbox',
            result_handler=ResultHandler.SCALAR,
        ).execute(conn) or 0
    # Transaction committed. Now read from a fresh connection.
    committed_count = await _count_rows(sa_engine, sa_schema)

    assert in_tx_count == 5, f"expected 5 in-tx rows, got {in_tx_count}"
    assert committed_count == 5, f"expected 5 committed rows, got {committed_count}"


@pytest.mark.asyncio
async def test_enqueue_bulk_sa_conn_rolls_back_with_outer_tx(sa_engine, sa_schema):
    """When the outer transaction aborts after enqueue, the outbox rows vanish.

    This proves atomicity: the outbox INSERT participates in the same
    transaction as the primary write. If the primary write raises after
    the enqueue, ``managed_transaction`` rolls back both.
    """
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.pg_outbox import PgOutboxStore
    from dynastore.models.protocols.indexing import OutboxRecord

    store = PgOutboxStore(pool=object(), single_conn=None)

    row = OutboxRecord(
        op_id=uuid4(),
        driver_id="d",
        driver_instance_id="di",
        collection_id="cc",
        op="upsert",
        item_id="item_rollback",
        payload={"rollback": True},
        idempotency_key="ik_rollback",
    )

    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler,
    )

    with pytest.raises(RuntimeError, match="simulated primary write failure"):
        async with managed_transaction(sa_engine) as conn:
            await store.enqueue_bulk(conn, catalog_id=sa_schema, rows=[row])
            # Prove the INSERT actually landed inside THIS transaction before
            # we abort it — otherwise the post-rollback count==0 assertion
            # below would pass trivially even if the enqueue had never written
            # (e.g. raised early). This is what makes the test a real atomicity
            # proof: row present mid-tx, absent after rollback.
            mid_tx = await DQLQuery(
                f'SELECT count(*) FROM "{sa_schema}".storage_outbox',
                result_handler=ResultHandler.SCALAR,
            ).execute(conn)
            assert mid_tx == 1, f"expected enqueued row visible in-tx, got {mid_tx}"
            # Simulate the primary write failing after the outbox enqueue.
            raise RuntimeError("simulated primary write failure")

    # The transaction was rolled back; the outbox row must not be present.
    count = await _count_rows(sa_engine, sa_schema)
    assert count == 0, (
        f"expected 0 rows after rollback, got {count}; "
        "outbox row survived the rolled-back transaction"
    )
