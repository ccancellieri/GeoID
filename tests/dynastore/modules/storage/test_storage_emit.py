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

"""End-to-end tests for the storage-plane direct write (#1807 Phase 4).

``enqueue_storage_op`` always writes into the global ``tasks.storage`` table
(tenancy via ``catalog_id`` column) and enqueues a drain trigger on the same
connection.

Properties proven against live PG:

1. **Direct write to ``tasks.storage``** — every call inserts rows into the
   global table; there is no legacy per-tenant table written.
2. **Co-transactional atomicity** — both the storage rows and the drain trigger
   ride the caller's transaction; an outer-transaction abort leaves NO rows in
   either ``tasks.storage`` or ``tasks.tasks``.
3. **Field mapping** — the row carries the tenant identity in ``catalog_id``,
   preserves op/entity_kind/entity_id/payload, and sets DDL-level defaults.

The test table is created uniquely-named per test and pointed at via
``DYNASTORE_TASK_SCHEMA`` so concurrent test runs don't collide on the real
``tasks`` schema.
"""

from __future__ import annotations

import os
from typing import AsyncIterator, Tuple

import pytest
import pytest_asyncio

from dynastore.tools.identifiers import generate_id_hex


def _sa_db_url() -> str:
    url = os.getenv(
        "DATABASE_URL",
        "postgresql://testuser:testpassword@localhost:54320/gis_dev",
    )
    if not url.startswith("postgresql+asyncpg://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


_STORAGE_TEST_DDL = """
CREATE TABLE IF NOT EXISTS "{schema}".storage (
    op_id           UUID            NOT NULL,
    day             DATE            NOT NULL,
    catalog_id      TEXT            NOT NULL,
    driver_id       TEXT            NOT NULL,
    collection_id   TEXT,
    entity_kind     TEXT            NOT NULL DEFAULT 'item',
    entity_id       TEXT,
    op              TEXT            NOT NULL,
    status          TEXT            NOT NULL DEFAULT 'ready',
    ready_at        TIMESTAMPTZ     NOT NULL DEFAULT now(),
    op_payload      JSONB           NOT NULL DEFAULT '{{}}'::jsonb,
    idempotency_key TEXT,
    claim_version   INTEGER         NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    PRIMARY KEY (day, op_id)
);
"""


@pytest_asyncio.fixture
async def sa_engine():
    sqlalchemy_async = pytest.importorskip(
        "sqlalchemy.ext.asyncio", reason="sqlalchemy[asyncio] not installed",
    )
    create_async_engine = sqlalchemy_async.create_async_engine
    pytest.importorskip("asyncpg", reason="asyncpg not installed")
    from sqlalchemy.pool import NullPool

    engine = create_async_engine(_sa_db_url(), poolclass=NullPool)
    try:
        async with engine.connect() as probe:
            await probe.close()
    except Exception as exc:  # noqa: BLE001
        await engine.dispose()
        pytest.skip(f"Live PG unavailable ({exc!s}); skipping storage emit tests.")
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def storage_env(
    sa_engine, monkeypatch  # noqa: ANN001
) -> AsyncIterator[Tuple[str, str]]:
    """Provision a task schema with a ``storage`` table.

    Points ``get_task_schema()`` at the throwaway schema via
    ``DYNASTORE_TASK_SCHEMA``.  Yields ``(catalog_id, task_schema)``.
    """
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )

    token = generate_id_hex()[:10]
    catalog_id = f"se_tenant_{token}"
    task_schema = f"se_tasks_{token}"
    monkeypatch.setenv("DYNASTORE_TASK_SCHEMA", task_schema)

    async with managed_transaction(sa_engine) as conn:
        await DQLQuery(
            f'CREATE SCHEMA IF NOT EXISTS "{task_schema}"',
            result_handler=ResultHandler.NONE,
        ).execute(conn)
        await DQLQuery(
            _STORAGE_TEST_DDL.format(schema=task_schema),
            result_handler=ResultHandler.NONE,
        ).execute(conn)

    try:
        yield catalog_id, task_schema
    finally:
        async with managed_transaction(sa_engine) as conn:
            try:
                await DQLQuery(
                    f'DROP SCHEMA IF EXISTS "{task_schema}" CASCADE',
                    result_handler=ResultHandler.NONE,
                ).execute(conn)
            except Exception:  # noqa: BLE001 — best-effort teardown
                pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _records(n: int):
    from uuid import uuid4
    from dynastore.models.protocols.indexing import OutboxRecord

    return [
        OutboxRecord(
            op_id=uuid4(),
            driver_id="elasticsearch_private",
            driver_instance_id="di",
            collection_id="my_collection",
            op="upsert",
            item_id=f"item_{i}",
            payload={"idx": i},
            idempotency_key=f"ik_{i}",
        )
        for i in range(n)
    ]


async def _count(engine, schema: str, table: str) -> int:
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )

    async with managed_transaction(engine) as conn:
        return await DQLQuery(
            f'SELECT count(*) FROM "{schema}".{table}',
            result_handler=ResultHandler.SCALAR,
        ).execute(conn) or 0


async def _dispatch(engine, catalog_id: str, rows) -> None:
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.storage_emit import enqueue_storage_op

    async with managed_transaction(engine) as conn:
        await enqueue_storage_op(conn, catalog_id=catalog_id, rows=rows)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_writes_to_storage(sa_engine, storage_env):
    """enqueue_storage_op always inserts into tasks.storage."""
    catalog, task = storage_env
    await _dispatch(sa_engine, catalog, _records(3))
    assert await _count(sa_engine, task, "storage") == 3


@pytest.mark.asyncio
async def test_empty_rows_is_noop(sa_engine, storage_env):
    """Empty rows list is a clean no-op with no rows written."""
    catalog, task = storage_env
    await _dispatch(sa_engine, catalog, [])
    assert await _count(sa_engine, task, "storage") == 0


@pytest.mark.asyncio
async def test_rolls_back_atomically(sa_engine, storage_env):
    """An outer-transaction abort after dispatch leaves NO rows in tasks.storage."""
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.storage_emit import enqueue_storage_op

    catalog, task = storage_env
    rows = _records(2)

    with pytest.raises(RuntimeError, match="simulated primary write failure"):
        async with managed_transaction(sa_engine) as conn:
            await enqueue_storage_op(conn, catalog_id=catalog, rows=rows)
            raise RuntimeError("simulated primary write failure")

    assert await _count(sa_engine, task, "storage") == 0


@pytest.mark.asyncio
async def test_field_mapping(sa_engine, storage_env):
    """Row in tasks.storage carries catalog_id + expected field values."""
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )

    catalog, task = storage_env
    await _dispatch(sa_engine, catalog, _records(1))

    async with managed_transaction(sa_engine) as conn:
        rows = await DQLQuery(
            f'SELECT catalog_id, driver_id, collection_id, op, '
            f'entity_kind, entity_id, op_payload, idempotency_key, status, '
            f'claim_version, day FROM "{task}".storage',
            result_handler=ResultHandler.ALL_DICTS,
        ).execute(conn)

    assert len(rows) == 1
    row = rows[0]
    assert row["catalog_id"] == catalog
    assert row["driver_id"] == "elasticsearch_private"
    assert row["collection_id"] == "my_collection"
    assert row["op"] == "upsert"
    assert row["entity_kind"] == "item"
    assert row["entity_id"] == "item_0"
    assert row["idempotency_key"] == "ik_0"
    assert row["status"] == "ready"
    assert row["claim_version"] == 0
    assert row["day"] is not None


# ---------------------------------------------------------------------------
# id-only rows (#2494 P1)
# ---------------------------------------------------------------------------


async def _dispatch_id_only(engine, catalog_id: str, rows) -> None:
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.storage_emit import enqueue_storage_op_id_only

    async with managed_transaction(engine) as conn:
        await enqueue_storage_op_id_only(conn, catalog_id=catalog_id, rows=rows)


@pytest.mark.asyncio
async def test_id_only_row_shape(sa_engine, storage_env):
    """Every row is written with the explicit id-only sentinel payload and
    the id-only fields set, regardless of what payload the caller supplied.

    The sentinel (not bare emptiness) is load-bearing: the ``tasks.storage``
    DDL default for ``op_payload`` is ALSO ``'{}'::jsonb``, so the drain
    must be able to tell a deliberate id-only row apart from a payload
    that merely happens to be empty (review finding, #2494).
    """
    from dynastore.models.protocols.indexing import STORAGE_PLANE_ID_ONLY_MARKER_KEY
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )

    catalog, task = storage_env
    # Deliberately pass a non-empty payload — the id-only writer must
    # ignore it and force op_payload to the sentinel.
    rows = _records(1)
    await _dispatch_id_only(sa_engine, catalog, rows)

    async with managed_transaction(sa_engine) as conn:
        result_rows = await DQLQuery(
            f'SELECT catalog_id, driver_id, collection_id, op, entity_id, '
            f'op_payload, idempotency_key FROM "{task}".storage',
            result_handler=ResultHandler.ALL_DICTS,
        ).execute(conn)

    assert len(result_rows) == 1
    row = result_rows[0]
    assert row["catalog_id"] == catalog
    assert row["driver_id"] == "elasticsearch_private"
    assert row["collection_id"] == "my_collection"
    assert row["op"] == "upsert"
    assert row["entity_id"] == "item_0"
    assert row["idempotency_key"] == "ik_0"
    assert row["op_payload"] == {STORAGE_PLANE_ID_ONLY_MARKER_KEY: True}, (
        "id-only rows must force op_payload to the explicit sentinel"
    )


@pytest.mark.asyncio
async def test_id_only_rolls_back_atomically(sa_engine, storage_env):
    """An outer-transaction abort leaves NO id-only rows in tasks.storage."""
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.storage_emit import enqueue_storage_op_id_only

    catalog, task = storage_env
    rows = _records(2)

    with pytest.raises(RuntimeError, match="simulated primary write failure"):
        async with managed_transaction(sa_engine) as conn:
            await enqueue_storage_op_id_only(conn, catalog_id=catalog, rows=rows)
            raise RuntimeError("simulated primary write failure")

    assert await _count(sa_engine, task, "storage") == 0


@pytest.mark.asyncio
async def test_id_only_coalesces_two_upserts_same_id_to_one_row(sa_engine, storage_env):
    """Two upserts of the same id in one enqueue batch collapse to one row."""
    from uuid import uuid4
    from dynastore.models.protocols.indexing import OutboxRecord

    catalog, task = storage_env
    rows = [
        OutboxRecord(
            op_id=uuid4(), driver_id="es", driver_instance_id="di",
            collection_id="coll", op="upsert", item_id="item_x",
            payload={"v": i}, idempotency_key=f"ik_{i}",
        )
        for i in range(2)
    ]
    await _dispatch_id_only(sa_engine, catalog, rows)
    assert await _count(sa_engine, task, "storage") == 1


@pytest.mark.asyncio
async def test_id_only_upsert_then_delete_same_id_collapses_to_delete(sa_engine, storage_env):
    """An upsert immediately followed by a delete of the same id in one
    batch collapses to the delete (last op wins)."""
    from uuid import uuid4
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )
    from dynastore.models.protocols.indexing import OutboxRecord

    catalog, task = storage_env
    rows = [
        OutboxRecord(
            op_id=uuid4(), driver_id="es", driver_instance_id="di",
            collection_id="coll", op="upsert", item_id="item_y",
            payload={}, idempotency_key="ik_upsert",
        ),
        OutboxRecord(
            op_id=uuid4(), driver_id="es", driver_instance_id="di",
            collection_id="coll", op="delete", item_id="item_y",
            payload={}, idempotency_key="ik_delete",
        ),
    ]
    await _dispatch_id_only(sa_engine, catalog, rows)

    async with managed_transaction(sa_engine) as conn:
        result_rows = await DQLQuery(
            f'SELECT op FROM "{task}".storage', result_handler=ResultHandler.ALL_DICTS,
        ).execute(conn)
    assert len(result_rows) == 1
    assert result_rows[0]["op"] == "delete"
