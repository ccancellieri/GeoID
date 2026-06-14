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

"""End-to-end tests for the storage-plane dual-write dispatch (#1807 PR-3).

``dispatch_storage_dual_write`` writes storage-outbox records to the legacy
per-tenant ``{schema}.storage_outbox`` table and/or the new global
``tasks.storage`` table, gated by ``WorkClassConfig.emit_target_storage``.

Properties proven against live PG:

1. **Default / fail-safe is legacy-only** — the default config, a missing
   ConfigsProtocol, and a ConfigsProtocol whose ``get_config`` raises all
   write ONLY ``storage_outbox`` (byte-for-byte today's behaviour). A config
   lookup must never break the write nor drop the legacy row.
2. **``both`` writes both tables**; **``new`` writes only ``tasks.storage``**.
3. **Co-transactional atomicity** — both writes ride the caller's transaction,
   so an outer-transaction abort leaves NO rows in EITHER table.
4. **Field mapping** — the ``tasks.storage`` row carries the tenant identity in
   the ``catalog_id`` column (no ``schema_name``) and preserves
   op/entity_kind/entity_id/payload.

The new table is created uniquely-named per test and pointed at via
``DYNASTORE_TASK_SCHEMA`` so concurrent test runs don't collide on the real
``tasks`` schema. It is created un-partitioned here on purpose — partition
routing is PR-1's concern; this suite tests the WRITE dispatch.
"""

from __future__ import annotations

import os
from typing import Any, AsyncIterator, Optional, Tuple
from uuid import uuid4

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
    -- Match production's composite PK (workclass_ddl.STORAGE_TABLE_DDL):
    -- a partitioned table requires its partition key (day) in the PK. This
    -- test table is un-partitioned, but the PK is kept faithful so the INSERT
    -- path faces the same uniqueness constraint as production.
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
        pytest.skip(f"Live PG unavailable ({exc!s}); skipping dual-write tests.")
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def dual_write_env(
    sa_engine, monkeypatch  # noqa: ANN001
) -> AsyncIterator[Tuple[str, str]]:
    """Provision a tenant schema (storage_outbox) + a task schema (storage).

    Points ``get_task_schema()`` at the throwaway task schema via
    ``DYNASTORE_TASK_SCHEMA`` so the dual-write writer targets it instead of
    the real ``tasks`` schema. Yields ``(tenant_schema, task_schema)``.
    """
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )
    from dynastore.modules.storage.outbox_ddl import ensure_storage_outbox

    token = generate_id_hex()[:10]
    tenant_schema = f"pr3_tenant_{token}"
    task_schema = f"pr3_tasks_{token}"
    monkeypatch.setenv("DYNASTORE_TASK_SCHEMA", task_schema)

    async with managed_transaction(sa_engine) as conn:
        await DQLQuery(
            f'CREATE SCHEMA IF NOT EXISTS "{tenant_schema}"',
            result_handler=ResultHandler.NONE,
        ).execute(conn)
        await DQLQuery(
            f'CREATE SCHEMA IF NOT EXISTS "{task_schema}"',
            result_handler=ResultHandler.NONE,
        ).execute(conn)
        await DQLQuery(
            _STORAGE_TEST_DDL.format(schema=task_schema),
            result_handler=ResultHandler.NONE,
        ).execute(conn)
    # Legacy per-tenant outbox via the production DDL helper.
    await ensure_storage_outbox(sa_engine, tenant_schema)

    try:
        yield tenant_schema, task_schema
    finally:
        async with managed_transaction(sa_engine) as conn:
            for sch in (tenant_schema, task_schema):
                try:
                    await DQLQuery(
                        f'DROP SCHEMA IF EXISTS "{sch}" CASCADE',
                        result_handler=ResultHandler.NONE,
                    ).execute(conn)
                except Exception:  # noqa: BLE001 — best-effort teardown
                    pass


# --- stub ConfigsProtocol returning a chosen WorkClassConfig --------------


class _StubConfigs:
    def __init__(self, cfg: Any) -> None:
        self._cfg = cfg

    async def get_config(self, config_cls: Any, **_kw: Any) -> Any:
        return self._cfg


class _RaisingConfigs:
    async def get_config(self, config_cls: Any, **_kw: Any) -> Any:
        raise RuntimeError("config backend down")


def _records(n: int):
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


def _new_store():
    from dynastore.modules.storage.pg_outbox import PgOutboxStore

    # Dummy pool sentinel — only the SA-conn (co-transactional) path is used,
    # which never acquires from the pool.
    return PgOutboxStore(pool=object(), single_conn=None)


def _config(target: Optional[str] = None):
    from dynastore.modules.tasks.workclass_config import EmitTarget, WorkClassConfig

    if target is None:
        return WorkClassConfig()
    return WorkClassConfig(emit_target_storage=EmitTarget(target))


async def _dispatch(engine, configs, tenant_schema: str, rows) -> None:
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.storage_dual_write import (
        dispatch_storage_dual_write,
    )

    async with managed_transaction(engine) as conn:
        await dispatch_storage_dual_write(
            conn,
            outbox=_new_store(),
            catalog_id=tenant_schema,
            rows=rows,
            configs=configs,
        )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_default_config_writes_only_legacy(sa_engine, dual_write_env):
    tenant, task = dual_write_env
    await _dispatch(sa_engine, _StubConfigs(_config()), tenant, _records(3))
    assert await _count(sa_engine, tenant, "storage_outbox") == 3
    assert await _count(sa_engine, task, "storage") == 0


@pytest.mark.asyncio
async def test_none_configs_writes_only_legacy(sa_engine, dual_write_env):
    tenant, task = dual_write_env
    await _dispatch(sa_engine, None, tenant, _records(2))
    assert await _count(sa_engine, tenant, "storage_outbox") == 2
    assert await _count(sa_engine, task, "storage") == 0


@pytest.mark.asyncio
async def test_raising_configs_writes_only_legacy(sa_engine, dual_write_env):
    # Fail-safe: a config backend error must not break the write nor drop
    # the legacy outbox row.
    tenant, task = dual_write_env
    await _dispatch(sa_engine, _RaisingConfigs(), tenant, _records(2))
    assert await _count(sa_engine, tenant, "storage_outbox") == 2
    assert await _count(sa_engine, task, "storage") == 0


@pytest.mark.asyncio
async def test_both_writes_both_tables(sa_engine, dual_write_env):
    tenant, task = dual_write_env
    await _dispatch(sa_engine, _StubConfigs(_config("both")), tenant, _records(4))
    assert await _count(sa_engine, tenant, "storage_outbox") == 4
    assert await _count(sa_engine, task, "storage") == 4


@pytest.mark.asyncio
async def test_new_writes_only_storage(sa_engine, dual_write_env):
    tenant, task = dual_write_env
    await _dispatch(sa_engine, _StubConfigs(_config("new")), tenant, _records(3))
    assert await _count(sa_engine, tenant, "storage_outbox") == 0
    assert await _count(sa_engine, task, "storage") == 3


@pytest.mark.asyncio
async def test_both_rolls_back_atomically(sa_engine, dual_write_env):
    """An outer-transaction abort after dispatch leaves NO rows in either table."""
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.storage_dual_write import (
        dispatch_storage_dual_write,
    )

    tenant, task = dual_write_env
    rows = _records(2)

    with pytest.raises(RuntimeError, match="simulated primary write failure"):
        async with managed_transaction(sa_engine) as conn:
            await dispatch_storage_dual_write(
                conn,
                outbox=_new_store(),
                catalog_id=tenant,
                rows=rows,
                configs=_StubConfigs(_config("both")),
            )
            raise RuntimeError("simulated primary write failure")

    assert await _count(sa_engine, tenant, "storage_outbox") == 0
    assert await _count(sa_engine, task, "storage") == 0


@pytest.mark.asyncio
async def test_storage_row_field_mapping(sa_engine, dual_write_env):
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )

    tenant, task = dual_write_env
    await _dispatch(sa_engine, _StubConfigs(_config("new")), tenant, _records(1))

    async with managed_transaction(sa_engine) as conn:
        rows = await DQLQuery(
            f'SELECT catalog_id, driver_id, collection_id, op, '
            f'entity_kind, entity_id, op_payload, idempotency_key, status, '
            f'claim_version, day FROM "{task}".storage',
            result_handler=ResultHandler.ALL_DICTS,
        ).execute(conn)

    assert len(rows) == 1
    row = rows[0]
    # Tenant identity carried by the catalog_id column, not the table location.
    assert row["catalog_id"] == tenant
    assert row["driver_id"] == "elasticsearch_private"
    assert row["collection_id"] == "my_collection"
    assert row["op"] == "upsert"
    # Items tier today; entity_kind branches for other tiers in #1807 P1.3.
    assert row["entity_kind"] == "item"
    assert row["entity_id"] == "item_0"
    assert row["idempotency_key"] == "ik_0"
    # DDL defaults applied.
    assert row["status"] == "ready"
    assert row["claim_version"] == 0
    assert row["day"] is not None
