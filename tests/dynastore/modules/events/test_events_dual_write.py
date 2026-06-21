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

"""Tests for the event emit path (#1807 P4 — tasks.events only).

These tests exercise ``emit_event_row`` against real PostgreSQL to prove
direct writes to ``tasks.events``, scope normalisation, and rollback atomicity.

The test schema is created fresh per-test and torn down afterwards.  The
``task_events`` table mirrors the ``tasks.events`` structure that
``emit_event_row`` writes to, created inside a unique test-scoped schema so
concurrent test runs on the same DB do not interfere.

The fixture skips automatically when PostgreSQL is unreachable.

Scenarios covered
-----------------
1. Direct write to tasks.events: a row appears in the task_events table and
   a valid event_id string is returned.
2. Scope normalisation: a mixed-case ``scope`` value is lowercased in the
   stored row.
3. Rollback atomicity: an exception inside the outer transaction leaves zero
   rows in the table.
4. Drain trigger: _enqueue_event_drain_trigger is called co-transactionally
   (verified via mock to avoid requiring the tasks schema).
"""

from __future__ import annotations

import os
from typing import AsyncIterator, Optional

import pytest
import pytest_asyncio

from dynastore.tools.identifiers import generate_id_hex


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sa_db_url() -> str:
    url = os.getenv(
        "DATABASE_URL",
        "postgresql://testuser:testpassword@localhost:54320/gis_dev",
    )
    if not url.startswith("postgresql+asyncpg://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def sa_engine():
    """SQLAlchemy AsyncEngine with NullPool; skips if DB is unreachable."""
    sqlalchemy_async = pytest.importorskip(
        "sqlalchemy.ext.asyncio",
        reason="sqlalchemy[asyncio] not installed",
    )
    pytest.importorskip("asyncpg", reason="asyncpg not installed")

    from sqlalchemy.pool import NullPool

    engine = sqlalchemy_async.create_async_engine(_sa_db_url(), poolclass=NullPool)
    try:
        async with engine.connect() as probe:
            await probe.close()
    except Exception as exc:  # noqa: BLE001
        await engine.dispose()
        pytest.skip(f"Live PG unavailable ({exc!s}); skipping emit tests.")

    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def emit_schema(sa_engine) -> AsyncIterator[str]:  # noqa: ANN001
    """Per-test schema with a tasks.events-like table.

    The table structure matches what ``emit_event_row`` writes to, but is
    isolated to a throwaway schema so nothing touches the real tasks schema.
    """
    from dynastore.modules.db_config.query_executor import (
        DQLQuery,
        ResultHandler,
        managed_transaction,
    )

    schema = f"emit_test_{generate_id_hex()[:10]}"

    async with managed_transaction(sa_engine) as conn:
        await DQLQuery(
            f'CREATE SCHEMA IF NOT EXISTS "{schema}"',
            result_handler=ResultHandler.NONE,
        ).execute(conn)

        # tasks.events-like table: matches tasks.events DDL exactly
        # (non-partitioned for simplicity — the INSERT path is identical).
        await DQLQuery(
            f"""
            CREATE TABLE "{schema}".events (
                event_id        UUID            NOT NULL,
                day             DATE            NOT NULL DEFAULT CURRENT_DATE,
                shard           SMALLINT        NOT NULL,
                schema_name     TEXT,
                scope           TEXT            NOT NULL DEFAULT 'platform'
                                    CHECK (scope = lower(scope)),
                event_type      TEXT            NOT NULL,
                status          TEXT            NOT NULL DEFAULT 'PENDING',
                payload         JSONB           NOT NULL DEFAULT '{{}}'::jsonb,
                claim_version   INTEGER         NOT NULL DEFAULT 0,
                owner_id        TEXT,
                locked_until    TIMESTAMPTZ,
                retry_count     INTEGER         NOT NULL DEFAULT 0,
                max_retries     INTEGER,
                error_message   TEXT,
                created_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
                processed_at    TIMESTAMPTZ,
                PRIMARY KEY (day, event_id)
            )
            """,
            result_handler=ResultHandler.NONE,
        ).execute(conn)

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
# Row-count and field helpers (read committed, fresh connection)
# ---------------------------------------------------------------------------


async def _count(engine, schema: str, table: str) -> int:
    from dynastore.modules.db_config.query_executor import (
        DQLQuery,
        ResultHandler,
        managed_transaction,
    )

    async with managed_transaction(engine) as conn:
        n = await DQLQuery(
            f'SELECT count(*) FROM "{schema}".{table}',
            result_handler=ResultHandler.SCALAR,
        ).execute(conn)
        return n or 0


async def _fetch_scope(engine, schema: str, table: str) -> Optional[str]:
    """Return the ``scope`` of the first row in the table, or None."""
    from dynastore.modules.db_config.query_executor import (
        DQLQuery,
        ResultHandler,
        managed_transaction,
    )

    async with managed_transaction(engine) as conn:
        row = await DQLQuery(
            f'SELECT scope FROM "{schema}".{table} LIMIT 1',
            result_handler=ResultHandler.ONE_DICT,
        ).execute(conn)
        return row["scope"] if row else None


# ---------------------------------------------------------------------------
# Patcher: redirect _events_insert_query to the test schema events table
# ---------------------------------------------------------------------------


def _patch_events_insert_query(schema: str):
    """Context manager: redirect ``_events_insert_query`` to the test schema."""
    import contextlib
    import unittest.mock as mock

    from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler

    patched = DQLQuery(
        f"""
        INSERT INTO "{schema}".events
            (event_id, shard, schema_name, scope, event_type, payload)
        VALUES
            (CAST(:event_id AS uuid), :shard, :schema_name, :scope, :event_type,
             CAST(:payload AS jsonb))
        """,
        result_handler=ResultHandler.NONE,
    )

    @contextlib.asynccontextmanager
    async def _ctx():
        with mock.patch(
            "dynastore.modules.tasks.events.events_emit._EVENTS_INSERT_QUERY_CACHE",
            {},
        ):
            with mock.patch(
                "dynastore.modules.tasks.events.events_emit._events_insert_query",
                return_value=patched,
            ):
                yield

    return _ctx()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_direct_write_to_task_events(sa_engine, emit_schema):
    """emit_event_row inserts one row into tasks.events and returns a valid event_id."""
    import unittest.mock as mock

    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.tasks.events.events_emit import emit_event_row

    with mock.patch(
        "dynastore.modules.tasks.events.events_emit._enqueue_event_drain_trigger",
        new=mock.AsyncMock(),
    ):
        async with _patch_events_insert_query(emit_schema):
            with mock.patch(
                "dynastore.modules.tasks.tasks_module.get_task_schema",
                return_value=emit_schema,
            ):
                async with managed_transaction(sa_engine) as conn:
                    event_id = await emit_event_row(
                        conn,
                        event_type="catalog_creation",
                        scope="PLATFORM",
                        schema_name=None,
                        catalog_id="mycat",
                        collection_id=None,
                        identity_id=None,
                        payload_str='{"catalog_id": "mycat"}',
                        shard=3,
                    )

    assert event_id is not None and len(event_id) > 0
    assert await _count(sa_engine, emit_schema, "events") == 1


@pytest.mark.asyncio
async def test_scope_lowercased_in_task_events(sa_engine, emit_schema):
    """Mixed-case scope is lowercased before INSERT to satisfy tasks.events CHECK."""
    import unittest.mock as mock

    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.tasks.events.events_emit import emit_event_row

    with mock.patch(
        "dynastore.modules.tasks.events.events_emit._enqueue_event_drain_trigger",
        new=mock.AsyncMock(),
    ):
        async with _patch_events_insert_query(emit_schema):
            with mock.patch(
                "dynastore.modules.tasks.tasks_module.get_task_schema",
                return_value=emit_schema,
            ):
                async with managed_transaction(sa_engine) as conn:
                    await emit_event_row(
                        conn,
                        event_type="scope_test",
                        scope="PLATFORM",
                        schema_name=None,
                        catalog_id=None,
                        collection_id=None,
                        identity_id=None,
                        payload_str="{}",
                        shard=0,
                    )

    scope = await _fetch_scope(sa_engine, emit_schema, "events")
    assert scope == "platform", f"expected 'platform', got {scope!r}"


@pytest.mark.asyncio
async def test_rollback_leaves_no_rows(sa_engine, emit_schema):
    """When the outer transaction is rolled back, tasks.events stays empty."""
    import unittest.mock as mock

    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.tasks.events.events_emit import emit_event_row

    with mock.patch(
        "dynastore.modules.tasks.events.events_emit._enqueue_event_drain_trigger",
        new=mock.AsyncMock(),
    ):
        async with _patch_events_insert_query(emit_schema):
            with mock.patch(
                "dynastore.modules.tasks.tasks_module.get_task_schema",
                return_value=emit_schema,
            ):
                with pytest.raises(RuntimeError, match="simulated failure"):
                    async with managed_transaction(sa_engine) as conn:
                        await emit_event_row(
                            conn,
                            event_type="rollback_test",
                            scope="platform",
                            schema_name=None,
                            catalog_id=None,
                            collection_id=None,
                            identity_id=None,
                            payload_str="{}",
                            shard=1,
                        )
                        # Verify the row is visible mid-transaction before aborting
                        from dynastore.modules.db_config.query_executor import (
                            DQLQuery,
                            ResultHandler,
                        )

                        mid = (
                            await DQLQuery(
                                f'SELECT count(*) FROM "{emit_schema}".events',
                                result_handler=ResultHandler.SCALAR,
                            ).execute(conn)
                            or 0
                        )
                        assert mid == 1, f"expected 1 in-tx events row, got {mid}"
                        raise RuntimeError("simulated failure")

    assert await _count(sa_engine, emit_schema, "events") == 0


@pytest.mark.asyncio
async def test_drain_trigger_enqueued_co_transactionally(sa_engine, emit_schema):
    """_enqueue_event_drain_trigger is called exactly once per emit_event_row call."""
    import unittest.mock as mock

    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.tasks.events.events_emit import emit_event_row

    mock_enqueue = mock.AsyncMock()

    with mock.patch(
        "dynastore.modules.tasks.events.events_emit._enqueue_event_drain_trigger",
        new=mock_enqueue,
    ):
        async with _patch_events_insert_query(emit_schema):
            with mock.patch(
                "dynastore.modules.tasks.tasks_module.get_task_schema",
                return_value=emit_schema,
            ):
                async with managed_transaction(sa_engine) as conn:
                    await emit_event_row(
                        conn,
                        event_type="trigger_test",
                        scope="platform",
                        schema_name="s_abc",
                        catalog_id="cat1",
                        collection_id=None,
                        identity_id=None,
                        payload_str='{"x": 42}',
                        shard=7,
                    )

    mock_enqueue.assert_awaited_once()


@pytest.mark.asyncio
async def test_search_events_filters_collection_id_via_payload(sa_engine, emit_schema):
    """search_events scopes to a collection via the payload JSONB path.

    tasks.events has no dedicated collection_id column (#1807 P4); the
    collection lifecycle publishers store collection_id in the event payload,
    so the collection-scoped events REST endpoint must filter on
    ``payload->>'collection_id'``. This pins that contract end-to-end.
    """
    import unittest.mock as mock

    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.tasks.events.events_emit import emit_event_row
    from dynastore.modules.tasks.event_driver import TaskEventDriver

    async def _seed(collection_id: str) -> None:
        async with managed_transaction(sa_engine) as conn:
            await emit_event_row(
                conn,
                event_type="collection_creation",
                scope="PLATFORM",
                schema_name="cat_search",
                catalog_id="cat_search",
                collection_id=collection_id,
                identity_id=None,
                payload_str=(
                    '{"catalog_id": "cat_search", "collection_id": "'
                    + collection_id
                    + '"}'
                ),
                shard=1,
            )

    svc = TaskEventDriver.__new__(TaskEventDriver)

    with mock.patch(
        "dynastore.modules.tasks.events.events_emit._enqueue_event_drain_trigger",
        new=mock.AsyncMock(),
    ):
        async with _patch_events_insert_query(emit_schema):
            with mock.patch(
                "dynastore.modules.tasks.tasks_module.get_task_schema",
                return_value=emit_schema,
            ):
                await _seed("colA")
                await _seed("colB")

                only_a = await svc.search_events(
                    engine=sa_engine, catalog_id="cat_search", collection_id="colA",
                )
                only_b = await svc.search_events(
                    engine=sa_engine, catalog_id="cat_search", collection_id="colB",
                )
                both = await svc.search_events(
                    engine=sa_engine, catalog_id="cat_search",
                )

    assert len(only_a) == 1, f"collection filter must scope to colA; got {only_a!r}"
    assert "colA" in str(only_a[0]["payload"])
    assert len(only_b) == 1, f"collection filter must scope to colB; got {only_b!r}"
    assert "colB" in str(only_b[0]["payload"])
    # No collection filter → both catalog events returned.
    assert len(both) == 2


@pytest.mark.asyncio
async def test_search_events_includes_platform_lifecycle_events(sa_engine, emit_schema):
    """Catalog-scoped search_events surfaces platform lifecycle rows (#2256).

    Platform lifecycle events (e.g. catalog_creation) are stored with
    schema_name=NULL and catalog_id carried only in the flat payload.
    The catalog-scoped filter must include these rows, not just
    schema_name-keyed tenant rows.

    Checks:
    - A catalog_creation row (schema_name=NULL, payload.catalog_id="catX") is
      returned by search_events(catalog_id="catX").
    - A row for a *different* catalog ("catY") is NOT returned (no over-match).
    """
    import unittest.mock as mock

    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.tasks.events.events_emit import emit_event_row
    from dynastore.modules.tasks.event_driver import TaskEventDriver

    async def _seed_platform(catalog_id: str) -> None:
        """Seed a platform lifecycle row: schema_name=NULL, catalog_id in payload."""
        async with managed_transaction(sa_engine) as conn:
            await emit_event_row(
                conn,
                event_type="catalog_creation",
                scope="PLATFORM",
                schema_name=None,
                catalog_id=catalog_id,
                collection_id=None,
                identity_id=None,
                payload_str=f'{{"catalog_id": "{catalog_id}"}}',
                shard=0,
            )

    svc = TaskEventDriver.__new__(TaskEventDriver)

    with mock.patch(
        "dynastore.modules.tasks.events.events_emit._enqueue_event_drain_trigger",
        new=mock.AsyncMock(),
    ):
        async with _patch_events_insert_query(emit_schema):
            with mock.patch(
                "dynastore.modules.tasks.tasks_module.get_task_schema",
                return_value=emit_schema,
            ):
                await _seed_platform("catX")
                await _seed_platform("catY")

                catX_rows = await svc.search_events(
                    engine=sa_engine, catalog_id="catX",
                )
                catY_rows = await svc.search_events(
                    engine=sa_engine, catalog_id="catY",
                )

    # catX platform lifecycle event must be visible in the catX-scoped view.
    assert len(catX_rows) == 1, (
        f"expected 1 platform event for catX, got {catX_rows!r}"
    )
    assert catX_rows[0]["payload"].get("catalog_id") == "catX"

    # catY's event must NOT bleed into the catX-scoped view.
    assert len(catY_rows) == 1, (
        f"expected 1 platform event for catY, got {catY_rows!r}"
    )
    assert catY_rows[0]["payload"].get("catalog_id") == "catY"
