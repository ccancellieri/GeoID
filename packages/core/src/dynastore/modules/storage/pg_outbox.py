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

"""``PgOutboxStore`` — :class:`OutboxStore` Protocol backed by PG.

Caller responsibility: connections passed to :meth:`enqueue_bulk` must
already have ``search_path`` set to the catalog's physical schema —
that method runs inside the caller's transaction context and we
deliberately do not mutate it. Methods that acquire their own
connection (:meth:`claim_batch`, ``mark_*``, :meth:`listen`) issue a
``SET LOCAL search_path TO "<schema>"`` separately via
:func:`dynastore.tools.db.validate_sql_identifier` before the
parameterised query so a fresh pool connection lands on the correct
per-tenant schema. asyncpg's extended-query protocol is single-
statement-only, which is why this is split into two ``execute`` calls
rather than concatenated into a single string.

* :meth:`enqueue_bulk` has two paths, selected by whether a caller
  connection is provided:

  - **Caller-conn path** (``conn is not None``): used by the atomic
    co-transactional write seam (``ItemService.upsert_bulk`` /
    ``_enqueue_index_deletes``). The caller holds a SQLAlchemy
    ``AsyncConnection`` from ``managed_transaction(engine)``; this path
    issues a schema-qualified ``INSERT`` per row via
    :class:`~dynastore.modules.db_config.query_executor.DQLQuery` so
    the enqueue runs on the caller's open transaction and rolls back
    with the primary write on failure. The table name is schema-qualified
    as ``"<catalog_id>".storage_outbox`` after validating ``catalog_id``
    with :func:`~dynastore.tools.db.validate_sql_identifier`.

  - **Own-conn fallback path** (``conn is None``): used by the
    dispatcher's missing-indexer path. The store acquires its own raw
    ``asyncpg`` connection (``single_conn`` for tests, otherwise a
    pool-acquired connection) and uses ``asyncpg.copy_records_to_table``
    for high-throughput ingest (100k/min target). ``search_path`` is
    pinned via :meth:`_ensure_search_path` before the COPY.

* :meth:`claim_batch` uses ``FOR UPDATE SKIP LOCKED`` so multiple
  drainers can claim disjoint subsets in parallel.
* :meth:`listen` yields :class:`Notification` per ``pg_notify`` fired
  by the AFTER INSERT trigger so drainers wake on commit instead of
  polling.
"""
from __future__ import annotations

import json
from typing import Any, AsyncIterator, List, Sequence
from uuid import UUID

from dynastore.models.protocols.indexing import (
    Notification,
    OutboxRecord,
    OutboxRow,
)
from dynastore.tools.db import validate_sql_identifier


# Per-attempt retry backoff in seconds. Index by ``attempts`` (0-based);
# the last entry is reused for any attempt at or beyond its position so
# the backoff caps at ~30min instead of growing unbounded.
_BACKOFF_SECONDS: List[int] = [1, 5, 30, 5 * 60, 30 * 60]


class PgOutboxStore:
    """Postgres-backed :class:`OutboxStore` implementation."""

    def __init__(
        self,
        *,
        pool: Any = None,
        single_conn: Any = None,
    ) -> None:
        if pool is None and single_conn is None:
            raise ValueError("PgOutboxStore: provide pool or single_conn")
        self._pool = pool
        self._single = single_conn

    async def _conn(self) -> Any:
        """Return the connection used for non-enqueue queries.

        ``single_conn`` mode wins — used by tests so the caller's
        ``search_path`` is preserved on the same physical session.
        """
        if self._single is not None:
            return self._single
        if self._pool is None:
            raise RuntimeError("PgOutboxStore: no connection source")
        return await self._pool.acquire()

    async def _ensure_search_path(self, conn: Any, catalog_id: str) -> None:
        """Pin ``search_path`` to ``catalog_id`` on a pool-acquired conn.

        No-op in ``single_conn`` mode — the test fixture has already set
        ``search_path`` on that session and we deliberately leave it alone.

        Issued as a standalone ``execute`` (not concatenated onto the
        next parameterised query) because asyncpg's extended-query
        protocol rejects multi-statement strings when ``$N`` placeholders
        are present.
        """
        if self._single is not None:
            return
        validate_sql_identifier(catalog_id)
        await conn.execute(f'SET LOCAL search_path TO "{catalog_id}"')

    async def enqueue_bulk(
        self,
        conn: Any = None,
        *,
        catalog_id: str,
        rows: Sequence[OutboxRecord],
    ) -> None:
        """Bulk-insert outbox rows, selecting path based on caller context.

        When ``conn`` is provided (the atomic co-transactional path from
        ``ItemService.upsert_bulk`` / ``_enqueue_index_deletes``), the
        caller holds a SQLAlchemy ``AsyncConnection`` from
        ``managed_transaction(engine)``. This path issues a
        schema-qualified parameterised ``INSERT`` per row via
        :class:`~dynastore.modules.db_config.query_executor.DQLQuery`,
        executing on the caller's open transaction. Any exception
        propagates so the caller's ``managed_transaction`` rolls back the
        primary write atomically with the failed enqueue.

        When ``conn`` is ``None`` (the dispatcher's missing-indexer path
        — see ``IndexDispatcher._enqueue_outbox_record``) the store falls
        back to its own connection source: ``single_conn`` for tests,
        otherwise a pool-acquired conn. That path uses
        ``asyncpg.copy_records_to_table`` for high-throughput ingest. In
        pool mode ``search_path`` is pinned via
        :meth:`_ensure_search_path`; in ``single_conn`` mode the caller
        has already set it.
        """
        if not rows:
            return
        if conn is not None:
            # Caller-conn (co-transactional) path: conn is a SQLAlchemy
            # AsyncConnection from managed_transaction(engine). Raw asyncpg
            # methods (copy_records_to_table) are not available on SA
            # connections. Use DQLQuery so the INSERT runs on the caller's
            # transaction and rolls back atomically with the primary write on
            # any failure. Schema-qualify the table with the validated
            # catalog_id to ensure correct tenant namespace regardless of the
            # caller's search_path setting.
            #
            # DQLQuery.execute() takes named bind parameters as kwargs; it has
            # no executemany interface. Iterating per row is correct here:
            # these batches are bounded by request size (the call site coalesces
            # per-item duplicates first), never the 100k/min COPY path.
            validate_sql_identifier(catalog_id)
            _INSERT_SQL = (
                f'INSERT INTO "{catalog_id}".storage_outbox ('
                "    op_id, driver_id, driver_instance_id, collection_id,"
                "    op, item_id, payload, idempotency_key"
                ") VALUES ("
                "    :op_id, :driver_id, :driver_instance_id, :collection_id,"
                "    :op, :item_id, CAST(:payload AS jsonb), :idempotency_key"
                ")"
            )
            from dynastore.modules.db_config.query_executor import (
                DQLQuery, ResultHandler,
            )
            _query = DQLQuery(_INSERT_SQL, result_handler=ResultHandler.NONE)
            for r in rows:
                await _query.execute(
                    conn,
                    op_id=str(r.op_id),
                    driver_id=r.driver_id,
                    driver_instance_id=r.driver_instance_id,
                    collection_id=r.collection_id,
                    op=r.op,
                    item_id=r.item_id,
                    payload=json.dumps(r.payload),
                    idempotency_key=r.idempotency_key,
                )
            return
        records = [
            (
                r.op_id,
                r.driver_id,
                r.driver_instance_id,
                r.collection_id,
                r.op,
                r.item_id,
                json.dumps(r.payload),
                r.idempotency_key,
            )
            for r in rows
        ]
        # Fallback: acquire from the store's own connection source so the
        # dispatcher seam doesn't have to plumb a conn through.
        own_conn = await self._conn()
        # Capture the pool reference up-front so the ``finally`` branch
        # narrows cleanly (pyright can't track the ``and`` guard across
        # the await above).
        pool_ref = self._pool if self._single is None else None
        try:
            await self._ensure_search_path(own_conn, catalog_id)
            await own_conn.copy_records_to_table(
                "storage_outbox",
                records=records,
                columns=[
                    "op_id",
                    "driver_id",
                    "driver_instance_id",
                    "collection_id",
                    "op",
                    "item_id",
                    "payload",
                    "idempotency_key",
                ],
            )
        finally:
            if pool_ref is not None:
                await pool_ref.release(own_conn)

    async def claim_batch(
        self,
        *,
        driver_id: str,
        catalog_id: str,
        batch_size: int,
        claimed_by: str,
    ) -> List[OutboxRow]:
        """Atomically claim up to ``batch_size`` ready rows.

        ``FOR UPDATE SKIP LOCKED`` makes concurrent claimers see disjoint
        subsets; the row lock is released at TX commit so the caller is
        responsible for opening a transaction around the claim/process
        cycle.
        """
        conn = await self._conn()
        await self._ensure_search_path(conn, catalog_id)
        rows = await conn.fetch(
            """
            WITH claimed AS (
                SELECT op_id FROM storage_outbox
                WHERE driver_id = $1 AND status = 'ready' AND ready_at <= now()
                ORDER BY ready_at, op_id
                LIMIT $2 FOR UPDATE SKIP LOCKED
            )
            UPDATE storage_outbox
            SET status='in_flight', claimed_at=now(), claimed_by=$3
            FROM claimed
            WHERE storage_outbox.op_id = claimed.op_id
            RETURNING storage_outbox.op_id, storage_outbox.driver_id,
                      storage_outbox.driver_instance_id,
                      storage_outbox.collection_id, storage_outbox.op,
                      storage_outbox.item_id, storage_outbox.payload,
                      storage_outbox.idempotency_key,
                      storage_outbox.attempts
            """,
            driver_id,
            batch_size,
            claimed_by,
        )
        return [
            OutboxRow(
                op_id=r["op_id"],
                driver_id=r["driver_id"],
                driver_instance_id=r["driver_instance_id"],
                catalog_id=catalog_id,
                collection_id=r["collection_id"],
                op=r["op"],
                item_id=r["item_id"],
                payload=(
                    json.loads(r["payload"])
                    if isinstance(r["payload"], str)
                    else r["payload"]
                ),
                idempotency_key=r["idempotency_key"],
                attempts=r["attempts"],
            )
            for r in rows
        ]

    async def mark_done(
        self, *, catalog_id: str, op_ids: Sequence[UUID],
    ) -> None:
        """Mark the listed ops ``done`` and stamp ``finished_at``."""
        if not op_ids:
            return
        conn = await self._conn()
        await self._ensure_search_path(conn, catalog_id)
        await conn.execute(
            "UPDATE storage_outbox SET status='done', finished_at=now() "
            "WHERE op_id = ANY($1::uuid[])",
            list(op_ids),
        )

    async def mark_retry(
        self,
        *,
        catalog_id: str,
        op_ids: Sequence[UUID],
        error: str,
        attempts_seen: int,
    ) -> None:
        """Bump ``attempts``, schedule retry per backoff curve.

        ``attempts_seen`` is informational — the row's own ``attempts``
        column drives the backoff lookup so concurrent retries can't
        de-sync the curve.
        """
        if not op_ids:
            return
        conn = await self._conn()
        await self._ensure_search_path(conn, catalog_id)
        await conn.execute(
            """
            UPDATE storage_outbox
            SET status='ready', attempts=attempts+1, last_error=$2,
                ready_at = now() + (
                    CASE WHEN attempts >= array_length($3::int[], 1) - 1
                         THEN ($3::int[])[array_length($3::int[], 1)]
                         ELSE ($3::int[])[attempts + 1]
                    END
                ) * INTERVAL '1 second'
            WHERE op_id = ANY($1::uuid[])
            """,
            list(op_ids),
            error,
            _BACKOFF_SECONDS,
        )

    async def mark_failed(
        self, *, catalog_id: str, op_ids: Sequence[UUID], error: str,
    ) -> None:
        """Mark the listed ops ``failed`` (terminal) with ``last_error``."""
        if not op_ids:
            return
        conn = await self._conn()
        await self._ensure_search_path(conn, catalog_id)
        await conn.execute(
            "UPDATE storage_outbox SET status='failed', last_error=$2, "
            "finished_at=now() WHERE op_id = ANY($1::uuid[])",
            list(op_ids),
            error,
        )

    def listen(
        self, *, driver_id: str, catalog_id: str,
    ) -> AsyncIterator[Notification]:
        """Yield a :class:`Notification` per ``pg_notify`` on the
        ``outbox_<driver_id>_<catalog_id>`` channel.

        The connection used for ``LISTEN`` must be dedicated for the
        lifetime of the iterator. The caller controls iteration
        lifecycle — break out of the ``async for`` to release the
        listener and (in pool mode) drop the connection back to the
        pool.

        Outer ``def`` returns an inner async generator directly so the
        signature matches :class:`OutboxStore.listen` (the streaming-
        Protocol convention is plain ``def`` returning ``AsyncIterator``,
        not ``async def`` with ``yield``).
        """

        async def _gen() -> AsyncIterator[Notification]:
            import asyncio

            conn = await self._conn()
            validate_sql_identifier(catalog_id)
            validate_sql_identifier(driver_id)
            channel = f"outbox_{driver_id}_{catalog_id}"
            queue: asyncio.Queue[Notification] = asyncio.Queue()

            async def _on_notify(
                _c: Any, _pid: int, _ch: str, payload: str,
            ) -> None:
                try:
                    doc = json.loads(payload)
                    await queue.put(
                        Notification(
                            driver_id=doc["driver_id"],
                            catalog_id=doc["catalog_id"],
                            op_id=UUID(doc["op_id"]),
                        )
                    )
                except Exception:  # noqa: BLE001 — malformed notify, drop
                    pass

            await conn.add_listener(channel, _on_notify)
            try:
                while True:
                    yield await queue.get()
            finally:
                await conn.remove_listener(channel, _on_notify)

        return _gen()
