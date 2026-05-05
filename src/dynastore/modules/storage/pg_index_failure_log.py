"""``PgIndexFailureLog`` — :class:`IndexFailureLog` Protocol backed by PG.

The per-tenant ``index_failure_log`` table (DDL in
:mod:`dynastore.modules.storage.outbox_ddl`) records two kinds of
events emitted by the outbox drain task:

* ``status='retrying'`` rows are written when an op crosses the
  retry-visible threshold but is still being retried by the drainer —
  tenants surface these via the REST endpoint so they can see a
  build-up before it tips over.
* ``status='failed'`` rows are terminal: the op was either poisoned
  by the failure classifier or exhausted its retry budget.

Connection model mirrors :class:`PgOutboxStore`:

* ``record()`` accepts an explicit ``conn`` so the drain task can join
  the same transaction that flips the outbox row to ``failed``. When
  given that conn, we trust its ``search_path`` (caller is inside a
  TX scoped to the tenant schema) and skip the ``SET LOCAL``.
* ``record()`` without an explicit conn — and ``list_failures()`` —
  fall back to ``single_conn`` (test path) or a pool-acquired
  connection (production read path), pinning ``search_path`` via a
  separate ``execute`` before the parameterised query because
  asyncpg's extended-query protocol rejects multi-statement strings
  when ``$N`` placeholders are present.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, List, Literal, Optional, Sequence
from uuid import UUID

from dynastore.models.protocols.indexing import IndexFailureRecord
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.identifiers import generate_uuidv7


# Hard cap on ``list_failures`` page size. Mirrors the catalog REST
# convention — keep the operator-visible budget bounded so a stray
# limit=10000 can't OOM the API process while a tenant scans a hot
# failure log.
_LIST_LIMIT_CAP: int = 500


class PgIndexFailureLog:
    """Postgres-backed :class:`IndexFailureLog` implementation."""

    def __init__(
        self,
        *,
        pool: Any = None,
        single_conn: Any = None,
    ) -> None:
        if pool is None and single_conn is None:
            raise ValueError(
                "PgIndexFailureLog: provide pool or single_conn",
            )
        self._pool = pool
        self._single = single_conn

    async def _conn(self) -> Any:
        """Return the connection used for non-record queries.

        ``single_conn`` mode wins — used by tests so the caller's
        ``search_path`` is preserved on the same physical session.
        """
        if self._single is not None:
            return self._single
        if self._pool is None:
            raise RuntimeError("PgIndexFailureLog: no connection source")
        return await self._pool.acquire()

    async def _ensure_search_path(self, conn: Any, catalog_id: str) -> None:
        """Pin ``search_path`` to ``catalog_id`` on a pool-acquired conn.

        No-op in ``single_conn`` mode — the test fixture has already
        set ``search_path`` on that session and we deliberately leave
        it alone.

        Issued as a standalone ``execute`` (not concatenated onto the
        next parameterised query) because asyncpg's extended-query
        protocol rejects multi-statement strings when ``$N``
        placeholders are present.
        """
        if self._single is not None:
            return
        validate_sql_identifier(catalog_id)
        await conn.execute(f'SET LOCAL search_path TO "{catalog_id}"')

    async def record(
        self,
        conn: Any = None,
        *,
        catalog_id: str,
        collection_id: str,
        driver_instance_id: str,
        driver_id: str,
        op_id: UUID,
        item_id: Optional[str],
        op: str,
        attempts: int,
        error_class: str,
        error_message: str,
        status: Literal["retrying", "failed"],
        correlation_id: Optional[str] = None,
    ) -> None:
        """Insert one failure row.

        When ``conn`` is provided, runs on the caller's connection
        inside their TX; ``search_path`` must already be set by the
        caller (this is the drain-task path that flips the outbox row
        to ``failed`` atomically with logging).

        When ``conn`` is ``None`` we fall back to ``single_conn`` /
        pool. Pool mode pins ``search_path`` first; single_conn mode
        trusts the caller.
        """
        if conn is not None:
            target = conn
            release_to_pool = False
        else:
            target = await self._conn()
            release_to_pool = self._single is None and self._pool is not None
            await self._ensure_search_path(target, catalog_id)

        try:
            await target.execute(
                """
                INSERT INTO index_failure_log
                    (failure_id, collection_id, driver_id,
                     driver_instance_id, op_id, item_id, op, attempts,
                     error_class, error_message, status, correlation_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                """,
                generate_uuidv7(),
                collection_id,
                driver_id,
                driver_instance_id,
                op_id,
                item_id,
                op,
                attempts,
                error_class,
                error_message,
                status,
                correlation_id,
            )
        finally:
            if release_to_pool and self._pool is not None:
                await self._pool.release(target)

    async def list_failures(
        self,
        *,
        catalog_id: str,
        collection_id: Optional[str] = None,
        driver_id: Optional[str] = None,
        since: Optional[datetime] = None,
        status: Optional[Literal["retrying", "failed"]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Sequence[IndexFailureRecord]:
        """Return the most-recent failures matching the filters.

        Filters compose with AND. ``limit`` is hard-capped at
        :data:`_LIST_LIMIT_CAP`; ``offset`` is offset-based pagination
        — fine for the operator UI's hundred-item windows, not
        intended for full-table cursors.

        Ordering is ``occurred_at DESC`` so the freshest failures
        surface first. ``occurred_at`` ties are broken by PG's row
        order; this is acceptable for the REST surface (failures with
        identical timestamps are rare and the UI treats them as a
        cluster).
        """
        conditions: List[str] = []
        params: List[Any] = []
        if collection_id is not None:
            params.append(collection_id)
            conditions.append(f"collection_id = ${len(params)}")
        if driver_id is not None:
            params.append(driver_id)
            conditions.append(f"driver_id = ${len(params)}")
        if since is not None:
            params.append(since)
            conditions.append(f"occurred_at >= ${len(params)}")
        if status is not None:
            params.append(status)
            conditions.append(f"status = ${len(params)}")
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        params.append(min(limit, _LIST_LIMIT_CAP))
        lim_idx = len(params)
        params.append(offset)
        off_idx = len(params)

        target = await self._conn()
        release_to_pool = self._single is None and self._pool is not None
        try:
            await self._ensure_search_path(target, catalog_id)
            rows = await target.fetch(
                f"""SELECT failure_id, occurred_at, collection_id,
                           driver_id, driver_instance_id, op_id,
                           item_id, op, attempts, error_class,
                           error_message, status, correlation_id
                    FROM index_failure_log {where}
                    ORDER BY occurred_at DESC
                    LIMIT ${lim_idx} OFFSET ${off_idx}""",
                *params,
            )
        finally:
            if release_to_pool and self._pool is not None:
                await self._pool.release(target)

        return [
            IndexFailureRecord(
                failure_id=r["failure_id"],
                occurred_at=r["occurred_at"],
                collection_id=r["collection_id"],
                driver_id=r["driver_id"],
                driver_instance_id=r["driver_instance_id"],
                op_id=r["op_id"],
                item_id=r["item_id"],
                op=r["op"],
                attempts=r["attempts"],
                error_class=r["error_class"],
                error_message=r["error_message"],
                status=r["status"],
                correlation_id=r["correlation_id"],
            )
            for r in rows
        ]
