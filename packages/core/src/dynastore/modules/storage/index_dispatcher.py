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

"""Index dispatcher — generic fan-out across configured Indexer drivers.

This is the production-readiness backbone for per-item index propagation.
It replaces (in subsequent phases) the event-driven listener pattern used
today by ``ItemsElasticsearchDriver`` and ``ElasticsearchModule`` with a
single, driver-agnostic fan-out site.

Design properties
-----------------

* **Generic** — knows nothing about ES.  Operates on
  :class:`~dynastore.models.protocols.indexer.Indexer` instances looked up
  by ``indexer_id`` from the protocol registry.  ES public, ES private,
  vector DB, audit log: all interchangeable.
* **Tier-uniform** — same code path for catalog / collection / item /
  asset entries; per-tier markers live on the implementations and on the
  routing-config auto-registration, not in the dispatcher.
* **In-process when possible** — when the dispatcher and the indexer run
  in the same pod the call is a direct ``await indexer.index(...)`` —
  no event/task hop.
* **Durable on failure** — the ``OUTBOX`` failure policy persists an
  ``_meta.index_outbox`` row in the *same* PG transaction as the upstream
  write.  PG TX commit guarantees neither the data nor the
  obligation-to-index can be lost.
* **Circuit-broken** — per-indexer-id breaker (Phase 3).  When open,
  sync attempts short-circuit; ``OUTBOX`` policy still enqueues so the
  worker drains when the breaker half-closes.

Phases
------

* Phase 1 (this module) — Protocol surface + dispatcher skeleton.  No
  consumer wiring yet; existing event-driven listeners remain in place.
* Phase 2 — ``_meta.index_outbox`` table + drain worker; replace the
  ES driver's event listeners with a direct dispatcher call from
  ``item_service.upsert``.
* Phase 3 — Circuit breaker (Valkey-backed shared state); migrate the
  private/per-catalog geoid-only indexer onto the same protocol.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Any, Dict, List, Optional, Protocol, Sequence, Union, cast

from dynastore.models.protocols.indexer import (
    BulkResult,
    Indexer,
    IndexContext,
    IndexOp,
)
from dynastore.models.protocols.indexing import IndexableOp, OutboxStore
from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    Operation,
    OperationDriverEntry,
    WriteMode,
)


# Public surface of the dispatcher accepts either the legacy
# ``IndexOp`` (Pydantic, in-process indexers still consume this shape)
# or the new ``IndexableOp`` (frozen dataclass, durable contract for
# outbox + bulk reindex).  Internals branch on the runtime type for the
# few code paths that need the richer ``IndexableOp`` fields
# (``op_id`` / ``driver_instance_id`` / ``idempotency_key``).
DispatchableOp = Union[IndexOp, IndexableOp]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class IndexerFatal(Exception):
    """Raised by the dispatcher when an indexer with ``FailurePolicy.FATAL``
    fails.  Propagates out of the dispatcher; the caller's PG transaction
    rolls back, taking the upstream data write with it.
    """

    def __init__(
        self,
        indexer_id: str,
        op: "DispatchableOp",
        original: BaseException,
    ) -> None:
        # Format depending on which op shape was passed — both fan_out
        # callers (legacy IndexOp and new IndexableOp) use this same
        # exception type.
        if isinstance(op, IndexableOp):
            descriptor = f"{op.op}/{op.collection_id}/{op.item_id}"
        else:
            descriptor = f"{op.op_type}/{op.entity_type}/{op.entity_id}"
        super().__init__(
            f"Fatal indexer failure: '{indexer_id}' on {descriptor}: {original}"
        )
        self.indexer_id = indexer_id
        self.op = op
        self.original = original


# ---------------------------------------------------------------------------
# Outbox writer — durable retry path backed by the existing ``tasks`` table
# ---------------------------------------------------------------------------


class OutboxWriterProtocol(Protocol):
    """Minimal surface the dispatcher needs from a durable retry queue.

    The default implementation (:class:`TaskTableOutboxWriter`) reuses
    the existing ``tasks.tasks`` table; callers wanting a different
    persistence (Kafka, SQS, …) implement the same one-method interface.
    """

    async def enqueue(
        self,
        *,
        indexer_id: str,
        ctx: IndexContext,
        op: IndexOp,
        last_error: Optional[str] = None,
    ) -> None:
        ...


class TaskTableOutboxWriter:
    """Outbox backed by the existing ``tasks.tasks`` table.

    Writes a ``task_type='index_propagation'`` row on the caller's PG
    connection so the row commits / rolls back atomically with the
    upstream data write.  The regular tasks worker pool drains it via
    the existing ``claim_batch`` / ``complete_task`` / ``fail_task``
    pipeline — retry budget, DEAD_LETTER, dedup are all reused.

    The dedup_key is a stable hash of
    ``(indexer_id, entity_type, entity_id, op_type)`` so concurrent
    failures on the same item don't fan-out into multiple retry rows.
    """

    TASK_TYPE = "index_propagation"

    def __init__(
        self,
        *,
        schema_resolver=None,
        task_schema_resolver=None,
    ) -> None:
        """
        Parameters
        ----------
        schema_resolver
            Optional callable ``(catalog: str) -> str`` mapping a catalog
            id to the per-tenant ``schema_name`` recorded on the task
            row.  Defaults to passing the catalog id through unchanged
            (matches the convention used elsewhere — schema_name == catalog
            schema name).
        task_schema_resolver
            Optional callable ``() -> str`` returning the SQL schema that
            owns the ``tasks`` table.  Defaults to importing
            :func:`dynastore.modules.tasks.tasks_module.get_task_schema`
            at call time, so this module stays importable in test
            contexts that don't pull the tasks runtime.
        """
        self._schema_resolver = schema_resolver or (lambda c: c)
        self._task_schema_resolver = task_schema_resolver

    async def enqueue(
        self,
        *,
        indexer_id: str,
        ctx: IndexContext,
        op: IndexOp,
        last_error: Optional[str] = None,
    ) -> None:
        if ctx.pg_conn is None:
            # Without a caller TX we can't honour the atomicity guarantee.
            # Emit a single warning and bail; the dispatcher's degrade
            # path already logged the original failure.
            logger.warning(
                "TaskTableOutboxWriter: ctx.pg_conn is None — skipping "
                "outbox enqueue for indexer '%s' on %s/%s/%s.  Caller "
                "must pass an open PG connection on IndexContext for "
                "the OUTBOX policy to be durable.",
                indexer_id, op.op_type, op.entity_type, op.entity_id,
            )
            return

        task_schema = self._resolve_task_schema()
        schema_name = self._schema_resolver(ctx.catalog)

        inputs = {
            "indexer_id": indexer_id,
            "op_type": op.op_type,
            "entity_type": op.entity_type,
            "entity_id": op.entity_id,
            "catalog": ctx.catalog,
            "collection": ctx.collection,
            "payload": op.payload,
            "correlation_id": ctx.correlation_id,
            "last_error": last_error,
        }
        dedup_key = self._dedup_key(indexer_id, op)

        from dynastore.tools.identifiers import generate_uuidv7
        from dynastore.tools.json import CustomJSONEncoder

        task_id = generate_uuidv7()
        await self._exec_insert(
            ctx.pg_conn,
            sql=f"""
                INSERT INTO {task_schema}.tasks (
                    task_id, schema_name, scope, task_type, type,
                    execution_mode, inputs, collection_id, dedup_key, status
                ) VALUES (
                    :task_id, :schema_name, 'CATALOG', :task_type,
                    'task', 'ASYNCHRONOUS', :inputs::jsonb,
                    :collection_id, :dedup_key, 'PENDING'
                )
                ON CONFLICT DO NOTHING
            """,
            params=dict(
                task_id=task_id,
                schema_name=schema_name,
                task_type=self.TASK_TYPE,
                inputs=json.dumps(inputs, cls=CustomJSONEncoder),
                collection_id=ctx.collection,
                dedup_key=dedup_key,
            ),
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _resolve_task_schema(self) -> str:
        if self._task_schema_resolver is not None:
            return self._task_schema_resolver()
        from dynastore.modules.tasks.tasks_module import get_task_schema
        return get_task_schema()

    @staticmethod
    def _dedup_key(indexer_id: str, op: IndexOp) -> str:
        """Stable hash so repeated failures coalesce into one row."""
        material = "|".join((
            indexer_id, op.op_type, op.entity_type, op.entity_id,
        )).encode("utf-8")
        return hashlib.sha256(material).hexdigest()[:64]

    async def _exec_insert(
        self, conn: Any, sql: str, params: Dict[str, Any],
    ) -> None:
        """Execute the INSERT on the caller's connection.

        Detects the connection flavour at call time:

        * SQLAlchemy ``AsyncConnection`` — uses ``conn.execute(text(...))``.
        * asyncpg connection — uses ``conn.execute(...)`` with ``$N``-style
          parameters and a positional argument list.
        * Anything else — raises so the dispatcher can fall back to
          degraded-WARN mode.
        """
        # Detect connection flavour without swallowing execute() errors.
        # The previous shape wrapped the isinstance check AND the execute in
        # one try/except, so a real SQLAlchemy failure (e.g. poisoned tx
        # state) silently fell through to the asyncpg branch and produced
        # a misleading "AsyncConnection.execute() takes 2-3 positional
        # arguments but N were given" error that masked the actual cause.
        sa_async_conn_cls: Any = None
        sa_text: Any = None
        try:
            from sqlalchemy import text as _sa_text
            from sqlalchemy.ext.asyncio import AsyncConnection as _SAAsyncConnection
            sa_async_conn_cls = _SAAsyncConnection
            sa_text = _sa_text
        except Exception:
            pass

        if sa_async_conn_cls is not None and isinstance(conn, sa_async_conn_cls):
            await conn.execute(sa_text(sql), params)
            return

        # asyncpg path — translate :name placeholders to $N positional args.
        sql_pg, args = _bind_named_to_positional(sql, params)
        asyncpg_conn: Any = conn
        await asyncpg_conn.execute(sql_pg, *args)


# ---------------------------------------------------------------------------
# Default factory — wires the dispatcher against the live routing config
# and the protocol-discovery indexer registry
# ---------------------------------------------------------------------------


_DEFAULT_DISPATCHER: Optional["IndexDispatcher"] = None

# Process-wide lazy asyncpg pool for the bulk OutboxStore. Created on
# first dispatch that needs it; reused across calls. Sized small by
# default — the pool is only used by the dispatcher's missing-indexer
# OUTBOX path, so production traffic is bounded by the dispatch rate
# rather than the request rate.
_OUTBOX_POOL: Any = None
_OUTBOX_POOL_LOCK = asyncio.Lock()


async def _get_outbox_pool() -> Any:
    """Return (lazily creating) the process-wide asyncpg pool used by
    :class:`PgOutboxStore`.

    Acquires the DSN from :class:`DBConfig` (the same env-driven source
    SQLAlchemy uses) and strips the ``+asyncpg`` dialect suffix so the
    libpq URL is acceptable to ``asyncpg.create_pool``.
    """
    global _OUTBOX_POOL
    if _OUTBOX_POOL is not None:
        return _OUTBOX_POOL
    async with _OUTBOX_POOL_LOCK:
        if _OUTBOX_POOL is not None:
            return _OUTBOX_POOL
        import asyncpg  # local import: keep module import-light

        from dynastore.modules.db_config.db_config import DBConfig

        dsn = DBConfig.database_url.replace("postgresql+asyncpg://", "postgresql://")
        _OUTBOX_POOL = await asyncpg.create_pool(
            dsn=dsn, min_size=1, max_size=10,
        )
        return _OUTBOX_POOL


async def _close_outbox_pool() -> None:
    """Test/shutdown hook — closes the lazy outbox pool if it was opened.

    Production processes typically rely on Cloud Run reaping the worker
    rather than explicit teardown, but tests that exercise the
    production-mode wiring need to release pool sockets cleanly.
    """
    global _OUTBOX_POOL
    if _OUTBOX_POOL is None:
        return
    pool = _OUTBOX_POOL
    _OUTBOX_POOL = None
    try:
        await pool.close()
    except Exception:
        # Best-effort shutdown; don't mask the original test failure.
        logger.exception("IndexDispatcher: outbox pool close failed")


class _LazyPoolProxy:
    """Pool-shaped object that resolves the real asyncpg pool on demand.

    ``PgOutboxStore`` only ever calls ``await pool.acquire()`` and
    ``await pool.release(conn)`` against the object passed to its
    ``pool=`` kw, so the proxy implements just those two methods.
    Connections are released back to the underlying pool — same
    contract as a direct asyncpg.Pool reference.
    """

    async def acquire(self) -> Any:
        pool = await _get_outbox_pool()
        return await pool.acquire()

    async def release(self, conn: Any) -> None:
        pool = await _get_outbox_pool()
        await pool.release(conn)


class _DualOutbox:
    """Bridge that satisfies both the bulk :class:`OutboxStore` Protocol
    (``enqueue_bulk`` / ``claim_batch`` / ``mark_*`` / ``listen``) and
    the legacy :class:`OutboxWriterProtocol` singular ``enqueue``
    surface.

    The dispatcher's ``_handle_missing`` path (Phase 6, ``IndexableOp``
    callers) goes through ``enqueue_bulk`` → :class:`PgOutboxStore`
    (the per-tenant ``storage_outbox`` table). The legacy
    ``_handle_failure`` path (resolved-but-failing indexer with the
    older :class:`IndexOp` shape) goes through ``enqueue`` →
    :class:`TaskTableOutboxWriter` (the existing ``tasks.tasks`` row
    with ``task_type='index_propagation'``).

    Keeping both lets the dispatcher honour OUTBOX policy for both call
    sites while the codebase finishes migrating off the legacy IndexOp
    shape.
    """

    def __init__(
        self, *, bulk: OutboxStore, legacy: "OutboxWriterProtocol",
    ) -> None:
        self._bulk = bulk
        self._legacy = legacy

    # OutboxStore surface — delegate to the bulk implementation.
    async def enqueue_bulk(
        self, conn: Any = None, *, catalog_id: str, rows: Sequence[Any],
    ) -> None:
        await self._bulk.enqueue_bulk(conn, catalog_id=catalog_id, rows=rows)

    async def claim_batch(
        self, *, driver_id: str, catalog_id: str,
        batch_size: int, claimed_by: str,
    ) -> List[Any]:
        return await self._bulk.claim_batch(
            driver_id=driver_id, catalog_id=catalog_id,
            batch_size=batch_size, claimed_by=claimed_by,
        )

    async def mark_done(
        self, *, catalog_id: str, op_ids: Sequence[Any],
    ) -> None:
        await self._bulk.mark_done(catalog_id=catalog_id, op_ids=op_ids)

    async def mark_retry(
        self, *, catalog_id: str, op_ids: Sequence[Any],
        error: str, attempts_seen: int,
    ) -> None:
        await self._bulk.mark_retry(
            catalog_id=catalog_id, op_ids=op_ids,
            error=error, attempts_seen=attempts_seen,
        )

    async def mark_failed(
        self, *, catalog_id: str, op_ids: Sequence[Any], error: str,
    ) -> None:
        await self._bulk.mark_failed(
            catalog_id=catalog_id, op_ids=op_ids, error=error,
        )

    def listen(self, *, driver_id: str, catalog_id: str) -> Any:
        return self._bulk.listen(driver_id=driver_id, catalog_id=catalog_id)

    # OutboxWriterProtocol surface — delegate to the legacy task-table
    # writer so the older IndexOp callers keep their durable retry path.
    async def enqueue(
        self, *, indexer_id: str, ctx: IndexContext, op: IndexOp,
        last_error: Optional[str] = None,
    ) -> None:
        await self._legacy.enqueue(
            indexer_id=indexer_id, ctx=ctx, op=op, last_error=last_error,
        )


def _make_default_routing_resolver():
    """Build a resolver that loads the live :class:`CollectionRoutingConfig`
    via ``ConfigsProtocol.get_config`` — the same path used elsewhere in
    the storage layer.
    """

    async def resolve(catalog: str, collection: Optional[str]):
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.storage.routing_config import CollectionRoutingConfig
        from dynastore.tools.discovery import get_protocol

        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            # No platform configs in this process — return a default
            # routing config so the dispatcher gracefully degrades to
            # empty-INDEX (no fan-out).
            return CollectionRoutingConfig()
        return await configs.get_config(
            CollectionRoutingConfig,
            catalog_id=catalog,
            collection_id=collection,
        )

    return resolve


def _make_default_indexer_registry():
    """Build a registry that resolves an :class:`Indexer` by ``indexer_id``
    via the protocol discovery system.

    Cached after first build because the set of registered indexers is
    fixed once app startup completes.
    """
    cache: Dict[str, Optional[Indexer]] = {}

    async def resolve(indexer_id: str) -> Optional[Indexer]:
        if indexer_id in cache:
            return cache[indexer_id]
        from dynastore.tools.discovery import get_protocols

        match: Optional[Indexer] = None
        for impl in get_protocols(Indexer):
            if getattr(impl, "indexer_id", None) == indexer_id:
                match = impl
                break
        cache[indexer_id] = match
        return match

    return resolve


def get_index_dispatcher() -> IndexDispatcher:
    """Process-wide singleton dispatcher — reuses live resolvers + a
    composite outbox (:class:`PgOutboxStore` for the bulk
    :class:`OutboxStore` Protocol path used by ``_handle_missing`` and
    :class:`TaskTableOutboxWriter` for the legacy singular ``enqueue``
    path used by ``_handle_failure``) + a per-indexer
    :class:`CircuitBreaker`.

    The asyncpg pool backing :class:`PgOutboxStore` is created lazily at
    first call to ``enqueue_bulk`` (see :func:`_get_outbox_pool`) so
    importing this module doesn't open DB sockets and tests that never
    exercise the missing-indexer OUTBOX path don't pay the pool cost.
    Pool size is small by default (1..10) since dispatch failures are
    expected to be rare relative to request volume.
    """
    global _DEFAULT_DISPATCHER
    if _DEFAULT_DISPATCHER is None:
        from dynastore.modules.storage.circuit_breaker import CircuitBreaker
        from dynastore.modules.storage.pg_outbox import PgOutboxStore

        bulk_outbox = PgOutboxStore(pool=_LazyPoolProxy())
        legacy_outbox = TaskTableOutboxWriter()
        _DEFAULT_DISPATCHER = IndexDispatcher(
            routing_resolver=_make_default_routing_resolver(),
            indexer_registry=_make_default_indexer_registry(),
            outbox=_DualOutbox(bulk=bulk_outbox, legacy=legacy_outbox),
            breaker=CircuitBreaker(),
        )
    return _DEFAULT_DISPATCHER


async def reset_index_dispatcher() -> None:
    """Test hook — drops the cached singleton so the next
    :func:`get_index_dispatcher` rebuilds with current discovery state.

    Also closes the lazy asyncpg outbox pool (see :func:`_close_outbox_pool`)
    so subsequent tests that exercise production-mode wiring don't leak
    sockets across resets. Must be awaited; tests that previously called
    this synchronously need to be async or wrap with ``asyncio.run``.
    """
    global _DEFAULT_DISPATCHER
    _DEFAULT_DISPATCHER = None
    await _close_outbox_pool()


def _bind_named_to_positional(sql: str, params: Dict[str, Any]):
    """Translate ``:name`` placeholders to ``$N`` for asyncpg.

    Skips PostgreSQL ``::`` cast operators (``:inputs::jsonb`` →
    ``$1::jsonb``).  Order is determined by first appearance in ``sql``
    — same convention SQLAlchemy uses internally for named-bind
    compilation.
    """
    out: List[str] = []
    args: List[Any] = []
    seen: Dict[str, int] = {}
    i = 0
    while i < len(sql):
        ch = sql[i]
        # Skip the PG ``::`` cast operator — it's not a bind site.
        if ch == ":" and i + 1 < len(sql) and sql[i + 1] == ":":
            out.append("::")
            i += 2
            continue
        if ch == ":" and (i + 1 < len(sql)) and (sql[i + 1].isalpha() or sql[i + 1] == "_"):
            j = i + 1
            while j < len(sql) and (sql[j].isalnum() or sql[j] == "_"):
                j += 1
            name = sql[i + 1:j]
            if name not in seen:
                seen[name] = len(args) + 1
                args.append(params[name])
            out.append(f"${seen[name]}")
            i = j
            continue
        out.append(ch)
        i += 1
    return "".join(out), args


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


class IndexDispatcher:
    """Fan-out point for index ops across all configured indexers.

    Instantiated once per process (typically a singleton resolved via
    protocol discovery).  Stateless except for the circuit-breaker
    handle (Phase 3).

    Parameters
    ----------
    routing_resolver
        Async callable ``(catalog, collection) -> CollectionRoutingConfig``
        used to look up ``operations[INDEX]``.  Pluggable so the dispatcher
        is testable without booting the full config service.
    indexer_registry
        Async callable ``(indexer_id) -> Indexer | None`` resolving the
        runtime instance for a routing entry's ``driver_id``.
    outbox
        Optional ``OutboxWriter`` (Phase 2) — when ``None``, ``OUTBOX``
        failure policy degrades to ``WARN`` with a one-time log.
    breaker
        Optional ``CircuitBreaker`` (Phase 3) — when ``None``, every
        sync attempt is tried unconditionally.
    """

    def __init__(
        self,
        *,
        routing_resolver,
        indexer_registry,
        outbox=None,
        breaker=None,
    ) -> None:
        self._resolve_routing = routing_resolver
        self._resolve_indexer = indexer_registry
        self._outbox = outbox
        self._breaker = breaker
        self._outbox_warning_emitted: set = set()
        # Dedupe key for ``_handle_missing`` WARN-policy log output:
        # ``(driver_id, catalog, collection)``. A single deployment can
        # legitimately omit a driver from a SCOPE; we don't want one
        # missing driver to flood logs at every item write for the
        # lifetime of the process.
        self._missing_warning_emitted: set = set()
        # ``ensure_indexer`` is idempotent on the driver side, but we
        # cache success here to avoid the per-call ES ``indices.exists``
        # round-trip.  Keyed by ``(indexer_id, catalog, collection)``;
        # the set survives for the dispatcher's process lifetime.
        self._ensured: set = set()

    # ------------------------------------------------------------------
    # Public surface — single-op and bulk
    # ------------------------------------------------------------------

    async def fan_out(
        self, ctx: IndexContext, op: DispatchableOp,
    ) -> None:
        """Dispatch a single index op across every configured indexer
        for ``(ctx.catalog, ctx.collection)``.

        Each indexer's outcome is governed by its routing entry's
        :attr:`OperationDriverEntry.on_failure` policy — the dispatcher
        does not stop on a single non-FATAL failure.

        Accepts either the legacy :class:`IndexOp` or the durable
        :class:`IndexableOp` shape; missing-indexer outbox enqueue uses
        the bulk ``OutboxStore`` interface and therefore requires
        :class:`IndexableOp`.
        """
        entries = await self._index_entries(ctx)
        for entry in entries:
            await self._dispatch_one(entry, ctx, op)

    async def fan_out_bulk(
        self, ctx: IndexContext, ops: Sequence[DispatchableOp],
    ) -> Dict[str, BulkResult]:
        """Dispatch a bulk of index ops across every configured indexer.

        Returns per-indexer ``BulkResult`` so callers can surface partial
        failures (a 207-style report).  Per-op failures inside an indexer
        are absorbed into ``BulkResult.failures``; an indexer raising
        applies ``FailurePolicy`` to the whole batch.
        """
        results: Dict[str, BulkResult] = {}
        entries = await self._index_entries(ctx)
        for entry in entries:
            indexer = await self._resolve_indexer(entry.driver_ref)
            if indexer is None:
                # Driver not registered locally — apply the routing
                # entry's FailurePolicy per-op so observable behaviour
                # matches the in-process "indexer raised" path.
                for op in ops:
                    await self._handle_missing(entry, ctx, op)
                continue
            results[entry.driver_ref] = await self._dispatch_bulk(
                entry, indexer, ctx, ops,
            )
        return results

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _index_entries(
        self, ctx: IndexContext,
    ) -> List[OperationDriverEntry]:
        try:
            routing = await self._resolve_routing(ctx.catalog, ctx.collection)
        except Exception as exc:
            logger.warning(
                "IndexDispatcher: routing lookup failed for %s/%s: %s",
                ctx.catalog, ctx.collection, exc,
            )
            return []
        ops_map = getattr(routing, "operations", {}) or {}
        return list(ops_map.get(Operation.INDEX, []))

    async def _dispatch_one(
        self,
        entry: OperationDriverEntry,
        ctx: IndexContext,
        op: DispatchableOp,
    ) -> None:
        indexer = await self._resolve_indexer(entry.driver_ref)
        if indexer is None:
            await self._handle_missing(entry, ctx, op)
            return

        # OUTBOX_ONLY shortcut: never attempt sync, always enqueue.
        if entry.write_mode == WriteMode.ASYNC and entry.on_failure == FailurePolicy.OUTBOX:
            await self._enqueue_or_warn(entry, ctx, op)
            return

        # Circuit breaker check (Phase 3 — no-op when self._breaker is None).
        if self._breaker is not None and self._breaker.is_open(entry.driver_ref):
            if entry.on_failure == FailurePolicy.OUTBOX:
                await self._enqueue_or_warn(entry, ctx, op)
            elif entry.on_failure == FailurePolicy.FATAL:
                raise IndexerFatal(
                    entry.driver_ref, op,
                    RuntimeError("circuit breaker open"),
                )
            else:
                logger.debug(
                    "IndexDispatcher: breaker open for '%s' — skipping (%s).",
                    entry.driver_ref, entry.on_failure,
                )
            return

        # Idempotent bootstrap of the indexer's storage (per-process cache).
        if not await self._ensure_or_handle(entry, indexer, ctx, op):
            return

        # Synchronous in-process attempt.
        try:
            # See note in ``_dispatch_bulk``: in-process indexers still
            # consume the legacy ``IndexOp`` shape; cast at the boundary
            # so the Union-typed dispatcher surface stays compatible
            # without forcing every indexer migration in this phase.
            await indexer.index(ctx, cast(IndexOp, op))
            if self._breaker is not None:
                self._breaker.record_success(entry.driver_ref)
        except Exception as exc:
            if self._breaker is not None:
                self._breaker.record_failure(entry.driver_ref)
            await self._handle_failure(entry, ctx, op, exc)

    async def _handle_missing(
        self,
        entry: OperationDriverEntry,
        ctx: IndexContext,
        op: DispatchableOp,
    ) -> None:
        """Apply ``entry.on_failure`` when the indexer is not locally
        registered.  Mirrors the policy semantics already used for
        resolved-but-failing indexers, so deployments don't see two
        different behaviours for "ES is down" vs "ES isn't installed
        in this SCOPE".

        WARN dedupes per ``(driver_id, catalog, collection)`` so a
        deliberately-omitted driver doesn't flood the log on every op.
        OUTBOX path requires the new :class:`IndexableOp` shape; legacy
        :class:`IndexOp` callers fall back to the existing
        :meth:`_enqueue_or_warn` path so the singular-``enqueue`` outbox
        writer still receives them.
        """
        policy = entry.on_failure
        if policy == FailurePolicy.FATAL:
            raise IndexerFatal(
                entry.driver_ref, op,
                RuntimeError(
                    f"indexer '{entry.driver_ref}' not registered locally and "
                    f"routing entry is FATAL"
                ),
            )
        if policy == FailurePolicy.OUTBOX:
            if isinstance(op, IndexableOp):
                await self._enqueue_outbox_record(entry, ctx, op)
            else:
                # Legacy IndexOp path — degrade through the existing
                # enqueue-or-warn helper so behaviour is unchanged for
                # callers still on the older value type.
                await self._enqueue_or_warn(entry, ctx, op)
            return
        if policy == FailurePolicy.WARN:
            key = (entry.driver_ref, ctx.catalog, ctx.collection)
            if key not in self._missing_warning_emitted:
                self._missing_warning_emitted.add(key)
                logger.warning(
                    "IndexDispatcher: indexer '%s' not registered locally "
                    "(catalog=%s, collection=%s) — skipping per WARN policy. "
                    "Future occurrences for this triple are suppressed.",
                    entry.driver_ref, ctx.catalog, ctx.collection,
                )
            return
        # IGNORE — silent.

    async def _enqueue_outbox_record(
        self,
        entry: OperationDriverEntry,
        ctx: IndexContext,
        op: IndexableOp,
    ) -> None:
        """Translate an :class:`IndexableOp` into an :class:`OutboxRecord`
        and enqueue via the bulk :class:`OutboxStore` interface.

        Used by :meth:`_handle_missing` when a driver is absent and the
        routing entry says ``OUTBOX``.  When no outbox is wired, degrade
        to a one-time WARN keyed identically to the missing-warning
        dedupe so operators see exactly one signal per
        ``(driver_id, catalog, collection)``.
        """
        if self._outbox is None:
            key = ("__no_outbox__", entry.driver_ref, ctx.catalog, ctx.collection)
            if key not in self._missing_warning_emitted:
                self._missing_warning_emitted.add(key)
                logger.warning(
                    "IndexDispatcher: indexer '%s' missing AND no outbox "
                    "wired (catalog=%s, collection=%s); op dropped per "
                    "fallback.",
                    entry.driver_ref, ctx.catalog, ctx.collection,
                )
            return
        from dynastore.models.protocols.indexing import OutboxRecord
        record = OutboxRecord(
            op_id=op.op_id,
            driver_id=entry.driver_ref,
            driver_instance_id=op.driver_instance_id,
            collection_id=op.collection_id,
            op=op.op,
            item_id=op.item_id,
            payload=op.payload,
            idempotency_key=op.idempotency_key,
        )
        # Outbox may implement the bulk OutboxStore Protocol
        # (``enqueue_bulk``) or only the legacy singular
        # ``OutboxWriterProtocol.enqueue`` — pick whichever the wired
        # instance offers so the dispatcher stays compatible with both
        # during the migration window.  ``OutboxStore`` is
        # ``@runtime_checkable`` so we narrow via ``isinstance`` rather
        # than ``getattr`` probing (project rule: Protocols over hasattr).
        if isinstance(self._outbox, OutboxStore):
            await self._outbox.enqueue_bulk(
                None,
                catalog_id=op.catalog_id,
                rows=[record],
            )
            return
        # Fall through to the legacy singular-enqueue path, which uses
        # the IndexOp shape internally.  This shouldn't fire in practice
        # for IndexableOp callers (they wire OutboxStore), but keeps the
        # dispatcher resilient mid-migration.
        await self._enqueue_or_warn(entry, ctx, op)

    async def _ensure_or_handle(
        self,
        entry: OperationDriverEntry,
        indexer: Indexer,
        ctx: IndexContext,
        op: DispatchableOp,
    ) -> bool:
        """Run ``ensure_indexer`` once per (indexer_id, catalog, collection).

        Returns True when the indexer is ready to receive ops, False when
        the bootstrap failed and the dispatcher already routed the op
        through its FailurePolicy (e.g. enqueued to outbox, logged WARN).
        Raises :class:`IndexerFatal` when the policy is FATAL.
        """
        key = (entry.driver_ref, ctx.catalog, ctx.collection)
        if key in self._ensured:
            return True
        # Some Indexer impls may not have ``ensure_indexer`` yet during the
        # transition window — treat absence as "no bootstrap needed".
        ensure = getattr(indexer, "ensure_indexer", None)
        if ensure is None:
            self._ensured.add(key)
            return True
        try:
            await ensure(ctx)
            self._ensured.add(key)
            return True
        except Exception as exc:
            await self._handle_failure(entry, ctx, op, exc)
            return False

    async def _dispatch_bulk(
        self,
        entry: OperationDriverEntry,
        indexer: Indexer,
        ctx: IndexContext,
        ops: Sequence[DispatchableOp],
    ) -> BulkResult:
        if self._breaker is not None and self._breaker.is_open(entry.driver_ref):
            return BulkResult(total=len(ops), failed=len(ops), failures=[
                {"reason": "circuit_breaker_open", "indexer": entry.driver_ref},
            ])
        # Bootstrap once per (indexer, catalog, collection).  Use the
        # first op as the failure-handling target if ensure raises.
        if ops and not await self._ensure_or_handle(entry, indexer, ctx, ops[0]):
            return BulkResult(total=len(ops), failed=len(ops), failures=[
                {"reason": "ensure_indexer_failed", "indexer": entry.driver_ref},
            ])
        try:
            # ``Indexer.index_bulk`` is still typed against the legacy
            # ``IndexOp``; concrete implementations duck-type the op
            # fields they need.  The dispatcher's Union accepts both
            # shapes — cast at the boundary, the migration to a unified
            # shape happens in a later phase.
            result = await indexer.index_bulk(
                ctx, cast(Sequence[IndexOp], ops),
            )
            if self._breaker is not None:
                self._breaker.record_success(entry.driver_ref)
            return result
        except Exception as exc:
            if self._breaker is not None:
                self._breaker.record_failure(entry.driver_ref)
            # Bulk failure: apply policy to the whole batch.
            for op in ops:
                await self._handle_failure(entry, ctx, op, exc, bulk=True)
            return BulkResult(
                total=len(ops),
                failed=len(ops),
                failures=[{"reason": str(exc), "indexer": entry.driver_ref}],
            )

    async def _handle_failure(
        self,
        entry: OperationDriverEntry,
        ctx: IndexContext,
        op: DispatchableOp,
        exc: BaseException,
        *,
        bulk: bool = False,
    ) -> None:
        policy = entry.on_failure
        if policy == FailurePolicy.FATAL:
            raise IndexerFatal(entry.driver_ref, op, exc) from exc
        if policy == FailurePolicy.OUTBOX:
            await self._enqueue_or_warn(entry, ctx, op, original=exc)
            return
        if policy == FailurePolicy.WARN:
            descriptor = _describe_op(op)
            logger.warning(
                "IndexDispatcher: indexer '%s' failed for %s "
                "(policy=warn%s): %s",
                entry.driver_ref, descriptor,
                ", bulk" if bulk else "", exc,
            )
            return
        # IGNORE — silent skip
        logger.debug(
            "IndexDispatcher: indexer '%s' failed (policy=ignore): %s",
            entry.driver_ref, exc,
        )

    # Public diagnostic — operator-facing introspection of the routing
    # entries that this dispatcher will fan out across, without invoking
    # any indexer.  Used by /_health and /index-dispatcher-status surfaces.
    async def describe(self, ctx: IndexContext) -> Dict[str, Any]:
        entries = await self._index_entries(ctx)
        return {
            "catalog": ctx.catalog,
            "collection": ctx.collection,
            "indexers": [
                {
                    "indexer_id": e.driver_ref,
                    "write_mode": e.write_mode,
                    "on_failure": e.on_failure,
                    "source": getattr(e, "source", None),
                    "registered": (await self._resolve_indexer(e.driver_ref)) is not None,
                }
                for e in entries
            ],
        }

    async def _enqueue_or_warn(
        self,
        entry: OperationDriverEntry,
        ctx: IndexContext,
        op: DispatchableOp,
        *,
        original: Optional[BaseException] = None,
    ) -> None:
        """Enqueue an outbox row when configured; otherwise degrade to WARN.

        Phase 1 ships without an outbox writer — this method emits a
        one-time warning per indexer and falls through to WARN-level
        log of the original exception.  Phase 2 wires the real writer.
        """
        if self._outbox is None:
            if entry.driver_ref not in self._outbox_warning_emitted:
                self._outbox_warning_emitted.add(entry.driver_ref)
                logger.warning(
                    "IndexDispatcher: indexer '%s' has on_failure=outbox but "
                    "no OutboxWriter is wired — failures degrade to WARN. "
                    "Phase 2 will activate durable retry.",
                    entry.driver_ref,
                )
            if original is not None:
                logger.warning(
                    "IndexDispatcher: indexer '%s' failed for %s "
                    "(policy=outbox, degraded): %s",
                    entry.driver_ref, _describe_op(op), original,
                )
            return

        # The legacy singular-``enqueue`` writer expects an IndexOp; if
        # this code path is entered with an IndexableOp the writer would
        # see attribute errors.  Skip with a single warning rather than
        # hard-failing — the caller's failure policy already chose
        # tolerance.
        # NOTE: ``OutboxWriterProtocol`` (defined above) pre-dates the
        # project's runtime_checkable typing baseline and is *not*
        # decorated with ``@runtime_checkable``; ``isinstance`` against
        # it would raise ``TypeError``.  The bulk path above already
        # narrowed via ``OutboxStore``; here we fall back to a ``getattr``
        # probe for the legacy singular surface only.
        enqueue = getattr(self._outbox, "enqueue", None)
        if enqueue is None:
            logger.warning(
                "IndexDispatcher: outbox writer for indexer '%s' has no "
                "singular ``enqueue`` method; cannot enqueue %s.",
                entry.driver_ref, _describe_op(op),
            )
            return
        try:
            await enqueue(
                indexer_id=entry.driver_ref,
                ctx=ctx,
                op=op,
                last_error=str(original) if original else None,
            )
        except Exception as enqueue_exc:
            # Outbox itself failed — last-resort WARN.  We do NOT escalate
            # to FATAL here because the upstream caller has already chosen
            # OUTBOX as a tolerant policy; surfacing this as fatal would
            # surprise them.
            logger.error(
                "IndexDispatcher: outbox enqueue failed for indexer '%s' "
                "on %s — original error: %s, enqueue error: %s",
                entry.driver_ref, _describe_op(op), original, enqueue_exc,
            )


def _describe_op(op: "DispatchableOp") -> str:
    """Format an op for log/exception output regardless of value type.

    The dispatcher accepts both the legacy :class:`IndexOp` and the new
    :class:`IndexableOp`; both shapes carry equivalent identity info
    under different field names.  This helper keeps log strings
    consistent without leaking value-type branching into every
    formatter.
    """
    if isinstance(op, IndexableOp):
        return f"{op.op}/{op.collection_id}/{op.item_id}"
    return f"{op.op_type}/{op.entity_type}/{op.entity_id}"
