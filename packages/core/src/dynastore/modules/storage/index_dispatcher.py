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

import hashlib
import json
import logging
from typing import Any, Dict, List, Optional, Protocol, Sequence, Tuple, Union, cast

from dynastore.models.protocols.indexer import (
    BulkResult,
    Indexer,
    IndexContext,
    IndexOp,
)
from dynastore.models.protocols.indexing import IndexableOp, OutboxStore
from dynastore.tools.async_utils import LoopLocalLock
from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    OperationDriverEntry,
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
        op: "Optional[DispatchableOp]",
        original: BaseException,
    ) -> None:
        # Format depending on which op shape was passed — bulk dispatch
        # accepts both legacy IndexOp and IndexableOp; either may be the
        # FATAL target (or None when an upstream filter rejects the
        # whole batch).
        if op is None:
            descriptor = "<empty batch>"
        elif isinstance(op, IndexableOp):
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
        ops: Sequence[IndexOp],
        last_error: Optional[str] = None,
        chunk_size: Optional[int] = None,
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

    DEFAULT_CHUNK_SIZE = 500

    async def enqueue(
        self,
        *,
        indexer_id: str,
        ctx: IndexContext,
        ops: Sequence[IndexOp],
        last_error: Optional[str] = None,
        chunk_size: Optional[int] = None,
    ) -> None:
        if not ops:
            return
        if ctx.pg_conn is None:
            # Without a caller TX we can't honour the atomicity guarantee.
            # Emit a single warning and bail; the dispatcher's degrade
            # path already logged the original failure.
            sample = ops[0]
            logger.warning(
                "TaskTableOutboxWriter: ctx.pg_conn is None — skipping "
                "outbox enqueue for indexer '%s' on %s/%s/%s (+%d more). "
                "Caller must pass an open PG connection on IndexContext "
                "for the OUTBOX policy to be durable.",
                indexer_id, sample.op_type, sample.entity_type,
                sample.entity_id, max(len(ops) - 1, 0),
            )
            return

        size = chunk_size if chunk_size and chunk_size > 0 else self.DEFAULT_CHUNK_SIZE
        # Chunk per (op_type, entity_type) so dedup_key stays meaningful.
        # Mixed op_types in one task row would either need a composite key
        # (over-coalesces) or no key (loses dedup) — split is cleaner.
        grouped: Dict[Tuple[str, str], List[IndexOp]] = {}
        for op in ops:
            grouped.setdefault((op.op_type, op.entity_type), []).append(op)

        for (op_type, entity_type), bucket in grouped.items():
            for start in range(0, len(bucket), size):
                chunk = bucket[start:start + size]
                await self._enqueue_chunk(
                    indexer_id=indexer_id,
                    ctx=ctx,
                    op_type=op_type,
                    entity_type=entity_type,
                    chunk=chunk,
                    last_error=last_error,
                )

    async def _enqueue_chunk(
        self,
        *,
        indexer_id: str,
        ctx: IndexContext,
        op_type: str,
        entity_type: str,
        chunk: Sequence[IndexOp],
        last_error: Optional[str],
    ) -> None:
        task_schema = self._resolve_task_schema()
        schema_name = self._schema_resolver(ctx.catalog)

        op_records = [
            {
                "entity_id": o.entity_id,
                "op_type": o.op_type,
                "payload": o.payload,
            }
            for o in chunk
        ]
        inputs = {
            "indexer_id": indexer_id,
            "op_type": op_type,
            "entity_type": entity_type,
            "catalog": ctx.catalog,
            "collection": ctx.collection,
            "ops": op_records,
            "correlation_id": ctx.correlation_id,
            "last_error": last_error,
        }
        dedup_key = self._dedup_key(
            indexer_id, op_type, entity_type,
            [o.entity_id for o in chunk],
        )

        from dynastore.tools.identifiers import generate_uuidv7
        from dynastore.tools.json import CustomJSONEncoder

        task_id = generate_uuidv7()
        # Observability (#504): structured log line for GCP log-based metric
        # `index_chunks_emitted_total{indexer,source,op_type}`. One row per
        # task — chunk_size reflects how many ops the row coalesces.
        logger.info(
            "index_chunk_emitted indexer=%s source=legacy op_type=%s "
            "catalog=%s collection=%s chunk_size=%d",
            indexer_id, op_type, ctx.catalog, ctx.collection, len(chunk),
        )
        _log_dispatch_path(
            mode="outbox_handoff",
            indexer_id=indexer_id,
            catalog=ctx.catalog,
            collection=ctx.collection,
            chunk_size=len(chunk),
        )
        await self._exec_insert(
            ctx.pg_conn,
            sql=f"""
                INSERT INTO {task_schema}.tasks (
                    task_id, schema_name, scope, caller_id, task_type, type,
                    execution_mode, inputs, collection_id, dedup_key, status
                ) VALUES (
                    :task_id, :schema_name, 'CATALOG', :caller_id, :task_type,
                    'task', 'ASYNCHRONOUS', CAST(:inputs AS jsonb),
                    :collection_id, :dedup_key, 'PENDING'
                )
                ON CONFLICT DO NOTHING
            """,
            params=dict(
                task_id=task_id,
                schema_name=schema_name,
                caller_id=f"index_dispatcher:{indexer_id}",
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
    def _dedup_key(
        indexer_id: str, op_type: str, entity_type: str,
        entity_ids: Sequence[str],
    ) -> str:
        """Stable hash over the sorted entity-id set of a chunk.

        Same chunk retried → same key (so retries coalesce). Different
        chunks of the same batch get distinct keys.
        """
        sorted_ids = sorted(entity_ids)
        material = "|".join(
            (indexer_id, op_type, entity_type, *sorted_ids),
        ).encode("utf-8")
        return hashlib.sha256(material).hexdigest()[:64]

    async def _exec_insert(
        self, conn: Any, sql: str, params: Dict[str, Any],
    ) -> None:
        """Execute the INSERT on the caller's connection.

        Delegates to :class:`DQLQuery` so connection-flavour dispatch
        (sync/async SA ``Connection``/``Session``) and named-bind
        translation are handled in one place — same path every other
        query in the codebase uses. This keeps the OUTBOX writer free
        of bespoke isinstance ladders and inherits the typed exception
        surface (``DatabaseConnectionError`` / ``QueryExecutionError``,
        plus transient-asyncpg detection from #235/#239).

        ``ctx.pg_conn`` always comes from ``managed_transaction(engine)``
        which yields a SA resource; raw asyncpg connections are not
        produced by that contract, so DQLQuery's SA-only dispatch is
        sufficient.
        """
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler,
        )
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn, **params,
        )


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
_OUTBOX_POOL_LOCK = LoopLocalLock()


def _log_dispatch_path(
    *,
    mode: str,
    indexer_id: str,
    catalog: str,
    collection: Optional[str],
    chunk_size: int,
) -> None:
    # Observability (#504): structured log line for GCP log-based metrics
    # `index_dispatch_path_total{mode}` (counter on mode label) and
    # `index_chunk_size_bucket` (distribution on the chunk_size field).
    # `mode` is one of: post_commit_inline | outbox_handoff.
    logger.info(
        "index_dispatch_path mode=%s indexer=%s catalog=%s collection=%s "
        "chunk_size=%d",
        mode, indexer_id, catalog, collection, chunk_size,
    )


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
        # Observability (#504): one structured log per *successful* bulk
        # enqueue — captures chunk size and per-indexer emission rate.
        # Emitted after the delegate returns so a raise does not leave a
        # log line describing a chunk that was never durably persisted.
        chunk_size = len(rows)
        if chunk_size:
            indexer_ids = sorted({getattr(r, "indexer_id", "?") for r in rows})
            indexer_label = ",".join(indexer_ids)
            logger.info(
                "index_chunk_emitted indexer=%s source=bulk catalog=%s "
                "chunk_size=%d",
                indexer_label, catalog_id, chunk_size,
            )
            _log_dispatch_path(
                mode="outbox_handoff",
                indexer_id=indexer_label,
                catalog=catalog_id,
                collection="",
                chunk_size=chunk_size,
            )

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
        self, *, indexer_id: str, ctx: IndexContext,
        ops: Sequence[IndexOp],
        last_error: Optional[str] = None,
        chunk_size: Optional[int] = None,
    ) -> None:
        await self._legacy.enqueue(
            indexer_id=indexer_id, ctx=ctx, ops=ops,
            last_error=last_error, chunk_size=chunk_size,
        )


def _make_default_routing_resolver():
    """Build an entity-aware resolver that loads the live PluginConfig
    matching the dispatched tier via ``ConfigsProtocol.get_config``.

    Per un-fao/GeoID#810 (Option B): the dispatcher is reached from three
    tiers and each must read its own PluginConfig:

    * ``entity_type="item"`` -> :class:`ItemsRoutingConfig` — OGC ingest
      (``item_service._dispatch_index_upsert``) and item delete
      (``item_query``). Carries the privacy-cascade validator's contract
      into runtime: a private collection that pins
      ``items_elasticsearch_private_driver`` as a secondary-index ``WRITE``
      entry (``secondary_index=True``) in ``ItemsRoutingConfig.operations[WRITE]``
      now fires on item upsert/delete via the OGC endpoints.
    * ``entity_type="collection"`` -> :class:`CollectionRoutingConfig` —
      collection metadata propagation (``_dispatch_collection_index``).
    * ``entity_type="catalog"`` -> :class:`CatalogRoutingConfig` — catalog
      metadata propagation (event-driven via ``ReindexWorker`` today, which
      resolves through its own ``_resolve_catalog_indexers`` rather than
      this dispatcher path; supported here for symmetry / future callers).
    * ``entity_type="asset"`` -> :class:`AssetRoutingConfig` — no
      production caller through this dispatcher today; supported for
      symmetry.
    * ``entity_type=None`` -> :class:`CollectionRoutingConfig` —
      back-compat for any caller that built an :class:`IndexContext`
      before the field was introduced.
    """

    async def resolve(
        catalog: str,
        collection: Optional[str],
        *,
        entity_type: Optional[str] = None,
    ):
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.storage.routing_config import (
            AssetRoutingConfig,
            CatalogRoutingConfig,
            CollectionRoutingConfig,
            ItemsRoutingConfig,
        )
        from dynastore.tools.discovery import get_protocol

        if entity_type == "item":
            config_cls = ItemsRoutingConfig
        elif entity_type == "catalog":
            config_cls = CatalogRoutingConfig
        elif entity_type == "asset":
            config_cls = AssetRoutingConfig
        else:
            # "collection" and the back-compat None branch both read
            # CollectionRoutingConfig.
            config_cls = CollectionRoutingConfig

        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            # No platform configs in this process — return a default
            # routing config so the dispatcher gracefully degrades to no
            # secondary-index entries (no fan-out).
            return config_cls()
        return await configs.get_config(
            config_cls,
            catalog_id=catalog,
            collection_id=collection,
        )

    return resolve


async def _call_resolver(
    resolver,
    catalog: str,
    collection: Optional[str],
    entity_type: Optional[str],
):
    """Invoke a routing resolver with entity-type awareness, tolerating
    legacy ``(catalog, collection)`` test stubs that predate the kwarg.

    Production resolvers (``_make_default_routing_resolver``) accept the
    ``entity_type`` kwarg; some unit-test stubs do not. Catching the
    ``TypeError`` lets the dispatcher route correctly in prod while
    preserving the existing test seam without forcing every fixture to
    grow a parameter it doesn't read.
    """
    try:
        return await resolver(catalog, collection, entity_type=entity_type)
    except TypeError:
        return await resolver(catalog, collection)


def _make_default_indexer_registry():
    """Build a registry that resolves an :class:`Indexer` by class identity.

    Identity is ``_to_snake(type(impl).__name__)`` — same convention as
    ``_self_register_indexers_into`` (routing_config.py) and
    ``index_propagation/task.py``. No separate ``indexer_id`` attribute.

    Cached after first build because the set of registered indexers is
    fixed once app startup completes.
    """
    cache: Dict[str, Optional[Indexer]] = {}

    async def resolve(indexer_id: str) -> Optional[Indexer]:
        if indexer_id in cache:
            return cache[indexer_id]
        from dynastore.tools.discovery import get_protocols
        from dynastore.tools.typed_store.base import _to_snake

        match: Optional[Indexer] = None
        for impl in get_protocols(Indexer):
            if _to_snake(type(impl).__name__) == indexer_id:
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
        Async callable used to look up the secondary-index ``WRITE`` entries
        (``secondary_index=True``) in ``operations[WRITE]``. The
        production resolver accepts an ``entity_type`` keyword and returns
        the matching ``*RoutingConfig`` per tier (items/collection/
        catalog/asset). Legacy 2-arg ``(catalog, collection)`` stubs are
        still accepted via :func:`_call_resolver`'s ``TypeError``
        fallback so existing fixtures continue to work; pre-#810 callers
        that don't set ``IndexContext.entity_type`` resolve to
        ``CollectionRoutingConfig`` (back-compat default). Pluggable so
        the dispatcher is testable without booting the full config
        service.
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
    # Public surface — bulk dispatch
    # ------------------------------------------------------------------

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
        if ops and not entries:
            # #914 — dispatch-level silent no-op: ops were submitted but no
            # routing entry exists for this (catalog, collection,
            # entity_type), so no indexer runs and the caller receives an
            # empty results dict. Surface it so a misconfigured routing
            # table can't silently swallow writes.
            logger.warning(
                "IndexDispatcher: %d op(s) submitted for catalog=%s "
                "collection=%s entity_type=%s but routing returned NO "
                "secondary-index entries — writes will not reach any indexer. "
                "Check RoutingConfig.operations[WRITE] for secondary-index "
                "entries (secondary_index=True) in this scope.",
                len(ops), ctx.catalog, ctx.collection,
                getattr(ctx, "entity_type", None),
            )
        for entry in entries:
            indexer = await self._resolve_indexer(entry.driver_ref)
            if indexer is None:
                # Driver not registered locally — apply the routing
                # entry's FailurePolicy per-op so observable behaviour
                # matches the in-process "indexer raised" path.
                for op in ops:
                    await self._handle_missing(entry, ctx, op)
                continue
            entry_ops, rejected = await self._apply_input_transformers(entry, ctx, ops)
            if not entry_ops:
                results[entry.driver_ref] = BulkResult(
                    total=len(ops),
                    failed=len(rejected),
                    failures=rejected,
                )
                continue
            result = await self._dispatch_bulk(entry, indexer, ctx, entry_ops)
            if result.failed == 0:
                _log_dispatch_path(
                    mode="post_commit_inline",
                    indexer_id=entry.driver_ref,
                    catalog=ctx.catalog,
                    collection=ctx.collection,
                    chunk_size=len(entry_ops),
                )
            # #914 — silent no-op trap: an indexer that returns
            # ``BulkResult(total=N, succeeded=0, failed=0)`` (e.g. ES bulk
            # response shape the driver doesn't parse) was previously
            # indistinguishable from a real success in logs, leaving the
            # target index empty with no warning.  Surface it loudly so
            # ops sees the divergence on the next write.
            if result.total > 0 and result.succeeded == 0 and result.failed == 0:
                logger.warning(
                    "IndexDispatcher: indexer '%s' returned a silent no-op "
                    "(total=%d, succeeded=0, failed=0) for catalog=%s "
                    "collection=%s — index will be empty despite a "
                    "'successful' dispatch. Check the driver's bulk-response "
                    "parser.",
                    entry.driver_ref, result.total,
                    ctx.catalog, ctx.collection,
                )
            if rejected:
                result = BulkResult(
                    total=result.total + len(rejected),
                    succeeded=result.succeeded,
                    failed=result.failed + len(rejected),
                    failures=[*result.failures, *rejected],
                )
            results[entry.driver_ref] = result
        return results

    async def _apply_input_transformers(
        self,
        entry: OperationDriverEntry,
        ctx: IndexContext,
        ops: Sequence[DispatchableOp],
    ) -> Tuple[List[DispatchableOp], List[Dict[str, Any]]]:
        """Resolve ``entry.input_transformers`` to instances and walk each
        op's payload through the chain. A failure on one op rejects only
        that op; the rest continue to the indexer. Empty chain ⇒ ops
        passed through unchanged.
        """
        if not entry.input_transformers:
            return list(ops), []
        transformers = await self._resolve_input_chain(
            entry.input_transformers, ctx,
        )
        if not transformers:
            return list(ops), []
        from dynastore.models.protocols.entity_transform import (
            TransformChainContext,
        )
        from dynastore.modules.storage.transform_runtime import apply_transform_chain

        # One context per batch — the same instance is threaded through every
        # op so I/O-bearing transformers reuse the dispatcher's live ``pg_conn``
        # and share ``cache`` across the bulk (N items ⇒ one lookup per key, #1568).
        chain_ctx = TransformChainContext(
            pg_conn=ctx.pg_conn,
            correlation_id=ctx.correlation_id or None,
        )
        kept: List[DispatchableOp] = []
        rejected: List[Dict[str, Any]] = []
        for op in ops:
            payload = _op_payload(op)
            if payload is None:
                kept.append(op)
                continue
            try:
                transformed = await apply_transform_chain(
                    payload,
                    transformers,
                    catalog_id=ctx.catalog,
                    collection_id=ctx.collection,
                    entity_kind=_op_entity_kind(op),
                    ctx=chain_ctx,
                )
            except Exception as exc:
                rejected.append({
                    "reason": f"input_transformer_failed: {exc}",
                    "indexer": entry.driver_ref,
                    "entity_id": _op_entity_id(op),
                })
                logger.warning(
                    "IndexDispatcher: input_transformer chain failed for "
                    "indexer '%s' on entity '%s' — rejecting this item, "
                    "continuing with the rest of the bulk: %s",
                    entry.driver_ref, _op_entity_id(op), exc,
                )
                continue
            kept.append(_with_payload(op, transformed))
        return kept, rejected

    async def _resolve_input_chain(
        self,
        refs: Sequence[str],
        ctx: IndexContext,
    ) -> List[Any]:
        from dynastore.models.protocols.entity_transform import (
            EntityTransformProtocol,
        )
        from dynastore.tools.discovery import get_protocols
        from dynastore.tools.typed_store.base import _to_snake

        by_ref = {
            _to_snake(type(t).__name__): t
            for t in get_protocols(EntityTransformProtocol)
        }
        chain: List[Any] = []
        for ref in refs:
            transformer = by_ref.get(ref)
            if transformer is None:
                logger.warning(
                    "IndexDispatcher: input transformer '%s' not registered "
                    "(catalog=%s, collection=%s); skipping in chain.",
                    ref, ctx.catalog, ctx.collection,
                )
                continue
            chain.append(transformer)
        return chain

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _index_entries(
        self, ctx: IndexContext,
    ) -> List[OperationDriverEntry]:
        try:
            routing = await _call_resolver(
                self._resolve_routing,
                ctx.catalog,
                ctx.collection,
                ctx.entity_type,
            )
        except Exception as exc:
            logger.warning(
                "IndexDispatcher: routing lookup failed for %s/%s: %s",
                ctx.catalog, ctx.collection, exc,
            )
            return []
        ops_map = getattr(routing, "operations", {}) or {}
        from dynastore.modules.storage.routing_config import (
            secondary_index_entries,
        )
        return secondary_index_entries(ops_map)

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
                await self._enqueue_or_warn(entry, ctx, [op])
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
        await self._enqueue_or_warn(entry, ctx, [op])

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
            # Breaker is open — the indexer is not healthy enough to attempt a
            # sync call.  We still MUST honour the entry's on_failure policy so
            # that OUTBOX entries are enqueued (and drained later when the
            # breaker half-closes) rather than being silently discarded.  This
            # matches the design intent stated in the module docstring:
            # "OUTBOX policy still enqueues so the worker drains when the
            # breaker half-closes."
            exc = RuntimeError(
                f"circuit_breaker_open for indexer '{entry.driver_ref}'"
            )
            await self._handle_failure_bulk(entry, ctx, ops, exc)
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
            # Bulk failure: apply policy to the whole batch in one call.
            await self._handle_failure_bulk(entry, ctx, ops, exc)
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
            await self._enqueue_or_warn(entry, ctx, [op], original=exc)
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

    async def _handle_failure_bulk(
        self,
        entry: OperationDriverEntry,
        ctx: IndexContext,
        ops: Sequence[DispatchableOp],
        exc: BaseException,
    ) -> None:
        """Apply ``entry.on_failure`` to a whole bulk batch in one call.

        FATAL still raises (preserves per-op semantics — the first op is
        used as the IndexerFatal target). OUTBOX enqueues the batch as
        chunked task rows. WARN/IGNORE log once at batch granularity
        rather than per op so 500-item batches don't fan out logs.
        """
        policy = entry.on_failure
        if policy == FailurePolicy.FATAL:
            target = ops[0] if ops else None
            raise IndexerFatal(entry.driver_ref, target, exc) from exc
        if policy == FailurePolicy.OUTBOX:
            await self._enqueue_or_warn(entry, ctx, ops, original=exc)
            return
        if policy == FailurePolicy.WARN:
            logger.warning(
                "IndexDispatcher: indexer '%s' failed for bulk batch of %d "
                "(policy=warn): %s",
                entry.driver_ref, len(ops), exc,
            )
            return
        logger.debug(
            "IndexDispatcher: indexer '%s' failed bulk (policy=ignore): %s",
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
        ops: Sequence[DispatchableOp],
        *,
        original: Optional[BaseException] = None,
    ) -> None:
        """Enqueue a batch of ops as chunked outbox rows when configured;
        otherwise degrade to WARN.

        Accepts a list so a 500-item bulk failure becomes one chunked
        ``enqueue`` call rather than 500 per-row writes (see #500).
        """
        if not ops:
            return
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
                    "IndexDispatcher: indexer '%s' failed for %d ops "
                    "(policy=outbox, degraded): %s",
                    entry.driver_ref, len(ops), original,
                )
            return

        # The legacy ``enqueue`` writer expects IndexOp shape; an
        # IndexableOp-only batch is skipped with a single warning rather
        # than hard-failing — the caller's failure policy already chose
        # tolerance.
        enqueue = getattr(self._outbox, "enqueue", None)
        if enqueue is None:
            logger.warning(
                "IndexDispatcher: outbox writer for indexer '%s' has no "
                "``enqueue`` method; cannot enqueue %d ops.",
                entry.driver_ref, len(ops),
            )
            return
        index_ops: List[IndexOp] = [
            cast(IndexOp, o) for o in ops if not isinstance(o, IndexableOp)
        ]
        if not index_ops:
            logger.warning(
                "IndexDispatcher: %d IndexableOp(s) routed to legacy "
                "task-table outbox for indexer '%s' — dropping; wire an "
                "OutboxStore for the bulk path.",
                len(ops), entry.driver_ref,
            )
            return
        try:
            await enqueue(
                indexer_id=entry.driver_ref,
                ctx=ctx,
                ops=index_ops,
                last_error=str(original) if original else None,
            )
        except Exception as enqueue_exc:
            # Outbox itself failed — last-resort WARN.  We do NOT escalate
            # to FATAL here because the upstream caller has already chosen
            # OUTBOX as a tolerant policy; surfacing this as fatal would
            # surprise them.
            logger.error(
                "IndexDispatcher: outbox enqueue failed for indexer '%s' "
                "on %d ops — original error: %s, enqueue error: %s",
                entry.driver_ref, len(ops), original, enqueue_exc,
            )


def _op_payload(op: "DispatchableOp") -> Any:
    return op.payload if hasattr(op, "payload") else None


def _op_entity_id(op: "DispatchableOp") -> str:
    if isinstance(op, IndexableOp):
        return op.item_id or ""
    return op.entity_id


def _op_entity_kind(op: "DispatchableOp") -> Any:
    if isinstance(op, IndexableOp):
        # IndexableOp models the bulk-reindex path which is item-centric.
        return "item"
    return op.entity_type


def _with_payload(op: "DispatchableOp", payload: Any) -> "DispatchableOp":
    """Return a copy of ``op`` carrying the transformed payload.

    Both shapes are frozen — IndexOp uses pydantic ``model_copy``;
    IndexableOp is a frozen dataclass and we fall back to ``dataclasses.replace``.
    """
    if isinstance(op, IndexableOp):
        from dataclasses import replace
        return replace(op, payload=payload)
    return op.model_copy(update={"payload": payload})


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
