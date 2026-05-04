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
from typing import Any, Dict, List, Optional, Protocol, Sequence

from dynastore.models.protocols.indexer import (
    BulkResult,
    Indexer,
    IndexContext,
    IndexOp,
)
from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    Operation,
    OperationDriverEntry,
    WriteMode,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class IndexerFatal(Exception):
    """Raised by the dispatcher when an indexer with ``FailurePolicy.FATAL``
    fails.  Propagates out of the dispatcher; the caller's PG transaction
    rolls back, taking the upstream data write with it.
    """

    def __init__(self, indexer_id: str, op: IndexOp, original: BaseException) -> None:
        super().__init__(
            f"Fatal indexer failure: '{indexer_id}' on "
            f"{op.op_type}/{op.entity_type}/{op.entity_id}: {original}"
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
        try:
            from sqlalchemy import text
            from sqlalchemy.ext.asyncio import AsyncConnection
            if isinstance(conn, AsyncConnection):
                await conn.execute(text(sql), params)
                return
        except Exception:
            pass

        # asyncpg path — translate :name placeholders to $N positional args.
        sql_pg, args = _bind_named_to_positional(sql, params)
        # conn is asyncpg.Connection (or compatible) at this point; the
        # isinstance branch above already returned for SQLAlchemy.
        asyncpg_conn: Any = conn
        await asyncpg_conn.execute(sql_pg, *args)


# ---------------------------------------------------------------------------
# Default factory — wires the dispatcher against the live routing config
# and the protocol-discovery indexer registry
# ---------------------------------------------------------------------------


_DEFAULT_DISPATCHER: Optional[IndexDispatcher] = None


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
    """Process-wide singleton dispatcher — reuses live resolvers + the
    default :class:`TaskTableOutboxWriter`.  Phase 3 plugs in the
    circuit breaker.
    """
    global _DEFAULT_DISPATCHER
    if _DEFAULT_DISPATCHER is None:
        _DEFAULT_DISPATCHER = IndexDispatcher(
            routing_resolver=_make_default_routing_resolver(),
            indexer_registry=_make_default_indexer_registry(),
            outbox=TaskTableOutboxWriter(),
        )
    return _DEFAULT_DISPATCHER


def reset_index_dispatcher() -> None:
    """Test hook — drops the cached singleton so the next
    :func:`get_index_dispatcher` rebuilds with current discovery state.
    """
    global _DEFAULT_DISPATCHER
    _DEFAULT_DISPATCHER = None


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
        # ``ensure_indexer`` is idempotent on the driver side, but we
        # cache success here to avoid the per-call ES ``indices.exists``
        # round-trip.  Keyed by ``(indexer_id, catalog, collection)``;
        # the set survives for the dispatcher's process lifetime.
        self._ensured: set = set()

    # ------------------------------------------------------------------
    # Public surface — single-op and bulk
    # ------------------------------------------------------------------

    async def fan_out(self, ctx: IndexContext, op: IndexOp) -> None:
        """Dispatch a single index op across every configured indexer
        for ``(ctx.catalog, ctx.collection)``.

        Each indexer's outcome is governed by its routing entry's
        :attr:`OperationDriverEntry.on_failure` policy — the dispatcher
        does not stop on a single non-FATAL failure.
        """
        entries = await self._index_entries(ctx)
        for entry in entries:
            await self._dispatch_one(entry, ctx, op)

    async def fan_out_bulk(
        self, ctx: IndexContext, ops: Sequence[IndexOp],
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
            indexer = await self._resolve_indexer(entry.driver_id)
            if indexer is None:
                continue
            results[entry.driver_id] = await self._dispatch_bulk(
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
        op: IndexOp,
    ) -> None:
        indexer = await self._resolve_indexer(entry.driver_id)
        if indexer is None:
            logger.debug(
                "IndexDispatcher: no runtime instance for indexer '%s' — skipping.",
                entry.driver_id,
            )
            return

        # OUTBOX_ONLY shortcut: never attempt sync, always enqueue.
        if entry.write_mode == WriteMode.ASYNC and entry.on_failure == FailurePolicy.OUTBOX:
            await self._enqueue_or_warn(entry, ctx, op)
            return

        # Circuit breaker check (Phase 3 — no-op when self._breaker is None).
        if self._breaker is not None and self._breaker.is_open(entry.driver_id):
            if entry.on_failure == FailurePolicy.OUTBOX:
                await self._enqueue_or_warn(entry, ctx, op)
            elif entry.on_failure == FailurePolicy.FATAL:
                raise IndexerFatal(
                    entry.driver_id, op,
                    RuntimeError("circuit breaker open"),
                )
            else:
                logger.debug(
                    "IndexDispatcher: breaker open for '%s' — skipping (%s).",
                    entry.driver_id, entry.on_failure,
                )
            return

        # Idempotent bootstrap of the indexer's storage (per-process cache).
        if not await self._ensure_or_handle(entry, indexer, ctx, op):
            return

        # Synchronous in-process attempt.
        try:
            await indexer.index(ctx, op)
            if self._breaker is not None:
                self._breaker.record_success(entry.driver_id)
        except Exception as exc:
            if self._breaker is not None:
                self._breaker.record_failure(entry.driver_id)
            await self._handle_failure(entry, ctx, op, exc)

    async def _ensure_or_handle(
        self,
        entry: OperationDriverEntry,
        indexer: Indexer,
        ctx: IndexContext,
        op: IndexOp,
    ) -> bool:
        """Run ``ensure_indexer`` once per (indexer_id, catalog, collection).

        Returns True when the indexer is ready to receive ops, False when
        the bootstrap failed and the dispatcher already routed the op
        through its FailurePolicy (e.g. enqueued to outbox, logged WARN).
        Raises :class:`IndexerFatal` when the policy is FATAL.
        """
        key = (entry.driver_id, ctx.catalog, ctx.collection)
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
        ops: Sequence[IndexOp],
    ) -> BulkResult:
        if self._breaker is not None and self._breaker.is_open(entry.driver_id):
            return BulkResult(total=len(ops), failed=len(ops), failures=[
                {"reason": "circuit_breaker_open", "indexer": entry.driver_id},
            ])
        # Bootstrap once per (indexer, catalog, collection).  Use the
        # first op as the failure-handling target if ensure raises.
        if ops and not await self._ensure_or_handle(entry, indexer, ctx, ops[0]):
            return BulkResult(total=len(ops), failed=len(ops), failures=[
                {"reason": "ensure_indexer_failed", "indexer": entry.driver_id},
            ])
        try:
            result = await indexer.index_bulk(ctx, ops)
            if self._breaker is not None:
                self._breaker.record_success(entry.driver_id)
            return result
        except Exception as exc:
            if self._breaker is not None:
                self._breaker.record_failure(entry.driver_id)
            # Bulk failure: apply policy to the whole batch.
            for op in ops:
                await self._handle_failure(entry, ctx, op, exc, bulk=True)
            return BulkResult(
                total=len(ops),
                failed=len(ops),
                failures=[{"reason": str(exc), "indexer": entry.driver_id}],
            )

    async def _handle_failure(
        self,
        entry: OperationDriverEntry,
        ctx: IndexContext,
        op: IndexOp,
        exc: BaseException,
        *,
        bulk: bool = False,
    ) -> None:
        policy = entry.on_failure
        if policy == FailurePolicy.FATAL:
            raise IndexerFatal(entry.driver_id, op, exc) from exc
        if policy == FailurePolicy.OUTBOX:
            await self._enqueue_or_warn(entry, ctx, op, original=exc)
            return
        if policy == FailurePolicy.WARN:
            logger.warning(
                "IndexDispatcher: indexer '%s' failed for %s/%s/%s "
                "(policy=warn%s): %s",
                entry.driver_id, op.op_type, op.entity_type, op.entity_id,
                ", bulk" if bulk else "", exc,
            )
            return
        # IGNORE — silent skip
        logger.debug(
            "IndexDispatcher: indexer '%s' failed (policy=ignore): %s",
            entry.driver_id, exc,
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
                    "indexer_id": e.driver_id,
                    "write_mode": e.write_mode,
                    "on_failure": e.on_failure,
                    "source": getattr(e, "source", None),
                    "registered": (await self._resolve_indexer(e.driver_id)) is not None,
                }
                for e in entries
            ],
        }

    async def _enqueue_or_warn(
        self,
        entry: OperationDriverEntry,
        ctx: IndexContext,
        op: IndexOp,
        *,
        original: Optional[BaseException] = None,
    ) -> None:
        """Enqueue an outbox row when configured; otherwise degrade to WARN.

        Phase 1 ships without an outbox writer — this method emits a
        one-time warning per indexer and falls through to WARN-level
        log of the original exception.  Phase 2 wires the real writer.
        """
        if self._outbox is None:
            if entry.driver_id not in self._outbox_warning_emitted:
                self._outbox_warning_emitted.add(entry.driver_id)
                logger.warning(
                    "IndexDispatcher: indexer '%s' has on_failure=outbox but "
                    "no OutboxWriter is wired — failures degrade to WARN. "
                    "Phase 2 will activate durable retry.",
                    entry.driver_id,
                )
            if original is not None:
                logger.warning(
                    "IndexDispatcher: indexer '%s' failed for %s/%s/%s "
                    "(policy=outbox, degraded): %s",
                    entry.driver_id, op.op_type, op.entity_type,
                    op.entity_id, original,
                )
            return

        try:
            await self._outbox.enqueue(
                indexer_id=entry.driver_id,
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
                "on %s/%s/%s — original error: %s, enqueue error: %s",
                entry.driver_id, op.op_type, op.entity_type, op.entity_id,
                original, enqueue_exc,
            )
