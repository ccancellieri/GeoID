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
* **Durable up front** — INDEX-lane entries are async by lane definition
  (see ``modules/storage/routing_config.py``): the dispatcher persists a
  ``tasks.storage`` obligation row in the *same* PG transaction as the
  upstream write, ahead of any indexer call. PG TX commit guarantees
  neither the data nor the obligation-to-index can be lost.  (The
  transactional-outbox PATTERN is unchanged; #1807 moved the durable
  plane from the per-tenant ``_meta.index_outbox`` table to the unified
  ``tasks.storage`` table.)  INDEX entries carry no per-entry failure
  policy — failure handling is structural: a genuinely inline attempt
  (the in-task-run absorption exception below) that fails is logged and
  dropped, since the durable obligation for every other path was already
  written up front, independent of that attempt's outcome.
* **Circuit-broken** — per-indexer-id breaker (Phase 3).  When open, an
  inline attempt short-circuits to a logged drop rather than calling a
  known-unhealthy indexer.

Phases
------

* Phase 1 (this module) — Protocol surface + dispatcher skeleton.  No
  consumer wiring yet; existing event-driven listeners remain in place.
* Phase 2 — durable plane (now ``tasks.storage``, #1807) + drain worker;
  replace the ES driver's event listeners with a direct dispatcher call
  from ``item_service.upsert``.
* Phase 3 — Circuit breaker (Valkey-backed shared state); migrate the
  private/per-catalog geoid-only indexer onto the same protocol.
"""

from __future__ import annotations

import itertools
import logging
from typing import Any, Callable, Dict, List, Optional, Protocol, Sequence, Tuple, Union, cast

from dynastore.models.protocols.indexer import (
    BulkResult,
    Indexer,
    IndexContext,
    IndexOp,
    merge_bulk_results,
)
from dynastore.models.protocols.indexing import IndexableOp
from dynastore.modules.storage.routing_config import OperationDriverEntry
from dynastore.tools.execution_context import current_task_catalog, in_task_run


# Public surface of the dispatcher accepts either the legacy
# ``IndexOp`` (Pydantic, in-process indexers still consume this shape)
# or the new ``IndexableOp`` (frozen dataclass, durable contract for
# outbox + bulk reindex).  Internals branch on the runtime type for the
# few code paths that need the richer ``IndexableOp`` fields
# (``op_id`` / ``driver_instance_id`` / ``idempotency_key``).
DispatchableOp = Union[IndexOp, IndexableOp]

logger = logging.getLogger(__name__)

# Chunk the inline in-task-run dispatch so a large batch never builds
# one oversized driver call.
INLINE_DISPATCH_CHUNK_SIZE = 500

# Monotonic sequence stamped on every inline ``indexer.index_bulk`` call
# (#2494 instrumentation) so a burst of same-second log lines can still be
# ordered and counted per process.
_INDEX_BULK_SEQUENCE = itertools.count(1)


# ---------------------------------------------------------------------------
# Outbox writer — durable retry path backed by the existing ``tasks`` table
# ---------------------------------------------------------------------------


class OutboxWriterProtocol(Protocol):
    """Minimal surface the dispatcher needs from a durable retry queue.

    The default implementation (:class:`StoragePlaneOutboxWriter`) writes
    into the unified ``tasks.storage`` table; callers wanting a different
    persistence (Kafka, SQS, …) implement the same one-method interface.
    """

    async def enqueue(
        self,
        *,
        indexer_id: str,
        ctx: IndexContext,
        ops: Sequence[DispatchableOp],
        last_error: Optional[str] = None,
        chunk_size: Optional[int] = None,
    ) -> None:
        ...


class StoragePlaneOutboxWriter:
    """Outbox backed by the storage-plane ``tasks.storage`` table.

    The ``OUTBOX`` failure-policy handler (un-fao/GeoID#2732 step 1) —
    durable retry via the plane ``storage_drain`` already drains, rather
    than a dedicated ``tasks.tasks`` outbox.

    Writes id-only rows via :func:`~dynastore.modules.storage.storage_emit.enqueue_storage_op_id_only`
    on the caller's PG connection so the enqueue commits / rolls back
    atomically with the upstream data write (same co-transactionality
    contract as the legacy writer). Upsert rows carry no payload — the
    drain re-reads canonical PG state at replay time, which is fresher
    than a payload frozen at enqueue time. Delete rows are written with
    ``op='delete'``: the drain's id-only canonical-reread branch only
    fires for ``op == 'upsert'``, so a delete replays as an actual
    delete, never as a doc rebuild.

    ``chunk_size`` is accepted for :class:`OutboxWriterProtocol`
    compatibility but unused — the storage plane already writes one row
    per op, so there is no oversized-JSONB-blob concern to chunk against.
    ``last_error`` has no dedicated column on ``tasks.storage``; it is
    logged instead of persisted (the row itself is enough for the drain
    to retry — operator triage reads the log line for the reason).
    """

    async def enqueue(
        self,
        *,
        indexer_id: str,
        ctx: IndexContext,
        ops: Sequence[DispatchableOp],
        last_error: Optional[str] = None,
        chunk_size: Optional[int] = None,
    ) -> None:
        if not ops:
            return
        if ctx.pg_conn is None:
            # Without a caller TX we can't honour the atomicity guarantee.
            logger.warning(
                "StoragePlaneOutboxWriter: ctx.pg_conn is None — skipping "
                "outbox enqueue for indexer '%s' on %s (+%d more). "
                "Caller must pass an open PG connection on IndexContext "
                "for the OUTBOX policy to be durable.",
                indexer_id, _describe_op(ops[0]), max(len(ops) - 1, 0),
            )
            return

        from dynastore.modules.storage.storage_emit import (
            enqueue_storage_op_id_only,
            enqueue_storage_op_write_id,
        )

        grouped_records, id_only_records = _build_storage_plane_records(
            driver_id=indexer_id, ctx=ctx, ops=ops,
            write_id_supported=await _primary_supports_write_id_reads(
                ctx.catalog, ctx.collection,
            ),
        )
        total_records = len(grouped_records) + len(id_only_records)
        logger.info(
            "index_chunk_emitted indexer=%s source=storage_plane_outbox "
            "catalog=%s collection=%s chunk_size=%d",
            indexer_id, ctx.catalog, ctx.collection, total_records,
        )
        if last_error:
            logger.warning(
                "StoragePlaneOutboxWriter: enqueueing %d op(s) for indexer "
                "'%s' (catalog=%s collection=%s) after inline failure: %s",
                total_records, indexer_id, ctx.catalog, ctx.collection,
                last_error,
            )
        _log_dispatch_path(
            mode="outbox_handoff",
            indexer_id=indexer_id,
            catalog=ctx.catalog,
            collection=ctx.collection,
            chunk_size=total_records,
        )
        if grouped_records:
            await enqueue_storage_op_write_id(
                ctx.pg_conn, catalog_id=ctx.catalog, rows=grouped_records,
            )
        if id_only_records:
            await enqueue_storage_op_id_only(
                ctx.pg_conn, catalog_id=ctx.catalog, rows=id_only_records,
            )


# ---------------------------------------------------------------------------
# Default factory — wires the dispatcher against the live routing config
# and the protocol-discovery indexer registry
# ---------------------------------------------------------------------------


_DEFAULT_DISPATCHER: Optional["IndexDispatcher"] = None


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
    # `mode` is one of: post_commit_inline | outbox_handoff |
    # partial_failure_drop (per-doc rejections from an otherwise-OK bulk; #2064) |
    # inline_in_task_run (ASYNC entry absorbed inline because the dispatch
    # is already running inside a background task/job execution).
    logger.info(
        "index_dispatch_path mode=%s indexer=%s catalog=%s collection=%s "
        "chunk_size=%d",
        mode, indexer_id, catalog, collection, chunk_size,
    )



async def _storage_plane_routing_enabled() -> bool:
    """Read ``TasksPluginConfig.items_secondary_via_storage_plane``, hot-reloaded.

    Mirrors the resolution pattern in
    ``dynastore.modules.tasks.async_writer_backlog._resolve_threshold``:
    fails open to the field default (``False``) when the platform configs
    protocol is unavailable (early startup, tests) so an unreadable flag
    degrades to the pre-#2494 dispatch path rather than raising.
    """
    try:
        from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
        from dynastore.modules.tasks.tasks_config import TasksPluginConfig
        from dynastore.tools.discovery import get_protocol

        config_mgr = get_protocol(PlatformConfigsProtocol)
        if config_mgr is None:
            return bool(
                TasksPluginConfig.model_fields["items_secondary_via_storage_plane"].default
            )
        cfg = await config_mgr.get_config(TasksPluginConfig)
        if isinstance(cfg, TasksPluginConfig):
            return cfg.items_secondary_via_storage_plane
    except Exception:  # noqa: BLE001 — config read is best-effort
        logger.debug(
            "IndexDispatcher: items_secondary_via_storage_plane flag "
            "unavailable — defaulting to the legacy dispatch path.",
            exc_info=True,
        )
    from dynastore.modules.tasks.tasks_config import TasksPluginConfig
    return bool(TasksPluginConfig.model_fields["items_secondary_via_storage_plane"].default)


async def _resolve_in_task_run_chunk_size() -> int:
    """Read ``TasksPluginConfig.in_task_run_inline_chunk_size``, hot-reloaded.

    Bounds the per-chunk size of the in-run absorption path (#2716) — a job
    container's memory budget was sized for ITS OWN write path, not for
    ``INLINE_DISPATCH_CHUNK_SIZE`` (500) full envelopes on top of it.
    Mirrors the fail-open resolution pattern of
    ``_storage_plane_routing_enabled``: falls back to the field default when
    the platform configs protocol is unavailable (early startup, tests).
    """
    try:
        from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
        from dynastore.modules.tasks.tasks_config import TasksPluginConfig
        from dynastore.tools.discovery import get_protocol

        config_mgr = get_protocol(PlatformConfigsProtocol)
        if config_mgr is None:
            return int(
                TasksPluginConfig.model_fields["in_task_run_inline_chunk_size"].default
            )
        cfg = await config_mgr.get_config(TasksPluginConfig)
        if isinstance(cfg, TasksPluginConfig):
            return cfg.in_task_run_inline_chunk_size
    except Exception:  # noqa: BLE001 — config read is best-effort
        logger.debug(
            "IndexDispatcher: in_task_run_inline_chunk_size unavailable — "
            "falling back to the field default.",
            exc_info=True,
        )
    from dynastore.modules.tasks.tasks_config import TasksPluginConfig
    return int(TasksPluginConfig.model_fields["in_task_run_inline_chunk_size"].default)


def _task_run_absorption_allowed(catalog: str) -> bool:
    """True when the currently running task run may absorb an ASYNC write
    for *catalog* inline instead of enqueuing it to the durable outbox.

    A task run that declared no catalog (``current_task_catalog() is
    None`` — a cross-tenant/global task, or a call site that predates
    catalog-scoped ``task_run_scope``, e.g. the Cloud Run Job entrypoint)
    is treated as unrestricted, preserving the original #2621 behaviour.
    A task run that DID declare a catalog may only absorb writes for that
    SAME catalog — a write for any other catalog is foreign backlog that
    belongs to the async-writer job, not to this job's memory budget
    (#2716).
    """
    scoped = current_task_catalog()
    return scoped is None or scoped == catalog


def _make_default_routing_resolver():
    """Build an entity-aware resolver that loads the live PluginConfig
    matching the dispatched tier via ``ConfigsProtocol.get_config``.

    Per un-fao/GeoID#810 (Option B): the dispatcher is reached from three
    tiers and each must read its own PluginConfig:

    * ``entity_type="item"`` -> :class:`ItemsRoutingConfig` — OGC ingest
      (``item_service._dispatch_index_upsert``) and item delete
      (``item_query``). Carries the privacy-cascade validator's contract
      into runtime: a private collection that pins
      ``items_elasticsearch_private_driver`` in
      ``ItemsRoutingConfig.operations[INDEX]`` now fires on item
      upsert/delete via the OGC endpoints.
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
    ``_self_register_indexers_into`` (routing_config.py). No separate
    ``indexer_id`` attribute.

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
    """Process-wide singleton dispatcher — reuses live resolvers +
    :class:`StoragePlaneOutboxWriter` for the ``OUTBOX`` failure path +
    a per-indexer :class:`CircuitBreaker`.
    """
    global _DEFAULT_DISPATCHER
    if _DEFAULT_DISPATCHER is None:
        from dynastore.modules.storage.circuit_breaker import CircuitBreaker

        _DEFAULT_DISPATCHER = IndexDispatcher(
            routing_resolver=_make_default_routing_resolver(),
            indexer_registry=_make_default_indexer_registry(),
            outbox=StoragePlaneOutboxWriter(),
            breaker=CircuitBreaker(),
        )
    return _DEFAULT_DISPATCHER


async def reset_index_dispatcher() -> None:
    """Test hook — drops the cached singleton so the next
    :func:`get_index_dispatcher` rebuilds with current discovery state.
    """
    global _DEFAULT_DISPATCHER
    _DEFAULT_DISPATCHER = None


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
        Async callable used to look up the INDEX-lane entries in
        ``operations[INDEX]``. The production resolver accepts an
        ``entity_type`` keyword and returns the matching ``*RoutingConfig``
        per tier (items/collection/catalog/asset). Legacy 2-arg
        ``(catalog, collection)`` stubs are still accepted via
        :func:`_call_resolver`'s ``TypeError`` fallback so existing
        fixtures continue to work; pre-#810 callers that don't set
        ``IndexContext.entity_type`` resolve to ``CollectionRoutingConfig``
        (back-compat default). Pluggable so the dispatcher is testable
        without booting the full config service.
    indexer_registry
        Async callable ``(indexer_id) -> Indexer | None`` resolving the
        runtime instance for a routing entry's ``driver_id``.
    outbox
        Optional ``OutboxWriter`` (Phase 2) — when ``None``, obligations
        cannot be durably enqueued and degrade to a one-time WARN log.
    breaker
        Optional ``CircuitBreaker`` (Phase 3) — when ``None``, every
        inline attempt is tried unconditionally.
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
        self,
        ctx: IndexContext,
        ops: Sequence[DispatchableOp],
        *,
        tx_factory: Optional[Callable[[], Any]] = None,
    ) -> Dict[str, BulkResult]:
        """Dispatch a bulk of index ops across every configured indexer.

        Returns per-indexer ``BulkResult`` so callers can surface partial
        failures (a 207-style report).  Per-op failures inside an indexer
        are absorbed into ``BulkResult.failures``; an indexer raising on
        the (rare) inline-dispatch leg logs and drops the whole batch —
        see :meth:`_handle_failure_bulk`.

        ``tx_factory`` (in-task-run inline path only): a zero-arg callable
        returning an ``async with``-able transaction. When supplied, the
        inline chunked dispatch opens a fresh short transaction per chunk
        (bound to that chunk's ``pg_conn``) instead of running the whole
        fan-out under one long-lived ``ctx.pg_conn`` — so a busy job never
        parks a pooled connection with an open transaction across the full
        sequential ES dispatch.
        """
        results: Dict[str, BulkResult] = {}
        entries = await self._index_entries(ctx)
        if ops:
            # #2657 instrumentation — one resolved-entry count per dispatch
            # call so a routing-config regression that fans one batch out
            # across an unexpectedly large entry set is visible in logs
            # (the ×N amplification that drove the ES secondary-write
            # runaway). DEBUG (review finding): this fires on EVERY
            # dispatch call regardless of the #2494 storage-plane flag, so
            # INFO would add unconditional log volume to every deployment.
            logger.debug(
                "index_dispatch_entries catalog=%s collection=%s "
                "entity_type=%s op_count=%d entry_count=%d",
                ctx.catalog, ctx.collection,
                getattr(ctx, "entity_type", None), len(ops), len(entries),
            )
        if ops and not entries:
            # #914 — dispatch-level silent no-op: ops were submitted but no
            # routing entry exists for this (catalog, collection,
            # entity_type), so no indexer runs and the caller receives an
            # empty results dict. Surface it so a misconfigured routing
            # table can't silently swallow writes.
            logger.warning(
                "IndexDispatcher: %d op(s) submitted for catalog=%s "
                "collection=%s entity_type=%s but routing returned NO "
                "INDEX-lane entries — writes will not reach any indexer. "
                "Check RoutingConfig.operations[INDEX] in this scope.",
                len(ops), ctx.catalog, ctx.collection,
                getattr(ctx, "entity_type", None),
            )
        for entry in entries:
            indexer = await self._resolve_indexer(entry.driver_ref)
            if indexer is None:
                # Driver not registered locally — durably enqueue per-op so
                # a configured-but-not-installed indexer is still recognised
                # and routed to the drain (see _handle_missing).
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
            # INDEX-lane entries are async by lane definition: enqueue a
            # durable obligation and skip the inline indexer call entirely.
            # The drain pumps the row in the background; the write path is
            # not blocked on ES.  BulkResult is built from the ACTUAL count
            # returned by the enqueue call — if it drops (no outbox, no
            # pg_conn, transient PG error) the returned 0 flows through as
            # succeeded=0/failed=N so _check_index_health escalates to
            # FAILED instead of silently claiming success.
            # #2494 P1: when the storage-plane flag is on and this is an
            # item-tier entry, ALWAYS route to id-only ``tasks.storage``
            # obligations — regardless of ``in_task_run()``. The drain
            # re-reads canonical PG state at replay time, so there is no
            # snapshot to go stale and no reason to ever absorb the write
            # inline (the in-task-run inline path is exactly the mechanism
            # #2657 traced the ES secondary-write runaway to). Because this
            # branch never falls through to ``_dispatch_bulk_chunked``, the
            # noop-reenqueue (:1044) and partial-failure-resurface (:1054)
            # amplifiers inside ``_dispatch_bulk`` cannot fire for these ops.
            #
            # Access-aware drivers are INCLUDED (#2687): the hub row now
            # persists the write-time owner (``access_owner`` column,
            # ``ItemService._resolve_write_owner``), and
            # ``read_canonical_index_inputs`` recomputes ``_visibility`` /
            # ``_owner`` / ``_attrs`` from that column plus live config
            # (``CatalogLookupAudience`` / ``AttributeStampingPolicy``) —
            # see ``canonical_index_read._resolve_access_context``. The drain
            # enforces the ABAC invariant fail-closed:
            # ``StorageDrainTask._build_canonical_doc`` raises (→ retry,
            # never index) rather than write an access-aware doc whose
            # envelope recompute failed or came back empty. There is
            # therefore no longer a payload requirement that would force an
            # access-aware entry onto a different plane than any other
            # item-tier entry.
            storage_plane_active = (
                ctx.entity_type == "item"
                and await _storage_plane_routing_enabled()
            )
            if storage_plane_active:
                actually_enqueued = await self._enqueue_storage_plane_ids(
                    entry, ctx, entry_ops, tx_factory=tx_factory,
                )
                enqueue_ok = actually_enqueued == len(entry_ops)
                _log_dispatch_path(
                    mode="storage_plane_id_only_enqueued" if enqueue_ok
                    else "storage_plane_enqueue_failed",
                    indexer_id=entry.driver_ref,
                    catalog=ctx.catalog,
                    collection=ctx.collection,
                    chunk_size=len(entry_ops),
                )
                storage_plane_failures: List[Dict[str, Any]] = (
                    [{"reason": "storage_plane_enqueue_failed", "indexer": entry.driver_ref}]
                    if not enqueue_ok else []
                )
                results[entry.driver_ref] = BulkResult(
                    total=len(entry_ops) + len(rejected),
                    succeeded=actually_enqueued,
                    failed=(len(entry_ops) - actually_enqueued) + len(rejected),
                    failures=storage_plane_failures + (rejected if rejected else []),
                )
                continue
            # Exception: when this dispatch is already running inside a
            # background task/job execution (``in_task_run()``), spawning an
            # outbox row per chunk would fan out onto the serving pods that
            # drain it — the write is instead absorbed inline below, in the
            # running job. #2716 narrows the exception two ways so a busy
            # job's memory budget is never spent on work that isn't its own:
            # (a) ``storage_plane_active`` — the flag owner (storage_drain)
            # already claimed item-tier obligations above; an access-aware
            # entry that fell through here still must not be absorbed once
            # the operator has opted into the storage plane; (b)
            # ``_task_run_absorption_allowed`` — a task run that declared
            # its own catalog via ``task_run_scope(catalog=...)`` may only
            # absorb writes for THAT catalog; a write for any other catalog
            # is foreign backlog that belongs to the async-writer job, not
            # to this job's container. A task run with no declared catalog
            # (``current_task_catalog() is None`` — e.g. the Cloud Run Job
            # entrypoint, which predates catalog-scoped ``task_run_scope``)
            # stays unrestricted, matching the original #2621 behaviour.
            if not (
                in_task_run()
                and not storage_plane_active
                and _task_run_absorption_allowed(ctx.catalog)
            ):
                actually_enqueued = await self._enqueue_obligation(entry, ctx, entry_ops)
                enqueue_ok = actually_enqueued == len(entry_ops)
                _log_dispatch_path(
                    mode="async_outbox_enqueued" if enqueue_ok else "async_enqueue_failed",
                    indexer_id=entry.driver_ref,
                    catalog=ctx.catalog,
                    collection=ctx.collection,
                    chunk_size=len(entry_ops),
                )
                enqueue_failures: List[Dict[str, Any]] = (
                    [{"reason": "async_enqueue_failed", "indexer": entry.driver_ref}]
                    if not enqueue_ok else []
                )
                results[entry.driver_ref] = BulkResult(
                    total=len(entry_ops) + len(rejected),
                    succeeded=actually_enqueued,
                    failed=(len(entry_ops) - actually_enqueued) + len(rejected),
                    failures=enqueue_failures + (rejected if rejected else []),
                )
                continue
            # SYNC entries, and ASYNC entries absorbed inline because the
            # dispatch is already running inside a task run, both land here:
            # the write is chunked and dispatched inline through the same
            # driver-agnostic path (silent no-op conversion lives inside
            # ``_dispatch_bulk``, applied per chunk).
            result = await self._dispatch_bulk_chunked(
                entry, indexer, ctx, entry_ops, tx_factory=tx_factory,
            )
            # Only log clean success when the batch genuinely has no failures
            # (placed after no-op conversion so a converted no-op — now
            # failed > 0 — does not appear in the success log).
            if result.failed == 0:
                _log_dispatch_path(
                    mode="post_commit_inline",
                    indexer_id=entry.driver_ref,
                    catalog=ctx.catalog,
                    collection=ctx.collection,
                    chunk_size=len(entry_ops),
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
        from dynastore.modules.storage.routing_config import index_entries

        return index_entries(ops_map)

    async def _handle_missing(
        self,
        entry: OperationDriverEntry,
        ctx: IndexContext,
        op: DispatchableOp,
    ) -> None:
        """Durably enqueue when an INDEX-lane indexer is not locally registered.

        A configured-but-not-locally-installed indexer is still recognised
        and routed to the drain: this call site runs BEFORE the
        proactive-enqueue branches in :meth:`fan_out_bulk` (the driver
        lookup fails before an indexer instance even exists to dispatch
        to), so no obligation has been written yet for this op — unlike the
        failure-handling call sites below, dropping here would silently
        lose the write forever instead of leaving it to drain on a replica
        that does have the driver installed (or a future deploy).  Logs
        once per ``(driver_id, catalog, collection)`` so a deliberately-
        omitted driver doesn't flood the log on every op.
        """
        key = (entry.driver_ref, ctx.catalog, ctx.collection)
        if key not in self._missing_warning_emitted:
            self._missing_warning_emitted.add(key)
            logger.warning(
                "IndexDispatcher: indexer '%s' not registered locally "
                "(catalog=%s, collection=%s) — enqueueing for the drain. "
                "Future occurrences for this triple are suppressed.",
                entry.driver_ref, ctx.catalog, ctx.collection,
            )
        await self._enqueue_obligation(entry, ctx, [op])

    async def _ensure_or_handle(
        self,
        entry: OperationDriverEntry,
        indexer: Indexer,
        ctx: IndexContext,
        op: DispatchableOp,
    ) -> bool:
        """Run ``ensure_indexer`` once per (indexer_id, catalog, collection).

        Returns True when the indexer is ready to receive ops, False when
        the bootstrap failed — logged and dropped (see :meth:`_handle_failure`).
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
            # Breaker is open — the indexer is not healthy enough to attempt
            # an inline call.  This path is only reached via the in-task-run
            # absorption exception (see the module docstring and
            # ``fan_out_bulk``): the caller deliberately chose NOT to write a
            # durable obligation up front for this op, so there is nothing to
            # compensate for reactively here — log and drop.  The item-tier
            # obligation sweep (#2688) is the safety net for exactly this
            # class of gap; it does not depend on this dispatcher retrying.
            logger.warning(
                "IndexDispatcher: circuit breaker open for indexer '%s' "
                "(catalog=%s collection=%s) — dropping %d op(s) inline; "
                "relying on the obligation sweep for eventual consistency.",
                entry.driver_ref, ctx.catalog, ctx.collection, len(ops),
            )
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
            # #2494 instrumentation — reason bucket + op count + monotonic
            # sequence for every inline ``index_bulk`` call. ``_dispatch_bulk``
            # has one call site reached from ``_dispatch_bulk_chunked``
            # (chunked SYNC dispatch, or an ASYNC entry absorbed inline
            # during a task run pre-#2494-flag); "primary" covers it today.
            # The remaining bucket names are reserved for future call sites
            # that resurface a batch through this same path (ensure_retry /
            # noop_reenqueue / partial_failure_resurface / transient_retry) —
            # not yet exercised, so only "primary" is emitted. DEBUG (review
            # finding): fires on every SYNC/inline dispatch regardless of
            # the #2494 flag, so INFO would add unconditional volume.
            logger.debug(
                "index_bulk_call reason=primary indexer=%s catalog=%s "
                "collection=%s op_count=%d seq=%d",
                entry.driver_ref, ctx.catalog, ctx.collection, len(ops),
                next(_INDEX_BULK_SEQUENCE),
            )
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
            # The bulk CALL succeeded, but ES can still reject individual
            # documents (HTTP 200 with per-item errors, e.g.
            # ``invalid_shape_exception``).  Those rejections are absorbed
            # into ``result.failures`` and returned as a count — without this
            # call they leave no per-item signal and no dispatch-path counter
            # (#2064).  Surfaced here, on the success path ONLY: the breaker /
            # ensure / raised branches return their synthetic failures AFTER
            # ``_handle_failure_bulk`` has already applied the on_failure
            # policy, so emitting for those too would double-signal.
            await self._surface_partial_failures(entry, ctx, result)
            # #914 — silent no-op trap: an indexer that returns
            # ``BulkResult(total=N, succeeded=0, failed=0)`` (e.g. ES bulk
            # response shape the driver doesn't parse) was previously
            # indistinguishable from a real success in logs, leaving the
            # target index empty with no warning.  Pure upsert no-ops are
            # converted to observable failures so the count is honest
            # (``BulkResult.failed`` reflects reality); this inline path is
            # only reached via the in-task-run absorption exception (see
            # ``fan_out_bulk``), so — as with the breaker-open branch above —
            # there is no pre-existing obligation to reactively re-enqueue.
            # The item-tier obligation sweep (#2688) is the safety net for
            # this class of gap.  Delete ops are unaffected — they have
            # their own pass-through.
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
                noop_upserts: List[DispatchableOp] = [
                    o for o in ops
                    if (
                        o.op == "upsert"
                        if isinstance(o, IndexableOp)
                        else o.op_type == "upsert"
                    )
                ]
                if noop_upserts:
                    result = BulkResult(
                        total=result.total,
                        succeeded=result.succeeded,
                        failed=result.failed + len(noop_upserts),
                        failures=[
                            *result.failures,
                            *[
                                {
                                    "reason": "silent_noop",
                                    "indexer": entry.driver_ref,
                                    "entity_id": _op_entity_id(o),
                                }
                                for o in noop_upserts
                            ],
                        ],
                    )
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

    async def _dispatch_bulk_chunked(
        self,
        entry: OperationDriverEntry,
        indexer: Indexer,
        ctx: IndexContext,
        ops: Sequence[DispatchableOp],
        *,
        tx_factory: Optional[Callable[[], Any]] = None,
    ) -> BulkResult:
        """Split ``ops`` into ``INLINE_DISPATCH_CHUNK_SIZE``-sized chunks and
        dispatch each one SEQUENTIALLY through :meth:`_dispatch_bulk`,
        aggregating the per-chunk results into one.

        Sequential on purpose — a bounded footprint per chunk is the whole
        point of the inline-dispatch path (both the ordinary SYNC entry and
        an ASYNC entry absorbed inline during a task run land here), so
        chunks are never fanned out concurrently.

        When ``tx_factory`` is supplied (the in-task-run inline path), each
        chunk is dispatched under a FRESH short transaction whose connection
        is stamped onto a per-chunk :class:`IndexContext` copy — the pooled
        connection is held only across that one chunk's dispatch (kept open
        so an on-failure outbox enqueue is still atomic) and released before
        the next chunk. Without it, dispatch runs under the ambient
        ``ctx.pg_conn`` (the serving SYNC path, unchanged).
        """
        if not ops:
            return BulkResult()
        # Distinguish the two callers that land here: an ASYNC entry absorbed
        # inline during a task/job run vs. an ordinary SYNC entry chunked on the
        # post-commit tail. Same code path, different provenance in the logs.
        running_in_task = in_task_run()
        chunk_mode = "inline_in_task_run" if running_in_task else "sync_chunked"
        # #2716: a job container's memory budget is sized for its own write
        # path, not for INLINE_DISPATCH_CHUNK_SIZE (500) full envelopes on
        # top of it. Inside a task run, chunk at the smaller, hot-reloadable
        # ``TasksPluginConfig.in_task_run_inline_chunk_size`` instead — the
        # serving-path SYNC chunk size (this same code path outside a task
        # run) is unaffected.
        chunk_size = (
            await _resolve_in_task_run_chunk_size()
            if running_in_task
            else INLINE_DISPATCH_CHUNK_SIZE
        )
        aggregated = BulkResult()
        for start in range(0, len(ops), chunk_size):
            chunk = ops[start:start + chunk_size]
            if tx_factory is not None:
                async with tx_factory() as chunk_conn:
                    chunk_ctx = ctx.model_copy(update={"pg_conn": chunk_conn})
                    chunk_result = await self._dispatch_bulk(
                        entry, indexer, chunk_ctx, chunk,
                    )
            else:
                chunk_result = await self._dispatch_bulk(entry, indexer, ctx, chunk)
            _log_dispatch_path(
                mode=chunk_mode,
                indexer_id=entry.driver_ref,
                catalog=ctx.catalog,
                collection=ctx.collection,
                chunk_size=len(chunk),
            )
            # Bounded merge (#2657) — an unbounded concat here was one of
            # three uncapped accumulation sites driving peak RSS to
            # O(dataset) instead of O(chunk) on a degraded secondary index.
            aggregated = merge_bulk_results(aggregated, chunk_result)
        return aggregated

    async def _handle_failure(
        self,
        entry: OperationDriverEntry,
        ctx: IndexContext,
        op: DispatchableOp,
        exc: BaseException,
        *,
        bulk: bool = False,
    ) -> None:
        """Log and drop an inline-dispatch failure.

        INDEX entries carry no per-entry failure policy.  This call site is
        only reached via the in-task-run absorption exception (see the
        module docstring), where the caller deliberately skipped the
        proactive durable enqueue — there is nothing to compensate for
        reactively, so failure handling belongs to the drain / the
        item-tier obligation sweep (#2688), not to a retry attempt here.
        """
        descriptor = _describe_op(op)
        logger.warning(
            "IndexDispatcher: indexer '%s' failed for %s%s: %s",
            entry.driver_ref, descriptor,
            ", bulk" if bulk else "", exc,
        )

    async def _surface_partial_failures(
        self,
        entry: OperationDriverEntry,
        ctx: IndexContext,
        result: BulkResult,
    ) -> None:
        """Make per-document rejections from a *successful* bulk call
        observable and attributable (#2064).

        When ``index_bulk`` returns ``BulkResult.failures`` (the bulk HTTP
        call passed but ES rejected individual docs — e.g.
        ``invalid_shape_exception`` on duplicate consecutive coordinates),
        those ops are not retried (poison classification is correct: a retry
        can never succeed) and the primary row stays committed in PG, so the
        item is permanently invisible to this index.  Without a signal the
        writer gets a success and only a PG-vs-index count diff reveals the
        loss.

        Emits, per failed doc, an ``index_failure_persistent`` log event on
        the existing event surface (the same type the outbox drain emits),
        plus one ``index_dispatch_path`` line with ``mode=partial_failure_drop``
        so the #504 ``index_dispatch_path_total`` metric counts the drops.

        Fail-open: the write has already committed; an observability emit must
        never raise into the dispatch path.
        """
        if not result.failures:
            return
        try:
            from dynastore.modules.catalog.log_manager import log_event

            for failure in result.failures:
                item_id = (
                    failure.get("id")
                    or failure.get("_id")
                    or failure.get("entity_id")
                )
                reason = failure.get("reason", "unknown")
                await log_event(
                    catalog_id=ctx.catalog,
                    collection_id=ctx.collection,
                    event_type="index_failure_persistent",
                    level="ERROR",
                    message=(
                        f"Indexer '{entry.driver_ref}' dropped item {item_id}: "
                        f"rejected by the index and not retried"
                    ),
                    details={
                        "driver_id": entry.driver_ref,
                        "item_id": item_id,
                        "reason": reason,
                        "source": "inline_dispatch_partial_bulk",
                        "status": "dropped",
                    },
                    is_system=True,
                )
            _log_dispatch_path(
                mode="partial_failure_drop",
                indexer_id=entry.driver_ref,
                catalog=ctx.catalog,
                collection=ctx.collection,
                chunk_size=len(result.failures),
            )
        except Exception as exc:  # noqa: BLE001 — observability is best-effort
            logger.debug(
                "IndexDispatcher: failed to surface %d partial bulk "
                "failure(s) for indexer '%s' (catalog=%s collection=%s): %s",
                len(result.failures), entry.driver_ref,
                ctx.catalog, ctx.collection, exc,
            )

    async def _handle_failure_bulk(
        self,
        entry: OperationDriverEntry,
        ctx: IndexContext,
        ops: Sequence[DispatchableOp],
        exc: BaseException,
    ) -> None:
        """Log and drop a whole failed bulk batch in one call.

        Mirrors :meth:`_handle_failure`'s rationale — INDEX entries carry no
        per-entry failure policy, and this call site is only reached via
        the in-task-run absorption exception where nothing was proactively
        enqueued to compensate for.  Logs once at batch granularity rather
        than per op so 500-item batches don't fan out logs.
        """
        logger.warning(
            "IndexDispatcher: indexer '%s' failed for bulk batch of %d: %s",
            entry.driver_ref, len(ops), exc,
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
                    "lane": "INDEX",
                    "source": getattr(e, "source", None),
                    "registered": (await self._resolve_indexer(e.driver_ref)) is not None,
                }
                for e in entries
            ],
        }

    async def _enqueue_obligation(
        self,
        entry: OperationDriverEntry,
        ctx: IndexContext,
        ops: Sequence[DispatchableOp],
    ) -> int:
        """Durably enqueue a batch of ops via the wired :class:`OutboxWriterProtocol`.

        The proactive (upfront) enqueue path for every INDEX-lane entry not
        absorbed inline during a task run — see :meth:`fan_out_bulk` — and
        the sole remaining caller of the generic outbox writer seam (the
        item-tier storage-plane path enqueues directly via
        :meth:`_enqueue_storage_plane_ids` instead).

        Returns the count of ops actually handed to the outbox writer (0 on
        any drop path).  The caller uses this count to build ``BulkResult``
        so health-check logic can distinguish "all accepted" from "silently
        dropped".

        Accepts a list so a 500-item batch becomes one chunked ``enqueue``
        call rather than 500 per-row writes (see #500).
        """
        if not ops:
            return 0
        # Drop path (a): no outbox writer wired.
        if self._outbox is None:
            if entry.driver_ref not in self._outbox_warning_emitted:
                self._outbox_warning_emitted.add(entry.driver_ref)
                logger.warning(
                    "IndexDispatcher: indexer '%s' is INDEX-lane but no "
                    "OutboxWriter is wired — obligations cannot be "
                    "durably enqueued.",
                    entry.driver_ref,
                )
            return 0

        # ``OutboxWriterProtocol.enqueue`` accepts the full ``DispatchableOp``
        # union — the wired :class:`StoragePlaneOutboxWriter` builds records
        # from either shape via ``_op_kind``/``_op_entity_id``/``_op_write_id``,
        # the same helpers ``_enqueue_storage_plane_ids`` already relies on.
        enqueue = getattr(self._outbox, "enqueue", None)
        if enqueue is None:
            logger.warning(
                "IndexDispatcher: outbox writer for indexer '%s' has no "
                "``enqueue`` method; cannot enqueue %d ops.",
                entry.driver_ref, len(ops),
            )
            return 0
        # Drop path (b): caller has no open PG connection.
        # ``StoragePlaneOutboxWriter.enqueue`` checks this too but returns
        # silently without raising — catch it here so the caller receives
        # the real failed count rather than a false success.
        if ctx.pg_conn is None:
            logger.warning(
                "IndexDispatcher: cannot enqueue %d op(s) for indexer '%s' — "
                "ctx.pg_conn is None (no open TX); ops dropped. "
                "Caller must supply an open PG connection for durable enqueue.",
                len(ops), entry.driver_ref,
            )
            return 0
        try:
            await enqueue(indexer_id=entry.driver_ref, ctx=ctx, ops=ops)
            return len(ops)
        except Exception as enqueue_exc:
            # Drop path (c): transient PG error during enqueue.
            logger.error(
                "IndexDispatcher: obligation enqueue failed for indexer "
                "'%s' on %d ops: %s",
                entry.driver_ref, len(ops), enqueue_exc,
            )
            return 0

    async def _enqueue_storage_plane_ids(
        self,
        entry: OperationDriverEntry,
        ctx: IndexContext,
        ops: Sequence[DispatchableOp],
        *,
        tx_factory: Optional[Callable[[], Any]] = None,
    ) -> int:
        """Enqueue ``tasks.storage`` obligations for the storage plane.

        Used for item-tier ASYNC secondary-index entries when
        ``TasksPluginConfig.items_secondary_via_storage_plane`` is enabled.
        Prefers ``ctx.pg_conn`` when the caller already has an open
        transaction (the serving-path wrapping TX
        ``ItemService._dispatch_index_upsert`` opens around the fan-out
        call) so the enqueue is co-transactional with that TX; falls back to
        opening a short transaction via ``tx_factory`` for the in-task-run
        inline path. Returns the count of ops actually enqueued (0 on any
        drop path), mirroring :meth:`_enqueue_obligation`'s health-check
        contract so ``BulkResult.succeeded`` reflects reality.
        """
        if not ops:
            return 0
        from dynastore.modules.storage.storage_emit import (
            enqueue_storage_op_id_only,
            enqueue_storage_op_write_id,
        )
        grouped_records, id_only_records = _build_storage_plane_records(
            driver_id=entry.driver_ref, ctx=ctx, ops=ops,
            write_id_supported=await _primary_supports_write_id_reads(
                ctx.catalog, ctx.collection,
            ),
        )
        total_records = len(grouped_records) + len(id_only_records)

        if ctx.pg_conn is not None:
            try:
                if grouped_records:
                    await enqueue_storage_op_write_id(
                        ctx.pg_conn, catalog_id=ctx.catalog, rows=grouped_records,
                    )
                if id_only_records:
                    await enqueue_storage_op_id_only(
                        ctx.pg_conn, catalog_id=ctx.catalog, rows=id_only_records,
                    )
                return total_records
            except Exception as exc:  # noqa: BLE001 — degrade like _enqueue_obligation
                logger.error(
                    "IndexDispatcher: storage-plane enqueue failed "
                    "for indexer '%s' (catalog=%s collection=%s): %s",
                    entry.driver_ref, ctx.catalog, ctx.collection, exc,
                )
                return 0

        if tx_factory is not None:
            try:
                async with tx_factory() as conn:
                    if grouped_records:
                        await enqueue_storage_op_write_id(
                            conn, catalog_id=ctx.catalog, rows=grouped_records,
                        )
                    if id_only_records:
                        await enqueue_storage_op_id_only(
                            conn, catalog_id=ctx.catalog, rows=id_only_records,
                        )
                return total_records
            except Exception as exc:  # noqa: BLE001 — degrade like _enqueue_obligation
                logger.error(
                    "IndexDispatcher: storage-plane enqueue (short "
                    "TX) failed for indexer '%s' (catalog=%s collection=%s): "
                    "%s",
                    entry.driver_ref, ctx.catalog, ctx.collection, exc,
                )
                return 0

        logger.warning(
            "IndexDispatcher: cannot enqueue %d storage-plane op(s) "
            "for indexer '%s' — no open PG connection and no tx_factory "
            "supplied. Caller must supply one for durable enqueue.",
            total_records, entry.driver_ref,
        )
        return 0


def _op_payload(op: "DispatchableOp") -> Any:
    return op.payload if hasattr(op, "payload") else None


def _op_write_id(op: "DispatchableOp") -> Optional[str]:
    write_id = getattr(op, "write_id", None)
    return write_id if isinstance(write_id, str) and write_id else None


async def _primary_supports_write_id_reads(
    catalog_id: str,
    collection_id: Optional[str],
) -> bool:
    """True when the collection's primary WRITE driver can hydrate
    write-id ledger rows (#3116 routing guard).

    Mirrors ``StorageDrainTask._resolve_primary_write_source`` — the drain
    hydrates from the FIRST resolved WRITE driver, so write-id rows must
    only be enqueued when that driver exposes the write-id chunk-read
    capability. Any resolution failure counts as unsupported: the caller
    falls back to id-only rows, which re-read canonical PG state and are
    always hydratable.
    """
    from dynastore.modules.storage.router import get_write_drivers
    from dynastore.modules.storage.storage_emit import (
        driver_supports_write_id_reads,
    )

    try:
        resolved = await get_write_drivers(catalog_id, collection_id)
    except Exception:  # noqa: BLE001 — resolution failure = no capability
        return False
    if not resolved:
        return False
    return driver_supports_write_id_reads(getattr(resolved[0], "driver", None))


def _build_storage_plane_records(
    *,
    driver_id: str,
    ctx: IndexContext,
    ops: Sequence[DispatchableOp],
    write_id_supported: bool = True,
) -> Tuple[List[Any], List[Any]]:
    from dynastore.models.protocols.indexing import OutboxRecord, WriteIdOutboxRecord
    from dynastore.modules.storage.driver_instance_id import (
        compute_driver_instance_id,
    )
    from dynastore.tools.identifiers import generate_uuidv7

    collection_id = ctx.collection or ""
    driver_instance_id = compute_driver_instance_id(
        driver_id, ctx.catalog, collection_id,
    )
    grouped_records: List[WriteIdOutboxRecord] = []
    id_only_records: List[OutboxRecord] = []
    for op in ops:
        write_id = _op_write_id(op) if write_id_supported else None
        if write_id is not None:
            grouped_records.append(
                WriteIdOutboxRecord(
                    op_id=generate_uuidv7(),
                    driver_id=driver_id,
                    driver_instance_id=driver_instance_id,
                    collection_id=collection_id,
                    op=cast(Any, _op_kind(op)),
                    write_id=write_id,
                    idempotency_key=write_id,
                ),
            )
            continue
        entity_id = _op_entity_id(op)
        id_only_records.append(
            OutboxRecord(
                op_id=generate_uuidv7(),
                driver_id=driver_id,
                driver_instance_id=driver_instance_id,
                collection_id=collection_id,
                op=cast(Any, _op_kind(op)),
                item_id=entity_id,
                idempotency_key=entity_id,
            ),
        )
    return grouped_records, id_only_records


def _op_entity_id(op: "DispatchableOp") -> str:
    if isinstance(op, IndexableOp):
        return op.item_id or ""
    return op.entity_id


def _op_entity_kind(op: "DispatchableOp") -> Any:
    if isinstance(op, IndexableOp):
        # IndexableOp models the bulk-reindex path which is item-centric.
        return "item"
    return op.entity_type


def _op_kind(op: "DispatchableOp") -> str:
    """``"upsert"`` or ``"delete"``, regardless of which op shape is used."""
    if isinstance(op, IndexableOp):
        return op.op
    return op.op_type


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
