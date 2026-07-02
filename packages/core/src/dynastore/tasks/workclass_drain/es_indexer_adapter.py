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

"""``ESBulkIndexer`` — adapt :class:`ItemsElasticsearchDriver`'s
ES bulk-write surface to the :class:`BulkIndexer` Protocol.

The adapter translates a batch of :class:`IndexableOp` to an ES
``_bulk`` request, dispatches it via the driver's resolved async
client, and partitions the per-row response into
:class:`BulkIndexResult` ``passed`` / ``transient`` / ``poison``
buckets so the storage drain task can mark each row appropriately.

Classification rules
--------------------

* ``2xx`` and ``error`` absent → ``passed``.
* ``429 Too Many Requests`` → ``transient`` (rate-limited; retry).
* ``5xx`` or ``error.type`` in :data:`_TRANSIENT_ERROR_TYPES`
  (e.g. ``es_rejected_execution_exception``,
  ``cluster_block_exception``, ``circuit_breaking_exception``,
  ``node_not_connected_exception``) → ``transient``.
* ``error.type`` in :data:`_POISON_ERROR_TYPES`
  (``mapper_parsing_exception``, ``illegal_argument_exception``,
  ``version_conflict_engine_exception``,
  ``document_missing_exception``, ``type_missing_exception``,
  ``invalid_shape_exception``) or any other ``4xx`` (non-429) → ``poison``.
* Connection-level exception raised by the client → every op in the
  batch lands in ``transient`` with a single shared reason; nothing
  reached the cluster, so retry is the right policy.
* Anything that doesn't fit the above (unknown error type with a 2xx
  status, unrecognised structure) → ``transient`` (conservative
  default — don't poison rows on classifier ambiguity).

Drain-path parity with inline/rebuild writes (#2769)
------------------------------------------------------
The inline (``write_entities``) and rebuild (bulk reindex, which calls
``write_entities``) write paths apply byte-budget geometry simplification
and ensure the tenant items index exists (correct mapping + alias) before
writing. This adapter bypassed both, so a drain-only write to an ES-primary
collection could land on a dynamically-mapped index (breaking every
subsequent term-filtered search) and an oversized geometry that inline
writes would have shrunk instead reached ES unsimplified. Both gaps are
closed here: :meth:`ESBulkIndexer._ensure_storage_once` mirrors
``ensure_storage`` (at most once per catalog per adapter instance —
adapters are memoised per drain run) and each upsert payload is run through
:func:`~dynastore.modules.storage.drivers.elasticsearch.maybe_simplify_for_es`
before it is added to the bulk body. A poison-classified rejection is also
retried through the geometry degradation ladder (see
:mod:`dynastore.modules.elasticsearch.geo_shape_ladder`) before it is
reported to the drain as a failure.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple, cast
from uuid import UUID

from dynastore.models.protocols.indexing import (
    BulkIndexResult,
    IndexableOp,
)
from dynastore.modules.elasticsearch.bulk_classify import classify_bulk_response
from dynastore.modules.elasticsearch.geo_shape_ladder import retry_doc_with_ladder

logger = logging.getLogger(__name__)


class ESBulkIndexer:
    """Wrap :class:`ItemsElasticsearchDriver` to expose the
    :class:`BulkIndexer` Protocol.

    The driver instance is passed in at construction so the adapter
    can resolve the active ES client (via
    :func:`dynastore.modules.storage.drivers.elasticsearch._es_client_required`)
    and the per-tenant items index name (via
    :func:`dynastore.modules.storage.drivers.elasticsearch._tenant_items_index`).
    The adapter does not touch the driver's higher-level
    ``write_entities`` path — drain tasks operate on pre-serialised
    STAC item payloads emitted by the dispatcher.
    """

    preferred_chunk_size: int = 1500

    def __init__(self, driver: Any) -> None:
        self._driver = driver
        # Per-adapter-instance memoisation (#2769): adapters are cached per
        # driver_id for the lifetime of one drain run (see
        # ``StorageDrainTask._resolve_indexer``), so these bound the extra
        # ES round trips this parity fix introduces to at most one
        # ``ensure_storage`` call and one simplify-config resolution per
        # (catalog[, collection]) per run rather than per batch.
        self._storage_ensured: set[str] = set()
        self._simplify_cache: "Dict[Tuple[str, str], Tuple[bool, int, bool, float]]" = {}

    async def index_bulk(self, ops: Sequence[IndexableOp]) -> BulkIndexResult:
        if not ops:
            return BulkIndexResult(passed=[], transient=[], poison=[])

        # Drain-path parity (#2769): ensure the tenant items index exists
        # with the correct mapping before writing — the inline/rebuild write
        # paths do this via ``write_entities``'s ``ensure_storage`` call;
        # this adapter bypassed it entirely.
        await self._ensure_storage_once(ops)

        # ES `_bulk` body: alternating action / source rows for index
        # ops; action-only for delete ops.  ``op_index_map`` parallels
        # the actions so we can correlate per-row results back to the
        # original :class:`IndexableOp` regardless of which kind it is.
        bulk_body: List[Any] = []
        op_index_map: List[IndexableOp] = []
        doc_by_key: Dict[str, Dict[str, Any]] = {}
        catalogs_in_batch: set[str] = set()

        for op in ops:
            index_name = self._index_name(op.catalog_id)
            catalogs_in_batch.add(op.catalog_id)
            if op.op == "delete":
                bulk_body.append({
                    "delete": {
                        "_index": index_name,
                        "_id": op.idempotency_key,
                        "routing": op.collection_id,
                    },
                })
            else:  # upsert — index op overwrites; safe given a deterministic _id
                # Drain-path parity (#2769): apply the same byte-budget
                # geometry simplification the inline/rebuild write paths
                # apply via ``write_entities`` — this adapter previously
                # wrote ``op.payload`` verbatim, so an oversized geometry
                # that inline writes would have shrunk to fit the ES
                # per-document limit instead reached ES unsimplified here.
                payload = await self._simplify_payload(op)
                doc_by_key[op.idempotency_key] = payload
                bulk_body.append({
                    "index": {
                        "_index": index_name,
                        "_id": op.idempotency_key,
                        "routing": op.collection_id,
                    },
                })
                bulk_body.append(payload)
            op_index_map.append(op)

        # Add each touched per-tenant index to the platform public alias.
        # Idempotent — already-aliased indices skip cheaply ES-side.
        from dynastore.modules.elasticsearch.aliases import (
            add_index_to_public_alias,
        )
        for catalog_id in catalogs_in_batch:
            await add_index_to_public_alias(self._index_name(catalog_id))

        es = self._get_client()

        try:
            # opensearch-py signature: ``bulk(*, body, ...)`` — NOT the
            # elasticsearch-py 8.x ``operations=`` keyword.
            response = await es.bulk(body=bulk_body)
        except Exception as exc:  # noqa: BLE001 — connection-level fail = retry all
            reason = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "ESBulkIndexer: whole-batch bulk call failed before/at the "
                "wire — %d op(s) funnelled to transient retry: %s",
                len(op_index_map),
                reason,
            )
            return BulkIndexResult(
                passed=[],
                transient=[(op.op_id, reason) for op in op_index_map],
                poison=[],
            )

        return await self._classify_response(op_index_map, response, doc_by_key)

    # ------------------------------------------------------------------
    # Drain-path parity helpers (#2769)
    # ------------------------------------------------------------------

    async def _ensure_storage_once(self, ops: Sequence[IndexableOp]) -> None:
        """Call the driver's ``ensure_storage`` once per catalog per run.

        No-op when the wrapped driver does not expose ``ensure_storage``
        (e.g. a test double) — this adapter degrades to its pre-#2769
        behaviour rather than raising, matching the get-or-skip pattern
        already used by :meth:`_index_name` / :meth:`_get_client`.
        """
        ensure_storage = getattr(self._driver, "ensure_storage", None)
        if ensure_storage is None:
            return
        for catalog_id in {op.catalog_id for op in ops}:
            if catalog_id in self._storage_ensured:
                continue
            try:
                await ensure_storage(catalog_id)
            except Exception as exc:  # noqa: BLE001 — best-effort, matches ensure_storage's own internal degrade-safe handling
                logger.warning(
                    "ESBulkIndexer: ensure_storage failed for catalog=%s: %s "
                    "— continuing; ES may auto-create the index with dynamic "
                    "mappings on first write.",
                    catalog_id, exc,
                )
            self._storage_ensured.add(catalog_id)

    async def _resolve_simplify_config(
        self, catalog_id: str, collection_id: str,
    ) -> "Tuple[bool, int, bool, float]":
        """Resolve ``(simplify, max_bytes, snap_to_grid, snap_grid_size)``.

        Memoised per ``(catalog_id, collection_id)`` for this adapter
        instance. Falls back to "no simplification" when the wrapped driver
        does not expose the resolver methods (test doubles).
        """
        key = (catalog_id, collection_id)
        cached = self._simplify_cache.get(key)
        if cached is not None:
            return cached

        resolve_simplify = getattr(self._driver, "_resolve_simplify_geometry", None)
        if resolve_simplify is None:
            from dynastore.tools.geometry_simplify import (
                DEFAULT_MAX_BYTES, DEFAULT_SNAP_GRID_SIZE,
            )
            result = (False, DEFAULT_MAX_BYTES, False, DEFAULT_SNAP_GRID_SIZE)
            self._simplify_cache[key] = result
            return result

        simplify = await resolve_simplify(catalog_id, collection_id)
        max_bytes = await self._driver._resolve_simplify_max_bytes(catalog_id, collection_id)
        snap_to_grid, snap_grid_size = await self._driver._resolve_snap_to_grid_config(
            catalog_id, collection_id,
        )
        result = (simplify, max_bytes, snap_to_grid, snap_grid_size)
        self._simplify_cache[key] = result
        return result

    async def _simplify_payload(self, op: IndexableOp) -> Dict[str, Any]:
        """Return a byte-budget-simplified copy of ``op.payload``."""
        from dynastore.modules.storage.drivers.elasticsearch import (
            _apply_geometry_simplification,
            maybe_simplify_for_es,
        )

        simplify, max_bytes, snap_to_grid, snap_grid_size = (
            await self._resolve_simplify_config(op.catalog_id, op.collection_id)
        )
        payload, factor, mode = maybe_simplify_for_es(
            dict(op.payload), simplify=simplify, max_bytes=max_bytes,
            snap_to_grid=snap_to_grid, snap_grid_size=snap_grid_size,
        )
        _apply_geometry_simplification(payload, factor, mode)
        return payload

    # ------------------------------------------------------------------
    # Driver-resolution helpers
    # ------------------------------------------------------------------

    def _index_name(self, catalog_id: str) -> str:
        """Resolve the per-tenant items index name."""
        helper = getattr(self._driver, "_get_index_name", None) or getattr(
            self._driver, "get_index_name", None,
        )
        if helper is not None:
            return cast(str, helper(catalog_id))
        from dynastore.modules.storage.drivers.elasticsearch import (
            _tenant_items_index,
        )
        return _tenant_items_index(catalog_id)

    def _get_client(self) -> Any:
        """Resolve the async ES client."""
        getter = getattr(self._driver, "_get_client", None)
        if getter is not None:
            return getter()
        from dynastore.modules.storage.drivers.elasticsearch import (
            _es_client_required,
        )
        return _es_client_required()

    # ------------------------------------------------------------------
    # Per-row classification
    # ------------------------------------------------------------------

    async def _classify_response(
        self,
        op_index_map: Sequence[IndexableOp],
        response: dict,
        doc_by_key: Dict[str, Dict[str, Any]],
    ) -> BulkIndexResult:
        """Translate an ES bulk response into a :class:`BulkIndexResult`.

        Poison-classified rejections are retried through the geometry
        degradation ladder (#2769) before being reported: a doc that lands
        on a coarser rung is WARNING-logged and folded into ``passed``
        instead of ``poison``, so a geo_shape rejection the ladder can
        recover from never reaches the drain's dead-letter path.
        """
        str_ids = [op.idempotency_key for op in op_index_map]
        op_by_key = {op.idempotency_key: op for op in op_index_map}

        passed_ids, transient_pairs, poison_pairs = classify_bulk_response(
            response, str_ids,
        )

        recovered_ids: List[str] = []
        remaining_poison_pairs: List[Tuple[str, str]] = []
        if poison_pairs:
            es = self._get_client()
            for sid, reason in poison_pairs:
                op = op_by_key.get(sid)
                doc = doc_by_key.get(sid)
                recovered = False
                rung: Optional[str] = None
                if op is not None and doc is not None:
                    recovered, rung = await retry_doc_with_ladder(
                        es,
                        index_name=self._index_name(op.catalog_id),
                        doc_id=sid,
                        doc=doc,
                        reason=reason,
                        routing=op.collection_id,
                    )
                if recovered:
                    logger.warning(
                        "ESBulkIndexer: doc id=%s recovered on degraded "
                        "geometry rung=%s after rejection (%s)",
                        sid, rung, reason,
                    )
                    recovered_ids.append(sid)
                else:
                    remaining_poison_pairs.append((sid, reason))

        passed_ids = passed_ids + recovered_ids

        passed: List[UUID] = [op_by_key[sid].op_id for sid in passed_ids if sid in op_by_key]
        transient: List[Tuple[UUID, str]] = [
            (op_by_key[sid].op_id, reason)
            for sid, reason in transient_pairs
            if sid in op_by_key
        ]
        poison: List[Tuple[UUID, str]] = [
            (op_by_key[sid].op_id, reason)
            for sid, reason in remaining_poison_pairs
            if sid in op_by_key
        ]
        return BulkIndexResult(passed=passed, transient=transient, poison=poison)
