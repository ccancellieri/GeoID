"""``ESBulkIndexer`` — adapt :class:`ItemsElasticsearchDriver`'s
ES bulk-write surface to the :class:`BulkIndexer` Protocol.

The adapter translates a batch of :class:`IndexableOp` to an ES
``_bulk`` request, dispatches it via the driver's resolved async
client, and partitions the per-row response into
:class:`BulkIndexResult` ``passed`` / ``transient`` / ``poison``
buckets so the outbox drain task can mark each row appropriately.

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
"""
from __future__ import annotations

from typing import Any, List, Sequence, Tuple, cast
from uuid import UUID

from dynastore.models.protocols.indexing import (
    BulkIndexResult,
    IndexableOp,
)
from dynastore.modules.elasticsearch.bulk_classify import classify_bulk_response


class ESBulkIndexer:
    """Wrap :class:`ItemsElasticsearchDriver` to expose the
    :class:`BulkIndexer` Protocol.

    The driver instance is passed in at construction so the adapter
    can resolve the active ES client (via
    :func:`dynastore.modules.storage.drivers.elasticsearch._es_client_required`)
    and the per-tenant items index name (via
    :func:`dynastore.modules.storage.drivers.elasticsearch._tenant_items_index`).
    The adapter does not touch the driver's higher-level
    ``write_entities`` path — outbox drain operates on pre-serialised
    STAC item payloads emitted by the dispatcher.
    """

    preferred_chunk_size: int = 1500

    def __init__(self, driver: Any) -> None:
        self._driver = driver

    async def index_bulk(self, ops: Sequence[IndexableOp]) -> BulkIndexResult:
        if not ops:
            return BulkIndexResult(passed=[], transient=[], poison=[])

        # ES `_bulk` body: alternating action / source rows for index
        # ops; action-only for delete ops.  ``op_index_map`` parallels
        # the actions so we can correlate per-row results back to the
        # original :class:`IndexableOp` regardless of which kind it is.
        bulk_body: List[Any] = []
        op_index_map: List[IndexableOp] = []
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
                bulk_body.append({
                    "index": {
                        "_index": index_name,
                        "_id": op.idempotency_key,
                        "routing": op.collection_id,
                    },
                })
                bulk_body.append(op.payload)
            op_index_map.append(op)

        # Add each touched per-tenant index to the platform public alias.
        # Idempotent — already-aliased indices skip cheaply ES-side. Without
        # this, ES auto-creates the per-tenant index on first bulk write
        # (drain path doesn't go through ItemsElasticsearchDriver.ensure_storage),
        # leaving the new index unaliased and invisible to alias-based
        # cross-catalog search.
        from dynastore.modules.elasticsearch.aliases import (
            add_index_to_public_alias,
        )
        for catalog_id in catalogs_in_batch:
            await add_index_to_public_alias(self._index_name(catalog_id))

        es = self._get_client()

        try:
            response = await es.bulk(operations=bulk_body)
        except Exception as exc:  # noqa: BLE001 — connection-level fail = retry all
            reason = f"{type(exc).__name__}: {exc}"
            return BulkIndexResult(
                passed=[],
                transient=[(op.op_id, reason) for op in op_index_map],
                poison=[],
            )

        return self._classify_response(op_index_map, response)

    # ------------------------------------------------------------------
    # Driver-resolution helpers
    # ------------------------------------------------------------------

    def _index_name(self, catalog_id: str) -> str:
        """Resolve the per-tenant items index name.

        Prefers a driver-instance helper if present (used by tests via
        ``_get_index_name`` / ``get_index_name``); falls back to the
        module-level :func:`_tenant_items_index` helper, which is the
        path production takes since :class:`ItemsElasticsearchDriver`
        does not expose a per-instance index-name method.
        """
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
        """Resolve the async ES client.

        Driver-instance ``_get_client`` wins when present (test
        adapters use this for stub injection). Production
        :class:`ItemsElasticsearchDriver` doesn't expose it; we fall
        back to the module-level
        :func:`_es_client_required` helper which raises a clear
        ``RuntimeError`` if the platform module hasn't started.
        """
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

    def _classify_response(
        self,
        op_index_map: Sequence[IndexableOp],
        response: dict,
    ) -> BulkIndexResult:
        """Translate an ES bulk response into a :class:`BulkIndexResult`.

        Delegates the per-row classification to the shared
        :func:`~dynastore.modules.elasticsearch.bulk_classify.classify_bulk_response`
        helper, then maps string ids back to the ``op_id`` UUIDs that the
        drain protocol requires.  The shared helper works with string ids so
        both the drain adapter and the inline write drivers can reuse it
        without importing ``IndexableOp``.
        """
        # Build a parallel list of string ids so classify_bulk_response can
        # correlate response items back to documents even when ES omits ``_id``
        # from an error entry (e.g. 429 with no body).
        str_ids = [op.idempotency_key for op in op_index_map]
        op_by_key = {op.idempotency_key: op for op in op_index_map}

        passed_ids, transient_pairs, poison_pairs = classify_bulk_response(
            response, str_ids,
        )

        passed: List[UUID] = [op_by_key[sid].op_id for sid in passed_ids if sid in op_by_key]
        transient: List[Tuple[UUID, str]] = [
            (op_by_key[sid].op_id, reason)
            for sid, reason in transient_pairs
            if sid in op_by_key
        ]
        poison: List[Tuple[UUID, str]] = [
            (op_by_key[sid].op_id, reason)
            for sid, reason in poison_pairs
            if sid in op_by_key
        ]
        return BulkIndexResult(passed=passed, transient=transient, poison=poison)
