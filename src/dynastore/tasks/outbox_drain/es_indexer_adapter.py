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
  ``document_missing_exception``, ``type_missing_exception``) or
  any other ``4xx`` (non-429) → ``poison``.
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


_TRANSIENT_ERROR_TYPES = frozenset({
    "es_rejected_execution_exception",
    "cluster_block_exception",
    "circuit_breaking_exception",
    "node_not_connected_exception",
    "process_cluster_event_timeout_exception",
})


_POISON_ERROR_TYPES = frozenset({
    "mapper_parsing_exception",
    "illegal_argument_exception",
    "version_conflict_engine_exception",  # idempotency violation — drop, don't retry
    "document_missing_exception",
    "type_missing_exception",
})


class ESBulkIndexer:
    """Wrap :class:`ItemsElasticsearchDriver` to expose the
    :class:`BulkIndexer` Protocol.

    The driver instance is passed in at construction so the adapter
    can resolve the active ES client (via
    :func:`dynastore.modules.storage.drivers.elasticsearch._es_client_required`)
    and the per-tenant items index name (via
    :func:`dynastore.modules.storage.drivers.elasticsearch._tenant_items_index`).
    The adapter does not touch the driver's higher-level
    ``write_entities`` / SFEOS path — outbox drain operates on
    pre-serialised STAC item payloads emitted by the dispatcher.
    """

    indexer_id: str = "items_elasticsearch_driver"
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

        for op in ops:
            index_name = self._index_name(op.catalog_id)
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
        passed: List[UUID] = []
        transient: List[Tuple[UUID, str]] = []
        poison: List[Tuple[UUID, str]] = []

        items = response.get("items", []) if isinstance(response, dict) else []
        for op, item in zip(op_index_map, items, strict=True):
            # Each `items` entry is a single-key dict keyed by op-name
            # ("index" or "delete"); take the inner record.
            entry = next(iter(item.values())) if isinstance(item, dict) and item else {}
            status = entry.get("status", 200) if isinstance(entry, dict) else 200
            error = entry.get("error") if isinstance(entry, dict) else None

            if not error and 200 <= int(status) < 300:
                passed.append(op.op_id)
                continue

            err_type = (error or {}).get("type", "unknown") if isinstance(error, dict) else "unknown"
            err_reason = (error or {}).get("reason", "no reason") if isinstance(error, dict) else str(error)

            # 429 always retried regardless of "type" (sometimes type
            # is es_rejected_execution_exception, sometimes empty).
            if int(status) == 429:
                transient.append((op.op_id, f"429 rate-limited: {err_reason}"))
                continue
            if err_type in _TRANSIENT_ERROR_TYPES or int(status) >= 500:
                transient.append((op.op_id, f"{status} {err_type}: {err_reason}"))
                continue
            if err_type in _POISON_ERROR_TYPES or 400 <= int(status) < 500:
                poison.append((op.op_id, f"{status} {err_type}: {err_reason}"))
                continue
            # Unknown shape — be conservative, send to retry.
            transient.append((op.op_id, f"{status} {err_type}: {err_reason}"))

        return BulkIndexResult(passed=passed, transient=transient, poison=poison)
