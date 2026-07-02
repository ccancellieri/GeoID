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
"""
from __future__ import annotations

import logging
from typing import Any, List, Sequence, Tuple, cast
from uuid import UUID

from dynastore.models.protocols.indexing import (
    BulkIndexResult,
    IndexableOp,
)
from dynastore.modules.elasticsearch.bulk_classify import classify_bulk_response

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

        return self._classify_response(op_index_map, response)

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

    def _classify_response(
        self,
        op_index_map: Sequence[IndexableOp],
        response: dict,
    ) -> BulkIndexResult:
        """Translate an ES bulk response into a :class:`BulkIndexResult`."""
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
