"""Shared ES bulk-response classifier.

Single home for the classification rules consumed by:
* :class:`~dynastore.tasks.outbox_drain.es_indexer_adapter.ESBulkIndexer`
  (async drain path — ``_classify_response`` delegates here).
* Every inline synchronous ES write driver that calls ``es.bulk()``
  inside the request path (``write_entities`` / ``index_bulk``).

Classification rules
--------------------
* ``2xx`` and ``error`` absent → **passed**.
* ``429 Too Many Requests`` → **transient** (rate-limited; retry).
* ``5xx`` or ``error.type`` in :data:`_TRANSIENT_ERROR_TYPES` → **transient**.
* ``error.type`` in :data:`_POISON_ERROR_TYPES` or any other ``4xx`` (non-429)
  → **poison**.
* Unknown shape → **transient** (conservative default).

``illegal_argument_exception`` is classified **poison** here because it
indicates a document-level incompatibility (field type conflict). The
higher-level :func:`~dynastore.modules.elasticsearch._mapping_errors.maybe_raise_bulk_mapping_mismatch`
maps it to :class:`~dynastore.modules.storage.errors.IndexMappingMismatchError`
(HTTP 503 — operator reindex needed) before this classifier is reached on
the inline write paths; the drain adapter does NOT call that wrapper, so
it lands here as a straightforward poison bucket.
"""
from __future__ import annotations

from typing import Any, List, Sequence, Tuple


_TRANSIENT_ERROR_TYPES: frozenset[str] = frozenset({
    "es_rejected_execution_exception",
    "cluster_block_exception",
    "circuit_breaking_exception",
    "node_not_connected_exception",
    "process_cluster_event_timeout_exception",
})


_POISON_ERROR_TYPES: frozenset[str] = frozenset({
    "mapper_parsing_exception",
    "illegal_argument_exception",
    "version_conflict_engine_exception",  # idempotency violation — drop, don't retry
    "document_missing_exception",
    "type_missing_exception",
    "invalid_shape_exception",            # duplicate consecutive coordinates etc.
})


def classify_bulk_response(
    response: Any,
    ids: Sequence[str],
) -> Tuple[List[str], List[Tuple[str, str]], List[Tuple[str, str]]]:
    """Partition an ES ``_bulk`` response into (passed, transient, poison).

    Parameters
    ----------
    response:
        The raw dict returned by the ES client's ``bulk()`` call.
    ids:
        The document ids corresponding to each entry in
        ``response["items"]``, in the same order.  Items in
        ``response["items"]`` that lack an explicit ``_id`` (e.g. because
        the action was a delete that ES never acknowledged) fall back to
        the corresponding entry in ``ids``.

    Returns
    -------
    passed:
        List of ids for docs that ES accepted (``2xx``, no error).
    transient:
        List of ``(id, reason)`` for docs that should be retried.
    poison:
        List of ``(id, reason)`` for docs that should be dead-lettered.
    """
    passed: List[str] = []
    transient: List[Tuple[str, str]] = []
    poison: List[Tuple[str, str]] = []

    items = response.get("items", []) if isinstance(response, dict) else []
    for raw_item, doc_id in zip(items, ids):
        entry = next(iter(raw_item.values())) if isinstance(raw_item, dict) and raw_item else {}
        status = entry.get("status", 200) if isinstance(entry, dict) else 200
        error = entry.get("error") if isinstance(entry, dict) else None
        item_id = (entry.get("_id") if isinstance(entry, dict) else None) or doc_id

        if not error and 200 <= int(status) < 300:
            passed.append(item_id)
            continue

        err_type = (
            (error or {}).get("type", "unknown") if isinstance(error, dict) else "unknown"
        )
        err_reason = (
            (error or {}).get("reason", "no reason") if isinstance(error, dict) else str(error)
        )

        if int(status) == 429:
            transient.append((item_id, f"429 rate-limited: {err_reason}"))
            continue
        if err_type in _TRANSIENT_ERROR_TYPES or int(status) >= 500:
            transient.append((item_id, f"{status} {err_type}: {err_reason}"))
            continue
        if err_type in _POISON_ERROR_TYPES or 400 <= int(status) < 500:
            poison.append((item_id, f"{status} {err_type}: {err_reason}"))
            continue
        # Unknown shape — be conservative, send to retry.
        transient.append((item_id, f"{status} {err_type}: {err_reason}"))

    return passed, transient, poison
