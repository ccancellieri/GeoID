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

"""Shared ES bulk-response classifier.

Single home for the classification rules consumed by:
* :class:`~dynastore.tasks.workclass_drain.es_indexer_adapter.ESBulkIndexer`
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

from typing import Any, List, Optional, Sequence, Tuple


# Cap on the formatted reason string (#2769): ES nested ``caused_by`` chains
# are normally short human-readable sentences, but the cap guarantees a log
# line can never balloon regardless of what a future ES version puts there
# (e.g. never a geometry coordinate dump).
_MAX_REASON_LEN = 2000

# Bound on how many ``caused_by`` hops are walked, defensively — an ES error
# body is not expected to nest deeper than a couple of levels; this just
# rules out an unbounded/cyclic structure looping forever.
_MAX_CAUSED_BY_DEPTH = 5


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


def _format_error_reason(error: Optional[dict]) -> str:
    """Render an ES bulk-item error object, including the nested
    ``caused_by`` chain (#2769).

    A ``document_parsing_exception`` for a rejected geo_shape typically
    carries a generic top-level reason ("failed to parse field [geometry]
    of type [geo_shape]") with the actual diagnosable cause ("Self-
    intersection at or near point ...") one or more ``caused_by`` hops
    down. The previous behaviour surfaced only the top-level reason, so a
    geo_shape rejection was undiagnosable without a live repro. Capped at
    :data:`_MAX_REASON_LEN`.
    """
    if not isinstance(error, dict):
        return str(error) if error is not None else "no reason"

    parts: List[str] = []
    node: Optional[dict] = error
    depth = 0
    while isinstance(node, dict) and depth < _MAX_CAUSED_BY_DEPTH:
        err_type = node.get("type", "unknown")
        reason = node.get("reason", "no reason")
        parts.append(f"{err_type}: {reason}")
        node = node.get("caused_by")
        depth += 1

    text = " | caused by: ".join(parts) if parts else "no reason"
    if len(text) > _MAX_REASON_LEN:
        text = text[:_MAX_REASON_LEN] + "...(truncated)"
    return text


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
        # Includes the nested caused_by chain (#2769) — err_type is already
        # the first segment of this string, so it is not re-prepended below.
        err_reason = _format_error_reason(error)

        if int(status) == 429:
            transient.append((item_id, f"429 rate-limited: {err_reason}"))
            continue
        if err_type in _TRANSIENT_ERROR_TYPES or int(status) >= 500:
            transient.append((item_id, f"{status} {err_reason}"))
            continue
        if err_type in _POISON_ERROR_TYPES or 400 <= int(status) < 500:
            poison.append((item_id, f"{status} {err_reason}"))
            continue
        # Unknown shape — be conservative, send to retry.
        transient.append((item_id, f"{status} {err_reason}"))

    return passed, transient, poison
