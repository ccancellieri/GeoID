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

"""Shared translation: opensearch ``illegal_argument_exception`` →
:class:`IndexMappingMismatchError`, and full bulk-error surfacing via
:func:`raise_on_bulk_errors`.

Single home for the wrappers used by every ES write path so a code-side
field added without re-rolling the index surfaces as 503 (typed,
actionable) instead of a generic 500, and so any other per-doc rejection
surfaces as :class:`~dynastore.modules.storage.errors.EsBulkWriteError`
instead of being silently discarded.
"""
import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _bulk_response_is_clean(bulk_resp: Any, ids: "List[str]") -> bool:
    """True only when ES reported no errors AND the response accounts for
    every submitted id.

    The ``errors`` flag alone is insufficient (#2799): a truncated/partial
    ``items`` array can arrive with ``errors: false``, in which case the
    trailing ids were never acknowledged. When the item count does not match
    the submitted id count we must NOT take the fast ``return list(ids)``
    path — the response has to be classified so the unconfirmed tail is
    surfaced as a failure rather than assumed successful.
    """
    if not isinstance(bulk_resp, dict) or bulk_resp.get("errors"):
        return False
    items = bulk_resp.get("items", []) or []
    return len(items) == len(ids)


def maybe_raise_mapping_mismatch(
    exc: Exception, index_name: str, doc_keys: Iterable[str],
) -> None:
    """No-op unless ``exc`` is an opensearch error of type
    ``illegal_argument_exception``; otherwise raise
    :class:`IndexMappingMismatchError` chained to ``exc``.

    Identifies the offending field by intersecting ``doc_keys`` with the
    ES error reason — best-effort; ``field`` is ``None`` when no key
    appears in the message.
    """
    info = getattr(exc, "info", None)
    if not isinstance(info, dict):
        return
    err = info.get("error")
    err_type = err.get("type") if isinstance(err, dict) else None
    if err_type != "illegal_argument_exception":
        return
    reason = (err.get("reason") if isinstance(err, dict) else None) or str(exc)
    field: Optional[str] = next(
        (k for k in doc_keys if k in reason), None,
    )
    from dynastore.modules.storage.errors import IndexMappingMismatchError
    raise IndexMappingMismatchError(
        f"ES rejected write to '{index_name}' — mapping is out of date "
        f"(field '{field}' not in mapping). "
        f"Reindex required. Original: {reason}",
        index=index_name,
        field=field,
    ) from exc


def maybe_raise_bulk_mapping_mismatch(
    bulk_resp: Dict[str, Any], index_name: str,
) -> None:
    """Same translation for `_bulk` responses where individual item
    failures land in ``response["items"][i][<op>]["error"]``.

    Raises on the first item that carries
    ``error.type == illegal_argument_exception``; remaining items in
    the response are not processed (mapping mismatch is not a
    per-document issue — one fail means the index is stale).
    """
    if not bulk_resp.get("errors"):
        return
    for item in bulk_resp.get("items", []) or []:
        for op_name in ("index", "create", "update"):
            op = item.get(op_name) if isinstance(item, dict) else None
            if not isinstance(op, dict):
                continue
            err = op.get("error")
            if isinstance(err, dict) and err.get("type") == "illegal_argument_exception":
                reason = err.get("reason") or "illegal_argument_exception"
                from dynastore.modules.storage.errors import IndexMappingMismatchError
                raise IndexMappingMismatchError(
                    f"ES rejected bulk write to '{index_name}' — mapping is "
                    f"out of date. Reindex required. Original: {reason}",
                    index=index_name,
                    field=None,
                )


def raise_on_bulk_errors(
    bulk_resp: Any,
    index_name: str,
    ids: "List[str]",
) -> "List[str]":
    """Check a ``_bulk`` response for per-doc errors and enforce the invariant.

    Must be called AFTER :func:`maybe_raise_bulk_mapping_mismatch` so
    ``illegal_argument_exception`` is already promoted to
    :class:`~dynastore.modules.storage.errors.IndexMappingMismatchError` (503)
    before we reach this point.

    For every remaining failure (any ``status >= 300`` or ``"error"`` key):

    1. Emit an ERROR-level log line with the item id, error type, and reason
       (guarantees a durable trace even when the caller has ``on_failure=WARN``
       or ``on_failure=IGNORE``).
    2. Collect all failures and raise a single
       :class:`~dynastore.modules.storage.errors.EsBulkWriteError` carrying the
       full ``(id, reason)`` list — plus the acknowledged ids on
       ``.acknowledged`` (#2799) — so the dispatcher's ``on_failure`` policy
       can route the batch to OUTBOX or propagate as FATAL.

    Parameters
    ----------
    bulk_resp:
        Raw dict returned by ``es.bulk()``.
    index_name:
        The ES index that was written — used in log messages.
    ids:
        The submitted document ids in the same order as ``bulk_resp["items"]``.
        Used to correlate failures back to the original documents when ES
        omits ``_id`` from an error entry.

    Returns
    -------
    List of ids ES actually acknowledged (``2xx``, no error). When no
    document is rejected this is simply ``ids``; on a partial rejection
    (raised as :class:`~dynastore.modules.storage.errors.EsBulkWriteError`)
    the same list is available on the exception's ``.acknowledged``.
    """
    if _bulk_response_is_clean(bulk_resp, ids):
        return list(ids)

    from dynastore.modules.elasticsearch.bulk_classify import classify_bulk_response
    from dynastore.modules.storage.errors import EsBulkWriteError

    passed, transient, poison = classify_bulk_response(bulk_resp, ids)
    all_failures: List[Tuple[str, str]] = list(transient) + list(poison)

    if not all_failures:
        return passed

    for doc_id, reason in all_failures:
        logger.error(
            "ES bulk write rejected: index=%s id=%s reason=%s",
            index_name, doc_id, reason,
        )

    raise EsBulkWriteError(
        f"ES bulk write to '{index_name}' rejected {len(all_failures)} "
        f"document(s) — see ERROR logs above for per-doc details.",
        failures=all_failures,
        acknowledged=passed,
    )


async def raise_on_bulk_errors_with_ladder(
    es: Any,
    bulk_resp: Any,
    index_name: str,
    ids: "List[str]",
    doc_by_id: "Dict[str, Dict[str, Any]]",
    *,
    routing: Optional[str] = None,
) -> "List[str]":
    """Same contract as :func:`raise_on_bulk_errors`, plus a degradation-
    ladder retry (#2769) for poison-classified rejections before raising.

    Every ``poison`` failure whose submitted ``_source`` is available in
    *doc_by_id* is retried through
    :func:`~dynastore.modules.elasticsearch.geo_shape_ladder.retry_doc_with_ladder`
    — progressively coarser geometry resubmitted as single-document
    ``index`` calls. A doc that lands on a rung is WARNING-logged (with the
    rung name) and counted as **acknowledged** (it now exists on a coarser
    rung); a doc with no geometry to degrade, or that exhausts every rung,
    keeps its original rejection and is ERROR-logged / raised exactly as
    :func:`raise_on_bulk_errors` would have done.

    ``doc_by_id`` supplies the just-submitted document for each id so the
    ladder starts from the exact geometry that was rejected rather than a
    fresh re-read — every caller building a bulk request already has this
    in scope.

    Must be called AFTER :func:`maybe_raise_bulk_mapping_mismatch` — same
    ordering requirement as :func:`raise_on_bulk_errors`.

    Returns
    -------
    List of ids ES actually acknowledged — the ``passed`` classification
    plus any poison id recovered on a ladder rung. On a partial rejection
    (raised as :class:`~dynastore.modules.storage.errors.EsBulkWriteError`)
    the same list is available on the exception's ``.acknowledged`` (#2799)
    — callers MUST use it instead of assuming every non-failed id in the
    request succeeded, since a rejected sub-chunk is not all-or-nothing.
    """
    if _bulk_response_is_clean(bulk_resp, ids):
        return list(ids)

    from dynastore.modules.elasticsearch.bulk_classify import classify_bulk_response
    from dynastore.modules.elasticsearch.geo_shape_ladder import retry_doc_with_ladder
    from dynastore.modules.storage.errors import EsBulkWriteError

    passed, transient, poison = classify_bulk_response(bulk_resp, ids)
    acknowledged: List[str] = list(passed)

    remaining_poison: List[Tuple[str, str]] = []
    for doc_id, reason in poison:
        doc = doc_by_id.get(doc_id)
        recovered = False
        rung: Optional[str] = None
        if doc is not None:
            recovered, rung = await retry_doc_with_ladder(
                es, index_name=index_name, doc_id=doc_id, doc=doc,
                reason=reason, routing=routing,
            )
        if recovered:
            acknowledged.append(doc_id)
            logger.warning(
                "ES bulk write: doc id=%s in index=%s recovered on degraded "
                "geometry rung=%s after rejection (%s)",
                doc_id, index_name, rung, reason,
            )
        else:
            remaining_poison.append((doc_id, reason))

    all_failures: List[Tuple[str, str]] = list(transient) + remaining_poison
    if not all_failures:
        return acknowledged

    for doc_id, reason in all_failures:
        logger.error(
            "ES bulk write rejected: index=%s id=%s reason=%s",
            index_name, doc_id, reason,
        )

    raise EsBulkWriteError(
        f"ES bulk write to '{index_name}' rejected {len(all_failures)} "
        f"document(s) — see ERROR logs above for per-doc details.",
        failures=all_failures,
        acknowledged=acknowledged,
    )
