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
) -> None:
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
       full ``(id, reason)`` list so the dispatcher's ``on_failure`` policy can
       route the batch to OUTBOX or propagate as FATAL.

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
    """
    if not isinstance(bulk_resp, dict) or not bulk_resp.get("errors"):
        return

    from dynastore.modules.elasticsearch.bulk_classify import classify_bulk_response
    from dynastore.modules.storage.errors import EsBulkWriteError

    _passed, transient, poison = classify_bulk_response(bulk_resp, ids)
    all_failures: List[Tuple[str, str]] = list(transient) + list(poison)

    if not all_failures:
        return

    for doc_id, reason in all_failures:
        logger.error(
            "ES bulk write rejected: index=%s id=%s reason=%s",
            index_name, doc_id, reason,
        )

    raise EsBulkWriteError(
        f"ES bulk write to '{index_name}' rejected {len(all_failures)} "
        f"document(s) — see ERROR logs above for per-doc details.",
        failures=all_failures,
    )
