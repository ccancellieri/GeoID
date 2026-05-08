"""Shared translation: opensearch ``illegal_argument_exception`` →
:class:`IndexMappingMismatchError`.

Single home for the wrapper used by every ES write path so a code-side
field added without re-rolling the index surfaces as 503 (typed,
actionable) instead of a generic 500.
"""
from typing import Any, Dict, Iterable, Optional


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
