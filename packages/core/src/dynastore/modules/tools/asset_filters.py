#    Copyright 2025 FAO
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

"""Translate ``AssetFilter`` lists into backend predicates.

A single operator vocabulary (:class:`~dynastore.models.query_builder.FilterOperator`)
drives both the PostgreSQL WHERE-clause builder (:func:`build_pg_where`) and the
Elasticsearch query builder (:func:`build_es_query`), so the asset-search surface
offers identical operator semantics on either backend without each driver
re-deriving the mapping.

Supported operators (asset metadata carries no geometry, so spatial and
PostGIS-range operators are rejected here):

``eq``, ``ne``/``neq``, ``gt``, ``gte``, ``lt``, ``lte``, ``like``, ``ilike``,
``in``, ``nin``, ``isnull``, ``isnotnull``.

PostgreSQL specifics:

* ``eq`` filters on ``metadata.*`` paths are folded into a single ``@>`` JSONB
  containment predicate so the planner can use the ``idx_assets_metadata_gin_*``
  GIN index. Containment is type-strict (a stored JSON number ``100`` matches
  ``100``, not ``"100"``).
* Every other ``metadata.*`` filter uses the ``#>>`` text accessor; numeric and
  boolean values are cast for comparison, so the stored value must actually be
  of that type.
* Top-level columns are validated as SQL identifiers and bound as parameters.
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Tuple

from dynastore.models.query_builder import AssetFilter, FilterOperator

# Operators this asset-search surface understands. Spatial / range operators
# are intentionally excluded — assets are metadata documents, not geometries.
SUPPORTED_OPERATORS: frozenset = frozenset({
    FilterOperator.EQ,
    FilterOperator.NE,
    FilterOperator.NEQ,
    FilterOperator.GT,
    FilterOperator.GTE,
    FilterOperator.LT,
    FilterOperator.LTE,
    FilterOperator.LIKE,
    FilterOperator.ILIKE,
    FilterOperator.IN,
    FilterOperator.NIN,
    FilterOperator.IS_NULL,
    FilterOperator.IS_NOT_NULL,
})

_NULL_OPS = frozenset({FilterOperator.IS_NULL, FilterOperator.IS_NOT_NULL})
_LIST_OPS = frozenset({FilterOperator.IN, FilterOperator.NIN})
_TEXT_MATCH_OPS = frozenset({FilterOperator.LIKE, FilterOperator.ILIKE})
_RANGE_OPS = frozenset({
    FilterOperator.GT, FilterOperator.GTE, FilterOperator.LT, FilterOperator.LTE,
})

_METADATA_PREFIX = "metadata."
# JSONB path segments are embedded into a ``'{a,b}'`` text-array literal, so they
# must not contain the characters that could break out of it.
_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9_]+$")


def _normalize_op(op: FilterOperator) -> FilterOperator:
    """Collapse the ``NEQ`` alias onto ``NE``."""
    return FilterOperator.NE if op == FilterOperator.NEQ else op


def reject_unsupported(filters: List[AssetFilter]) -> None:
    """Raise ``ValueError`` if any filter uses an operator outside the
    asset-search vocabulary, listing the offending operators so the caller
    can correct the request."""
    bad = sorted({f.op.value for f in filters if _normalize_op(f.op) not in SUPPORTED_OPERATORS})
    if bad:
        supported = sorted(o.value for o in SUPPORTED_OPERATORS)
        raise ValueError(
            f"asset search supports operators {supported}; "
            f"received unsupported operator(s): {bad}"
        )


def _split_metadata(field: str) -> List[str]:
    """Split ``metadata.a.b`` into ``["a", "b"]``; raise on an empty segment."""
    parts = field[len(_METADATA_PREFIX):].split(".")
    if not parts or any(not p for p in parts):
        raise ValueError(f"invalid metadata path {field!r}: empty segment")
    return parts


def _require_scalar(field: str, value: Any) -> None:
    if isinstance(value, (list, tuple, set, dict)):
        raise ValueError(
            f"filter on {field!r} expects a scalar value for this operator; "
            f"got {type(value).__name__}"
        )


def _as_list(field: str, value: Any) -> List[Any]:
    if not isinstance(value, (list, tuple, set)):
        raise ValueError(
            f"IN/NIN filter on {field!r} requires a list value; "
            f"got {type(value).__name__}"
        )
    items = list(value)
    if not items:
        raise ValueError(f"IN/NIN filter on {field!r} requires a non-empty list")
    return items


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------


def _fold_into_container(container: Dict[str, Any], segs: List[str], value: Any, field: str) -> None:
    """Fold a metadata ``eq`` filter into the nested ``@>`` containment dict."""
    d: Any = container
    for p in segs[:-1]:
        nxt = d.setdefault(p, {})
        if not isinstance(nxt, dict):
            raise ValueError(
                f"conflicting filter on metadata path {field!r}: earlier filter "
                f"set a scalar at the same prefix"
            )
        d = nxt
    leaf = segs[-1]
    if leaf in d and isinstance(d[leaf], dict):
        raise ValueError(
            f"conflicting filter on metadata path {field!r}: earlier filter set a "
            f"sub-object at the same key"
        )
    d[leaf] = value


def _jsonb_text_accessor(segs: List[str]) -> str:
    """Return ``metadata #>> '{a,b}'`` for a validated path."""
    for seg in segs:
        if not _SAFE_SEGMENT.match(seg):
            raise ValueError(
                f"unsafe metadata path segment {seg!r}: only [A-Za-z0-9_] allowed "
                f"for non-equality operators"
            )
    return "metadata #>> '{" + ",".join(segs) + "}'"


def _pg_predicate(lhs: str, op: FilterOperator, key: str, value: Any, field: str, *, numeric_lhs: bool) -> Tuple[str, Dict[str, Any]]:
    """Build one ``<lhs> <op> <param>`` predicate + its bind params.

    ``numeric_lhs`` is True for top-level columns (already typed); for the
    metadata text accessor the caller wraps numeric/boolean values itself.
    """
    params: Dict[str, Any] = {}
    if op in _NULL_OPS:
        return f"{lhs} {op.to_sql()}", params
    if op in _LIST_OPS:
        items = _as_list(field, value)
        placeholders = []
        for j, item in enumerate(items):
            pk = f"{key}_{j}"
            params[pk] = item
            placeholders.append(f":{pk}")
        return f"{lhs} {op.to_sql()} ({', '.join(placeholders)})", params
    _require_scalar(field, value)
    params[key] = value
    return f"{lhs} {op.to_sql()} :{key}", params


def build_pg_where(filters: List[AssetFilter], *, prefix: str = "af") -> Tuple[List[str], Dict[str, Any]]:
    """Build SQL WHERE fragments and bind params from asset filters.

    Returns ``(where_parts, params)``. ``eq`` filters on ``metadata.*`` paths
    are folded into one ``@>`` JSONB containment predicate; everything else
    becomes its own parameterized predicate. Bind-param names are namespaced by
    *prefix* so they never collide with the driver's own params.
    """
    reject_unsupported(filters)
    where_parts: List[str] = []
    params: Dict[str, Any] = {}
    metadata_container: Dict[str, Any] = {}

    for i, f in enumerate(filters):
        op = _normalize_op(f.op)
        key = f"{prefix}{i}"
        if f.field.startswith(_METADATA_PREFIX):
            segs = _split_metadata(f.field)
            if op == FilterOperator.EQ:
                _fold_into_container(metadata_container, segs, f.value, f.field)
                continue
            accessor = _jsonb_text_accessor(segs)
            if op in _RANGE_OPS or (op == FilterOperator.NE and isinstance(f.value, (int, float)) and not isinstance(f.value, bool)):
                # Numeric comparison on a JSON text value needs a cast.
                accessor = f"({accessor})::numeric"
            elif isinstance(f.value, bool):
                # JSON booleans render as the text 'true'/'false' via #>>.
                f = f.model_copy(update={"value": "true" if f.value else "false"})
            clause, p = _pg_predicate(accessor, op, key, f.value, f.field, numeric_lhs=False)
            where_parts.append(clause)
            params.update(p)
        else:
            from dynastore.tools.db import validate_sql_identifier

            validate_sql_identifier(f.field)
            clause, p = _pg_predicate(f'"{f.field}"', op, key, f.value, f.field, numeric_lhs=True)
            where_parts.append(clause)
            params.update(p)

    if metadata_container:
        where_parts.append("metadata @> CAST(:metadata_container AS jsonb)")
        params["metadata_container"] = json.dumps(metadata_container)

    return where_parts, params


# ---------------------------------------------------------------------------
# Elasticsearch
# ---------------------------------------------------------------------------


def _like_to_wildcard(pattern: str) -> str:
    """Translate an SQL LIKE pattern to an ES wildcard pattern.

    SQL ``%`` → ES ``*`` (any run), SQL ``_`` → ES ``?`` (one char). Literal
    ``*`` / ``?`` in the input are escaped so they keep their literal meaning.
    """
    out: List[str] = []
    for ch in pattern:
        if ch in ("*", "?", "\\"):
            out.append("\\" + ch)
        elif ch == "%":
            out.append("*")
        elif ch == "_":
            out.append("?")
        else:
            out.append(ch)
    return "".join(out)


def build_es_query(filters: List[AssetFilter]) -> Dict[str, Any]:
    """Build an ES query dict from asset filters (without collection scoping).

    Positive operators land in ``bool.filter``; negations (``ne``, ``nin``,
    ``isnull``) land in ``bool.must_not``. Dot-notation fields (``metadata.x``)
    are passed through — ES resolves them natively.
    """
    reject_unsupported(filters)
    if not filters:
        return {"match_all": {}}

    must: List[Dict[str, Any]] = []
    must_not: List[Dict[str, Any]] = []

    for f in filters:
        op = _normalize_op(f.op)
        field = f.field
        if op == FilterOperator.EQ:
            must.append({"term": {field: f.value}})
        elif op == FilterOperator.NE:
            must_not.append({"term": {field: f.value}})
        elif op in _RANGE_OPS:
            _require_scalar(field, f.value)
            must.append({"range": {field: {op.value: f.value}}})
        elif op == FilterOperator.IN:
            must.append({"terms": {field: _as_list(field, f.value)}})
        elif op == FilterOperator.NIN:
            must_not.append({"terms": {field: _as_list(field, f.value)}})
        elif op in _TEXT_MATCH_OPS:
            _require_scalar(field, f.value)
            wc: Dict[str, Any] = {"value": _like_to_wildcard(str(f.value))}
            if op == FilterOperator.ILIKE:
                wc["case_insensitive"] = True
            must.append({"wildcard": {field: wc}})
        elif op == FilterOperator.IS_NULL:
            must_not.append({"exists": {"field": field}})
        elif op == FilterOperator.IS_NOT_NULL:
            must.append({"exists": {"field": field}})

    bool_q: Dict[str, Any] = {}
    if must:
        bool_q["filter"] = must
    if must_not:
        bool_q["must_not"] = must_not
    if not bool_q:
        return {"match_all": {}}
    return {"bool": bool_q}
