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

"""Translate a neutral :class:`AccessFilter` into PostgreSQL SQL fragments.

This is the PG mirror of
:func:`~dynastore.modules.storage.drivers.elasticsearch_envelope.access_translate.access_filter_to_es`.
It translates the compiled, backend-neutral :class:`AccessFilter` (with its
:class:`FieldPredicate` clauses) into parameterised SQL ``WHERE`` fragments for
PG drivers that store the access envelope in a JSONB column.

Scope of this slice (#1441):

* Only ``_attrs.<key>`` predicates are translated (JSONB path lookup).
* Standard scope fields (``catalog_id``, ``collection_id``, ``visibility``,
  ``owner``) map to their own first-class columns, NOT to the JSONB path, so
  they continue to use the existing column-based predicates already applied by
  the PG driver's query builder.

Usage pattern (PG driver query building)::

    from dynastore.modules.storage.access_filter_pg import access_filter_to_pg_clause
    clause, params = access_filter_to_pg_clause(
        request.access_filter,
        envelope_col="access_envelope",
    )
    if clause:
        query = query.where(text(clause)).bindparams(**params)

This module imports nothing from the IAM package; it depends only on the
neutral :class:`AccessFilter` / :class:`FieldPredicate` contracts.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple, Union

from dynastore.models.protocols.access_filter import FieldPredicate, RangePredicate

__all__ = ["access_filter_to_pg_clause"]

_ATTRS_PREFIX = "_attrs."
_SAFE_ATTR_KEY_RE = re.compile(r"[A-Za-z0-9_]+")

# First-class column names that are stored as regular PG columns, NOT in the
# JSONB envelope.  Predicates on these must be handled by the caller's
# existing column-based filter logic.
_FIRST_CLASS_COLS = frozenset(
    {"catalog_id", "collection_id", "visibility", "owner"}
)


def _range_predicate_to_sql(
    predicate: RangePredicate,
    param_prefix: str,
    params: Dict[str, Any],
    *,
    envelope_col: str,
) -> Optional[str]:
    """Translate one :class:`RangePredicate` to a SQL fragment.

    Only ``_attrs.<key>`` predicates (JSONB path) are translated.  The bound
    values are cast to NUMERIC (default) or TIMESTAMPTZ depending on
    ``predicate.kind``.  Returns ``None`` for unrecognised fields (fail-safe).
    """
    field: str = predicate.field
    if not field.startswith(_ATTRS_PREFIX):
        return None

    attr_key = field[len(_ATTRS_PREFIX):]
    if not attr_key:
        return None
    if not _SAFE_ATTR_KEY_RE.fullmatch(attr_key):
        return None

    cast_type = "TIMESTAMPTZ" if predicate.kind == "timestamp" else "NUMERIC"
    jsonb_path = f"({envelope_col}->'attrs'->>'{attr_key}')"

    if predicate.op == "lte":
        if len(predicate.bounds) != 1:
            return None
        p = f"{param_prefix}_{attr_key}_lte"
        params[p] = predicate.bounds[0]
        return f"CAST({jsonb_path} AS {cast_type}) <= CAST(:{p} AS {cast_type})"

    if predicate.op == "gte":
        if len(predicate.bounds) != 1:
            return None
        p = f"{param_prefix}_{attr_key}_gte"
        params[p] = predicate.bounds[0]
        return f"CAST({jsonb_path} AS {cast_type}) >= CAST(:{p} AS {cast_type})"

    if predicate.op == "between":
        if len(predicate.bounds) != 2:
            return None
        p_lo = f"{param_prefix}_{attr_key}_lo"
        p_hi = f"{param_prefix}_{attr_key}_hi"
        params[p_lo] = predicate.bounds[0]
        params[p_hi] = predicate.bounds[1]
        return (
            f"CAST({jsonb_path} AS {cast_type}) "
            f"BETWEEN CAST(:{p_lo} AS {cast_type}) AND CAST(:{p_hi} AS {cast_type})"
        )

    # Unknown op — skip (fail-safe, never widen).
    return None


def _field_predicate_to_sql(
    predicate: Union[FieldPredicate, RangePredicate, Any],
    param_prefix: str,
    params: Dict[str, Any],
    *,
    envelope_col: str,
) -> Optional[str]:
    """Translate one :class:`FieldPredicate` or :class:`RangePredicate` to SQL.

    Only ``_attrs.<key>`` predicates (JSONB path) are translated.  All other
    field names (first-class columns and unknowns) are skipped (returns
    ``None``): the caller already handles them via existing column predicates,
    and skipping never widens access.
    """
    if isinstance(predicate, RangePredicate):
        return _range_predicate_to_sql(predicate, param_prefix, params, envelope_col=envelope_col)

    field: str = predicate.field
    values: Tuple[str, ...] = predicate.values

    if field in _FIRST_CLASS_COLS:
        return None  # handled by existing column predicates

    if field.startswith(_ATTRS_PREFIX):
        attr_key = field[len(_ATTRS_PREFIX):]
        if not attr_key or not values:
            return None
        # JSONB key must match the AttributePredicate.key pattern enforced at
        # the grant ingress; defense-in-depth: skip the predicate (never widen)
        # if it slipped through with disallowed characters.
        if not _SAFE_ATTR_KEY_RE.fullmatch(attr_key):
            return None
        p_name = f"{param_prefix}_{attr_key}"
        params[p_name] = list(values)
        return (
            f"({envelope_col}->'attrs'->>'{attr_key}') = ANY(:{p_name})"
        )

    # Unknown field — skip (fail-safe, not fail-open).
    return None


def _clause_to_sql(
    clause: Any,
    idx: int,
    params: Dict[str, Any],
    *,
    envelope_col: str,
) -> Optional[str]:
    """Translate one :class:`AccessClause` to a SQL AND fragment."""
    preds = list(clause.predicates)
    parts: List[str] = []
    for j, pred in enumerate(preds):
        sql = _field_predicate_to_sql(
            pred,
            param_prefix=f"af_{idx}_{j}",
            params=params,
            envelope_col=envelope_col,
        )
        if sql is not None:
            parts.append(sql)
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return "(" + " AND ".join(parts) + ")"


def access_filter_to_pg_clause(
    access_filter: Any,
    *,
    envelope_col: str = "access_envelope",
) -> Tuple[Optional[str], Dict[str, Any]]:
    """Translate an :class:`AccessFilter` to a PG SQL clause and bind-params.

    Returns ``(clause_sql, params)`` where:
    * ``clause_sql`` is ``None`` when no JSONB-envelope restriction can be
      expressed (caller applies no additional WHERE).
    * ``params`` is a dict of named bind parameters for parameterised queries.

    Only ``_attrs.*`` JSONB predicates are translated; first-class column
    predicates are left to the caller's existing query-builder path.

    This is intentionally a best-effort, additive translator.  Unrecognised
    predicates are skipped (safe annoyance — never a security widening) so the
    caller's existing column predicates still enforce the primary constraints.
    """
    if access_filter is None:
        return None, {}

    if getattr(access_filter, "deny_all", False):
        # Deny-everything: no row should be returned.  Return a clause that
        # matches nothing.
        return "FALSE", {}

    allow_all = getattr(access_filter, "allow_all", False)
    allow_clauses = getattr(access_filter, "allow", ())

    if allow_all and not allow_clauses:
        return None, {}

    # Empty allow set without allow_all is the "deny everything by empty allow"
    # semantic; mirror the ES translator's match-nothing behaviour.
    if not allow_all and not allow_clauses:
        return "FALSE", {}

    params: Dict[str, Any] = {}
    or_parts: List[str] = []
    for i, clause in enumerate(allow_clauses):
        sql = _clause_to_sql(clause, i, params, envelope_col=envelope_col)
        if sql is not None:
            or_parts.append(sql)

    if not or_parts:
        return None, {}
    if len(or_parts) == 1:
        return or_parts[0], params
    return "(" + " OR ".join(or_parts) + ")", params
