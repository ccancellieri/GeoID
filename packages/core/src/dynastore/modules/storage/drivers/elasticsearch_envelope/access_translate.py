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

"""
Translate a neutral :class:`AccessFilter` into an Elasticsearch ``bool`` clause.

This is the Elasticsearch mirror of
:meth:`dynastore.models.protocols.access_filter.AccessFilter.admits` — the
security-critical piece of the envelope driver. The compilation of the
``AccessFilter`` from principals happens elsewhere (the authorization engine);
this module only turns the already-compiled, backend-neutral filter into ES
Query DSL. It imports nothing from the IAM module and depends only on the
public field/clause structure of an ``AccessFilter``.

Translation rules (a faithful mirror of ``admits``):

* ``None`` input            → ``None`` (no row-level restriction).
* ``deny_all``              → ``{"match_none": {}}`` (return nothing).
* non-empty ``union``       → ``bool`` ``should`` of each sub-filter's COMPLETE
  translation with ``minimum_should_match: 1`` (exclusion-union; each
  collection's allow+deny stays scoped to its own sub-clause — see ``admits``).
* ``allow_all`` and no deny → ``None`` (no restriction).
* otherwise build a ``bool``:
    - allow clauses (unless ``allow_all``) become a ``should`` list with
      ``minimum_should_match: 1`` — each clause is a ``bool`` whose ``filter``
      list holds one ``terms`` predicate per :class:`FieldPredicate`; an
      empty-predicate clause (unconditional grant) becomes ``match_all``;
    - deny clauses become a ``must_not`` list of the same per-clause shape;
    - when ``allow_all`` but there ARE deny clauses, only the ``must_not`` is
      emitted (no ``should`` gate — everything is allowed except the denials).

The DENY side is evaluated by ES *over* the ALLOW side (``must_not`` always
excludes), which preserves the deny-precedence invariant structurally, exactly
as ``admits`` does.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def _predicate_to_terms(predicate: Any) -> Dict[str, Any]:
    """One :class:`FieldPredicate` → an ES ``terms`` clause.

    ``terms`` membership matches the predicate's pure-Python semantics: a
    scalar field matches when its value is in ``values``; an array field
    matches when it intersects ``values`` (``grant_subjects``).
    """
    return {"terms": {predicate.field: list(predicate.values)}}


def _clause_to_bool(clause: Any) -> Dict[str, Any]:
    """One :class:`AccessClause` → an ES clause matching the same documents.

    An empty clause (no predicates) is an unconditional grant within its
    resource scope, so it matches every document → ``match_all``. Otherwise
    every predicate must hold (AND) → a ``bool`` with a ``filter`` list.
    """
    predicates = list(clause.predicates)
    if not predicates:
        return {"match_all": {}}
    return {"bool": {"filter": [_predicate_to_terms(p) for p in predicates]}}


def access_filter_to_es(access_filter: Any) -> Optional[Dict[str, Any]]:
    """Translate an :class:`AccessFilter` to an ES query clause (or ``None``).

    Returns the *inner* clause (a ``bool`` / ``match_none`` / ``None``), NOT a
    ``{"query": ...}`` envelope — callers AND it into their query body via
    :func:`merge_es_filter`. ``None`` means "apply no restriction".

    This is a pure function (no I/O, no globals) precisely because it is the
    security mirror of ``AccessFilter.admits`` and is exhaustively unit-tested
    branch-by-branch and property-checked against ``admits``.
    """
    if access_filter is None:
        return None

    if access_filter.deny_all:
        return {"match_none": {}}

    union = getattr(access_filter, "union", ())
    if union:
        # Exclusion-union: OR the COMPLETE translation of each sub-filter so each
        # collection's allow+deny logic stays scoped to its own subtree (the ES
        # mirror of ``AccessFilter.admits``'s union branch). A sub-filter that is
        # ``deny_everything`` translates to ``match_none`` and so contributes
        # nothing inside the ``should`` — its collection simply yields no rows,
        # without suppressing any other collection's documents.
        should_subs: List[Dict[str, Any]] = []
        for sub in union:
            sub_clause = access_filter_to_es(sub)
            if sub_clause is None:
                # An ``allow_all`` sub-filter with no deny imposes no restriction
                # → it admits every document in its own (collection-pinned) scope.
                # Mirror ``admits`` exactly: that sub-filter alone would return
                # True, so the union must too.
                sub_clause = {"match_all": {}}
            should_subs.append(sub_clause)
        return {"bool": {"should": should_subs, "minimum_should_match": 1}}

    deny_clauses: List[Dict[str, Any]] = [
        _clause_to_bool(c) for c in access_filter.deny
    ]

    if access_filter.allow_all:
        if not deny_clauses:
            return None
        # Everything is allowed except the denials.
        return {"bool": {"must_not": deny_clauses}}

    # Allow side: a document must match at least one allow clause.
    should_clauses: List[Dict[str, Any]] = [
        _clause_to_bool(c) for c in access_filter.allow
    ]

    bool_body: Dict[str, Any] = {
        "should": should_clauses,
        "minimum_should_match": 1,
    }
    if deny_clauses:
        bool_body["must_not"] = deny_clauses
    return {"bool": bool_body}
