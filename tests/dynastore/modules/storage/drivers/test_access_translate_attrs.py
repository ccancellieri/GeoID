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

"""Sanity tests for ES access_translate with ``_attrs.*`` predicates (#1441).

The ES translator already handles arbitrary field names via
``_predicate_to_terms``.  These tests verify that dotted field names like
``_attrs.dept`` flow through unmodified to the ES ``terms`` clause, so no
change to the translator was required.
"""
from __future__ import annotations

from dynastore.models.protocols.access_filter import (
    AccessClause,
    AccessFilter,
    FieldPredicate,
    RangePredicate,
)
from dynastore.modules.storage.drivers.elasticsearch_envelope.access_translate import (
    access_filter_to_es,
)


def _filter_with_attr_clause(field: str, values: tuple) -> AccessFilter:
    clause = AccessClause(predicates=(FieldPredicate(field, values),))
    return AccessFilter(allow=(clause,))


def test_attrs_dept_predicate_becomes_terms_clause():
    """``_attrs.dept`` FieldPredicate translates to an ES ``terms`` clause."""
    af = _filter_with_attr_clause("_attrs.dept", ("finance", "global"))
    result = access_filter_to_es(af)

    assert result is not None
    # The outer bool should have a should clause.
    assert "bool" in result
    should = result["bool"]["should"]
    assert len(should) == 1

    # The inner clause should be a bool/filter with a terms on _attrs.dept.
    inner = should[0]
    assert "bool" in inner
    filters = inner["bool"]["filter"]
    assert len(filters) == 1
    terms = filters[0]["terms"]
    assert "_attrs.dept" in terms
    assert set(terms["_attrs.dept"]) == {"finance", "global"}


def test_attrs_dotted_field_passes_through_unchanged():
    """``_attrs.sensitivity`` field name is not altered by the translator."""
    af = _filter_with_attr_clause("_attrs.sensitivity", ("low",))
    result = access_filter_to_es(af)

    assert result is not None
    should = result["bool"]["should"]
    inner = should[0]["bool"]["filter"][0]
    assert "_attrs.sensitivity" in inner["terms"]
    assert inner["terms"]["_attrs.sensitivity"] == ["low"]


def test_mixed_scope_and_attr_predicates():
    """scope pin (catalog_id) + attrs predicate in one clause."""
    clause = AccessClause(predicates=(
        FieldPredicate("catalog_id", ("cat1",)),
        FieldPredicate("_attrs.dept", ("finance",)),
    ))
    af = AccessFilter(allow=(clause,))
    result = access_filter_to_es(af)

    assert result is not None
    should = result["bool"]["should"]
    inner = should[0]["bool"]["filter"]
    # Should have two terms: catalog_id and _attrs.dept.
    fields = {list(t["terms"].keys())[0] for t in inner}
    assert "catalog_id" in fields
    assert "_attrs.dept" in fields


def test_deny_all_attrs_filter():
    """deny_all with attribute predicates → match_none."""
    af = AccessFilter(deny_all=True)
    result = access_filter_to_es(af)
    assert result == {"match_none": {}}


# ---------------------------------------------------------------------------
# Range predicate tests (F3 additions)
# ---------------------------------------------------------------------------

def _filter_with_range_clause(
    field: str, op: str, bounds: tuple, kind: str = "numeric"
) -> AccessFilter:
    rp = RangePredicate(field, op, bounds, kind=kind)  # type: ignore[arg-type]
    clause = AccessClause(predicates=(rp,))
    return AccessFilter(allow=(clause,))


def test_lte_range_predicate_becomes_es_range_clause():
    """``RangePredicate`` with op=lte → ES ``range`` with ``lte`` key."""
    af = _filter_with_range_clause("_attrs.score", "lte", ("100",))
    result = access_filter_to_es(af)

    assert result is not None
    should = result["bool"]["should"]
    assert len(should) == 1
    inner = should[0]["bool"]["filter"]
    assert len(inner) == 1
    range_clause = inner[0]
    assert "range" in range_clause
    assert "_attrs.score" in range_clause["range"]
    assert range_clause["range"]["_attrs.score"] == {"lte": "100"}


def test_gte_range_predicate_becomes_es_range_clause():
    """``RangePredicate`` with op=gte → ES ``range`` with ``gte`` key."""
    af = _filter_with_range_clause("_attrs.score", "gte", ("50",))
    result = access_filter_to_es(af)

    assert result is not None
    should = result["bool"]["should"]
    inner = should[0]["bool"]["filter"]
    range_clause = inner[0]
    assert "range" in range_clause
    assert range_clause["range"]["_attrs.score"] == {"gte": "50"}


def test_between_range_predicate_becomes_es_range_clause():
    """``RangePredicate`` with op=between → ES ``range`` with both gte and lte."""
    af = _filter_with_range_clause("_attrs.score", "between", ("10", "90"))
    result = access_filter_to_es(af)

    assert result is not None
    should = result["bool"]["should"]
    inner = should[0]["bool"]["filter"]
    range_clause = inner[0]
    assert "range" in range_clause
    assert range_clause["range"]["_attrs.score"] == {"gte": "10", "lte": "90"}


def test_range_predicate_timestamp_kind_same_shape():
    """Timestamp kind produces the same ES ``range`` shape (ES index mapping handles casting)."""
    af = _filter_with_range_clause(
        "_attrs.observed_at", "lte", ("2026-01-01T00:00:00Z",), kind="timestamp"
    )
    result = access_filter_to_es(af)

    assert result is not None
    should = result["bool"]["should"]
    inner = should[0]["bool"]["filter"]
    range_clause = inner[0]
    assert "range" in range_clause
    assert range_clause["range"]["_attrs.observed_at"] == {"lte": "2026-01-01T00:00:00Z"}


def test_mixed_field_and_range_predicates_in_one_clause():
    """A clause mixing FieldPredicate (dept) and RangePredicate (score) generates both."""
    fp = FieldPredicate("_attrs.dept", ("finance",))
    rp = RangePredicate("_attrs.score", "gte", ("10",))
    clause = AccessClause(predicates=(fp, rp))
    af = AccessFilter(allow=(clause,))
    result = access_filter_to_es(af)

    assert result is not None
    should = result["bool"]["should"]
    inner = should[0]["bool"]["filter"]
    assert len(inner) == 2
    types = {list(c.keys())[0] for c in inner}
    assert "terms" in types
    assert "range" in types
