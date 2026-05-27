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
