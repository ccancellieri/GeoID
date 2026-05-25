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

"""Unit tests for the standardized-envelope ES items driver.

Covers the security-critical pieces with no live Elasticsearch:

* ``access_filter_to_es`` — branch-by-branch + a property-style cross-check
  against ``AccessFilter.admits`` (the reference semantics).
* ``build_envelope_feature_doc`` — access-field stamping (explicit + ``_``
  fallbacks), grant-subjects normalization, underscore-property stripping.
* driver class registration — config ``class_key()``, ClassVars, index name.
* the row-level seam — ``_query_request_to_es`` ANDs the access clause and
  produces a no-result query for ``deny_all``.
"""

from __future__ import annotations

import itertools
from typing import Any, Dict, List

import pytest

from dynastore.models.protocols.access_filter import (
    AccessClause,
    AccessFilter,
    FieldPredicate,
)
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.storage.driver_config import (
    ItemsElasticsearchEnvelopeDriverConfig,
)
from dynastore.modules.storage.drivers.elasticsearch_envelope.access_translate import (
    access_filter_to_es,
)
from dynastore.modules.storage.drivers.elasticsearch_envelope.doc_builder import (
    build_envelope_feature_doc,
)
from dynastore.modules.storage.drivers.elasticsearch_envelope.driver import (
    ItemsElasticsearchEnvelopeDriver,
)
from dynastore.modules.storage.drivers.elasticsearch_envelope.mappings import (
    ENVELOPE_FEATURE_MAPPING,
    get_envelope_index_name,
)


# ---------------------------------------------------------------------------
# access_filter_to_es — branch coverage
# ---------------------------------------------------------------------------


def test_none_input_returns_none():
    assert access_filter_to_es(None) is None


def test_deny_all_returns_match_none():
    f = AccessFilter.deny_everything()
    assert access_filter_to_es(f) == {"match_none": {}}


def test_allow_all_no_deny_returns_none():
    f = AccessFilter.allow_everything()
    assert access_filter_to_es(f) is None


def test_allow_all_with_deny_returns_must_not_only():
    deny = AccessClause(predicates=(FieldPredicate("owner", ("bob",)),))
    f = AccessFilter(allow_all=True, deny=(deny,))
    es = access_filter_to_es(f)
    assert es == {
        "bool": {
            "must_not": [
                {"bool": {"filter": [{"terms": {"owner": ["bob"]}}]}},
            ]
        }
    }
    # No ``should`` gate when allow_all.
    assert "should" not in es["bool"]


def test_allow_clauses_become_should_with_minimum_should_match():
    c1 = AccessClause(predicates=(FieldPredicate("visibility", ("public",)),))
    c2 = AccessClause(predicates=(FieldPredicate("owner", ("alice",)),))
    f = AccessFilter(allow=(c1, c2))
    es = access_filter_to_es(f)
    assert es["bool"]["minimum_should_match"] == 1
    should = es["bool"]["should"]
    assert {"bool": {"filter": [{"terms": {"visibility": ["public"]}}]}} in should
    assert {"bool": {"filter": [{"terms": {"owner": ["alice"]}}]}} in should
    assert "must_not" not in es["bool"]


def test_empty_predicate_clause_becomes_match_all():
    # An unconditional grant (empty clause) admits everything within scope.
    f = AccessFilter(allow=(AccessClause(predicates=()),))
    es = access_filter_to_es(f)
    assert es["bool"]["should"] == [{"match_all": {}}]
    assert es["bool"]["minimum_should_match"] == 1


def test_array_grant_subjects_predicate_uses_terms():
    clause = AccessClause(
        predicates=(FieldPredicate("grant_subjects", ("g:team-a", "g:team-b")),),
    )
    f = AccessFilter(allow=(clause,))
    es = access_filter_to_es(f)
    inner = es["bool"]["should"][0]
    assert inner == {
        "bool": {"filter": [{"terms": {"grant_subjects": ["g:team-a", "g:team-b"]}}]}
    }


def test_multi_predicate_clause_is_conjunction():
    clause = AccessClause(predicates=(
        FieldPredicate("visibility", ("restricted",)),
        FieldPredicate("owner", ("alice",)),
    ))
    f = AccessFilter(allow=(clause,))
    es = access_filter_to_es(f)
    filters = es["bool"]["should"][0]["bool"]["filter"]
    assert {"terms": {"visibility": ["restricted"]}} in filters
    assert {"terms": {"owner": ["alice"]}} in filters
    assert len(filters) == 2


def test_allow_and_deny_combined():
    allow = AccessClause(predicates=(FieldPredicate("visibility", ("public",)),))
    deny = AccessClause(predicates=(FieldPredicate("owner", ("blocked",)),))
    f = AccessFilter(allow=(allow,), deny=(deny,))
    es = access_filter_to_es(f)
    assert es["bool"]["minimum_should_match"] == 1
    assert es["bool"]["must_not"] == [
        {"bool": {"filter": [{"terms": {"owner": ["blocked"]}}]}},
    ]


def test_empty_allow_not_allow_all_matches_nothing():
    # Mirrors ``admits``: empty allow + not allow_all ⟹ deny everything.
    # ``from_clauses`` collapses this to deny_all, so build the raw shape.
    f = AccessFilter(allow=())
    es = access_filter_to_es(f)
    # should=[] with minimum_should_match=1 matches zero documents.
    assert es == {"bool": {"should": [], "minimum_should_match": 1}}


# ---------------------------------------------------------------------------
# Union node (exclusion-union of per-collection sub-filters)
# ---------------------------------------------------------------------------


def test_union_translates_to_should_of_complete_subfilters():
    sub_a = AccessFilter(allow=(AccessClause((
        FieldPredicate("collection_id", ("a",)),
        FieldPredicate("visibility", ("public",)),
    )),))
    sub_b = AccessFilter(allow=(AccessClause((
        FieldPredicate("collection_id", ("b",)),
        FieldPredicate("owner", ("me",)),
    )),))
    u = AccessFilter.union_of([sub_a, sub_b])
    es = access_filter_to_es(u)
    assert es["bool"]["minimum_should_match"] == 1
    should = es["bool"]["should"]
    # Each sub-filter contributes its OWN complete clause (not flattened).
    assert {"bool": {"should": [{"bool": {"filter": [
        {"terms": {"collection_id": ["a"]}},
        {"terms": {"visibility": ["public"]}},
    ]}}], "minimum_should_match": 1}} in should
    assert {"bool": {"should": [{"bool": {"filter": [
        {"terms": {"collection_id": ["b"]}},
        {"terms": {"owner": ["me"]}},
    ]}}], "minimum_should_match": 1}} in should


def test_union_deny_does_not_cross_contaminate():
    # A: public allowed but owner=blocked denied (scoped to A); B: public allowed.
    sub_a = AccessFilter(
        allow=(AccessClause((
            FieldPredicate("collection_id", ("a",)),
            FieldPredicate("visibility", ("public",)),
        )),),
        deny=(AccessClause((
            FieldPredicate("collection_id", ("a",)),
            FieldPredicate("owner", ("blocked",)),
        )),),
    )
    sub_b = AccessFilter(allow=(AccessClause((
        FieldPredicate("collection_id", ("b",)),
        FieldPredicate("visibility", ("public",)),
    )),))
    u = AccessFilter.union_of([sub_a, sub_b])
    es = access_filter_to_es(u)
    blocked_a = {"collection_id": "a", "visibility": "public", "owner": "blocked"}
    blocked_b = {"collection_id": "b", "visibility": "public", "owner": "blocked"}
    # A's deny excludes its own blocked doc; B's same-owner doc is unaffected.
    assert _es_clause_admits(es, blocked_a) is False
    assert _es_clause_admits(es, blocked_b) is True
    # ES path agrees with admits() reference semantics.
    assert _es_clause_admits(es, blocked_a) == u.admits(blocked_a)
    assert _es_clause_admits(es, blocked_b) == u.admits(blocked_b)


def test_union_drops_deny_everything_subfilter():
    # A grants access; B (no access) compiles to deny_everything → inert in OR.
    sub_a = AccessFilter(allow=(AccessClause((
        FieldPredicate("collection_id", ("a",)),
    )),))
    u = AccessFilter.union_of([sub_a, AccessFilter.deny_everything()])
    # Single live branch → returned as-is (no wrapper node).
    assert u.union == ()
    assert u.admits({"collection_id": "a"}) is True
    assert u.admits({"collection_id": "b"}) is False


def test_union_of_all_denied_is_deny_everything():
    u = AccessFilter.union_of([
        AccessFilter.deny_everything(),
        AccessFilter.deny_everything(),
    ])
    assert u.deny_all is True
    assert access_filter_to_es(u) == {"match_none": {}}


# ---------------------------------------------------------------------------
# Property-style cross-check: ES translation agrees with admits()
# ---------------------------------------------------------------------------


def _es_clause_admits(clause: Any, doc: Dict[str, Any]) -> bool:
    """Pure-Python evaluator of the ES clauses this translator emits.

    Supports exactly the shapes ``access_filter_to_es`` produces:
    ``match_none`` / ``match_all`` / ``bool`` with ``filter`` (AND of
    ``terms``) / ``should`` (OR, ``minimum_should_match``) / ``must_not``.
    """
    if clause is None:
        return True
    if "match_none" in clause:
        return False
    if "match_all" in clause:
        return True
    if "terms" in clause:
        (field, values), = clause["terms"].items()
        actual = doc.get(field)
        allowed = set(values)
        if isinstance(actual, (list, tuple, set)):
            return any(v in allowed for v in actual)
        return actual in allowed
    if "term" in clause:
        (field, value), = clause["term"].items()
        return doc.get(field) == value
    if "bool" in clause:
        body = clause["bool"]
        ok = True
        for sub in body.get("filter", []):
            ok = ok and _es_clause_admits(sub, doc)
        for sub in body.get("must", []):
            ok = ok and _es_clause_admits(sub, doc)
        for sub in body.get("must_not", []):
            ok = ok and not _es_clause_admits(sub, doc)
        should = body.get("should")
        if should is not None:
            mim = body.get("minimum_should_match", 0)
            n = sum(1 for sub in should if _es_clause_admits(sub, doc))
            ok = ok and (n >= mim)
        return ok
    raise AssertionError(f"unexpected clause shape: {clause!r}")


def _sample_docs() -> List[Dict[str, Any]]:
    visibilities = [None, "public", "restricted", "private"]
    owners = [None, "alice", "bob"]
    grants = [None, ["g:team-a"], ["g:team-a", "g:team-b"], ["g:team-c"]]
    docs: List[Dict[str, Any]] = []
    for vis, own, gr in itertools.product(visibilities, owners, grants):
        d: Dict[str, Any] = {"geoid": "x"}
        if vis is not None:
            d["visibility"] = vis
        if own is not None:
            d["owner"] = own
        if gr is not None:
            d["grant_subjects"] = gr
        docs.append(d)
    return docs


def _sample_filters() -> List[AccessFilter]:
    pub = AccessClause(predicates=(FieldPredicate("visibility", ("public",)),))
    own_alice = AccessClause(predicates=(FieldPredicate("owner", ("alice",)),))
    team = AccessClause(
        predicates=(FieldPredicate("grant_subjects", ("g:team-a", "g:team-b")),),
    )
    deny_bob = AccessClause(predicates=(FieldPredicate("owner", ("bob",)),))
    multi = AccessClause(predicates=(
        FieldPredicate("visibility", ("restricted",)),
        FieldPredicate("owner", ("alice",)),
    ))
    unconditional = AccessClause(predicates=())
    return [
        AccessFilter.deny_everything(),
        AccessFilter.allow_everything(),
        AccessFilter(allow_all=True, deny=(deny_bob,)),
        AccessFilter(allow=(pub,)),
        AccessFilter(allow=(pub, own_alice)),
        AccessFilter(allow=(team,)),
        AccessFilter(allow=(pub,), deny=(deny_bob,)),
        AccessFilter(allow=(multi,)),
        AccessFilter(allow=(unconditional,)),
        AccessFilter(allow=(unconditional,), deny=(deny_bob,)),
        AccessFilter(allow=()),
        # Exclusion-union of per-collection sub-filters: each stays intact so
        # the ES translation must agree with the union branch of ``admits``.
        AccessFilter.union_of([
            AccessFilter(allow=(pub,)),
            AccessFilter(allow=(own_alice,), deny=(deny_bob,)),
        ]),
    ]


@pytest.mark.parametrize("flt", _sample_filters())
def test_es_translation_agrees_with_admits(flt: AccessFilter):
    clause = access_filter_to_es(flt)
    for doc in _sample_docs():
        assert _es_clause_admits(clause, doc) == flt.admits(doc), (
            f"divergence for filter={flt!r} doc={doc!r}"
        )


# ---------------------------------------------------------------------------
# build_envelope_feature_doc
# ---------------------------------------------------------------------------


def test_mapping_declares_access_fields_as_keyword():
    props = ENVELOPE_FEATURE_MAPPING["properties"]
    assert props["visibility"] == {"type": "keyword"}
    assert props["owner"] == {"type": "keyword"}
    assert props["grant_subjects"] == {"type": "keyword"}
    # Root is static so dynamic fields cannot pollute the access envelope.
    assert ENVELOPE_FEATURE_MAPPING["dynamic"] is False
    # Tenant attributes stay dynamic.
    assert props["properties"] == {"type": "object", "dynamic": True}


def test_access_fields_from_explicit_args():
    item = {
        "id": "geo-1",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "properties": {"name": "x"},
    }
    doc = build_envelope_feature_doc(
        item,
        catalog_id="cat",
        collection_id="col",
        visibility="restricted",
        owner="alice",
        grant_subjects=["g:team-a", "g:team-b"],
    )
    assert doc["visibility"] == "restricted"
    assert doc["owner"] == "alice"
    assert doc["grant_subjects"] == ["g:team-a", "g:team-b"]


def test_access_fields_fall_back_to_underscore_source_keys():
    item = {
        "id": "geo-2",
        "_visibility": "public",
        "_owner": "bob",
        "_grant_subjects": ["g:x"],
        "properties": {"name": "y"},
    }
    doc = build_envelope_feature_doc(item, catalog_id="cat", collection_id="col")
    assert doc["visibility"] == "public"
    assert doc["owner"] == "bob"
    assert doc["grant_subjects"] == ["g:x"]


def test_explicit_args_override_underscore_keys():
    item = {
        "id": "geo-3",
        "_visibility": "private",
        "_owner": "src",
        "properties": {},
    }
    doc = build_envelope_feature_doc(
        item, catalog_id="cat", collection_id="col",
        visibility="public", owner="explicit",
    )
    assert doc["visibility"] == "public"
    assert doc["owner"] == "explicit"


def test_grant_subjects_scalar_normalizes_to_list():
    item = {"id": "geo-4", "properties": {}}
    doc = build_envelope_feature_doc(
        item, catalog_id="cat", collection_id="col", grant_subjects="g:solo",
    )
    assert doc["grant_subjects"] == ["g:solo"]


def test_grant_subjects_set_normalizes_to_list_of_strings():
    item = {"id": "geo-5", "properties": {}}
    doc = build_envelope_feature_doc(
        item, catalog_id="cat", collection_id="col", grant_subjects={1, 2},
    )
    assert sorted(doc["grant_subjects"]) == ["1", "2"]


def test_empty_grant_subjects_omits_the_field():
    item = {"id": "geo-6", "properties": {}}
    doc = build_envelope_feature_doc(
        item, catalog_id="cat", collection_id="col", grant_subjects=[],
    )
    assert "grant_subjects" not in doc


def test_no_access_fields_omits_them():
    item = {"id": "geo-7", "properties": {"name": "z"}}
    doc = build_envelope_feature_doc(item, catalog_id="cat", collection_id="col")
    assert "visibility" not in doc
    assert "owner" not in doc
    assert "grant_subjects" not in doc


def test_underscore_properties_are_stripped():
    item = {
        "id": "geo-8",
        "properties": {"name": "keep", "_internal": "drop", "_external_id": "ext"},
    }
    doc = build_envelope_feature_doc(item, catalog_id="cat", collection_id="col")
    assert doc["properties"] == {"name": "keep"}


def test_identity_fields_stamped():
    item = {"id": "geo-9", "_external_id": "ext-1", "_asset_id": "as-1", "properties": {}}
    doc = build_envelope_feature_doc(item, catalog_id="cat", collection_id="col")
    assert doc["geoid"] == "geo-9"
    assert doc["catalog_id"] == "cat"
    assert doc["collection_id"] == "col"
    assert doc["external_id"] == "ext-1"
    assert doc["asset_id"] == "as-1"


def test_geometry_and_bbox_carried():
    item = {
        "id": "geo-10",
        "geometry": {"type": "Point", "coordinates": [1, 2]},
        "bbox": [1, 2, 1, 2],
        "properties": {},
    }
    doc = build_envelope_feature_doc(item, catalog_id="cat", collection_id="col")
    assert doc["geometry"] == {"type": "Point", "coordinates": [1, 2]}
    assert doc["bbox"] == [1, 2, 1, 2]


# ---------------------------------------------------------------------------
# Driver class registration + identity
# ---------------------------------------------------------------------------


def test_config_class_key():
    assert (
        ItemsElasticsearchEnvelopeDriverConfig.class_key()
        == "items_elasticsearch_envelope_driver"
    )


def test_config_is_bound_to_driver():
    # Binding the config via TypedDriver[Config] is what makes class_key resolve
    # to the driver class name; assert_bound raises if the binding is missing.
    ItemsElasticsearchEnvelopeDriverConfig.assert_bound()


def test_driver_classvars():
    assert ItemsElasticsearchEnvelopeDriver.is_item_indexer is True
    assert ItemsElasticsearchEnvelopeDriver.auto_register_for_routing == frozenset()
    from dynastore.models.protocols.storage_driver import Capability
    caps = ItemsElasticsearchEnvelopeDriver.capabilities
    assert Capability.EXTERNAL_ID_TRACKING in caps
    assert Capability.TENANT_ISOLATED in caps
    assert Capability.READ in caps
    assert Capability.WRITE in caps


def test_envelope_fields_are_canonical():
    ef = ItemsElasticsearchEnvelopeDriver._envelope_fields
    assert ef.collection == "collection_id"
    assert ef.item_id == "geoid"
    assert ef.geoid == "geoid"
    assert ef.external_id == "external_id"


def test_index_name_seam():
    drv = ItemsElasticsearchEnvelopeDriver.__new__(ItemsElasticsearchEnvelopeDriver)
    # Patch the prefix resolution by going through the naming helper directly,
    # which is what _items_index_name composes.
    assert get_envelope_index_name("p", "cat").endswith("-envelope-items")
    assert get_envelope_index_name("p", "cat") == "p-cat-envelope-items"
    # _collection_routing returns None (single tenant-isolated index).
    assert drv._collection_routing("anything") is None


# ---------------------------------------------------------------------------
# Row-level seam: _query_request_to_es ANDs the access clause
# ---------------------------------------------------------------------------


def _drv() -> ItemsElasticsearchEnvelopeDriver:
    return ItemsElasticsearchEnvelopeDriver.__new__(ItemsElasticsearchEnvelopeDriver)


def test_query_without_access_filter_fails_closed():
    # An access-controlled driver MUST NOT serve an unfiltered query when no
    # read scope was established. A request with no ``access_filter`` means no
    # caller compiled a scope → deny by default (no leak). Trusted callers opt
    # into an unrestricted read explicitly via ``AccessFilter.allow_everything``
    # (see test_query_with_allow_all_no_deny_is_unscoped).
    drv = _drv()
    req = QueryRequest()
    body = drv._query_request_to_es(req, drv._envelope_fields)
    assert body == {"query": {"match_none": {}}}


def test_query_with_deny_all_produces_no_result_query():
    drv = _drv()
    req = QueryRequest(access_filter=AccessFilter.deny_everything())
    body = drv._query_request_to_es(req, drv._envelope_fields)
    # merge_es_filter wraps a leaf base (match_all collapses) — match_none wins.
    query = body["query"]
    # Either a bare match_none (match_all base collapsed) — assert it returns nothing.
    assert _es_clause_admits(query, {"geoid": "x", "visibility": "public"}) is False


def test_query_with_allow_all_no_deny_is_unscoped():
    drv = _drv()
    req = QueryRequest(access_filter=AccessFilter.allow_everything())
    body = drv._query_request_to_es(req, drv._envelope_fields)
    assert body == {"query": {"match_all": {}}}


def test_query_ands_access_clause_into_structural_query():
    drv = _drv()
    allow = AccessClause(predicates=(FieldPredicate("visibility", ("public",)),))
    req = QueryRequest(
        collections=["col-a"],
        access_filter=AccessFilter(allow=(allow,)),
    )
    body = drv._query_request_to_es(req, drv._envelope_fields)
    query = body["query"]
    # A doc in col-a but private must NOT match; public must match.
    assert _es_clause_admits(
        query, {"geoid": "1", "collection_id": "col-a", "visibility": "public"},
    ) is True
    assert _es_clause_admits(
        query, {"geoid": "2", "collection_id": "col-a", "visibility": "private"},
    ) is False
    # A public doc in a different collection must NOT match (structural scope).
    assert _es_clause_admits(
        query, {"geoid": "3", "collection_id": "col-b", "visibility": "public"},
    ) is False


def test_query_access_clause_ands_with_es_filter():
    drv = _drv()
    allow = AccessClause(predicates=(FieldPredicate("owner", ("alice",)),))
    req = QueryRequest(
        es_filter={"term": {"properties.kind": "field"}},
        access_filter=AccessFilter(allow=(allow,)),
    )
    body = drv._query_request_to_es(req, drv._envelope_fields)
    query = body["query"]
    # Owner alice + kind field -> match; wrong owner -> no match.
    assert _es_clause_admits(
        query, {"owner": "alice", "properties.kind": "field"},
    ) is True
    assert _es_clause_admits(
        query, {"owner": "bob", "properties.kind": "field"},
    ) is False
