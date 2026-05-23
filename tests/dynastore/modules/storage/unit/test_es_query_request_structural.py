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

"""``_ItemsElasticsearchBase._query_request_to_es`` honors the full set of
structural-search dimensions by routing through the ``build_items_query`` SSOT.

This is the foundation for retiring the materialized ``search_items_struct``:
once the streaming ``read_entities``/``count_entities`` path (which both build
their ES body via ``_query_request_to_es``) understands ids / collections /
bbox / intersects / datetime carried on a ``QueryRequest``, the structural
ES "fast path" no longer needs a separate materialized method.

Pure-unit: drives the static translator directly, no ES client, no DB.
"""
from __future__ import annotations

from dynastore.models.query_builder import FilterCondition, QueryRequest
from dynastore.modules.storage.drivers.elasticsearch import _ItemsElasticsearchBase


def _to_es(request: QueryRequest) -> dict:
    return _ItemsElasticsearchBase._query_request_to_es(request)


def _clauses(body: dict) -> list:
    """Flatten every must/filter clause of the produced bool query."""
    q = body.get("query", {})
    bool_ = q.get("bool", {})
    return list(bool_.get("must", [])) + list(bool_.get("filter", []))


def test_item_ids_become_terms_on_id():
    body = _to_es(QueryRequest(item_ids=["a", "b"]))
    assert {"terms": {"id": ["a", "b"]}} in _clauses(body)


def test_collections_become_terms_on_collection():
    body = _to_es(QueryRequest(collections=["c1", "c2"]))
    assert {"terms": {"collection": ["c1", "c2"]}} in _clauses(body)


def test_bbox_becomes_geo_shape_envelope():
    body = _to_es(QueryRequest(bbox=[-10.0, -5.0, 10.0, 5.0]))
    geo = [c for c in _clauses(body) if "geo_shape" in c]
    assert geo, f"expected a geo_shape clause, got {_clauses(body)}"
    shape = geo[0]["geo_shape"]["geometry"]["shape"]
    assert shape["type"] == "envelope"


def test_intersects_becomes_geo_shape():
    geom = {"type": "Point", "coordinates": [1.0, 2.0]}
    body = _to_es(QueryRequest(intersects=geom))
    geo = [c for c in _clauses(body) if "geo_shape" in c]
    assert geo, f"expected a geo_shape clause, got {_clauses(body)}"
    assert geo[0]["geo_shape"]["geometry"]["shape"] == geom


def test_datetime_becomes_temporal_filter():
    body = _to_es(QueryRequest(datetime="2020-01-01T00:00:00Z/2020-12-31T00:00:00Z"))
    # build_items_query emits a range/date clause via parse_datetime_filter;
    # whatever its exact shape, a non-empty filter must be present.
    assert _clauses(body), "datetime should produce at least one filter clause"


def test_attribute_eq_filter_still_supported():
    body = _to_es(QueryRequest(filters=[FilterCondition(field="properties.x", operator="eq", value=7)]))
    assert {"term": {"properties.x": 7}} in _clauses(body)


def test_empty_request_is_match_all():
    body = _to_es(QueryRequest())
    assert body == {"query": {"match_all": {}}}


def _filter_clauses(body: dict) -> list:
    return list(body.get("query", {}).get("bool", {}).get("filter", []))


def test_read_body_single_collection_forces_term_and_routing():
    body, params = _ItemsElasticsearchBase._build_read_search_body("col1", QueryRequest(), 100, 0)
    assert params["routing"] == "col1"
    assert {"term": {"collection": "col1"}} in _filter_clauses(body)


def test_read_body_multi_collection_no_forced_term_no_routing():
    req = QueryRequest(collections=["c1", "c2"])
    body, params = _ItemsElasticsearchBase._build_read_search_body("c1", req, 100, 0)
    # Multi-collection queries all shards: no single-collection routing.
    assert "routing" not in params
    # Scoping comes from the terms filter (build_items_query), not a forced
    # single-collection term clause.
    assert {"term": {"collection": "c1"}} not in _filter_clauses(body)
    assert {"terms": {"collection": ["c1", "c2"]}} in _filter_clauses(body)


def test_read_body_limit_offset_from_request_override_args():
    body, params = _ItemsElasticsearchBase._build_read_search_body(
        "c1", QueryRequest(limit=5, offset=10), 100, 0
    )
    assert params["size"] == "5"
    assert params["from"] == "10"


def test_structural_and_attribute_filters_combine():
    body = _to_es(
        QueryRequest(
            collections=["c1"],
            filters=[FilterCondition(field="properties.x", operator="eq", value=7)],
        )
    )
    clauses = _clauses(body)
    assert {"terms": {"collection": ["c1"]}} in clauses
    assert {"term": {"properties.x": 7}} in clauses
