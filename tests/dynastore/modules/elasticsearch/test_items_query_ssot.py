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
"""Golden test: the core ES items-query DSL SSOT must emit byte-identical
output to the legacy ``SearchService._build_item_query`` it replaces.

This pins ``dynastore.modules.elasticsearch.items_query.build_items_query``
(the routing-aware single source of truth, consumed by both the ES storage
drivers' structural search and the search extension) against the previous
search-extension-local builder, so the convergence carries zero behaviour
change for the public search path.
"""
from __future__ import annotations

import pytest

from dynastore.extensions.search.search_models import SearchBody
from dynastore.extensions.search.search_service import _build_item_query
from dynastore.modules.elasticsearch.items_query import (
    build_items_query,
    parse_datetime_filter,
)

# A representative matrix of SearchBody field combinations exercising every
# branch of the legacy builder: free-text, ids/geoid/external_id, collection
# scoping, bbox vs intersects (mutually exclusive), datetime (closed / open /
# instant ranges), and combinations.
_POLYGON = {
    "type": "Polygon",
    "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
}

_CASES = [
    {},
    {"q": "wheat"},
    {"ids": ["a", "b"]},
    {"geoid": ["g1"]},
    {"external_id": ["x1", "x2"]},
    {"collections": ["c1", "c2"]},
    {"bbox": [-10.0, -5.0, 10.0, 5.0]},
    {"intersects": _POLYGON},
    {"bbox": [-10.0, -5.0, 10.0, 5.0], "intersects": _POLYGON},  # bbox wins
    {"datetime": "2020-01-01T00:00:00Z/2020-12-31T23:59:59Z"},
    {"datetime": "../2020-12-31T23:59:59Z"},
    {"datetime": "2020-01-01T00:00:00Z/.."},
    {"datetime": "2020-06-15T00:00:00Z"},
    {
        "q": "maize",
        "ids": ["a"],
        "collections": ["c1"],
        "bbox": [0.0, 0.0, 1.0, 1.0],
        "datetime": "2021-01-01T00:00:00Z/..",
    },
]


@pytest.mark.parametrize("fields", _CASES)
def test_build_items_query_matches_legacy_builder(fields):
    body = SearchBody(**fields)
    expected = _build_item_query(body)
    got = build_items_query(
        q=body.q,
        ids=body.ids,
        geoid=body.geoid,
        external_id=body.external_id,
        collections=body.collections,
        bbox=body.bbox,
        intersects=body.intersects,
        datetime=body.datetime,
    )
    assert got == expected, f"DSL drift for {fields!r}: {got!r} != {expected!r}"


@pytest.mark.parametrize(
    "dt,expected",
    [
        (None, None),
        (
            "2020-01-01T00:00:00Z/2020-12-31T23:59:59Z",
            {"range": {"properties.datetime": {
                "gte": "2020-01-01T00:00:00Z", "lte": "2020-12-31T23:59:59Z"}}},
        ),
        (
            "../2020-12-31T23:59:59Z",
            {"range": {"properties.datetime": {"lte": "2020-12-31T23:59:59Z"}}},
        ),
        (
            "2020-01-01T00:00:00Z/..",
            {"range": {"properties.datetime": {"gte": "2020-01-01T00:00:00Z"}}},
        ),
        (
            "2020-06-15T00:00:00Z",
            {"bool": {"should": [
                {"term": {"properties.datetime": "2020-06-15T00:00:00Z"}},
                {"range": {"properties.datetime": {"lte": "2020-06-15T00:00:00Z"}}},
            ]}},
        ),
    ],
)
def test_parse_datetime_filter_snapshot(dt, expected):
    """Durable contract snapshot of the datetime → ES range mapping."""
    assert parse_datetime_filter(dt) == expected
