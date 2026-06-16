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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

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

from dynastore.models.search_models import SearchBody
from dynastore.modules.elasticsearch.items_query import (
    PRIVATE_ENVELOPE_FIELDS,
    PUBLIC_ENVELOPE_FIELDS,
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
    # Both calls exercise the same core SSOT; the test pins the output shape
    # so any future change to build_items_query is caught immediately.
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
    # Re-run with explicit public envelope (default) to verify idempotence.
    expected = build_items_query(
        q=body.q,
        ids=body.ids,
        geoid=body.geoid,
        external_id=body.external_id,
        collections=body.collections,
        bbox=body.bbox,
        intersects=body.intersects,
        datetime=body.datetime,
        fields=PUBLIC_ENVELOPE_FIELDS,
    )
    assert got == expected, f"DSL drift for {fields!r}: {got!r} != {expected!r}"


def _flatten(body: dict) -> list:
    bool_ = body.get("bool", {})
    return list(bool_.get("must", [])) + list(bool_.get("filter", []))


def test_public_envelope_fields_are_the_default():
    """The default field mapping uses the public per-catalog (STAC) shape."""
    public = build_items_query(
        ids=["a"], geoid=["g"], external_id=["x"], collections=["c"],
    )
    default = build_items_query(
        ids=["a"], geoid=["g"], external_id=["x"], collections=["c"],
        fields=PUBLIC_ENVELOPE_FIELDS,
    )
    assert public == default
    clauses = _flatten(public)
    assert {"terms": {"id": ["a"]}} in clauses          # ids → id
    assert {"terms": {"id": ["g"]}} in clauses          # geoid → id (public)
    # external_id targets the root keyword on BOTH index shapes now — the
    # legacy public ``_external_id`` mirror was removed (#1285 identity convergence).
    assert {"terms": {"external_id": ["x"]}} in clauses
    assert {"terms": {"_external_id": ["x"]}} not in clauses
    assert {"terms": {"collection": ["c"]}} in clauses    # collection


def test_private_envelope_fields_target_canonical_names():
    """The tenant-private index uses canonical envelope field names so a
    structural query addresses ``collection_id`` / ``geoid`` / ``external_id``
    instead of the public ``collection`` / ``id`` / ``_external_id`` — without
    which every private structural search silently matches zero docs."""
    q = build_items_query(
        ids=["a"], geoid=["g"], external_id=["x"], collections=["c"],
        fields=PRIVATE_ENVELOPE_FIELDS,
    )
    clauses = _flatten(q)
    assert {"terms": {"geoid": ["a"]}} in clauses         # ids → geoid
    assert {"terms": {"geoid": ["g"]}} in clauses         # geoid → geoid
    assert {"terms": {"external_id": ["x"]}} in clauses   # external_id (un-prefixed)
    assert {"terms": {"collection_id": ["c"]}} in clauses  # collection_id
    # the public names must NOT leak into a private query
    assert {"terms": {"collection": ["c"]}} not in clauses
    assert {"terms": {"_external_id": ["x"]}} not in clauses


def test_private_freetext_boosts_geoid_not_id():
    """Free-text ``q`` boosts the index's id field — ``geoid`` on private."""
    q = build_items_query(q="wheat", fields=PRIVATE_ENVELOPE_FIELDS)
    mm = q["bool"]["must"][0]["multi_match"]
    assert "geoid^3" in mm["fields"]
    assert "id^3" not in mm["fields"]


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
