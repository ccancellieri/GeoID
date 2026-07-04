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

"""CQL2 AST → Elasticsearch Query DSL translator (``es_common.cql_to_es``).

Each operator maps to its expected ES dict; field-mapping resolution covers
envelope vs ``properties`` vs ``extras`` and private-vs-public; an unknown
property raises; and a value carrying quotes/special chars stays a literal
``term`` value (injection-safe by construction).
"""
from __future__ import annotations

import pytest
from pygeofilter.parsers.cql2_json import parse as parse_cql2_json

from dynastore.models.protocols.field_definition import (
    FieldCapability,
    FieldDefinition,
)
from dynastore.modules.storage.drivers.es_common.cql_to_es import (
    UntranslatableFilterError,
    build_es_field_mapping,
    build_es_fulltext_mapping,
    cql_ast_to_es_query,
    merge_es_filter,
)

# A flat mapping where every property resolves to ``properties.<name>`` and the
# geometry field to ``geometry`` (mirrors the private flat doc).
_MAP = {
    "cloud_cover": "properties.cloud_cover",
    "name": "properties.name",
    "count": "properties.count",
    "geometry": "geometry",
    "datetime": "properties.datetime",
    "external_id": "external_id",
}


def _es(cql_json):
    return cql_ast_to_es_query(parse_cql2_json(cql_json), _MAP)


# --------------------------------------------------------------------------- #
# Operator coverage
# --------------------------------------------------------------------------- #

def test_equal_to_term():
    assert _es({"op": "=", "args": [{"property": "cloud_cover"}, 10]}) == {
        "term": {"properties.cloud_cover": 10}
    }


def test_not_equal_to_must_not_term():
    assert _es({"op": "<>", "args": [{"property": "cloud_cover"}, 10]}) == {
        "bool": {"must_not": [{"term": {"properties.cloud_cover": 10}}]}
    }


@pytest.mark.parametrize(
    "op,es_op",
    [("<", "lt"), ("<=", "lte"), (">", "gt"), (">=", "gte")],
)
def test_comparison_to_range(op, es_op):
    assert _es({"op": op, "args": [{"property": "count"}, 5]}) == {
        "range": {"properties.count": {es_op: 5}}
    }


def test_and_to_bool_filter():
    node = {
        "op": "and",
        "args": [
            {"op": "=", "args": [{"property": "cloud_cover"}, 10]},
            {"op": ">", "args": [{"property": "count"}, 5]},
        ],
    }
    assert _es(node) == {
        "bool": {
            "filter": [
                {"term": {"properties.cloud_cover": 10}},
                {"range": {"properties.count": {"gt": 5}}},
            ]
        }
    }


def test_or_to_bool_should():
    node = {
        "op": "or",
        "args": [
            {"op": "=", "args": [{"property": "cloud_cover"}, 10]},
            {"op": "=", "args": [{"property": "count"}, 5]},
        ],
    }
    assert _es(node) == {
        "bool": {
            "should": [
                {"term": {"properties.cloud_cover": 10}},
                {"term": {"properties.count": 5}},
            ],
            "minimum_should_match": 1,
        }
    }


def test_not_to_bool_must_not():
    node = {"op": "not", "args": [{"op": "=", "args": [{"property": "count"}, 5]}]}
    assert _es(node) == {
        "bool": {"must_not": [{"term": {"properties.count": 5}}]}
    }


def test_between_to_range():
    node = {"op": "between", "args": [{"property": "count"}, [1, 5]]}
    assert _es(node) == {
        "range": {"properties.count": {"gte": 1, "lte": 5}}
    }


def test_in_to_terms():
    node = {"op": "in", "args": [{"property": "count"}, [1, 2, 3]]}
    assert _es(node) == {"terms": {"properties.count": [1, 2, 3]}}


def test_like_to_wildcard():
    node = {"op": "like", "args": [{"property": "name"}, "%foo%"]}
    q = _es(node)
    assert q == {
        "wildcard": {
            "properties.name": {"value": "*foo*", "case_insensitive": False}
        }
    }


# --------------------------------------------------------------------------- #
# LIKE escape handling (#3006): an escaped wildcard/single-char/escape-char
# token in a CQL2 LIKE pattern must survive as its literal character, not be
# misread as a live ES wildcard.
# --------------------------------------------------------------------------- #

def test_like_to_wildcard_escaped_percent_stays_literal():
    """``\\%`` is an escaped literal percent, not the CQL2 wildcard."""
    node = {"op": "like", "args": [{"property": "name"}, "100\\%off"]}
    q = _es(node)
    assert q == {
        "wildcard": {
            "properties.name": {"value": "100%off", "case_insensitive": False}
        }
    }


def test_like_to_wildcard_escaped_singlechar_stays_literal():
    """``\\.`` is an escaped literal dot, not the CQL2 single-char wildcard."""
    node = {"op": "like", "args": [{"property": "name"}, "fo\\.o"]}
    q = _es(node)
    assert q == {
        "wildcard": {
            "properties.name": {"value": "fo.o", "case_insensitive": False}
        }
    }


def test_like_to_wildcard_escaped_escapechar_stays_literal():
    """``\\\\`` (escaped escape char) survives as a single literal backslash."""
    node = {"op": "like", "args": [{"property": "name"}, "a\\\\b"]}
    q = _es(node)
    assert q == {
        "wildcard": {
            "properties.name": {"value": "a\\b", "case_insensitive": False}
        }
    }


def test_like_to_wildcard_unescaped_wildcard_still_translates():
    """Unescaped wildcard/single-char tokens still translate normally
    alongside an escaped literal in the same pattern."""
    node = {"op": "like", "args": [{"property": "name"}, "100\\%%off"]}
    q = _es(node)
    assert q == {
        "wildcard": {
            "properties.name": {"value": "100%*off", "case_insensitive": False}
        }
    }


def test_like_on_fulltext_field_escaped_percent_stays_literal():
    """Same escape parity for the FULLTEXT ``match`` path (#1291 + #3006)."""
    field_map = {"title": "properties.title.keyword"}
    fulltext_map = {"title": "properties.title.text"}
    node = {"op": "like", "args": [{"property": "title"}, "100\\%off"]}
    q = cql_ast_to_es_query(parse_cql2_json(node), field_map, fulltext_map)
    assert q == {"match": {"properties.title.text": "100%off"}}


def test_isnull_to_must_not_exists():
    node = {"op": "isNull", "args": [{"property": "name"}]}
    assert _es(node) == {
        "bool": {"must_not": [{"exists": {"field": "properties.name"}}]}
    }


def test_spatial_intersects_to_geo_shape():
    node = {
        "op": "s_intersects",
        "args": [
            {"property": "geometry"},
            {"type": "Point", "coordinates": [1.0, 2.0]},
        ],
    }
    assert _es(node) == {
        "geo_shape": {
            "geometry": {
                "shape": {"type": "Point", "coordinates": [1.0, 2.0]},
                "relation": "intersects",
            }
        }
    }


def test_temporal_after_to_range():
    node = {
        "op": "t_after",
        "args": [
            {"property": "datetime"},
            {"timestamp": "2020-01-01T00:00:00Z"},
        ],
    }
    q = _es(node)
    assert "range" in q
    assert "properties.datetime" in q["range"]
    assert "gt" in q["range"]["properties.datetime"]


# --------------------------------------------------------------------------- #
# Field-mapping resolution
# --------------------------------------------------------------------------- #

def _fd(name, data_type="string", expose=True):
    return FieldDefinition(name=name, data_type=data_type, expose=expose)


def test_mapping_envelope_fields():
    fields = {
        "external_id": _fd("external_id"),
        "asset_id": _fd("asset_id"),
        "geoid": _fd("geoid"),
        "datetime": _fd("datetime", data_type="timestamp"),
        "the_geom": _fd("the_geom", data_type="geometry"),
    }
    m = build_es_field_mapping(fields, private=True)
    assert m["external_id"] == "external_id"
    assert m["asset_id"] == "asset_id"
    assert m["geoid"] == "geoid"
    assert m["datetime"] == "properties.datetime"
    # any geometry-typed field maps to the root geometry field
    assert m["the_geom"] == "geometry"


def test_mapping_private_flat_properties():
    fields = {"adm2_pcode": _fd("adm2_pcode"), "pop": _fd("pop", data_type="integer")}
    m = build_es_field_mapping(fields, private=True)
    # String attribute → ``.keyword`` sub-field (dynamic ES default maps strings
    # as analyzed ``text`` + ``keyword``; exact CQL match needs the sub-field).
    assert m["adm2_pcode"] == "properties.adm2_pcode.keyword"
    # Numeric attribute → queried directly, no ``.keyword``.
    assert m["pop"] == "properties.pop"


def test_mapping_public_known_vs_extras():
    fields = {
        "known": _fd("known", expose=True),
        "hidden": _fd("hidden", expose=False),
    }
    m = build_es_field_mapping(fields, private=False)
    # exposed → flat properties; not-exposed → extras bucket; both string →
    # ``.keyword`` sub-field for exact match.
    assert m["known"] == "properties.known.keyword"
    assert m["hidden"] == "properties.extras.hidden.keyword"


def test_mapping_keyword_subfield_only_for_strings():
    fields = {
        "code": _fd("code", data_type="string"),
        "uid": _fd("uid", data_type="uuid"),
        "count": _fd("count", data_type="integer"),
        "ts": _fd("ts", data_type="timestamp"),
        "flag": _fd("flag", data_type="boolean"),
    }
    m = build_es_field_mapping(fields, private=True)
    assert m["code"] == "properties.code.keyword"
    assert m["uid"] == "properties.uid.keyword"
    assert m["count"] == "properties.count"
    assert m["ts"] == "properties.ts"
    assert m["flag"] == "properties.flag"


def test_mapping_public_geometry_and_envelope_unaffected_by_expose():
    fields = {
        "geom": _fd("geom", data_type="geometry", expose=False),
        "external_id": _fd("external_id", expose=False),
    }
    m = build_es_field_mapping(fields, private=False)
    assert m["geom"] == "geometry"
    assert m["external_id"] == "external_id"


# --------------------------------------------------------------------------- #
# FULLTEXT capability (#1291): LIKE on a FULLTEXT field → analyzed ``match``
# over the ``.text`` sub-field; everything else keeps the ``wildcard`` behaviour.
# --------------------------------------------------------------------------- #

def _ft_fd(name, *, fulltext, data_type="string", expose=True):
    caps = [FieldCapability.FILTERABLE]
    if fulltext:
        caps.append(FieldCapability.FULLTEXT)
    return FieldDefinition(
        name=name, data_type=data_type, capabilities=caps, expose=expose
    )


def test_fulltext_mapping_only_lists_fulltext_strings():
    fields = {
        "title": _ft_fd("title", fulltext=True),
        "code": _ft_fd("code", fulltext=False),
        "pop": _ft_fd("pop", fulltext=True, data_type="integer"),  # non-string skipped
    }
    ft = build_es_fulltext_mapping(fields, private=True)
    assert ft == {"title": "properties.title.text"}
    # The exact-match mapping is unchanged for all of them (no regression).
    exact = build_es_field_mapping(fields, private=True)
    assert exact["title"] == "properties.title.keyword"
    assert exact["code"] == "properties.code.keyword"


def test_fulltext_mapping_public_extras_for_unexposed():
    fields = {"notes": _ft_fd("notes", fulltext=True, expose=False)}
    ft = build_es_fulltext_mapping(fields, private=False)
    assert ft == {"notes": "properties.extras.notes.text"}


def test_like_on_fulltext_field_uses_match():
    field_map = {"title": "properties.title.keyword"}
    fulltext_map = {"title": "properties.title.text"}
    node = {"op": "like", "args": [{"property": "title"}, "%annual report%"]}
    q = cql_ast_to_es_query(parse_cql2_json(node), field_map, fulltext_map)
    assert q == {"match": {"properties.title.text": "annual report"}}


def test_like_on_fulltext_field_negated():
    field_map = {"title": "properties.title.keyword"}
    fulltext_map = {"title": "properties.title.text"}
    node = {"op": "not", "args": [
        {"op": "like", "args": [{"property": "title"}, "%draft%"]}
    ]}
    q = cql_ast_to_es_query(parse_cql2_json(node), field_map, fulltext_map)
    assert q == {"bool": {"must_not": [{"match": {"properties.title.text": "draft"}}]}}


def test_like_without_fulltext_map_keeps_wildcard():
    """No fulltext mapping → historical ``wildcard`` behaviour, unchanged."""
    field_map = {"title": "properties.title.keyword"}
    node = {"op": "like", "args": [{"property": "title"}, "%foo%"]}
    q = cql_ast_to_es_query(parse_cql2_json(node), field_map)
    assert q == {
        "wildcard": {"properties.title.keyword": {"value": "*foo*", "case_insensitive": False}}
    }


def test_like_on_non_fulltext_field_keeps_wildcard_even_with_map():
    """A field absent from the fulltext map still uses ``wildcard``."""
    field_map = {"code": "properties.code.keyword", "title": "properties.title.keyword"}
    fulltext_map = {"title": "properties.title.text"}
    node = {"op": "like", "args": [{"property": "code"}, "%X%"]}
    q = cql_ast_to_es_query(parse_cql2_json(node), field_map, fulltext_map)
    assert q == {
        "wildcard": {"properties.code.keyword": {"value": "*X*", "case_insensitive": False}}
    }


# --------------------------------------------------------------------------- #
# Unknown property + injection safety
# --------------------------------------------------------------------------- #

def test_unknown_property_raises():
    node = {"op": "=", "args": [{"property": "does_not_exist"}, 1]}
    with pytest.raises(UntranslatableFilterError):
        cql_ast_to_es_query(parse_cql2_json(node), _MAP)


def test_unknown_property_is_value_error_subclass():
    # The dispatch catches ValueError to fall back to PG; ensure compatibility.
    assert issubclass(UntranslatableFilterError, ValueError)


def test_value_with_quotes_stays_literal_term():
    nasty = "o'brien\" OR 1=1 --"
    node = {"op": "=", "args": [{"property": "name"}, nasty]}
    q = _es(node)
    # The value is carried verbatim as a term value — no interpolation.
    assert q == {"term": {"properties.name": nasty}}
    assert q["term"]["properties.name"] == nasty


def test_in_values_with_special_chars_stay_literal():
    vals = ["a*b", "c?d", "{\"inject\": true}"]
    node = {"op": "in", "args": [{"property": "name"}, vals]}
    q = _es(node)
    assert q == {"terms": {"properties.name": vals}}


# --------------------------------------------------------------------------- #
# merge_es_filter
# --------------------------------------------------------------------------- #

def test_merge_into_match_all_collapses_to_clause():
    clause = {"term": {"properties.x": 1}}
    assert merge_es_filter({"match_all": {}}, clause) == clause
    assert merge_es_filter(None, clause) == clause
    assert merge_es_filter({}, clause) == clause


def test_merge_into_bool_appends_to_filter():
    base = {"bool": {"filter": [{"term": {"collection": "c"}}]}}
    clause = {"term": {"properties.x": 1}}
    out = merge_es_filter(base, clause)
    assert out == {
        "bool": {
            "filter": [
                {"term": {"collection": "c"}},
                {"term": {"properties.x": 1}},
            ]
        }
    }
    # input not mutated
    assert base == {"bool": {"filter": [{"term": {"collection": "c"}}]}}


def test_merge_wraps_leaf_base():
    base = {"term": {"collection": "c"}}
    clause = {"term": {"properties.x": 1}}
    assert merge_es_filter(base, clause) == {
        "bool": {
            "filter": [
                {"term": {"collection": "c"}},
                {"term": {"properties.x": 1}},
            ]
        }
    }
