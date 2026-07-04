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

"""Regression test for #2945: the STAC ``/search`` PG fallback (#2943) must
not silently narrow a CQL2 ``filter`` an operator the ES translator would
have served faithfully.

``search_items``'s PG branch converts ``ItemSearchRequest.filter``
(``AttributeFilter``/``QueryFilter``) to a CQL2-JSON dict via
``_filter_to_cql2_json`` and compiles it with
``dynastore.modules.tools.cql.parse_cql2_json_filter`` — the exact same
conversion the ES branch (``_translate_filter_to_es``) uses before handing
the parsed AST to the ES CQL2->DSL translator. This test drives that same
conversion chain for a ``LIKE`` filter containing the CQL2 single-char
wildcard, which pygeofilter's bundled PG evaluator left un-adapted before
#2945 (mismatching the ES translator's own wildcard adaptation and matching
fewer rows than a real CQL2 caller — or the ES path — would expect).
"""

import pytest
from sqlalchemy.sql import column

from dynastore.extensions.stac.search import AttributeFilter, _filter_to_cql2_json
from dynastore.models.query_builder import FilterOperator
from dynastore.modules.tools.cql import parse_cql2_json_filter


def test_pg_fallback_like_singlechar_wildcard_now_matches_es_semantics():
    """A LIKE filter with a CQL2 ``.`` wildcard char used to compile to a
    literal-dot SQL pattern on the PG fallback — narrower than the ES path,
    which does adapt the wildcard. It now compiles to the equivalent SQL
    ``_`` wildcard, matching what the ES translator would have matched.
    """
    filt = AttributeFilter(field="name", operator=FilterOperator.LIKE, value="fo.o")
    cql2_json = _filter_to_cql2_json(filt)

    field_mapping = {"name": column("name")}
    sql, params = parse_cql2_json_filter(cql2_json, field_mapping=field_mapping)

    assert "ESCAPE" in sql
    assert list(params.values()) == ["fo_o"]


def test_pg_fallback_like_literal_underscore_no_longer_broadened():
    """A LIKE filter with a literal underscore (not a CQL2 wildcard token)
    used to reach SQL unescaped and match extra rows via SQL's own ``_``
    wildcard. It is now escaped, matching only the literal value.
    """
    filt = AttributeFilter(field="name", operator=FilterOperator.LIKE, value="foo_bar")
    cql2_json = _filter_to_cql2_json(filt)

    field_mapping = {"name": column("name")}
    _, params = parse_cql2_json_filter(cql2_json, field_mapping=field_mapping)

    assert list(params.values()) == ["foo\\_bar"]


def test_pg_fallback_has_no_wire_path_for_temporal_operators():
    """The STAC search wire filter (``AttributeFilter``/``FilterOperator``)
    has no temporal member at all — ``_filter_to_cql2_json`` only maps the
    comparison/LIKE/IN family (refs #2945 audit). A caller cannot construct
    a temporal predicate through this endpoint's filter today, on either the
    ES or the PG path, so there is no ES-vs-PG asymmetry to close here; the
    temporal parity fixes apply to the OGC API Features ``/items``
    ``filter=`` path, which compiles a raw CQL2 string through the same
    ``dynastore.modules.tools.cql`` module (see ``test_cql_parser.py``).
    """
    with pytest.raises(ValueError):
        FilterOperator("t_before")
