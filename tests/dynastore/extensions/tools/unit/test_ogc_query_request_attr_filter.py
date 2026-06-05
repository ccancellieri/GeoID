"""Unit tests for single-field attribute equality shorthand in
``parse_ogc_query_request`` (#1141).

These cover the ``?{property}={value}`` shorthand that is folded into the
existing CQL2 ``filter`` pipeline, plus the helpers that build and escape the
generated CQL2-Text equality clauses.
"""

from dynastore.extensions.tools.query import (
    OGC_RESERVED_QUERY_PARAMS,
    build_attribute_equality_cql,
    parse_ogc_query_request,
    _cql_escape_literal,
)


def test_single_property_shorthand_becomes_cql_equality():
    req = parse_ogc_query_request(extra_filters={"adm2_pcode": "PK001"})
    assert req.cql_filter == "adm2_pcode = 'PK001'"


def test_multiple_properties_are_anded():
    req = parse_ogc_query_request(
        extra_filters={"adm2_pcode": "PK001", "status": "active"}
    )
    # Each pair becomes an equality clause joined by AND (order preserved).
    assert "adm2_pcode = 'PK001'" in req.cql_filter
    assert "status = 'active'" in req.cql_filter
    assert " AND " in req.cql_filter


def test_explicit_filter_combined_with_shorthand():
    req = parse_ogc_query_request(
        filter="population > 1000",
        extra_filters={"adm2_pcode": "PK001"},
    )
    # Both the explicit CQL filter and the shorthand survive, each parenthesised.
    assert "(population > 1000)" in req.cql_filter
    assert "(adm2_pcode = 'PK001')" in req.cql_filter
    assert req.cql_filter.count(" AND ") == 1


def test_explicit_filter_only_is_passed_through_unwrapped():
    req = parse_ogc_query_request(filter="adm2_pcode='PK001'")
    assert req.cql_filter == "adm2_pcode='PK001'"


def test_no_filters_leaves_cql_filter_none():
    req = parse_ogc_query_request()
    assert req.cql_filter is None
    req2 = parse_ogc_query_request(extra_filters={})
    assert req2.cql_filter is None


def test_empty_value_is_skipped():
    # An empty value contributes no clause.
    assert build_attribute_equality_cql({"adm2_pcode": ""}) == "adm2_pcode = ''"
    # None values are dropped entirely.
    assert build_attribute_equality_cql({"adm2_pcode": None}) is None


def test_value_single_quote_is_escaped_for_cql():
    # Single quotes in the value are doubled so the generated CQL stays
    # well-formed; the value is bound as a parameter by the CQL backend.
    assert _cql_escape_literal("O'Brien") == "O''Brien"
    req = parse_ogc_query_request(extra_filters={"name": "O'Brien"})
    assert req.cql_filter == "name = 'O''Brien'"


def test_reserved_params_set_contains_core_ogc_params():
    for reserved in ("bbox", "datetime", "limit", "offset", "filter", "f", "crs"):
        assert reserved in OGC_RESERVED_QUERY_PARAMS


def test_reserved_params_set_contains_projection_and_geometry_params():
    # Regression: ``properties``, the geometry switches and the CRS overrides
    # are real endpoint parameters. If they are not reserved, the items
    # equality shorthand sweeps them into a bogus ``param = 'value'`` CQL
    # filter and the request 400s with "Unknown properties: <param>".
    for reserved in (
        "properties",
        "skipGeometry",
        "skip_geometry",
        "returnGeometry",
        "return_geometry",
        "filter-crs",
        "filter_crs",
        "bbox_crs",
    ):
        assert reserved in OGC_RESERVED_QUERY_PARAMS


def test_shorthand_property_name_is_not_interpolated_as_value():
    # The property name appears verbatim (it is validated against queryable
    # fields downstream); only the value is quoted as a CQL literal.
    cql = build_attribute_equality_cql({"adm2_pcode": "PK001"})
    assert cql == "adm2_pcode = 'PK001'"
