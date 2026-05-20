"""Edge cases for the shared asset-filter builders (#1096).

The PG/ES driver tests exercise the common paths; these pin behaviour the
driver tests don't reach directly: the ``neq`` alias, boolean values on the
metadata accessor, and the scalar-value guard.
"""
from __future__ import annotations

import pytest

from dynastore.models.query_builder import AssetFilter, FilterOperator
from dynastore.modules.tools.asset_filters import build_es_query, build_pg_where


def test_neq_alias_collapses_to_ne_pg():
    parts, params = build_pg_where(
        [AssetFilter(field="asset_type", op=FilterOperator.NEQ, value="X")]
    )
    assert parts == ['"asset_type" != :af0']
    assert params == {"af0": "X"}


def test_neq_alias_collapses_to_ne_es():
    q = build_es_query([AssetFilter(field="asset_type", op=FilterOperator.NEQ, value="X")])
    assert q["bool"]["must_not"] == [{"term": {"asset_type": "X"}}]
    assert "filter" not in q["bool"]


def test_metadata_boolean_renders_as_text_token():
    parts, params = build_pg_where(
        [AssetFilter(field="metadata.active", op=FilterOperator.NE, value=True)]
    )
    # bool is NOT numeric-cast; the JSON #>> accessor yields the text 'true'
    assert parts == ["metadata #>> '{active}' != :af0"]
    assert params == {"af0": "true"}


def test_scalar_operator_rejects_list_value_pg():
    with pytest.raises(ValueError, match="expects a scalar value"):
        build_pg_where(
            [AssetFilter(field="size_bytes", op=FilterOperator.GT, value=[1, 2])]
        )


def test_scalar_operator_rejects_list_value_es():
    with pytest.raises(ValueError, match="expects a scalar value"):
        build_es_query(
            [AssetFilter(field="size_bytes", op=FilterOperator.GT, value=[1, 2])]
        )


def test_empty_filters_build_match_all_es():
    assert build_es_query([]) == {"match_all": {}}


def test_empty_filters_build_nothing_pg():
    parts, params = build_pg_where([])
    assert parts == []
    assert params == {}
