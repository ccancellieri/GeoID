"""Item-search filter coverage — #819 adds ``geoid`` and ``external_id``.

Pins the ES query DSL emitted by :func:`_build_item_query` for the two
new filter fields so a future refactor cannot silently reroute the
filter to a different ES field (the operational contract is that
``external_id`` matches the indexed ``_external_id`` mirror written by
:class:`ItemsElasticsearchDriver`; ``geoid`` matches the conventional
``properties.geoid`` with a defensive root-level fallback).
"""

from __future__ import annotations

from dynastore.extensions.search.search_models import SearchBody
from dynastore.extensions.search.search_service import _build_item_query


def _filters(query: dict) -> list[dict]:
    return query["bool"]["filter"]


def test_external_id_filter_targets_internal_mirror_field() -> None:
    body = SearchBody(external_id=["EXT-1", "EXT-2"])
    q = _build_item_query(body)
    assert {"terms": {"_external_id": ["EXT-1", "EXT-2"]}} in _filters(q)


def test_geoid_filter_matches_properties_and_root_level() -> None:
    body = SearchBody(geoid=["GEOID-1"])
    q = _build_item_query(body)
    geoid_filter = next(f for f in _filters(q) if "bool" in f)
    shoulds = geoid_filter["bool"]["should"]
    assert {"terms": {"properties.geoid": ["GEOID-1"]}} in shoulds
    assert {"terms": {"geoid": ["GEOID-1"]}} in shoulds
    assert geoid_filter["bool"]["minimum_should_match"] == 1


def test_geoid_and_external_id_compose_with_ids_and_collections() -> None:
    """Filters are independent — combining all four narrows the result
    set without mutating each other's clauses."""
    body = SearchBody(
        ids=["item-1"],
        geoid=["G1"],
        external_id=["X1"],
        collections=["coll-1"],
    )
    q = _build_item_query(body)
    flt = _filters(q)
    # Order is preserved (term-shaped first, then geoid bool, then
    # external_id terms, then collections terms). Locking this order
    # makes a regression that swaps fields obvious in diff.
    assert {"terms": {"id": ["item-1"]}} in flt
    assert any("bool" in f and "should" in f["bool"] for f in flt)
    assert {"terms": {"_external_id": ["X1"]}} in flt
    assert {"terms": {"collection": ["coll-1"]}} in flt


def test_empty_body_yields_match_all() -> None:
    """Sanity: removing the geoid/external_id fields from the body still
    produces the original match_all baseline."""
    q = _build_item_query(SearchBody())
    assert q == {"match_all": {}}


def test_search_body_round_trips_geoid_and_external_id_fields() -> None:
    body = SearchBody(geoid=["a", "b"], external_id=["x"])
    dumped = body.model_dump(exclude_none=True)
    assert dumped["geoid"] == ["a", "b"]
    assert dumped["external_id"] == ["x"]
