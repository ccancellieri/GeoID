"""Item-search filter coverage — #819 ``geoid`` + ``external_id``.

Pins the ES query DSL emitted by :func:`_build_item_query`:

* ``external_id`` → ``_external_id`` (the indexed mirror written by
  :class:`ItemsElasticsearchDriver` when the write policy resolves a
  computed external_id).
* ``geoid`` → ``id``: in this stack the STAC Item ``id`` IS the geoid,
  and it is the only field reliably indexed at the doc root for every
  item. The reserved ``properties.geoid`` / root ``geoid`` mappings
  exist but are never populated by the public ES writers, so targeting
  them produced empty hits (the bug filed in #819's second comment).
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


def test_geoid_filter_targets_root_id_field() -> None:
    """Item ``id`` IS the geoid — geoid filter must target the indexed
    root ``id`` keyword. Pinned because the prior implementation
    targeted unwritten ``properties.geoid`` / root ``geoid`` mappings
    and returned zero hits (#819 comment 2)."""
    body = SearchBody(geoid=["GEOID-1", "GEOID-2"])
    q = _build_item_query(body)
    assert {"terms": {"id": ["GEOID-1", "GEOID-2"]}} in _filters(q)


def test_geoid_and_external_id_compose_with_collections() -> None:
    """Filters are independent — combining all three narrows the result
    set without mutating each other's clauses."""
    body = SearchBody(
        geoid=["G1"],
        external_id=["X1"],
        collections=["coll-1"],
    )
    q = _build_item_query(body)
    flt = _filters(q)
    assert {"terms": {"id": ["G1"]}} in flt
    assert {"terms": {"_external_id": ["X1"]}} in flt
    assert {"terms": {"collection": ["coll-1"]}} in flt


def test_ids_and_geoid_filters_both_target_root_id() -> None:
    """``?ids=`` and ``?geoid=`` both target root ``id`` — this is by
    design (item id is the geoid in this stack). Combining them yields
    two independent ``terms`` clauses on the same field; ES intersects
    them, which is the correct narrowing behaviour."""
    body = SearchBody(ids=["item-1"], geoid=["G1"])
    flt = _filters(_build_item_query(body))
    id_clauses = [f for f in flt if f.get("terms", {}).get("id") is not None]
    assert {"terms": {"id": ["item-1"]}} in id_clauses
    assert {"terms": {"id": ["G1"]}} in id_clauses


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
