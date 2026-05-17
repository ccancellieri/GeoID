"""Unit tests for the items projection helper + factory mapping shape.

Five anchor tests (#887 commit 1):

* **a** — round-trip projection: known props stay, unknown move to ``extras``.
* **b** — sort path rewrites unknown ``properties.<key>`` to ``properties.extras.<key>``.
* **c** — mapping pin: ``ITEM_MAPPING`` is ``dynamic: false`` at root and properties subtree
  (the runtime ES rejection of unknown top-level keys is exercised in integration).
* **d** — mapping pin: internal ``_*`` write-trackers are whitelisted in ``COMMON_PROPERTIES``.
* **e** — fan-out: an item with hundreds of unknown columnar keys keeps top-level
  ``properties`` bounded; the long tail lives under ``extras``.
"""
from __future__ import annotations

from typing import Any, Dict

import pytest

from dynastore.modules.elasticsearch.items_projection import (
    TIER_1_FIELDS,
    build_known_fields,
    project_item_for_es,
    resolve_es_field_path,
)
from dynastore.modules.elasticsearch.mappings import (
    COMMON_PROPERTIES,
    ITEM_MAPPING,
    build_item_mapping,
)


# --- a -----------------------------------------------------------------


def test_project_item_for_es_round_trip_known_and_unknown() -> None:
    known = build_known_fields()
    doc: Dict[str, Any] = {
        "id": "item-1",
        "type": "Feature",
        "collection": "col",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "bbox": [0, 0, 0, 0],
        "properties": {
            # Tier 1
            "datetime": "2026-05-17T00:00:00Z",
            "eo:cloud_cover": 12.3,
            "proj:epsg": 4326,
            # Unknown — must route to extras
            "vendor:custom_alpha": "value",
            "fao:custom_score": 0.42,
        },
    }
    out = project_item_for_es(doc, known)

    assert out["properties"]["datetime"] == "2026-05-17T00:00:00Z"
    assert out["properties"]["eo:cloud_cover"] == 12.3
    assert out["properties"]["proj:epsg"] == 4326

    assert "vendor:custom_alpha" not in out["properties"]
    assert "fao:custom_score" not in out["properties"]
    extras = out["properties"]["extras"]
    assert extras == {"vendor:custom_alpha": "value", "fao:custom_score": 0.42}

    # Pure function: input is not mutated.
    assert "extras" not in doc["properties"]
    assert "vendor:custom_alpha" in doc["properties"]


def test_project_item_for_es_idempotent_when_extras_already_present() -> None:
    known = build_known_fields()
    doc = {
        "properties": {
            "datetime": "2026-05-17T00:00:00Z",
            "extras": {"prev:field": 1},
            "vendor:new": 2,
        }
    }
    out = project_item_for_es(doc, known)
    assert out["properties"]["extras"] == {"prev:field": 1, "vendor:new": 2}


def test_project_item_for_es_passthrough_when_no_properties() -> None:
    doc = {"id": "x", "geometry": None}
    out = project_item_for_es(doc, build_known_fields())
    assert out == doc


# --- b -----------------------------------------------------------------


def test_resolve_es_field_path_known_passthrough() -> None:
    known = build_known_fields()
    assert resolve_es_field_path("properties.datetime", known) == "properties.datetime"
    assert resolve_es_field_path("properties.eo:cloud_cover", known) == "properties.eo:cloud_cover"
    # Sub-paths under a Tier-1 object field stay intact so ES resolves
    # the leaf via the parent's sub-properties.
    assert resolve_es_field_path("properties.title.en", known) == "properties.title.en"


def test_resolve_es_field_path_unknown_routes_to_extras() -> None:
    known = build_known_fields()
    assert (
        resolve_es_field_path("properties.foo:bar", known)
        == "properties.extras.foo:bar"
    )
    assert (
        resolve_es_field_path("properties.vendor:custom", known)
        == "properties.extras.vendor:custom"
    )


def test_resolve_es_field_path_top_level_passthrough() -> None:
    known = build_known_fields()
    assert resolve_es_field_path("_external_id", known) == "_external_id"
    assert resolve_es_field_path("geometry", known) == "geometry"
    assert resolve_es_field_path("bbox", known) == "bbox"
    assert resolve_es_field_path("id", known) == "id"


def test_parse_sort_routes_unknown_field_to_extras() -> None:
    """End-to-end sort rewrite through the search service.

    Pinning here (and not just in resolve_es_field_path) catches a future
    refactor that bypasses the resolver in ``_parse_sort``.
    """
    from dynastore.extensions.search.search_service import _parse_sort

    clause = _parse_sort("properties.foo:bar")
    # Returns [{<field>: {order}}, {_score: {order}}]
    field_clause = clause[0]
    assert "properties.extras.foo:bar" in field_clause
    assert field_clause["properties.extras.foo:bar"]["order"] == "asc"


def test_parse_sort_known_field_passthrough() -> None:
    from dynastore.extensions.search.search_service import _parse_sort

    clause = _parse_sort("-properties.datetime")
    field_clause = clause[0]
    assert "properties.datetime" in field_clause
    assert field_clause["properties.datetime"]["order"] == "desc"


# --- c -----------------------------------------------------------------


def test_item_mapping_is_strict_at_root_and_properties() -> None:
    """Pin the strict ``dynamic: false`` shape at both levels.

    ES enforces unknown-field rejection at runtime; this test ensures the
    mapping we ship has the contract wired before it ever reaches ES.
    """
    assert ITEM_MAPPING["dynamic"] is False
    nested = ITEM_MAPPING["properties"]["properties"]
    assert nested["dynamic"] is False
    # The extras lane must be dynamic so the long tail can land somewhere.
    extras = nested["properties"]["extras"]
    assert extras["type"] == "object"
    assert extras["dynamic"] is True


def test_item_mapping_includes_all_tier_1_fields() -> None:
    """Every Tier-1 field must appear in the items mapping properties subtree."""
    nested_props = ITEM_MAPPING["properties"]["properties"]["properties"]
    for name in TIER_1_FIELDS:
        assert name in nested_props, f"Tier-1 field {name!r} missing from items mapping"


# --- d -----------------------------------------------------------------


def test_common_properties_includes_internal_write_trackers() -> None:
    """Internal ``_*`` fields set by the items driver write path must be
    whitelisted at the strict-root level so writes survive ingestion.

    If a future refactor adds another internal tracker on
    ItemsElasticsearchDriver.write_entities, this test surfaces the gap
    by failing — extend COMMON_PROPERTIES *and* this test together.
    """
    for required in (
        "_asset_id",
        "_external_id",
        "_valid_from",
        "_valid_to",
        "_simplification_factor",
        "_simplification_mode",
    ):
        assert required in COMMON_PROPERTIES, (
            f"{required} must be declared in COMMON_PROPERTIES so the "
            "strict items mapping accepts it"
        )
    # Sanity-check the strict root accepts them.
    root_props = ITEM_MAPPING["properties"]
    assert "_asset_id" in root_props
    assert "_simplification_factor" in root_props


# --- e -----------------------------------------------------------------


def test_iceberg_style_fan_out_keeps_top_level_bounded() -> None:
    """Simulate a wide parquet-derived item (~500 columns).

    Top-level ``properties`` after projection must contain only Tier-1
    fields that appeared in the doc; everything else lives under
    ``extras``. This is the load-bearing guard against unbounded mapping
    growth from columnar reverse-projection (iceberg.py:720).
    """
    known = build_known_fields()
    props: Dict[str, Any] = {
        # A handful of Tier-1 fields that a wide table might carry.
        "datetime": "2026-05-17T00:00:00Z",
        "eo:cloud_cover": 5.0,
        # The long tail — synthetic columnar columns.
        **{f"col_{i:04d}": i for i in range(500)},
    }
    doc = {"id": "x", "properties": props}
    out = project_item_for_es(doc, known)

    # Top-level properties should be: every Tier-1 key that appeared
    # (datetime, eo:cloud_cover) plus exactly one ``extras`` bucket.
    top_keys = set(out["properties"].keys())
    assert top_keys == {"datetime", "eo:cloud_cover", "extras"}

    extras = out["properties"]["extras"]
    assert len(extras) == 500
    assert "col_0000" in extras
    assert "col_0499" in extras
    # And nothing from Tier 1 leaked into extras.
    assert "datetime" not in extras
    assert "eo:cloud_cover" not in extras


# --- factory regression ----------------------------------------------


def test_build_item_mapping_respects_custom_known_fields() -> None:
    """The factory must surface custom known-fields (foundation for Tier 2)."""
    custom: Dict[str, Dict[str, Any]] = {
        "fao:custom_score": {"type": "float"},
        "fao:custom_label": {"type": "keyword"},
    }
    mapping = build_item_mapping(custom)
    nested = mapping["properties"]["properties"]["properties"]
    assert nested["fao:custom_score"] == {"type": "float"}
    assert nested["fao:custom_label"] == {"type": "keyword"}
    # ``extras`` lane is always present.
    assert nested["extras"]["dynamic"] is True


@pytest.mark.parametrize(
    "field,known,expected",
    [
        ("properties.datetime", True, "properties.datetime"),
        ("properties.unknown:thing", False, "properties.extras.unknown:thing"),
        ("properties.eo:cloud_cover", True, "properties.eo:cloud_cover"),
    ],
)
def test_resolve_es_field_path_parametric(field: str, known: bool, expected: str) -> None:
    out = resolve_es_field_path(field, build_known_fields())
    assert out == expected
