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
    strip_reserved_members,
    unproject_item_from_es,
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


# --- #1212: GeoJSON/STAC structural members are item-level identity, never user
# properties. When one leaks into ``properties`` (e.g. the input feature's
# top-level ``id`` preserved as ``properties.id``) it must be dropped, not routed
# into ``extras`` — otherwise it surfaces as ``extras: {"id": ...}`` on read and
# the STAC endpoint mis-coerces it to a bare string.


def test_project_item_for_es_drops_leaked_reserved_id_from_properties() -> None:
    known = build_known_fields()
    doc = {
        "id": "feat-1",
        "type": "Feature",
        "properties": {"datetime": "2026-05-17T00:00:00Z", "id": "feat-1"},
    }
    out = project_item_for_es(doc, known)
    assert out["properties"] == {"datetime": "2026-05-17T00:00:00Z"}
    assert "extras" not in out["properties"]
    # Top-level identity is untouched.
    assert out["id"] == "feat-1"


def test_project_item_for_es_drops_all_reserved_members_from_properties() -> None:
    known = build_known_fields()
    doc = {
        "id": "feat-2",
        "properties": {
            "datetime": "2026-05-17T00:00:00Z",
            "id": "feat-2",
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "bbox": [0, 0, 0, 0],
            "collection": "col",
        },
    }
    out = project_item_for_es(doc, known)
    assert out["properties"] == {"datetime": "2026-05-17T00:00:00Z"}


# --- #1232: strip_reserved_members is the shared boundary helper used by the
# POST/PUT echo and raw-row read fallback (ogc_generator._db_row_to_ogc_feature)
# so the create response matches the GET read contract for both PG and ES.


def test_strip_reserved_members_drops_leaked_id() -> None:
    out = strip_reserved_members(
        {"datetime": "2026-05-17T00:00:00Z", "id": "019e507b-64c9-72a1"}
    )
    assert out == {"datetime": "2026-05-17T00:00:00Z"}


def test_strip_reserved_members_drops_all_reserved_members() -> None:
    out = strip_reserved_members(
        {
            "datetime": "2026-05-17T00:00:00Z",
            "id": "feat-3",
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "bbox": [0, 0, 0, 0],
            "links": [],
            "assets": {},
            "collection": "col",
            "stac_version": "1.0.0",
            "stac_extensions": [],
        }
    )
    assert out == {"datetime": "2026-05-17T00:00:00Z"}


def test_strip_reserved_members_preserves_geoid_property() -> None:
    # expose_geoid surfaces a ``geoid`` property (not a reserved member) — it
    # must survive the strip; only structural identity (``id``) is removed.
    out = strip_reserved_members(
        {"datetime": "2026-05-17T00:00:00Z", "geoid": "019e507b", "id": "019e507b"}
    )
    assert out == {"datetime": "2026-05-17T00:00:00Z", "geoid": "019e507b"}


def test_strip_reserved_members_returns_fresh_dict() -> None:
    src = {"datetime": "2026-05-17T00:00:00Z", "id": "x"}
    out = strip_reserved_members(src)
    assert out is not src
    assert "id" in src  # input not mutated


def test_strip_reserved_members_passthrough_non_dict() -> None:
    assert strip_reserved_members(None) is None  # type: ignore[arg-type]


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
    """End-to-end sort rewrite through the items-query SSOT.

    Pinning here (and not just in resolve_es_field_path) catches a future
    refactor that bypasses the resolver in ``parse_sort``.
    """
    from dynastore.modules.elasticsearch.items_query import parse_sort

    clause = parse_sort("properties.foo:bar")
    # Returns [{<field>: {order}}, {_score: {order}}]
    field_clause = clause[0]
    assert "properties.extras.foo:bar" in field_clause
    assert field_clause["properties.extras.foo:bar"]["order"] == "asc"


def test_parse_sort_known_field_passthrough() -> None:
    from dynastore.modules.elasticsearch.items_query import parse_sort

    clause = parse_sort("-properties.datetime")
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
    # The extras lane must be a single ``flattened`` field — the whole
    # bucket counts as one mapping entry regardless of how many distinct
    # leaf keys arrive across the collections sharing this per-catalog
    # index (#1295 cap-safe long-tail).
    extras = nested["properties"]["extras"]
    assert extras == {"type": "flattened"}


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
        "_valid_from",
        "_valid_to",
        "_simplification_factor",
        "_simplification_mode",
        # Cap-safe analyzed catch-all populated at write time from the
        # ``properties.extras`` values (#1295).
        "_search_text",
    ):
        assert required in COMMON_PROPERTIES, (
            f"{required} must be declared in COMMON_PROPERTIES so the "
            "strict items mapping accepts it"
        )
    # ``_external_id`` / ``_asset_id`` were removed in #1285 identity
    # convergence — identity lives only on the root ``external_id`` /
    # ``asset_id`` keywords, so the driver no longer writes the ``_``-mirrors
    # and they must NOT be declared.
    for removed in ("_external_id", "_asset_id"):
        assert removed not in COMMON_PROPERTIES
    assert "external_id" in COMMON_PROPERTIES and "asset_id" in COMMON_PROPERTIES
    # Sanity-check the strict root accepts them.
    root_props = ITEM_MAPPING["properties"]
    assert "_asset_id" not in root_props
    assert root_props["external_id"] == {"type": "keyword"}
    assert "_simplification_factor" in root_props
    assert root_props["_search_text"] == {"type": "text", "analyzer": "standard"}


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
    # ``extras`` lane is always present as a single ``flattened`` field.
    assert nested["extras"] == {"type": "flattened"}


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


# --- unproject (read path) ---------------------------------------------
#
# Inverse of project_item_for_es: rebuild the GeoJSON/STAC read contract
# from an indexed _source. Regression guard for the malformed feature read
# where ES served items with attributes nested under properties.extras,
# internal/echo keys leaked at the top level, and geometry was {}.


def test_unproject_hoists_extras_to_flat_properties() -> None:
    src: Dict[str, Any] = {
        "type": "Feature",
        "id": "325",
        "geometry": {},
        "collection": "glosis_demo",
        "catalog_id": "datamgr01",
        "_external_id": "325",
        "_asset_id": "ALBL1_01",
        "properties": {
            "extras": {
                "CODE": "325",
                "NAME": "Lushnje",
                "area": 580580547.78,
            }
        },
        # leaked top-level echo (surfaces via Feature extra="allow")
        "CODE": "325",
        "NAME": "Lushnje",
        "area": 580580547.78,
    }
    out = unproject_item_from_es(src)

    assert out["type"] == "Feature"
    assert out["id"] == "325"
    assert out["properties"] == {
        "CODE": "325",
        "NAME": "Lushnje",
        "area": 580580547.78,
    }
    assert "extras" not in out["properties"]
    # No internal fields, no top-level attribute echo, no catalog_id leak.
    for leaked in (
        "CODE",
        "NAME",
        "area",
        "catalog_id",
        "_external_id",
        "_asset_id",
    ):
        assert leaked not in out
    # collection is a structural STAC member — preserved.
    assert out["collection"] == "glosis_demo"
    # empty {} geometry normalised to a valid GeoJSON null geometry.
    assert out["geometry"] is None


def test_unproject_keeps_real_geometry_and_bbox() -> None:
    src = {
        "type": "Feature",
        "id": "1",
        "geometry": {"type": "Point", "coordinates": [1, 2]},
        "bbox": [1, 2, 1, 2],
        "properties": {"extras": {"a": 1}},
    }
    out = unproject_item_from_es(src)
    assert out["geometry"] == {"type": "Point", "coordinates": [1, 2]}
    assert out["bbox"] == [1, 2, 1, 2]
    assert out["properties"] == {"a": 1}


def test_unproject_flat_key_wins_on_collision() -> None:
    src = {
        "type": "Feature",
        "id": "1",
        "geometry": None,
        "properties": {"k": "flat", "extras": {"k": "nested", "j": "only"}},
    }
    out = unproject_item_from_es(src)
    assert out["properties"]["k"] == "flat"
    assert out["properties"]["j"] == "only"


def test_unproject_is_inverse_of_project() -> None:
    known = build_known_fields()
    original = {
        "type": "Feature",
        "id": "x",
        "collection": "c",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "properties": {
            "datetime": "2026-01-01T00:00:00Z",  # Tier 1 — stays flat
            "CODE": "325",  # unknown — projected to extras
            "NAME": "L",
        },
    }
    projected = project_item_for_es(original, known)
    assert "extras" in projected["properties"]  # sanity: projection happened

    restored = unproject_item_from_es(projected)
    assert restored["properties"]["datetime"] == "2026-01-01T00:00:00Z"
    assert restored["properties"]["CODE"] == "325"
    assert restored["properties"]["NAME"] == "L"
    assert "extras" not in restored["properties"]


def test_unproject_non_dict_passthrough() -> None:
    assert unproject_item_from_es(None) is None
    assert unproject_item_from_es("x") == "x"


# --- f: cap-safe long-tail (#1295) -----------------------------------------
#
# The ``extras`` lane is a single ``flattened`` field (exact-key only)
# paired with a root ``_search_text`` analyzed catch-all populated at
# write time from the same extras values. Together: "fulltext or
# better" at exactly two mapping entries no matter how many distinct
# extension keys arrive across the collections sharing the per-catalog
# index.


def test_project_populates_search_text_from_extras_values() -> None:
    known = build_known_fields()
    doc = {
        "id": "x",
        "properties": {
            "datetime": "2026-01-01T00:00:00Z",  # Tier 1 — not in search_text
            "vendor:label": "alpha beta",
            "vendor:score": 0.42,
            "vendor:flag": True,
        },
    }
    out = project_item_for_es(doc, known)
    assert "_search_text" in out
    txt = out["_search_text"]
    assert "alpha beta" in txt
    assert "0.42" in txt
    assert "true" in txt
    # Tier-1 values do NOT bleed into the catch-all.
    assert "2026-01-01T00:00:00Z" not in txt


def test_project_no_extras_produces_no_search_text() -> None:
    known = build_known_fields()
    doc = {
        "id": "x",
        "properties": {
            "datetime": "2026-01-01T00:00:00Z",
            "eo:cloud_cover": 5.0,
        },
    }
    out = project_item_for_es(doc, known)
    assert "_search_text" not in out
    assert "extras" not in out["properties"]


def test_project_search_text_handles_nested_and_arrays() -> None:
    known = build_known_fields()
    doc = {
        "id": "x",
        "properties": {
            "vendor:nested": {"deep": "hello"},
            "vendor:tags": ["one", "two"],
        },
    }
    out = project_item_for_es(doc, known)
    txt = out["_search_text"]
    assert "hello" in txt
    assert "one" in txt
    assert "two" in txt


def test_project_search_text_skips_none() -> None:
    known = build_known_fields()
    doc = {
        "id": "x",
        "properties": {
            "vendor:label": None,
            "vendor:other": "kept",
        },
    }
    out = project_item_for_es(doc, known)
    assert "kept" in out["_search_text"]
    # Empty extras-only doc still gets _search_text iff any value survives.


def test_unproject_drops_search_text_from_response() -> None:
    """``_search_text`` is an index-only artefact; it must not surface
    on the read contract (it's not in ``_RESERVED_MEMBER_KEYS`` so the
    existing unproject loop naturally drops it — this test pins that
    behaviour against future refactors)."""
    src = {
        "type": "Feature",
        "id": "x",
        "geometry": None,
        "properties": {"datetime": "2026-01-01T00:00:00Z"},
        "_search_text": "vendor data noise",
    }
    out = unproject_item_from_es(src)
    assert "_search_text" not in out


def test_long_tail_keeps_mapping_entry_count_flat() -> None:
    """500 distinct extras keys → still 2 mapping entries (extras +
    _search_text). The whole point of #1295."""
    known = build_known_fields()
    props = {f"col_{i:04d}": f"v{i}" for i in range(500)}
    doc = {"id": "x", "properties": props}
    out = project_item_for_es(doc, known)
    assert len(out["properties"]["extras"]) == 500
    assert isinstance(out["_search_text"], str)
    assert "v0" in out["_search_text"]
    assert "v499" in out["_search_text"]
