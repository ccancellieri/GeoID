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
"""Task 2 — Canonical stats/system containers in the item mapping (refs #1800).

Asserts that ``build_item_mapping`` emits the typed nested ``stats`` and
``system`` objects alongside the existing ``properties`` lane, and that the
pinned ES types are correct.
"""
from __future__ import annotations

import pytest

from dynastore.models.protocols.field_definition import FieldDefinition
from dynastore.modules.elasticsearch.mappings import build_item_mapping


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_known_fields(**extras) -> dict:
    """Return a known-fields map carrying the canonical stat/system entries."""
    base = {
        # user / STAC properties (container="properties", default)
        "datetime": FieldDefinition(name="datetime", data_type="timestamp"),
        "eo:cloud_cover": FieldDefinition(name="eo:cloud_cover", data_type="double"),
        # stats fields
        "area": FieldDefinition(name="area", data_type="double", container="stats"),
        "centroid": FieldDefinition(name="centroid", data_type="string", container="stats"),
        "s2_7": FieldDefinition(name="s2_7", data_type="string", container="stats"),
        "h3_5": FieldDefinition(name="h3_5", data_type="string", container="stats"),
        "geohash_6": FieldDefinition(name="geohash_6", data_type="string", container="stats"),
        # system fields
        "geometry_hash": FieldDefinition(name="geometry_hash", data_type="string", container="system"),
        "attributes_hash": FieldDefinition(name="attributes_hash", data_type="string", container="system"),
        "validity": FieldDefinition(name="validity", data_type="string", container="system"),
        "transaction_time": FieldDefinition(name="transaction_time", data_type="timestamp", container="system"),
        "deleted_at": FieldDefinition(name="deleted_at", data_type="timestamp", container="system"),
        # identity fields (flat at root)
        "external_id": FieldDefinition(name="external_id", data_type="string", container="identity"),
        "asset_id": FieldDefinition(name="asset_id", data_type="string", container="identity"),
        "geoid": FieldDefinition(name="geoid", data_type="string", container="identity"),
    }
    base.update(extras)
    return base


def _mapping(known_fields=None):
    if known_fields is None:
        known_fields = _make_known_fields()
    return build_item_mapping(known_fields)


# ---------------------------------------------------------------------------
# Root mapping contract (unchanged from existing behaviour)
# ---------------------------------------------------------------------------

def test_root_is_dynamic_false() -> None:
    """Root mapping must be ``dynamic: false`` to reject unknown top-level keys."""
    m = _mapping()
    assert m["dynamic"] is False


def test_geometry_is_geo_shape() -> None:
    """geometry field must be typed as geo_shape."""
    m = _mapping()
    assert m["properties"]["geometry"]["type"] == "geo_shape"


def test_common_properties_preserved() -> None:
    """Fields from COMMON_PROPERTIES (id, catalog_id, etc.) must survive."""
    from dynastore.modules.elasticsearch.mappings import COMMON_PROPERTIES
    m = _mapping()
    for key in COMMON_PROPERTIES:
        assert key in m["properties"], f"COMMON_PROPERTIES key {key!r} missing from root"


# ---------------------------------------------------------------------------
# stats container
# ---------------------------------------------------------------------------

def test_stats_object_is_present() -> None:
    """``stats`` nested object must be emitted when stats fields are present."""
    m = _mapping()
    assert "stats" in m["properties"]


def test_stats_object_is_dynamic_false() -> None:
    """``stats`` must be dynamic=false to prevent unknown stats key accumulation."""
    m = _mapping()
    assert m["properties"]["stats"]["dynamic"] is False


def test_stats_area_is_double() -> None:
    """area must be mapped as ``double``."""
    m = _mapping()
    assert m["properties"]["stats"]["properties"]["area"]["type"] == "double"


def test_stats_centroid_is_keyword() -> None:
    """centroid is stored as WKB hex — keyword type, NOT geo_point."""
    # A geo_point would parse the WKB hex as text and misinterpret it, whereas a
    # keyword field gives us a single sortable/filterable token that round-trips
    # to WKB for further decoding client-side.
    m = _mapping()
    centroid_mapping = m["properties"]["stats"]["properties"]["centroid"]
    assert centroid_mapping["type"] == "keyword"


def test_stats_spatial_cells_are_keyword() -> None:
    """Spatial-cell resolved names (s2_*, h3_*, geohash_*) must be keyword."""
    m = _mapping()
    stats_props = m["properties"]["stats"]["properties"]
    for name in ("s2_7", "h3_5", "geohash_6"):
        assert stats_props[name]["type"] == "keyword", (
            f"stats.{name} must be keyword, got {stats_props[name]}"
        )


def test_stats_fields_not_leaked_into_properties_lane() -> None:
    """Stats fields must NOT appear in the ``properties`` (user attrs) sub-object."""
    m = _mapping()
    props_sub = m["properties"].get("properties", {}).get("properties", {})
    for stat_name in ("area", "centroid", "s2_7", "h3_5", "geohash_6"):
        assert stat_name not in props_sub, (
            f"{stat_name!r} leaked into properties lane"
        )


# ---------------------------------------------------------------------------
# system container
# ---------------------------------------------------------------------------

def test_system_object_is_present() -> None:
    """``system`` nested object must be emitted when system fields are present."""
    m = _mapping()
    assert "system" in m["properties"]


def test_system_object_is_dynamic_false() -> None:
    """``system`` must be dynamic=false."""
    m = _mapping()
    assert m["properties"]["system"]["dynamic"] is False


@pytest.mark.parametrize(
    "field_name,expected_type",
    [
        ("geometry_hash", "keyword"),
        ("attributes_hash", "keyword"),
        # validity keeps its current es_type until #1828 Phase 2 lands the
        # date_range mapping together with the driver-side range-object write
        # (typing date_range while the raw tstzrange value is still written into
        # system.validity would fail ingest).
        ("validity", "keyword"),
        ("transaction_time", "date"),
        ("deleted_at", "date"),
    ],
)
def test_system_pinned_types(field_name: str, expected_type: str) -> None:
    """System field ES types must match the canonical pins (refs #1800/#1828)."""
    m = _mapping()
    got = m["properties"]["system"]["properties"][field_name]["type"]
    assert got == expected_type, (
        f"system.{field_name}: expected {expected_type!r}, got {got!r}"
    )


def test_system_fields_not_leaked_into_properties_lane() -> None:
    """System fields must NOT appear in the ``properties`` (user attrs) sub-object."""
    m = _mapping()
    props_sub = m["properties"].get("properties", {}).get("properties", {})
    for sys_name in ("geometry_hash", "attributes_hash", "validity", "transaction_time", "deleted_at"):
        assert sys_name not in props_sub, (
            f"{sys_name!r} leaked into properties lane"
        )


# ---------------------------------------------------------------------------
# identity fields — flat at root
# ---------------------------------------------------------------------------

def test_identity_fields_are_flat_at_root_not_in_system_or_stats() -> None:
    """Identity fields (external_id, asset_id, geoid) are part of COMMON_PROPERTIES
    and must be routable flat at the doc root, not buried in stats/system objects."""
    m = _mapping()
    # They must not appear under stats or system nested objects.
    stats_props = m["properties"].get("stats", {}).get("properties", {})
    system_props = m["properties"].get("system", {}).get("properties", {})
    for name in ("external_id", "asset_id", "geoid"):
        assert name not in stats_props, f"{name!r} leaked into stats"
        assert name not in system_props, f"{name!r} leaked into system"


# ---------------------------------------------------------------------------
# properties (user attrs) lane
# ---------------------------------------------------------------------------

def test_properties_lane_contains_user_attrs() -> None:
    """User/STAC attribute fields must appear in the ``properties`` sub-object."""
    m = _mapping()
    props_sub = m["properties"].get("properties", {}).get("properties", {})
    for name in ("datetime", "eo:cloud_cover"):
        assert name in props_sub, f"{name!r} missing from properties lane"


def test_properties_lane_has_extras_flattened() -> None:
    """``properties.extras`` must be typed as ``flattened``."""
    m = _mapping()
    extras = m["properties"]["properties"]["properties"].get("extras", {})
    assert extras.get("type") == "flattened"


def test_properties_lane_is_dynamic_false() -> None:
    """``properties`` sub-object must remain ``dynamic: false``."""
    m = _mapping()
    assert m["properties"]["properties"]["dynamic"] is False


# ---------------------------------------------------------------------------
# No container → falls back to properties lane (additive safety)
# ---------------------------------------------------------------------------

def test_unknown_container_field_routes_to_properties() -> None:
    """A FieldDefinition with default container ('properties') lands in the
    properties lane, not in stats or system."""
    known = {"my_custom_field": FieldDefinition(name="my_custom_field", data_type="string")}
    m = build_item_mapping(known)
    props_sub = m["properties"].get("properties", {}).get("properties", {})
    assert "my_custom_field" in props_sub
    # Must not be in stats or system objects.
    assert "my_custom_field" not in m["properties"].get("stats", {}).get("properties", {})
    assert "my_custom_field" not in m["properties"].get("system", {}).get("properties", {})


# ---------------------------------------------------------------------------
# ITEM_MAPPING backward compat — Tier-1 only, no stats/system yet
# ---------------------------------------------------------------------------

def test_item_mapping_default_has_no_stats_or_system_when_tier1_only() -> None:
    """The default ITEM_MAPPING (Tier 1 only via build_known_fields()) does not
    emit stats/system containers — those only appear when FieldDefinition values
    carry the container tag, which the Tier-1 plain-dict fields do not."""
    from dynastore.modules.elasticsearch.mappings import ITEM_MAPPING
    # Tier-1 is plain dicts, not FieldDefinition objects; no container tag.
    # build_item_mapping should handle both shapes gracefully.
    assert ITEM_MAPPING["dynamic"] is False
    # The standard properties lane must be intact.
    assert "properties" in ITEM_MAPPING["properties"]
    # stats / system are absent from the Tier-1 default mapping (no tagged fields).
    assert "stats" not in ITEM_MAPPING["properties"]
    assert "system" not in ITEM_MAPPING["properties"]


# ---------------------------------------------------------------------------
# metadata container — present in all three entity mappings (refs #1828)
# ---------------------------------------------------------------------------

def test_item_mapping_has_metadata_container() -> None:
    """``metadata`` typed container must always be present in item mapping."""
    m = _mapping()
    assert "metadata" in m["properties"]
    meta = m["properties"]["metadata"]
    assert meta.get("dynamic") is False
    assert "properties" in meta


def test_item_mapping_metadata_title_is_localized() -> None:
    """metadata.title must be a per-language localized object with dynamic:false."""
    m = _mapping()
    title = m["properties"]["metadata"]["properties"]["title"]
    assert title.get("dynamic") is False
    # Each supported locale must appear as a text subfield.
    from dynastore.modules.elasticsearch.items_projection import LANGUAGE_ANALYZERS
    for lang in LANGUAGE_ANALYZERS:
        assert lang in title["properties"], f"metadata.title missing lang {lang!r}"
        assert title["properties"][lang]["type"] == "text"


def test_item_mapping_metadata_description_is_localized() -> None:
    """metadata.description must be a per-language localized object."""
    m = _mapping()
    desc = m["properties"]["metadata"]["properties"]["description"]
    assert desc.get("dynamic") is False
    from dynastore.modules.elasticsearch.items_projection import LANGUAGE_ANALYZERS
    for lang in LANGUAGE_ANALYZERS:
        assert lang in desc["properties"], f"metadata.description missing lang {lang!r}"


def test_item_mapping_metadata_keywords_is_keyword_with_text_subfield() -> None:
    """metadata.keywords must be keyword-exact with a .text analyzed sub-field."""
    m = _mapping()
    kw = m["properties"]["metadata"]["properties"]["keywords"]
    assert kw["type"] == "keyword"
    assert kw["fields"]["text"]["type"] == "text"


def test_item_mapping_default_tier1_still_has_metadata_container() -> None:
    """Tier-1 ITEM_MAPPING must include the metadata container even when no
    FieldDefinition-tagged metadata fields are present."""
    from dynastore.modules.elasticsearch.mappings import ITEM_MAPPING
    assert "metadata" in ITEM_MAPPING["properties"]
    assert ITEM_MAPPING["properties"]["metadata"]["dynamic"] is False


def test_collection_mapping_has_metadata_container() -> None:
    """build_collection_mapping must emit the ``metadata`` container."""
    from dynastore.modules.elasticsearch.mappings import build_collection_mapping
    m = build_collection_mapping({})
    assert "metadata" in m["properties"]
    meta = m["properties"]["metadata"]
    assert meta.get("dynamic") is False
    assert "title" in meta["properties"]
    assert "description" in meta["properties"]
    assert "keywords" in meta["properties"]


def test_catalog_mapping_has_metadata_container() -> None:
    """build_catalog_mapping must emit the ``metadata`` container."""
    from dynastore.modules.elasticsearch.mappings import build_catalog_mapping
    m = build_catalog_mapping({})
    assert "metadata" in m["properties"]
    meta = m["properties"]["metadata"]
    assert meta.get("dynamic") is False
    assert "title" in meta["properties"]
    assert "description" in meta["properties"]
    assert "keywords" in meta["properties"]


def test_metadata_container_shared_structure_is_identical() -> None:
    """All three entity mappings must share the same metadata container structure
    (same language set, same field types) — no drift between levels."""
    from dynastore.modules.elasticsearch.mappings import (
        build_catalog_mapping,
        build_collection_mapping,
    )
    item_meta = _mapping()["properties"]["metadata"]
    coll_meta = build_collection_mapping({})["properties"]["metadata"]
    cat_meta = build_catalog_mapping({})["properties"]["metadata"]
    assert item_meta == coll_meta == cat_meta


# ---------------------------------------------------------------------------
# system.geometry_simplification typed nested object (refs #1828)
# ---------------------------------------------------------------------------

def test_system_has_geometry_simplification_when_system_fields_present() -> None:
    """system.geometry_simplification must be emitted when system is emitted."""
    m = _mapping()
    system_props = m["properties"]["system"]["properties"]
    assert "geometry_simplification" in system_props


def test_system_geometry_simplification_is_dynamic_false() -> None:
    """system.geometry_simplification must be dynamic:false."""
    m = _mapping()
    gs = m["properties"]["system"]["properties"]["geometry_simplification"]
    assert gs.get("dynamic") is False


def test_system_geometry_simplification_has_factor_and_mode() -> None:
    """system.geometry_simplification must carry factor (float) and mode (keyword)."""
    m = _mapping()
    gs_props = m["properties"]["system"]["properties"]["geometry_simplification"]["properties"]
    assert gs_props["factor"]["type"] == "float"
    assert gs_props["mode"]["type"] == "keyword"


def test_flat_simplification_fields_still_in_common_properties() -> None:
    """The backward-compat flat ``_simplification_factor`` / ``_simplification_mode``
    root trackers must remain in COMMON_PROPERTIES until Phase 2 drivers migrate."""
    from dynastore.modules.elasticsearch.mappings import COMMON_PROPERTIES
    assert "_simplification_factor" in COMMON_PROPERTIES
    assert COMMON_PROPERTIES["_simplification_factor"]["type"] == "float"
    assert "_simplification_mode" in COMMON_PROPERTIES
    assert COMMON_PROPERTIES["_simplification_mode"]["type"] == "keyword"


# ---------------------------------------------------------------------------
# validity — date_range deferred to #1828 Phase 2 (mapping + driver write move
# together); for now it keeps its current es_type so ingest of the raw tstzrange
# value into system.validity does not fail.
# ---------------------------------------------------------------------------

def test_system_validity_not_yet_date_range() -> None:
    """Until Phase 2, system.validity must NOT be date_range (the raw tstzrange
    value is still written there; date_range would reject it on ingest)."""
    m = _mapping()
    validity = m["properties"]["system"]["properties"]["validity"]
    assert validity["type"] != "date_range"


def test_flat_valid_from_to_still_in_common_properties() -> None:
    """Backward-compat ``_valid_from`` / ``_valid_to`` flat trackers must stay
    in COMMON_PROPERTIES until drivers migrate (#1828 Phase 2)."""
    from dynastore.modules.elasticsearch.mappings import COMMON_PROPERTIES
    assert "_valid_from" in COMMON_PROPERTIES
    assert COMMON_PROPERTIES["_valid_from"]["type"] == "date"
    assert "_valid_to" in COMMON_PROPERTIES
    assert COMMON_PROPERTIES["_valid_to"]["type"] == "date"
