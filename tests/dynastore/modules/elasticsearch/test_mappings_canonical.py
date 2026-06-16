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


def test_stats_centroid_is_geo_point() -> None:
    """centroid is a ``[x, y]`` WGS84 coordinate pair → geo_point (refs #1285).

    The PG geometries sidecar emits the centroid via ``ARRAY[ST_X, ST_Y]``, a
    coordinate pair ES ingests natively as a geo_point. ``ignore_malformed`` is
    pinned so a stray projected-SRID coordinate is kept in ``_source`` rather
    than rejecting the whole doc.
    """
    m = _mapping()
    centroid_mapping = m["properties"]["stats"]["properties"]["centroid"]
    assert centroid_mapping["type"] == "geo_point"
    assert centroid_mapping.get("ignore_malformed") is True


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
        # validity is the temporal window, typed as date_range (#1828); the
        # driver write converts the PG tstzrange into the matching range body.
        ("validity", "date_range"),
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

def test_identity_fields_are_searchable_keyword_at_root() -> None:
    """Identity fields (external_id, asset_id, geoid) are flat-at-root keyword
    fields — the canonical SEARCHABLE identity lane (refs #1285).

    Per ``classify_container`` the identity axes live flat at the document root,
    and the read-side resolvers route their filters/sorts there. Pre-#1285 the
    public items mapping declared neither ``external_id`` nor ``asset_id`` at
    root, so they rode in ``_source`` un-indexed and every term filter silently
    missed. They must be indexed keyword at root and must NOT leak into the
    ``system`` or ``stats`` nested objects."""
    m = _mapping()
    root_props = m["properties"]
    stats_props = root_props.get("stats", {}).get("properties", {})
    system_props = root_props.get("system", {}).get("properties", {})
    for name in ("external_id", "asset_id", "geoid"):
        assert root_props.get(name) == {"type": "keyword"}, (
            f"{name!r} must be a searchable keyword at the document root"
        )
        assert name not in system_props, f"{name!r} leaked into system"
        assert name not in stats_props, f"{name!r} leaked into stats"


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
# ITEM_MAPPING — canonical system/stats vocab is ALWAYS emitted (refs #1285)
# ---------------------------------------------------------------------------

def test_item_mapping_default_always_has_canonical_system_and_stats() -> None:
    """The default ITEM_MAPPING (Tier-1 only, no FieldDefinition container tags)
    STILL emits the typed ``system`` and ``stats`` containers, seeded from the
    bounded canonical vocab (refs #1285).

    Pre-#1285 these containers appeared only when a known-field carried a
    container tag, so a plain Tier-1 catalog left ``system.*`` / ``stats.*``
    stored-but-not-indexed. The bounded vocab is now baked in unconditionally so
    every system/stats field is queryable and uniformly typed across the
    cross-catalog alias regardless of per-collection config."""
    from dynastore.modules.elasticsearch.mappings import ITEM_MAPPING
    assert ITEM_MAPPING["dynamic"] is False
    assert "properties" in ITEM_MAPPING["properties"]
    # Both canonical containers present even with Tier-1 plain dicts only.
    sys_props = ITEM_MAPPING["properties"]["system"]["properties"]
    stats_props = ITEM_MAPPING["properties"]["stats"]["properties"]
    # Independent expected oracles (NOT the module SSOT) so the assertion also
    # guards against accidental mutation of CANONICAL_* leaking into the build.
    expected_system = {
        "geometry_hash": {"type": "keyword"},
        "attributes_hash": {"type": "keyword"},
        "validity": {"type": "date_range"},
        "transaction_time": {"type": "date"},
        "deleted_at": {"type": "date"},
    }
    # Identity axes are flat-at-root keyword, NOT system members.
    for ident in ("external_id", "asset_id", "geoid"):
        assert ITEM_MAPPING["properties"].get(ident) == {"type": "keyword"}
        assert ident not in sys_props
    expected_stats = {
        "area": {"type": "double"},
        "length": {"type": "double"},
        "perimeter": {"type": "double"},
        "vertex_count": {"type": "long"},
        "hole_count": {"type": "long"},
        "centroid": {"type": "geo_point", "ignore_malformed": True},
        "bbox": {"type": "float"},
    }
    for name, es_type in expected_system.items():
        assert sys_props.get(name) == es_type, f"system.{name} missing/mistyped"
    for name, es_type in expected_stats.items():
        assert stats_props.get(name) == es_type, f"stats.{name} missing/mistyped"


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


def test_item_mapping_metadata_keywords_is_localized_object() -> None:
    """metadata.keywords must be a localized per-language object, NOT a flat keyword.

    The i18n layer serialises keywords as {"en": ["a", "b"], ...} before
    indexing.  A flat ``keyword`` mapping rejects an object value with
    ``mapper_parsing_exception`` → 500 on every multilingual ingest.
    The correct shape is ``type:object, dynamic:false`` with one
    ``keyword`` sub-property per supported locale (refs #1828).
    """
    from dynastore.modules.elasticsearch.items_projection import LANGUAGE_ANALYZERS
    m = _mapping()
    kw = m["properties"]["metadata"]["properties"]["keywords"]
    assert kw["type"] == "object", (
        f"metadata.keywords must be type:object for i18n payloads, got {kw['type']!r}"
    )
    assert kw.get("dynamic") is False
    # Every supported locale must have a keyword sub-property with a .text sub-field.
    for lang, analyzer in LANGUAGE_ANALYZERS.items():
        assert lang in kw["properties"], f"metadata.keywords missing locale {lang!r}"
        locale_field = kw["properties"][lang]
        assert locale_field["type"] == "keyword", (
            f"metadata.keywords.{lang} must be keyword, got {locale_field['type']!r}"
        )
        assert locale_field["fields"]["text"]["type"] == "text"
        assert locale_field["fields"]["text"]["analyzer"] == analyzer


def test_metadata_keywords_accepts_i18n_payload_shape() -> None:
    """Verify the metadata.keywords mapping structure accepts the i18n payload.

    When the i18n layer sends ``{"en": ["climate", "water"]}`` the value is
    an object.  Under the old flat ``keyword`` mapping ES would reject it.
    Under the new ``type:object`` mapping each locale sub-property receives
    an array of keyword values — valid ES multi-value semantics.

    This is a unit-level structural check (no live ES required).
    """
    from dynastore.modules.elasticsearch.items_projection import LANGUAGE_ANALYZERS
    m = _mapping()
    kw = m["properties"]["metadata"]["properties"]["keywords"]
    # Structural assertion: the mapping shape accepts a dict keyed by locale.
    assert kw["type"] == "object"
    # The i18n payload {"en": [...]} maps onto kw.properties.en — which must exist.
    assert "en" in kw["properties"]
    # Confirm every platform locale is present — the LANGUAGE_ANALYZERS set is
    # the authoritative list (en/fr/es/ru/ar/it/de/zh).
    assert set(kw["properties"].keys()) == set(LANGUAGE_ANALYZERS.keys())


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
# validity — typed as ES date_range (#1828). The mapping and the driver-side
# range-object write land together: the canonical doc builder converts the PG
# tstzrange Range object into the matching {gte|gt, lte|lt} body so the
# date_range field accepts it on ingest.
# ---------------------------------------------------------------------------

def test_system_validity_is_date_range() -> None:
    """system.validity must be typed as date_range (#1828)."""
    m = _mapping()
    validity = m["properties"]["system"]["properties"]["validity"]
    assert validity["type"] == "date_range"


def test_flat_valid_from_to_still_in_common_properties() -> None:
    """Backward-compat ``_valid_from`` / ``_valid_to`` flat trackers must stay
    in COMMON_PROPERTIES until drivers migrate (#1828 Phase 2)."""
    from dynastore.modules.elasticsearch.mappings import COMMON_PROPERTIES
    assert "_valid_from" in COMMON_PROPERTIES
    assert COMMON_PROPERTIES["_valid_from"]["type"] == "date"
    assert "_valid_to" in COMMON_PROPERTIES
    assert COMMON_PROPERTIES["_valid_to"]["type"] == "date"


# ---------------------------------------------------------------------------
# license — object mapping for LicenseInfo (refs #1828).
#
# STAC ``license`` surfaces as a LicenseInfo object
# {license_id, is_osi_compliant, localized_content?} after the i18n layer
# normalises a bare SPDX string.  A flat ``keyword`` mapping at
# ``properties.license`` rejects the object with ``mapper_parsing_exception``.
# The fix pins ``type:object, dynamic:false`` with the two indexed sub-fields;
# ``localized_content`` (if present) rides in ``_source`` unindexed, covered
# by ``dynamic:false`` on the parent (refs #1828).
# ---------------------------------------------------------------------------

def test_properties_license_is_object_not_keyword() -> None:
    """properties.license must be type:object to accept a LicenseInfo payload."""
    from dynastore.modules.elasticsearch.items_projection import _STAC_CORE_FIELDS
    lic = _STAC_CORE_FIELDS["license"]
    assert lic.get("type") == "object", (
        f"properties.license must be type:object, got {lic.get('type')!r}; "
        "a flat keyword rejects the LicenseInfo object with mapper_parsing_exception"
    )


def test_properties_license_is_dynamic_false() -> None:
    """properties.license must be dynamic:false so localized_content is unindexed."""
    from dynastore.modules.elasticsearch.items_projection import _STAC_CORE_FIELDS
    lic = _STAC_CORE_FIELDS["license"]
    assert lic.get("dynamic") is False


def test_properties_license_has_license_id_and_osi_compliant() -> None:
    """properties.license must index license_id (keyword) and is_osi_compliant (boolean)."""
    from dynastore.modules.elasticsearch.items_projection import _STAC_CORE_FIELDS
    props = _STAC_CORE_FIELDS["license"]["properties"]
    assert props["license_id"]["type"] == "keyword"
    assert props["is_osi_compliant"]["type"] == "boolean"


def test_license_object_survives_in_item_mapping() -> None:
    """The license object mapping must survive into the built item mapping."""
    from dynastore.modules.elasticsearch.mappings import ITEM_MAPPING
    lic = ITEM_MAPPING["properties"]["properties"]["properties"]["license"]
    assert lic.get("type") == "object"
    assert lic.get("dynamic") is False
    assert lic["properties"]["license_id"]["type"] == "keyword"
    assert lic["properties"]["is_osi_compliant"]["type"] == "boolean"


# ---------------------------------------------------------------------------
# Canonical queryable advertisement (OGC API Part 3 / STAC Filter) — #2230
# ---------------------------------------------------------------------------

def test_canonical_queryable_properties_covers_system_and_stats() -> None:
    """Every canonical system/stats field is advertised as a queryable with a
    JSON-Schema type derived from its ES mapping type (refs #2230)."""
    from dynastore.modules.elasticsearch.mappings import (
        CANONICAL_STATS_TYPES,
        CANONICAL_SYSTEM_TYPES,
        canonical_queryable_properties,
    )
    q = canonical_queryable_properties()
    for name in CANONICAL_SYSTEM_TYPES:
        assert name in q, f"system field {name} missing from queryables"
    for name in CANONICAL_STATS_TYPES:
        assert name in q, f"stats field {name} missing from queryables"


def test_canonical_queryable_types_mapped_correctly() -> None:
    """ES types map to the expected JSON-Schema queryable types."""
    from dynastore.modules.elasticsearch.mappings import canonical_queryable_properties
    q = canonical_queryable_properties()
    # keyword -> string
    assert q["geometry_hash"]["type"] == "string"
    # double -> number
    assert q["area"]["type"] == "number"
    # long -> integer
    assert q["vertex_count"]["type"] == "integer"
    # date -> string/date-time
    assert q["transaction_time"]["type"] == "string"
    assert q["transaction_time"]["format"] == "date-time"
    # date_range (validity) -> string/date-time with range note
    assert q["validity"]["type"] == "string"
    assert q["validity"]["format"] == "date-time"
    # geo_point -> object
    assert q["centroid"]["type"] == "object"
    # every fragment carries a default title
    assert q["area"]["title"] == "area"
