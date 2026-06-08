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

"""TDD tests for the canonical COLLECTION envelope (refs #1285/#1800).

Pins:
  1. Storage envelope: internal identity + system lifecycle + reserved
     structural members at top; attributes under properties (unknown→extras).
  2. STAC read projector hoists attributes to the TOP level (no `properties`
     member) and drops internal identity / system / access from the wire.
  3. Round-trip: build → unproject reconstructs the original STAC Collection
     (extent treated opaquely, as the driver does).
"""
from __future__ import annotations

from dynastore.modules.elasticsearch.collection_canonical import (
    build_canonical_collection_doc,
    unproject_collection_from_es,
)
from dynastore.modules.elasticsearch.items_projection import TIER_1_FIELDS

# A representative STAC Collection (extent kept opaque — the driver
# enriches/unenriches it around these pure functions).
_COLLECTION = {
    "type": "Collection",
    "stac_version": "1.1.0",
    "id": "coll1",
    "title": "Sentinel-2 L2A",
    "description": "Surface reflectance",
    "keywords": ["sentinel", "s2"],
    "license": {"code": "proprietary"},
    "language": {"code": "en"},
    "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}},
    "providers": [{"name": "ESA"}],
    "links": [{"rel": "self", "href": "https://x/coll1"}],
    "stac_extensions": ["https://stac-extensions.github.io/language/v1.0.0/schema.json"],
    "cube:dimensions": {"t": {"type": "temporal"}},
}


def _build(metadata):
    return build_canonical_collection_doc(
        metadata,
        catalog_id="cat",
        collection_id="coll1",
        known_fields=dict(TIER_1_FIELDS),
    )


def test_storage_envelope_sections():
    doc = _build(_COLLECTION)
    # internal identity at top
    assert doc["id"] == "coll1"
    assert doc["catalog_id"] == "cat"
    assert doc["collection_id"] == "coll1"
    # reserved structural members carried at the top level
    assert doc["extent"] == _COLLECTION["extent"]
    assert doc["providers"] == _COLLECTION["providers"]
    assert doc["links"] == _COLLECTION["links"]
    assert doc["type"] == "Collection"
    # attributes live under properties; title/description are Tier-1 known →
    # stay flat; cube:dimensions is unknown → extras lane
    assert doc["properties"]["title"] == "Sentinel-2 L2A"
    assert doc["properties"]["description"] == "Surface reflectance"
    assert doc["properties"]["extras"]["cube:dimensions"] == {"t": {"type": "temporal"}}
    # structural members never leak into properties
    for structural in ("extent", "providers", "links", "type", "stac_version"):
        assert structural not in doc["properties"]


def test_read_projector_hoists_attributes_to_top_level():
    doc = _build(_COLLECTION)
    wire = unproject_collection_from_es(doc)
    # no `properties` member on a STAC Collection
    assert "properties" not in wire
    # attributes back at top level
    assert wire["title"] == "Sentinel-2 L2A"
    assert wire["description"] == "Surface reflectance"
    assert wire["keywords"] == ["sentinel", "s2"]
    assert wire["cube:dimensions"] == {"t": {"type": "temporal"}}
    # internal identity dropped from the wire
    assert "catalog_id" not in wire
    assert "collection_id" not in wire
    assert "system" not in wire and "access" not in wire


def test_round_trip_reconstructs_original_collection():
    doc = _build(_COLLECTION)
    wire = unproject_collection_from_es(doc)
    assert wire == _COLLECTION


def test_round_trip_with_lifecycle_and_unknown_extension():
    md = dict(_COLLECTION)
    md["created"] = "2026-06-01T00:00:00Z"
    md["updated"] = "2026-06-04T00:00:00Z"
    md["custom:flavor"] = "vanilla"
    doc = _build(md)
    # lifecycle parked in system, not properties
    assert doc["system"] == {
        "created": "2026-06-01T00:00:00Z",
        "updated": "2026-06-04T00:00:00Z",
    }
    assert "created" not in doc["properties"]
    wire = unproject_collection_from_es(doc)
    assert wire == md


def test_access_sidecar_stored_not_on_wire():
    doc = build_canonical_collection_doc(
        _COLLECTION,
        catalog_id="cat",
        collection_id="coll1",
        known_fields=dict(TIER_1_FIELDS),
        access={"owner": "u1"},
    )
    assert doc["access"] == {"owner": "u1"}
    wire = unproject_collection_from_es(doc)
    assert "access" not in wire


# ---------------------------------------------------------------------------
# i18n / multilingual keywords regression (refs #1932/#1828)
# ---------------------------------------------------------------------------

_MULTILINGUAL_COLLECTION = {
    "type": "Collection",
    "stac_version": "1.1.0",
    "id": "coll-i18n",
    "title": {"en": "Sentinel-2 L2A", "fr": "Sentinel-2 N2A"},
    "description": {"en": "Surface reflectance", "fr": "Réflectance de surface"},
    "keywords": {"en": ["satellite", "sentinel"], "fr": ["satellite", "sentinelle"]},
    "license": "proprietary",
    "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}},
    "links": [],
}


def _build_i18n(metadata):
    return build_canonical_collection_doc(
        metadata,
        catalog_id="cat",
        collection_id="coll-i18n",
        known_fields=dict(TIER_1_FIELDS),
    )


def test_multilingual_keywords_land_in_metadata_not_properties():
    """Language-keyed keyword dicts must go into metadata, not the flat
    properties bag that maps to keyword type in ES (refs #1932)."""
    doc = _build_i18n(_MULTILINGUAL_COLLECTION)
    assert "metadata" in doc
    assert doc["metadata"]["keywords"] == {
        "en": ["satellite", "sentinel"],
        "fr": ["satellite", "sentinelle"],
    }
    assert doc["metadata"]["title"] == {"en": "Sentinel-2 L2A", "fr": "Sentinel-2 N2A"}
    assert doc["metadata"]["description"] == {
        "en": "Surface reflectance",
        "fr": "Réflectance de surface",
    }
    # NOT in the flat properties bag (would cause mapper_parsing_exception in ES)
    assert "keywords" not in doc.get("properties", {})
    assert "title" not in doc.get("properties", {})
    assert "description" not in doc.get("properties", {})


def test_multilingual_keywords_round_trip():
    """build → unproject restores language-keyed dicts unchanged to the
    top-level STAC wire format (refs #1932)."""
    doc = _build_i18n(_MULTILINGUAL_COLLECTION)
    wire = unproject_collection_from_es(doc)
    assert wire["keywords"] == {"en": ["satellite", "sentinel"], "fr": ["satellite", "sentinelle"]}
    assert wire["title"] == {"en": "Sentinel-2 L2A", "fr": "Sentinel-2 N2A"}
    assert wire["description"] == {"en": "Surface reflectance", "fr": "Réflectance de surface"}
    # Internal containers excluded from the wire
    assert "metadata" not in wire
    assert "properties" not in wire


def test_plain_keywords_stay_in_properties():
    """Plain list keywords (non-i18n) must still land in properties,
    not the metadata container (backwards compatibility)."""
    doc = _build(_COLLECTION)
    # _COLLECTION has keywords=["sentinel", "s2"] — a list, not a dict
    assert doc["properties"]["keywords"] == ["sentinel", "s2"]
    assert "metadata" not in doc or "keywords" not in doc.get("metadata", {})


def test_metadata_container_excluded_from_wire():
    """The internal metadata container must never appear on the STAC wire."""
    doc = _build_i18n(_MULTILINGUAL_COLLECTION)
    wire = unproject_collection_from_es(doc)
    assert "metadata" not in wire
