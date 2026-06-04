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

"""TDD tests for the canonical CATALOG envelope (refs #1285/#1800)."""
from __future__ import annotations

from dynastore.modules.elasticsearch.catalog_canonical import (
    build_canonical_catalog_doc,
    unproject_catalog_from_es,
)
from dynastore.modules.elasticsearch.items_projection import TIER_1_FIELDS

_CATALOG = {
    "type": "Catalog",
    "stac_version": "1.1.0",
    "id": "cat1",
    "title": "My Catalog",
    "description": "A test catalog",
    "keywords": ["demo"],
    "links": [{"rel": "self", "href": "https://x/cat1"}],
    "stac_extensions": [],
    "custom:flavor": "vanilla",
}


def _build(metadata):
    return build_canonical_catalog_doc(
        metadata, catalog_id="cat1", known_fields=dict(TIER_1_FIELDS)
    )


def test_storage_envelope_sections():
    doc = _build(_CATALOG)
    assert doc["id"] == "cat1"
    assert doc["catalog_id"] == "cat1"
    assert doc["links"] == _CATALOG["links"]
    assert doc["type"] == "Catalog"
    # known attrs flat under properties; unknown ext → extras
    assert doc["properties"]["title"] == "My Catalog"
    assert doc["properties"]["extras"]["custom:flavor"] == "vanilla"
    # structural never leaks into properties
    for structural in ("links", "type", "stac_version", "id"):
        assert structural not in doc["properties"]


def test_read_projector_hoists_attributes_to_top_level():
    wire = unproject_catalog_from_es(_build(_CATALOG))
    assert "properties" not in wire
    assert wire["type"] == "Catalog"
    assert wire["title"] == "My Catalog"
    assert wire["custom:flavor"] == "vanilla"
    assert "catalog_id" not in wire
    assert "system" not in wire and "access" not in wire


def test_round_trip_reconstructs_original_catalog():
    wire = unproject_catalog_from_es(_build(_CATALOG))
    assert wire == _CATALOG


def test_round_trip_with_lifecycle():
    md = dict(_CATALOG)
    md["created"] = "2026-06-01T00:00:00Z"
    md["updated"] = "2026-06-04T00:00:00Z"
    doc = _build(md)
    assert doc["system"] == {
        "created": "2026-06-01T00:00:00Z",
        "updated": "2026-06-04T00:00:00Z",
    }
    assert "created" not in doc["properties"]
    assert unproject_catalog_from_es(doc) == md


def test_access_sidecar_stored_not_on_wire():
    doc = build_canonical_catalog_doc(
        _CATALOG, catalog_id="cat1", known_fields=dict(TIER_1_FIELDS),
        access={"owner": "u1"},
    )
    assert doc["access"] == {"owner": "u1"}
    assert "access" not in unproject_catalog_from_es(doc)
