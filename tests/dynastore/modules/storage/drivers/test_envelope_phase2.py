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

"""Phase 2 envelope driver tests (refs #1828).

Covers:
- ``_stamp_simplification``: writes into ``system.geometry_simplification``;
  skips when mode="none"; preserves existing system keys.
- ``build_envelope_feature_doc``: system/metadata containers forwarded.
- ``_envelope_source_to_feature``: reads simplification from canonical path
  and falls back to old flat keys; resolves multilingual metadata fields.
- ``ENVELOPE_FEATURE_MAPPING``: declares ``system`` + ``geometry_simplification``
  sub-object; root access fields still present.

No live Elasticsearch; no shapely dependency.
"""

from __future__ import annotations

from dynastore.modules.storage.drivers.elasticsearch_envelope.doc_builder import (
    build_envelope_feature_doc,
)
from dynastore.modules.storage.drivers.elasticsearch_envelope.driver import (
    ItemsElasticsearchEnvelopeDriver,
    _stamp_simplification,
)
from dynastore.modules.storage.drivers.elasticsearch_envelope.mappings import (
    ENVELOPE_FEATURE_MAPPING,
)


# ---------------------------------------------------------------------------
# _stamp_simplification helper
# ---------------------------------------------------------------------------


def test_stamp_simplification_none_mode_does_not_write():
    """When mode='none' (no simplification), system must NOT be added."""
    doc: dict = {}
    _stamp_simplification(doc, factor=1.0, mode="none")
    assert "system" not in doc


def test_stamp_simplification_tolerance_mode_writes_canonical_path():
    """Simplification result writes into system.geometry_simplification."""
    doc: dict = {}
    _stamp_simplification(doc, factor=0.75, mode="tolerance")
    assert doc["system"]["geometry_simplification"] == {"factor": 0.75, "mode": "tolerance"}


def test_stamp_simplification_bbox_mode_writes_canonical_path():
    doc: dict = {}
    _stamp_simplification(doc, factor=0.0, mode="bbox")
    gs = doc["system"]["geometry_simplification"]
    assert gs["factor"] == 0.0
    assert gs["mode"] == "bbox"


def test_stamp_simplification_does_not_write_flat_keys():
    """The old flat root keys must NOT be written by the new path."""
    doc: dict = {}
    _stamp_simplification(doc, factor=0.5, mode="tolerance")
    assert "simplification_factor" not in doc
    assert "simplification_mode" not in doc


def test_stamp_simplification_preserves_existing_system_keys():
    """Existing system entries must survive the simplification stamp."""
    doc: dict = {"system": {"geometry_hash": "abc123"}}
    _stamp_simplification(doc, factor=0.5, mode="tolerance")
    assert doc["system"]["geometry_hash"] == "abc123"
    assert doc["system"]["geometry_simplification"]["factor"] == 0.5


def test_stamp_simplification_none_mode_leaves_existing_system_intact():
    """mode='none' with a pre-existing system must not disturb it."""
    doc: dict = {"system": {"geometry_hash": "xyz"}}
    _stamp_simplification(doc, factor=1.0, mode="none")
    assert doc["system"] == {"geometry_hash": "xyz"}


# ---------------------------------------------------------------------------
# build_envelope_feature_doc — system and metadata forwarding
# ---------------------------------------------------------------------------


def test_build_doc_accepts_system_kwarg():
    item = {"id": "g1", "geometry": {"type": "Point", "coordinates": [0, 0]}, "properties": {}}
    doc = build_envelope_feature_doc(
        item, catalog_id="cat", collection_id="col",
        system={"geometry_simplification": {"factor": 0.5, "mode": "tolerance"}},
    )
    assert doc["system"]["geometry_simplification"]["factor"] == 0.5


def test_build_doc_none_system_omits_system_key():
    item = {"id": "g2", "properties": {}}
    doc = build_envelope_feature_doc(item, catalog_id="cat", collection_id="col", system=None)
    assert "system" not in doc


def test_build_doc_empty_system_omits_system_key():
    item = {"id": "g3", "properties": {}}
    doc = build_envelope_feature_doc(item, catalog_id="cat", collection_id="col", system={})
    assert "system" not in doc


def test_build_doc_accepts_metadata_kwarg():
    item = {"id": "g4", "properties": {}}
    meta = {"title": {"en": "Hello", "fr": "Bonjour"}}
    doc = build_envelope_feature_doc(item, catalog_id="cat", collection_id="col", metadata=meta)
    assert doc["metadata"] == meta


def test_build_doc_none_metadata_omits_key():
    item = {"id": "g5", "properties": {}}
    doc = build_envelope_feature_doc(item, catalog_id="cat", collection_id="col", metadata=None)
    assert "metadata" not in doc


def test_build_doc_access_fields_still_at_root():
    """ABAC access fields must remain at root even when system/metadata are present."""
    item = {"id": "g6", "properties": {}, "_visibility": "restricted", "_owner": "alice"}
    doc = build_envelope_feature_doc(
        item, catalog_id="cat", collection_id="col",
        system={"geometry_simplification": {"factor": 0.8, "mode": "tolerance"}},
        metadata={"title": {"en": "T"}},
    )
    # Root ABAC fields intact.
    assert doc["visibility"] == "restricted"
    assert doc["owner"] == "alice"
    # Canonical containers present.
    assert "system" in doc
    assert "metadata" in doc


# ---------------------------------------------------------------------------
# _envelope_source_to_feature — read path
# ---------------------------------------------------------------------------


def _feature_from(source: dict, **kw) -> dict:
    """Helper — call the static read method and return properties."""
    feature = ItemsElasticsearchEnvelopeDriver._envelope_source_to_feature(
        source, "cat", "col", source.get("geoid", "fallback"), **kw
    )
    return feature.properties or {}


def test_read_canonical_path_simplification():
    """system.geometry_simplification round-trips to properties."""
    source = {
        "geoid": "g-read-1",
        "system": {"geometry_simplification": {"factor": 0.75, "mode": "tolerance"}},
        "properties": {"name": "x"},
    }
    props = _feature_from(source)
    assert props["simplification_factor"] == 0.75
    assert props["simplification_mode"] == "tolerance"


def test_read_flat_fallback_simplification():
    """Old docs with flat root simplification keys are still readable."""
    source = {
        "geoid": "g-read-2",
        "simplification_factor": 0.5,
        "simplification_mode": "bbox",
        "properties": {},
    }
    props = _feature_from(source)
    assert props["simplification_factor"] == 0.5
    assert props["simplification_mode"] == "bbox"


def test_read_canonical_path_takes_precedence_over_flat_keys():
    """When both canonical system path and old flat keys are present,
    the canonical path wins."""
    source = {
        "geoid": "g-read-3",
        "system": {"geometry_simplification": {"factor": 0.9, "mode": "tolerance"}},
        "simplification_factor": 0.1,   # old key — must be ignored
        "simplification_mode": "bbox",  # old key — must be ignored
        "properties": {},
    }
    props = _feature_from(source)
    assert props["simplification_factor"] == 0.9
    assert props["simplification_mode"] == "tolerance"


def test_read_no_simplification_omits_keys():
    """If neither system.geometry_simplification nor flat keys are present,
    simplification keys must not appear in properties."""
    source = {"geoid": "g-read-4", "properties": {"name": "y"}}
    props = _feature_from(source)
    assert "simplification_factor" not in props
    assert "simplification_mode" not in props


def test_read_metadata_title_resolved_default_en():
    """Multilingual title in metadata is resolved to 'en' by default."""
    source = {
        "geoid": "g-meta-1",
        "metadata": {"title": {"en": "English Title", "fr": "Titre Français"}},
        "properties": {},
    }
    props = _feature_from(source)
    assert props["title"] == "English Title"


def test_read_metadata_title_resolved_to_requested_lang():
    source = {
        "geoid": "g-meta-2",
        "metadata": {"title": {"en": "English", "fr": "Français"}},
        "properties": {},
    }
    props = _feature_from(source, lang="fr")
    assert props["title"] == "Français"


def test_read_metadata_falls_back_to_en_when_lang_missing():
    source = {
        "geoid": "g-meta-3",
        "metadata": {"title": {"en": "Fallback", "fr": "French"}},
        "properties": {},
    }
    props = _feature_from(source, lang="es")
    assert props["title"] == "Fallback"


def test_read_metadata_keywords_resolved():
    source = {
        "geoid": "g-meta-4",
        "metadata": {"keywords": {"en": ["a", "b"], "fr": ["c"]}},
        "properties": {},
    }
    props = _feature_from(source, lang="en")
    assert props["keywords"] == ["a", "b"]


def test_read_metadata_does_not_clobber_existing_property():
    """Metadata resolution uses setdefault — pre-existing property keys win."""
    source = {
        "geoid": "g-meta-5",
        "metadata": {"title": {"en": "Canonical"}},
        "properties": {"title": "Existing"},
    }
    props = _feature_from(source)
    assert props["title"] == "Existing"


def test_read_no_metadata_no_side_effects():
    """Source with no metadata must not add title/description/keywords."""
    source = {"geoid": "g-meta-6", "properties": {"name": "z"}}
    props = _feature_from(source)
    for key in ("title", "description", "keywords"):
        assert key not in props


def test_read_access_fields_not_surfaced():
    """visibility/owner/attrs must NOT appear in the read Feature properties."""
    source = {
        "geoid": "g-read-5",
        "visibility": "restricted",
        "owner": "alice",
        "attrs": {"project": "fao"},
        "properties": {"name": "n"},
    }
    props = _feature_from(source)
    assert "visibility" not in props
    assert "owner" not in props
    assert "attrs" not in props


# ---------------------------------------------------------------------------
# ENVELOPE_FEATURE_MAPPING — structural assertions
# ---------------------------------------------------------------------------


def test_mapping_declares_system_container():
    sys_mapping = ENVELOPE_FEATURE_MAPPING["properties"]["system"]
    assert sys_mapping["type"] == "object"
    assert sys_mapping["dynamic"] is False
    geo_simp = sys_mapping["properties"]["geometry_simplification"]
    assert geo_simp["type"] == "object"
    assert geo_simp["dynamic"] is False
    assert geo_simp["properties"]["factor"] == {"type": "float"}
    assert geo_simp["properties"]["mode"] == {"type": "keyword"}


def test_mapping_root_access_fields_still_present():
    """Root access fields must remain for row-level filter reliability."""
    props = ENVELOPE_FEATURE_MAPPING["properties"]
    assert props["visibility"] == {"type": "keyword"}
    assert props["owner"] == {"type": "keyword"}
    assert "attrs" in props
    assert ENVELOPE_FEATURE_MAPPING["dynamic"] is False


def test_mapping_no_flat_simplification_fields():
    """Old flat simplification keys must not be in the mapping
    (the canonical path is system.geometry_simplification now)."""
    props = ENVELOPE_FEATURE_MAPPING["properties"]
    assert "simplification_factor" not in props
    assert "simplification_mode" not in props


def test_mapping_metadata_container_present():
    """metadata container must be declared in the mapping."""
    assert "metadata" in ENVELOPE_FEATURE_MAPPING["properties"]


# ---------------------------------------------------------------------------
# Write → read round-trip (doc_builder + _stamp_simplification + read path)
# ---------------------------------------------------------------------------


def test_write_read_simplification_round_trip():
    """A write that produces simplification round-trips correctly via read."""
    item = {
        "id": "g-rt-1",
        "geometry": {"type": "Point", "coordinates": [12.5, 41.9]},
        "properties": {"kind": "station"},
    }
    doc = build_envelope_feature_doc(item, catalog_id="cat", collection_id="col",
                                     visibility="public", owner="alice")
    # Simulate simplification stamp (mode != "none").
    _stamp_simplification(doc, factor=0.6, mode="tolerance")

    # Verify write shape.
    assert "simplification_factor" not in doc
    assert "simplification_mode" not in doc
    assert doc["system"]["geometry_simplification"] == {"factor": 0.6, "mode": "tolerance"}
    assert doc["visibility"] == "public"
    assert doc["owner"] == "alice"

    # Read path via _envelope_source_to_feature (add geoid for source format).
    doc["geoid"] = "g-rt-1"
    props = _feature_from(doc)
    assert props["simplification_factor"] == 0.6
    assert props["simplification_mode"] == "tolerance"
    # ABAC fields must not leak into the wire output.
    assert "visibility" not in props
    assert "owner" not in props


def test_write_read_no_simplification_round_trip():
    """When mode='none', system key absent; read path has no simplification keys."""
    item = {"id": "g-rt-2", "properties": {"kind": "field"}}
    doc = build_envelope_feature_doc(item, catalog_id="cat", collection_id="col")
    _stamp_simplification(doc, factor=1.0, mode="none")

    assert "system" not in doc
    doc["geoid"] = "g-rt-2"
    props = _feature_from(doc)
    assert "simplification_factor" not in props
    assert "simplification_mode" not in props
