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

"""TDD tests for the level-agnostic canonical envelope core (refs #1285/#1800).

The item envelope (#1800) is generalised into a single modular, pluggable
assembler usable for every entity level (catalog / collection / item / asset):

  * ``system``      — the core (identity + lifecycle), always at the top
  * ``properties``  — the attribute bag (unknown keys routed to ``extras``)
  * ``stats``       — derived values
  * ``access``      — IAM-plugged authorization sidecar
  * reserved members — protocol-structural keys (geometry/bbox/extent/links/
    assets/stac_extensions …) surfaced verbatim by the read-time projector

These tests pin the core contract AND guard that the item builder, refactored
to delegate to the core, keeps byte-identical output.
"""
from __future__ import annotations

from dynastore.modules.elasticsearch.canonical_doc import (
    build_canonical_envelope,
    build_canonical_index_doc,
)
from dynastore.modules.elasticsearch.items_projection import (
    TIER_1_FIELDS,
    unproject_envelope_from_es,
    unproject_item_from_es,
)


# ---------------------------------------------------------------------------
# Core contract
# ---------------------------------------------------------------------------


def test_envelope_assembles_named_sections():
    doc = build_canonical_envelope(
        identity={"id": "C1", "catalog_id": "cat", "collection_id": "C1"},
        properties={"title": "Hi", "license": "CC-BY", "foo:bar": 1},
        known_fields={"title": {"type": "text"}, "license": {"type": "keyword"}},
        reserved_members={"extent": {"spatial": {"bbox": [[0, 0, 1, 1]]}}},
        system={"transaction_time": "2026-06-04T00:00:00Z"},
        access={"owner": "u1"},
    )
    # flat identity at the top
    assert doc["id"] == "C1"
    assert doc["catalog_id"] == "cat"
    # reserved structural member surfaced verbatim
    assert doc["extent"] == {"spatial": {"bbox": [[0, 0, 1, 1]]}}
    # known attrs stay flat; unknown routed to extras
    assert doc["properties"]["title"] == "Hi"
    assert doc["properties"]["license"] == "CC-BY"
    assert doc["properties"]["extras"] == {"foo:bar": 1}
    # named containers present
    assert doc["system"] == {"transaction_time": "2026-06-04T00:00:00Z"}
    assert doc["access"] == {"owner": "u1"}
    assert "stats" not in doc  # omitted when empty


def test_envelope_omits_empty_optional_sections():
    doc = build_canonical_envelope(
        identity={"id": "K1", "catalog_id": "K1"},
        properties={},
        known_fields={},
    )
    assert doc["id"] == "K1"
    assert doc["properties"] == {}
    for absent in ("system", "stats", "access", "metadata"):
        assert absent not in doc


# ---------------------------------------------------------------------------
# metadata section in build_canonical_envelope (refs #1828)
# ---------------------------------------------------------------------------


def test_envelope_emits_metadata_when_provided():
    """build_canonical_envelope must emit a ``metadata`` section when a non-empty
    dict is passed, with the same copy-semantics as system/stats/access."""
    meta = {"title": {"en": "Hello", "fr": "Bonjour"}, "keywords": ["a", "b"]}
    doc = build_canonical_envelope(
        identity={"id": "M1", "catalog_id": "cat"},
        properties={},
        known_fields={},
        metadata=meta,
    )
    assert doc["metadata"] == meta
    # metadata must not pollute other sections
    assert "metadata" not in doc.get("properties", {})
    assert "metadata" not in doc.get("system", {})


def test_envelope_omits_metadata_when_not_provided():
    """Omitting ``metadata`` (None, the default) must leave the key absent."""
    doc = build_canonical_envelope(
        identity={"id": "M2", "catalog_id": "cat"},
        properties={},
        known_fields={},
    )
    assert "metadata" not in doc


def test_envelope_omits_metadata_when_empty_dict():
    """An empty dict passed as ``metadata`` must be omitted (same as system/stats)."""
    doc = build_canonical_envelope(
        identity={"id": "M3", "catalog_id": "cat"},
        properties={},
        known_fields={},
        metadata={},
    )
    assert "metadata" not in doc


def test_envelope_metadata_is_copy_not_alias():
    """build_canonical_envelope must copy the metadata dict, not alias it."""
    meta = {"title": {"en": "Original"}}
    doc = build_canonical_envelope(
        identity={"id": "M4", "catalog_id": "cat"},
        properties={},
        known_fields={},
        metadata=meta,
    )
    # Mutating the original must not affect the stored doc.
    meta["title"] = {"en": "Changed"}
    assert doc["metadata"]["title"] == {"en": "Original"}


def test_envelope_pure_does_not_mutate_inputs():
    props = {"title": "x"}
    ident = {"id": "I", "catalog_id": "c"}
    build_canonical_envelope(identity=ident, properties=props, known_fields={})
    assert props == {"title": "x"}
    assert ident == {"id": "I", "catalog_id": "c"}


# ---------------------------------------------------------------------------
# Read-side: generic unprojection parameterised by reserved member set
# ---------------------------------------------------------------------------


def test_unproject_envelope_hoists_extras_and_keeps_only_reserved():
    source = {
        "id": "C1",
        "catalog_id": "cat",
        "collection_id": "C1",
        "_external_id": "x",
        "extent": {"spatial": {"bbox": [[0, 0, 1, 1]]}},
        "properties": {"title": "Hi", "extras": {"foo:bar": 1}},
        "system": {"transaction_time": "t"},
        "access": {"owner": "u1"},
    }
    wire = unproject_envelope_from_es(
        source, reserved_member_keys=frozenset({"id", "type", "extent"})
    )
    # extras hoisted back to flat properties
    assert wire["properties"] == {"title": "Hi", "foo:bar": 1}
    # only reserved members survive at top level
    assert wire["id"] == "C1"
    assert wire["extent"] == {"spatial": {"bbox": [[0, 0, 1, 1]]}}
    for dropped in ("catalog_id", "collection_id", "_external_id", "system", "access"):
        assert dropped not in wire


# ---------------------------------------------------------------------------
# Item parity — the refactored item builder must keep identical output
# ---------------------------------------------------------------------------


def _item_kwargs():
    row = {
        "geoid": "019e917f-dd1d-7f5b-b133-54e8a96c4ec0",
        "external_id": "EXT-1",
        "geometry_hash": "abc",
        "attributes_hash": "def",
    }
    return dict(
        row=row,
        resolved_sidecars=[],
        known_fields=dict(TIER_1_FIELDS),
        catalog_id="cat",
        collection_id="coll",
        geometry={"type": "Point", "coordinates": [12.49, 41.89]},
        bbox=[12.49, 41.89, 12.49, 41.89],
        user_properties={"datetime": "2026-06-04T00:00:00Z", "note": "n", "platform": "s2a"},
    )


def test_item_builder_output_is_canonical_and_roundtrips():
    doc = build_canonical_index_doc(**_item_kwargs())
    # identity
    assert doc["id"] == "019e917f-dd1d-7f5b-b133-54e8a96c4ec0"
    assert doc["catalog_id"] == "cat"
    assert doc["collection_id"] == "coll"
    assert doc["_external_id"] == "EXT-1"
    # known stays flat, unknown ('note') to extras
    assert doc["properties"]["datetime"] == "2026-06-04T00:00:00Z"
    assert doc["properties"]["platform"] == "s2a"
    assert doc["properties"]["extras"] == {"note": "n"}
    # system from SYSTEM_FIELD_KEYS
    assert doc["system"]["geometry_hash"] == "abc"
    assert doc["system"]["attributes_hash"] == "def"
    # read path surfaces id + clean flat properties (the listing/search contract)
    wire = unproject_item_from_es(doc)
    assert wire["id"] == "019e917f-dd1d-7f5b-b133-54e8a96c4ec0"
    assert wire["properties"] == {
        "datetime": "2026-06-04T00:00:00Z",
        "note": "n",
        "platform": "s2a",
    }
    assert "system" not in wire and "catalog_id" not in wire
