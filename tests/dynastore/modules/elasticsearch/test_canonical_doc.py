"""Unit tests for build_canonical_index_doc (canonical ES _source builder).

Tests cover:
  - id is always geoid, not external_id
  - flat identity fields (catalog_id, collection_id, external_id, asset_id,
    validity) and _external_id transition tracker
  - geometry and bbox pass-through
  - properties reshape: known keys stay flat, unknown keys move to extras
  - system section: SYSTEM_FIELD_KEYS values from the row (content hashes land
    here, not in stats)
  - stats section: sidecar-produced values not in SYSTEM_FIELD_KEYS
  - system wins when a sidecar produces a name that overlaps SYSTEM_FIELD_KEYS
  - access pass-through and omission when falsy
  - no id/geoid leak into properties
"""
from dynastore.modules.elasticsearch.canonical_doc import build_canonical_index_doc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _row(**over):
    base = {
        "geoid": "019e6318-d99e-7da2-bdd9-1223a0d9cd35",
        "external_id": "305",
        "asset_id": "ALBL1_01",
        "validity": "[2024-01-01,)",
        "geometry_hash": "abc",
        "attributes_hash": "def",
        "transaction_time": "2026-02-26T18:09:04.131762+00:00",
    }
    base.update(over)
    return base


class _FakeSidecar:
    """Minimal sidecar stub for testing stats resolution."""

    def __init__(self, produce: dict):
        self._produce = produce   # {resolved_name: value}

    def producible_computed_names(self):
        return set(self._produce)

    def resolve_computed_value(self, row, name):  # noqa: ARG002
        if name in self._produce:
            return True, self._produce[name]
        return False, None


# ---------------------------------------------------------------------------
# Step 1 / Step 4 — identity & geoid-as-id
# ---------------------------------------------------------------------------

def test_id_is_always_geoid_even_with_external_id():
    # Pass NAME as a known field so it stays flat; unknown fields go to extras.
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={"NAME": {"type": "keyword"}},
        catalog_id="cat", collection_id="col",
        geometry={"type": "Point", "coordinates": [1, 2]},
        user_properties={"NAME": "Berat"},
    )
    assert doc["id"] == "019e6318-d99e-7da2-bdd9-1223a0d9cd35"
    assert doc["catalog_id"] == "cat" and doc["collection_id"] == "col"
    assert doc["external_id"] == "305" and doc["asset_id"] == "ALBL1_01"
    assert doc["_external_id"] == "305"            # transition tracker preserved
    assert doc["properties"] == {"NAME": "Berat"}  # user-only, known key stays flat
    assert "id" not in doc["properties"] and "geoid" not in doc["properties"]


def test_id_unchanged_when_no_external_id():
    row = _row()
    del row["external_id"]
    doc = build_canonical_index_doc(
        row, resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
    )
    assert doc["id"] == "019e6318-d99e-7da2-bdd9-1223a0d9cd35"
    assert "external_id" not in doc
    assert "_external_id" not in doc


def test_emits_both_collection_wire_member_and_collection_id():
    """The doc must carry the STAC/GeoJSON wire member ``collection`` AND the
    internal queryable ``collection_id`` (both equal the collection id).

    ``collection`` is a reserved member key that the read reconstruction
    (``unproject_item_from_es``) surfaces verbatim onto the wire Feature, and
    the ``collection``-term filter behind the REFUSE write policy queries it.
    ``collection_id`` is what the search/sort field path resolves to. Dropping
    either breaks an ES-served read or a write-conflict check.
    """
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={},
        catalog_id="cat", collection_id="col",
    )
    assert doc["collection"] == "col"
    assert doc["collection_id"] == "col"

    # Read round-trip: the wire Feature must keep ``collection``.
    from dynastore.modules.elasticsearch.items_projection import (
        unproject_item_from_es,
    )
    wire = unproject_item_from_es(doc)
    assert wire["collection"] == "col"
    # ``collection_id`` is internal — it must NOT leak onto the wire Feature.
    assert "collection_id" not in wire


# ---------------------------------------------------------------------------
# Step 5 — stats / system split + extras reshape + access
# ---------------------------------------------------------------------------

def test_stats_and_system_split_with_hash_in_system():
    # geometry_hash is producible by a sidecar AND in SYSTEM_FIELD_KEYS ->
    # must land in system, never stats.
    sc = _FakeSidecar({"area": 17999118217.7, "s2_res12": 1.22e18, "geometry_hash": "abc"})
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[sc], known_fields={},
        catalog_id="c", collection_id="k",
        user_properties={"NAME": "x"},
    )
    assert doc["stats"] == {"area": 17999118217.7, "s2_res12": 1.22e18}
    assert "geometry_hash" not in doc["stats"]
    assert doc["system"]["geometry_hash"] == "abc"
    assert doc["system"]["geoid"] == _row()["geoid"]


def test_unknown_user_property_moves_to_extras():
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={"title": {}},
        catalog_id="c", collection_id="k",
        user_properties={"title": "Known", "weird:ext": 7},
    )
    assert doc["properties"]["title"] == "Known"
    assert doc["properties"]["extras"] == {"weird:ext": 7}


def test_access_section_passthrough_and_omitted_when_empty():
    with_access = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={}, catalog_id="c", collection_id="k",
        access={"roles": ["r1"], "visibility": "private"},
    )
    assert with_access["access"] == {"roles": ["r1"], "visibility": "private"}
    without = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={}, catalog_id="c", collection_id="k",
    )
    assert "access" not in without


# ---------------------------------------------------------------------------
# Additional coverage
# ---------------------------------------------------------------------------

def test_geometry_and_bbox_pass_through():
    geom = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]}
    bbox = [0.0, 0.0, 1.0, 1.0]
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
        geometry=geom, bbox=bbox,
    )
    assert doc["geometry"] == geom
    assert doc["bbox"] == bbox


def test_geometry_and_bbox_absent_when_not_given():
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
    )
    assert "geometry" not in doc
    assert "bbox" not in doc


def test_system_section_absent_when_row_has_no_system_keys():
    row = {"geoid": "abc-123"}
    doc = build_canonical_index_doc(
        row, resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
    )
    # id must always be geoid; no external_id -> no _external_id
    assert doc["id"] == "abc-123"
    # All SYSTEM_FIELD_KEYS are absent from the row except geoid itself
    # system section should still be emitted for geoid
    assert doc["system"]["geoid"] == "abc-123"
    # no extra keys
    for key in ("external_id", "asset_id", "geometry_hash", "attributes_hash",
                "validity", "transaction_time", "deleted_at"):
        assert key not in doc["system"]


def test_stats_absent_when_no_sidecars():
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
    )
    assert "stats" not in doc


def test_multiple_sidecars_merged_into_stats():
    sc1 = _FakeSidecar({"area": 100.0})
    sc2 = _FakeSidecar({"s2_res7": "abc123"})
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[sc1, sc2], known_fields={},
        catalog_id="c", collection_id="k",
    )
    assert doc["stats"]["area"] == 100.0
    assert doc["stats"]["s2_res7"] == "abc123"


def test_first_sidecar_wins_duplicate_stat_name():
    sc1 = _FakeSidecar({"area": 111.0})
    sc2 = _FakeSidecar({"area": 999.0})
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[sc1, sc2], known_fields={},
        catalog_id="c", collection_id="k",
    )
    # sc1 was first; sc2's duplicate is skipped
    assert doc["stats"]["area"] == 111.0


def test_empty_user_properties_yields_empty_properties():
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
        user_properties={},
    )
    assert doc["properties"] == {}


def test_none_user_properties_defaults_to_empty():
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
        user_properties=None,
    )
    assert doc["properties"] == {}


def test_all_system_fields_present_on_row():
    row = {
        "geoid": "gid",
        "external_id": "eid",
        "asset_id": "aid",
        "geometry_hash": "gh",
        "attributes_hash": "ah",
        "validity": "[2020,)",
        "transaction_time": "2026-01-01T00:00:00Z",
        "deleted_at": "2026-06-01T00:00:00Z",
    }
    doc = build_canonical_index_doc(
        row, resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
    )
    sys = doc["system"]
    assert sys["geoid"] == "gid"
    assert sys["external_id"] == "eid"
    assert sys["asset_id"] == "aid"
    assert sys["geometry_hash"] == "gh"
    assert sys["attributes_hash"] == "ah"
    assert sys["validity"] == "[2020,)"
    assert sys["transaction_time"] == "2026-01-01T00:00:00Z"
    assert sys["deleted_at"] == "2026-06-01T00:00:00Z"


def test_access_omitted_when_none():
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
        access=None,
    )
    assert "access" not in doc


def test_access_omitted_when_empty_dict():
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
        access={},
    )
    assert "access" not in doc


def test_sidecar_none_value_not_written_to_stats():
    sc = _FakeSidecar({"area": None})
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[sc], known_fields={},
        catalog_id="c", collection_id="k",
    )
    assert "stats" not in doc


# ---------------------------------------------------------------------------
# metadata section — item_title / item_description / item_keywords (refs #1828)
# ---------------------------------------------------------------------------


def test_metadata_section_populated_from_row_columns():
    """When the row carries item_title / item_description / item_keywords,
    build_canonical_index_doc must surface them in the ``metadata`` section."""
    row = _row()
    row["item_title"] = {"en": "Rome", "fr": "Rome"}
    row["item_description"] = {"en": "The Eternal City"}
    row["item_keywords"] = ["city", "europe"]
    doc = build_canonical_index_doc(
        row, resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
    )
    assert "metadata" in doc
    assert doc["metadata"]["title"] == {"en": "Rome", "fr": "Rome"}
    assert doc["metadata"]["description"] == {"en": "The Eternal City"}
    assert doc["metadata"]["keywords"] == ["city", "europe"]


def test_metadata_section_absent_when_no_columns():
    """When none of item_title / item_description / item_keywords are on the
    row, the ``metadata`` section must be absent (not an empty dict)."""
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
    )
    assert "metadata" not in doc


def test_metadata_section_partial_columns():
    """If only some metadata columns are present, only those keys are emitted."""
    row = _row()
    row["item_title"] = {"en": "Partial"}
    doc = build_canonical_index_doc(
        row, resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
    )
    assert "metadata" in doc
    assert doc["metadata"] == {"title": {"en": "Partial"}}
    assert "description" not in doc["metadata"]
    assert "keywords" not in doc["metadata"]


def test_metadata_columns_do_not_leak_into_system_or_stats():
    """item_title / item_description / item_keywords must not bleed into
    ``system`` or ``stats`` sections."""
    row = _row()
    row["item_title"] = {"en": "Title"}
    row["item_keywords"] = ["kw"]
    doc = build_canonical_index_doc(
        row, resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
    )
    for section in ("system", "stats"):
        if section in doc:
            assert "title" not in doc[section]
            assert "item_title" not in doc[section]
            assert "keywords" not in doc[section]


def test_reserved_stac_key_in_user_properties_is_dropped():
    # "id" is a reserved GeoJSON/STAC member — project_item_for_es drops it from
    # properties rather than routing it into extras.
    # "NAME" is not a known field (known_fields={}), so it moves to extras.
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
        user_properties={"id": "should-be-dropped", "NAME": "in-extras"},
    )
    # reserved "id" must be absent from both properties and extras
    assert "id" not in doc["properties"]
    extras = doc["properties"].get("extras", {})
    assert "id" not in extras
    # NAME is unknown -> landed in extras (not flat)
    assert extras.get("NAME") == "in-extras"
    # top-level id is still the geoid, not the leaked value
    assert doc["id"] == "019e6318-d99e-7da2-bdd9-1223a0d9cd35"
