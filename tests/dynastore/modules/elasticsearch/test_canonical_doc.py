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

"""Unit tests for build_canonical_index_doc (canonical ES _source builder).

Tests cover:
  - id is always geoid, not external_id
  - flat identity fields (catalog_id, collection_id, external_id, asset_id,
    validity) live at the root only — the identity axes are NOT duplicated into
    the system container and the legacy ``_external_id`` mirror is not written
    (#1285 identity convergence)
  - geometry and bbox pass-through
  - properties reshape: known keys stay flat, unknown keys move to extras
  - system section: the lifecycle SYSTEM_FIELD_KEYS that are NOT identity axes
    (content hashes / validity / timestamps land here, not in stats)
  - stats section: sidecar-produced values not in SYSTEM_FIELD_KEYS
  - system wins when a sidecar produces a name that overlaps SYSTEM_FIELD_KEYS
  - access pass-through and omission when falsy
  - no id/geoid leak into properties
"""
from datetime import datetime, timezone

from dynastore.modules.elasticsearch.canonical_doc import build_canonical_index_doc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _Range:
    """Minimal tstzrange Range-like stub mirroring the asyncpg.Range duck-type
    (``lower`` / ``upper`` bounds + ``lower_inc`` / ``upper_inc`` flags) that the
    canonical doc builder converts into an ES ``date_range`` body."""

    def __init__(self, lower, upper, *, lower_inc=True, upper_inc=False):
        self.lower = lower
        self.upper = upper
        self.lower_inc = lower_inc
        self.upper_inc = upper_inc


def _row(**over):
    base = {
        "geoid": "019e6318-d99e-7da2-bdd9-1223a0d9cd35",
        "external_id": "305",
        "asset_id": "ALBL1_01",
        "validity": _Range(datetime(2024, 1, 1, tzinfo=timezone.utc), None),
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

    def producible_metadata_names(self):
        return set()

    def resolve_metadata_value(self, row, name):  # noqa: ARG002
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
    assert "_external_id" not in doc               # no legacy _source mirror
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
    # geoid is an identity axis — it lives at the root, never in system.
    assert doc["id"] == _row()["geoid"]
    assert "geoid" not in doc["system"]


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
    # geoid is an identity axis (root only), and the row has no system-only
    # lifecycle keys, so the system container is omitted entirely.
    assert "system" not in doc
    assert "_external_id" not in doc


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
        "validity": _Range(datetime(2020, 1, 1, tzinfo=timezone.utc), None),
        "transaction_time": "2026-01-01T00:00:00Z",
        "deleted_at": "2026-06-01T00:00:00Z",
    }
    doc = build_canonical_index_doc(
        row, resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
    )
    # Identity axes live at the root only — never duplicated into system.
    assert doc["id"] == "gid" and "geoid" not in doc["system"]
    assert doc["external_id"] == "eid" and "external_id" not in doc["system"]
    assert doc["asset_id"] == "aid" and "asset_id" not in doc["system"]
    sys = doc["system"]
    assert sys["geometry_hash"] == "gh"
    assert sys["attributes_hash"] == "ah"
    # validity is converted to an ES date_range body: inclusive lower bound
    # (tstzrange default ``[``) -> ``gte``; open upper bound -> omitted.
    assert sys["validity"] == {"gte": "2020-01-01T00:00:00+00:00"}
    assert sys["transaction_time"] == "2026-01-01T00:00:00Z"
    assert sys["deleted_at"] == "2026-06-01T00:00:00Z"


def test_identity_convergence_no_source_duplication_and_root_is_queryable():
    """#1285 identity convergence invariant.

    The three identity axes (geoid / external_id / asset_id) are written ONCE,
    flat at the document root. They are NOT mirrored into ``_source`` under any
    of the legacy duplicate homes (``_external_id`` tracker, ``system.geoid`` /
    ``system.external_id`` / ``system.asset_id``). Search is preserved because
    the public items mapping declares the root ``external_id`` / ``asset_id`` as
    indexed ``keyword`` fields, so a ``term`` filter on them is an exact,
    inverted-index lookup (not an un-indexed ``_source`` scan).
    """
    from dynastore.modules.elasticsearch.mappings import COMMON_PROPERTIES
    from dynastore.modules.elasticsearch.items_query import (
        build_items_query,
        PUBLIC_ENVELOPE_FIELDS,
        PRIVATE_ENVELOPE_FIELDS,
    )

    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
    )
    # Identity at the root, exactly once.
    assert doc["id"] == _row()["geoid"]
    assert doc["external_id"] == "305"
    assert doc["asset_id"] == "ALBL1_01"
    # No legacy duplicate homes anywhere in _source.
    assert "_external_id" not in doc and "_asset_id" not in doc
    sys = doc.get("system", {})
    for axis in ("geoid", "external_id", "asset_id"):
        assert axis not in sys, f"identity axis {axis!r} leaked into system"

    # Root identity fields are indexed keywords → fast term lookups.
    assert COMMON_PROPERTIES["external_id"] == {"type": "keyword"}
    assert COMMON_PROPERTIES["asset_id"] == {"type": "keyword"}

    # Both index shapes filter external_id on the root keyword (search preserved).
    for fields in (PUBLIC_ENVELOPE_FIELDS, PRIVATE_ENVELOPE_FIELDS):
        q = build_items_query(external_id=["305"], fields=fields)
        clauses = q["bool"]["filter"]
        assert {"terms": {"external_id": ["305"]}} in clauses
        assert {"terms": {"_external_id": ["305"]}} not in clauses


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
# stats.centroid 2D projection for geo_point (#2232)
# ---------------------------------------------------------------------------

def test_centroid_2d_passes_through_unchanged():
    sc = _FakeSidecar({"centroid": [12.5, 41.9]})
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[sc], known_fields={},
        catalog_id="c", collection_id="k",
    )
    assert doc["stats"]["centroid"] == [12.5, 41.9]
    assert "centroid_z" not in doc["stats"]


def test_centroid_3d_projected_to_2d_with_z_split_out():
    """A POINTZ-configured centroid yields [x, y, z]; ES ``geo_point`` only
    accepts [lon, lat], so the builder projects to 2D and keeps Z as the
    separate typed scalar ``stats.centroid_z`` (refs #2232)."""
    sc = _FakeSidecar({"centroid": [12.5, 41.9, 137.0]})
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[sc], known_fields={},
        catalog_id="c", collection_id="k",
    )
    assert doc["stats"]["centroid"] == [12.5, 41.9]
    assert doc["stats"]["centroid_z"] == 137.0


def test_centroid_3d_with_null_z_drops_centroid_z():
    sc = _FakeSidecar({"centroid": [12.5, 41.9, None]})
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[sc], known_fields={},
        catalog_id="c", collection_id="k",
    )
    assert doc["stats"]["centroid"] == [12.5, 41.9]
    assert "centroid_z" not in doc["stats"]


# ---------------------------------------------------------------------------
# metadata section — via ItemMetadataSidecar (refs #1828 Phase 2 / #1838)
# ---------------------------------------------------------------------------

# The metadata section is populated exclusively through the sidecar protocol:
# ItemMetadataSidecar.producible_metadata_names() returns {"title","description",
# "keywords"} and resolve_metadata_value() reads item_* columns from the row.
# Direct row-column reads were removed in #1838; the sidecar must be in
# resolved_sidecars for metadata to appear.

from dynastore.modules.storage.drivers.pg_sidecars.item_metadata import (
    ItemMetadataSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.item_metadata_config import (
    ItemMetadataSidecarConfig,
)

_METADATA_SIDECAR = ItemMetadataSidecar(ItemMetadataSidecarConfig())


def test_metadata_section_populated_via_sidecar():
    """Metadata must come from ItemMetadataSidecar.resolve_metadata_value(), not
    from direct row-column reads. Wire shape is identical to the pre-refactor
    output (title / description / keywords in metadata{})."""
    row = _row()
    row["item_title"] = {"en": "Rome", "fr": "Rome"}
    row["item_description"] = {"en": "The Eternal City"}
    row["item_keywords"] = ["city", "europe"]
    doc = build_canonical_index_doc(
        row, resolved_sidecars=[_METADATA_SIDECAR], known_fields={},
        catalog_id="c", collection_id="k",
    )
    assert "metadata" in doc
    assert doc["metadata"]["title"] == {"en": "Rome", "fr": "Rome"}
    assert doc["metadata"]["description"] == {"en": "The Eternal City"}
    assert doc["metadata"]["keywords"] == ["city", "europe"]


def test_metadata_section_absent_when_no_sidecar():
    """Without a metadata-producing sidecar the metadata section is absent."""
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
    )
    assert "metadata" not in doc


def test_metadata_section_absent_when_sidecar_row_columns_missing():
    """When the row has no item_* columns the metadata section is absent even
    with the sidecar in the list."""
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[_METADATA_SIDECAR], known_fields={},
        catalog_id="c", collection_id="k",
    )
    assert "metadata" not in doc


def test_metadata_section_partial_columns_via_sidecar():
    """Only the keys present in the row are emitted in metadata{}."""
    row = _row()
    row["item_title"] = {"en": "Partial"}
    doc = build_canonical_index_doc(
        row, resolved_sidecars=[_METADATA_SIDECAR], known_fields={},
        catalog_id="c", collection_id="k",
    )
    assert "metadata" in doc
    assert doc["metadata"] == {"title": {"en": "Partial"}}
    assert "description" not in doc["metadata"]
    assert "keywords" not in doc["metadata"]


def test_metadata_columns_do_not_leak_into_system_or_stats():
    """item_title / item_description / item_keywords must not bleed into
    system or stats sections."""
    row = _row()
    row["item_title"] = {"en": "Title"}
    row["item_keywords"] = ["kw"]
    doc = build_canonical_index_doc(
        row, resolved_sidecars=[_METADATA_SIDECAR], known_fields={},
        catalog_id="c", collection_id="k",
    )
    for section in ("system", "stats"):
        if section in doc:
            assert "title" not in doc[section]
            assert "item_title" not in doc[section]
            assert "keywords" not in doc[section]


def test_metadata_wire_shape_byte_identical_before_and_after_sidecar_wiring():
    """The wire shape emitted by the sidecar path must be byte-for-byte
    identical to the previous direct-column read (the refactor is provenance-
    only; the ES _source must not change). Verifies the contract from #1838."""
    row = _row()
    row["item_title"] = {"en": "Rome", "fr": "Rome"}
    row["item_description"] = {"en": "The Eternal City"}
    row["item_keywords"] = ["city", "europe"]

    doc_via_sidecar = build_canonical_index_doc(
        row, resolved_sidecars=[_METADATA_SIDECAR], known_fields={},
        catalog_id="c", collection_id="k",
    )

    # Reference shape: what the previous direct-column read produced.
    expected_metadata = {
        "title": {"en": "Rome", "fr": "Rome"},
        "description": {"en": "The Eternal City"},
        "keywords": ["city", "europe"],
    }
    assert doc_via_sidecar["metadata"] == expected_metadata


def test_metadata_first_sidecar_wins_duplicate_name():
    """First metadata-producing sidecar for a given name wins; second is ignored."""
    class _MetaSidecar:
        def producible_computed_names(self): return set()
        def resolve_computed_value(self, row, name): return (False, None)
        def producible_metadata_names(self): return {"title"}
        def resolve_metadata_value(self, row, name):
            return (True, {"en": "First"}) if name == "title" else (False, None)

    class _SecondMetaSidecar:
        def producible_computed_names(self): return set()
        def resolve_computed_value(self, row, name): return (False, None)
        def producible_metadata_names(self): return {"title"}
        def resolve_metadata_value(self, row, name):
            return (True, {"en": "Second"}) if name == "title" else (False, None)

    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[_MetaSidecar(), _SecondMetaSidecar()],
        known_fields={}, catalog_id="c", collection_id="k",
    )
    assert doc["metadata"]["title"] == {"en": "First"}


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


# ---------------------------------------------------------------------------
# validity -> ES date_range conversion (#1828)
# ---------------------------------------------------------------------------

from dynastore.modules.elasticsearch.canonical_doc import _validity_to_es_range


def test_validity_range_both_bounds_default_inclusivity():
    """Default tstzrange ``[lower, upper)`` -> gte (inclusive) + lt (exclusive)."""
    r = _Range(
        datetime(2020, 1, 1, tzinfo=timezone.utc),
        datetime(2021, 1, 1, tzinfo=timezone.utc),
    )
    assert _validity_to_es_range(r) == {
        "gte": "2020-01-01T00:00:00+00:00",
        "lt": "2021-01-01T00:00:00+00:00",
    }


def test_validity_range_open_upper_bound_omits_upper():
    """An open upper bound (``[lower,)``) yields only the lower bound."""
    r = _Range(datetime(2020, 1, 1, tzinfo=timezone.utc), None)
    assert _validity_to_es_range(r) == {"gte": "2020-01-01T00:00:00+00:00"}


def test_validity_range_open_lower_bound_omits_lower():
    """An open lower bound (``(,upper]``) yields only the upper bound."""
    r = _Range(None, datetime(2021, 1, 1, tzinfo=timezone.utc), lower_inc=False, upper_inc=True)
    assert _validity_to_es_range(r) == {"lte": "2021-01-01T00:00:00+00:00"}


def test_validity_range_exclusive_lower_inclusive_upper():
    """Inclusivity flags map to gt/lte respectively."""
    r = _Range(
        datetime(2020, 1, 1, tzinfo=timezone.utc),
        datetime(2021, 1, 1, tzinfo=timezone.utc),
        lower_inc=False,
        upper_inc=True,
    )
    assert _validity_to_es_range(r) == {
        "gt": "2020-01-01T00:00:00+00:00",
        "lte": "2021-01-01T00:00:00+00:00",
    }


def test_validity_fully_open_window_is_dropped():
    """A range with no bounds carries no temporal info -> None (field dropped)."""
    assert _validity_to_es_range(_Range(None, None)) is None


def test_validity_none_is_dropped():
    assert _validity_to_es_range(None) is None


def test_validity_dict_passes_through_idempotently():
    """A pre-converted range body (re-index path) is returned unchanged."""
    body = {"gte": "2020-01-01T00:00:00+00:00", "lt": "2021-01-01T00:00:00+00:00"}
    assert _validity_to_es_range(body) == body


def test_validity_empty_dict_is_dropped():
    assert _validity_to_es_range({}) is None


def test_validity_bare_string_is_dropped_not_misread_as_range():
    """A bare str exposes ``.lower``/``.upper`` *methods* but no ``lower_inc``;
    it must NOT be mistaken for a range, and cannot be a valid date_range body,
    so it is dropped."""
    assert _validity_to_es_range("[2020-01-01,)") is None


def test_validity_string_bounds_pass_through_without_isoformat():
    """Range bounds that are already strings (no isoformat) pass through as-is."""
    r = _Range("2020-01-01T00:00:00+00:00", None)
    assert _validity_to_es_range(r) == {"gte": "2020-01-01T00:00:00+00:00"}


# ---------------------------------------------------------------------------
# ES-only STAC: stac_reserved_members round-trip (refs #1757)
# ---------------------------------------------------------------------------
# For stac_level=items, stac_storage=ES (no PG sidecar), per-item STAC content
# (assets, stac_extensions) lives only in the inbound feature.  The write
# path must thread these into the canonical _source so unproject_item_from_es
# surfaces them verbatim on read.


def test_stac_reserved_members_assets_stored_and_round_trips():
    """assets from stac_reserved_members survive the canonical doc and unproject."""
    from dynastore.modules.elasticsearch.items_projection import unproject_item_from_es
    from dynastore.models.shared_models import Feature

    assets = {
        "data": {
            "href": "https://example.com/data.tif",
            "type": "image/tiff; application=geotiff",
            "roles": ["data"],
        }
    }
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
        geometry={"type": "Point", "coordinates": [12.0, 41.0]},
        stac_reserved_members={"assets": assets},
    )

    # assets must be stored at the top level of the ES _source
    assert doc["assets"] == assets

    # unproject restores the wire shape with assets intact
    wire = unproject_item_from_es(doc)
    assert wire["assets"] == assets

    # Feature.model_validate carries assets through (via extra="allow")
    feature = Feature.model_validate(wire)
    assert getattr(feature, "assets", None) == assets


def test_stac_reserved_members_stac_extensions_stored_and_round_trips():
    """stac_extensions from stac_reserved_members survive canonical doc and unproject."""
    from dynastore.modules.elasticsearch.items_projection import unproject_item_from_es
    from dynastore.models.shared_models import Feature

    exts = [
        "https://stac-extensions.github.io/eo/v1.0.0/schema.json",
        "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
    ]
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
        stac_reserved_members={"stac_extensions": exts},
    )

    assert doc["stac_extensions"] == exts

    wire = unproject_item_from_es(doc)
    assert wire["stac_extensions"] == exts

    feature = Feature.model_validate(wire)
    assert getattr(feature, "stac_extensions", None) == exts


def test_stac_reserved_members_assets_and_extensions_together():
    """Full ES-only STAC round-trip: assets + stac_extensions survive write→read."""
    from dynastore.modules.elasticsearch.items_projection import unproject_item_from_es
    from dynastore.models.shared_models import Feature

    assets = {"thumbnail": {"href": "https://example.com/thumb.png", "roles": ["thumbnail"]}}
    exts = ["https://stac-extensions.github.io/eo/v1.0.0/schema.json"]

    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={"cloud_cover": {}},
        catalog_id="c", collection_id="k",
        geometry={"type": "Point", "coordinates": [0.0, 0.0]},
        bbox=[0.0, 0.0, 0.0, 0.0],
        user_properties={"cloud_cover": 12, "datetime": "2024-01-01T00:00:00Z"},
        stac_reserved_members={"assets": assets, "stac_extensions": exts},
    )

    # Both stored at top level of _source
    assert doc["assets"] == assets
    assert doc["stac_extensions"] == exts
    # user properties are not corrupted
    assert doc["properties"]["cloud_cover"] == 12

    # Round-trip: unproject → Feature
    wire = unproject_item_from_es(doc)
    feature = Feature.model_validate(wire)
    assert getattr(feature, "assets", None) == assets
    assert getattr(feature, "stac_extensions", None) == exts
    # user properties survive
    assert feature.properties.get("cloud_cover") == 12


def test_stac_reserved_members_none_omits_sections():
    """When stac_reserved_members is None, no extra keys appear in the doc."""
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
        stac_reserved_members=None,
    )
    assert "assets" not in doc
    assert "stac_extensions" not in doc


def test_stac_reserved_members_empty_omits_sections():
    """Empty stac_reserved_members dict behaves like None."""
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
        stac_reserved_members={},
    )
    assert "assets" not in doc
    assert "stac_extensions" not in doc


def test_stac_reserved_members_none_values_skipped():
    """Keys with None values inside stac_reserved_members are not written."""
    doc = build_canonical_index_doc(
        _row(), resolved_sidecars=[], known_fields={},
        catalog_id="c", collection_id="k",
        stac_reserved_members={"assets": None, "stac_extensions": None},
    )
    assert "assets" not in doc
    assert "stac_extensions" not in doc
