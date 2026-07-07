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

"""Unit tests for STAC collection extras round-trip (#1880).

Verifies two complementary halves of the fix:

1. Write-path helper ``_pack_stac_extras`` (stac_service):
   - Moves non-schema STAC extension fields (``cube:dimensions``, ``themes``,
     ``sci:citation``, …) into ``extra_metadata``.
   - Copies ``providers`` / ``summaries`` into ``extra_metadata`` as a
     fallback so they survive catalogs where the ``collection_stac`` PG
     sidecar is not active.

2. Read-path fallback in ``stac_generator.create_collection``:
   - When ``collection.providers`` / ``collection.summaries`` are not set by
     the stac sidecar, they are resolved from ``extra_metadata`` and applied
     to the typed pystac fields.
   - STAC extras stored in ``extra_metadata`` appear as ``extra_fields`` on
     the resulting ``pystac.Collection``.

No live database or HTTP stack is needed — both helpers are pure functions or
use lightweight mocks.
"""

from __future__ import annotations

from typing import Any, Dict


# ---------------------------------------------------------------------------
# 1. _pack_stac_extras — write-path helper
# ---------------------------------------------------------------------------


def test_pack_stac_extras_moves_cube_dimensions_to_extra_metadata():
    from dynastore.extensions.stac.stac_service import _pack_stac_extras

    input_data = {
        "id": "rich",
        "type": "Collection",
        "description": "A collection",
        "extent": {},
        "license": "notspecified",
        "cube:dimensions": {"x": {"type": "spatial", "axis": "x"}},
    }
    out = _pack_stac_extras(dict(input_data), "en")
    assert "cube:dimensions" not in out, "extra should be removed from top level"
    em = out.get("extra_metadata")
    assert isinstance(em, dict), "extra_metadata must be set"
    assert "cube:dimensions" in em, "cube:dimensions must land in extra_metadata"


def test_pack_stac_extras_moves_themes_and_sci_citation():
    from dynastore.extensions.stac.stac_service import _pack_stac_extras

    input_data = {
        "id": "rich",
        "type": "Collection",
        "description": "A collection",
        "extent": {},
        "license": "notspecified",
        "themes": [{"scheme": "x"}],
        "sci:citation": "cite me",
    }
    out = _pack_stac_extras(dict(input_data), "en")
    em = out.get("extra_metadata", {})
    assert "themes" in em
    assert "sci:citation" in em
    assert "themes" not in out
    assert "sci:citation" not in out


def test_pack_stac_extras_copies_providers_and_summaries_as_fallback():
    from dynastore.extensions.stac.stac_service import _pack_stac_extras

    providers = [{"name": "FAO", "roles": ["producer"]}]
    summaries = {"datetime": {"min": "2015", "max": "2020"}}
    input_data = {
        "id": "rich",
        "type": "Collection",
        "description": "A collection",
        "extent": {},
        "license": "notspecified",
        "providers": providers,
        "summaries": summaries,
    }
    out = _pack_stac_extras(dict(input_data), "en")
    # providers/summaries remain in input_data (for collection_stac path)
    assert out.get("providers") == providers
    assert out.get("summaries") == summaries
    # AND appear in extra_metadata as fallback
    em = out.get("extra_metadata", {})
    assert em.get("providers") == providers
    assert em.get("summaries") == summaries


def test_pack_stac_extras_copies_assets_extent_and_extensions_as_fallback():
    from dynastore.extensions.stac.stac_service import _pack_stac_extras

    assets = {"thumbnail": {"href": "https://example.test/thumb.png"}}
    extent = {
        "spatial": {"bbox": [[10.0, 20.0, 30.0, 40.0]]},
        "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
    }
    stac_extensions = ["https://stac-extensions.github.io/datacube/v2.3.0/schema.json"]
    input_data = {
        "id": "rich",
        "type": "Collection",
        "description": "A collection",
        "extent": extent,
        "license": "notspecified",
        "assets": assets,
        "stac_extensions": stac_extensions,
    }

    out = _pack_stac_extras(dict(input_data), "en")

    assert out.get("assets") == assets
    assert out.get("extent") == extent
    em = out.get("extra_metadata", {})
    assert em.get("assets") == assets
    assert em.get("extent") == extent
    assert em.get("stac_extensions") == stac_extensions


def test_pack_stac_extras_merges_with_existing_flat_extra_metadata():
    from dynastore.extensions.stac.stac_service import _pack_stac_extras

    input_data = {
        "id": "rich",
        "type": "Collection",
        "description": "A collection",
        "extent": {},
        "license": "notspecified",
        "extra_metadata": {"existing_key": "existing_value"},
        "cube:dimensions": {"x": {"type": "spatial"}},
    }
    out = _pack_stac_extras(dict(input_data), "en")
    em = out.get("extra_metadata", {})
    assert em.get("existing_key") == "existing_value"
    assert "cube:dimensions" in em


def test_pack_stac_extras_merges_with_language_keyed_extra_metadata():
    from dynastore.extensions.stac.stac_service import _pack_stac_extras

    input_data = {
        "id": "rich",
        "type": "Collection",
        "description": "A collection",
        "extent": {},
        "license": "notspecified",
        "extra_metadata": {"en": {"existing_key": "existing_value"}},
        "cube:dimensions": {"x": {"type": "spatial"}},
    }
    out = _pack_stac_extras(dict(input_data), "en")
    em = out.get("extra_metadata", {})
    # Should be merged into the 'en' bucket
    assert isinstance(em, dict)
    en_bucket = em.get("en", em)  # either lang-keyed or flat after merge
    assert "cube:dimensions" in en_bucket or "cube:dimensions" in em


def test_pack_stac_extras_noop_when_only_schema_fields_present():
    from dynastore.extensions.stac.stac_service import _pack_stac_extras

    input_data = {
        "id": "plain",
        "type": "Collection",
        "description": "Plain",
        "extent": {},
        "license": "notspecified",
    }
    out = _pack_stac_extras(dict(input_data), "en")
    assert "extra_metadata" not in out, "no extra_metadata when nothing to fold"


def test_pack_stac_extras_does_not_remove_schema_fields():
    from dynastore.extensions.stac.stac_service import _pack_stac_extras

    providers = [{"name": "FAO"}]
    input_data = {
        "id": "rich",
        "type": "Collection",
        "description": "A collection",
        "extent": {},
        "license": "notspecified",
        "providers": providers,
        "stac_version": "1.0.0",
        "stac_extensions": ["https://example.com/ext/v1.0/schema.json"],
    }
    out = _pack_stac_extras(dict(input_data), "en")
    assert out.get("providers") == providers
    assert out.get("stac_version") == "1.0.0"
    assert out.get("stac_extensions") is not None


# ---------------------------------------------------------------------------
# 2. stac_generator fallback — read-path providers/summaries from extra_metadata
# ---------------------------------------------------------------------------


def _make_pystac_collection_with_extra_fields(extra_fields: Dict[str, Any]):
    """Build a minimal pystac.Collection with given extra_fields pre-populated."""
    import pystac

    coll = pystac.Collection(
        id="test",
        description="test",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
            temporal=pystac.TemporalExtent([[None, None]]),
        ),
    )
    coll.extra_fields.update(extra_fields)
    return coll


def test_generator_reads_providers_from_extra_fields_when_typed_is_none():
    """Simulates the fallback block in stac_generator.create_collection:
    if collection.providers is None, take from extra_fields["providers"].
    """
    import pystac
    from dynastore.extensions.stac.stac_generator import _apply_extra_metadata_fallbacks

    collection = _make_pystac_collection_with_extra_fields({
        "providers": [{"name": "FAO", "roles": ["producer"]}],
    })
    assert collection.providers is None

    _apply_extra_metadata_fallbacks(collection, {})
    assert collection.providers is not None
    assert len(collection.providers) == 1
    assert isinstance(collection.providers[0], pystac.Provider)
    assert collection.providers[0].name == "FAO"
    # Must be removed from extra_fields to prevent double-serialization
    assert "providers" not in collection.extra_fields


def test_generator_reads_summaries_from_extra_fields_when_typed_is_none():
    """Summaries fallback from extra_fields.

    pystac always initialises ``collection.summaries`` to an empty
    ``Summaries()`` object (never ``None``), so we check ``is_empty()``
    as the "not yet populated" sentinel.
    """
    from dynastore.extensions.stac.stac_generator import _apply_extra_metadata_fallbacks

    collection = _make_pystac_collection_with_extra_fields({
        "summaries": {"datetime": {"min": "2015-01-01"}},
    })
    # pystac sets summaries to an empty Summaries() by default — never None
    assert collection.summaries is None or collection.summaries.is_empty()

    _apply_extra_metadata_fallbacks(collection, {})
    assert collection.summaries is not None
    assert not collection.summaries.is_empty()
    # Summaries are set and the key removed from extra_fields
    assert "summaries" not in collection.extra_fields


def test_generator_can_serialize_explicit_empty_summaries_from_extras():
    collection = _make_pystac_collection_with_extra_fields({"summaries": {}})

    stac_top_level = {
        "stac_version", "stac_extensions", "links", "conformsTo", "id",
        "title", "description", "extent", "keywords", "license",
        "providers", "summaries",
    }
    stored_extras = dict(collection.extra_fields)
    for internal in stac_top_level:
        collection.extra_fields.pop(internal, None)
    if (
        "summaries" in stored_extras
        and isinstance(stored_extras.get("summaries"), dict)
        and not stored_extras["summaries"]
    ):
        collection.extra_fields["summaries"] = {}

    assert collection.to_dict()["summaries"] == {}


def test_generator_does_not_overwrite_providers_from_stac_sidecar():
    """When providers are already set (from collection_stac), the fallback must not
    replace them.
    """
    import pystac
    from dynastore.extensions.stac.stac_generator import _apply_extra_metadata_fallbacks

    original_provider = pystac.Provider(name="Original")
    collection = _make_pystac_collection_with_extra_fields({
        "providers": [{"name": "Fallback"}],
    })
    collection.providers = [original_provider]

    _apply_extra_metadata_fallbacks(collection, {})
    assert len(collection.providers) == 1
    assert collection.providers[0].name == "Original", (
        "sidecar-sourced providers must not be overwritten by extra_metadata fallback"
    )
    assert "providers" not in collection.extra_fields


def test_generator_cube_dimensions_survives_via_extra_fields():
    """STAC extras stored in extra_metadata appear in collection.extra_fields
    (via the existing lines 512-517 in stac_generator).  This is a structural
    assertion — no fallback helper needed for extras that aren't typed pystac
    fields.
    """
    cube_dims = {"x": {"type": "spatial", "axis": "x"}}
    collection = _make_pystac_collection_with_extra_fields({
        "cube:dimensions": cube_dims,
    })
    # The existing stac_generator code puts extra_metadata content into
    # extra_fields.  After the stac_top_level cleanup, cube:dimensions is
    # NOT in stac_top_level so it survives.
    stac_top_level = {
        "stac_version", "stac_extensions", "links", "conformsTo", "id",
        "title", "description", "extent", "keywords", "license",
        "providers", "summaries",
    }
    for k in stac_top_level:
        collection.extra_fields.pop(k, None)
    # cube:dimensions must still be in extra_fields
    assert collection.extra_fields.get("cube:dimensions") == cube_dims


def test_generator_promotes_assets_and_item_assets_from_extra_fields():
    from dynastore.extensions.stac.stac_generator import _apply_extra_metadata_fallbacks

    assets = {"thumbnail": {"href": "https://example.test/thumb.png"}}
    item_assets = {"data": {"type": "image/tiff"}}
    collection = _make_pystac_collection_with_extra_fields({
        "assets": assets,
        "item_assets": item_assets,
    })

    _apply_extra_metadata_fallbacks(collection, {})

    as_dict = collection.to_dict()
    assert as_dict["assets"]["thumbnail"]["href"] == assets["thumbnail"]["href"]
    assert as_dict["item_assets"] == item_assets
    assert "assets" not in collection.extra_fields
    assert collection.extra_fields["item_assets"] == item_assets


def test_pg_collection_listing_restores_extra_metadata_extent_and_bbox():
    from dynastore.extensions.stac.stac_service import _pg_collections_to_stac_dicts
    from dynastore.models.shared_models import Collection

    source_extent = {
        "spatial": {"bbox": [[10.0, 20.0, 30.0, 40.0]]},
        "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
    }
    coll = Collection.model_validate({
        "id": "agera5-rh12",
        "type": "Collection",
        "description": {"en": "Relative humidity"},
        "license": "CC-BY-SA-4.0",
        "extra_metadata": {
            "en": {
                "extent": source_extent,
                "assets": {
                    "thumbnail": {"href": "https://example.test/thumb.png"}
                },
                "stac_extensions": [
                    "https://stac-extensions.github.io/datacube/v2.3.0/schema.json"
                ],
                "cube:dimensions": {
                    "time": {"type": "temporal", "extent": ["2020", "2020"]}
                },
            }
        },
    })

    rendered = _pg_collections_to_stac_dicts([coll], "en")[0]

    assert rendered["extent"] == source_extent
    assert rendered["assets"]["thumbnail"]["href"] == "https://example.test/thumb.png"
    assert (
        "https://stac-extensions.github.io/datacube/v2.3.0/schema.json"
        in rendered["stac_extensions"]
    )
    assert rendered["cube:dimensions"]["time"]["type"] == "temporal"


# ---------------------------------------------------------------------------
# 3. Round-trip integration: _pack_stac_extras → extra_metadata → generator
# ---------------------------------------------------------------------------


def test_round_trip_extras_end_to_end():
    """Minimal unit-level round-trip without a live DB:
    _pack_stac_extras folds extras into extra_metadata;
    the generator fallback reads providers/summaries back.
    """
    from dynastore.extensions.stac.stac_service import _pack_stac_extras
    from dynastore.extensions.stac.stac_generator import _apply_extra_metadata_fallbacks

    input_data = {
        "id": "rich",
        "type": "Collection",
        "description": "Datacube collection",
        "extent": {},
        "license": "notspecified",
        "providers": [{"name": "FAO", "roles": ["producer"]}],
        "summaries": {"datetime": {"min": "2015", "max": "2020"}},
        "cube:dimensions": {"x": {"type": "spatial", "axis": "x"}},
        "themes": [{"scheme": "x"}],
        "sci:citation": "cite me",
    }
    packed = _pack_stac_extras(dict(input_data), "en")

    # extra_metadata now has fallback providers/summaries and STAC extras
    em = packed.get("extra_metadata", {})
    assert "providers" in em
    assert "summaries" in em
    assert "cube:dimensions" in em
    assert "themes" in em
    assert "sci:citation" in em

    # Simulate what stac_generator.create_collection does:
    # 1. Put extra_metadata content into collection.extra_fields
    collection = _make_pystac_collection_with_extra_fields(em)

    # 2. Apply the fallback (our new helper)
    _apply_extra_metadata_fallbacks(collection, {})

    # Providers round-trip
    assert collection.providers is not None
    assert collection.providers[0].name == "FAO"
    # Summaries round-trip
    assert collection.summaries is not None
    # STAC extras survive in extra_fields
    assert collection.extra_fields.get("cube:dimensions") == {
        "x": {"type": "spatial", "axis": "x"}
    }
    assert collection.extra_fields.get("themes") == [{"scheme": "x"}]
    assert collection.extra_fields.get("sci:citation") == "cite me"
