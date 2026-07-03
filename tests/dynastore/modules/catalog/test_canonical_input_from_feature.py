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

"""No-PG canonical index input from a feature dict (file-backed collections, #375).

The canonical ES document is normally built from a raw PostgreSQL row. A
file-backed collection has no PG rows, so we elevate the feature-derived fallback
(previously inline in ItemsElasticsearchDriver.write_entities) into a first-class
function that turns a serialized feature dict into a CanonicalIndexInput with no
database access — the same struct the PG path produces, minus stats/system that
only a PG row + sidecars can supply.
"""
from __future__ import annotations

import pytest


def _feature_dict():
    return {
        "type": "Feature",
        "id": "ignored-source-id",
        "geometry": {"type": "Point", "coordinates": [12.0, 41.9]},
        "bbox": [12.0, 41.9, 12.0, 41.9],
        "properties": {"name": "Rome", "pop": 2800000},
        "assets": {"data": {"href": "gs://b/rome.parquet"}},
        "stac_extensions": ["https://stac-extensions.github.io/x/v1.0.0/schema.json"],
    }


def test_canonical_input_from_feature_sets_geoid_in_row():
    from dynastore.modules.catalog.canonical_index_read import canonical_input_from_feature
    ci = canonical_input_from_feature(
        _feature_dict(), "cat1", "col1", geoid="geo-123",
    )
    assert ci.row["geoid"] == "geo-123"


def test_canonical_input_from_feature_extracts_geometry_and_bbox():
    from dynastore.modules.catalog.canonical_index_read import canonical_input_from_feature
    ci = canonical_input_from_feature(_feature_dict(), "cat1", "col1", geoid="geo-123")
    assert ci.geometry == {"type": "Point", "coordinates": [12.0, 41.9]}
    assert ci.bbox == [12.0, 41.9, 12.0, 41.9]


def test_canonical_input_from_feature_user_properties_exclude_system_keys():
    from dynastore.modules.catalog.canonical_index_read import canonical_input_from_feature
    from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS
    sys_key = next(iter(SYSTEM_FIELD_KEYS))
    feat = _feature_dict()
    feat["properties"][sys_key] = "leaked"
    ci = canonical_input_from_feature(feat, "cat1", "col1", geoid="geo-123")
    user_props = ci.user_properties or {}
    assert "name" in user_props
    assert sys_key not in user_props


def test_canonical_input_from_feature_routes_stac_reserved_members():
    from dynastore.modules.catalog.canonical_index_read import canonical_input_from_feature
    ci = canonical_input_from_feature(_feature_dict(), "cat1", "col1", geoid="geo-123")
    assert ci.stac_reserved_members is not None
    assert "assets" in ci.stac_reserved_members
    assert "stac_extensions" in ci.stac_reserved_members
    # Reserved members must NOT also leak into user_properties.
    assert "assets" not in (ci.user_properties or {})
    assert "stac_extensions" not in (ci.user_properties or {})


def test_canonical_input_from_feature_threads_external_and_asset_id():
    from dynastore.modules.catalog.canonical_index_read import canonical_input_from_feature
    ci = canonical_input_from_feature(
        _feature_dict(), "cat1", "col1",
        geoid="geo-123", external_id="ext-9", asset_id="asset-7",
    )
    assert ci.row["external_id"] == "ext-9"
    assert ci.row["asset_id"] == "asset-7"


def test_canonical_input_from_feature_computes_bbox_from_geometry_when_absent():
    """#2864: a feature with no explicit ``bbox`` still yields one — computed
    from ``geometry`` — so the ES-only write path's ``_source`` doesn't
    silently omit bbox (spatial filtering/sort depend on it)."""
    from dynastore.modules.catalog.canonical_index_read import canonical_input_from_feature
    feat = _feature_dict()
    del feat["bbox"]
    ci = canonical_input_from_feature(feat, "cat1", "col1", geoid="geo-123")
    assert ci.bbox == pytest.approx([12.0, 41.9, 12.0, 41.9])


def test_canonical_input_from_feature_bbox_none_when_no_geometry():
    """No geometry, no explicit bbox → bbox stays None (nothing to derive)."""
    from dynastore.modules.catalog.canonical_index_read import canonical_input_from_feature
    feat = _feature_dict()
    del feat["bbox"]
    feat["geometry"] = None
    ci = canonical_input_from_feature(feat, "cat1", "col1", geoid="geo-123")
    assert ci.bbox is None


def test_canonical_input_from_feature_no_pg_no_sidecars():
    """The whole point: no DB access, empty resolved_sidecars."""
    from dynastore.modules.catalog.canonical_index_read import canonical_input_from_feature
    ci = canonical_input_from_feature(_feature_dict(), "cat1", "col1", geoid="geo-123")
    assert ci.resolved_sidecars == []
    assert ci.access is None


def test_canonical_input_from_feature_feeds_build_canonical_index_doc():
    """Smoke parity: the produced input drives build_canonical_index_doc to a doc
    whose identity + geometry survive, with no PG row in sight."""
    from dynastore.modules.catalog.canonical_index_read import canonical_input_from_feature
    from dynastore.modules.elasticsearch.canonical_doc import build_canonical_index_doc
    ci = canonical_input_from_feature(_feature_dict(), "cat1", "col1", geoid="geo-123")
    doc = build_canonical_index_doc(
        ci.row,
        resolved_sidecars=ci.resolved_sidecars,
        known_fields={},
        catalog_id="cat1",
        collection_id="col1",
        geometry=ci.geometry,
        bbox=ci.bbox,
        user_properties=ci.user_properties,
        access=ci.access,
        stac_reserved_members=ci.stac_reserved_members,
    )
    assert doc.get("id") == "geo-123"
    assert doc.get("geometry") == {"type": "Point", "coordinates": [12.0, 41.9]}
    # Unknown (un-typed) user attributes land in the flattened long-tail lane
    # properties.extras; STAC reserved members surface at the doc top level.
    assert doc.get("properties", {}).get("extras", {}).get("name") == "Rome"
    assert "assets" in doc
    assert doc.get("stac_extensions") == ["https://stac-extensions.github.io/x/v1.0.0/schema.json"]
