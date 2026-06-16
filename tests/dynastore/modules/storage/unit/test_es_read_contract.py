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

"""Unit tests for the ES items read-contract reconstruction.

``ItemsElasticsearchDriver._es_source_to_feature`` turns a raw indexed
``_source`` (shaped for indexing — ``properties.extras`` tier, internal ``_*``
fields, leaked top-level echo, possibly empty geometry) into a valid GeoJSON
read-contract Feature, applying the collection's ``ItemsReadPolicy``.

Regression guard for the malformed feature read where ES served items like::

    {"type":"Feature","geometry":{},"properties":{"extras":{...}},
     "id":"325","CODE":"325","NAME":"...","catalog_id":"...", ...}
"""
from __future__ import annotations

from typing import Any, Dict

from dynastore.modules.storage.computed_fields import FeatureType
from dynastore.modules.storage.drivers.elasticsearch import ItemsElasticsearchDriver
from dynastore.modules.storage.read_policy import ItemsReadPolicy


def _malformed_source() -> Dict[str, Any]:
    return {
        "type": "Feature",
        "id": "geoid-uuid-value",
        "geometry": {},
        "collection": "glosis_demo",
        "catalog_id": "datamgr01",
        # Canonical storage shape (#1285): identity lives at the root only —
        # no legacy ``_external_id`` / ``_asset_id`` mirrors.
        "external_id": "325",
        "asset_id": "ALBL1_01",
        "properties": {
            "extras": {
                "CODE": "325",
                "NAME": "Lushnje",
                "LEVEL": 1,
                "area": 580580547.78,
                "geoid": "geoid-uuid-value",
                "created": "2026-05-22T00:00:00Z",
            }
        },
        # leaked top-level echo
        "CODE": "325",
        "NAME": "Lushnje",
        "area": 580580547.78,
    }


def _dump(feature) -> Dict[str, Any]:
    return feature.model_dump(exclude_none=True, by_alias=True)


def test_es_source_to_feature_default_policy() -> None:
    policy = ItemsReadPolicy(
        feature_type=FeatureType(
            expose=["area"],
            external_id_as_feature_id=True,
            expose_geoid=False,
            expose_created=False,
        )
    )
    feat = ItemsElasticsearchDriver._es_source_to_feature(
        _malformed_source(), policy
    )
    out = _dump(feat)

    # id replaced by external_id per policy.
    assert out["id"] == "325"
    # flat properties — no extras nesting.
    assert "extras" not in out["properties"]
    assert out["properties"]["CODE"] == "325"
    assert out["properties"]["NAME"] == "Lushnje"
    assert out["properties"]["area"] == 580580547.78
    # geoid/created suppressed by policy.
    assert "geoid" not in out["properties"]
    assert "created" not in out["properties"]
    # no top-level attribute echo, no internal fields, no catalog_id.
    for leaked in ("CODE", "NAME", "area", "catalog_id", "_external_id", "_asset_id"):
        assert leaked not in out
    # empty geometry normalised to null (excluded by exclude_none dump).
    assert "geometry" not in out or out["geometry"] is None


def test_es_source_to_feature_expose_geoid_and_created() -> None:
    policy = ItemsReadPolicy(
        feature_type=FeatureType(
            expose=["area"],
            external_id_as_feature_id=True,
            expose_geoid=True,
            expose_created=True,
        )
    )
    feat = ItemsElasticsearchDriver._es_source_to_feature(
        _malformed_source(), policy
    )
    out = _dump(feat)
    assert out["properties"]["geoid"] == "geoid-uuid-value"
    assert out["properties"]["created"] == "2026-05-22T00:00:00Z"


def test_es_source_to_feature_no_policy_keeps_source_id() -> None:
    feat = ItemsElasticsearchDriver._es_source_to_feature(
        _malformed_source(), None
    )
    out = _dump(feat)
    # Without a policy the indexed id is preserved (no external_id override).
    assert out["id"] == "geoid-uuid-value"
    assert "extras" not in out["properties"]
    assert out["properties"]["CODE"] == "325"


def test_es_source_to_feature_preserves_real_geometry() -> None:
    src = {
        "type": "Feature",
        "id": "1",
        "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
        "bbox": [1.0, 2.0, 1.0, 2.0],
        "properties": {"extras": {"k": "v"}},
    }
    feat = ItemsElasticsearchDriver._es_source_to_feature(src, None)
    out = _dump(feat)
    # geojson_pydantic canonicalises coordinates to a tuple on dump.
    assert out["geometry"]["type"] == "Point"
    assert list(out["geometry"]["coordinates"]) == [1.0, 2.0]
    assert list(out["bbox"]) == [1.0, 2.0, 1.0, 2.0]
    assert out["properties"] == {"k": "v"}
