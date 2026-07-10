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

"""Tests for the reshaped composed-config DTOs.

Asserts that the new DTOs carry no legacy envelope keys (``class_key``,
``value``, ``source``, ``resolved_drivers``) and that the nested
``configs`` tree roundtrips through pydantic cleanly.  Post-#517 the
top-level ``meta`` parallel tree is gone — per-class field docs live
INLINE on each leaf as ``_meta`` (asserted in test_config_api_service.py
via the composer).
"""

from dynastore.extensions.configs.config_api_dto import (
    CatalogConfigResponse,
    CollectionConfigResponse,
    DriverRef,
    PatchConfigBody,
    PlatformConfigResponse,
)


def test_driver_ref_defaults():
    ref = DriverRef(driver_ref="catalog_core_postgresql_driver")
    assert ref.on_failure == "fatal"
    assert not hasattr(ref, "write_mode")
    assert ref.links == []


def test_driver_ref_no_config_ref_field_post_f7d3():
    """Cycle F.7d.3 dropped the ``config_ref: Optional[str]`` scalar.
    Routing entries with a registered config carry a HATEOAS
    ``rel="driver-config"`` Link instead; un-registered drivers carry
    no link at all."""
    ref = DriverRef(driver_ref="SomeDriver")
    assert not hasattr(ref, "config_ref")
    assert ref.links == []


def test_platform_response_defaults_slim():
    r = PlatformConfigResponse()
    assert r.scope == "platform"
    assert r.configs == {}
    # ``categories``, ``routing_resolution`` and (#517) the top-level
    # ``meta`` parallel tree were all retired.
    assert not hasattr(r, "categories")
    assert not hasattr(r, "routing_resolution")
    assert not hasattr(r, "meta")


def test_platform_response_no_legacy_keys():
    r = PlatformConfigResponse(configs={"platform": {"web": {"WebConfig": {"brand_name": "x"}}}})
    dumped = r.model_dump()
    # The composed tree must not carry any of the old envelope keys.
    flat = str(dumped)
    assert "class_key" not in flat
    assert "resolved_drivers" not in flat


def test_catalog_response_nested_tree_roundtrip():
    r = CatalogConfigResponse(
        catalog_id="cat_1",
        configs={
            "storage": {
                "routing": {
                    "catalog_routing_config": {
                        "enabled": True,
                        "operations": {
                            "WRITE": [
                                {"driver_ref": "catalog_core_postgresql_driver",
                                 "on_failure": "fatal",
                                 "write_mode": "sync"}
                            ],
                        },
                    },
                },
                "drivers": {
                    "catalog": {"catalog_core_postgresql_driver": {"enabled": True}},
                },
            },
        },
    )
    assert r.catalog_id == "cat_1"
    assert not hasattr(r, "meta")
    assert "routing" in r.configs["storage"]


def test_collection_response_carries_ids():
    r = CollectionConfigResponse(catalog_id="cat", collection_id="coll", configs={})
    assert r.catalog_id == "cat"
    assert r.collection_id == "coll"


def test_inline_meta_and_links_on_leaf_payload():
    """Post-#517: per-class docs and HATEOAS edit affordances live INLINE
    on each plugin leaf as ``_meta`` and ``_links`` siblings of the
    plugin's own fields.  The top-level ``meta`` parallel tree is gone.

    The composer injects these — here we only assert the response model
    accepts arbitrary nested dicts in ``configs`` (the leaf may carry
    ``_meta`` / ``_links`` alongside the plugin's own fields).
    """
    r = CatalogConfigResponse(
        catalog_id="cat",
        configs={
            "platform": {
                "web": {
                    "web_config": {
                        "brand_name": "DynaStore",
                        "_meta": {"docs": {"brand_name": "Display name."}},
                        "_links": [
                            {"rel": "self", "method": "GET",
                             "href": "/configs/catalogs/cat/plugins/web_config"},
                        ],
                    },
                },
            },
        },
    )
    leaf = r.configs["platform"]["web"]["web_config"]
    assert leaf["brand_name"] == "DynaStore"
    assert leaf["_meta"]["docs"]["brand_name"] == "Display name."
    assert leaf["_links"][0]["rel"] == "self"


def test_patch_body_accepts_null_for_delete():
    body = PatchConfigBody.model_validate({"WebConfig": {"brand_name": "x"}, "StatsConfig": None})
    assert body.root["WebConfig"] == {"brand_name": "x"}
    assert body.root["StatsConfig"] is None
