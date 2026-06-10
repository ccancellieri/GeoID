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

"""assets_gcs_uploads preset (#972) — assets-tier GCS upload, catalog_scopable.

Covers the bundle shape and the ``catalog_scopable`` dispatch at both
catalog and collection URL families via
``presets_api._preset_reachable_at`` and
``presets_api._resolve_preset_for_scope``.
"""
from __future__ import annotations

from dynastore.extensions.configs.presets_api import (
    _preset_reachable_at,
    _resolve_preset_for_scope,
)
from dynastore.modules.storage.presets import PresetTier, get_preset
from dynastore.modules.storage.routing_config import (
    AssetRoutingConfig,
    Operation,
)
from fastapi import HTTPException

import pytest


def test_assets_gcs_uploads_registered_as_catalog_scopable_assets_tier():
    p = get_preset("assets_gcs_uploads")
    assert p.name == "assets_gcs_uploads"
    assert p.tier == PresetTier.ASSETS
    assert p.catalog_scopable is True
    assert p.description, "preset must carry a non-empty description"


def test_assets_gcs_uploads_bundle_has_single_asset_entry():
    bundle = get_preset("assets_gcs_uploads").build()
    entries = list(bundle.iter_apply())
    assert len(entries) == 1
    entry = entries[0]
    assert entry.slot == "asset_template"
    assert entry.config_cls is AssetRoutingConfig
    assert isinstance(entry.instance, AssetRoutingConfig)
    # The preset leaves scope empty — the admin endpoint layers the
    # URL-derived catalog_id / collection_id on top.
    assert dict(entry.scope) == {}


def test_assets_gcs_uploads_pins_gcp_module_upload_driver():
    bundle = get_preset("assets_gcs_uploads").build()
    asset_template = bundle.asset_template
    assert isinstance(asset_template, AssetRoutingConfig)
    upload_entries = asset_template.operations.get(str(Operation.UPLOAD), [])
    assert len(upload_entries) == 1
    entry = upload_entries[0]
    assert entry.driver_ref == "gcp_module"
    assert entry.source == "operator", "UPLOAD entry must be operator-managed"


# --- catalog_scopable dispatch (mirrors items_es_private test shape) --------


def test_reachable_at_collection_and_catalog_when_scopable():
    """A catalog_scopable ASSETS preset reaches BOTH URL families."""
    preset = get_preset("assets_gcs_uploads")
    assert _preset_reachable_at(preset, PresetTier.COLLECTION) is True
    assert _preset_reachable_at(preset, PresetTier.CATALOG) is True


def test_not_reachable_at_platform_scope():
    preset = get_preset("assets_gcs_uploads")
    assert _preset_reachable_at(preset, PresetTier.PLATFORM) is False


def test_resolve_preset_for_scope_allows_catalog_for_scopable():
    preset = _resolve_preset_for_scope("assets_gcs_uploads", PresetTier.CATALOG)
    assert preset.name == "assets_gcs_uploads"


def test_resolve_preset_for_scope_allows_collection_for_scopable():
    preset = _resolve_preset_for_scope("assets_gcs_uploads", PresetTier.COLLECTION)
    assert preset.name == "assets_gcs_uploads"


def test_resolve_preset_for_scope_409_at_platform():
    with pytest.raises(HTTPException) as exc:
        _resolve_preset_for_scope("assets_gcs_uploads", PresetTier.PLATFORM)
    assert exc.value.status_code == 409
    assert "assets" in exc.value.detail.lower()
