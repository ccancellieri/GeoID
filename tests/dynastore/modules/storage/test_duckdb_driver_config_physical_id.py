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

"""Tests for ItemsDuckdbDriverConfig.asset_physical_id field (#2296)."""

from __future__ import annotations

from dynastore.modules.storage.driver_config import ItemsDuckdbDriverConfig


_PHYS_ID = "01966b7f-0000-7000-8000-000000000001"


class TestItemsDuckdbDriverConfigAssetPhysicalId:
    def test_defaults_to_none(self):
        cfg = ItemsDuckdbDriverConfig()
        assert cfg.asset_physical_id is None

    def test_accepts_uuid_string(self):
        cfg = ItemsDuckdbDriverConfig(asset_physical_id=_PHYS_ID)
        assert cfg.asset_physical_id == _PHYS_ID

    def test_asset_id_and_physical_id_coexist(self):
        cfg = ItemsDuckdbDriverConfig(asset_id="my-asset", asset_physical_id=_PHYS_ID)
        assert cfg.asset_id == "my-asset"
        assert cfg.asset_physical_id == _PHYS_ID

    def test_model_copy_preserves_physical_id(self):
        base = ItemsDuckdbDriverConfig(asset_id="my-asset")
        updated = base.model_copy(update={"asset_physical_id": _PHYS_ID})
        assert updated.asset_physical_id == _PHYS_ID
        assert updated.asset_id == "my-asset"

    def test_model_dump_includes_physical_id(self):
        cfg = ItemsDuckdbDriverConfig(asset_physical_id=_PHYS_ID)
        dumped = cfg.model_dump()
        assert dumped["asset_physical_id"] == _PHYS_ID

    def test_model_dump_physical_id_absent_when_none(self):
        cfg = ItemsDuckdbDriverConfig()
        dumped = cfg.model_dump()
        # None value present in dump; key exists with None
        assert dumped.get("asset_physical_id") is None

    def test_physical_id_not_in_json_schema_as_user_settable(self):
        """asset_physical_id must carry readOnly=true in the JSON schema
        (Computed marker) so the config API rejects user writes."""
        schema = ItemsDuckdbDriverConfig.model_json_schema()
        props = schema.get("properties", {})
        phys = props.get("asset_physical_id", {})
        assert phys.get("readOnly") is True, (
            "asset_physical_id must be readOnly in the JSON schema "
            "(Computed marker was not applied)"
        )
