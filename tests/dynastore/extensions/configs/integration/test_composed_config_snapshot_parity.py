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

"""Integration parity test for #2830 (A3) — composed-config vs runtime.

Proves the composed-config view (``ConfigApiService._get_effective_configs``,
behind ``GET /configs/.../composed``) resolves the SAME value as the runtime
read path (``ConfigService.get_config``) for a snapshot-frozen catalog
(#1079 c). Before the fix the composer rebuilt the waterfall from the live
platform/code default and ignored the catalog's creation-time defaults
snapshot, so an operator reading the composed view could see a value no
driver would ever actually resolve at write/read time.
"""

import asyncio

import pytest
from dynastore.extensions.configs.config_api_service import ConfigApiService
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
from dynastore.modules.storage.driver_config import (
    ItemsWritePolicy,
    WriteConflictPolicy,
)
from dynastore.tools.discovery import get_protocol


@pytest.mark.asyncio
async def test_composed_effective_value_matches_runtime_for_snapshotted_catalog(
    app_lifespan, catalog_obj, catalog_id
):
    """Composed/effective value must equal ``ConfigService.get_config`` for a
    catalog whose creation-time defaults snapshot shadows a later platform
    default change — the #2830 correctness fix.
    """
    catalogs = get_protocol(CatalogsProtocol)
    configs = get_protocol(ConfigsProtocol)

    default_oc = ItemsWritePolicy().on_conflict
    other_oc = next(m for m in WriteConflictPolicy if m != default_oc)

    await catalogs.delete_catalog(catalog_id, force=True)
    await asyncio.sleep(1)

    try:
        # 1. Create the catalog → snapshot captures the current (code) default.
        await catalogs.create_catalog(catalog_obj)

        # 2. Change the PLATFORM default AFTER the catalog's snapshot was taken.
        await configs.set_config(
            ItemsWritePolicy,
            ItemsWritePolicy(on_conflict=other_oc),
            check_immutability=False,
        )

        # 3. Runtime resolution: shadowed by the frozen snapshot (#1079 c).
        runtime = await configs.get_config(ItemsWritePolicy, catalog_id)
        assert runtime.on_conflict == default_oc

        # 4. Composed view must agree with the runtime value.
        composer = ConfigApiService(config_service=configs)
        by_class, _sources, _tier_data = await composer._get_effective_configs(
            catalog_id=catalog_id, collection_id=None, resolved=True,
        )
        composed_value = by_class["items_write_policy"]["on_conflict"]
        assert composed_value == runtime.on_conflict == default_oc, (
            "composed/effective value diverged from ConfigService.get_config "
            "for a snapshot-frozen catalog — the composed view must never "
            "show an operator a value no driver would actually resolve"
        )
    finally:
        await configs.delete_config(ItemsWritePolicy)
        await catalogs.delete_catalog(catalog_id, force=True)
