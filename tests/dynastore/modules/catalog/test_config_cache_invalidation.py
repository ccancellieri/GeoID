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

import pytest
from tests.dynastore.test_utils import generate_test_id
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.catalog.config_service import (
    ConfigService,
)
from dynastore.models.plugin_config import PluginConfig
from dynastore.modules.db_config.platform_config_service import PlatformConfigService
from dynastore.modules.tiles.tiles_config import TilesConfig
from dynastore.models.driver_context import DriverContext

@pytest.mark.enable_modules("db_config", "db", "catalog", "stac", "tiles", "collection_postgresql", "catalog_postgresql")
@pytest.mark.enable_extensions("tiles")
@pytest.mark.asyncio
async def test_hierarchical_config_cache_invalidation(app_lifespan, data_id):
    """
    Verifies that updating a catalog-level configuration correctly 
    invalidates/updates the effective configuration seen at the collection level.
    """
    catalog_id = f"cat_cache_{data_id}"
    collection_id = f"coll_cache_{generate_test_id()}"
    plugin_id = TilesConfig
    
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import ConfigsProtocol, CatalogsProtocol
    from dynastore.tools.protocol_helpers import get_engine
    
    config_service: ConfigService = get_protocol(ConfigsProtocol)
    catalogs = get_protocol(CatalogsProtocol)

    # 0. Setup: Create catalog and collection
    async with managed_transaction(get_engine()) as conn:
        await catalogs.ensure_catalog_exists(catalog_id, ctx=DriverContext(db_resource=conn))
        if not await catalogs.get_collection_model(catalog_id, collection_id, db_resource=conn):
            await catalogs.create_collection(catalog_id, {"id": collection_id}, lang="*", ctx=DriverContext(db_resource=conn))
    
    # 1. Set a catalog-level config
    config_v1 = TilesConfig(enabled=True, max_zoom=10)
    await config_service.set_config(plugin_id, config_v1, catalog_id=catalog_id)
    
    # 2. Get config at collection level (should fallback to catalog)
    effective_v1 = await config_service.get_config(plugin_id, catalog_id=catalog_id, collection_id=collection_id)
    assert effective_v1.max_zoom == 10
    
    # 3. Update catalog-level config
    config_v2 = TilesConfig(enabled=True, max_zoom=11)
    await config_service.set_config(plugin_id, config_v2, catalog_id=catalog_id)
    
    # 4. Get config at collection level again
    effective_v2 = await config_service.get_config(plugin_id, catalog_id=catalog_id, collection_id=collection_id)
    assert effective_v2.max_zoom == 11, "Stale config served! Cache invalidation failed."
    
    # 5. Set a collection-level override
    config_override = TilesConfig(enabled=True, max_zoom=12)
    await config_service.set_config(plugin_id, config_override, catalog_id=catalog_id, collection_id=collection_id)
    
    # 6. Verify collection override
    effective_v3 = await config_service.get_config(plugin_id, catalog_id=catalog_id, collection_id=collection_id)
    assert effective_v3.max_zoom == 12
    
    # 7. Delete collection override and verify fallback to catalog v2
    await config_service.delete_config(plugin_id, catalog_id=catalog_id, collection_id=collection_id)
    effective_v4 = await config_service.get_config(plugin_id, catalog_id=catalog_id, collection_id=collection_id)
    assert effective_v4.max_zoom == 11
