#    Copyright 2025 FAO
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

import logging
from typing import Optional, Any
from dynastore.tools.cache import cached
from dynastore.modules.db_config.query_executor import (
    DQLQuery, ResultHandler, managed_transaction, DbResource
)
from dynastore.models.protocols.properties import PropertiesProtocol

logger = logging.getLogger(__name__)

# --- Queries ---

_upsert_shared_property_query = DQLQuery(
    """
    INSERT INTO catalog.shared_properties (key_name, key_value, owner_code)
    VALUES (:key_name, :key_value, :owner_code)
    ON CONFLICT (key_name) DO UPDATE SET
        key_value = EXCLUDED.key_value,
        owner_code = EXCLUDED.owner_code;
    """,
    result_handler=ResultHandler.ROWCOUNT
)

_get_shared_property_query = DQLQuery(
    "SELECT key_value FROM catalog.shared_properties WHERE key_name = :key_name",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE
)

_delete_shared_property_query = DQLQuery(
    "DELETE FROM catalog.shared_properties WHERE key_name = :key_name",
    result_handler=ResultHandler.ROWCOUNT
)

class PropertiesService(PropertiesProtocol):
    """Implementation of PropertiesProtocol for managing shared system-wide properties."""
    
    priority: int = 10
    
    def __init__(self, engine: Optional[DbResource] = None):
        self.engine = engine
        # Instance-bound cache
        self._get_property_cached = cached(maxsize=64, namespace="properties")(self._get_property_db)

    def is_available(self) -> bool:
        return self.engine is not None

    async def get_property(self, key_name: str, db_resource: Optional[Any] = None) -> Optional[str]:
        if db_resource:
            async with managed_transaction(db_resource) as conn:
                return await _get_shared_property_query.execute(conn, key_name=key_name)
        
        if not self.engine:
            # Fallback for late initialization or test context where engine is in app_state
            return None
            
        return await self._get_property_cached(key_name)

    async def _get_property_db(self, key_name: str) -> Optional[str]:
        async with managed_transaction(self.engine) as conn:
            return await _get_shared_property_query.execute(conn, key_name=key_name)

    async def set_property(self, key_name: str, key_value: str, owner_code: str, db_resource: Optional[Any] = None) -> int:
        target_engine = db_resource or self.engine
        if not target_engine:
            raise RuntimeError("PropertiesService not initialized (no engine)")
            
        async with managed_transaction(target_engine) as conn:
            rows = await _upsert_shared_property_query.execute(conn, key_name=key_name, key_value=key_value, owner_code=owner_code)
            self._get_property_cached.cache_invalidate(key_name)
            return rows

    async def delete_property(self, key_name: str, db_resource: Optional[Any] = None) -> int:
        target_engine = db_resource or self.engine
        if not target_engine:
            raise RuntimeError("PropertiesService not initialized (no engine)")
            
        async with managed_transaction(target_engine) as conn:
            rows = await _delete_shared_property_query.execute(conn, key_name=key_name)
            self._get_property_cached.cache_invalidate(key_name)
            return rows
