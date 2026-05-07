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

from typing import Protocol, Optional, List, Any, runtime_checkable
from dynastore.modules.db_config.query_executor import DbResource

@runtime_checkable
class CRSProtocol(Protocol):
    """
    Protocol for managing and resolving Coordinate Reference Systems (CRS).
    """

    async def create_crs(self, conn: DbResource, catalog_id: str, crs_data: Any) -> Any: ...

    async def update_crs(self, conn: DbResource, catalog_id: str, crs_uri: str, crs_data: Any) -> Optional[Any]: ...

    async def delete_crs(self, conn: DbResource, catalog_id: str, crs_uri: str) -> bool: ...

    async def get_crs_by_uri(self, conn: DbResource, catalog_id: str, crs_uri: str) -> Optional[Any]: ...

    async def get_crs_by_name(self, conn: DbResource, catalog_id: str, crs_name: str) -> Optional[Any]: ...

    async def list_crs(self, conn: DbResource, catalog_id: str, limit: int = 20, offset: int = 0) -> List[Any]: ...

    async def search_crs(self, conn: DbResource, catalog_id: str, search_term: str, limit: int = 20, offset: int = 0) -> List[Any]: ...
