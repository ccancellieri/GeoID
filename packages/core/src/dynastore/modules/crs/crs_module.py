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

# src/dynastore/modules/csr/crs_module.py

import logging
from typing import Optional, List, AsyncGenerator
from contextlib import asynccontextmanager

from dynastore.modules import ModuleProtocol, get_protocol
from dynastore.models.protocols import DatabaseProtocol, CRSProtocol
from dynastore.modules.db_config.query_executor import managed_transaction, DDLQuery
from dynastore.modules.db_config.query_executor import DbResource
from dynastore.modules.db_config import maintenance_tools
from dynastore.modules.db_config.partition_tools import ensure_partition_exists
from .models import CRS, CRSCreate
from . import queries

logger = logging.getLogger(__name__)
class CRSModule(ModuleProtocol, CRSProtocol):
    priority: int = 100
    """
    The foundational module for managing Coordinate Reference System definitions.
    It owns the `crs_definitions` table and all related business logic.
    """

    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncGenerator[None, None]:
        """
        Manages the database schema for the CRS module at application startup.
        """
        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None
        
        if not engine:
            logger.critical("CRSModule cannot initialize: database engine not found.")
            yield; return

        logger.info("CRSModule: Initializing crs_definitions table.")
        try:
            async with managed_transaction(engine) as conn:
                async with maintenance_tools.acquire_startup_lock(conn, "crs_module"):
                    await maintenance_tools.ensure_schema_exists(conn, "crs")
                    await queries.CREATE_CUSTOM_CRS_TABLE.execute(conn)
                    # Partition creation handled on demand via ensure_partition_exists
        except Exception as e:
            logger.error(f"CRITICAL: CRSModule initialization failed: {e}", exc_info=True)
        logger.info("CRSModule: Initialization complete.")
        yield

    async def create_crs(self, conn: DbResource, catalog_id: str, crs_data: CRSCreate) -> CRS:
        """
        Inserts a new CRS definition into the database.
        The input `crs_data` has already passed Pydantic validation (WKT2 compliance).
        """
        async with managed_transaction(conn) as transaction_conn:
            # Ensure partition exists for the catalog
            await ensure_partition_exists(
                transaction_conn, table_name="crs_definitions", schema="crs",
                strategy="LIST", partition_value=catalog_id
            )
            
            # Insert using the validated definition
            # Note: model_dump_json() handles the serialization of the nested CRSDefinition
            result = await queries.insert_custom_crs_query.execute(
                transaction_conn, 
                catalog_id=catalog_id, 
                crs_uri=crs_data.crs_uri,
                definition=crs_data.definition.model_dump_json()
            )
        return CRS(**result)

    async def update_crs(self, conn: DbResource, catalog_id: str, crs_uri: str, crs_data: CRSCreate) -> Optional[CRS]:
        """
        Updates an existing CRS definition.
        """
        async with managed_transaction(conn) as transaction_conn:
            result = await queries.update_custom_crs_query.execute(
                transaction_conn, 
                catalog_id=catalog_id, 
                crs_uri=crs_uri,
                definition=crs_data.definition.model_dump_json()
            )
        if not result:
            return None
        return await self.get_crs_by_uri(conn, catalog_id, crs_uri)

    async def get_crs_by_uri(self, conn: DbResource, catalog_id: str, crs_uri: str) -> Optional[CRS]:
        """Fetches a single CRS definition by its URI."""
        result = await queries.get_custom_crs_by_uri_query.execute(
            conn, catalog_id=catalog_id, crs_uri=crs_uri
        )
        return CRS(**result) if result else None

    async def get_crs_by_name(self, conn: DbResource, catalog_id: str, crs_name: str) -> Optional[CRS]:
        """Fetches a single CRS definition by its name."""
        result = await queries.get_custom_crs_by_name_query.execute(
            conn, catalog_id=catalog_id, crs_name=crs_name
        )
        return CRS(**result) if result else None

    async def list_crs(self, conn: DbResource, catalog_id: str, limit: int = 20, offset: int = 0) -> List[CRS]:
        """Lists all CRS definitions for a specific catalog."""
        results = await queries.list_custom_crs_query.execute(
            conn, catalog_id=catalog_id, limit=limit, offset=offset
        )
        return [CRS(**row) for row in results]

    async def search_crs(self, conn: DbResource, catalog_id: str, search_term: str, limit: int = 20, offset: int = 0) -> List[CRS]:
        """
        Searches for CRS definitions.
        """
        wildcard_search = f"%{search_term}%"
        results = await queries.search_custom_crs_query.execute(
            conn, catalog_id=catalog_id, search_term=wildcard_search, limit=limit, offset=offset
        )
        return [CRS(**row) for row in results]

    async def delete_crs(self, conn: DbResource, catalog_id: str, crs_uri: str) -> bool:
        """Deletes a CRS definition."""
        rows_affected = await queries.delete_custom_crs_query.execute(
            conn, catalog_id=catalog_id, crs_uri=crs_uri
        )
        return rows_affected > 0