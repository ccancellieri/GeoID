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
import hashlib
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any, Protocol, BinaryIO, runtime_checkable
from dynastore.tools.cache import cached
from dynastore.models.driver_context import DriverContext
from dynastore.modules import (
    ModuleProtocol,
    get_protocol,
)
from dynastore.tools.discovery import register_plugin, unregister_plugin
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    ResultHandler,
    DbResource,
    managed_transaction,
)
from dynastore.modules.db_config.partition_tools import ensure_partition_exists
from dynastore.modules.db_config.locking_tools import check_table_exists
from dynastore.tools.protocol_helpers import get_engine
from dynastore.modules.db_config import maintenance_tools
from sqlalchemy import text
from dynastore.modules.tiles.tiles_models import (
    StoredTileMatrixSet,
    TileMatrixSetCreate,
    TileMatrixSet,
)
from dynastore.modules.tiles.tiles_config import (
    TilesConfig,
)
from dynastore.models.protocols import CatalogsProtocol, DatabaseProtocol
from dynastore.modules.catalog.catalog_module import (
    CatalogEventType,
    register_event_listener as register_catalog_event_listener,
)

logger = logging.getLogger(__name__)

# --- DDL Definitions (in Python) ---


TILE_MATRIX_SETS_DDL = """
CREATE TABLE IF NOT EXISTS tiles.tile_matrix_sets (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    catalog_id VARCHAR NOT NULL,
    tms_id VARCHAR NOT NULL,
    definition JSONB NOT NULL, -- The full OGC TileMatrixSet definition
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),    
    PRIMARY KEY (catalog_id, id),
    UNIQUE (catalog_id, tms_id) -- Ensures uniqueness of TMS within a catalog
) PARTITION BY LIST (catalog_id);
"""


TILE_MATRIX_SETS_COMMENT_DDL = "COMMENT ON TABLE tiles.tile_matrix_sets IS 'Stores custom, user-defined TileMatrixSet definitions, partitioned by catalog.';"
TILE_MATRIX_SETS_COMMENT_DDL = "COMMENT ON TABLE tiles.tile_matrix_sets IS 'Stores custom, user-defined TileMatrixSet definitions, partitioned by catalog.';"

# --- Module Implementation ---
class TilesModule(ModuleProtocol, DatabaseProtocol):
    # Must start after CatalogModule (priority=20) because boot-time tile
    # storage provider selection depends on CatalogsProtocol being registered.
    priority: int = 25
    """
    The foundational module for managing custom TileMatrixSets.
    It owns the `tiles.tile_matrix_sets` table and provides the core logic
    for creating, retrieving, and managing TMS definitions.
    """

    _engine: Optional[DbResource] = None
    app_state: object = None

    def __init__(self, app_state: object = None):
        self.app_state = app_state

        # Register MVT transformer
        from dynastore.tools.discovery import register_plugin
        from .query_transform import MVTQueryTransform

        register_plugin(MVTQueryTransform())

    @property
    def engine(self) -> Any:
        """DatabaseProtocol implementation."""
        if self._engine:
            return self._engine

        engine = getattr(self.app_state, "engine", None)
        if engine:
            return engine

        engine = getattr(self.app_state, "sync_engine", None)
        if engine:
            return engine

        return None

    @property
    def async_engine(self) -> Optional[Any]:
        """DatabaseProtocol implementation."""
        return getattr(self.app_state, "engine", None)

    @property
    def sync_engine(self) -> Optional[Any]:
        """DatabaseProtocol implementation."""
        return getattr(self.app_state, "sync_engine", None)

    def get_any_engine(self) -> Optional[Any]:
        """DatabaseProtocol implementation."""
        return get_engine()

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        """
        Initializes the tiles module. It creates the schema and the partitioned
        `tile_matrix_sets` table directly using DDL queries in Python.
        """
        engine = get_engine()

        if not engine:
            logger.critical("TilesModule cannot initialize: database engine not found.")
            yield
            return

        self._engine = engine
        logger.info("TilesModule: Initializing schema...")
        try:
            async with managed_transaction(engine) as conn:
                await maintenance_tools.ensure_schema_exists(conn, "tiles")
                await DDLQuery(TILE_MATRIX_SETS_DDL).execute(conn)
                await DDLQuery(TILE_MATRIX_SETS_COMMENT_DDL).execute(conn)

            # Register PG tile storage as fallback if no higher-priority provider (e.g. GCS) registered yet
            if get_protocol(TileStorageProtocol) is None:
                self._pg_tile_storage = TilePGPreseedStorage()
                register_plugin(self._pg_tile_storage)
                logger.info("TilesModule: Registered TilePGPreseedStorage as fallback.")

            # Register PG archive storage as fallback if no higher-priority provider registered yet
            if get_protocol(TileArchiveStorageProtocol) is None:
                self._pg_tile_archive = PGTileArchive()
                register_plugin(self._pg_tile_archive)
                logger.info("TilesModule: Registered PGTileArchive as fallback.")

            logger.info("TilesModule: Initialization complete.")
        except Exception as e:
            logger.error(
                f"CRITICAL: TilesModule initialization failed: {e}", exc_info=True
            )

        yield

        # --- SHUTDOWN ---
        for attr in ("_pg_tile_storage", "_pg_tile_archive"):
            obj = getattr(self, attr, None)
            if obj is not None:
                unregister_plugin(obj)
                setattr(self, attr, None)

        # --- REGISTER LISTENERS ---
        # Register in-process listeners to cleanup tile resources when a collection deletion event occurs.
        logger.info("TilesModule: Registering event listeners.")
        register_listeners()


# --- Tile Storage Protocols ---


@runtime_checkable
class TileStorageProtocol(Protocol):
    """Protocol for per-tile MVT cache providers."""

    async def save_tile(
        self,
        catalog_id: str,
        collection_id: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
        data: bytes,
        format: str,
    ) -> str:
        """Saves a tile and returns a URI or identifier."""
        ...

    async def get_tile(
        self,
        catalog_id: str,
        collection_id: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
        format: str,
    ) -> Optional[bytes]:
        """Retrieves a tile content or None if not found."""
        ...

    async def get_preseed_state(
        self, catalog_id: str, collection_id: str, tms_id: str
    ) -> Dict[str, Any]:
        """Returns the state of the preseed process for a given configuration."""
        ...

    async def get_tile_url(
        self,
        catalog_id: str,
        collection_id: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
        format: str,
    ) -> Optional[str]:
        """Returns a direct URL for the tile if available (e.g. for redirects)."""
        return None

    async def delete_tiles_for_collection(
        self, catalog_id: str, collection_id: str
    ) -> int:
        """Deletes all tiles for a given collection and returns the number of deleted records."""
        ...

    async def delete_storage_for_catalog(self, catalog_id: str):
        """Deletes the entire storage infrastructure for a catalog (e.g., a table)."""
        ...

    async def check_tile_exists(
        self,
        catalog_id: str,
        collection_id: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
        format: str,
    ) -> bool:
        """Checks if a tile exists in storage without downloading the payload."""
        ...


# Backwards-compat alias — callers that still use TileStorageSPI continue to work.
TileStorageSPI = TileStorageProtocol


class TilePGPreseedStorage(TileStorageProtocol):
    """Default Postgres-based tile storage, using catalog-specific schemas."""

    engine: DbResource
    catalogs: CatalogsProtocol

    def __init__(self):
        self.engine = _get_engine()
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise RuntimeError("CatalogsProtocol is not registered")
        self.catalogs = catalogs

    async def _get_schema(self, catalog_id: str) -> str:
        catalogs = get_protocol(CatalogsProtocol)
        phys_schema = (
            await catalogs.resolve_physical_schema(catalog_id) if catalogs else None
        )
        if phys_schema:
            return phys_schema
        return catalog_id  # Fallback if not physically resolved (might be system or legacy)

    async def _ensure_storage(self, conn, schema: str):
        """Ensures the catalog-specific preseed table exists."""
        # This function should be idempotent and fast.
        # We rely on 'CREATE TABLE IF NOT EXISTS'.
        # Using a proper creation function that handles schema existence.
        await ensure_preseed_storage_exists(conn, schema)

    async def save_tile(
        self,
        catalog_id: str,
        collection_id: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
        data: bytes,
        format: str,
    ) -> str:

        async with managed_transaction(self.engine) as conn:
            schema = await self._get_schema(catalog_id)
            await self._ensure_storage(conn, schema)

            # Dynamic table name: "{catalog_id}".preseeded_tiles
            query_str = f"""
                INSERT INTO "{schema}".preseeded_tiles (collection_id, tms_id, z, x, y, format, data) 
                VALUES (:collection_id, :tms_id, :z, :x, :y, :format, :data) 
                ON CONFLICT (collection_id, tms_id, z, x, y, format) 
                DO UPDATE SET data = EXCLUDED.data, created_at = NOW() 
                RETURNING created_at;
            """

            await DQLQuery(query_str, result_handler=ResultHandler.ROWCOUNT).execute(
                conn,
                collection_id=collection_id,
                tms_id=tms_id,
                z=z,
                x=x,
                y=y,
                format=format,
                data=data,
            )
        return f"pg://{catalog_id}/{collection_id}/{tms_id}/{z}/{x}/{y}.{format}"

    async def get_tile(
        self,
        catalog_id: str,
        collection_id: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
        format: str,
    ) -> Optional[bytes]:
        # We assume storage exists if we are reading. If table missing -> error or None?
        # Better to return None implies "not found". SQL error means "system error".
        # But for "table not found", it effectively means no tiles.

        schema = await self._get_schema(catalog_id)
        query_str = f"""
            SELECT data FROM "{schema}".preseeded_tiles 
            WHERE collection_id=:collection_id AND tms_id=:tms_id AND z=:z AND x=:x AND y=:y AND format=:format;
        """

        try:
            async with managed_transaction(self.engine) as conn:
                return await DQLQuery(
                    query_str, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
                ).execute(
                    conn,
                    collection_id=collection_id,
                    tms_id=tms_id,
                    z=z,
                    x=x,
                    y=y,
                    format=format,
                )
        except Exception as e:
            # If table doesn't exist, we can treat as miss
            # But we must be careful not to mask other errors.
            # Checking error code for "undefined table" (42P01) is robust.
            # For now, let's log and re-raise or return None?
            # Re-raising is safer for now to detect issues.
            # But if a catalog has no preseed table, it's just a miss.
            if "UndefinedTableError" in str(type(e).__name__) or "42P01" in str(e):
                return None
            raise e

    async def get_preseed_state(
        self, catalog_id: str, collection_id: str, tms_id: str
    ) -> Dict[str, Any]:
        """
        Returns stats about pre-seeded tiles.
        """
        schema = await self._get_schema(catalog_id)
        query_str = f"""
            SELECT count(*) as tile_count, sum(octet_length(data)) as total_size 
            FROM "{schema}".preseeded_tiles 
            WHERE collection_id=:collection_id AND tms_id=:tms_id;
        """
        try:
            async with managed_transaction(self.engine) as conn:
                result = await DQLQuery(
                    query_str, result_handler=ResultHandler.ONE_DICT
                ).execute(conn, collection_id=collection_id, tms_id=tms_id)
                return {
                    "tile_count": result.get("tile_count", 0),
                    "total_size_bytes": result.get("total_size", 0),
                }
        except Exception as e:
            if "UndefinedTableError" in str(type(e).__name__) or "42P01" in str(e):
                return {"tile_count": 0, "total_size_bytes": 0}
            raise e

    @cached(maxsize=4096, namespace="tiles_check_tile_exists")
    async def check_tile_exists(
        self,
        catalog_id: str,
        collection_id: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
        format: str,
    ) -> bool:
        """Checks for tile existence using a lightweight SELECT EXISTS query."""
        schema = await self._get_schema(catalog_id)
        query_str = f"""
            SELECT EXISTS (
                SELECT 1 FROM "{schema}".preseeded_tiles 
                WHERE collection_id=:collection_id AND tms_id=:tms_id AND z=:z AND x=:x AND y=:y AND format=:format
            );
        """
        try:
            async with managed_transaction(self.engine) as conn:
                return await DQLQuery(
                    query_str, result_handler=ResultHandler.SCALAR_ONE
                ).execute(
                    conn,
                    collection_id=collection_id,
                    tms_id=tms_id,
                    z=z,
                    x=x,
                    y=y,
                    format=format,
                )
        except Exception as e:
            if "UndefinedTableError" in str(type(e).__name__) or "42P01" in str(e):
                return False  # Table doesn't exist, so tile doesn't exist.
            raise e

    async def get_tile_url(
        self,
        catalog_id: str,
        collection_id: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
        format: str,
    ) -> Optional[str]:
        """Postgres storage does not provide direct URLs."""
        return None

    async def delete_tiles_for_collection(
        self, catalog_id: str, collection_id: str
    ) -> int:
        """Deletes all tiles associated with a collection from the PG storage."""
        schema = await self._get_schema(catalog_id)
        query_str = f"""
            DELETE FROM "{schema}".preseeded_tiles
            WHERE collection_id = :collection_id;
        """
        try:
            async with managed_transaction(self.engine) as conn:
                # We use ROWCOUNT to get the number of deleted rows.
                deleted_count = await DQLQuery(
                    query_str, result_handler=ResultHandler.ROWCOUNT
                ).execute(conn, collection_id=collection_id)
                return deleted_count or 0
        except Exception as e:
            if "UndefinedTableError" in str(type(e).__name__) or "42P01" in str(e):
                logger.info(
                    f"Preseed table for catalog '{catalog_id}' not found. No tiles to delete for collection '{collection_id}'."
                )
                return 0  # Table doesn't exist, so 0 tiles deleted.
            logger.error(
                f"Error deleting tiles for collection '{collection_id}' in catalog '{catalog_id}': {e}",
                exc_info=True,
            )
            raise e

    async def delete_storage_for_catalog(self, catalog_id: str):
        """Drops the catalog-specific preseeded_tiles table."""
        schema = await self._get_schema(catalog_id)
        query_str = f'DROP TABLE IF EXISTS "{schema}".preseeded_tiles;'
        try:
            async with managed_transaction(self.engine) as conn:
                await DDLQuery(query_str).execute(conn)
                logger.info(
                    f"Successfully dropped preseeded tiles table for catalog '{catalog_id}'."
                )
        except Exception as e:
            # Don't raise on error, just log it, as the catalog is being deleted anyway.
            logger.error(
                f"Error dropping preseeded tiles table for catalog '{catalog_id}': {e}",
                exc_info=True,
            )


# --- Internal Query Objects ---

_create_tms_query = DQLQuery(
    "INSERT INTO tiles.tile_matrix_sets (catalog_id, tms_id, definition) VALUES (:catalog_id, :tms_id, :definition) RETURNING id, catalog_id, definition;",
    result_handler=ResultHandler.ONE_DICT,
)

_get_tms_query = DQLQuery(
    "SELECT id, catalog_id, definition FROM tiles.tile_matrix_sets WHERE catalog_id = :catalog_id AND tms_id = :tms_id;",
    result_handler=ResultHandler.ONE_DICT,
)

_list_tms_query = DQLQuery(
    "SELECT definition FROM tiles.tile_matrix_sets WHERE catalog_id = :catalog_id ORDER BY tms_id LIMIT :limit OFFSET :offset;",
    result_handler=ResultHandler.ALL_DICTS,
)

_check_srid_query = DQLQuery(
    "SELECT srid FROM spatial_ref_sys WHERE auth_name = :auth_name AND auth_srid = :auth_srid;",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
)

_find_free_srid_query = DQLQuery(
    "SELECT max(srid) + 1 FROM spatial_ref_sys WHERE srid >= 100000;",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
)

_insert_srid_query = DQLQuery(
    "INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, srtext, proj4text) VALUES (:srid, :auth_name, :auth_srid, :srtext, :proj4text);",
    result_handler=ResultHandler.ROWCOUNT,
)


async def ensure_preseed_storage_exists(conn: DbResource, schema: str):
    """
    Ensures that the catalog-specific preseeded_tiles table exists.
    """
    # Create schema if not exists (should already exist for a catalog, but good for safety)
    from dynastore.modules.db_config import maintenance_tools

    await maintenance_tools.ensure_schema_exists(conn, schema)

    # Simplified table definition for catalog-specific storage
    # We remove 'schema' from columns as it is implicit in the schema.
    # We remove 'PARTITION BY' as each catalog has its own independent table.
    ddl = f"""
    CREATE TABLE IF NOT EXISTS "{schema}".preseeded_tiles (
        collection_id VARCHAR NOT NULL,
        tms_id VARCHAR NOT NULL,
        z INTEGER NOT NULL,
        x INTEGER NOT NULL,
        y INTEGER NOT NULL,
        format VARCHAR NOT NULL,
        data BYTEA NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (collection_id, tms_id, z, x, y, format)
    );
    """
    await DDLQuery(ddl).execute(conn)

    # Indexes for performance
    # Usually PK covers it, but maybe we want index on just TMS/Z/X/Y for ranges?
    # PK (collection_id, tms_id, z, x, y, format) is good for exact retrieval.
    # An index on (collection_id, tms_id) might help for deletions or stats.
    index_ddl = f"""
    CREATE INDEX IF NOT EXISTS idx_preseeded_tiles_tms ON "{schema}".preseeded_tiles (collection_id, tms_id);
    """
    await DDLQuery(index_ddl).execute(conn)


@lifecycle_registry.sync_catalog_initializer()
async def initialize_tiles_tenant_slice(conn: DbResource, schema: str, catalog_id: str):
    """Initializes the tiles module's slice of the tenant schema."""
    await ensure_preseed_storage_exists(conn, schema)


# --- Public Module API Functions ---


def _get_engine() -> DbResource:
    """Internal helper to safely get the module's engine."""
    db = get_protocol(DatabaseProtocol)
    if not db or not db.engine:
        raise RuntimeError("DatabaseProtocol (engine) is not available.")
    return db.engine


async def create_custom_tms(
    catalog_id: str, tms_data: TileMatrixSetCreate
) -> StoredTileMatrixSet:
    """Creates a new custom TileMatrixSet for a given catalog."""
    async with managed_transaction(_get_engine()) as db_conn:
        await ensure_partition_exists(
            db_conn,
            table_name="tile_matrix_sets",
            schema="tiles",
            strategy="LIST",
            partition_value=catalog_id,
        )
        result = await _create_tms_query.execute(
            db_conn,
            catalog_id=catalog_id,
            tms_id=tms_data.id,
            definition=tms_data.definition.model_dump_json(),
        )
        get_custom_tms.cache_invalidate(catalog_id, tms_data.id)
        list_custom_tms.cache_invalidate(
            catalog_id, 100, 0
        )  # Basic invalidation for default params
        return StoredTileMatrixSet.model_validate(result)


@cached(maxsize=128, namespace="tiles_get_custom_tms")
async def get_custom_tms(catalog_id: str, tms_id: str) -> Optional[TileMatrixSet]:
    """Retrieves a specific custom TileMatrixSet from a catalog."""
    result = await _get_tms_query.execute(
        _get_engine(), catalog_id=catalog_id, tms_id=tms_id
    )
    return TileMatrixSet.model_validate(result["definition"]) if result else None


@cached(maxsize=32, namespace="tiles_list_custom_tms")
async def list_custom_tms(
    catalog_id: str, limit: int = 100, offset: int = 0
) -> List[TileMatrixSet]:
    """Lists all custom TileMatrixSets for a given catalog."""
    results = await _list_tms_query.execute(
        _get_engine(), catalog_id=catalog_id, limit=limit, offset=offset
    )
    return [TileMatrixSet.model_validate(row["definition"]) for row in results]


@cached(maxsize=128, namespace="tiles_resolve_srid", ignore=["conn"])
async def resolve_srid(
    conn: DbResource, crs_str: str, catalog_id: Optional[str] = None
) -> int:
    """
    Resolves a CRS string (URI, EPSG, WKT) to a PostGIS-compatible SRID.
    Handles well-known URIs, standard EPSG codes, and custom CRS definitions from crs_module.
    """
    srid = await _resolve_srid_logic(conn, crs_str, catalog_id)
    return normalize_srid(srid)


def normalize_srid(srid: int) -> int:
    """
    Standardizes SRID by mapping common synonyms (like Google Web Mercator variants)
    to preferred PostGIS standard SRIDs.
    """
    # 3857 Synonyms
    if srid in (900911, 900912, 900913, 900914, 900916, 102100, 102113, 102133):
        return 3857
    # 4326 Synonyms
    if srid in (4040,):
        return 4326
    return srid


async def _resolve_srid_logic(
    conn: DbResource, crs_str: str, catalog_id: Optional[str] = None
) -> int:
    # (Existing resolve_srid logic moved here, renamed to avoid recursion)
    # 1. Fast mapping for well-known OGC URIs and SRID synonyms
    crs_str_clean = str(crs_str).strip()
    well_known = {
        "http://www.opengis.net/def/crs/OGC/1.3/CRS84": 4326,
        "urn:ogc:def:crs:OGC:1.3:CRS84": 4326,
        "http://www.opengis.net/def/crs/EPSG/0/4326": 4326,
        "http://www.opengis.net/def/crs/EPSG/0/3857": 3857,
        "urn:ogc:def:crs:EPSG::3857": 3857,
        "urn:ogc:def:crs:EPSG:3857": 3857,
        "EPSG:3857": 3857,
        "900913": 3857,  # Google Web Mercator (Legacy)
        "900916": 3857,  # Google Web Mercator (Synonym)
        "102100": 3857,  # ESRI Web Mercator
        "102113": 3857,
        "102133": 3857,
        "3857": 3857,
        "4326": 4326,
    }
    if crs_str_clean in well_known:
        return well_known[crs_str_clean]

    # 2. Try to resolve via crs_module if it's a URI and catalog_id is provided
    if catalog_id and (
        crs_str.startswith("http") or "://" in crs_str or crs_str.startswith("urn:")
    ):
        try:
            from dynastore.models.protocols.crs import CRSProtocol
            from dynastore.tools.discovery import get_protocol

            crs_svc = get_protocol(CRSProtocol)
            if crs_svc is not None:
                custom_crs = await crs_svc.get_crs_by_uri(conn, catalog_id, crs_str)
                if custom_crs:
                    crs_str = custom_crs.definition.definition
        except Exception as e:
            logger.debug(
                f"Could not lookup CRS '{crs_str}' for catalog '{catalog_id}': {e}"
            )

    try:
        from pyproj import CRS as PyprojCRS

        # from_user_input is more robust than from_string
        crs_obj = PyprojCRS.from_user_input(crs_str)

        # 3. Standard EPSG resolution
        epsg = crs_obj.to_epsg()
        if epsg:
            # Map common synonyms to standard PostGIS SRIDs
            if epsg in (900911, 900913, 900914, 900916, 102100, 102113, 102133):
                return 3857
            return epsg

        # 4. Fallback to authority check
        auth = crs_obj.to_authority()
        if auth and auth[0].upper() == "EPSG":
            try:
                auth_srid = int(auth[1])
                if auth_srid in (
                    900911,
                    900913,
                    900914,
                    900916,
                    102100,
                    102113,
                    102133,
                ):
                    return 3857
                return auth_srid
            except (ValueError, TypeError):
                pass

        # 5. Custom CRS Case: Register in PostGIS spatial_ref_sys
        wkt = crs_obj.to_wkt(version="WKT2_2019")
        proj4 = crs_obj.to_proj4()
        return await ensure_custom_crs_in_postgis(conn, wkt, proj4)

    except Exception as e:
        logger.debug(
            f"pyproj could not parse CRS '{crs_str}': {e}. Falling back to custom PostGIS registration."
        )
        try:
            return int(crs_str)
        except (ValueError, TypeError):
            # If not an int and pyproj failed, treat as custom WKT for PostGIS registration
            return await ensure_custom_crs_in_postgis(conn, crs_str)


async def ensure_custom_crs_in_postgis(
    conn: DbResource, wkt: str, proj4: str = ""
) -> int:
    """
    Ensures a CRS defined by WKT exists in spatial_ref_sys and returns its SRID.
    This function is critical for using custom projections with ST_Transform.
    """
    auth_name = "dynastore"
    # Ensure wkt is a string (could be a CRS object if passed incorrectly)
    wkt_str = str(wkt)
    wkt_hash = hashlib.sha256(wkt_str.encode("utf-8")).hexdigest()
    auth_srid = int(wkt_hash[:8], 16)
    # Ensure auth_srid fits in a signed 32-bit integer range (-2^31 to 2^31-1)
    # PostGIS spatial_ref_sys.auth_srid is an integer (signed 32-bit).
    if auth_srid > 2147483647:
        auth_srid -= 4294967296

    # Use the provided connection
    existing_srid = await _check_srid_query.execute(
        conn, auth_name=auth_name, auth_srid=auth_srid
    )
    if existing_srid:
        return existing_srid

    next_srid = await _find_free_srid_query.execute(conn)
    if not next_srid or next_srid < 100000:
        next_srid = 100000

    logger.info(
        f"Registering new custom CRS with SRID {next_srid} (auth_srid: {auth_srid})."
    )

    await _insert_srid_query.execute(
        conn,
        srid=next_srid,
        auth_name=auth_name,
        auth_srid=auth_srid,
        srtext=wkt,
        proj4text=proj4,
    )
    return next_srid


@cached(maxsize=1024, namespace="tiles_collection_source_srid")
async def get_collection_source_srid(
    catalog_id: str, collection_id: str
) -> Optional[int]:
    """
    Retrieves the source SRID for a collection's 'geom' column.
    1. Checks collection configuration/metadata for custom CRS URIs.
    2. Falls back to PostGIS Find_SRID lookup.
    """
    engine = _get_engine()
    async with managed_transaction(engine) as conn:
        # Check logical configuration first for CRS hints
        # We can look for 'source_crs' in TilesConfig at collection level
        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            return 4326
        config_service = catalogs.configs
        tiles_config = await config_service.get_config(
            TilesConfig, catalog_id, collection_id, ctx=DriverContext(db_resource=conn
        ))

        source_crs_uri = getattr(tiles_config, "source_crs", None)
        if source_crs_uri:
            try:
                # Use module's resolve_srid which integrates with crs_module
                return await resolve_srid(conn, source_crs_uri, catalog_id)
            except Exception as e:
                logger.debug(
                    f"Failed to resolve custom CRS URI '{source_crs_uri}' for {collection_id}: {e}"
                )

        # Fallback to physical lookup via driver
        from dynastore.modules.storage.router import get_driver
        from dynastore.modules.storage.routing_config import Operation

        try:
            driver = await get_driver(Operation.READ, catalog_id, collection_id)
            location = await driver.location(catalog_id, collection_id)
        except (ValueError, Exception):
            return 4326

        phys_schema = location.identifiers.get("schema")
        phys_table = location.identifiers.get("table")

        if not phys_schema or not phys_table:
            return None

        srid_query = text("""
            SELECT srid FROM geometry_columns
            WHERE f_table_schema = :schema
            AND f_table_name IN (:table, :sidecar)
            AND f_geometry_column = 'geom'
            LIMIT 1;
        """)
        sidecar_table = f"{phys_table}_geometries"
        srid = await DQLQuery(
            srid_query, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
        ).execute(conn, schema=phys_schema, table=phys_table, sidecar=sidecar_table)
        if not srid:
            return 4326
        return srid


@cached(maxsize=1024, ttl=120, jitter=15, namespace="tiles_resolution_params")
async def get_tile_resolution_params(
    catalog_id: str, collection_id: str
) -> Dict[str, Any]:
    """
    Consolidated cached metadata for tile generation.
    Returns: physical names, source_srid, and simplification rules.
    """
    engine = _get_engine()
    async with managed_transaction(engine) as conn:
        # 1. Resolve Physical Names (Cached inside catalogs protocol)
        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            return {}
        from dynastore.modules.storage.router import get_driver
        from dynastore.modules.storage.routing_config import Operation

        try:
            driver = await get_driver(Operation.READ, catalog_id, collection_id)
            location = await driver.location(catalog_id, collection_id)
        except (ValueError, Exception):
            return {}

        phys_schema = location.identifiers.get("schema")
        phys_table = location.identifiers.get("table")

        if not phys_schema or not phys_table:
            return {}

        # 2. Resolve Source SRID (Cached)
        source_srid = await get_collection_source_srid(catalog_id, collection_id)

        # 3. Resolve Simplification Configuration (Waterfall handled by ConfigManager)
        config_service = catalogs.configs
        tiles_config = await config_service.get_config(
            TilesConfig, catalog_id, ctx=DriverContext(db_resource=conn
        ))
        if not tiles_config:
            # If not present, we can initialize it with defaults
            tiles_config = TilesConfig()
            await config_service.set_config(
                TilesConfig, tiles_config, catalog_id=catalog_id, ctx=DriverContext(db_resource=conn
            ))

        # Extract relevant fields
        simplification_by_zoom = {}
        if isinstance(tiles_config, TilesConfig):
            simplification_by_zoom = tiles_config.simplification_by_zoom or {}

        # 4. Resolve Collection Config (for sidecar-aware queries)
        col_config = await catalogs.get_collection_config(
            catalog_id, collection_id, ctx=DriverContext(db_resource=conn)
        )

        return {
            "phys_schema": phys_schema,
            "phys_table": phys_table,
            "source_srid": source_srid,
            "simplification_by_zoom": simplification_by_zoom,
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "col_config": col_config,
        }


async def invalidate_collection_tiles(catalog_id: str, collection_id: str):
    """
    Public API function to invalidate all tiles for a specific collection across all storage providers.
    Also clears internal metadata caches.
    """
    logger.info(
        f"TilesModule: Invalidating tiles and metadata cache for '{catalog_id}:{collection_id}'."
    )

    # 1. Clear internal metadata caches
    get_collection_source_srid.cache_clear()
    get_tile_resolution_params.cache_clear()

    # 2. Clear physical provider
    provider = get_protocol(TileStorageProtocol)
    if provider is None:
        logger.warning("No TileStorageProtocol registered — skipping physical invalidation.")
        return
    try:
        result = await provider.delete_tiles_for_collection(catalog_id, collection_id)
        logger.info(
            f"TileStorageProtocol: invalidated {result} tile records for collection '{collection_id}'."
        )
    except Exception as exc:
        logger.error(f"Error during tile invalidation: {exc}")


async def invalidate_catalog_tiles(catalog_id: str):
    """
    Public API function to invalidate all tiles for an entire catalog across all storage providers.
    Also clears internal metadata caches.
    """
    logger.info(
        f"TilesModule: Invalidating all tile storage and metadata cache for catalog '{catalog_id}'."
    )

    # 1. Clear internal metadata caches
    get_collection_source_srid.cache_clear()
    get_tile_resolution_params.cache_clear()

    # 2. Clear physical provider
    provider = get_protocol(TileStorageProtocol)
    if provider is not None:
        try:
            await provider.delete_storage_for_catalog(catalog_id)
        except Exception as exc:
            logger.error(f"Error during catalog tile invalidation: {exc}")


# --- Event Handlers & Listeners ---


async def on_collection_hard_deletion(catalog_id: str, collection_id: str, **kwargs):
    """
    Handler to cleanup tile caches when a collection is hard-deleted.
    This function is triggered by an internal catalog event.
    """
    logger.info(
        f"TilesModule: Event 'collection_hard_deletion' received for '{catalog_id}:{collection_id}'. Purging associated tiles."
    )
    await invalidate_collection_tiles(catalog_id, collection_id)


async def on_catalog_hard_deletion(catalog_id: str, **kwargs):
    """
    Handler to cleanup tile infrastructure when a catalog is hard-deleted.
    This function is triggered by an internal catalog event.
    """
    logger.info(
        f"TilesModule: Event 'catalog_hard_deletion' received for '{catalog_id}'. Purging associated tile storage."
    )
    await invalidate_catalog_tiles(catalog_id)


def register_listeners():
    """Subscribes the Tiles module to internal catalog events."""
    register_catalog_event_listener(
        CatalogEventType.COLLECTION_HARD_DELETION, on_collection_hard_deletion
    )
    register_catalog_event_listener(
        CatalogEventType.CATALOG_HARD_DELETION, on_catalog_hard_deletion
    )


# ---------------------------------------------------------------------------
# TileArchiveStorageProtocol — PMTiles archive-level storage
# ---------------------------------------------------------------------------



@runtime_checkable
class TileArchiveStorageProtocol(Protocol):
    """Protocol for PMTiles archive-level tile storage.

    Separate from TileStorageProtocol (per-tile MVT cache) — different granularity.
    Implementations register via register_plugin(); callers use
    get_protocol(TileArchiveStorageProtocol).
    """

    async def save_archive(
        self,
        catalog_id: str,
        collection_id: str,
        tms_id: str,
        data_file: BinaryIO,
    ) -> str:
        """Save a PMTiles archive. Returns storage URI."""
        ...

    async def archive_exists(
        self, catalog_id: str, collection_id: str, tms_id: str
    ) -> bool:
        """Return True when an archive exists for this (catalog, collection, tms)."""
        ...

    async def get_tile_from_archive(
        self,
        catalog_id: str,
        collection_id: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
    ) -> Optional[bytes]:
        """Extract a single MVT tile from the archive. None if tile absent."""
        ...

    async def delete_archive(
        self, catalog_id: str, collection_id: str, tms_id: str
    ) -> bool:
        """Delete the archive. Returns True on success."""
        ...


# DDL for the PG fallback pmtiles_archives table (per-catalog schema)
_PMTILES_ARCHIVE_DDL = """
CREATE TABLE IF NOT EXISTS "{schema}".pmtiles_archives (
    collection_id VARCHAR NOT NULL,
    tms_id        VARCHAR NOT NULL,
    data          BYTEA   NOT NULL,
    n_tiles       INTEGER,
    min_zoom      INTEGER,
    max_zoom      INTEGER,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (collection_id, tms_id)
);
"""


async def ensure_pmtiles_archive_storage_exists(conn: DbResource, schema: str) -> None:
    """Idempotent DDL guard: creates pmtiles_archives if it does not yet exist."""
    if await check_table_exists(conn, "pmtiles_archives", schema=schema):
        return
    await DDLQuery(_PMTILES_ARCHIVE_DDL.format(schema=schema)).execute(conn)


class PGTileArchive(TileArchiveStorageProtocol):
    """PG BYTEA archive store. On-premise fallback only.

    ⚠ get_tile_from_archive downloads the full archive per call —
    suitable only for small datasets. Production deployments should prefer
    StorageBackedTileArchive (GCS / S3) which uses range-reads.
    """

    def __init__(self):
        self.engine: DbResource = _get_engine()
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise RuntimeError("CatalogsProtocol is not registered")
        self.catalogs = catalogs

    async def _get_schema(self, catalog_id: str) -> str:
        catalogs = get_protocol(CatalogsProtocol)
        phys_schema = (
            await catalogs.resolve_physical_schema(catalog_id) if catalogs else None
        )
        return phys_schema or catalog_id

    async def save_archive(
        self,
        catalog_id: str,
        collection_id: str,
        tms_id: str,
        data_file: BinaryIO,
    ) -> str:
        data = data_file.read()
        schema = await self._get_schema(catalog_id)
        sql = f"""
            INSERT INTO "{schema}".pmtiles_archives
                (collection_id, tms_id, data)
            VALUES (:collection_id, :tms_id, :data)
            ON CONFLICT (collection_id, tms_id)
            DO UPDATE SET data = EXCLUDED.data, created_at = NOW();
        """
        async with managed_transaction(self.engine) as conn:
            await ensure_pmtiles_archive_storage_exists(conn, schema)
            await DDLQuery(sql).execute(
                conn, collection_id=collection_id, tms_id=tms_id, data=data
            )
        return f"pg://{catalog_id}/{collection_id}/{tms_id}.pmtiles"

    @cached(maxsize=512, namespace="pg_pmtiles_archive_exists")
    async def archive_exists(
        self, catalog_id: str, collection_id: str, tms_id: str
    ) -> bool:
        schema = await self._get_schema(catalog_id)
        sql = f"""
            SELECT EXISTS (
                SELECT 1 FROM "{schema}".pmtiles_archives
                WHERE collection_id=:collection_id AND tms_id=:tms_id
            );
        """
        try:
            async with managed_transaction(self.engine) as conn:
                return await DQLQuery(sql, result_handler=ResultHandler.SCALAR).execute(
                    conn, collection_id=collection_id, tms_id=tms_id
                ) or False
        except Exception as exc:
            if "42P01" in str(exc):
                return False
            raise

    async def get_tile_from_archive(
        self,
        catalog_id: str,
        collection_id: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
    ) -> Optional[bytes]:
        schema = await self._get_schema(catalog_id)
        sql = f"""
            SELECT data FROM "{schema}".pmtiles_archives
            WHERE collection_id=:collection_id AND tms_id=:tms_id;
        """
        try:
            async with managed_transaction(self.engine) as conn:
                data: Optional[bytes] = await DQLQuery(
                    sql, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
                ).execute(conn, collection_id=collection_id, tms_id=tms_id)
        except Exception as exc:
            if "42P01" in str(exc):
                return None
            raise

        if data is None:
            return None

        try:
            from pmtiles.reader import Reader, MemorySource

            reader = Reader(MemorySource(data))
            return reader.get(z, x, y)
        except Exception:
            logger.warning(
                f"PGTileArchive: failed to extract tile z={z} x={x} y={y} "
                f"from archive for {collection_id}/{tms_id}"
            )
            return None

    async def delete_archive(
        self, catalog_id: str, collection_id: str, tms_id: str
    ) -> bool:
        schema = await self._get_schema(catalog_id)
        sql = f"""
            DELETE FROM "{schema}".pmtiles_archives
            WHERE collection_id=:collection_id AND tms_id=:tms_id;
        """
        try:
            async with managed_transaction(self.engine) as conn:
                await DDLQuery(sql).execute(
                    conn, collection_id=collection_id, tms_id=tms_id
                )
            return True
        except Exception as exc:
            if "42P01" in str(exc):
                return False
            raise
