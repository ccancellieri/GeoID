# src/dynastore/extensions/proxy/tenant_initialization.py

import logging
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.modules.db_config.query_executor import DDLQuery, DbResource

logger = logging.getLogger(__name__)

PROXY_TENANT_TABLES_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.collection_proxy_urls (
    collection_id VARCHAR NOT NULL,
    short_key VARCHAR(20) NOT NULL PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
-- Index for listing all URLs of a collection
CREATE INDEX IF NOT EXISTS idx_collection_proxy_urls_code ON {schema}.collection_proxy_urls (collection_id);
"""

@lifecycle_registry.sync_catalog_initializer  # type: ignore[arg-type]
async def _initialize_proxy_tenant_slice(conn: DbResource, schema: str, catalog_id: str):
    """Initializes the proxy extension's slice of the tenant schema."""
    logger.info(f"Initializing proxy tenant slice for {schema} (Catalog: {catalog_id})")
    
    # Quoting schema name is CRITICAL for case-sensitive schema names
    table_ddl = f"""
    CREATE TABLE IF NOT EXISTS "{schema}".collection_proxy_urls (
        collection_id VARCHAR NOT NULL,
        short_key VARCHAR(20) NOT NULL PRIMARY KEY,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    """
    index_ddl = f'CREATE INDEX IF NOT EXISTS idx_collection_proxy_urls_code ON "{schema}".collection_proxy_urls (collection_id);'

    await DDLQuery(table_ddl).execute(conn)
    await DDLQuery(index_ddl).execute(conn)
