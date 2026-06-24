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

# src/dynastore/extensions/proxy/tenant_initialization.py

import logging
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.modules.db_config.query_executor import DDLQuery, DbResource

logger = logging.getLogger(__name__)


@lifecycle_registry.sync_catalog_initializer  # type: ignore[arg-type]
async def _initialize_proxy_tenant_slice(conn: DbResource, schema: str, catalog_id: str):
    """Initializes the proxy extension's per-tenant collection_proxy_urls table.

    Columns mirror the ShortURL model: id (from global proxy.short_url_id_seq),
    short_key (PRIMARY KEY), long_url, collection_id, created_at, comment.
    Key generation still uses the shared proxy-schema sequence and functions
    (proxy.base62 / proxy.obfuscate_id) so no per-tenant DDL is needed for those.
    """
    logger.info(
        "Initializing proxy tenant slice for %s (Catalog: %s)", schema, catalog_id
    )

    table_ddl = f"""
    CREATE TABLE IF NOT EXISTS \"{schema}\".collection_proxy_urls (
        id         BIGINT       NOT NULL,
        short_key  VARCHAR(20)  NOT NULL PRIMARY KEY,
        long_url   TEXT         NOT NULL,
        collection_id VARCHAR(255) NOT NULL DEFAULT '_catalog_',
        created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        comment    TEXT
    );
    """
    index_ddl = (
        f'CREATE INDEX IF NOT EXISTS idx_collection_proxy_urls_coll '
        f'ON \"{schema}\".collection_proxy_urls (collection_id);'
    )

    await DDLQuery(table_ddl).execute(conn)
    await DDLQuery(index_ddl).execute(conn)
    logger.info(
        "Proxy tenant slice initialization complete for schema '%s'", schema
    )
