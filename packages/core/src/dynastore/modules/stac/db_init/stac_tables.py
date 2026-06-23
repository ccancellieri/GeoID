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

"""STAC-slice metadata DDL.

When the STAC module is installed, the per-tenant lifecycle initializer
``_ensure_tenant_stac_tables`` (decorated with
``@lifecycle_registry.sync_catalog_initializer``) runs at every
``CatalogService.create_catalog`` call — alongside the core-tables
initializer in ``modules.catalog.db_init.core_tables``. The
global STAC table is applied once during ``StacModule.lifespan``.

Without the STAC module loaded, neither the global nor the per-tenant
STAC sidecar table is created — a STAC field write hits the loud
"StacCollectionEntityStoreCapability not registered" error from
``stac_service._has_stac()``.
"""

from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DbResource,
)


CATALOG_METADATA_STAC_DDL = """
CREATE TABLE IF NOT EXISTS catalog.catalog_stac (
    catalog_id      VARCHAR PRIMARY KEY REFERENCES catalog.catalogs(id) ON DELETE CASCADE ON UPDATE CASCADE,
    stac_version    VARCHAR(20),
    stac_extensions JSONB,
    conforms_to     JSONB,
    links           JSONB,
    assets          JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


TENANT_METADATA_STAC_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.collection_stac (
    -- Keyed on the immutable collection physical id (the c_… token from
    -- {schema}.collections.physical_id).  A collection rename only changes
    -- collections.id; these metadata rows are never touched by a rename.
    collection_physical_id VARCHAR PRIMARY KEY,
    stac_version    VARCHAR(20),
    stac_extensions JSONB,
    extent          JSONB,
    providers       JSONB,
    summaries       JSONB,
    links           JSONB,
    assets          JSONB,
    item_assets     JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


async def ensure_global_stac_tables(conn: DbResource) -> None:
    """Apply the global STAC DDL (``catalog.catalog_stac``).

    Idempotent. ``StacModule.lifespan`` calls this once at app startup
    after ``CatalogModule`` has created ``catalog.catalogs`` (the FK
    target). Module priority ordering enforces the sequencing.
    """
    await DDLQuery(CATALOG_METADATA_STAC_DDL).execute(conn)


@lifecycle_registry.sync_catalog_initializer(priority=2)
async def _ensure_tenant_stac_tables(
    conn: DbResource, schema: str, catalog_id: str
) -> None:
    """Per-tenant lifecycle hook: create ``{schema}.collection_stac``.

    Priority 2 keeps this very early — before any module-specific
    lifecycle hooks (priority 5+) that might write STAC metadata
    immediately. Only runs when the STAC module is loaded (the
    decorator only fires on import).
    """
    await DDLQuery(TENANT_METADATA_STAC_DDL).execute(conn, schema=schema)
