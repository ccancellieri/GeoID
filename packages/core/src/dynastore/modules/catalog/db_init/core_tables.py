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

"""
Core PostgreSQL metadata DDL — catalog + tenant ``*_collection_core`` tables.

Backs the ``CatalogStore`` / ``CollectionStore`` core
drivers (``modules.storage.drivers.core_postgresql``).

Global (under ``catalog.`` schema):

- ``catalog.catalog_core`` — CORE fields (title, description,
  keywords, license, extra_metadata) keyed on ``catalog_id``.

Per-tenant (under each ``{schema}``):

- ``{schema}.collection_core`` — CORE collection metadata
  (title, description, keywords, license, extra_metadata).

The STAC sidecar tables (``*.catalog_stac`` /
``*.collection_stac``) live in
``modules.stac.db_init.stac_tables`` and run only when the STAC
module is installed.
"""

import logging

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DbResource,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Global (catalog.* schema)
# ---------------------------------------------------------------------------

CATALOG_METADATA_CORE_DDL = """
CREATE TABLE IF NOT EXISTS catalog.catalog_core (
    catalog_id     VARCHAR PRIMARY KEY REFERENCES catalog.catalogs(id) ON DELETE CASCADE,
    title          JSONB,
    description    JSONB,
    keywords       JSONB,
    license        JSONB,
    extra_metadata JSONB,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

# Freshness columns (``created_at`` / ``updated_at``) for the canonical
# INDEX / BACKUP propagation contract live directly on the
# ``catalog.catalogs`` CREATE TABLE in
# :mod:`dynastore.modules.catalog.catalog_module`.  Pre-1.0, schema is
# delete-and-rebuild; no migration shim needed.


# ---------------------------------------------------------------------------
# Per-tenant ({schema}.* schema)
# ---------------------------------------------------------------------------

TENANT_METADATA_CORE_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.collection_core (
    collection_id  VARCHAR PRIMARY KEY,
    title          JSONB,
    description    JSONB,
    keywords       JSONB,
    license        JSONB,
    extra_metadata JSONB,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


async def ensure_global_core_tables(conn: DbResource) -> None:
    """Apply the global CORE DDL (``catalog.catalog_core``).

    The STAC sidecar table (``catalog.catalog_stac``) is owned
    by the STAC module — when installed, ``StacModule.lifespan`` applies
    its DDL via ``ensure_global_stac_tables``.

    Idempotent.  Called once during ``CatalogModule`` init after the
    ``catalog.catalogs`` table has been created.
    """
    await DDLQuery(CATALOG_METADATA_CORE_DDL).execute(conn)


async def ensure_tenant_core_tables(conn: DbResource, schema: str) -> None:
    """Apply the per-tenant CORE DDL (``{schema}.collection_core``).

    The per-tenant STAC sidecar (``{schema}.collection_stac``) is
    owned by the STAC module — when installed, the lifecycle-registered
    ``_ensure_tenant_stac_tables`` initializer in
    ``modules.stac.db_init.stac_tables`` runs alongside this.

    Idempotent.  Called from ``create_catalog`` as the canonical
    collection-metadata storage provision step.  FK to
    ``{schema}.collections(id)`` is intentionally omitted because the
    collection row is inserted after ``create_catalog`` completes;
    enforcing the FK would require reordering or deferring constraints.
    A later cleanup pass may add deferred FKs once every tenant init has
    completed.
    """
    await DDLQuery(TENANT_METADATA_CORE_DDL).execute(conn, schema=schema)
