# src/dynastore/extensions/notebooks/tenant_initialization.py
"""Lifecycle hook: creates/migrates the notebooks table in each tenant schema."""
import logging
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.modules.db_config.query_executor import DbResource
from dynastore.modules.notebooks.notebooks_db import init_notebooks_storage

logger = logging.getLogger(__name__)


@lifecycle_registry.sync_catalog_initializer  # type: ignore[arg-type]
async def _initialize_notebooks_tenant_slice(conn: DbResource, schema: str, catalog_id: str):
    """Create/migrate the notebooks table when a catalog is provisioned."""
    logger.info(f"Initializing notebooks tenant slice for {schema} (Catalog: {catalog_id})")
    await init_notebooks_storage(conn, schema, catalog_id)
