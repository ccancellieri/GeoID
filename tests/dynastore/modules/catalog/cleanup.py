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

from tests.dynastore.test_utils.cleanup_registry import CleanupRegistry
from tests.dynastore.test_utils.db_cleanup import (
    CATALOG_METADATA_TABLES,
    SCHEMA_DROP_BATCH_SIZE,
    TENANT_SCHEMA_DISCOVERY_SQL,
    TENANT_SCHEMA_PATTERN,
    drop_schemas_batch_sql,
)
from dynastore.modules.db_config.query_executor import DDLQuery, DQLQuery, ResultHandler, managed_nested_transaction
from dynastore.modules.db_config.locking_tools import check_table_exists, force_truncate_table, force_drop_schema
import logging

logger = logging.getLogger(__name__)

@CleanupRegistry.register
async def cleanup_catalog(engine):
    """Truncate the catalog schema and drop tenant schemas on the worker's DB.

    Safe under pytest-xdist: each worker has its own cloned database
    (``gis_dev_<worker_id>``, see ``tests/conftest.py::_ensure_worker_db``
    commit ``e3d5458``), so dropping ``s_*`` tenant schemas here cannot
    affect another worker's state. The earlier ``PYTEST_XDIST_WORKER``
    early-return was a stale guard from before per-worker DB cloning
    landed.
    """
    async with managed_nested_transaction(engine) as conn:
        logger.info("Cleaning up catalog schema and dropping tenant schemas...")

    try:
        # Target schemas matching the generated physical name format: s_ + 8 base36 chars
        # (see generate_physical_name in catalog_service.py).
        # The pattern + discovery SQL are the SSOT in dynastore.tools.db_cleanup
        # so this routine and dynastore.scripts.db_reset stay aligned.
        # NOTE: append the trailing `;` here — TemplateQueryBuilder treats `{8}`
        # as an identifier placeholder and silently strips it, so the pattern
        # MUST be passed as a bind parameter (which TENANT_SCHEMA_PATTERN is
        # designed for).
        query = DQLQuery(
            f"{TENANT_SCHEMA_DISCOVERY_SQL};",
            result_handler=ResultHandler.ALL_SCALARS
        )
        # Use a fresh connection to get the list of schemas
        async with managed_nested_transaction(engine) as conn:
            schemas = await query.execute(conn, pattern=TENANT_SCHEMA_PATTERN)
        
        schemas: list[str] = list(schemas)
        if not schemas:
            logger.debug("No tenant schemas found to clean up.")
        else:
            logger.info(f"Found {len(schemas)} tenant schemas to drop. Terminating access and dropping in batches...")

        # Step 1: Force terminate backend access sequentially
        # We use a dedicated connection and the imported `force_drop_schema` 
        # or `terminate_backends_locking_schema` (by using force_drop_schema it's automatic).
        # We will terminate the locks first.
        from dynastore.modules.db_config.locking_tools import terminate_backends_locking_schema
        for schema in schemas:
            try:
                # Terminate backends without starting long transactions
                async with managed_nested_transaction(engine) as conn:
                    await terminate_backends_locking_schema(conn, schema)
            except Exception as e:
                logger.debug(f"Failed to terminate backends for {schema} (ignored): {e}")

        # Step 2: Drop schemas in batches to avoid exceeding max_locks_per_transaction
        # (default 64; SCHEMA_DROP_BATCH_SIZE is the canonical safe value).
        failed: list[str] = []
        batch: list[str] = []
        for schema in schemas:
            batch.append(schema)
            if len(batch) >= SCHEMA_DROP_BATCH_SIZE:
                drop_sql = drop_schemas_batch_sql(batch)
                try:
                    async with managed_nested_transaction(engine) as conn:
                        await DDLQuery(drop_sql).execute(conn)
                    logger.debug(f"Dropped batch of {len(batch)} schemas.")
                except Exception as e:
                    logger.warning(f"Batch drop failed ({e}), falling back to individual drops for this batch.")
                    failed.extend(batch)
                batch = []
        if batch:
            drop_sql = drop_schemas_batch_sql(batch)
            try:
                async with managed_nested_transaction(engine) as conn:
                    await DDLQuery(drop_sql).execute(conn)
                logger.debug(f"Dropped final batch of {len(batch)} schemas.")
            except Exception as e:
                logger.warning(f"Final batch drop failed ({e}), falling back to individual drops.")
                failed.extend(batch)

        # Fallback: drop individually any schemas that failed in batch
        for schema in failed:
            try:
                async with managed_nested_transaction(engine) as conn:
                    await force_drop_schema(conn, schema)
            except Exception as inner_e:
                logger.debug(f"Error dropping schema {schema}: {inner_e}")

        if schemas:
            logger.info(f"Finished dropping {len(schemas)} tenant schemas ({len(failed)} fell back to individual drops).")

        # Step 3: Remove orphaned pg_cron jobs for dropped tenant schemas
        if schemas:
            try:
                patterns = [f"%{s}%" for s in schemas]
                async with managed_nested_transaction(engine) as conn:
                    for pat in patterns:
                        await DQLQuery(
                            "DELETE FROM cron.job WHERE jobname LIKE :pat;",
                            result_handler=ResultHandler.ROWCOUNT,
                        ).execute(conn, pat=pat)
                logger.info(f"Removed orphaned cron jobs for {len(schemas)} tenant schemas.")
            except Exception as e:
                logger.debug(f"Failed to remove orphaned cron jobs (ignored): {e}")

        # Step 4: Truncate catalog metadata tables (always runs, even if no tenant schemas were found).
        # Table list is canonical in dynastore.tools.db_cleanup.
        for table in CATALOG_METADATA_TABLES:
            try:
                async with managed_nested_transaction(engine) as conn:
                    if await check_table_exists(conn, table, "catalog"):
                        logger.info(f"Truncating catalog.{table}...")
                        await force_truncate_table(conn, "catalog", table)
            except Exception as e:
                logger.debug(f"Failed to truncate catalog.{table} (ignored): {e}")

    except Exception as e:
        logger.error(f"Error during tenant schema discovery: {e}")
