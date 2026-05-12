from tests.dynastore.test_utils.cleanup_registry import CleanupRegistry
from dynastore.modules.db_config.query_executor import DDLQuery, managed_nested_transaction
from dynastore.modules.db_config.locking_tools import check_table_exists
import logging

logger = logging.getLogger(__name__)

@CleanupRegistry.register
async def cleanup_iam(engine):
    """Truncate the iam and users schemas on the worker's DB.

    Safe under pytest-xdist: each worker has its own cloned database
    (``gis_dev_<worker_id>``, see ``tests/conftest.py::_ensure_worker_db``
    commit ``e3d5458``), so truncating shared tables here cannot affect
    another worker's state. The earlier ``PYTEST_XDIST_WORKER`` early-return
    was a stale guard from before per-worker DB cloning landed.
    """
    async with managed_nested_transaction(engine) as conn:
        logger.info("Cleaning up iam and users schemas...")

        # Tables to truncate in order of dependency
        tables = [
            ("iam", "identity_policies"),
            ("iam", "identity_roles"),
            ("iam", "identity_authorization"),
            ("iam", "refresh_tokens"),
            ("iam", "api_keys"),
            ("iam", "identity_links"),
            ("iam", "principals"),
            ("iam", "role_hierarchy"),
            ("iam", "roles"),
            ("iam", "policies"),
            ("users", "oauth_tokens"),
            ("users", "oauth_codes"),
            ("users", "users")
        ]
        
        for schema, table in tables:
            try:
                async with managed_nested_transaction(conn) as nested:
                    if await check_table_exists(nested, table, schema=schema):
                        await DDLQuery(f'DELETE FROM "{schema}"."{table}";').execute(nested)
                        logger.debug(f"Truncated table {schema}.{table}")
            except Exception as e:
                logger.debug(f"Could not truncate {schema}.{table}: {e}")

    # Also clean up partitions if they were created and are not the 'global' ones
    # For now, TRUNCATE CASCADE on the parent table should handle it.
