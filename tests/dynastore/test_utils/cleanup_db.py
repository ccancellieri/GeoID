import asyncio
import os
import sys
import logging
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from dynastore.modules.db_config.query_executor import managed_transaction, DDLQuery

# Add project root and src to path if running directly
root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
if root not in sys.path:
    sys.path.append(root)
sys.path.append(os.path.join(root, "src"))

from tests.dynastore.test_utils.cleanup_registry import CleanupRegistry

# Import modules from tests/ to register their cleanup handlers
try:
    import tests.dynastore.extensions.auth.cleanup
    import tests.dynastore.modules.iam.cleanup
    import tests.dynastore.modules.catalog.cleanup
except ImportError:
    pass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://testuser:testpassword@localhost:54320/gis_dev")
if "asyncpg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

# Boot tier (packages/core/src/dynastore/scripts/db_reset.sh, invoked by the dev/test db container
# entrypoint) already drops all user schemas + orphan cron jobs before Postgres
# is reported healthy. When that ran recently the pre-session wipe is redundant,
# so we probe first and short-circuit. The pytest_sessionfinish hook keeps
# running unconditionally to guarantee a clean state between successive runs
# against a long-lived stack.
async def _boot_tier_already_clean(engine) -> bool:
    try:
        async with engine.connect() as conn:
            tenant_count = (await conn.execute(text(
                "SELECT count(*) FROM pg_namespace "
                "WHERE nspname ~ '^s_[0-9a-z]{8}$'"
            ))).scalar_one()
            if tenant_count:
                return False
            iam_present = (await conn.execute(text(
                "SELECT to_regclass('iam.identity') IS NOT NULL"
            ))).scalar_one()
            if not iam_present:
                return True
            iam_rows = (await conn.execute(text(
                "SELECT count(*) FROM iam.identity"
            ))).scalar_one()
            return iam_rows == 0
    except Exception as e:
        logger.debug(f"Boot-tier probe failed, falling back to full cleanup: {e}")
        return False


async def cleanup_db(skip_if_clean: bool = False):
    """Run CleanupRegistry against the configured DB.

    Set ``skip_if_clean=True`` from the session-start fixture so that a freshly
    reset database (boot tier just ran) becomes a cheap no-op. Session-finish
    callers leave it False so the wipe runs unconditionally — that's what
    guarantees a clean slate for the *next* run when the stack is long-lived.
    """
    engine = create_async_engine(DATABASE_URL)

    if skip_if_clean and await _boot_tier_already_clean(engine):
        logger.info("Boot-tier already clean, skipping wholesale wipe.")
        await engine.dispose()
        return

    logger.info("Starting database cleanup...")

    # Run cleanup without holding a global lock that blocks other workers.
    # Each handler uses its own nested transactions with IF EXISTS / CASCADE logic.
    try:
        await CleanupRegistry.run_all(engine)
    except Exception as e:
        logger.warning(f"Cleanup failed (non-critical): {e}")

    logger.info("Database cleanup complete.")
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(cleanup_db())
