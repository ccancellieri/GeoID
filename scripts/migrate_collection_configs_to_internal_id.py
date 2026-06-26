#!/usr/bin/env python
"""
Migration script for Issue #2430: Update collection_configs to use internal collection_id.

This script migrates existing collection_configs rows from using mutable external_id
to using immutable internal id as the collection_id column.

Usage:
    python scripts/migrate_collection_configs_to_internal_id.py [--dry-run]

The script:
1. Scans all catalog schemas for collection_configs tables
2. For each row, looks up the internal_id from the collections table
3. Updates the collection_id column to use the internal_id
4. Handles rows that already use internal_id (no-op)
5. Handles orphaned rows (external_id not found in collections table)

Prerequisites:
- The collection must exist in the collections table
- Run during a maintenance window or with minimal traffic
"""

import argparse
import asyncio
import logging
from typing import Any, Dict, List, Optional

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DMLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.tools.db import get_engine
from dynastore.modules.catalog.catalog_service import is_internal_physical_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def get_all_catalog_schemas(conn: Any) -> List[str]:
    """Get all catalog schema names (c_xxx pattern)."""
    rows = await DQLQuery(
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE schema_name LIKE 'c\\_%' AND schema_name NOT LIKE 'c\\_%\\_%'",
        result_handler=ResultHandler.ALL_DICTS,
    ).execute(conn)
    return [r["schema_name"] for r in rows]


async def get_collection_configs(conn: Any, schema: str) -> List[Dict[str, Any]]:
    """Get all collection_configs rows for a schema."""
    rows = await DQLQuery(
        f'SELECT collection_id, ref_key, class_key FROM "{schema}".collection_configs',
        result_handler=ResultHandler.ALL_DICTS,
    ).execute(conn)
    return rows


async def get_internal_id_for_external(
    conn: Any, schema: str, external_id: str
) -> Optional[str]:
    """Look up internal_id for a given external_id in the collections table."""
    row = await DQLQuery(
        f'SELECT id FROM "{schema}".collections '
        "WHERE external_id = :external_id AND deleted_at IS NULL",
        result_handler=ResultHandler.ONE_OR_NONE,
    ).execute(conn, external_id=external_id)
    return row["id"] if row else None


async def migrate_catalog_schema(
    conn: Any, schema: str, dry_run: bool = True
) -> Dict[str, int]:
    """Migrate collection_configs for a single catalog schema.

    Returns stats: {total, migrated, already_internal, orphaned}
    """
    stats = {"total": 0, "migrated": 0, "already_internal": 0, "orphaned": 0}

    try:
        configs = await get_collection_configs(conn, schema)
    except Exception as e:
        logger.warning(f"Could not read collection_configs for {schema}: {e}")
        return stats

    stats["total"] = len(configs)

    for config in configs:
        collection_id = config["collection_id"]

        if is_internal_physical_name(collection_id, "col"):
            stats["already_internal"] += 1
            continue

        internal_id = await get_internal_id_for_external(conn, schema, collection_id)

        if not internal_id:
            logger.warning(
                f"Orphaned config in {schema}: collection_id={collection_id}, "
                f"ref_key={config['ref_key']} - external_id not found in collections table"
            )
            stats["orphaned"] += 1
            continue

        if not dry_run:
            await DMLQuery(
                f'UPDATE "{schema}".collection_configs '
                "SET collection_id = :internal_id "
                "WHERE collection_id = :external_id AND ref_key = :ref_key"
            ).execute(
                conn,
                internal_id=internal_id,
                external_id=collection_id,
                ref_key=config["ref_key"],
            )
            logger.info(
                f"Migrated {schema}: {collection_id} -> {internal_id} "
                f"(ref_key={config['ref_key']})"
            )
        else:
            logger.info(
                f"[DRY-RUN] Would migrate {schema}: {collection_id} -> {internal_id} "
                f"(ref_key={config['ref_key']})"
            )
        stats["migrated"] += 1

    return stats


async def migrate_all(dry_run: bool = True) -> Dict[str, Any]:
    """Migrate all catalog schemas."""
    engine = get_engine()
    total_stats = {
        "catalogs_processed": 0,
        "total_rows": 0,
        "migrated": 0,
        "already_internal": 0,
        "orphaned": 0,
    }

    async with managed_transaction(engine) as conn:
        schemas = await get_all_catalog_schemas(conn)
        logger.info(f"Found {len(schemas)} catalog schemas to process")

        for schema in schemas:
            stats = await migrate_catalog_schema(conn, schema, dry_run)
            total_stats["catalogs_processed"] += 1
            total_stats["total_rows"] += stats["total"]
            total_stats["migrated"] += stats["migrated"]
            total_stats["already_internal"] += stats["already_internal"]
            total_stats["orphaned"] += stats["orphaned"]

    return total_stats


def main():
    parser = argparse.ArgumentParser(
        description="Migrate collection_configs to use internal collection_id"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Show what would be changed without making changes (default: True)",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Actually perform the migration (default: dry-run)",
    )
    args = parser.parse_args()

    dry_run = not args.commit
    if dry_run:
        logger.info("Running in DRY-RUN mode - no changes will be made")
    else:
        logger.info("Running in COMMIT mode - changes WILL be made")

    stats = asyncio.run(migrate_all(dry_run=dry_run))

    logger.info("=" * 60)
    logger.info("Migration Summary:")
    logger.info(f"  Catalogs processed: {stats['catalogs_processed']}")
    logger.info(f"  Total config rows:   {stats['total_rows']}")
    logger.info(f"  Already internal:    {stats['already_internal']}")
    logger.info(f"  Migrated:            {stats['migrated']}")
    logger.info(f"  Orphaned (skipped):  {stats['orphaned']}")
    logger.info("=" * 60)

    if dry_run and stats["migrated"] > 0:
        logger.info("Run with --commit to apply changes")


if __name__ == "__main__":
    main()
