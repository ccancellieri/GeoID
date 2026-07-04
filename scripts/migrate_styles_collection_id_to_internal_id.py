#!/usr/bin/env python
"""
Backfill script for Issue #2952: styles.styles rows keyed by external collection id.

create_style_for_collection (and every other CRUD/list/get call site in
extensions/styles/styles_service.py) used to pass the raw external
collection_id path parameter straight through to styles_db instead of
resolving it to the immutable internal id first, as the code already did for
catalog_id. That bug is fixed alongside this script. Rows written before the
fix carry the external collection_id in styles.styles.collection_id and are
invisible to any read path that resolves the external id to internal first
(e.g. the styled-map render route), which is why this backfill exists: to
remap those pre-existing rows onto the internal collection id used
everywhere else.

Usage:
    python scripts/migrate_styles_collection_id_to_internal_id.py [--commit]

The script:
1. Reads the distinct set of catalog_id values present in styles.styles
   (already internal ids -- the write path already resolved catalog_id
   correctly, only collection_id was wrong).
2. For each catalog_id, its physical schema IS the internal id itself (see
   CatalogService.resolve_physical_schema): "{catalog_id}".collections holds
   that catalog's collections.
3. For each styles.styles row, checks whether collection_id is already a
   live internal id; if not, resolves it as an external_id and updates the
   row.
4. Handles collisions: if (catalog_id, internal_id, style_id) already
   exists, deletes the stale external-id-keyed row instead of updating
   (avoids a unique constraint violation on
   (catalog_id, collection_id, style_id)).
5. Handles orphaned rows (collection_id is neither a live internal id nor a
   live external_id).

Dry-run by default; pass --commit to apply changes.
"""

import argparse
import asyncio
import logging
from typing import Any, Dict, List, Optional

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.tools.protocol_helpers import get_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def get_style_catalog_ids(conn: Any) -> List[str]:
    """Distinct catalog_id values present in styles.styles."""
    rows = await DQLQuery(
        "SELECT DISTINCT catalog_id FROM styles.styles",
        result_handler=ResultHandler.ALL_DICTS,
    ).execute(conn)
    return [r["catalog_id"] for r in rows]


async def get_styles_for_catalog(conn: Any, catalog_id: str) -> List[Dict[str, Any]]:
    rows = await DQLQuery(
        "SELECT id, collection_id, style_id FROM styles.styles WHERE catalog_id = :catalog_id",
        result_handler=ResultHandler.ALL_DICTS,
    ).execute(conn, catalog_id=catalog_id)
    return rows


async def is_live_internal_collection_id(conn: Any, schema: str, collection_id: str) -> bool:
    row = await DQLQuery(
        f'SELECT 1 FROM "{schema}".collections WHERE id = :collection_id AND deleted_at IS NULL',
        result_handler=ResultHandler.ONE_OR_NONE,
    ).execute(conn, collection_id=collection_id)
    return row is not None


async def resolve_internal_id_for_external(
    conn: Any, schema: str, external_id: str
) -> Optional[str]:
    row = await DQLQuery(
        f'SELECT id FROM "{schema}".collections '
        "WHERE external_id = :external_id AND deleted_at IS NULL",
        result_handler=ResultHandler.ONE_OR_NONE,
    ).execute(conn, external_id=external_id)
    return row["id"] if row else None


async def style_row_exists(
    conn: Any, catalog_id: str, collection_id: str, style_id: str
) -> bool:
    row = await DQLQuery(
        "SELECT 1 FROM styles.styles WHERE catalog_id = :catalog_id "
        "AND collection_id = :collection_id AND style_id = :style_id",
        result_handler=ResultHandler.ONE_OR_NONE,
    ).execute(conn, catalog_id=catalog_id, collection_id=collection_id, style_id=style_id)
    return row is not None


async def migrate_catalog(engine: Any, catalog_id: str, dry_run: bool = True) -> Dict[str, int]:
    """Migrate styles.styles rows for a single catalog.

    Runs in its own transaction so a failure for this catalog does not roll
    back progress on other catalogs.
    """
    stats = {
        "total": 0,
        "migrated": 0,
        "already_internal": 0,
        "orphaned": 0,
        "collision_deleted": 0,
    }

    try:
        async with managed_transaction(engine) as conn:
            styles = await get_styles_for_catalog(conn, catalog_id)
            stats["total"] = len(styles)

            for row in styles:
                collection_id = row["collection_id"]
                style_id = row["style_id"]

                if await is_live_internal_collection_id(conn, catalog_id, collection_id):
                    stats["already_internal"] += 1
                    continue

                internal_id = await resolve_internal_id_for_external(
                    conn, catalog_id, collection_id
                )
                if not internal_id:
                    logger.warning(
                        f"Orphaned style in catalog={catalog_id}: "
                        f"collection_id={collection_id}, style_id={style_id} - "
                        "not a live internal or external collection id"
                    )
                    stats["orphaned"] += 1
                    continue

                collides = await style_row_exists(conn, catalog_id, internal_id, style_id)

                if not dry_run:
                    if collides:
                        await DQLQuery(
                            "DELETE FROM styles.styles WHERE catalog_id = :catalog_id "
                            "AND collection_id = :external_id AND style_id = :style_id",
                            result_handler=ResultHandler.ROWCOUNT,
                        ).execute(
                            conn,
                            catalog_id=catalog_id,
                            external_id=collection_id,
                            style_id=style_id,
                        )
                        logger.warning(
                            f"Collision in catalog={catalog_id}: "
                            f"(collection_id={internal_id}, style_id={style_id}) "
                            "already exists; deleted stale external-id-keyed row"
                        )
                        stats["collision_deleted"] += 1
                    else:
                        await DQLQuery(
                            "UPDATE styles.styles SET collection_id = :internal_id "
                            "WHERE catalog_id = :catalog_id "
                            "AND collection_id = :external_id AND style_id = :style_id",
                            result_handler=ResultHandler.ROWCOUNT,
                        ).execute(
                            conn,
                            internal_id=internal_id,
                            catalog_id=catalog_id,
                            external_id=collection_id,
                            style_id=style_id,
                        )
                        logger.info(
                            f"Migrated catalog={catalog_id}: {collection_id} -> "
                            f"{internal_id} (style_id={style_id})"
                        )
                        stats["migrated"] += 1
                else:
                    if collides:
                        logger.info(
                            f"[DRY-RUN] Would delete stale row in catalog={catalog_id}: "
                            f"collection_id={collection_id}, style_id={style_id} "
                            f"(internal_id={internal_id} already present)"
                        )
                        stats["collision_deleted"] += 1
                    else:
                        logger.info(
                            f"[DRY-RUN] Would migrate catalog={catalog_id}: "
                            f"{collection_id} -> {internal_id} (style_id={style_id})"
                        )
                        stats["migrated"] += 1

    except Exception as exc:
        logger.error(
            f"Failed to migrate styles for catalog {catalog_id}: {exc}; "
            "skipping (other catalogs unaffected)"
        )

    return stats


async def migrate_all(dry_run: bool = True) -> Dict[str, Any]:
    """Migrate styles.styles for every catalog that has style rows.

    Each catalog runs in its own transaction so a failure for one catalog
    does not roll back progress on others.
    """
    engine = get_engine()
    total_stats = {
        "catalogs_processed": 0,
        "total_rows": 0,
        "migrated": 0,
        "already_internal": 0,
        "orphaned": 0,
        "collision_deleted": 0,
    }

    async with managed_transaction(engine) as conn:
        catalog_ids = await get_style_catalog_ids(conn)
    logger.info(f"Found {len(catalog_ids)} catalogs with styles to process")

    for catalog_id in catalog_ids:
        stats = await migrate_catalog(engine, catalog_id, dry_run)
        total_stats["catalogs_processed"] += 1
        total_stats["total_rows"] += stats["total"]
        total_stats["migrated"] += stats["migrated"]
        total_stats["already_internal"] += stats["already_internal"]
        total_stats["orphaned"] += stats["orphaned"]
        total_stats["collision_deleted"] += stats["collision_deleted"]

    return total_stats


def main():
    parser = argparse.ArgumentParser(
        description="Migrate styles.styles rows to use internal collection_id (Issue #2952)"
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
    logger.info(f"  Catalogs processed:  {stats['catalogs_processed']}")
    logger.info(f"  Total style rows:    {stats['total_rows']}")
    logger.info(f"  Already internal:    {stats['already_internal']}")
    logger.info(f"  Migrated:            {stats['migrated']}")
    logger.info(f"  Collision-deleted:   {stats['collision_deleted']}")
    logger.info(f"  Orphaned (skipped):  {stats['orphaned']}")
    logger.info("=" * 60)

    if dry_run and (stats["migrated"] > 0 or stats["collision_deleted"] > 0):
        logger.info("Run with --commit to apply changes")


if __name__ == "__main__":
    main()
