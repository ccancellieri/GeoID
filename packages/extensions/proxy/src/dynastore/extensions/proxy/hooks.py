from typing import Optional
import logging
from dynastore.modules.catalog.event_service import register_event_listener, CatalogEventType
from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler, DbResource
from dynastore.models.protocols import CatalogsProtocol
from dynastore.modules import get_protocol
from dynastore.models.driver_context import DriverContext

logger = logging.getLogger(__name__)

async def cleanup_collection_proxy_urls(event_payload: Optional[dict] = None, cache_conn: Optional[DbResource] = None, db_resource: Optional[DbResource] = None, **kwargs):
    """
    Listener for COLLECTION_DELETION and COLLECTION_HARD_DELETION events.
    Cleans up all proxy URLs associated with a deleted collection.
    """
    catalog_id = kwargs.get("catalog_id") or (event_payload.get("catalog_id") if event_payload else None)
    collection_id = kwargs.get("collection_id") or (event_payload.get("collection_id") if event_payload else None)
    
    active_db = db_resource or cache_conn

    if not catalog_id or not collection_id or not active_db:
        return

    logger.info(f"Proxy Hook: Cleaning up proxy URLs for deleted collection '{catalog_id}:{collection_id}'")

    try:
        # 1. Resolve physical schema
        catalogs = get_protocol(CatalogsProtocol)
        phys_schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=active_db))  # type: ignore[union-attr]
        if not phys_schema:
            return

        # 2. Find all short keys
        find_keys_query = DQLQuery(
            f"SELECT short_key FROM {phys_schema}.collection_proxy_urls WHERE collection_id = :coll",
            result_handler=ResultHandler.ALL_SCALARS
        )
        # Use active_db directly to avoid nested transaction deadlocks
        short_keys = await find_keys_query.execute(active_db, coll=collection_id)
        
        if not short_keys:
            return
        
        logger.info(f"Proxy Hook: Found {len(short_keys)} short keys to delete for collection '{collection_id}'")

        # 3. Delete from global proxy table
        from dynastore.modules.proxy.proxy_module import delete_short_url
        for key in short_keys:
            try:
                await delete_short_url(active_db, catalog_id=catalog_id, short_key=key)
            except Exception as e:
                logger.error(f"Proxy Hook: Failed to delete short key '{key}' from storage: {e}")

        # 4. Clean up tracking table — DML, use DQLQuery (DDLQuery would
        # wrap each DELETE in advisory lock + savepoint, wrong for hot path)
        await DQLQuery(
            f"DELETE FROM {phys_schema}.collection_proxy_urls WHERE collection_id = :coll",
            result_handler=ResultHandler.NONE,
        ).execute(active_db, coll=collection_id)

    except Exception as e:
        logger.error(f"Proxy Hook: Error during cleanup for collection '{collection_id}': {e}", exc_info=True)

def register_proxy_listeners():
    """Registers proxy cleanup listeners."""
    register_event_listener(CatalogEventType.COLLECTION_DELETION, cleanup_collection_proxy_urls)
    register_event_listener(CatalogEventType.COLLECTION_HARD_DELETION, cleanup_collection_proxy_urls)

