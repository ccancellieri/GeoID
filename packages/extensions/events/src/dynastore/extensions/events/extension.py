from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import DbResource
from contextlib import asynccontextmanager
from typing import Optional, Any
import logging
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.models.protocols import DatabaseProtocol, EventDriverProtocol
from fastapi import APIRouter, Query, HTTPException, Depends


logger = logging.getLogger(__name__)

# Try to import EventsProtocol, create dummy if it doesn't exist yet for registry check
try:
    from dynastore.models.protocols.events import EventsProtocol  # type: ignore[assignment]
except ImportError:
    class EventsProtocol:
        pass


class EventsExtension(ExtensionProtocol, EventsProtocol):
    priority: int = 100
    router = APIRouter(prefix="/events", tags=["Events"])

    def __init__(self, app: Optional[Any] = None):
        super().__init__()
        self.app = app
        self._engine: Optional[DbResource] = None
        self._register_routes()

    def _register_routes(self):
        # --- Event search routes ---
        self.router.add_api_route("/system", self.get_system_events, methods=["GET"])
        self.router.add_api_route(
            "/catalogs/{catalog_id}/events", self.get_catalog_events, methods=["GET"]
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/events",
            self.get_collection_events,
            methods=["GET"],
        )

        # --- Subscription routes: platform scope ---
        self.router.add_api_route(
            "/subscriptions",
            self.create_subscription,
            methods=["POST"],
        )
        self.router.add_api_route(
            "/subscriptions",
            self.list_subscriptions,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/subscriptions/{subscriber_name}/{event_type}",
            self.delete_subscription,
            methods=["DELETE"],
        )

        # --- Subscription routes: catalog scope ---
        self.router.add_api_route(
            "/catalogs/{catalog_id}/subscriptions",
            self.create_catalog_subscription,
            methods=["POST"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/subscriptions",
            self.list_catalog_subscriptions,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/subscriptions/{subscriber_name}/{event_type}",
            self.delete_catalog_subscription,
            methods=["DELETE"],
        )

        # --- Subscription routes: collection scope ---
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/subscriptions",
            self.create_collection_subscription,
            methods=["POST"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/subscriptions",
            self.list_collection_subscriptions,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}"
            "/subscriptions/{subscriber_name}/{event_type}",
            self.delete_collection_subscription,
            methods=["DELETE"],
        )

    # ------------------------------------------------------------------
    # Event search handlers
    # ------------------------------------------------------------------

    async def get_system_events(
        self,
        event_type: Optional[str] = Query(None),
        limit: int = 100,
        offset: int = 0,
    ):
        """Retrieve global system-level events."""
        from dynastore.modules.catalog.event_service import event_service
        return await event_service.search_events(
            engine=self.engine,
            catalog_id="_system_",
            event_type=event_type,
            limit=limit,
            offset=offset,
        )

    async def get_catalog_events(
        self,
        catalog_id: str,
        event_type: Optional[str] = Query(None),
        limit: int = 100,
        offset: int = 0,
    ):
        """Retrieve events for a specific catalog."""
        from dynastore.modules.catalog.event_service import event_service
        return await event_service.search_events(
            engine=self.engine,
            catalog_id=catalog_id,
            event_type=event_type,
            limit=limit,
            offset=offset,
        )

    async def get_collection_events(
        self,
        catalog_id: str,
        collection_id: str,
        event_type: Optional[str] = Query(None),
        limit: int = 100,
        offset: int = 0,
    ):
        """Retrieve events for a specific collection."""
        from dynastore.modules.catalog.event_service import event_service
        return await event_service.search_events(
            engine=self.engine,
            catalog_id=catalog_id,
            collection_id=collection_id,
            event_type=event_type,
            limit=limit,
            offset=offset,
        )

    # ------------------------------------------------------------------
    # Subscription handlers — platform scope
    # ------------------------------------------------------------------

    def _get_driver(self) -> Any:
        driver = get_protocol(EventDriverProtocol)
        if not driver:
            raise HTTPException(status_code=503, detail="EventDriverProtocol not active.")
        return driver

    async def create_subscription(self, body: Any):
        """Create or update a platform-scoped webhook subscription."""
        from dynastore.modules.events.models import EventSubscriptionCreate
        driver = self._get_driver()
        return await driver.subscribe(body)

    async def list_subscriptions(
        self, event_type: Optional[str] = Query(None)
    ):
        """List platform-scoped webhook subscriptions."""
        driver = self._get_driver()
        if event_type:
            return await driver.get_subscriptions_for_event_type(event_type)
        return []

    async def delete_subscription(self, subscriber_name: str, event_type: str):
        """Delete a platform-scoped webhook subscription."""
        driver = self._get_driver()
        result = await driver.unsubscribe(
            subscriber_name=subscriber_name, event_type=event_type
        )
        if result is None:
            raise HTTPException(status_code=404, detail="Subscription not found.")
        return result

    # ------------------------------------------------------------------
    # Subscription handlers — catalog scope
    # ------------------------------------------------------------------

    async def create_catalog_subscription(self, catalog_id: str, body: Any):
        """Create or update a catalog-scoped webhook subscription."""
        driver = self._get_driver()
        return await driver.subscribe(body)

    async def list_catalog_subscriptions(
        self,
        catalog_id: str,
        event_type: Optional[str] = Query(None),
    ):
        """List catalog-scoped webhook subscriptions."""
        driver = self._get_driver()
        if event_type:
            return await driver.get_subscriptions_for_event_type(event_type)
        return []

    async def delete_catalog_subscription(
        self, catalog_id: str, subscriber_name: str, event_type: str
    ):
        """Delete a catalog-scoped webhook subscription."""
        driver = self._get_driver()
        result = await driver.unsubscribe(
            subscriber_name=subscriber_name, event_type=event_type
        )
        if result is None:
            raise HTTPException(status_code=404, detail="Subscription not found.")
        return result

    # ------------------------------------------------------------------
    # Subscription handlers — collection scope
    # ------------------------------------------------------------------

    async def create_collection_subscription(
        self, catalog_id: str, collection_id: str, body: Any
    ):
        """Create or update a collection-scoped webhook subscription."""
        driver = self._get_driver()
        return await driver.subscribe(body)

    async def list_collection_subscriptions(
        self,
        catalog_id: str,
        collection_id: str,
        event_type: Optional[str] = Query(None),
    ):
        """List collection-scoped webhook subscriptions."""
        driver = self._get_driver()
        if event_type:
            return await driver.get_subscriptions_for_event_type(event_type)
        return []

    async def delete_collection_subscription(
        self, catalog_id: str, collection_id: str, subscriber_name: str, event_type: str
    ):
        """Delete a collection-scoped webhook subscription."""
        driver = self._get_driver()
        result = await driver.unsubscribe(
            subscriber_name=subscriber_name, event_type=event_type
        )
        if result is None:
            raise HTTPException(status_code=404, detail="Subscription not found.")
        return result

    # ------------------------------------------------------------------
    # Lifespan
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def lifespan(self, app: Any):
        db = get_protocol(DatabaseProtocol)
        if db:
            self.engine = db.engine

        if not self._engine:
            logger.warning(
                "EventsExtension: No DB engine found via DatabaseProtocol. Extension disabled."
            )
            yield
            return

        logger.info("EventsExtension initialized.")
        yield


def get_events_extension():
    ext = get_protocol(EventsProtocol)
    if not ext:
        raise HTTPException(status_code=503, detail="EventsExtension not active.")
    return ext
