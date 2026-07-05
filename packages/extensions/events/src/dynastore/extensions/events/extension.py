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

from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import DbResource
from dynastore.modules.tasks.events.models import EventSubscriptionCreate
from contextlib import asynccontextmanager
from typing import Optional, Any
import logging
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.models.protocols import DatabaseProtocol, EventDriverProtocol
from fastapi import APIRouter, Query, HTTPException


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
        self.engine: Optional[DbResource] = None
        self._register_routes()

    def _register_routes(self):
        route_table: list[tuple[str, str, list[str], dict[str, Any]]] = [
            # --- Event search routes ---
            ("/system", "get_system_events", ["GET"], {}),
            ("/catalogs/{catalog_id}/events", "get_catalog_events", ["GET"], {}),
            (
                "/catalogs/{catalog_id}/collections/{collection_id}/events",
                "get_collection_events",
                ["GET"],
                {},
            ),
            # --- Subscription routes: platform scope ---
            ("/subscriptions", "create_subscription", ["POST"], {}),
            ("/subscriptions", "list_subscriptions", ["GET"], {}),
            (
                "/subscriptions/{subscriber_name}/{event_type}",
                "delete_subscription",
                ["DELETE"],
                {},
            ),
            # --- Subscription routes: catalog scope ---
            (
                "/catalogs/{catalog_id}/subscriptions",
                "create_catalog_subscription",
                ["POST"],
                {},
            ),
            (
                "/catalogs/{catalog_id}/subscriptions",
                "list_catalog_subscriptions",
                ["GET"],
                {},
            ),
            (
                "/catalogs/{catalog_id}/subscriptions/{subscriber_name}/{event_type}",
                "delete_catalog_subscription",
                ["DELETE"],
                {},
            ),
            # --- Subscription routes: collection scope ---
            (
                "/catalogs/{catalog_id}/collections/{collection_id}/subscriptions",
                "create_collection_subscription",
                ["POST"],
                {},
            ),
            (
                "/catalogs/{catalog_id}/collections/{collection_id}/subscriptions",
                "list_collection_subscriptions",
                ["GET"],
                {},
            ),
            (
                "/catalogs/{catalog_id}/collections/{collection_id}"
                "/subscriptions/{subscriber_name}/{event_type}",
                "delete_collection_subscription",
                ["DELETE"],
                {},
            ),
        ]
        for path, handler_name, methods, kwargs in route_table:
            self.router.add_api_route(path, getattr(self, handler_name), methods=methods, **kwargs)

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
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_driver(self) -> Any:
        driver = get_protocol(EventDriverProtocol)
        if not driver:
            raise HTTPException(status_code=503, detail="EventDriverProtocol not active.")
        return driver

    # ------------------------------------------------------------------
    # Subscription handlers — platform scope
    # ------------------------------------------------------------------

    async def create_subscription(self, body: EventSubscriptionCreate):
        """Create or update a platform-scoped webhook subscription."""
        driver = self._get_driver()
        body.scope = "PLATFORM"
        body.catalog_id = None
        body.collection_id = None
        return await driver.subscribe(body)

    async def list_subscriptions(
        self, event_type: Optional[str] = Query(None)
    ):
        """List platform-scoped webhook subscriptions."""
        driver = self._get_driver()
        if not event_type:
            return []
        all_subs = await driver.get_subscriptions_for_event_type(event_type)
        return [s for s in all_subs if s.scope == "PLATFORM"]

    async def delete_subscription(self, subscriber_name: str, event_type: str):
        """Delete a platform-scoped webhook subscription."""
        driver = self._get_driver()
        result = await driver.unsubscribe(
            subscriber_name=subscriber_name,
            event_type=event_type,
            scope="PLATFORM",
            catalog_id=None,
            collection_id=None,
        )
        if result is None:
            raise HTTPException(status_code=404, detail="Subscription not found.")
        return result

    # ------------------------------------------------------------------
    # Subscription handlers — catalog scope
    # ------------------------------------------------------------------

    async def create_catalog_subscription(self, catalog_id: str, body: EventSubscriptionCreate):
        """Create or update a catalog-scoped webhook subscription."""
        driver = self._get_driver()
        body.scope = "CATALOG"
        body.catalog_id = catalog_id
        body.collection_id = None
        return await driver.subscribe(body)

    async def list_catalog_subscriptions(
        self,
        catalog_id: str,
        event_type: Optional[str] = Query(None),
    ):
        """List catalog-scoped webhook subscriptions."""
        driver = self._get_driver()
        if not event_type:
            return []
        all_subs = await driver.get_subscriptions_for_event_type(event_type)
        return [
            s for s in all_subs
            if s.scope == "CATALOG" and s.catalog_id == catalog_id
        ]

    async def delete_catalog_subscription(
        self, catalog_id: str, subscriber_name: str, event_type: str
    ):
        """Delete a catalog-scoped webhook subscription."""
        driver = self._get_driver()
        result = await driver.unsubscribe(
            subscriber_name=subscriber_name,
            event_type=event_type,
            scope="CATALOG",
            catalog_id=catalog_id,
            collection_id=None,
        )
        if result is None:
            raise HTTPException(status_code=404, detail="Subscription not found.")
        return result

    # ------------------------------------------------------------------
    # Subscription handlers — collection scope
    # ------------------------------------------------------------------

    async def create_collection_subscription(
        self, catalog_id: str, collection_id: str, body: EventSubscriptionCreate
    ):
        """Create or update a collection-scoped webhook subscription."""
        driver = self._get_driver()
        body.scope = "COLLECTION"
        body.catalog_id = catalog_id
        body.collection_id = collection_id
        return await driver.subscribe(body)

    async def list_collection_subscriptions(
        self,
        catalog_id: str,
        collection_id: str,
        event_type: Optional[str] = Query(None),
    ):
        """List collection-scoped webhook subscriptions."""
        driver = self._get_driver()
        if not event_type:
            return []
        all_subs = await driver.get_subscriptions_for_event_type(event_type)
        return [
            s for s in all_subs
            if s.scope == "COLLECTION"
            and s.catalog_id == catalog_id
            and s.collection_id == collection_id
        ]

    async def delete_collection_subscription(
        self, catalog_id: str, collection_id: str, subscriber_name: str, event_type: str
    ):
        """Delete a collection-scoped webhook subscription."""
        driver = self._get_driver()
        result = await driver.unsubscribe(
            subscriber_name=subscriber_name,
            event_type=event_type,
            scope="COLLECTION",
            catalog_id=catalog_id,
            collection_id=collection_id,
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

        if not self.engine:
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
