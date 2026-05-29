#    Copyright 2025 FAO
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
Item CRUD protocol — create, read, update, delete operations.
"""

from typing import Protocol, Optional, Any, List, Dict, Union, runtime_checkable, TYPE_CHECKING

from dynastore.models.ogc import Feature, FeatureCollection

if TYPE_CHECKING:
    from dynastore.models.driver_context import DriverContext


@runtime_checkable
class ItemCrudProtocol(Protocol):
    """
    Protocol for item lifecycle operations (CRUD).

    Covers creating, retrieving, and deleting items (features) within a
    collection.  Does not expose query, search, or introspection concerns.
    """

    async def upsert(
        self,
        catalog_id: str,
        collection_id: str,
        items: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        ctx: Optional["DriverContext"] = None,
        processing_context: Optional[Dict[str, Any]] = None,
    ) -> Union[Feature, List[Feature]]:
        """Create or update items (single or bulk)."""
        ...

    async def get_item(
        self,
        catalog_id: str,
        collection_id: str,
        geoid: Any,
        ctx: Optional["DriverContext"] = None,
        lang: str = "en",
        context: Optional[Any] = None,
        access_filter: Optional[Any] = None,
    ) -> Optional[Feature]:
        """Retrieve a specific item by its internal geoid or feature ID.

        ``access_filter``: when the collection uses a PG ``access_envelope``
        sidecar, callers MUST supply this.  User-facing reads compile it from
        request state via ``compile_read_access_filter``; privileged/system
        reads pass ``AccessFilter.allow_everything()`` to avoid a fail-closed
        blackout.  ``None`` is accepted for backward compatibility with callers
        that route through the non-PG driver dispatch path (which ignores the
        field).
        """
        ...

    async def delete_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        ctx: Optional["DriverContext"] = None,
        caller_id: Optional[str] = None,
    ) -> int:
        """Soft-delete all versions of an item by its external ID.

        ``caller_id`` records the originating principal so write-reactive
        tile-cache invalidation tasks are attributed to the user who issued
        the delete (mirror of the create/update path — see #1404/#1407).
        """
        ...

    async def delete_item_language(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        lang: str,
        ctx: Optional["DriverContext"] = None,
    ) -> int:
        """Delete a specific language translation for an item."""
        ...

    async def resolve_external_id_by_geoid(
        self,
        catalog_id: str,
        collection_id: str,
        geoid: str,
        ctx: Optional["DriverContext"] = None,
    ) -> Optional[str]:
        """Translate a path-id geoid into its row's stored ``external_id``.

        Returns ``None`` when ``geoid`` is not a UUID, the
        catalog/collection is unresolved, the collection has no identity-
        carrying sidecar, or no active row matches. Used by write handlers
        whose API contract carries the geoid as the surface-level id (post-
        #1212) to translate back to the external_id ``upsert`` keys on.
        """
        ...
