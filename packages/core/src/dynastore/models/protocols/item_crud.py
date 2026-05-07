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
    ) -> Optional[Feature]:
        """Retrieve a specific item by its internal geoid or feature ID."""
        ...

    async def delete_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        ctx: Optional["DriverContext"] = None,
    ) -> int:
        """Soft-delete all versions of an item by its external ID."""
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
