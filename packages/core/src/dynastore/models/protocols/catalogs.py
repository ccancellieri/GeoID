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
Catalog-related protocol definitions.
"""

from typing import (
    Protocol,
    Optional,
    Any,
    List,
    Dict,
    Union,
    Set,
    runtime_checkable,
    TYPE_CHECKING,
)

from dynastore.models.protocols.item_crud import ItemCrudProtocol
from dynastore.models.protocols.item_query import ItemQueryProtocol
from dynastore.models.protocols.item_introspection import ItemIntrospectionProtocol
from dynastore.models.protocols.items import ItemsProtocol  # backward-compat composite
from dynastore.models.protocols.collections import CollectionsProtocol

if TYPE_CHECKING:
    from dynastore.models.shared_models import Catalog, CatalogUpdate
    from dynastore.models.protocols.assets import AssetsProtocol
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.models.protocols.localization import LocalizationProtocol
    from dynastore.models.driver_context import DriverContext  # noqa: F401


@runtime_checkable
class CatalogsProtocol(ItemCrudProtocol, ItemQueryProtocol, ItemIntrospectionProtocol, CollectionsProtocol, Protocol):
    """
    Unified protocol for catalog ecosystem operations.

    Provides access to:
    - Catalog CRUD operations
    - Collection CRUD operations (via inheritance)
    - Item CRUD operations (via inheritance)
    - Asset management (via delegation)
    - Configuration management (via delegation)
    - Localization utilities (via delegation)

    This protocol uses composition and inheritance to provide a single
    entry point for all catalog-related operations while keeping
    the interface modular and logically separated.
    """

    # === Sub-Protocol Access (Properties) ===

    @property
    def items(self) -> ItemsProtocol:
        """Access to item management operations."""
        ...

    @property
    def collections(self) -> CollectionsProtocol:
        """Access to collection management operations."""
        ...

    @property
    def assets(self) -> "AssetsProtocol":
        """Access to asset management operations."""
        ...

    @property
    def configs(self) -> "ConfigsProtocol":
        """Access to configuration management operations."""
        ...

    @property
    def localization(self) -> "LocalizationProtocol":
        """Access to localization utilities."""
        ...

    # === Global Schema/Catalog Management ===

    async def resolve_physical_schema(
        self,
        catalog_id: Optional[str] = None,
        ctx: Optional["DriverContext"] = None,
        allow_missing: bool = False,
    ) -> Optional[str]:
        """
        Resolves the physical schema name for a given catalog ID.
        """
        ...

    async def ensure_catalog_exists(
        self, catalog_id: str, lang: str = "en", ctx: Optional["DriverContext"] = None,
    ) ->None:
        """
        Ensures that a catalog exists, creating it if necessary (JIT creation).
        """
        ...

    async def ensure_collection_exists(
        self, catalog_id: str, collection_id: str, lang: str = "en", ctx: Optional["DriverContext"] = None,
    ) ->None:
        """
        Ensures that a collection exists, creating it if necessary (JIT creation).
        """
        ...

    async def ensure_partition_exists(
        self,
        catalog_id: str,
        collection_id: str,
        config: Any,
        partition_value: Any,
        ctx: Optional["DriverContext"] = None,
    ) -> None:
        """
        Ensures that a partition exists for a collection's table.
        """
        ...

    # === Core Catalog Operations ===

    async def upsert(
        self,
        catalog_id: str,
        collection_id: str,
        items: Union[Dict[str, Any], Any],
        ctx: Optional["DriverContext"] = None,
        processing_context: Optional[Dict[str, Any]] = None,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]: ...

    async def get_catalog(
        self, catalog_id: str, lang: str = "en", ctx: Optional["DriverContext"] = None,
    ) ->"Catalog":
        """
        Retrieves the catalog metadata model for a specific language.
        """
        ...

    async def get_catalog_model(
        self, catalog_id: str, ctx: Optional["DriverContext"] = None,
    ) ->Optional["Catalog"]:
        """
        Retrieves the raw catalog model (often cached).
        """
        ...

    async def create_catalog(
        self,
        catalog_data: Union[Dict[str, Any], "Catalog"],
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> "Catalog":
        """
        Creates a new catalog.
        """
        ...

    async def update_catalog(
        self,
        catalog_id: str,
        updates: Union[Dict[str, Any], "CatalogUpdate"],
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> Optional["Catalog"]:
        """
        Updates an existing catalog.
        """
        ...

    async def delete_catalog(
        self, catalog_id: str, force: bool = False, ctx: Optional["DriverContext"] = None,
    ) ->bool:
        """
        Deletes a catalog and its associated resources.
        """
        ...

    async def update_provisioning_status(
        self, catalog_id: str, status: str, ctx: Optional["DriverContext"] = None,
    ) ->bool:
        """
        Updates the provisioning status of a catalog (provisioning | ready | failed).
        """
        ...

    async def get_catalog_config(
        self, catalog_id: str, ctx: Optional["DriverContext"] = None,
    ) ->Any:
        """Retrieves the configuration for a catalog."""
        ...

    async def get_collection_config(
        self, catalog_id: str, collection_id: str, ctx: Optional["DriverContext"] = None,
    ) ->Any:
        """Retrieves the configuration for a collection."""
        ...

    async def get_collection_column_names(
        self, catalog_id: str, collection_id: str, ctx: Optional["DriverContext"] = None,
    ) ->Set[str]:
        """Retrieves the physical column names for a collection."""
        ...

    async def delete_catalog_language(
        self, catalog_id: str, lang: str, ctx: Optional["DriverContext"] = None,
    ) ->bool:
        """
        Deletes a specific language translation for a catalog.
        """
        ...

    async def list_catalogs(
        self,
        limit: int = 10,
        offset: int = 0,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
        q: Optional[str] = None,
    ) -> List["Catalog"]:
        """
        Lists all catalogs.
        """
        ...
