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

"""
Catalog-related protocol definitions.
"""

from typing import (
    FrozenSet,
    Protocol,
    Optional,
    Any,
    List,
    Dict,
    Tuple,
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

    async def resolve_catalog_id(
        self,
        external_id: str,
        allow_missing: bool = False,
    ) -> Optional[str]:
        """
        Resolves a public ``external_id`` to the immutable internal catalog id.

        Returns ``None`` when no live catalog carries that ``external_id``;
        with ``allow_missing=False`` callers treat that as not-found.
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
    # NB: ``upsert`` is inherited from ``ItemCrudProtocol`` — do not re-declare
    # here. The earlier stub narrowed input/widened output incompatibly with
    # the base contract (caught by pyright's reportIncompatibleMethodOverride;
    # ref #1359). Concrete implementations (``CatalogService.upsert``) may
    # return ``dict`` rows; consumers should treat the return as
    # ``Feature | List[Feature]`` per the base type and rely on the OGC
    # façade to coerce dict rows into Feature models at the boundary.

    async def get_catalog(
        self,
        catalog_id: str,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
        *,
        hints: FrozenSet[Any] = frozenset(),
    ) -> "Catalog":
        """
        Retrieves the catalog metadata model for a specific language.
        """
        ...

    async def get_catalog_model(
        self,
        catalog_id: str,
        ctx: Optional["DriverContext"] = None,
        *,
        hints: FrozenSet[Any] = frozenset(),
    ) -> Optional["Catalog"]:
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

    async def mark_provisioning_step(
        self,
        catalog_id: str,
        key: str,
        step_status: str = "complete",
        ctx: Optional["DriverContext"] = None,
    ) -> bool:
        """Mark one provisioning-checklist step terminal and re-evaluate readiness (#1175).

        Provisioners call this when their work finishes (``complete``), is not
        applicable for this deployment (``skipped``), or genuinely failed
        (``failed``). When the whole checklist is terminal the catalog flips to
        ``ready`` (all complete/skipped) or ``failed`` (any failed). A catalog
        with no checklist is a no-op returning ``False``.
        """
        ...

    async def drain_pending_checklist_steps(
        self,
        catalog_id: str,
        terminal_status: str = "degraded",
        ctx: Optional["DriverContext"] = None,
    ) -> bool:
        """Mark all still-pending checklist steps terminal and re-evaluate (#1902).

        Structural backstop: called when a provisioning task exits (any path)
        without having marked every step, and by the reconciler for catalogs
        stuck in ``provisioning`` with no live task.

        Steps already in a terminal state are not touched.  ``terminal_status``
        defaults to ``"degraded"`` so the catalog becomes ready; pass
        ``"failed"`` on a hard-failure path.

        Returns ``True`` when at least one step was updated.
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
        ids: Optional[Set[str]] = None,
    ) -> List["Catalog"]:
        """
        Lists all catalogs.

        ``ids`` — restrict results to these catalog ids; applied before
        pagination so that LIMIT/OFFSET are consistent with the filtered
        result set.  ``None`` means no restriction (all catalogs).
        """
        ...

    # === Rename / Alias Operations ===

    async def rename_catalog(
        self,
        internal_id: str,
        new_external_id: str,
        ctx: Optional["DriverContext"] = None,
    ) -> Tuple[str, str]:
        """Rename a catalog's public label (external_id) without touching storage.

        Returns ``(prev_external_id, new_external_id)``.

        Raises:
            CatalogRenameConflictError: if another live catalog already holds
                ``external_id = new_external_id``.
            ValueError: if no live catalog row exists for ``internal_id``.
        """
        ...

    async def rename_collection(
        self,
        catalog_internal_id: str,
        collection_internal_id: str,
        new_external_id: str,
        ctx: Optional["DriverContext"] = None,
    ) -> Tuple[str, str]:
        """Rename a collection's public label (external_id) within a catalog.

        Returns ``(prev_external_id, new_external_id)``.

        Raises:
            CollectionRenameConflictError: if another live collection in the
                same catalog already holds ``external_id = new_external_id``.
            ValueError: if no live collection row exists for the given internal ids.
        """
        ...

