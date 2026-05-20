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
Asset management protocol definitions.

``AssetsProtocol`` is the backend-agnostic contract for all asset CRUD
operations.  SQL-backed modules implement it via ``AssetService``; future
NoSQL or object-storage backends implement the same interface.

The protocol is split into two concerns:

1. **CRUD** — ``get_asset``, ``list_assets``, ``create_asset``,
   ``update_asset``, ``delete_asset``, ``delete_assets``, ``search_assets``.

2. **Reference tracking** — ``add_asset_reference``,
   ``remove_asset_reference``, ``list_asset_references``.
   Used by driver modules to declare dependencies on assets and to control
   whether hard-deletion is blocked (``cascade_delete=False``) or allowed
   (``cascade_delete=True``).

Quick reference
~~~~~~~~~~~~~~~
::

    from dynastore.modules import get_protocol
    from dynastore.models.protocols import AssetsProtocol

    assets = get_protocol(AssetsProtocol)

    # Create
    asset = await assets.create_asset(
        catalog_id="my_catalog",
        asset=AssetCreate(asset_id="scene_001", filename="scene.tif"),
    )

    # Read
    asset = await assets.get_asset(asset_id="scene_001", catalog_id="my_catalog")

    # Search (with metadata filter)
    results = await assets.search_assets(
        catalog_id="my_catalog",
        filters=[AssetFilter(field="metadata.sensor", op=FilterOperator.EQ, value="OLI-2")],
        limit=50,
    )

    # Soft delete (reversible)
    await assets.delete_assets(catalog_id="my_catalog", asset_id="scene_001", hard=False)

    # Hard delete — raises AssetReferencedError (HTTP 409) if blocking refs remain
    await assets.delete_assets(catalog_id="my_catalog", asset_id="scene_001", hard=True)

    # Reference inspection after a 409
    refs = await assets.list_asset_references(
        asset_id="scene_001", catalog_id="my_catalog"
    )
    for r in refs:
        print(r.ref_type, r.ref_id, "blocking:", not r.cascade_delete)
"""

from typing import Protocol, Optional, Any, List, Dict, runtime_checkable, TYPE_CHECKING

if TYPE_CHECKING:
    from dynastore.modules.catalog.asset_service import (
        Asset,
        AssetBase,
        AssetUploadDefinition,
        AssetUpdate,
        AssetReference,
        AssetReferenceType,
    )
    from dynastore.models.query_builder import AssetFilter
    from dynastore.models.driver_context import DriverContext  # noqa: F401


@runtime_checkable
class AssetsProtocol(Protocol):
    """
    Protocol for asset management operations, enabling decoupled access to asset
    CRUD operations without direct dependency on AssetManager.

    This protocol is backend-agnostic: implementations may use SQL, NoSQL, or
    object-storage as their backing store.  The ``db_resource`` parameter that
    formerly leaked SQLAlchemy internals has been removed from the contract;
    SQL-backed implementations keep it as an *internal* optional kwarg.
    """

    async def get_asset(
        self,
        asset_id: str,
        catalog_id: str,
        collection_id: Optional[str] = None,
    ) -> Optional["Asset"]:
        """
        Retrieves an asset by ID.

        Args:
            asset_id: The asset ID to retrieve.
            catalog_id: The catalog ID containing the asset.
            collection_id: Optional collection ID for scoping.

        Returns:
            Asset model instance, or ``None`` if not found.
        """
        ...

    async def list_assets(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
    ) -> List["Asset"]:
        """
        Lists assets in a catalog/collection with offset-based pagination.

        Args:
            catalog_id: The catalog ID to list assets from.
            collection_id: Optional collection ID for scoping.
            limit: Maximum number of assets to return (1–100).
            offset: Number of assets to skip.

        Returns:
            List of Asset model instances.
        """
        ...

    async def create_asset(
        self,
        catalog_id: str,
        asset: "AssetBase",
        collection_id: Optional[str] = None,
        ctx: Optional["DriverContext"] = None,
    ) -> "Asset":
        """
        Creates a new asset.

        Args:
            asset: Asset definition to create.
            catalog_id: The catalog ID to create the asset in.
            collection_id: Optional collection ID for scoping.

        Returns:
            Created Asset model instance.
        """
        ...

    async def update_asset(
        self,
        asset_id: str,
        update: "AssetUpdate",
        catalog_id: str,
        collection_id: Optional[str] = None,
    ) -> "Asset":
        """
        Updates an existing asset's mutable fields (currently: ``metadata``).

        Args:
            asset_id: The asset ID to update.
            update: Asset update data.
            catalog_id: The catalog ID containing the asset.
            collection_id: Optional collection ID for scoping.

        Returns:
            Updated Asset model instance.
        """
        ...

    async def delete_asset(
        self,
        asset_id: str,
        catalog_id: str,
        collection_id: Optional[str] = None,
    ) -> None:
        """
        Soft-deletes a single asset.

        Args:
            asset_id: The asset ID to delete.
            catalog_id: The catalog ID containing the asset.
            collection_id: Optional collection ID for scoping.
        """
        ...

    async def delete_assets(
        self,
        catalog_id: str,
        asset_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        hard: bool = False,
        propagate: bool = False,
    ) -> int:
        """
        Deletes assets matching criteria.

        Soft deletes (``hard=False``) set ``status='deleted'``; hard deletes
        remove the row entirely.  For assets with ``owned_by`` set, hard
        deletion is blocked
        (``AssetReferencedError``) when non-cascading references remain.

        Args:
            catalog_id: The catalog ID.
            asset_id: Optional asset ID to target a specific asset.
            collection_id: Optional collection ID to scope deletion.
            hard: If ``True``, physically removes the row.
            propagate: If ``True``, propagates deletion to linked storage/features.

        Returns:
            Number of assets deleted.

        Raises:
            AssetReferencedError: When hard-deleting an owned asset that still has
                blocking (``cascade_delete=False``) references.
        """
        ...

    async def search_assets(
        self,
        catalog_id: str,
        filters: List["AssetFilter"],
        limit: int = 10,
        offset: int = 0,
        collection_id: Optional[str] = None,
        all_collections: bool = False,
    ) -> List["Asset"]:
        """
        Searches for assets using a list of filters.

        Supports metadata JSONB path notation (e.g. ``metadata.provider.name``).

        Args:
            catalog_id: The catalog ID to search in.
            filters: List of ``AssetFilter`` instances to apply.
            limit: Maximum number of assets to return.
            offset: Number of assets to skip.
            collection_id: Optional collection ID for scoping (catalog-tier when None).
            all_collections: When True, search every asset under the catalog
                across all collections and the catalog tier; ``collection_id``
                is ignored. When False (default), ``collection_id=None`` scopes
                to catalog-tier assets only.

        Returns:
            List of matching Asset model instances.
        """
        ...

    async def ensure_asset_cleanup_trigger(
        self,
        schema: str,
        table: str,
        ctx: Optional["DriverContext"] = None,
    ) -> None:
        """
        Ensures the ``trg_asset_cleanup`` DB trigger is installed on *table*.

        **PostgreSQL-specific** — called by SQL-backed drivers after creating
        tables that reference assets, so that row-level cascade cleanup fires
        automatically on DELETE.  Non-SQL implementations (e.g. when
        Elasticsearch is the primary asset store) should implement this as a
        no-op.

        Args:
            schema: PostgreSQL schema name.
            table: Table name to attach the trigger to.
        """
        ...

    # -------------------------------------------------------------------------
    # Asset reference tracking
    # -------------------------------------------------------------------------

    async def add_asset_reference(
        self,
        asset_id: str,
        catalog_id: str,
        ref_type: "AssetReferenceType",
        ref_id: str,
        cascade_delete: bool = True,
        ctx: Optional["DriverContext"] = None,
    ) -> "AssetReference":
        """
        Registers a dependency from *ref_id* (e.g. a collection or table) on
        this asset.

        ``cascade_delete=True``  — informational; the referencing driver handles
        its own cleanup (e.g. via PostgreSQL trigger).  Does **not** block
        hard-deletion.

        ``cascade_delete=False`` — protective; hard-deletion of the asset is
        **blocked** (HTTP 409 / ``AssetReferencedError``) until this reference
        is explicitly removed.

        The operation is idempotent — calling it again with the same
        ``(asset_id, catalog_id, ref_type, ref_id)`` tuple updates
        ``cascade_delete`` in place rather than creating a duplicate.

        Args:
            asset_id: The referenced asset ID.
            catalog_id: Catalog scope.
            ref_type: Pluggable enum value identifying the reference kind
                      (e.g. ``CoreAssetReferenceType.COLLECTION``).
            ref_id: Owner-scoped identifier (collection_id, table name, …).
            cascade_delete: Whether the owner can self-clean on asset deletion.

        Returns:
            The created or updated ``AssetReference`` record.

        Examples::

            from dynastore.modules.catalog.models import CoreAssetReferenceType

            # Ingestion: informational, does NOT block deletion
            await assets.add_asset_reference(
                asset_id="stations_2025",
                catalog_id="field_data",
                ref_type=CoreAssetReferenceType.COLLECTION,
                ref_id="weather_stations",
                cascade_delete=True,
            )

            # DuckDB collection: protective, BLOCKS hard-deletion
            await assets.add_asset_reference(
                asset_id="stations_parquet",
                catalog_id="field_data",
                ref_type=DuckDbReferenceType.TABLE,   # "duckdb:table"
                ref_id="weather_stations_duckdb",
                cascade_delete=False,
            )
        """
        ...

    async def remove_asset_reference(
        self,
        asset_id: str,
        catalog_id: str,
        ref_type: "AssetReferenceType",
        ref_id: str,
    ) -> None:
        """
        Removes a previously registered reference.

        **Warning:** removing a ``cascade_delete=False`` reference unblocks
        hard-deletion even if the referencing driver has not cleaned up its
        data yet.  Only call this after the referencing entity (collection,
        DuckDB table, Iceberg table, …) has been dropped.

        Args:
            asset_id: The referenced asset ID.
            catalog_id: Catalog scope.
            ref_type: Reference type discriminator.
            ref_id: Owner-scoped identifier.

        Example::

            # Called from DuckDB collection teardown:
            await assets.remove_asset_reference(
                asset_id="stations_parquet",
                catalog_id="field_data",
                ref_type=DuckDbReferenceType.TABLE,
                ref_id="weather_stations_duckdb",
            )
            # Now hard-delete is permitted (if no other blocking refs remain):
            await assets.delete_assets(
                catalog_id="field_data",
                asset_id="stations_parquet",
                hard=True,
            )
        """
        ...

    async def list_asset_references(
        self,
        asset_id: str,
        catalog_id: str,
    ) -> List["AssetReference"]:
        """
        Returns all active references for the given asset.

        Useful for diagnosing why a hard-delete was rejected (HTTP 409): any
        entry with ``cascade_delete=False`` is a blocking reference.

        Args:
            asset_id: The asset ID to query.
            catalog_id: Catalog scope.

        Returns:
            List of ``AssetReference`` records (may be empty).

        Example::

            refs = await assets.list_asset_references(
                asset_id="stations_parquet",
                catalog_id="field_data",
            )
            blocking = [r for r in refs if not r.cascade_delete]
            if blocking:
                print("Cannot hard-delete — blocking references:")
                for r in blocking:
                    print(f"  {r.ref_type}:{r.ref_id} (registered {r.created_at})")
        """
        ...
