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
Entity stores — pluggable storage for collection-row and catalog-row data.

``CollectionStore`` and ``CatalogStore`` mirror the ``CollectionItemsStore``
architecture:
  - Capability-gated operations (CQL filter, spatial filter, fulltext, etc.)
  - Extensible schema (fields are not fixed — enrichers and users can add
    arbitrary fields)
  - Driver config via ``ConfigsProtocol`` waterfall
  - Multiple backends: PostgreSQL (always available), Elasticsearch (default,
    faster for JSON), or any future backend

Drivers are registered through ``register_plugin`` and routed through
``CollectionRoutingConfig`` / ``CatalogRoutingConfig``.
"""

from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    FrozenSet,
    List,
    Optional,
    Protocol,
    Tuple,
    runtime_checkable,
)

# Re-exported so concrete driver classes can declare ``sla`` as
# ``ClassVar[Optional[DriverSla]]`` alongside their other protocol
# attributes without a separate import of driver_roles.
from dynastore.models.protocols.driver_roles import DriverSla  # noqa: F401

if TYPE_CHECKING:
    from dynastore.modules.storage.storage_location import StorageLocation


class EntityStoreCapability:
    """Well-known capability constants for entity-store drivers.

    Usage::

        capabilities: FrozenSet[str] = frozenset({
            EntityStoreCapability.READ, EntityStoreCapability.WRITE,
            EntityStoreCapability.SEARCH,
        })
    """

    READ = "read"
    WRITE = "write"
    SEARCH = "search"                    # keyword/fulltext search (q parameter) — umbrella grade
    # --- Search grades (additive refinements of SEARCH) ---
    # A driver declares whichever grades it genuinely supports; routing matches
    # query intent to capability.  See role-based driver plan §Routing.
    SEARCH_FULLTEXT = "search_fulltext"  # ES / OpenSearch-style keyword + fulltext
    SEARCH_VECTOR = "search_vector"      # Vertex AI / pgvector / Qdrant / vector-db kNN
    SEARCH_EXACT = "search_exact"        # primary-key / id lookup only (e.g. PG pk)

    # --- Transform (role-based driver plan) ---
    TRANSFORM = "transform"              # driver computes / enriches an envelope lazily

    CQL_FILTER = "cql_filter"            # CQL2-JSON/Text filter support
    SOFT_DELETE = "soft_delete"          # mark deleted but retain
    SPATIAL_FILTER = "spatial_filter"    # filter by extent bbox / geo_shape
    AGGREGATION = "aggregation"          # faceted counts, stats on metadata fields
    PHYSICAL_ADDRESSING = "physical_addressing"  # driver exposes location()
    BULK_EXPORT = "bulk_export"          # driver can stream whole partitions (used by BACKUP role)

    # --- Query fallback ---
    QUERY_FALLBACK_SOURCE = "query_fallback_source"
    # Driver serves as the authoritative fallback for metadata storage.
    # Callers that write to PG directly then sync to other drivers skip any
    # driver that declares this capability (it was already written).


@runtime_checkable
class CollectionStore(Protocol):
    """Pluggable storage abstraction for collection metadata.

    Each driver provides CRUD + search operations for metadata using
    a specific backend.  The ``driver_id`` is used by routing config
    to select the active metadata driver for a catalog.

    ``capabilities`` declares what the driver supports.  Callers check
    membership before invoking capability-gated methods.

    Drivers that own only a slice of the payload (e.g. STAC PG drivers
    persisting STAC-extension columns) declare a sub-Capability Protocol
    owned by their extension (e.g. ``StacCollectionEntityStoreCapability``
    in ``extensions/stac/protocols.py``); consumers dispatch via
    ``isinstance`` against the sub-Capability, not against a string tag
    or enum value on this base Protocol.

    ``sla`` is a **mandatory** per-class SLA when ``EntityStoreCapability.TRANSFORM``
    is declared — a transform without an SLA can quietly tax the hot path.
    Non-transform drivers may leave it ``None``.  Per-entry SLAs on
    ``OperationDriverEntry`` override this class-level default.

    ``description`` is a ``ClassVar[LocalizedText]`` defined statically in each
    driver class.  It is returned by the driver discovery API and must not be
    stored in the database.
    """

    capabilities: FrozenSet[str]
    # sla: ClassVar[Optional[DriverSla]]   — mandatory when TRANSFORM is declared.
    # description: ClassVar[LocalizedText] — declared in each concrete driver class.

    async def get_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Read collection metadata by ID.

        Returns a dict with STAC/OGC metadata fields (title, description,
        extent, etc.) or None if no metadata exists for this collection.

        Args:
            context: Optional request-scoped dict (authenticated user,
                request headers, driver-specific hints).  Drivers that
                don't need context ignore this kwarg.
        """
        ...

    async def upsert_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Write or update collection metadata.

        Drivers handle merge semantics internally (INSERT ON CONFLICT for PG,
        index/update for ES, etc.).
        """
        ...

    async def delete_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Delete collection metadata.

        Args:
            soft: If True and ``EntityStoreCapability.SOFT_DELETE`` is supported,
                mark as deleted but retain for recovery.
        """
        ...

    async def search_metadata(
        self,
        catalog_id: str,
        *,
        q: Optional[str] = None,
        bbox: Optional[List[float]] = None,
        datetime_range: Optional[str] = None,
        filter_cql: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Search/list collection metadata.

        Args:
            q: Fulltext keyword search (requires ``SEARCH``).
            bbox: Spatial filter on extent (requires ``SPATIAL_FILTER``).
            datetime_range: Temporal filter on extent.
            filter_cql: CQL2-JSON filter dict (requires ``CQL_FILTER``).
            limit: Maximum results.
            offset: Skip count.
            context: Optional request-scoped dict (see ``get_metadata``).

        Returns:
            Tuple of (results_list, total_count).
        """
        ...

    async def get_driver_config(
        self,
        catalog_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Any:
        """Fetch driver-specific config from the config waterfall."""
        ...

    async def is_available(self) -> bool:
        """Health check — used for fallback logic on first resolution."""
        ...

    async def location(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> "StorageLocation":
        """Return typed physical storage coordinates for this metadata collection.

        Drivers advertising ``EntityStoreCapability.PHYSICAL_ADDRESSING`` MUST
        implement this.  Parallel to ``CollectionItemsStore.location()``.
        """
        ...

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Ensure backing storage exists for this driver (idempotent).

        Mirrors ``CollectionItemsStore.ensure_storage`` and ``AssetStore.ensure_storage``
        — provisioning is a uniform driver concern across the three store
        protocols.  Called by routing-config apply handlers and by the
        collection-creation flow when this driver is referenced for a catalog.

        Drivers with no provisioning work (e.g. transform-only) inherit a no-op
        from :class:`TransformOnlyCollectionStoreMixin`.
        """
        ...


# ---------------------------------------------------------------------------
# Helper mixin: default-raising stubs for TRANSFORM-only drivers
# ---------------------------------------------------------------------------


class TransformOnlyCollectionStoreMixin:
    """Default stubs for drivers that only implement ``TRANSFORM``.

    A TRANSFORM-only driver contributes a partial envelope via its
    ``get_metadata`` method (returning e.g. ``{"bq_stats": ...}``) and
    should never be routed as a ``READ`` / ``WRITE`` / ``SEARCH``
    Primary.  Implementers subclass this mixin to get loud
    ``NotImplementedError`` stubs for the non-TRANSFORM methods so a
    routing misconfiguration is detectable at the first call rather
    than producing silently wrong data.

    Use:

    .. code-block:: python

        class MyTransformDriver(TransformOnlyCollectionStoreMixin):
            capabilities: FrozenSet[str] = frozenset({EntityStoreCapability.TRANSFORM})
            sla: ClassVar[DriverSla] = DriverSla(timeout_ms=2000, on_timeout="degrade")

            async def get_metadata(self, catalog_id, collection_id, *, context=None, db_resource=None):
                return {"my_partial": ...}

    Structural-typing note: the driver still satisfies
    :class:`CollectionStore` because every method exists
    (either via the mixin's raising stubs or the subclass's own
    implementation).  Callers check ``capabilities`` before invoking a
    method; if they don't and the method is unimplemented, the raise
    is intentional and informative.
    """

    async def upsert_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        raise NotImplementedError(
            f"{type(self).__name__} is TRANSFORM-only; route writes to a Primary driver."
        )

    async def delete_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> None:
        raise NotImplementedError(
            f"{type(self).__name__} is TRANSFORM-only; route deletes to a Primary driver."
        )

    async def search_metadata(
        self,
        catalog_id: str,
        *,
        q: Optional[str] = None,
        bbox: Optional[List[float]] = None,
        datetime_range: Optional[str] = None,
        filter_cql: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        raise NotImplementedError(
            f"{type(self).__name__} is TRANSFORM-only; route searches to an Indexer "
            f"or Primary with SEARCH capability."
        )

    async def get_driver_config(
        self,
        catalog_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Any:
        """TRANSFORM drivers typically have no driver-specific config waterfall entry."""
        return None

    async def location(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> "StorageLocation":
        raise NotImplementedError(
            f"{type(self).__name__} does not declare PHYSICAL_ADDRESSING."
        )

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """No-op: TRANSFORM-only drivers have no backing storage to provision."""
        return None


# ---------------------------------------------------------------------------
# Catalog-tier metadata protocol — mirror of CollectionStore
# ---------------------------------------------------------------------------


@runtime_checkable
class CatalogStore(Protocol):
    """Pluggable storage abstraction for catalog-level metadata.

    **Status**: vocabulary introduced by M1a, no concrete implementer yet.
    First consumers land in M2 (``CatalogMetadataCorePostgresDriver``,
    ``CatalogMetadataStacPostgresDriver``) — see plan §M2.  Introduced
    early so ``CatalogRoutingConfig`` can reference it at its apply
    handler without a forward declaration.

    Mirrors :class:`CollectionStore` but scoped to the catalog tier.
    Drivers own a ``domain`` (CORE / STAC / future) and stamp ``updated_at``
    on writes so that INDEX / BACKUP drivers can use it as their freshness
    token (see role-based driver plan §Freshness contract — tentative).

    Catalog-level metadata includes the Records-minimum envelope fields
    (title, description, keywords, license, extra_metadata) and any
    domain-specific payload owned by an extension (STAC conforms_to, links,
    etc.).  Per-domain tables: ``catalog.catalog_core`` (always),
    ``catalog.catalog_stac`` (STAC extension only).

    ``capabilities`` — same semantics as :class:`CollectionStore`.
    ``sla``           — mandatory when ``EntityStoreCapability.TRANSFORM``.
    """

    capabilities: FrozenSet[str]
    # sla: ClassVar[Optional[DriverSla]]  — mandatory when TRANSFORM is declared.

    async def get_catalog_metadata(
        self,
        catalog_id: str,
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Read catalog-level metadata for this driver's domain.

        Returns ``None`` if no metadata exists for this catalog in this domain.
        """
        ...

    async def upsert_catalog_metadata(
        self,
        catalog_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Write or update catalog-level metadata for this driver's domain.

        The driver stamps ``updated_at`` as part of the transaction; it is the
        canonical freshness token used by INDEX / BACKUP propagation (see
        role-based driver plan §Freshness contract).
        """
        ...

    async def delete_catalog_metadata(
        self,
        catalog_id: str,
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Delete catalog-level metadata for this driver's domain.

        Args:
            soft: If True and ``EntityStoreCapability.SOFT_DELETE`` is supported,
                mark as deleted but retain for recovery.  Soft-delete still
                bumps ``updated_at`` so a tombstone event can be emitted.
        """
        ...

    async def get_driver_config(
        self,
        catalog_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Any:
        """Fetch driver-specific config from the config waterfall."""
        ...

    async def is_available(self) -> bool:
        """Health check — used for fallback logic on first resolution."""
        ...
