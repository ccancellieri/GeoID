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
Collection Storage Driver Protocol — entity-level storage abstraction.

Drivers implement this protocol to provide read/write/delete operations
on collections using any backend (PostgreSQL, Elasticsearch, DuckDB,
Iceberg, Neo4j, Spanner, BigQuery, Redis, static files, etc.).

Design principles:
  - Entity-level, not SQL-level: drivers work with typed Pydantic models.
  - Streaming-first (O(1) memory): ``read_entities`` returns ``AsyncIterator``.
  - Query via ``QueryRequest``: structured, validated queries with typed
    filters, sorts, and field selections.
  - Any driver can be primary or secondary — the framework is driver-agnostic.
  - Capabilities: drivers declare what they support via ``FrozenSet[str]``
    using ``Capability`` string constants or any custom string.
  - Unified soft delete: ``delete_entities(soft=True)`` and
    ``drop_storage(soft=True)`` for recoverable operations.

Type contracts:
  - Write input: ``Feature | FeatureCollection | Dict[str, Any] | List[Dict]``
    (same union accepted by ``ItemCrudProtocol.upsert``).
  - Read output: ``AsyncIterator[Feature]`` — typed Pydantic models, not raw dicts.
  - Query input: ``QueryRequest`` — structured, not opaque dicts.
"""

from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    ClassVar,
    Dict,
    FrozenSet,
    List,
    Optional,
    Protocol,
    Union,
    runtime_checkable,
)


from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.protocols.teardown_lane import TeardownLane
from dynastore.models.query_builder import QueryRequest

if TYPE_CHECKING:
    from dynastore.models.protocols.field_definition import FieldDefinition
    from dynastore.modules.storage.computed_fields import ComputedField
    from dynastore.modules.storage.hints import Hint
    from dynastore.modules.storage.storage_location import StorageLocation


class Capability:
    """Well-known capability constants. Drivers may declare any string.

    Usage::

        capabilities: FrozenSet[str] = frozenset({
            Capability.READ, Capability.WRITE, Capability.STREAMING,
        })

    Callers check membership::

        if Capability.WRITE not in driver.capabilities:
            raise ReadOnlyDriverError(...)
    """

    # --- I/O ---
    READ = "read"
    READ_ONLY = "read_only"
    WRITE = "write"
    STREAMING = "streaming"
    EXPORT = "export"

    # NB. Read-flavour members (``FULLTEXT``, ``SPATIAL_FILTER``,
    # ``ATTRIBUTE_FILTER``, ``AGGREGATION``, ``COUNT``, ``STATISTICS``,
    # ``SORT``, ``GROUP_BY``) were retired in PR #3b of the routing
    # cleanup.  They were not structural facts about the driver — they
    # were per-request flavours of READ/search that callers asked for,
    # and they've moved to the canonical ``Hint`` catalogue
    # (``modules/storage/hints.py``).  Drivers express which flavours
    # they serve via ``supported_hints``; routing dispatches via
    # ``get_items_search_driver(catalog_id, hints=frozenset({Hint.AGGREGATION}))``.

    # --- Data management ---
    SOFT_DELETE = "soft_delete"
    TIME_TRAVEL = "time_travel"
    VERSIONING = "versioning"
    SCHEMA_EVOLUTION = "schema_evolution"
    SNAPSHOTS = "snapshots"

    # --- Per-feature processing ---
    GEOSPATIAL = "geospatial"      # bbox, centroid, geometry validation/fix per row
    SPATIAL_INDEX = "spatial_index"  # H3/S2 indexing per row

    # --- Per-feature tracking ---
    ASSET_TRACKING = "asset_tracking"      # tracks asset_id per feature (source provenance)
    SOURCE_REFERENCE = "source_reference"  # provides source reference per feature

    # --- Cross-driver composition ---
    ENRICHMENT = "enrichment"  # can provide filter keys + extra attrs for cross-driver join

    # --- Write-time identity & versioning ---
    EXTERNAL_ID_TRACKING = "external_id_tracking"  # driver tracks external_id per feature
    TEMPORAL_VALIDITY = "temporal_validity"          # driver tracks valid_from / valid_to

    # --- Field constraints (FieldDefinition.required / .unique) ---
    REQUIRED_ENFORCEMENT = "required_enforcement"  # driver enforces NOT-NULL at storage layer
    UNIQUE_ENFORCEMENT = "unique_enforcement"      # driver enforces UNIQUE at storage layer

    # --- Analytics & introspection ---
    INTROSPECTION = "introspection"  # schema discovery (field names, types)

    # --- Stored statistics introspection ---
    STORED_STATS = "stored_stats"
    # Driver can enumerate the stored, query-addressable per-feature
    # statistics it materialises for a collection, together with their
    # physical field names. Backend-neutral discovery point for callers
    # (e.g. tile rendering) that want to bind to a stored statistic
    # without hardcoding any driver-specific column/table shape.

    # --- Storage location & addressing ---
    PHYSICAL_ADDRESSING = "physical_addressing"
    # Driver exposes stable physical coordinates via location().
    # Replaces isinstance(driver, PostgresDriver) patterns that read
    # physical_schema / physical_table directly.

    # --- Soft-delete guarantees ---
    SOFT_DELETE_ATOMIC = "soft_delete_atomic"
    # Driver guarantees a single-statement atomic soft-delete (no read-modify-write).
    # A subset of SOFT_DELETE — callers that need atomicity check this constant.

    # --- Query fallback ---
    QUERY_FALLBACK_SOURCE = "query_fallback_source"
    # Driver can serve any query as a fallback when the primary driver
    # lacks the required capability.  Router uses this to pick an
    # alternative without hardcoding a specific driver class.

    # --- Bulk ingest ---
    BULK_COPY = "bulk_copy"
    # Driver supports a fast-path bulk ingest channel (e.g. COPY for PG,
    # append for Iceberg).  Callers check this before invoking bulk_copy().

    # --- Canonical write-id replay ---
    WRITE_ID_CHUNK_READ = "write_id_chunk_read"
    # Driver can page canonical primary rows by ``system.write_id`` so async
    # secondary-write drains can rehydrate one logical write chunk at a time.

    # --- Tenant isolation ---
    TENANT_ISOLATED = "tenant_isolated"
    # Driver maintains a per-catalog physical storage unit (ES index,
    # PG schema, bucket, …) that must be provisioned proactively when a
    # tenant flips its privacy policy.  Replaces isinstance(driver,
    # SomePrivateDriver) callsites that target the privacy-cascade
    # ensure_storage hook.
    #
    # KEEP IN SYNC: the string value is mirrored on
    # ``entity_store.EntityStoreCapability.TENANT_ISOLATED``. Both enum values
    # are read when discovering per-tenant private drivers; a rename in only
    # one place silently breaks the other branch. Test:
    # ``tests/dynastore/modules/storage/unit/test_routing_privacy_cascade.py
    # ::test_tenant_isolated_string_matches``.


@runtime_checkable
class CollectionItemsStore(Protocol):
    """Entity-level storage abstraction for collection data.

    Each driver provides CRUD + lifecycle operations for a specific backend.
    The ``driver_id`` is used by routing config to select the active driver
    for a given collection.

    Business logic dispatches on driver *class* (``isinstance(driver, X)``)
    rather than a string family tag; ``driver_id`` is only for routing
    resolution.

    ``capabilities`` declares what the driver supports as a ``FrozenSet[str]``
    using ``Capability`` constants and/or custom strings.

    ``preferred_for`` declares which routing hints this driver is optimized for.
    The router uses this for auto-selection when no explicit hint mapping exists.

    ``supported_hints`` declares which hints this driver accepts in routing config.
    Config validation rejects entries with hints not in this set.

    ``description`` is a ``ClassVar[LocalizedText]`` defined statically in each
    driver class.  It is returned by the driver discovery API
    (``GET /storage/drivers``) and must not be stored in the database.
    """

    teardown_lane: ClassVar[TeardownLane] = TeardownLane.ASYNC_CASCADE

    capabilities: FrozenSet[str]
    preferred_for: "FrozenSet[Hint]"
    supported_hints: "FrozenSet[Hint]"
    preferred_chunk_size: int  # 0 = no preference; >0 = max items per write_entities call
    # description: ClassVar[LocalizedText]  — declared in each concrete driver class

    def is_available(self) -> bool:
        """Health check — returning False hides the driver from discovery."""
        ...

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]], List[Feature]],
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        """Write/upsert entities. Returns written Feature models.

        Args:
            entities: One or more features to write.
            context: Runtime write context — carries ingestion-pipeline metadata
                that is not part of the feature payload itself:

                - ``asset_id``             — source asset URN (from ingestion)
                - ``external_id_override`` — explicit external_id bypassing field extraction
                - ``valid_from``           — validity range start (datetime or ISO-8601 str)
                - ``valid_to``             — validity range end (None = open-ended)

                Drivers that declare ``Capability.EXTERNAL_ID_TRACKING`` or
                ``Capability.TEMPORAL_VALIDITY`` MUST honour these keys and apply
                the ``ItemsWritePolicy`` (class_key ``"items_write_policy"``) retrieved
                from ``ConfigsProtocol``.

            db_resource: Optional connection/transaction to reuse (PG only).
        """
        ...

    def read_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        entity_ids: Optional[List[str]] = None,
        request: Optional[QueryRequest] = None,
        context: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> AsyncIterator[Feature]:
        """Stream entities as an async iterator (O(1) memory).

        Args:
            entity_ids: Fetch specific entities by ID.
            request: Structured query with filters, sorts, field selections.
            context: Driver-specific query context. Each driver documents what
                keys it expects (input) and what it publishes to
                ``FeaturePipelineContext`` (output). See driver docstrings.
            limit: Maximum entities to return.
            offset: Number of entities to skip.
            db_resource: Optional connection/transaction to reuse.

        Yields:
            ``Feature`` instances — typed Pydantic models.
        """
        ...

    async def delete_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> int:
        """Delete entities by ID. Returns count of entities deleted.

        Args:
            soft: If True, mark as deleted but retain data for recovery.
                Raises ``SoftDeleteNotSupportedError`` if the driver
                lacks ``Capability.SOFT_DELETE``.
        """
        ...

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Ensure backing storage exists (create table/index/bucket/etc.).

        Each driver resolves its own connection and configuration internally
        (e.g. via ``ConfigsProtocol``). Drivers may accept driver-specific
        keyword arguments (e.g. ``physical_table``, ``layer_config``,
        ``db_resource`` for PostgreSQL).

        Args:
            catalog_id: The catalog that owns the collection.
            collection_id: Optional collection within the catalog.
            **kwargs: Driver-specific options.
        """
        ...

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
        **kwargs: Any,
    ) -> None:
        """Remove backing storage for a catalog or collection.

        Contract: this is the exact inverse of ``ensure_storage`` — it tears
        down every physical artifact this driver provisioned (tables and
        sidecar tables, indexes, files, namespaces). It MUST be idempotent
        (calling it twice, or when nothing was provisioned, is a no-op) and
        MUST NOT call back into catalog services: registry rows, driver
        configs and metadata fan-out are owned by ``CollectionService``.

        Drivers may accept driver-specific keyword arguments, mirroring
        ``ensure_storage`` (e.g. ``db_resource``, ``physical_table``,
        ``physical_schema`` for PostgreSQL so teardown can join the caller's
        transaction).

        Args:
            catalog_id: The catalog that owns the collection.
            collection_id: Optional collection within the catalog.
            soft: If True, mark as deleted but retain data for recovery.
                Raises ``SoftDeleteNotSupportedError`` if the driver
                lacks ``Capability.SOFT_DELETE``.
            **kwargs: Driver-specific options.
        """
        ...

    async def export_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        format: str = "parquet",
        target_path: str = "",
        db_resource: Optional[Any] = None,
    ) -> str:
        """Export entities to an interchange format. Returns path to exported data.

        Can be wrapped by ``TaskProtocol`` for async execution on task runners.
        """
        ...

    # Collection-metadata CRUD is no longer part of the
    # CollectionItemsStore protocol.  The M2.5 hard cut moved ownership
    # of collection metadata to the CollectionStore protocol; the
    # PG-tier implementer is now the composition wrapper
    # CollectionPostgresqlDriver (PR 1e step 3b), which fans CRUD across
    # collection_core + collection_stac sidecars internally.  Routing happens
    # via :mod:`dynastore.modules.catalog.collection_router`.
    # Storage drivers now handle items-only; metadata is out-of-band.

    async def count_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        request: Optional[Any] = None,
        db_resource: Optional[Any] = None,
    ) -> int:
        """Count entities matching the optional query request.

        Drivers self-declare COUNT support via ``Hint.COUNT`` in
        ``supported_hints``.  Returns 0 if collection has no data.
        """
        ...

    async def introspect_schema(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> "List[FieldDefinition]":
        """Discover the field schema for a collection.

        Requires ``Capability.INTROSPECTION``.

        Returns a list of ``FieldDefinition`` instances::

            [FieldDefinition(name="temperature", data_type="numeric"),
             FieldDefinition(name="station_id", data_type="string"), ...]

        Field types are driver-agnostic strings: ``"string"``, ``"numeric"``,
        ``"integer"``, ``"boolean"``, ``"datetime"``, ``"geometry"``, ``"json"``,
        ``"array"``, ``"unknown"``.
        """
        ...

    async def compute_extents(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Compute spatial and temporal extents for a collection.

        Drivers self-declare STATISTICS support via ``Hint.STATISTICS``
        in ``supported_hints``.

        Returns::

            {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]}
            }

        Returns None if the collection has no data or extents cannot be computed.
        """
        ...

    async def aggregate(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        aggregation_type: str,
        field: Optional[str] = None,
        request: Optional[Any] = None,
        db_resource: Optional[Any] = None,
    ) -> Any:
        """Run an aggregation query on the collection.

        Drivers self-declare AGGREGATION support via ``Hint.AGGREGATION``
        in ``supported_hints``.

        Common ``aggregation_type`` values:
          - ``"terms"``: unique values + counts for ``field``
          - ``"stats"``: min/max/avg/sum for ``field``
          - ``"datetime_range"``: earliest/latest datetime
          - ``"bbox"``: bounding box of all geometries

        The ``request`` parameter optionally filters the aggregation scope.
        Return type varies by ``aggregation_type``.
        """
        ...

    async def restore_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        db_resource: Optional[Any] = None,
    ) -> int:
        """Restore soft-deleted entities by ID. Returns count of entities restored.

        Requires ``Capability.SOFT_DELETE``.
        Raises ``SoftDeleteNotSupportedError`` if the driver lacks the capability.
        """
        ...

    async def rename_storage(
        self,
        catalog_id: str,
        old_collection_id: str,
        new_collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Rename storage for a collection (e.g. rename PG table, ES index).

        Not all drivers support renaming. Drivers that do not support it
        should raise ``NotImplementedError``.
        """
        ...

    async def get_entity_fields(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        entity_level: str = "item",
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Return field definitions for entities at the given level.

        Requires ``Capability.INTROSPECTION``.

        Args:
            catalog_id: The catalog scope.
            collection_id: The collection scope (required for ``item`` and
                ``asset`` levels; None for ``catalog`` and ``collection`` levels).
            entity_level: One of ``"catalog"``, ``"collection"``, ``"item"``,
                ``"asset"`` (see ``EntityLevel`` enum in
                ``models.protocols.field_definition``).
            db_resource: Optional connection/transaction to reuse.

        Returns a mapping of ``field_name`` →
        ``dynastore.models.protocols.field_definition.FieldDefinition``.
        Each driver maps its native type system to the common FieldDefinition
        model (name, data_type, capabilities, etc.).
        """
        ...

    async def get_driver_config(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        db_resource: Optional[Any] = None,
    ) -> Any:
        """Fetch this driver's typed config from the config waterfall.

        Each driver resolves its own class_key (e.g. ``ItemsPostgresqlDriverConfig``,
        ``ItemsElasticsearchDriverConfig``) and returns the matching
        ``CollectionDriverConfig`` subclass.  Returns code defaults when no
        config has been stored.

        This method replaces driver-specific standalone functions like
        Callers should obtain the driver via routing and call
        ``driver.get_driver_config()`` instead.
        """
        ...

    async def location(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> "StorageLocation":
        """Return the typed physical storage coordinates for this collection.

        Drivers that advertise ``Capability.PHYSICAL_ADDRESSING`` MUST implement
        this method.  Drivers without physical addressing return a placeholder.

        Returns:
            ``StorageLocation`` with ``backend``, ``canonical_uri``, ``identifiers``,
            and ``display_label`` populated from this driver's config for the
            given catalog/collection.
        """
        ...


@runtime_checkable
class WriteIdChunkReadablePrimary(Protocol):
    """Optional primary-driver capability for write-id chunk replays."""

    async def read_active_rows_by_write_id(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        write_id: str,
        limit: int,
        after_geoid: Optional[str] = None,
        db_resource: Optional[Any] = None,
    ) -> tuple[List[Dict[str, Any]], Optional[str]]:
        """Return one active-row page for ``write_id`` ordered by geoid."""
        ...

    async def read_tombstoned_ids_by_write_id(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        write_id: str,
        limit: int,
        after_geoid: Optional[str] = None,
        db_resource: Optional[Any] = None,
    ) -> tuple[List[str], Optional[str]]:
        """Return one tombstoned-geoid page for ``write_id`` ordered by geoid."""
        ...


@runtime_checkable
class StoredStatsProvider(Protocol):
    """Optional capability for drivers that can enumerate the storage-bearing
    per-feature statistics they materialise for a collection."""

    async def stored_stats(
        self, catalog_id: str, collection_id: str, *, db_resource: Any = None,
    ) -> "List[ComputedField]":
        """Return the stored, query-addressable per-feature statistics for
        this collection.

        Each entry is a geometry, JSON-FG place, or attribute statistic
        materialised for the collection — ``kind`` identifies which
        derivation, ``resolved_name`` is the physical field/column name,
        ``storage_mode`` and ``indexed`` describe how it is stored. Empty
        list when the collection stores none.
        """
        ...


class ReadOnlyDriverMixin:
    """Mixin for read-only drivers. Raises ``ReadOnlyDriverError`` on all writes."""

    async def write_entities(self, *args, **kwargs):
        from dynastore.modules.storage.errors import ReadOnlyDriverError
        raise ReadOnlyDriverError(
            f"Driver '{getattr(self, 'driver_id', '?')}' is read-only"
        )

    async def delete_entities(self, *args, **kwargs):
        from dynastore.modules.storage.errors import ReadOnlyDriverError
        raise ReadOnlyDriverError(
            f"Driver '{getattr(self, 'driver_id', '?')}' is read-only"
        )

    async def drop_storage(self, *args, **kwargs):
        from dynastore.modules.storage.errors import ReadOnlyDriverError
        raise ReadOnlyDriverError(
            f"Driver '{getattr(self, 'driver_id', '?')}' is read-only"
        )
