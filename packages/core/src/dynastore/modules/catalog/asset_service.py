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
Asset lifecycle management for the DynaStore catalog.

Overview
--------
An **asset** is a pointer to a file (local path, GCS URI, HTTP URL, S3 key, …)
registered in the catalog database.  Assets are the central linking mechanism:

- **Ingestion tasks** create an asset from a source-file URI, then write
  feature rows into the collection's physical table keyed by ``asset_id``.
- **GCS/S3 uploads** mint a PENDING row up front (``initiate_upload``); the
  storage backend's finalize event then activates that row inline (the GCS
  Pub/Sub HTTP push handler runs the finalize activator in a single
  transaction; future S3 backends will follow the same shape).
- **Direct API calls** create assets immediately via
  ``POST /assets/catalogs/{id}`` or the collection-scoped variant.

Key models
----------
``AssetBase``
    Input/creation shape.  ``owned_by`` marks which storage backend owns the
    underlying file; assets with this field set are protected from hard-deletion
    while non-cascading references remain active.

``Asset``
    Fully persisted asset returned by reads, including ``catalog_id``,
    ``collection_id``, and audit timestamps.

``AssetReference``
    A dependency row in ``{schema}.asset_references`` that links an asset to a
    referencing entity (collection, DuckDB table, Iceberg table, …).  Rows key
    on the asset's immutable ``physical_id`` (UUIDv7) so a rename of
    ``asset_id`` requires no propagation in this table.  The
    ``cascade_delete`` flag controls deletion safety:

    * ``True`` — informational; the referring driver handles its own cleanup
      (e.g. the PostgreSQL ``trg_asset_cleanup`` trigger).  **Does not block**
      hard-deletion.
    * ``False`` — protective; hard-deletion of the asset is **blocked**
      (raises ``AssetReferencedError`` → HTTP 409) until this reference is
      explicitly removed.

Reference type extension
------------------------
Each driver module defines its own ``AssetReferenceType`` subclass so values
are namespaced and type-safe::

    # In dynastore/modules/duckdb/models.py
    from dynastore.models.shared_models import AssetReferenceType

    class DuckDbReferenceType(AssetReferenceType):
        TABLE = "duckdb:table"

    # When creating a DuckDB-backed collection:
    await assets.add_asset_reference(
        asset_id=asset_id,
        catalog_id=catalog_id,
        ref_type=DuckDbReferenceType.TABLE,
        ref_id=table_name,
        cascade_delete=False,   # DuckDB cannot auto-cascade → blocks deletion
    )

    # On collection drop — must happen BEFORE the hard-delete attempt:
    await assets.remove_asset_reference(
        asset_id=asset_id,
        catalog_id=catalog_id,
        ref_type=DuckDbReferenceType.TABLE,
        ref_id=table_name,
    )

Ingestion (PostgreSQL, cascade-safe)
-------------------------------------
::

    from dynastore.modules.catalog.models import CoreAssetReferenceType

    # After successful ingestion the DB trigger already handles row cleanup,
    # so the reference is informational only (cascade_delete=True):
    await asset_manager.add_asset_reference(
        asset_id=asset.asset_id,
        catalog_id=catalog_id,
        ref_type=CoreAssetReferenceType.COLLECTION,
        ref_id=collection_id,
        cascade_delete=True,
        ctx=DriverContext(db_resource=engine),
    )

Upload flow (GCS)
-----------------
::

    from dynastore.modules.storage.router import get_asset_upload_driver

    # Per-catalog UPLOAD routing — see AssetRoutingConfig.operations[UPLOAD].
    # Falls back to first-registered backend if no UPLOAD entry resolves.
    upload = await get_asset_upload_driver(catalog_id="imagery_catalog")
    ticket = await upload.initiate_upload(
        catalog_id="imagery_catalog",
        asset_def=AssetUploadDefinition(
            asset_id="LC09_198030_20251225",
            asset_type=AssetTypeEnum.RASTER,
            metadata={"sensor": "OLI-2"},
        ),
        filename="LC09_L1TP_198030_20251225_02_T1.tif",
        content_type="image/tiff",
        collection_id="landsat_scenes",
    )
    # → ticket.upload_url is a GCS signed resumable PUT URL
    # → PUT file to ticket.upload_url with ticket.headers
    # → GCS fires OBJECT_FINALIZE → Pub/Sub push → inline finalize activator
    #   transitions the PENDING row to ACTIVE in one transaction
    # → poll GET /assets/catalogs/{id}/upload/{ticket.ticket_id}/status

Deletion guard
--------------
Hard-deletion of an ``owned_by`` asset with blocking references raises
``AssetReferencedError`` (caught by the API layer → HTTP 409)::

    # Will succeed — soft delete never checks references:
    await assets.delete_assets(catalog_id, asset_id=id, hard=False)

    # Will raise AssetReferencedError if cascade_delete=False refs remain:
    await assets.delete_assets(catalog_id, asset_id=id, hard=True)

    # Inspect blocking references before retrying:
    refs = await assets.list_asset_references(asset_id=id, catalog_id=catalog_id)
    blocking = [r for r in refs if not r.cascade_delete]
"""

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, List, Literal, Optional, Dict, Any, Union, Callable, Annotated

if TYPE_CHECKING:
    from dynastore.modules.storage.router import ResolvedDriver
    from dynastore.extensions.ogc_models_shared import RenameResponse
from dynastore.tools.cache import cached
from pydantic import BaseModel, Field, ConfigDict, StringConstraints, model_validator

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DDLQuery,
    ResultHandler,
    managed_transaction,
    DbResource,
    DbConnection,
)
from dynastore.modules.catalog.models import AssetReferenceType, EventType
from dynastore.modules.catalog.log_manager import log_info
from dynastore.models.shared_models import Link
from dynastore.models.protocols.assets import AssetsProtocol
from dynastore.models.driver_context import DriverContext
from dynastore.models.query_builder import AssetFilter
from enum import Enum

logger = logging.getLogger(__name__)

# --- Asset-Specific Enums ---


class AssetTypeEnum(str, Enum):
    VECTORIAL = "VECTORIAL"
    RASTER = "RASTER"
    ASSET = "ASSET"


class AssetKind(str, Enum):
    """Discriminates between assets we manage and external references."""

    PHYSICAL = "physical"  # blob in our managed bucket / disk
    VIRTUAL = "virtual"    # external href; we don't manage the bytes


class AssetStatus(str, Enum):
    """Lifecycle states a row passes through."""

    PENDING = "pending"  # row born at upload-create; awaiting OBJECT_FINALIZE
    ACTIVE = "active"    # blob present (physical) or href registered (virtual)
    FAILED = "failed"    # finalize never came / hash mismatch / external 404
    DELETED = "deleted"  # logical delete


class AssetEventType(EventType):
    ASSET_CREATED = "asset_created"
    ASSET_UPDATED = "asset_updated"
    ASSET_DELETED = "asset_deleted"
    ASSET_HARD_DELETED = "asset_hard_deleted"
    ASSET_MAP_LINKED = "asset_map_linked"


# --- Asset Models ---

# Filename validator: no slashes, no NUL, max 255 chars.
Filename = Annotated[
    str,
    StringConstraints(min_length=1, max_length=255, pattern=r"^[^\x00/\\]+$"),
]


class AssetBase(BaseModel):
    """
    Identity and metadata fields shared across creates and reads.

    Identity (``asset_id``, ``filename``, ``href``) is immutable once a row
    becomes ``ACTIVE``; only ``metadata`` can be mutated through ``AssetUpdate``.
    """

    asset_id: str = Field(..., min_length=1, max_length=255,
                          description="Unique logical identifier for the asset.")
    asset_type: AssetTypeEnum = Field(
        default=AssetTypeEnum.ASSET,
        description="Type of the asset. Could be VECTORIAL, RASTER, or generic ASSET.",
        examples=["VECTORIAL", "RASTER", "ASSET"],
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary metadata associated with the asset.",
        examples=[{"owner": "", "provider": ""}],
    )
    owned_by: Optional[str] = Field(
        default=None,
        description=(
            "Identifier of the system that manages the underlying file "
            "(e.g. 'gcs', 'local', 'http'). "
            "Assets with this field set are protected from hard-deletion while "
            "non-cascading references (cascade_delete=False) remain active."
        ),
        examples=["gcs", "local", "http", None],
    )

    model_config = ConfigDict(populate_by_name=True)


class AssetCreate(AssetBase):
    """
    Physical asset create payload — blob lives in a managed bucket / disk.

    Used by upload-create REST routes.  At least one of ``filename`` or
    ``uri`` must be supplied: upload-create flows pass ``filename`` (the blob
    has not landed yet, so there is no URI), while direct registration of an
    already-stored blob passes ``uri`` (e.g. ``gs://bucket/file.zip``) and the
    ``filename`` is derived from the URI tail when omitted. ``filename`` must
    not contain slashes or NUL bytes.

    ``uri`` is also used by internal storage-event callers (GCS / local
    finalize) that already know the storage URI and need to create the row as
    ACTIVE in one shot.
    """

    kind: Literal[AssetKind.PHYSICAL] = AssetKind.PHYSICAL
    filename: Optional[Filename] = Field(
        default=None,
        description=(
            "Display filename (no slashes/NUL). Required unless ``uri`` is "
            "given, in which case it is derived from the URI tail."
        ),
    )
    uri: Optional[str] = Field(
        default=None,
        description=(
            "Pre-resolved storage URI for direct registration of an "
            "already-stored blob (e.g. 'gs://bucket/file.zip') or for "
            "storage-event finalize creates."
        ),
    )

    @model_validator(mode="after")
    def _require_filename_or_uri(self) -> "AssetCreate":
        if not self.filename:
            if not self.uri:
                raise ValueError(
                    "AssetCreate requires either 'filename' or 'uri'."
                )
            # Derive a slash-free filename from the URI tail.
            tail = self.uri.rstrip("/").split("/")[-1]
            if not tail or "\x00" in tail:
                raise ValueError(
                    f"Cannot derive a valid filename from uri {self.uri!r}; "
                    "supply 'filename' explicitly."
                )
            self.filename = tail
        return self


class VirtualAssetCreate(AssetBase):
    """
    Virtual asset create payload — registers an external href without
    bucket presence on our side.
    """

    kind: Literal[AssetKind.VIRTUAL] = AssetKind.VIRTUAL
    href: str = Field(..., min_length=1, description="External URL the asset points at.")
    filename: Optional[Filename] = Field(
        default=None,
        description="Optional display filename; derived from href tail if absent.",
    )


class AssetUpdate(BaseModel):
    """Mutable surface — metadata only.

    Identity (``asset_id``, ``filename``, ``href``) is immutable once a row
    becomes ``ACTIVE``; use a NEW_VERSION write policy to replace.

    Used by ``PUT`` (full replace): the request's ``metadata`` overwrites the
    stored ``metadata`` wholesale. For partial / nested updates use ``PATCH``
    with :class:`AssetPatchBody` (RFC 7396 JSON Merge Patch).
    """

    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Arbitrary metadata for the asset."
    )


class AssetPatchBody(BaseModel):
    """RFC 7396 JSON Merge Patch body for ``PATCH`` on an asset.

    The only patchable top-level key is ``metadata``. Inside ``metadata``,
    standard merge-patch rules apply: keys absent from the patch are left
    untouched; a key set to ``null`` is removed from the stored document;
    nested dicts are merged recursively; lists / scalars replace.

    Use this to update a few keys without losing siblings injected by
    other writers (e.g. the gdalinfo enrichment task).
    """

    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Partial metadata patch. Keys with value ``null`` are removed; "
            "nested objects are deep-merged."
        ),
    )

    model_config = ConfigDict(extra="forbid")


class AssetUploadDefinition(BaseModel):
    """
    Internal-use only: asset metadata embedded inside upload tickets.

    NOT a public REST request model.  Public callers use ``AssetCreate`` /
    ``VirtualAssetCreate``; this shape is reserved for ticket-bearing
    upload-driver internals where the URI is filled in by the backend after
    delivery.
    """

    asset_id: str = Field(..., description="Unique logical identifier for the asset.")
    asset_type: AssetTypeEnum = Field(
        AssetTypeEnum.ASSET, description="Type of the asset."
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Arbitrary metadata for the asset."
    )

    model_config = ConfigDict(populate_by_name=True)


class AssetReference(BaseModel):
    """
    Tracks a dependency between an asset and a referencing entity.

    Two modes
    ~~~~~~~~~
    ``cascade_delete=True`` — **informational**.  The referring driver handles
    its own cleanup (e.g. the PostgreSQL ``trg_asset_cleanup`` trigger cascades
    feature-row deletion automatically).  This reference **does not block**
    hard-deletion of the asset.

    ``cascade_delete=False`` — **protective**.  Hard-deletion of the asset is
    **blocked** (raises ``AssetReferencedError`` → HTTP 409) while this
    reference is active.  Use this for drivers that cannot auto-cascade (e.g.
    DuckDB, Iceberg, HTTP remote files).

    Typical usage
    ~~~~~~~~~~~~~
    Ingestion (PostgreSQL, cascade-safe)::

        await assets.add_asset_reference(
            asset_id="stations_2025",
            catalog_id="my_catalog",
            ref_type=CoreAssetReferenceType.COLLECTION,
            ref_id="weather_stations",
            cascade_delete=True,   # DB trigger already handles row cleanup
        )

    DuckDB-backed collection (non-cascading)::

        # On collection creation:
        await assets.add_asset_reference(
            asset_id="parquet_file_id",
            catalog_id="my_catalog",
            ref_type=DuckDbReferenceType.TABLE,   # "duckdb:table"
            ref_id="weather_stations_duckdb",
            cascade_delete=False,  # blocks deletion until the table is dropped
        )

        # On collection deletion (MUST precede the hard-delete attempt):
        await assets.remove_asset_reference(
            asset_id="parquet_file_id",
            catalog_id="my_catalog",
            ref_type=DuckDbReferenceType.TABLE,
            ref_id="weather_stations_duckdb",
        )
    """

    asset_id: str = Field(
        ...,
        description=(
            "The current live logical asset ID.  Populated from the ``assets`` "
            "JOIN in driver read paths so it always reflects the post-rename "
            "label; never a stale stored copy."
        ),
    )
    catalog_id: str = Field(..., description="Catalog scope of the asset.")
    ref_type: Union[AssetReferenceType, str] = Field(
        ...,
        description=(
            "Pluggable reference kind (e.g. 'collection', 'duckdb:table'). "
            "Use namespaced strings to avoid collisions between modules. "
            "Raw strings are accepted for forward-compat with driver types "
            "unknown at parse time (e.g. records read back from the database)."
        ),
    )
    ref_id: str = Field(
        ...,
        description=(
            "Owner-scoped identifier of the referencing entity "
            "(e.g. collection_id, DuckDB table name)."
        ),
    )
    cascade_delete: bool = Field(
        default=True,
        description=(
            "If True: the referring driver handles cleanup on asset deletion. "
            "If False: hard-deletion is BLOCKED while this reference is active."
        ),
    )
    created_at: datetime = Field(..., description="Timestamp when the reference was registered.")
    valid_until: Optional[datetime] = Field(
        default=None,
        description=(
            "When the reference was invalidated. NULL means currently active. "
            "Stamped by soft-delete and NEW_VERSION archive of the referenced "
            "asset so a successor row re-using the same asset_id isn't blocked "
            "by stale references. Default `list_asset_references` calls filter "
            "to active rows only (pass include_invalidated=True for the audit "
            "view)."
        ),
    )

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class AssetRenameConflictError(ValueError):
    """Raised when the target ``new_id`` already exists in the assets table."""

    def __init__(self, new_id: str) -> None:
        super().__init__(
            f"Asset id '{new_id}' already exists."
        )
        self.new_id = new_id


class AssetReferencedError(ValueError):
    """
    Raised when a hard-delete of an owned asset is attempted while one or more
    ``cascade_delete=False`` references remain active.

    Caught by the API layer and converted to HTTP 409 Conflict.
    """

    def __init__(self, asset_id: str, blocking_refs: List[AssetReference]) -> None:
        self.asset_id = asset_id
        self.blocking_refs = blocking_refs
        refs_summary = ", ".join(
            f"{r.ref_type}:{r.ref_id}" for r in blocking_refs
        )
        super().__init__(
            f"Asset '{asset_id}' cannot be hard-deleted: "
            f"{len(blocking_refs)} blocking reference(s) remain — {refs_summary}. "
            "Remove the referencing entities first or use soft-delete."
        )


class Asset(AssetBase):
    """Fully persisted asset retrieved from the catalog backend.

    ``filename`` is required for ``PHYSICAL`` rows; ``href`` is required for
    ``VIRTUAL`` rows (enforced as a CHECK constraint at the DB layer).
    Soft-delete is represented by ``status = AssetStatus.DELETED``; there is
    no separate ``deleted_at`` column.
    """

    kind: AssetKind
    status: AssetStatus
    catalog_id: Annotated[str, Field(description="The catalog ID.")]
    collection_id: Optional[str] = Field(
        default=None,
        description="Collection scope; NULL for catalog-tier assets.",
    )
    filename: Optional[str] = Field(
        default=None,
        description="Storage filename (PHYSICAL only); NOT NULL when kind=physical.",
    )
    href: Optional[str] = Field(
        default=None,
        description="External URL (VIRTUAL only); NOT NULL when kind=virtual.",
    )
    uri: Optional[str] = Field(
        default=None,
        description="Resolved storage URI (gs://, file://, ...) set by finalize for PHYSICAL once ACTIVE.",
    )
    content_hash: Optional[str] = Field(
        default=None,
        description="SHA-256 hex (or md5) set by finalize event.",
    )
    size_bytes: Optional[int] = Field(default=None, description="Object size in bytes.")
    created_at: datetime
    updated_at: Optional[datetime] = None
    # physical_id (#2296) is the immutable UUIDv7 join key. It is INTERNAL-ONLY
    # and intentionally NOT a field on this public model so it never serializes
    # to the REST/STAC API — users only ever deal with asset_id/external_id.
    # Internal callers read it from the row dict (it is in the SQL projections)
    # or via AssetsProtocol.resolve_asset_physical_id; the model strips any
    # stray copy by simply not declaring it.
    links: Optional[List[Link]] = Field(
        default=None,
        description=(
            "OGC API HATEOAS navigation links (``self``, ``collection``, "
            "``alternate`` to download). Populated by the asset REST surface "
            "on single-asset GET responses; ``None`` on rows returned by "
            "lower layers (DB / driver) so list responses stay terse."
        ),
    )

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# --- Dynamic Query Helpers ---
# ``AssetFilter`` lives in ``dynastore.models.query_builder`` (imported above)
# so the driver protocol + drivers can consume it without importing this
# service module.
# Query helpers are kept inside methods to allow schema injection.

# Obsolete global DDL removed.
# Assets correspond to {schema}.assets in the tenant schema.


@cached(
    maxsize=4096,
    ttl=300,
    namespace="asset_physical_id",
    ignore=["service"],
    condition=lambda v: v is not None,
)
async def _asset_physical_id_cache(
    service: "AssetService",
    catalog_id: str,
    asset_id: str,
    collection_id: Optional[str],
) -> Optional[str]:
    """Resolve an asset's immutable ``physical_id`` (UUIDv7) as a plain string.

    Pure accelerator over ``{schema}.assets.physical_id`` (the single source of
    truth, #2296).  Shared L1/L2 cache — the same ``@cached`` machinery as the
    collection resolver — so hot callers (the sidecar writer, ``stac_virtual``,
    the GCS finalize-metadata and DuckDB dispatch paths) pay the DB round-trip
    at most once per TTL window.  ``condition`` keeps misses out so a freshly
    created asset resolves immediately; ``service`` is ignored for keying so
    every service instance shares one backend.  Only the cache-friendly (no
    ``db_resource``) path routes here; in-transaction callers read straight
    from their connection.
    """
    return await service._resolve_asset_physical_id_db(
        catalog_id, asset_id, collection_id
    )


def _invalidate_asset_physical_id_cache(
    catalog_id: str, asset_id: str, collection_id: Optional[str]
) -> None:
    """Drop the shared asset physical-id cache entry for one logical id.

    Wired into ``rename_asset`` to bust both the old and the new logical id so a
    stale ``old_id -> physical_id`` mapping cannot survive the label change.
    The asset keeps its ``physical_id`` across the rename; only the logical key
    into this cache moves.
    """
    _asset_physical_id_cache.cache_invalidate(None, catalog_id, asset_id, collection_id)


# ---------------------------------------------------------------------------
# Reverse resolver: physical_id (UUIDv7) -> current logical asset_id
# ---------------------------------------------------------------------------
# Mirrors the collection-level _collection_logical_id_cache pattern from
# catalog_service.py.  The DuckDB items driver config persists the immutable
# physical_id so that a rename of the logical asset_id does not strand the
# config; at dispatch time it resolves the live asset_id here and calls
# get_asset(...) with the current label.

@cached(
    maxsize=4096,
    ttl=300,
    namespace="asset_logical_id",
    ignore=["service"],
    condition=lambda v: v is not None,
)
async def _asset_logical_id_cache(
    service: "AssetService",
    catalog_id: str,
    physical_id: str,
) -> Optional[str]:
    """Resolve an asset's current logical ``asset_id`` from its immutable ``physical_id``.

    The reverse of ``_asset_physical_id_cache``: reads ``{schema}.assets.asset_id``
    keyed on the stable UUIDv7 ``physical_id``.  Shared L1/L2 cache (TTL 300 s) —
    the same ``@cached`` machinery as the forward resolver.  ``condition`` keeps
    misses out so a just-created asset is resolvable immediately without a stale
    ``None`` entry blocking the hot path.  ``service`` is ignored for keying so
    every service instance shares one backend.  Only the cache-friendly (no live
    ``db_resource``) path routes through here; in-transaction callers read straight
    from their connection via ``_resolve_asset_logical_id_db``.
    """
    return await service._resolve_asset_logical_id_db(catalog_id, physical_id)


def _invalidate_asset_logical_id_cache(catalog_id: str, physical_id: str) -> None:
    """Drop the shared reverse cache entry for one asset physical_id.

    Must be called in ``rename_asset`` after the label change commits so a stale
    ``physical_id -> old_asset_id`` entry cannot outlive the TTL window.  The
    physical_id itself is immutable; only the logical label it maps to has moved.
    """
    _asset_logical_id_cache.cache_invalidate(None, catalog_id, physical_id)


class AssetService(AssetsProtocol):
    """
    SQL-backed implementation of ``AssetsProtocol``.

    Provides full asset lifecycle management against a PostgreSQL tenant schema:
    CRUD, soft/hard delete with reference guard, paginated list/search, and
    the ``asset_references`` tracking table for cross-driver dependency management.

    The ``db_resource`` parameter is an **internal** optional kwarg preserved for
    intra-module transactional calls (e.g. ingestion passing an open engine).
    It is NOT part of the ``AssetsProtocol`` contract and must not be called
    from outside the catalog module.

    Schema layout
    ~~~~~~~~~~~~~
    ::

        {catalog_schema}.assets   (PARTITION BY LIST (collection_physical_id))
          asset_id               VARCHAR   (mutable logical label)
          asset_physical_id      UUID NOT NULL  (immutable UUIDv7; join key)
          catalog_id             VARCHAR
          collection_id          VARCHAR   (NULL = catalog-tier; default partition)
          collection_physical_id VARCHAR   (immutable partition key)
          asset_type             VARCHAR
          kind                   VARCHAR   ('physical' | 'virtual')
          status                 VARCHAR   ('pending' | 'active' | 'failed' | 'deleted')
          filename               VARCHAR   (NOT NULL for kind=physical, CHECK)
          href                   TEXT      (NOT NULL for kind=virtual, CHECK)
          uri                    TEXT      (resolved storage URI for PHYSICAL, set by finalize)
          content_hash           VARCHAR(96)
          size_bytes             BIGINT
          metadata               JSONB
          owned_by               VARCHAR   (NULL = not file-owned; set = deletion guard active)
          created_at             TIMESTAMPTZ
          updated_at             TIMESTAMPTZ
          CONSTRAINT assets_identity_uq UNIQUE NULLS NOT DISTINCT (collection_physical_id, asset_id)
          CONSTRAINT assets_physical_uq UNIQUE (collection_physical_id, asset_physical_id)

        {catalog_schema}.asset_references
          asset_physical_id    UUID NOT NULL  (immutable join key from assets.asset_physical_id)
          ref_type             VARCHAR        (namespaced enum value, e.g. 'collection', 'duckdb:table')
          ref_id               VARCHAR        (collection_id, table_name, …)
          cascade_delete       BOOLEAN        DEFAULT TRUE
          created_at           TIMESTAMPTZ
          valid_until          TIMESTAMPTZ    (NULL = active; stamped by soft-delete/NEW_VERSION)
          PRIMARY KEY (asset_physical_id, ref_type, ref_id)
          PARTIAL INDEX  on (asset_physical_id) WHERE cascade_delete = FALSE AND valid_until IS NULL
    """

    # Protocol attributes
    priority: int = 10  # Higher priority than CatalogModule

    def __init__(
        self,
        engine: Optional[DbResource] = None,
        event_emitter: Optional[Callable] = None,
    ):
        self.engine = engine
        self._event_emitter = event_emitter

        # Instance-bound cache for assets.
        # TTL=60 s prevents stale reads across multi-worker deployments where
        # another process may have updated the row. Jitter spreads expiry to
        # avoid a thundering herd when many keys were warmed at the same time.
        self.get_asset_cached = cached(
            maxsize=128, ttl=60, jitter=5, namespace="assets"
        )(self._get_asset_db)

    def is_available(self) -> bool:
        """Returns True if the manager is initialized and ready."""
        return self.engine is not None

    async def _emit_durable(
        self,
        event_type: "AssetEventType",
        doc: Any,
        db_resource: Optional[DbResource],
    ) -> None:
        """Emit an asset domain event, guaranteeing the outbox write.

        When the caller supplies a transaction (bulk/ingest paths) we ride it.
        On the REST single-asset path db_resource is None and the emitter's
        ``if db_resource:`` guard would otherwise skip the durable outbox write,
        so we open a dedicated short transaction (#2256). The asset row is
        already committed at this point, so a separate transaction is safe.
        """
        if not self._event_emitter:
            return
        if db_resource is not None:
            await self._event_emitter(event_type, doc, db_resource=db_resource)
        elif self.engine is not None:
            async with managed_transaction(self.engine) as ev_conn:
                await self._event_emitter(event_type, doc, db_resource=ev_conn)
        else:
            await self._event_emitter(event_type, doc, db_resource=None)

    async def _resolve_schema(
        self, catalog_id: str, db_resource: DbResource
    ) -> Optional[str]:
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.catalogs import CatalogsProtocol

        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            return None
        return await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=db_resource)
        )

    def _get_partition_name(self, catalog_id: str, collection_id: str) -> str:
        """Generates the partition name for a given catalog and collection."""
        # Standardize: remove dots or other non-identifier chars if necessary, but here we assume safe
        return f"assets_{catalog_id}_{collection_id}"

    async def _resolve_asset_physical_id_db(
        self,
        catalog_id: str,
        asset_id: str,
        collection_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[str]:
        """Read ``physical_id`` from the per-tenant assets table.

        ``{schema}.assets.physical_id`` is the single source of truth for an
        asset's immutable join key.  Returns ``None`` when the catalog schema
        cannot be resolved, no engine is available, or the (non-deleted) row is
        absent.  When ``collection_id`` is given the lookup is scoped to that
        collection's physical shard; otherwise the most recent live row for the
        logical id wins (catalog-tier and cross-collection callers).
        """
        engine = db_resource or getattr(self, "engine", None)
        if engine is None:
            return None
        async with managed_transaction(engine) as conn:
            phys_schema = await self._resolve_schema(catalog_id, conn)
            if not phys_schema:
                return None
            if collection_id is not None:
                coll_phys = await DQLQuery(
                    f'SELECT physical_id FROM "{phys_schema}".collections '
                    "WHERE id = :collection_id AND deleted_at IS NULL;",
                    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
                ).execute(conn, collection_id=collection_id)
                if coll_phys is not None:
                    return await DQLQuery(
                        f'SELECT asset_physical_id::text FROM "{phys_schema}".assets '
                        "WHERE asset_id = :asset_id "
                        "AND collection_physical_id = :coll_phys "
                        "AND status <> 'deleted';",
                        result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
                    ).execute(conn, asset_id=asset_id, coll_phys=coll_phys)
            return await DQLQuery(
                f'SELECT asset_physical_id::text FROM "{phys_schema}".assets '
                "WHERE asset_id = :asset_id AND status <> 'deleted' "
                "ORDER BY created_at DESC LIMIT 1;",
                result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
            ).execute(conn, asset_id=asset_id)

    async def resolve_asset_physical_id(
        self,
        catalog_id: str,
        asset_id: str,
        collection_id: Optional[str] = None,
        *,
        ctx: Optional[DriverContext] = None,
        allow_missing: bool = True,
    ) -> Optional[str]:
        """Return the immutable ``physical_id`` (UUIDv7) for a logical asset.

        Mirrors ``CatalogsProtocol.resolve_physical_id``: in-transaction callers
        (``ctx.db_resource`` set) read straight from their connection so
        uncommitted state is visible and never serve from the cache; everyone
        else routes through the shared L1/L2 accelerator.  ``allow_missing``
        defaults ``True`` — denormalized surfaces resolve opportunistically and
        fall back to the logical id when the row has not landed yet.
        """
        db_resource = ctx.db_resource if ctx else None
        if db_resource is not None:
            phys = await self._resolve_asset_physical_id_db(
                catalog_id, asset_id, collection_id, db_resource=db_resource
            )
        else:
            phys = await _asset_physical_id_cache(
                self, catalog_id, asset_id, collection_id
            )
        if not phys and not allow_missing:
            raise ValueError(f"Asset '{catalog_id}/{asset_id}' not found.")
        return phys

    async def _resolve_asset_logical_id_db(
        self,
        catalog_id: str,
        physical_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[str]:
        """Read the current logical ``asset_id`` for an asset's immutable physical id.

        The reverse of :meth:`_resolve_asset_physical_id_db`: keys on the stable
        UUIDv7 ``physical_id`` column and returns the live logical ``asset_id``.
        Returns ``None`` when the catalog schema cannot be resolved, no engine is
        available, or the (non-deleted) row is absent.
        """
        engine = db_resource or getattr(self, "engine", None)
        if engine is None:
            return None
        async with managed_transaction(engine) as conn:
            phys_schema = await self._resolve_schema(catalog_id, conn)
            if not phys_schema:
                return None
            return await DQLQuery(
                f'SELECT asset_id FROM "{phys_schema}".assets '
                "WHERE asset_physical_id = :physical_id AND status <> 'deleted' "
                "ORDER BY created_at DESC LIMIT 1;",
                result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
            ).execute(conn, physical_id=physical_id)

    async def resolve_asset_logical_id(
        self,
        catalog_id: str,
        physical_id: str,
        *,
        ctx: Optional[DriverContext] = None,
    ) -> Optional[str]:
        """Return the current logical ``asset_id`` for an asset's immutable physical id.

        The inverse of :meth:`resolve_asset_physical_id`: given the UUIDv7
        ``physical_id`` stored in a persisted config (e.g. ``asset_physical_id`` on
        the DuckDB items driver config), return the live user-facing ``asset_id``.
        After an asset rename the physical_id is stable while the logical label
        changes, so dispatch paths that persist the physical_id and re-resolve here
        remain rename-safe.

        When ``ctx`` carries a connection the lookup joins that transaction directly
        so uncommitted state is visible; otherwise the shared ``_asset_logical_id_cache``
        (TTL 300 s) serves as a lossless accelerator.  Returns ``None`` for an unknown
        physical_id; callers should treat ``None`` as "asset not found".
        """
        db_resource = ctx.db_resource if ctx else None
        if db_resource is not None:
            return await self._resolve_asset_logical_id_db(
                catalog_id, physical_id, db_resource=db_resource
            )
        return await _asset_logical_id_cache(self, catalog_id, physical_id)

    # --- Retrieval & Advanced Search ---

    async def _get_asset_db(
        self, catalog_id: str, asset_id: str, collection_id: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Fetch asset dict from the configured read driver (cached path)."""
        from dynastore.modules.storage.router import get_asset_driver
        driver = await get_asset_driver("READ", catalog_id, collection_id)
        return await driver.get_asset(
            catalog_id, asset_id,
            collection_id=collection_id,
            db_resource=self.engine,
        )

    # _apply_enricher_pipeline removed in the role-based driver refactor
    # (plan §Protocols — AssetEnricherProtocol deleted).  Asset enrichment
    # is now handled by TRANSFORM drivers routed through AssetRoutingConfig
    # and invoked lazily by opt-in endpoints / the async reindex pipeline.
    # Default read paths return raw asset docs — no enrichment hop.

    async def _get_secondary_drivers(
        self, catalog_id: str, collection_id: Optional[str]
    ) -> List["ResolvedDriver"]:
        """Return non-primary WRITE asset drivers for fan-out.

        Reads only the primary WRITE entries (``secondary_index=False``) in
        ``AssetRoutingConfig.operations[WRITE]`` — operator-pinned
        multi-writes. The first entry is the primary (the caller handles it
        directly); the rest are returned here.

        The secondary-index ``WRITE`` entries (``secondary_index=True``,
        auto-augmented with discoverable ``AssetIndexer`` drivers like
        ``AssetElasticsearchDriver``) are **not** folded in here —
        indexer drivers are driven by ``AssetEntitySyncSubscriber`` via the
        events bus, decoupling sync writers from indexer fan-out.

        Returns :class:`ResolvedDriver` instances (preserving ``on_failure``
        and ``write_mode``) instead of raw driver objects.
        """
        from dynastore.modules.storage.router import get_asset_write_drivers

        try:
            resolved = await get_asset_write_drivers(catalog_id, collection_id)
            # Skip the first (primary) — caller already handles it
            return resolved[1:]
        except Exception:
            return []

    async def _fan_out_asset_writes(
        self,
        catalog_id: str,
        collection_id: Optional[str],
        asset_doc: Dict[str, Any],
        method_name: str,
    ) -> None:
        """Fan-out asset writes to secondary drivers, respecting on_failure + write_mode.

        Args:
            asset_doc: The asset document to write.
            method_name: Driver method to call (e.g. ``"index_asset"``).
        """
        import asyncio
        from dynastore.modules.concurrency import run_in_background
        from dynastore.modules.storage.routing_config import FailurePolicy, WriteMode

        secondaries = await self._get_secondary_drivers(catalog_id, collection_id)
        if not secondaries:
            return

        sync_drivers = [r for r in secondaries if r.write_mode == WriteMode.SYNC]
        async_drivers = [r for r in secondaries if r.write_mode == WriteMode.ASYNC]

        # Sync phase: parallel writes
        if sync_drivers:
            tasks = [
                getattr(r.driver, method_name)(catalog_id, asset_doc)
                for r in sync_drivers
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r, result in zip(sync_drivers, results):
                if isinstance(result, BaseException):
                    if r.on_failure == FailurePolicy.FATAL:
                        raise result if isinstance(result, Exception) else RuntimeError(str(result))
                    elif r.on_failure == FailurePolicy.WARN:
                        logger.warning(
                            "Secondary driver '%s' %s failed for %s: %s",
                            r.driver_ref, method_name, catalog_id, result,
                        )

        # Async phase: fire-and-forget. ``run_in_background`` holds a strong
        # reference in a module-level set so the task cannot be GC'd
        # mid-execution and silently drop the write (asyncio docs warn the
        # loop only keeps weak refs to bare ``create_task`` returns).
        for r in async_drivers:
            run_in_background(
                self._async_asset_write(r, catalog_id, asset_doc, method_name),
                name=f"asset_secondary_write:{method_name}:{r.driver_ref}",
            )

    async def _async_asset_write(
        self,
        resolved: "ResolvedDriver",
        catalog_id: str,
        asset_doc: Dict[str, Any],
        method_name: str,
    ) -> None:
        """Fire-and-forget wrapper for async asset writes."""
        from dynastore.modules.storage.routing_config import FailurePolicy

        try:
            await getattr(resolved.driver, method_name)(catalog_id, asset_doc)
        except Exception as err:
            if resolved.on_failure == FailurePolicy.FATAL:
                logger.error(
                    "Async asset driver '%s' %s FATAL failure for %s: %s",
                    resolved.driver_ref, method_name, catalog_id, err,
                )
            elif resolved.on_failure == FailurePolicy.WARN:
                logger.warning(
                    "Async asset driver '%s' %s failed for %s: %s",
                    resolved.driver_ref, method_name, catalog_id, err,
                )

    async def get_asset(
        self,
        catalog_id: str,
        asset_id: str,
        collection_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[Asset]:
        """Get asset by ID, routing through the configured read driver."""
        from dynastore.modules.storage.router import get_asset_driver

        if db_resource:
            driver = await get_asset_driver("READ", catalog_id, collection_id)
            asset_doc = await driver.get_asset(
                catalog_id, asset_id,
                collection_id=collection_id,
                db_resource=db_resource,
            )
        else:
            asset_doc = await self.get_asset_cached(catalog_id, asset_id, collection_id)

        if not asset_doc:
            return None

        return Asset.model_validate(dict(asset_doc))

    # Deprecated alias methods for backward compatibility if needed, but we should switch to get_asset using internal logic
    async def get_asset_by_code(
        self,
        catalog_id: str,
        asset_code: str,
        collection_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[Asset]:
        # 'code' is now 'asset_id'. This is just an alias.
        return await self.get_asset(catalog_id, asset_code, collection_id, db_resource)

    # _get_asset_by_code_cached removed as it's redundant with _get_asset_cached

    async def list_assets(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
        db_resource: Optional[DbResource] = None,
    ) -> List[Asset]:
        from dynastore.modules.storage.router import get_asset_driver
        driver = await get_asset_driver("READ", catalog_id, collection_id)
        docs = await driver.search_assets(
            catalog_id,
            collection_id=collection_id,
            limit=limit,
            offset=offset,
            db_resource=db_resource or self.engine,
        )
        return [Asset.model_validate(dict(doc)) for doc in docs]

    async def search_assets(
        self,
        catalog_id: str,
        filters: List[AssetFilter],
        collection_id: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
        all_collections: bool = False,
        db_resource: Optional[DbResource] = None,
    ) -> List[Asset]:
        """
        Performs a granular search across assets using a list of filters.

        Collection scope is tri-state: ``collection_id=None`` (the default)
        scopes to catalog-tier assets, a value scopes to one collection, and
        ``all_collections=True`` spans every collection plus the catalog tier
        under the catalog (the ``collection_id`` predicate is dropped).

        ``filters`` are forwarded to the routed driver, which translates them
        via :mod:`dynastore.modules.tools.asset_filters` (one operator
        vocabulary shared by the PG and ES backends). Unsupported operators
        raise ``ValueError`` from that layer (mapped to HTTP 400).
        """
        from dynastore.modules.storage.router import get_asset_search_driver

        driver = await get_asset_search_driver(catalog_id, collection_id)
        docs = await driver.search_assets(
            catalog_id,
            collection_id=collection_id,
            filters=filters or None,
            limit=limit,
            offset=offset,
            all_collections=all_collections,
            db_resource=db_resource or self.engine,
        )
        return [Asset.model_validate(dict(doc)) for doc in docs]

    # --- Lifecycle ---

    async def create_asset(
        self,
        catalog_id: str,
        asset: AssetBase,
        collection_id: Optional[str] = None,
        ctx: Optional[DriverContext] = None,
    ) -> Asset:
        from dynastore.modules.storage.router import get_asset_driver
        db_resource = ctx.db_resource if ctx else None
        now = datetime.now(timezone.utc)

        # Resolve discriminated fields based on which create-shape was passed.
        # Stage 4 will replace this with policy-driven PENDING inserts; for now
        # we default to PHYSICAL/ACTIVE (so existing call sites that pass a
        # plain ``AssetBase`` still produce a valid row), and propagate
        # ``filename`` / ``href`` / ``kind`` from the typed subclasses.
        kind_val = getattr(asset, "kind", AssetKind.PHYSICAL)
        if hasattr(kind_val, "value"):
            kind_str = kind_val.value
        else:
            kind_str = str(kind_val)
        filename_val: Optional[str] = getattr(asset, "filename", None)
        href_val: Optional[str] = getattr(asset, "href", None)
        uri_val: Optional[str] = getattr(asset, "uri", None)

        asset_doc: Dict[str, Any] = {
            "asset_id": asset.asset_id,
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "asset_type": asset.asset_type.value,
            "kind": kind_str,
            "status": AssetStatus.ACTIVE.value,
            "filename": filename_val,
            "href": href_val,
            "uri": uri_val,
            "content_hash": None,
            "size_bytes": None,
            "created_at": now,
            "metadata": asset.metadata,
            "owned_by": asset.owned_by,
        }

        write_driver = await get_asset_driver("WRITE", catalog_id, collection_id)
        await write_driver.index_asset(catalog_id, asset_doc, db_resource=db_resource)

        # Fan-out to secondary drivers respecting on_failure + write_mode
        await self._fan_out_asset_writes(
            catalog_id, collection_id, asset_doc, "index_asset",
        )

        # Fetch canonical state from the write driver (captures DB-set timestamps)
        fetched_doc = await write_driver.get_asset(
            catalog_id, asset.asset_id,
            collection_id=collection_id,
            db_resource=db_resource,
        )
        created = Asset.model_validate(fetched_doc or asset_doc)

        self._invalidate_cache(created.asset_id, catalog_id, collection_id)

        await self._emit_durable(
            AssetEventType.ASSET_CREATED, created.model_dump(), db_resource
        )
        await log_info(
            catalog_id, "asset_created",
            f"Asset '{created.asset_id}' created",
            collection_id=collection_id,
        )
        return created

    async def update_asset(
        self,
        catalog_id: str,
        asset_id: str,
        update: AssetUpdate,
        collection_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> Asset:
        """Updates an existing asset's metadata via the configured write driver."""
        from dynastore.modules.storage.router import get_asset_driver

        # Fetch current asset
        current = await self.get_asset(
            asset_id=asset_id,
            catalog_id=catalog_id,
            collection_id=collection_id,
            db_resource=db_resource,
        )
        if not current:
            raise ValueError(
                f"Asset '{asset_id}' not found in catalog '{catalog_id}'."
            )

        # Build updated doc
        updated_doc: Dict[str, Any] = current.model_dump()
        updated_doc["metadata"] = update.metadata
        updated_doc["collection_id"] = collection_id
        updated_doc["updated_at"] = datetime.now(timezone.utc)

        write_driver = await get_asset_driver("WRITE", catalog_id, collection_id)
        await write_driver.index_asset(catalog_id, updated_doc, db_resource=db_resource)

        # Fan-out to secondary drivers respecting on_failure + write_mode
        await self._fan_out_asset_writes(
            catalog_id, collection_id, updated_doc, "index_asset",
        )

        # Fetch canonical state
        fetched_doc = await write_driver.get_asset(
            catalog_id, asset_id,
            collection_id=collection_id,
            db_resource=db_resource,
        )
        updated = Asset.model_validate(fetched_doc or updated_doc)

        self._invalidate_cache(updated.asset_id, catalog_id, collection_id)

        await self._emit_durable(
            AssetEventType.ASSET_UPDATED, updated.model_dump(), db_resource
        )
        await log_info(
            catalog_id, "asset_updated",
            f"Asset '{updated.asset_id}' updated",
            collection_id=collection_id,
        )
        return updated

    async def patch_asset(
        self,
        catalog_id: str,
        asset_id: str,
        patch: AssetPatchBody,
        collection_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> Asset:
        """RFC 7396 partial update of an asset's ``metadata``.

        Unlike :meth:`update_asset` (which replaces ``metadata`` wholesale),
        this merges the patch into the stored document key-by-key: ``null``
        removes a key, nested dicts recurse, lists/scalars replace. Sibling
        keys not mentioned in the patch are preserved — so an enrichment
        writer (e.g. gdalinfo) and an end-user editing ``metadata.title``
        no longer clobber each other.
        """
        from dynastore.modules.storage.router import get_asset_driver
        from dynastore.modules.catalog._merge_patch import merge_patch

        current = await self.get_asset(
            asset_id=asset_id,
            catalog_id=catalog_id,
            collection_id=collection_id,
            db_resource=db_resource,
        )
        if not current:
            raise ValueError(
                f"Asset '{asset_id}' not found in catalog '{catalog_id}'."
            )

        # No-op patch: body absent or `{"metadata": null}` -> nothing to do.
        # The user can still call PUT with `{"metadata": {}}` to clear it.
        if patch.metadata is None:
            return current

        merged_metadata = merge_patch(current.metadata or {}, patch.metadata)

        updated_doc: Dict[str, Any] = current.model_dump()
        updated_doc["metadata"] = merged_metadata
        updated_doc["collection_id"] = collection_id
        updated_doc["updated_at"] = datetime.now(timezone.utc)

        write_driver = await get_asset_driver("WRITE", catalog_id, collection_id)
        await write_driver.index_asset(catalog_id, updated_doc, db_resource=db_resource)

        await self._fan_out_asset_writes(
            catalog_id, collection_id, updated_doc, "index_asset",
        )

        fetched_doc = await write_driver.get_asset(
            catalog_id, asset_id,
            collection_id=collection_id,
            db_resource=db_resource,
        )
        updated = Asset.model_validate(fetched_doc or updated_doc)

        self._invalidate_cache(updated.asset_id, catalog_id, collection_id)

        await self._emit_durable(
            AssetEventType.ASSET_UPDATED, updated.model_dump(), db_resource
        )
        await log_info(
            catalog_id, "asset_updated",
            f"Asset '{updated.asset_id}' updated",
            collection_id=collection_id,
        )
        return updated

    async def finalize_pending_upload(
        self,
        catalog_id: str,
        *,
        uri: str,
        owned_by: str,
        ticket_id: Optional[str] = None,
        asset_id: Optional[str] = None,
        size_bytes: Optional[int] = None,
        content_hash: Optional[str] = None,
        collection_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[Asset]:
        """Transition a born-claimed PENDING upload row to ACTIVE.

        Backend-neutral counterpart to the GCS OBJECT_FINALIZE activator
        (``modules/gcp/gcp_finalize_activator.activate``): the REST upload-
        create flow inserts a PENDING ``assets`` row up front (stamping
        ``metadata._upload.ticket_id``), and the storage backend calls this
        once the bytes have landed to set ``uri`` / ``owned_by`` /
        ``size_bytes`` / ``content_hash`` and flip ``status`` to ACTIVE.

        Row selection prefers a match on ``metadata._upload.ticket_id`` (the
        born-claimed row stamped at initiate time), falling back to
        ``asset_id``. Returns the activated :class:`Asset`, or ``None`` when no
        PENDING row matched (e.g. the policy path was skipped and no
        born-claimed row exists) — callers fall back to ``create_asset`` in
        that case.
        """
        engine = db_resource or self.engine
        if engine is None:
            return None

        schema = await self._resolve_schema(catalog_id, engine)
        if not schema:
            return None

        # Resolve the collection's physical id for partition-pruning predicates.
        from dynastore.tools.discovery import get_protocol as _get_proto
        from dynastore.models.protocols.catalogs import CatalogsProtocol as _CatProto
        from dynastore.models.driver_context import DriverContext as _DCtx
        _catalogs = _get_proto(_CatProto)
        collection_physical_id: Optional[str] = None
        if collection_id is not None and _catalogs is not None:
            collection_physical_id = await _catalogs.resolve_physical_id(
                catalog_id,
                collection_id,
                ctx=_DCtx(db_resource=engine),
                allow_missing=True,
            )

        # Schema scopes the catalog; catalog_id is a mutable label, omitted here
        # to stay rename-robust.
        select_sql = (
            f'SELECT asset_id, status, metadata '
            f'FROM "{schema}".assets '
            "WHERE collection_physical_id IS NOT DISTINCT FROM :collection_physical_id "
            "AND kind = 'physical' "
            "AND status = 'pending' "
            "ORDER BY created_at ASC "
            "FOR UPDATE"
        )
        update_sql = (
            f'UPDATE "{schema}".assets '
            "SET status = 'active', "
            "    uri = :uri, "
            "    owned_by = :owned_by, "
            "    content_hash = :content_hash, "
            "    size_bytes = :size_bytes, "
            "    metadata = metadata - '_upload', "
            "    updated_at = NOW() "
            "WHERE collection_physical_id IS NOT DISTINCT FROM :collection_physical_id "
            "AND asset_id = :asset_id"
        )

        async with managed_transaction(engine) as conn:
            rows = await DQLQuery(
                select_sql, result_handler=ResultHandler.ALL
            ).execute(
                conn,
                collection_physical_id=collection_physical_id,
            )
            rows = list(rows or [])
            if not rows:
                return None

            chosen = None
            if ticket_id is not None:
                for row in rows:
                    if self._row_ticket_id(row) == str(ticket_id):
                        chosen = row
                        break
            if chosen is None and asset_id is not None:
                for row in rows:
                    if str(self._row_value(row, "asset_id")) == str(asset_id):
                        chosen = row
                        break
            if chosen is None:
                # No correlation key matched a born-claimed row — leave the
                # caller to fall back to create_asset.
                return None

            chosen_asset_id = self._row_value(chosen, "asset_id")
            await DQLQuery(
                update_sql, result_handler=ResultHandler.NONE
            ).execute(
                conn,
                uri=uri,
                owned_by=owned_by,
                content_hash=content_hash,
                size_bytes=size_bytes,
                collection_physical_id=collection_physical_id,
                asset_id=chosen_asset_id,
            )

        self._invalidate_cache(str(chosen_asset_id), catalog_id, collection_id)

        activated = await self.get_asset(
            catalog_id=catalog_id,
            asset_id=str(chosen_asset_id),
            collection_id=collection_id,
            db_resource=engine,
        )

        if activated is not None:
            await self._emit_durable(
                AssetEventType.ASSET_UPDATED, activated.model_dump(), engine
            )
            await log_info(
                catalog_id, "asset_updated",
                f"Asset '{activated.asset_id}' activated",
                collection_id=collection_id,
            )
        return activated

    @staticmethod
    def _row_value(row: Any, key: str) -> Any:
        """Read a column from a SQLAlchemy Row or a plain dict (test-friendly)."""
        if row is None:
            return None
        if isinstance(row, dict):
            return row.get(key)
        mapping = getattr(row, "_mapping", None)
        if mapping is not None:
            return mapping.get(key)
        return getattr(row, key, None)

    @classmethod
    def _row_ticket_id(cls, row: Any) -> Optional[str]:
        """Extract ``metadata._upload.ticket_id`` from a row (dict-or-JSON)."""
        meta = cls._row_value(row, "metadata")
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = None
        if not isinstance(meta, dict):
            return None
        upload = meta.get("_upload")
        if not isinstance(upload, dict):
            return None
        tid = upload.get("ticket_id")
        return str(tid) if tid is not None else None

    def _invalidate_cache(self, asset_id: str, catalog_id: str, collection_id: Optional[str]):
        """Invalidates related cache entries."""
        # Signature of _get_asset_db is (catalog_id, asset_id, collection_id)
        self.get_asset_cached.cache_invalidate(catalog_id, asset_id, collection_id)

    async def delete_assets(
        self,
        catalog_id: str,
        asset_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        hard: bool = False,
        propagate: bool = False,
        external: bool = True,
        db_resource: Optional[DbResource] = None,
    ) -> int:
        """Delete assets matching the given criteria.

        Uses the PG driver for candidate lookup, reference guard, and the
        canonical delete.  Hard-deletes are fanned out to non-PG write driver
        and secondary drivers.

        When ``external`` is ``False`` the non-PG fan-out is skipped: only the
        canonical PG delete runs.  This is for callers executing inside an open
        DB transaction (e.g. the collection hard-delete) where the external
        (Elasticsearch) teardown is owned by the async cascade — doing ES HTTP
        I/O here would hold the transaction's connection idle in-transaction and
        the commit would fail (idle_in_transaction_session_timeout).
        """
        from dynastore.modules.storage.router import get_asset_driver
        from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver

        pg_driver = AssetPostgresqlDriver(engine=db_resource or self.engine)
        rowcount, rows_or_blocking = await pg_driver.delete_assets_bulk(
            catalog_id,
            asset_id=asset_id,
            collection_id=collection_id,
            hard=hard,
            db_resource=db_resource,
        )

        # rowcount == -1 means blocking references were found
        if rowcount == -1:
            blocking_rows = [AssetReference.model_validate(r) for r in rows_or_blocking]
            first_asset_id = blocking_rows[0].asset_id
            asset_blocking = [
                r for r in blocking_rows if r.asset_id == first_asset_id
            ]
            raise AssetReferencedError(first_asset_id, asset_blocking)

        if rowcount == 0:
            return 0

        asset_rows = rows_or_blocking

        # Invalidate cache
        for a in asset_rows:
            self._invalidate_cache(a["asset_id"], a["catalog_id"], a["collection_id"])

        # Fan-out hard-deletes to write driver (if non-PG) and secondary drivers.
        # Skipped when ``external=False``: the caller (collection hard-delete)
        # runs inside an open transaction and delegates external teardown to the
        # async cascade, so no external driver I/O happens on the delete conn.
        if hard and external:
            from dynastore.modules.storage.routing_config import FailurePolicy

            write_driver = await get_asset_driver("WRITE", catalog_id, collection_id)
            secondaries = await self._get_secondary_drivers(catalog_id, collection_id)

            # Build drivers to notify: non-PG primary + all secondaries
            drivers_to_delete = []
            from dynastore.models.protocols.storage_driver import Capability
            if Capability.QUERY_FALLBACK_SOURCE not in write_driver.capabilities:
                from dynastore.modules.storage.router import ResolvedDriver
                drivers_to_delete.append(
                    ResolvedDriver(driver=write_driver, on_failure=FailurePolicy.WARN)
                )
            drivers_to_delete.extend(secondaries)

            for a in asset_rows:
                for resolved in drivers_to_delete:
                    try:
                        await resolved.driver.delete_asset(
                            a["catalog_id"], a["asset_id"]
                        )
                    except Exception as err:
                        if resolved.on_failure == FailurePolicy.FATAL:
                            raise
                        elif resolved.on_failure == FailurePolicy.WARN:
                            logger.warning(
                                "Driver '%s' delete_asset failed for %s/%s: %s",
                                resolved.driver_ref, a["catalog_id"], a["asset_id"], err,
                            )

        # Emit events. ``propagate`` is stamped on each payload so the
        # ``ItemReverseCascadeSubscriber`` can opt into the cascade only
        # when the caller asked for it.
        event_type = (
            AssetEventType.ASSET_HARD_DELETED if hard
            else AssetEventType.ASSET_DELETED
        )
        log_event_type = "asset_deleted"
        for a in asset_rows:
            doc = dict(a)
            doc["propagate"] = bool(propagate)
            await self._emit_durable(event_type, doc, db_resource)
            await log_info(
                catalog_id, log_event_type,
                f"Asset '{a.get('asset_id')}' deleted (hard={hard})",
                collection_id=a.get("collection_id"),
            )

        return rowcount

    async def soft_delete_asset(
        self,
        catalog_id: str,
        asset_id: str,
        collection_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> int:
        return await self.delete_assets(
            catalog_id,
            asset_id=asset_id,
            collection_id=collection_id,
            db_resource=db_resource,
        )

    async def hard_delete_asset(
        self,
        catalog_id: str,
        asset_id: str,
        propagate: bool = False,
        db_resource: Optional[DbResource] = None,
    ) -> int:
        return await self.delete_assets(
            catalog_id,
            asset_id=asset_id,
            hard=True,
            propagate=propagate,
            db_resource=db_resource,
        )

    async def soft_delete_collection_assets(
        self,
        catalog_id: str,
        collection_id: str,
        propagate: bool = False,
        db_resource: Optional[DbResource] = None,
    ) -> int:
        return await self.delete_assets(
            catalog_id,
            collection_id=collection_id,
            propagate=propagate,
            db_resource=db_resource,
        )

    async def soft_delete_catalog_assets(
        self,
        catalog_id: str,
        propagate: bool = False,
        db_resource: Optional[DbResource] = None,
    ) -> int:
        return await self.delete_assets(
            catalog_id, propagate=propagate, db_resource=db_resource
        )

    async def ensure_asset_cleanup_trigger(
        self,
        schema: str,
        table: str,
        ctx: Optional[DriverContext] = None,
    ) -> None:
        """
        Ensures that the asset cleanup trigger is present on the specified table.
        This includes ensuring the 'asset_cleanup' function exists in the tenant schema.
        """
        db_resource = ctx.db_resource if ctx else None
        async with managed_transaction(db_resource or self.engine) as conn:
            # 1. Check if asset_id column exists
            check_sql = """
            SELECT 1 
            FROM information_schema.columns 
            WHERE table_schema = :schema 
              AND table_name = :table 
              AND column_name = 'asset_id'
            """
            has_asset_id = await DQLQuery(
                check_sql, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
            ).execute(conn, schema=schema, table=table)
            
            if not has_asset_id:
                logger.debug(f"Skipping asset cleanup trigger for {schema}.{table}: column 'asset_id' not found.")
                return

            # 2. Ensure the stored procedure exists in this schema
            from .db_init.stored_procedures import ensure_stored_procedures
            await ensure_stored_procedures(conn)

            # 3. Derive Hub Physical Table Name
            # If it's a sidecar table, we need the hub name for the trigger argument.
            hub_table = table
            for suffix in ["_attributes", "_geometry", "_geoms"]:
                if table.endswith(suffix):
                    hub_table = table[: -len(suffix)]
                    break

            # 4. Create the trigger. Warm paths skip DDL via auto-inferred existence
            # check — avoids the AccessExclusiveLock that DROP+CREATE TRIGGER
            # would take against concurrent ingest DML.
            trigger_ddl = f"""
            CREATE TRIGGER trg_asset_cleanup
            AFTER DELETE OR UPDATE OF asset_id ON "{schema}"."{table}"
            FOR EACH ROW
            EXECUTE FUNCTION catalog.asset_cleanup('{hub_table}');
            """.strip()

            await DDLQuery(trigger_ddl).execute(conn)

    # -------------------------------------------------------------------------
    # Asset reference table bootstrap
    # -------------------------------------------------------------------------

    async def ensure_asset_references_table(
        self,
        schema: str,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        """Idempotently creates the ``asset_references`` table in *schema*.

        Delegates to ``AssetPostgresqlDriver.ensure_storage`` which owns the
        canonical DDL for both ``assets`` and ``asset_references``.  This
        method is kept for call-site compatibility but is a thin wrapper.
        """
        from dynastore.modules.catalog.drivers.pg_asset_driver import (
            AssetPostgresqlDriver,
        )

        driver = AssetPostgresqlDriver(engine=db_resource or self.engine)
        # Resolve catalog_id from the schema name is not trivial here; callers
        # that only have a schema string can pass it through kwargs so
        # ensure_storage skips the schema-resolution step.
        await driver.ensure_storage(
            catalog_id="",  # unused when schema is supplied
            db_resource=db_resource or self.engine,
            schema=schema,
        )

    # -------------------------------------------------------------------------
    # Asset reference CRUD
    # -------------------------------------------------------------------------

    async def add_asset_reference(
        self,
        asset_id: str,
        catalog_id: str,
        ref_type: AssetReferenceType,
        ref_id: str,
        cascade_delete: bool = True,
        ctx: Optional[DriverContext] = None,
    ) -> AssetReference:
        """Registers a dependency on an asset. Delegates to AssetPostgresqlDriver."""
        from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver
        db_resource = ctx.db_resource if ctx else None
        pg = AssetPostgresqlDriver(engine=db_resource or self.engine)
        row = await pg.add_asset_reference(
            asset_id=asset_id,
            catalog_id=catalog_id,
            ref_type=ref_type,
            ref_id=ref_id,
            cascade_delete=cascade_delete,
            db_resource=db_resource,
        )
        return AssetReference.model_validate(row)

    async def remove_asset_reference(
        self,
        asset_id: str,
        catalog_id: str,
        ref_type: AssetReferenceType,
        ref_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        """Removes a previously registered asset reference."""
        from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver
        pg = AssetPostgresqlDriver(engine=db_resource or self.engine)
        await pg.remove_asset_reference(
            asset_id=asset_id,
            catalog_id=catalog_id,
            ref_type=ref_type,
            ref_id=ref_id,
            db_resource=db_resource,
        )

    async def list_asset_references(
        self,
        asset_id: str,
        catalog_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> List[AssetReference]:
        """Returns all active references for the given asset."""
        from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver
        pg = AssetPostgresqlDriver(engine=db_resource or self.engine)
        rows = await pg.list_asset_references(
            asset_id=asset_id,
            catalog_id=catalog_id,
            db_resource=db_resource,
        )
        return [AssetReference.model_validate(r) for r in rows]

    async def list_assets_for_reference(
        self,
        catalog_id: str,
        ref_type: Any,
        ref_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> List[str]:
        """Returns asset IDs that carry a given reference (inverse lookup)."""
        from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver
        pg = AssetPostgresqlDriver(engine=db_resource or self.engine)
        return await pg.list_assets_for_reference(
            catalog_id=catalog_id,
            ref_type=ref_type,
            ref_id=ref_id,
            db_resource=db_resource,
        )

    async def _list_blocking_references_bulk(
        self,
        physical_ids: List[str],
        catalog_id: str,
        conn: DbConnection,
        phys_schema: str,
    ) -> List[AssetReference]:
        """Return cascade_delete=False references for all given asset physical_ids.

        Passes ``physical_ids`` (not logical asset_ids) to
        ``check_blocking_references`` so the lookup is rename-stable.
        """
        from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver
        pg = AssetPostgresqlDriver(engine=conn)
        rows = await pg.check_blocking_references(
            physical_ids=physical_ids,
            catalog_id=catalog_id,
            db_resource=conn,
        )
        return [AssetReference.model_validate(r) for r in rows]

    async def rename_asset(
        self,
        catalog_id: str,
        asset_id: str,
        new_id: str,
        collection_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> "RenameResponse":
        """Rename an asset's logical ``asset_id`` (label change only).

        The physical file (GCS URI, S3 key, disk path) is NOT moved.  The
        partition key ``collection_physical_id`` and the storage backends are
        unchanged.  Only the label column ``{schema}.assets.asset_id`` is
        updated inside a single tight transaction.

        ``asset_references`` is NOT touched: it keys on the immutable
        ``physical_id`` (UUIDv7), so a rename is a pure one-column label
        change on ``assets`` and zero propagation is needed in the
        references table.  Any active reference still resolves to the new
        ``asset_id`` via the ``assets`` JOIN in the driver query methods.

        The ``collection_physical_id`` partition key is the immutable physical
        identity of the collection shard and is never touched.
        """
        from dynastore.tools.db import validate_asset_id
        from dynastore.extensions.ogc_models_shared import RenameResponse

        new_id = validate_asset_id(new_id)

        # Fast path: no-op when the ids are already identical.
        if new_id == asset_id:
            return RenameResponse(
                old_id=asset_id,
                new_id=new_id,
                level="asset",
                warnings=[],
                reindex_required=False,
                iam_manual_update_required=False,
            )

        engine = db_resource or self.engine

        async with managed_transaction(engine) as conn:
            if db_resource is None:
                from typing import cast as _cast
                from sqlalchemy import text as _text
                from sqlalchemy.ext.asyncio import AsyncConnection as _AsyncConn
                await _cast(_AsyncConn, conn).execute(
                    _text("SET LOCAL idle_in_transaction_session_timeout = '0'")
                )

            phys_schema = await self._resolve_schema(catalog_id, conn)
            if not phys_schema:
                raise ValueError(f"Catalog '{catalog_id}' not found.")

            # Derive the physical collection id for locking and scope filtering.
            coll_phys: Optional[str] = None
            if collection_id is not None:
                coll_phys_row = await DQLQuery(
                    f'SELECT physical_id FROM "{phys_schema}".collections '
                    "WHERE id = :collection_id AND deleted_at IS NULL;",
                    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
                ).execute(conn, collection_id=collection_id)
                coll_phys = coll_phys_row

            # Serialize concurrent renames on the same asset in this scope.
            lock_key = (
                f"{phys_schema}:asset_rename:{coll_phys or '_cat_'}:{asset_id}"
            )
            await DQLQuery(
                "SELECT pg_advisory_xact_lock(hashtext(:lock_key))",
                result_handler=ResultHandler.SCALAR,
            ).execute(conn, lock_key=lock_key)

            # Existence check (404).  Also fetch asset_physical_id so the
            # reverse cache (asset_physical_id -> logical asset_id) can be
            # busted post-commit.
            if collection_id is not None and coll_phys is not None:
                existing_row = await DQLQuery(
                    f'SELECT asset_id, asset_physical_id::text FROM "{phys_schema}".assets '
                    "WHERE asset_id = :asset_id "
                    "AND collection_physical_id = :coll_phys "
                    "AND status <> 'deleted';",
                    result_handler=ResultHandler.ONE_OR_NONE,
                ).execute(conn, asset_id=asset_id, coll_phys=coll_phys)
            else:
                existing_row = await DQLQuery(
                    f'SELECT asset_id, asset_physical_id::text FROM "{phys_schema}".assets '
                    "WHERE asset_id = :asset_id AND status <> 'deleted' "
                    "ORDER BY created_at DESC LIMIT 1;",
                    result_handler=ResultHandler.ONE_OR_NONE,
                ).execute(conn, asset_id=asset_id)

            if existing_row is None:
                raise ValueError(
                    f"Asset '{asset_id}' not found in catalog '{catalog_id}'."
                )
            existing_physical_id: Optional[str] = existing_row[1]

            # Collision check — any existing row with the new_id (409).
            if collection_id is not None and coll_phys is not None:
                collision = await DQLQuery(
                    f'SELECT asset_id FROM "{phys_schema}".assets '
                    "WHERE asset_id = :new_id "
                    "AND collection_physical_id = :coll_phys;",
                    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
                ).execute(conn, new_id=new_id, coll_phys=coll_phys)
            else:
                collision = await DQLQuery(
                    f'SELECT asset_id FROM "{phys_schema}".assets '
                    "WHERE asset_id = :new_id;",
                    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
                ).execute(conn, new_id=new_id)

            if collision is not None:
                raise AssetRenameConflictError(new_id)

            # Update assets.asset_id (the mutable logical label).
            # asset_references keys on the immutable physical_id and needs
            # no update — the rename is complete in one statement.
            if collection_id is not None and coll_phys is not None:
                await DDLQuery(
                    f'UPDATE "{phys_schema}".assets '
                    "SET asset_id = :new_id "
                    "WHERE asset_id = :old_id "
                    "AND collection_physical_id = :coll_phys;",
                ).execute(conn, new_id=new_id, old_id=asset_id, coll_phys=coll_phys)
            else:
                await DDLQuery(
                    f'UPDATE "{phys_schema}".assets '
                    "SET asset_id = :new_id "
                    "WHERE asset_id = :old_id;",
                ).execute(conn, new_id=new_id, old_id=asset_id)

        # Bust read caches for both old and new logical ids post-commit.
        # The physical_id cache for the old id becomes stale (the label moved);
        # the new id cache needs to be warm-able immediately.  The underlying
        # physical_id is unchanged — only the logical key into this cache moves.
        self._invalidate_cache(asset_id, catalog_id, collection_id)
        self._invalidate_cache(new_id, catalog_id, collection_id)
        _invalidate_asset_physical_id_cache(catalog_id, asset_id, collection_id)
        _invalidate_asset_physical_id_cache(catalog_id, new_id, collection_id)
        # Also bust the reverse (physical_id -> logical asset_id) cache: the
        # physical_id is immutable but the logical label it maps to has changed.
        if existing_physical_id:
            _invalidate_asset_logical_id_cache(catalog_id, existing_physical_id)

        # ES items index keys on the immutable asset_physical_id UUID, not the
        # logical asset_id, so an asset rename leaves every indexed item doc
        # correct as-is.  No reindex is needed.
        return RenameResponse(
            old_id=asset_id,
            new_id=new_id,
            level="asset",
            warnings=[],
            reindex_required=False,
            iam_manual_update_required=False,
        )


# assets and asset_references tables are now created by
# AssetPostgresqlDriver._pg_asset_driver_init_tenant (priority 5) in
# modules/catalog/drivers/pg_asset_driver.py
