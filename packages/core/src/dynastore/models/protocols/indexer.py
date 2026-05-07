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

"""
Indexer protocol definitions.

Abstracts document indexing lifecycle so that event-driven modules can
dispatch indexing operations without coupling to a specific search backend.

Two protocol surfaces:

* :class:`Indexer` — slim generic surface (``index`` / ``index_bulk``).
  Every concrete indexer (public ES tenant index, private geoid-only ES
  index, vector DB, audit log, …) implements the same shape.  The
  :class:`IndexDispatcher` walks ``routing.operations[INDEX]`` and calls
  this surface uniformly; failure policy + outbox + circuit breaker are
  driver-agnostic.

* :class:`IndexerProtocol` — the historical fat surface (per-entity
  methods, private-index slots).  Retained for backward compatibility
  with ``ElasticsearchModule``; new code targets :class:`Indexer`.

Per-tier marker Protocols (``CatalogIndexer``, ``CollectionIndexer``,
``AssetIndexer``, ``ItemIndexer``) let drivers opt in to one or more tiers
they can serve as INDEX-role propagation targets.  Routing-config
self-registration validators walk these markers per tier to auto-populate
``operations[INDEX]`` with sensible async defaults.  Both metadata and
data are indexable — markers are tier-scoped, not metadata-vs-data.
"""

from __future__ import annotations

from typing import (
    Any, ClassVar, Dict, List, Literal, Optional, Protocol, Sequence,
    runtime_checkable,
)

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Generic Indexer surface (Phase 1 of the indexer-protocol harmonisation)
# ---------------------------------------------------------------------------


EntityType = Literal["catalog", "collection", "item", "asset"]
IndexOpType = Literal["upsert", "delete"]


class IndexOp(BaseModel):
    """A single index operation — opaque to the dispatcher.

    The dispatcher does not interpret ``payload``; each :class:`Indexer`
    implementation projects/serialises it as it sees fit.  ``entity_id`` is
    the stable identity within the ``(catalog, collection)`` scope set on
    :class:`IndexContext`.
    """

    op_type: IndexOpType = Field(description="``upsert`` or ``delete``.")
    entity_type: EntityType = Field(description="Tier of the indexed entity.")
    entity_id: str = Field(description="Stable identity within the ctx scope.")
    payload: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Document body for upsert; ``None`` for delete.",
    )

    model_config = {"frozen": True}


class IndexContext(BaseModel):
    """Per-call context — the scope and the live PG transaction handle.

    ``pg_conn`` is the load-bearing field for the production durability
    guarantee: when an indexer call needs to enqueue an outbox row (because
    the synchronous attempt failed and ``on_failure`` is ``OUTBOX``), the
    INSERT runs on this connection so the outbox row is committed (or
    rolled back) atomically with the upstream data write.
    """

    catalog: str
    collection: Optional[str] = None
    correlation_id: str = Field(default="")
    pg_conn: Optional[Any] = Field(
        default=None,
        description=(
            "Live PG connection / transaction handle from the caller. "
            "Used for in-TX outbox enqueue. ``None`` when called from a "
            "non-PG context (e.g. operator-triggered bulk reindex)."
        ),
    )

    model_config = {"arbitrary_types_allowed": True, "frozen": True}


class BulkResult(BaseModel):
    """Outcome of a bulk index call — per-op pass/fail summary."""

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    failures: List[Dict[str, Any]] = Field(default_factory=list)


@runtime_checkable
class Indexer(Protocol):
    """Slim, generic index sink.

    Every concrete indexer — public ES tenant index, private geoid-only
    ES index, OpenSearch, vector DB, audit log, future search engine —
    implements this same surface.  Routing config decides which fires
    per ``(catalog, collection)`` via ``operations[INDEX]``; the
    :class:`IndexDispatcher` walks the entries and calls this Protocol
    uniformly.

    Implementations remain free to expose richer per-backend operations
    (bulk reindex, ensure_index, mapping management) on their concrete
    classes — those are operator/admin surfaces, not part of the per-item
    write path.
    """

    indexer_id: ClassVar[str]

    async def ensure_indexer(self, ctx: IndexContext) -> None:
        """Ensure this indexer's per-tenant storage is provisioned.

        Each backend has different needs — ES creates a per-catalog
        index plus alias membership, a vector DB creates a collection
        with a configured dimension, an audit-log indexer is a no-op.
        Implementations MUST be idempotent: dispatched repeatedly per
        process, but only the first call per (indexer, catalog,
        collection) is uncached on the dispatcher side.

        Called by :class:`IndexDispatcher` before the first
        :meth:`index` / :meth:`index_bulk` for a given
        ``(catalog, collection)``.  Failures surface to the dispatcher
        and are governed by the routing entry's ``FailurePolicy`` —
        OUTBOX persists the obligation; the drain worker re-attempts
        ``ensure_indexer`` automatically.
        """
        ...

    async def index(self, ctx: IndexContext, op: IndexOp) -> None:
        """Apply a single index op (upsert or delete) to this sink.

        Must raise on transient/durable failure so the dispatcher can
        apply the configured ``FailurePolicy`` (FATAL → caller rollback,
        OUTBOX → enqueue retry row, WARN → log, IGNORE → silent skip).
        """
        ...

    async def index_bulk(
        self, ctx: IndexContext, ops: Sequence[IndexOp],
    ) -> BulkResult:
        """Apply a batch of index ops.  Per-op failures are reported in
        ``BulkResult.failures``; an unhandled exception aborts the batch
        and the dispatcher applies ``FailurePolicy`` to the whole batch.
        """
        ...


@runtime_checkable
class IndexerProtocol(Protocol):
    """
    Protocol for document indexing operations.

    Implementations manage the lifecycle of indexed documents —
    create/update, delete, and bulk reindex — without exposing backend
    specifics.  The event-driven module (``ElasticsearchModule``) is the
    primary consumer; it dispatches indexing tasks via this protocol.

    Implementors:
        - ``ElasticsearchModule`` in ``modules/elasticsearch/module.py``
          (dispatches tasks to the Elasticsearch cluster).
    """

    async def index_document(
        self,
        entity_type: Literal["catalog", "collection", "item", "asset"],
        entity_id: str,
        document: Dict[str, Any],
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        db_resource: Optional[Any] = None,
    ) -> None:
        """
        Index or update a single document.

        Args:
            entity_type: The entity kind being indexed.
            entity_id: Unique document identifier.
            document: Full document payload to index.
            catalog_id: Owning catalog (used for index routing/naming).
            collection_id: Owning collection (optional).
            db_resource: Database resource for transactional context.
        """
        ...

    async def delete_document(
        self,
        entity_type: Literal["catalog", "collection", "item", "asset"],
        entity_id: str,
        catalog_id: Optional[str] = None,
        db_resource: Optional[Any] = None,
    ) -> None:
        """
        Remove a document from the search index.

        Args:
            entity_type: The entity kind being deleted.
            entity_id: The document ID to delete.
            catalog_id: Owning catalog.
            db_resource: Database resource for transactional context.
        """
        ...

    async def bulk_reindex(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Trigger a bulk reindex of all items in a catalog or collection.

        Typically dispatched as a durable background task or Cloud Run
        Job rather than executed inline.

        Args:
            catalog_id: The catalog to reindex.
            collection_id: Optional single collection (if ``None``, all
                           collections in the catalog are reindexed).
            db_resource: Database resource for transactional context.

        Returns:
            Dict with reindex result metadata (``total_indexed``, ``status``).
        """
        ...

    async def ensure_index(
        self,
        entity_type: Literal["catalog", "collection", "item", "asset"],
        catalog_id: Optional[str] = None,
    ) -> None:
        """Ensure the index for the given entity type exists, creating it
        with the correct mapping if necessary.

        Args:
            entity_type: The index to ensure.
            catalog_id: Optional catalog scope hint.
        """
        ...


# ---------------------------------------------------------------------------
# Per-tier indexer marker Protocols
# ---------------------------------------------------------------------------


@runtime_checkable
class CatalogIndexer(Protocol):
    """Marker — driver indexes catalog-tier records.

    A driver opts in by setting ``is_catalog_indexer: ClassVar[bool] = True``.
    Routing-config self-registration validators walk this marker to
    auto-populate the catalog routing config's ``operations[INDEX]`` with
    ``write_mode='async'``, ``on_failure='warn'``.
    """

    is_catalog_indexer: ClassVar[bool]


@runtime_checkable
class CollectionIndexer(Protocol):
    """Marker — driver indexes collection-tier records.

    A driver opts in by setting ``is_collection_indexer: ClassVar[bool] = True``.
    Auto-registers into the collection routing config's ``operations[INDEX]``
    with ``write_mode='async'``, ``on_failure='warn'``.
    """

    is_collection_indexer: ClassVar[bool]


@runtime_checkable
class AssetIndexer(Protocol):
    """Marker — driver indexes asset-tier records (catalog + collection assets).

    Today the canonical implementer (``AssetElasticsearchDriver``)
    indexes BOTH catalog-level and collection-level assets in a single
    per-catalog index keyed by ``(catalog_id, nullable collection_id)``.
    The ``AssetIndexer`` marker is therefore tier-spanning at the
    catalog/collection level.

    A driver opts in by setting ``is_asset_indexer: ClassVar[bool] = True``.
    Auto-registers into the asset routing config's ``operations[INDEX]``
    with ``write_mode='async'``, ``on_failure='warn'``.

    Per-tier asset markers
    ----------------------
    :class:`ItemAssetIndexer` and :class:`PlatformAssetIndexer` (below)
    are the extension axis for tiers ``AssetIndexer`` does NOT cover
    today: item-embedded assets (currently stored as opaque blob in item
    docs — promoting them to first-class index entries is a deferred
    STAC read/write refactor) and platform-level assets (no design yet).
    Future drivers — or a future extension of ``AssetElasticsearchDriver``
    when item-asset promotion lands — opt in to those markers as the
    tiers they serve grow.
    """

    is_asset_indexer: ClassVar[bool]


@runtime_checkable
class ItemAssetIndexer(Protocol):
    """Marker — driver indexes item-embedded assets as first-class index entries.

    Today STAC item documents carry an embedded ``assets`` map that is
    stored as opaque blob (``mappings.py`` ``COMMON_PROPERTIES`` declares
    ``"assets": {"type": "object", "enabled": False}``).  A driver opting
    in to ``ItemAssetIndexer`` promotes those item-embedded assets to
    individual searchable documents in the assets index — making per-asset
    search + filter possible without re-shaping the item write path
    operators rely on.

    The opt-in flag ``is_item_asset_indexer: ClassVar[bool] = True`` is
    the extension axis only — no implementer ships in this PR.  Reserves
    the marker so future drivers (or a future ``AssetElasticsearchDriver``
    extension) can self-register without renaming.
    """

    is_item_asset_indexer: ClassVar[bool]


@runtime_checkable
class PlatformAssetIndexer(Protocol):
    """Marker — driver indexes platform-scope assets (above any catalog).

    A "platform" asset is one not owned by any specific catalog —
    typically global static resources (UI assets, shared imagery,
    cross-tenant references).  No "platform asset" concept exists in the
    asset model today (``AssetBase`` requires ``catalog_id``); this
    marker reserves the design space.

    The opt-in flag ``is_platform_asset_indexer: ClassVar[bool] = True``
    is the extension axis only — no implementer ships in this PR.
    """

    is_platform_asset_indexer: ClassVar[bool]


@runtime_checkable
class ItemIndexer(Protocol):
    """Marker — driver indexes item / feature records (the per-collection
    items table; aka record-tier indexer).

    A driver opts in by setting ``is_item_indexer: ClassVar[bool] = True``.
    Auto-registers into the items routing config's ``operations[INDEX]``
    with ``write_mode='async'``, ``on_failure='warn'``.
    """

    is_item_indexer: ClassVar[bool]
