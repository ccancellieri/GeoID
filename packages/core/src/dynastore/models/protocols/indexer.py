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
Indexer protocol definitions.

Abstracts document indexing lifecycle so that event-driven modules can
dispatch indexing operations without coupling to a specific search backend.

Two protocol surfaces:

* :class:`Indexer` — slim generic surface (``index`` / ``index_bulk``).
  Every concrete indexer (public ES tenant index, private geoid-only ES
  index, vector DB, audit log, …) implements the same shape.  The
  :class:`IndexDispatcher` walks the ``INDEX``-lane entries in
  ``routing.operations[INDEX]`` and calls this surface uniformly; lane
  membership IS the materialization-target role.

* :class:`IndexerProtocol` — the historical fat surface (per-entity
  methods, private-index slots).  Retained for backward compatibility
  with ``ElasticsearchModule``; new code targets :class:`Indexer`.

:class:`IndexTierDriver` is the discovery/seeding/validation-time marker:
a driver declares which tiers it materializes via
``index_tiers: ClassVar[FrozenSet[str]]`` (values from :data:`IndexTier`).
Routing-config self-registration walks this marker, checked BY VALUE, to
auto-populate ``operations[INDEX]`` with entries for the requested tier.
Both metadata and data are indexable — tiers are orthogonal to the
metadata-vs-data distinction.  Runtime dispatch never reads
``index_tiers``: once an entry lands in ``operations[INDEX]``, lane
membership alone drives it.
"""

from __future__ import annotations

from typing import (
    Any, ClassVar, Dict, FrozenSet, List, Literal, Optional, Protocol,
    Sequence, runtime_checkable,
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
    write_id: Optional[str] = Field(
        default=None,
        description="Logical primary write operation id, when available.",
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
    entity_type: Optional[EntityType] = Field(
        default=None,
        description=(
            "Tier of the ops being dispatched in this call. Read by the "
            "default routing resolver to pick the right PluginConfig: "
            "``item`` -> ItemsRoutingConfig, ``collection`` -> "
            "CollectionRoutingConfig, ``catalog`` -> CatalogRoutingConfig, "
            "``asset`` -> AssetRoutingConfig. ``None`` falls back to "
            "CollectionRoutingConfig for back-compat with pre-#810 callers. "
            "Per-call dispatch is homogeneous (all ops share the same "
            "entity_type), so the resolver only needs the context value."
        ),
    )
    lifecycle_status: Optional[str] = Field(
        default=None,
        description=(
            "Transitional lifecycle overlay to stamp on the indexed document "
            "when non-None (e.g. ``'provisioning'`` during async init).  "
            "None (the default) means the field is omitted from the indexed "
            "doc so active / already-indexed documents remain visible.  Only "
            "meaningful for collection-tier ops; items and assets ignore it."
        ),
    )

    model_config = {"arbitrary_types_allowed": True, "frozen": True}


class BulkResult(BaseModel):
    """Outcome of a bulk index call — per-op pass/fail summary."""

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    failures: List[Dict[str, Any]] = Field(default_factory=list)


# Cap the per-indexer failure-detail sample carried across every site that
# merges ``BulkResult``s (chunk aggregation in ``IndexDispatcher``, per-batch
# aggregation in ``item_service``, and the cross-batch accumulator in the
# ingestion task). The accumulated ``failures`` list is only ever read as a
# debugging sample — indexer health is classified purely on the integer
# counts — so an unbounded concatenation across chunks/batches is pure
# memory growth. On a large ingest against a degraded secondary index (e.g.
# an Elasticsearch endpoint under load returning per-doc errors), each
# failure dict carries an echoed payload and the list grows without bound,
# driving peak RSS to O(dataset) instead of O(chunk). Bounding at every
# merge site — not just the outermost one — is what keeps peak memory
# proportional to a single chunk.
MAX_ACCUMULATED_FAILURE_SAMPLES = 100


def merge_bulk_results(prev: "BulkResult", curr: "BulkResult") -> "BulkResult":
    """Merge two :class:`BulkResult`\\ s, capping the accumulated
    ``failures`` detail list at :data:`MAX_ACCUMULATED_FAILURE_SAMPLES`.

    The integer counts (``total``/``succeeded``/``failed``) stay exact
    regardless of how many merges are folded in; only the debugging sample
    is bounded.
    """
    return BulkResult(
        total=prev.total + curr.total,
        succeeded=prev.succeeded + curr.succeeded,
        failed=prev.failed + curr.failed,
        failures=(prev.failures + curr.failures)[-MAX_ACCUMULATED_FAILURE_SAMPLES:],
    )


@runtime_checkable
class Indexer(Protocol):
    """Slim, generic index sink.

    Every concrete indexer — public ES tenant index, private geoid-only
    ES index, OpenSearch, vector DB, audit log, future search engine —
    implements this same surface.  Routing config decides which fires
    per ``(catalog, collection)`` via the ``INDEX``-lane entries in
    ``operations[INDEX]``; the :class:`IndexDispatcher` walks those
    entries and calls this Protocol uniformly.

    Implementations remain free to expose richer per-backend operations
    (bulk reindex, ensure_index, mapping management) on their concrete
    classes — those are operator/admin surfaces, not part of the per-item
    write path.

    Identity: ``_to_snake(type(driver).__name__)`` — same convention used
    by ``DriverRegistry`` and ``_self_register_indexers_into``.  No
    separate ``indexer_id`` attribute.
    """

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
# Index-tier marker Protocol
# ---------------------------------------------------------------------------


IndexTier = Literal[
    "catalog", "collection", "item", "asset", "item_asset", "platform_asset",
]
"""The tier vocabulary a driver can claim via ``index_tiers``.

``item_asset`` and ``platform_asset`` are reserved tokens — no implementer
ships in this PR:

* ``item_asset`` — item-embedded assets promoted to first-class index
  entries.  Today STAC item documents carry an embedded ``assets`` map
  stored as opaque blob (``mappings.py`` ``COMMON_PROPERTIES`` declares
  ``"assets": {"type": "object", "enabled": False}``); promoting them is a
  deferred STAC read/write refactor.
* ``platform_asset`` — assets above any catalog scope.  No "platform
  asset" concept exists in the asset model today (``AssetBase`` requires
  ``catalog_id``); this token reserves the design space.
"""


@runtime_checkable
class IndexTierDriver(Protocol):
    """Marker — driver declares which INDEX-lane tiers it materializes.

    Replaces the six per-tier boolean marker Protocols
    (``CatalogIndexer``, ``CollectionIndexer``, ``AssetIndexer``,
    ``ItemIndexer``, ``ItemAssetIndexer``, ``PlatformAssetIndexer``) with
    one declarative ClassVar, checked BY VALUE rather than by attribute
    presence — a driver opts in to one or more tiers via
    ``index_tiers: ClassVar[FrozenSet[str]]`` (values from
    :data:`IndexTier`), e.g. ``index_tiers = frozenset({"catalog"})``.

    Structural discovery (``isinstance(obj, IndexTierDriver)`` /
    ``get_protocols(IndexTierDriver)``) only tests that ``index_tiers`` is
    present on the class — the presence-not-value trap that made the old
    boolean markers undeletable.  Consumers MUST additionally check
    ``tier in driver.index_tiers`` to classify which tier(s) a driver
    serves; a driver indexing multiple tiers lists them all in the one
    frozenset.

    Routing-config self-registration (``_self_register_indexers_into``)
    walks this marker per tier to auto-populate the matching routing
    config's ``operations[INDEX]``.  A driver claiming a tier must also
    structurally satisfy :class:`Indexer` (``index_bulk``) — validated at
    routing-config validate time (``_validate_routing_entries`` /
    ``_validate_collection_routing_config``), not here.

    This marker is a discovery/seeding/validation-time construct only —
    runtime dispatch never reads ``index_tiers``; once a driver lands in
    ``operations[INDEX]``, lane membership alone drives it.
    """

    index_tiers: ClassVar[FrozenSet[str]]
