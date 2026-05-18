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

from enum import Enum
from pydantic import (
    BaseModel,
    Field,
    model_validator,
)
from dynastore.models.mutability import Mutable
from dynastore.modules.db_config.plugin_config import PluginConfig
from typing import Any, ClassVar, List, Optional, Tuple

from dynastore.modules.storage.drivers.pg_sidecars.base import SidecarConfig
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import GeometriesSidecarConfig
from dynastore.modules.storage.routing_config import CatalogRoutingDefaults


class CollectionKind(str, Enum):
    VECTOR = "VECTOR"
    RASTER = "RASTER"
    RECORDS = "RECORDS"


class CollectionInfo(PluginConfig):
    """Semantic kind of a collection — VECTOR, RASTER, or RECORDS.

    Hoisted out of ``ItemsPostgresqlDriverConfig.collection_type`` (Phase 1.6
    of the config restructure).  The collection's kind is a property of the
    DATA, not of one storage backend; every capable driver (PG, Iceberg,
    DuckDB, …) reads this single config to decide its per-kind defaults
    (e.g. the PG driver omits the geometry sidecar when ``kind == RECORDS``).

    Setting this on the PG driver config is no longer accepted — the PG
    driver's apply handler refuses payloads containing the lifted key
    (it's removed from the model entirely so Pydantic rejects on parse).
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "info")
    _visibility: ClassVar[Optional[str]] = "collection"

    kind: Mutable[CollectionKind] = Field(
        default=CollectionKind.VECTOR,
        description=(
            "VECTOR (default — features with geometry), RECORDS "
            "(catalog-style records, no geometry), or RASTER (coverage / "
            "image data)."
        ),
    )


# Legacy alias
GeometryStorageConfig = GeometriesSidecarConfig


# --- Partitioning (Physical) ---


class CompositePartitionConfig(BaseModel):
    """
    Configuration for Composite Partitioning.
    Defines the ordered list of keys (columns) that form the partition key.
    These keys must be provided by the enabled sidecars (or the Hub).
    """

    enabled: Mutable[bool] = Field(default=False, description="Enable partitioning for this collection.")
    partition_keys: Mutable[List[str]] = Field(
        default_factory=list,
        description="Ordered list of column names to partition by (e.g. ['asset_id', 'h3_res12']).",
    )

    @model_validator(mode="after")
    def validate_keys(self) -> "CompositePartitionConfig":
        if self.enabled and not self.partition_keys:
            raise ValueError(
                "partition_keys must be provided if partitioning is enabled."
            )
        return self


# --- Main Catalog Config ---


class CollectionPluginConfig(PluginConfig):
    """Collection configuration — structural only.

    PG-specific fields (``sidecars``, ``partitioning``, ``collection_type``)
    have moved to ``ItemsPostgresqlDriverConfig``
    (operator-facing ``plugin_id = "items_postgresql_driver"`` per the
    TypedDriver bind — wire key drops the ``Config`` suffix).

    Storage routing is handled by ``ItemsRoutingConfig``
    (``plugin_id = "items_routing_config"``).

    Privacy is no longer expressed as a dedicated flag (#733). A collection
    is "private" iff its routing configs pin the private driver variants:
    ``items_elasticsearch_private_driver`` in ``ItemsRoutingConfig`` and/or
    ``collection_elasticsearch_private_driver`` in ``CollectionRoutingConfig``.
    The cascade rule (collection-private requires items-private) is enforced
    by validate handlers registered on the two routing configs themselves.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "envelope")
    _visibility: ClassVar[Optional[str]] = "collection"


    model_config = {"extra": "allow"}

    max_bulk_features: Mutable[int] = Field(
        default=10000,
        description="Maximum number of features allowed in a single bulk insert.",
    )

    ingest_chunk_size: Mutable[int] = Field(
        default=50,
        ge=1,
        le=10000,
        description=(
            "Number of items per write-transaction chunk during bulk ingest. "
            "Each chunk commits independently, releasing row locks before the "
            "next chunk opens its tx. Default 50 is safe for geometry-heavy "
            "collections (large per-row payloads); lightweight attribute-only "
            "collections can raise this to several hundred."
        ),
    )


CollectionPluginConfig.model_rebuild()


class CatalogPrivacy(PluginConfig):
    """Catalog-tier defaults seeded onto newly-created collections.

    Holds optional ``ItemsRoutingConfig`` / ``CollectionRoutingConfig``
    templates (see :class:`CatalogRoutingDefaults` in
    ``modules/storage/routing_config.py``) that
    ``apply_catalog_default_routing_seed`` writes onto a freshly-created
    collection. When either template pins the corresponding private driver
    variant the new collection is created as a private collection.

    Per-collection privacy is no longer a flag (#733 retired the
    ``CollectionPrivacy.is_private`` field): a collection is "private"
    iff its routing configs pin ``items_elasticsearch_private_driver``
    and/or ``collection_elasticsearch_private_driver``. Mid-lifecycle
    operators flip privacy by editing those routing configs directly —
    the cascade validator on the routing configs guarantees a sound
    combination.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "privacy")
    _visibility: ClassVar[Optional[str]] = "catalog"

    # ``CatalogRoutingDefaults`` lives in ``modules/storage/routing_config``
    # so the storage module's cascade handlers don't have to import catalog
    # configs (avoids the storage→catalog→storage cycle).
    collection_defaults: Mutable[CatalogRoutingDefaults] = Field(
        default_factory=CatalogRoutingDefaults,
        description=(
            "Optional routing templates seeded onto new collections in this "
            "catalog. Set ``items_routing`` and/or ``collection_routing`` to "
            "templates pinning the private driver variants to seed new "
            "collections as private; leave both ``None`` (the default) for "
            "no-op create-time behaviour."
        ),
    )


# CatalogLookupAudience moved to packages/extensions/geoid/.../configs.py

# ---------------------------------------------------------------------------
# #733 — collection-create routing seed flow
# ---------------------------------------------------------------------------


def _build_private_collection_routing() -> Any:
    """Build the ``CollectionRoutingConfig`` seeded onto a private collection.

    ``collection_router.py`` is config-driven: it resolves WRITE/READ/SEARCH
    from ``CollectionRoutingConfig``, falling back to the model defaults
    (public PG + public ES) when no explicit config is present.  All four
    operations must therefore be pinned explicitly:

    - WRITE / READ → ``collection_postgresql_driver`` (PG is the system of
      record for collection envelopes; durable, strongly consistent).
    - INDEX / SEARCH → ``collection_elasticsearch_private_driver`` (per-tenant
      private index; DENY policy blocks ``all_users`` GET access).

    Without an explicit SEARCH entry a private collection's
    ``search_collection_metadata`` call would fall through to the model
    default ``collection_elasticsearch_driver`` — the PUBLIC shared index —
    leaking private collection envelopes into public search results.
    """
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig,
        FailurePolicy,
        Operation,
        OperationDriverEntry,
        WriteMode,
    )

    return CollectionRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.INDEX: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_private_driver",
                    write_mode=WriteMode.ASYNC,
                    on_failure=FailurePolicy.OUTBOX,
                    source="auto",
                ),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_private_driver",
                    source="auto",
                ),
            ],
        },
    )


def _build_private_items_routing() -> Any:
    """Build the items-tier ``ItemsRoutingConfig`` template that pins
    ``items_elasticsearch_private_driver``.  Default shape used by
    ``CatalogRoutingDefaults`` when an operator opts into create-time
    privacy seeding without supplying an explicit template.
    """
    from dynastore.modules.storage.routing_config import (
        FailurePolicy,
        ItemsRoutingConfig,
        Operation,
        OperationDriverEntry,
        WriteMode,
    )

    return ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_private_driver",
                    write_mode=WriteMode.ASYNC,
                    on_failure=FailurePolicy.OUTBOX,
                    source="auto",
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(driver_ref="items_postgresql_driver"),
            ],
            Operation.INDEX: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_private_driver",
                    write_mode=WriteMode.ASYNC,
                    on_failure=FailurePolicy.OUTBOX,
                    input_transformers=("private_entity_transformer",),
                    source="auto",
                ),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_private_driver",
                    output_transformers=("private_entity_transformer",),
                    source="auto",
                ),
            ],
            Operation.TRANSFORM: [
                OperationDriverEntry(
                    driver_ref="private_entity_transformer",
                    source="auto",
                ),
            ],
        },
    )


async def apply_catalog_default_routing_seed(
    catalog_id: str,
    collection_id: str,
    *,
    configs: Any,
    db_resource: Any = None,
) -> bool:
    """Seed a freshly-created collection's routing configs from the catalog's
    :class:`CatalogPrivacy.collection_defaults` templates (#733).

    Returns ``True`` iff at least one routing template was written.  No-ops
    when:

    - The catalog has no ``CatalogPrivacy`` row yet, or
    - Both ``collection_defaults.items_routing`` and ``collection_routing``
      are ``None`` (the default — no create-time routing override), or
    - ``configs`` is ``None`` (early-fixture / partial-deployment path).

    Apply ordering matters: items-routing template lands BEFORE the
    collection-routing template so the cascade validator on the collection
    routing finds the private items driver already pinned.
    """
    if configs is None:
        return False

    from dynastore.models.driver_context import DriverContext
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig,
        ItemsRoutingConfig,
    )

    ctx = DriverContext(db_resource=db_resource) if db_resource is not None else None

    try:
        policy = await configs.get_config(
            CatalogPrivacy, catalog_id=catalog_id, ctx=ctx,
        )
    except Exception:
        return False
    if not isinstance(policy, CatalogPrivacy):
        return False

    items_template = policy.collection_defaults.items_routing
    coll_template = policy.collection_defaults.collection_routing
    if items_template is None and coll_template is None:
        return False

    wrote_any = False
    if items_template is not None:
        await configs.set_config(
            ItemsRoutingConfig,
            items_template,
            catalog_id=catalog_id,
            collection_id=collection_id,
            ctx=ctx,
        )
        wrote_any = True
    if coll_template is not None:
        await configs.set_config(
            CollectionRoutingConfig,
            coll_template,
            catalog_id=catalog_id,
            collection_id=collection_id,
            ctx=ctx,
        )
        wrote_any = True
    return wrote_any


# ---------------------------------------------------------------------------
# Catalog-tier lifecycle hook (#733 — routing-template driven)
# ---------------------------------------------------------------------------


def _items_template_has_private_driver(items_template: Any) -> bool:
    """Return True iff the embedded items-routing template pins the
    items-private driver in any operation."""
    if items_template is None:
        return False
    from dynastore.modules.storage.routing_config import (
        _PRIVATE_ITEMS_DRIVER_ID,
    )
    for entries in items_template.operations.values():
        for entry in entries:
            if entry.driver_ref == _PRIVATE_ITEMS_DRIVER_ID:
                return True
    return False


def _collection_template_has_private_driver(coll_template: Any) -> bool:
    """Return True iff the embedded collection-routing template pins the
    collection-private driver in any operation."""
    if coll_template is None:
        return False
    from dynastore.modules.storage.routing_config import (
        _PRIVATE_COLLECTION_DRIVER_ID,
    )
    for entries in coll_template.operations.values():
        for entry in entries:
            if entry.driver_ref == _PRIVATE_COLLECTION_DRIVER_ID:
                return True
    return False


async def _on_apply_catalog_privacy(
    config: "CatalogPrivacy",
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Any,
) -> None:
    """Eagerly create per-tenant private indexes when the catalog's
    routing-seed templates pin private driver variants (#733).

    When ``collection_defaults.items_routing`` pins
    ``items_elasticsearch_private_driver``, call ``ensure_storage(catalog_id)``
    on every tenant-isolated ``CollectionItemsStore`` (the items-private
    driver). When ``collection_defaults.collection_routing`` pins
    ``collection_elasticsearch_private_driver``, do the same on every
    tenant-isolated ``CollectionStore`` (the collection-private driver).

    Idempotent — both drivers' ``ensure_storage`` swallow
    ``resource_already_exists`` so re-applying the same policy is a no-op
    at the ES layer.

    No-op when:

    - Neither template is set, or neither template pins a private driver;
    - ``catalog_id`` is ``None`` (platform-tier write — no tenant to bootstrap);
    - The relevant private driver isn't installed (deployment SCOPE excludes
      ``elasticsearch_private``).
    """
    import logging
    logger = logging.getLogger(__name__)

    if not catalog_id:
        return

    items_private = _items_template_has_private_driver(
        config.collection_defaults.items_routing,
    )
    coll_private = _collection_template_has_private_driver(
        config.collection_defaults.collection_routing,
    )
    if not (items_private or coll_private):
        return

    from dynastore.tools.discovery import get_protocols
    from dynastore.models.protocols.entity_store import (
        CollectionStore,
        EntityStoreCapability,
    )
    from dynastore.models.protocols.storage_driver import (
        Capability,
        CollectionItemsStore,
    )

    if items_private:
        for d in get_protocols(CollectionItemsStore):
            if Capability.TENANT_ISOLATED not in getattr(d, "capabilities", frozenset()):
                continue
            try:
                await d.ensure_storage(catalog_id)
            except Exception as exc:
                logger.warning(
                    "CatalogPrivacy apply: items ensure_storage(%r) on %s failed: %s",
                    catalog_id, type(d).__name__, exc,
                )

    if coll_private:
        for d in get_protocols(CollectionStore):
            if EntityStoreCapability.TENANT_ISOLATED not in getattr(d, "capabilities", frozenset()):
                continue
            try:
                await d.ensure_storage(catalog_id)
            except Exception as exc:
                logger.warning(
                    "CatalogPrivacy apply: collection ensure_storage(%r) on %s failed: %s",
                    catalog_id, type(d).__name__, exc,
                )


CatalogPrivacy.register_apply_handler(_on_apply_catalog_privacy)
