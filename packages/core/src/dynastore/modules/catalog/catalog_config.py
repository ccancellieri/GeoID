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
from dynastore.modules.db_config.platform_config_service import (
    Immutable,
    PluginConfig,
)
from typing import Any, ClassVar, List, Optional, Tuple

from dynastore.modules.storage.drivers.pg_sidecars.base import SidecarConfig
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import GeometriesSidecarConfig


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

    kind: CollectionKind = Field(
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

    enabled: bool = Field(default=False, description="Enable partitioning for this collection.")
    partition_keys: List[str] = Field(
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

    Privacy state has moved to ``CollectionPrivacy`` at
    ``(platform, catalog, collection, privacy)`` — see the dedicated class
    below.  Cycle F.0d harmonised the privacy surface so catalog and
    collection both expose ``is_private: bool`` under a uniform
    ``privacy`` slot.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "envelope")
    _visibility: ClassVar[Optional[str]] = "collection"


    model_config = {"extra": "allow"}

    max_bulk_features: int = Field(
        default=10000,
        description="Maximum number of features allowed in a single bulk insert.",
    )

    ingest_chunk_size: int = Field(
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


class CollectionPrivacy(PluginConfig):
    """Per-collection privacy state.

    Cycle F.0d hoisted ``is_private`` out of ``CollectionPluginConfig``
    onto its own ``PluginConfig`` so the catalog and collection tiers
    expose a uniform privacy surface (per H4 — both tiers carry
    ``is_private: bool`` under a ``privacy`` slot).

    When ``is_private == True``, the routing-resolution layer
    auto-substitutes the private variants of the items + collection ES
    drivers in INDEX/SEARCH operations, and the per-tenant private
    indexes (``{prefix}-geoid-{catalog}`` for items +
    ``{prefix}-{catalog}-collections-private`` for collection envelopes)
    carry DENY policies blocking GET access to ``all_users``.

    Immutable: flipping privacy on an existing collection requires
    moving its docs across indexes (schema-level operation, not a
    runtime PATCH).

    Cascade rule: collection-private REQUIRES items-private (reverse
    direction allowed); the cascade is enforced by the apply handlers
    on this class and ``ItemsRoutingConfig``.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "privacy")
    _visibility: ClassVar[Optional[str]] = "collection"

    is_private: Immutable[bool] = Field(
        default=False,
        description=(
            "When True, the items + collection ES drivers route to the "
            "private variants and the per-tenant indexes carry DENY "
            "policies blocking GET access to all_users.  Immutable: "
            "flipping privacy on an existing collection requires moving "
            "its docs across indexes."
        ),
    )


# Cycle E.2 / F.0d — register the collection-side privacy-cascade handler.
# The helper itself lives in ``modules/storage/routing_config`` because it
# inspects ``ItemsRoutingConfig``; we do the registration HERE so it
# fires only after ``CollectionPrivacy`` is fully defined (avoids
# the storage→catalog→storage import cycle that would trigger if the
# storage module attempted ``from modules.catalog.catalog_config import
# CollectionPrivacy`` at its own module-load time).
def _register_collection_privacy_cascade_handler() -> None:
    from dynastore.modules.storage.routing_config import (
        _enforce_collection_privacy_cascade,
    )

    CollectionPrivacy.register_apply_handler(
        _enforce_collection_privacy_cascade,
    )


_register_collection_privacy_cascade_handler()


class CollectionPrivacyDefaults(BaseModel):
    """Catalog-level defaults seeded onto newly-created collections.

    Mirrors the leaf shape of ``CollectionPrivacy`` so operators see
    the same field name at both tiers (H4 harmonisation).  This is a
    plain ``BaseModel`` (not a ``PluginConfig``) because it is embedded
    inside ``CatalogPrivacy``; the per-collection persisted shape is
    ``CollectionPrivacy``.
    """

    is_private: bool = Field(
        default=False,
        description=(
            "Default ``is_private`` value for newly-created collections "
            "in this catalog.  Existing collections are unaffected by "
            "changes — their own ``CollectionPrivacy.is_private`` is "
            "authoritative."
        ),
    )


class CatalogPrivacy(PluginConfig):
    """Catalog-tier privacy defaults — visibility, future RBAC hooks.

    Holds knobs that apply uniformly across a catalog's collections
    unless overridden per-collection.  Today: ``collection_defaults``
    (``is_private`` default seeded onto new collections).  Future:
    read/write permission defaults for the ``is_private`` cascade.

    The privacy default is read at collection-create time to seed
    ``CollectionPrivacy.is_private`` when the operator does not pass
    one explicitly.  After creation the collection's own
    ``CollectionPrivacy.is_private`` is the source of truth — flipping
    the catalog default does not retroactively re-flag existing
    collections.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "privacy")
    _visibility: ClassVar[Optional[str]] = "catalog"

    collection_defaults: CollectionPrivacyDefaults = Field(
        default_factory=CollectionPrivacyDefaults,
        description=(
            "Default privacy state seeded onto newly-created collections "
            "in this catalog.  When ``collection_defaults.is_private`` is "
            "True, new collections are seeded with the private items + "
            "collection ES drivers pinned and the per-tenant private "
            "indexes pre-created."
        ),
    )


# ---------------------------------------------------------------------------
# Cycle E.2.c — collection-create seed flow
# ---------------------------------------------------------------------------


async def apply_catalog_default_privacy_seed(
    catalog_id: str,
    collection_id: str,
    *,
    configs: Any,
    db_resource: Any = None,
) -> bool:
    """Seed a freshly-created collection's privacy state from the
    catalog's ``CatalogPrivacy.collection_defaults.is_private``.

    Returns ``True`` iff the seed was applied (catalog default is
    ``is_private=True`` AND ConfigsProtocol is reachable).  No-ops when:

    - The catalog has no ``CatalogPrivacy`` row yet (default is
      ``is_private=False`` per the field default; nothing to seed);
    - The catalog default is ``is_private=False`` (matches the
      ``CollectionPrivacy.is_private`` default — no writes needed);
    - ``configs`` is ``None`` (early-fixture / partial-deployment path
      — caller's apply handler eventually catches the cascade on
      a later config write).

    Cycle E.2.c slice 1: the wiring lives in
    ``CollectionService.create_collection``.  Mid-lifecycle privacy
    flips on existing collections still go through the cascade
    validator (operator pins items routing first, then sets
    ``is_private=True``); this helper covers ONLY the
    create-time seed path.

    Apply ordering matters: we write ``ItemsRoutingConfig`` BEFORE
    ``CollectionPrivacy`` so the cascade validator on the
    second write finds the private driver already pinned.  The
    inverse order would trigger an immediate cascade rejection on
    ``CollectionPrivacy`` apply (because items routing wouldn't
    yet have the private driver).
    """
    if configs is None:
        return False

    from dynastore.models.driver_context import DriverContext
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig,
        FailurePolicy,
        ItemsRoutingConfig,
        Operation,
        OperationDriverEntry,
        WriteMode,
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
    if not policy.collection_defaults.is_private:
        return False

    private_items_routing = ItemsRoutingConfig(
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
        },
    )
    await configs.set_config(
        ItemsRoutingConfig,
        private_items_routing,
        catalog_id=catalog_id,
        collection_id=collection_id,
        ctx=ctx,
    )
    await configs.set_config(
        CollectionPrivacy,
        CollectionPrivacy(is_private=True),
        catalog_id=catalog_id,
        collection_id=collection_id,
        ctx=ctx,
    )

    # Cycle E.2.c slice 3 — also seed CollectionRoutingConfig with the
    # collection-envelope private driver pinned (Cycle E.2.b shipped
    # this driver class).  Without this third write the collection
    # envelope would fall through to the public shared
    # ``{prefix}-collections`` index — the catalog-wide DENY policy
    # (owned by the items-private driver via the cascade rule) would
    # still block public GET reads, but operators auditing the index
    # list would see the envelope in the public index, which is
    # surprising for a "private" collection.  Symmetrical seed: items
    # AND envelope both land in per-tenant private indexes when the
    # catalog default is "private".
    #
    # Independent of the cascade ordering above — the privacy cascade
    # validator gates ItemsRoutingConfig vs CollectionPluginConfig only;
    # CollectionRoutingConfig has no cross-config constraint, so this
    # write's position relative to the pair above is incidental (we
    # put it last for clarity).
    private_collection_routing = CollectionRoutingConfig(
        operations={
            Operation.INDEX: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_private_driver",
                    write_mode=WriteMode.ASYNC,
                    on_failure=FailurePolicy.OUTBOX,
                    source="auto",
                ),
            ],
        },
    )
    await configs.set_config(
        CollectionRoutingConfig,
        private_collection_routing,
        catalog_id=catalog_id,
        collection_id=collection_id,
        ctx=ctx,
    )
    return True


# ---------------------------------------------------------------------------
# Cycle E.2.c slice 2 — catalog-tier lifecycle hook
# ---------------------------------------------------------------------------


async def _on_apply_catalog_privacy(
    config: "CatalogPrivacy",
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Any,
) -> None:
    """Eagerly create per-tenant private indexes when the catalog default
    privacy is ``is_private=True`` (Cycle E.2.c slice 2).

    Fires whenever an operator writes a ``CatalogPrivacy`` row.
    When ``collection_defaults.is_private`` is True, we proactively
    call ``ensure_storage(catalog_id)`` on both per-tenant private
    drivers so the indexes exist before any collection-create lands.
    Without this hook the drivers' lazy-create fallback covers the
    first-write race, but eager creation:

    - Removes the first-write latency spike;
    - Lets operators verify ``GET _cat/indices`` shows the per-tenant
      private indexes immediately after flipping the policy.

    Idempotent — both drivers' ``ensure_storage`` swallow
    ``resource_already_exists`` so re-applying the same policy is a
    no-op at the ES layer.

    No-op when:

    - ``collection_defaults.is_private`` is False — public is the
      default; flipping back to public does NOT drop the existing
      per-tenant indexes (the catalog policy applies to NEW
      collections; existing ``CollectionPrivacy.is_private`` flags
      remain authoritative for already-created collections).
    - ``catalog_id`` is ``None`` (platform-tier write — there's no
      tenant to bootstrap indexes for).
    - The relevant private driver isn't installed (deployment SCOPE
      excludes ``elasticsearch_private``).
    """
    import logging
    logger = logging.getLogger(__name__)

    if not config.collection_defaults.is_private:
        return
    if not catalog_id:
        return

    from dynastore.tools.discovery import get_protocols
    from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
        ItemsElasticsearchPrivateDriver,
    )
    from dynastore.modules.storage.drivers.elasticsearch_private.collection_driver import (
        CollectionElasticsearchPrivateDriver,
    )
    from dynastore.models.protocols.entity_store import CollectionStore
    from dynastore.models.protocols.storage_driver import CollectionItemsStore

    items_private: Optional[ItemsElasticsearchPrivateDriver] = next(
        (
            d
            for d in get_protocols(CollectionItemsStore)
            if isinstance(d, ItemsElasticsearchPrivateDriver)
        ),
        None,
    )
    if items_private is not None:
        try:
            await items_private.ensure_storage(catalog_id)
        except Exception as exc:
            logger.warning(
                "CatalogPrivacy apply: items-private ensure_storage(%r) failed: %s",
                catalog_id, exc,
            )

    coll_private: Optional[CollectionElasticsearchPrivateDriver] = next(
        (
            d
            for d in get_protocols(CollectionStore)
            if isinstance(d, CollectionElasticsearchPrivateDriver)
        ),
        None,
    )
    if coll_private is not None:
        try:
            await coll_private.ensure_storage(catalog_id)
        except Exception as exc:
            logger.warning(
                "CatalogPrivacy apply: collection-private ensure_storage(%r) failed: %s",
                catalog_id, exc,
            )


CatalogPrivacy.register_apply_handler(_on_apply_catalog_privacy)
