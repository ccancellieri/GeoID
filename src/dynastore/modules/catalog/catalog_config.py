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
from typing import Any, ClassVar, List, Literal, Optional, Tuple

from dynastore.modules.storage.drivers.pg_sidecars.base import SidecarConfig
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import GeometriesSidecarConfig


class CollectionTypeEnum(str, Enum):
    VECTOR = "VECTOR"
    RASTER = "RASTER"
    RECORDS = "RECORDS"


class CollectionType(PluginConfig):
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
    _address: ClassVar[Tuple[str, str, Optional[str]]] = ("collection", "type", None)
    _visibility: ClassVar[Optional[str]] = "collection"

    kind: CollectionTypeEnum = Field(
        default=CollectionTypeEnum.VECTOR,
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
    """
    _address: ClassVar[Tuple[str, str, Optional[str]]] = ("catalog", "collection", None)
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

    is_private: Immutable[bool] = Field(
        default=False,
        description=(
            "Privacy flag for this collection (Cycle E.2). When True, the "
            "routing-resolution layer auto-substitutes the private variants "
            "of the items + collection ES drivers in INDEX/SEARCH "
            "operations, and the per-tenant private indexes "
            "({prefix}-geoid-{catalog} for items + "
            "{prefix}-{catalog}-collections-private for collection "
            "envelopes) carry DENY policies blocking GET access to "
            "all_users.  Immutable: flipping privacy on an existing "
            "collection requires moving its docs across indexes "
            "(schema-level operation, not a runtime PATCH).  Cascade "
            "rule: collection-private REQUIRES items-private (reverse "
            "direction allowed); the cascade is enforced by the apply "
            "handlers on this class and ItemsRoutingConfig."
        ),
    )


CollectionPluginConfig.model_rebuild()


# Cycle E.2 — register the collection-side privacy-cascade handler.  The
# helper itself lives in ``modules/storage/routing_config`` because it
# inspects ``ItemsRoutingConfig``; we do the registration HERE so it
# fires only after ``CollectionPluginConfig`` is fully defined (avoids
# the storage→catalog→storage import cycle that would trigger if the
# storage module attempted ``from modules.catalog.catalog_config import
# CollectionPluginConfig`` at its own module-load time).
def _register_collection_privacy_cascade_handler() -> None:
    from dynastore.modules.storage.routing_config import (
        _enforce_collection_privacy_cascade,
    )

    CollectionPluginConfig.register_apply_handler(
        _enforce_collection_privacy_cascade,
    )


_register_collection_privacy_cascade_handler()


class CatalogPolicyConfig(PluginConfig):
    """Catalog-tier policy defaults — visibility, future RBAC hooks.

    Holds knobs that apply uniformly across a catalog's collections unless
    overridden per-collection.  Today: ``default_collection_privacy``.
    Future: read/write permission defaults for the ``is_private`` cascade.

    The privacy default is read at collection-create time to seed
    ``CollectionPluginConfig.is_private`` when the operator does not pass
    one explicitly.  After creation the collection's own flag is the
    source of truth — flipping the catalog default does not retroactively
    re-flag existing collections.
    """
    _address: ClassVar[Tuple[str, str, Optional[str]]] = ("catalog", "policy", None)
    _visibility: ClassVar[Optional[str]] = "catalog"

    default_collection_privacy: Literal["public", "private"] = Field(
        default="public",
        description=(
            "Default privacy for newly-created collections in this catalog. "
            "When 'private', new collections are seeded with "
            "is_private=True (auto-resolves the private items + collection ES "
            "indexers, applies DENY policies). Existing collections are "
            "unaffected by changes to this default — their own is_private "
            "field is authoritative."
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
    catalog's ``CatalogPolicyConfig.default_collection_privacy``.

    Returns ``True`` iff the seed was applied (catalog default is
    ``"private"`` AND ConfigsProtocol is reachable).  No-ops when:

    - The catalog has no ``CatalogPolicyConfig`` row yet (default is
      ``"public"`` per the field default; nothing to seed);
    - The catalog policy is ``"public"`` (matches the
      ``CollectionPluginConfig.is_private`` default — no writes
      needed);
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
    ``CollectionPluginConfig`` so the cascade validator on the
    second write finds the private driver already pinned.  The
    inverse order would trigger an immediate cascade rejection on
    ``CollectionPluginConfig`` apply (because items routing wouldn't
    yet have the private driver).
    """
    if configs is None:
        return False

    from dynastore.models.driver_context import DriverContext
    from dynastore.modules.storage.routing_config import (
        FailurePolicy,
        ItemsRoutingConfig,
        Operation,
        OperationDriverEntry,
        WriteMode,
    )

    ctx = DriverContext(db_resource=db_resource) if db_resource is not None else None

    try:
        policy = await configs.get_config(
            CatalogPolicyConfig, catalog_id=catalog_id, ctx=ctx,
        )
    except Exception:
        return False
    if not isinstance(policy, CatalogPolicyConfig):
        return False
    if policy.default_collection_privacy != "private":
        return False

    private_routing = ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_id="items_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
                OperationDriverEntry(
                    driver_id="items_elasticsearch_private_driver",
                    write_mode=WriteMode.ASYNC,
                    on_failure=FailurePolicy.OUTBOX,
                    source="auto",
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(driver_id="items_postgresql_driver"),
            ],
        },
    )
    await configs.set_config(
        ItemsRoutingConfig,
        private_routing,
        catalog_id=catalog_id,
        collection_id=collection_id,
        ctx=ctx,
    )
    await configs.set_config(
        CollectionPluginConfig,
        CollectionPluginConfig(is_private=True),
        catalog_id=catalog_id,
        collection_id=collection_id,
        ctx=ctx,
    )
    return True
