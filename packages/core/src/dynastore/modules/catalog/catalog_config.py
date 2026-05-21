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
from dynastore.modules.storage.hints import Hint


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
    _freeze_at: ClassVar[Optional[str]] = "collection"

    kind: Mutable[CollectionKind] = Field(
        default=CollectionKind.VECTOR,
        description=(
            "VECTOR (default — features with geometry), RECORDS "
            "(catalog-style records, no geometry), or RASTER (coverage / "
            "image data)."
        ),
    )


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
    is "private" iff its items routing config pins
    ``items_elasticsearch_private_driver`` in ``ItemsRoutingConfig``.
    Collection envelopes for private catalogs are stored in PostgreSQL only
    — no ES collection-private index (#1047).
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "envelope")
    _freeze_at: ClassVar[Optional[str]] = "collection"


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


# CatalogLookupAudience moved to packages/extensions/geoid/.../configs.py

def _build_private_items_routing() -> Any:
    """Build the items-tier ``ItemsRoutingConfig`` template that pins
    ``items_elasticsearch_private_driver``.  Used by the ``private_catalog``
    routing preset to seed catalog-scope items routing.
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
                    write_mode=WriteMode.SYNC
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

            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_private_driver",
                    hints={Hint.GEOMETRY_SIMPLIFIED},
                    source="auto",
                ),
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    hints={Hint.GEOMETRY_EXACT},
                    write_mode=WriteMode.SYNC,
                    source="auto",
                )
            ]
        },
    )