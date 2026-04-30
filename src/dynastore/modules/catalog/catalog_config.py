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
    PluginConfig,
)
from typing import ClassVar, List, Optional, Tuple

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

    Storage routing is handled by ``CollectionRoutingConfig``
    (``plugin_id = "collection_routing_config"``).
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


CollectionPluginConfig.model_rebuild()
