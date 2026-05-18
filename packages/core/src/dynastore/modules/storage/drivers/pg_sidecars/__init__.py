from typing import Any, List

from .base import (
    SidecarProtocol,
    SidecarConfig,
    FeaturePipelineContext,
    ConsumerType,
    ValidationResult,
    FieldDefinition,
    FieldCapability,
)
from .geometries import GeometriesSidecar
from .attributes import FeatureAttributeSidecar
from .item_metadata import ItemMetadataSidecar
from .geometries_config import GeometriesSidecarConfig
from .attributes_config import FeatureAttributeSidecarConfig
from .item_metadata_config import ItemMetadataSidecarConfig
from .registry import SidecarRegistry
from .resolver import _effective_sidecars


def driver_sidecars(driver_or_config: Any) -> List[SidecarConfig]:
    """Return the PG-driver sidecar configs declared on ``driver_or_config``,
    or ``[]`` for any other driver / config.

    Sidecars are a PG-driver-internal concept (per-collection auxiliary
    PG tables: feature_attributes, geometries, item_metadata). The
    storage Protocol does NOT carry a ``sidecars`` field — non-PG
    driver configs (Elasticsearch, DuckDB, Iceberg, …) don't have one.

    Call sites outside the PG driver subpackage that previously did
    ``config.sidecars`` should switch to ``driver_sidecars(config)`` so
    they degrade cleanly when the resolved driver isn't PG (e.g. when
    the storage router picks ``items_elasticsearch_driver`` for the
    READ path). The PG driver itself can keep accessing ``self.sidecars``
    directly inside the subpackage.
    """
    sidecars = getattr(driver_or_config, "sidecars", None)
    if not sidecars:
        return []
    try:
        return list(sidecars)
    except TypeError:
        return []


__all__ = [
    "SidecarProtocol",
    "SidecarConfig",
    "FeaturePipelineContext",
    "ConsumerType",
    "ValidationResult",
    "FieldDefinition",
    "FieldCapability",
    "GeometriesSidecar",
    "FeatureAttributeSidecar",
    "ItemMetadataSidecar",
    "GeometriesSidecarConfig",
    "FeatureAttributeSidecarConfig",
    "ItemMetadataSidecarConfig",
    "SidecarRegistry",
    "_effective_sidecars",
    "driver_sidecars",
]
