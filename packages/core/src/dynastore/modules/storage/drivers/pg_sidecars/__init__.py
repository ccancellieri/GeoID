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
from .access_envelope import AccessEnvelopeSidecar
from .geometries_config import GeometriesSidecarConfig
from .attributes_config import FeatureAttributeSidecarConfig
from .item_metadata_config import ItemMetadataSidecarConfig
from .access_envelope_config import AccessEnvelopeSidecarConfig
from .registry import SidecarRegistry
from .resolver import _effective_sidecars
from .naming import sidecar_table_name


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
    "AccessEnvelopeSidecar",
    "GeometriesSidecarConfig",
    "FeatureAttributeSidecarConfig",
    "ItemMetadataSidecarConfig",
    "AccessEnvelopeSidecarConfig",
    "SidecarRegistry",
    "_effective_sidecars",
    "driver_sidecars",
    "sidecar_table_name",
]
