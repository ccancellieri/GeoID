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

"""
Multi-driver storage abstraction layer.

Public API::

    from dynastore.modules.storage import (
        get_driver, resolve_drivers, Operation, FailurePolicy,
    )

    driver = await get_driver("READ", catalog_id, collection_id, hints=frozenset({Hint.SEARCH}))
    async for entity in driver.read_entities(catalog_id, collection_id):
        ...

Storage routing is controlled by ``ItemsRoutingConfig`` via the existing
config API (identity is the class itself; see ``class_key()`` in
``platform_config_service.py``).
Per-driver settings are in ``DriverPluginConfig`` subclasses.
"""

from dynastore.models.protocols.storage_driver import (
    Capability,
    CollectionItemsStore,
    ReadOnlyDriverMixin,
)
from dynastore.modules.storage.storage_location import StorageLocation
from dynastore.modules.storage.driver_config import (
    BatchConflictPolicy,
    AssetDriverConfig,
    CollectionDriverConfig,
    ItemsWritePolicy,
    ItemsSchema,
    DriverCapability,
    DriverPluginConfig,
    ItemsPostgresqlDriverConfig,
    WriteConflictPolicy,
)
from dynastore.modules.storage.computed_fields import (
    AttributeStat,
    ComputedField,
    ComputedKind,
    DeriveSpec,
    FeatureType,
    GeometryStat,
    IdentityRule,
    SpatialCell,
)
from dynastore.modules.storage.read_policy import ItemsReadPolicy
from dynastore.modules.storage.validity import ValiditySpec
from dynastore.modules.storage.schema_types import (
    FieldConstraint,
    RequiredConstraint,
    UniqueConstraint,
    SchemaViolation,
    SchemaExtension,
    StacSchemaExtension,
    OgcFeaturesSchemaExtension,
    ConfigScopeMixin,
)
from dynastore.modules.storage.transform_runtime import (
    apply_transform_chain,
    restore_transform_chain,
)
from dynastore.modules.storage.errors import ReadOnlyDriverError, SoftDeleteNotSupportedError
from dynastore.modules.storage.router import (
    ResolvedDriver,
    get_asset_driver,
    get_asset_write_drivers,
    get_driver,
    get_write_drivers,
    resolve_drivers,
)
from dynastore.modules.storage.config_cache import (
    init_request_driver_cache,
    clear_request_driver_cache,
    get_request_driver_cache,
)
from dynastore.modules.storage.routing_config import (
    AssetRoutingConfig,
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
    TransformerEntry,
    WriteMode,
)

__all__ = [
    # Protocol
    "CollectionItemsStore",
    "StorageLocation",
    "Capability",
    "ReadOnlyDriverMixin",
    # Driver configs
    "DriverPluginConfig",
    "CollectionDriverConfig",
    "AssetDriverConfig",
    "DriverCapability",
    # Schema (M8)
    "ItemsSchema",
    "FieldConstraint",
    "RequiredConstraint",
    "UniqueConstraint",
    "SchemaViolation",
    "SchemaExtension",
    "StacSchemaExtension",
    "OgcFeaturesSchemaExtension",
    "ConfigScopeMixin",
    # Write policy
    "ItemsWritePolicy",
    "WriteConflictPolicy",
    "BatchConflictPolicy",
    # Temporal validity spec
    "ValiditySpec",
    # Read policy (#950 phase 3 — registered, not yet consumed by drivers)
    "ItemsReadPolicy",
    # Computed-field model (#957/#950 phase 1)
    "ComputedField",
    "ComputedKind",
    "FeatureType",
    "IdentityRule",
    # Derivation buckets — the authored shape (engine flattens to ComputedField)
    "DeriveSpec",
    "SpatialCell",
    "GeometryStat",
    "AttributeStat",
    # Entity-transform chain runtime (operates on the ``transformers`` registry)
    "apply_transform_chain",
    "restore_transform_chain",
    # Routing configs
    "ItemsRoutingConfig",
    "CollectionRoutingConfig",
    "AssetRoutingConfig",
    "CatalogRoutingConfig",
    "OperationDriverEntry",
    "TransformerEntry",
    "FailurePolicy",
    "Operation",
    "WriteMode",
    # Router
    "resolve_drivers",
    "ResolvedDriver",
    "get_driver",
    "get_write_drivers",
    "get_asset_driver",
    "get_asset_write_drivers",
    # Cache (M6)
    "init_request_driver_cache",
    "clear_request_driver_cache",
    "get_request_driver_cache",
    # Errors
    "ReadOnlyDriverError",
    "SoftDeleteNotSupportedError",
]
