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
Centralized protocol definitions for DynaStore.

This package contains protocol definitions organized by domain:
- catalogs.py: Catalog-related protocols
- assets.py: Asset management protocols (CRUD + reference tracking)
- asset_upload.py: Backend-agnostic asset upload protocol (GCS, S3, local, HTTP)
- configs.py: Configuration management protocols
- logs.py: Logging protocols
- storage.py: Storage protocols
- job_execution.py: Serverless job execution protocols (optional, enterprise)
- cloud_storage_client.py: Cloud storage client access protocols (optional, enterprise)
- cloud_identity.py: Cloud identity and context protocols (optional, enterprise)
- eventing.py: Cloud eventing and notification protocols (optional, enterprise)
- search.py: Search query protocols (backend-agnostic)
- indexer.py: Document indexing lifecycle protocols (backend-agnostic)
"""

from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.models.protocols.item_crud import ItemCrudProtocol
from dynastore.models.protocols.item_query import ItemQueryProtocol
from dynastore.models.protocols.item_introspection import ItemIntrospectionProtocol
from dynastore.models.protocols.items import ItemsProtocol
from dynastore.models.protocols.collections import CollectionsProtocol
from dynastore.models.protocols.assets import AssetsProtocol
from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.models.protocols.logs import LogsProtocol
from dynastore.models.protocols.storage import StorageProtocol
from dynastore.models.protocols.job_execution import JobExecutionProtocol
from dynastore.models.protocols.cloud_storage_client import CloudStorageClientProtocol
from dynastore.models.protocols.cloud_identity import CloudIdentityProtocol
from dynastore.models.protocols.eventing import EventingProtocol
from dynastore.models.protocols.gcp_provisioning import GcpCatalogProvisioning
from dynastore.models.protocols.events import EventsProtocol
from dynastore.models.protocols.tasks import TasksProtocol
from dynastore.models.protocols.task_queue import TaskQueueProtocol
from dynastore.models.protocols.event_bus import EventBusProtocol
from dynastore.models.protocols.event_driver import EventDriverProtocol
EventStorageProtocol = EventDriverProtocol  # backward-compat alias
from dynastore.models.protocols.proxy import ProxyProtocol
from dynastore.models.protocols.database import DatabaseProtocol
DbProtocol = DatabaseProtocol
from dynastore.models.protocols.properties import PropertiesProtocol
from dynastore.models.protocols.localization import LocalizationProtocol
from dynastore.models.protocols.web import WebModuleProtocol
from dynastore.models.protocols.query_transform import QueryTransformProtocol
from dynastore.models.protocols.crs import CRSProtocol
from dynastore.models.protocols.httpx import HttpxProtocol
from dynastore.models.protocols.styles import StylesProtocol
from dynastore.models.protocols.moving_features import MovingFeaturesProtocol
from dynastore.models.protocols.connected_systems import ConnectedSystemsProtocol
from dynastore.models.protocols.search import SearchProtocol
from dynastore.models.protocols.indexer import (
    BulkResult,
    Indexer,
    IndexContext,
    IndexerProtocol,
    IndexOp,
)
from dynastore.models.protocols.storage_driver import CollectionItemsStore
from dynastore.models.protocols.entity_store import (
    CatalogStore,
    CollectionStore,
)
from dynastore.models.protocols.driver_roles import DriverSla
from dynastore.models.protocols.asset_driver import AssetStore
# CollectionMetadataEnricherProtocol and AssetEnricherProtocol were deleted
# in the role-based driver refactor.  Replacement: TRANSFORM-capable drivers
# routed through CollectionRoutingConfig / AssetRoutingConfig.
from dynastore.models.protocols.asset_contrib import (
    AssetContributor,
    AssetLink,
    ResourceRef,
)
from dynastore.models.protocols.conformance import ConformanceContributor
from dynastore.models.protocols.web_ui import (
    WebPageContributor,
    StaticAssetProvider,
    WebPageSpec,
    StaticAsset,
)
from dynastore.models.protocols.authentication import AuthenticatorProtocol
from dynastore.models.protocols.authorization import (
    AuthorizerProtocol,
    Permission,
)
from dynastore.models.protocols.authorization_context import SecurityContext
from dynastore.models.protocols.role_admin import RoleAdminProtocol
from dynastore.models.protocols.principal_admin import PrincipalAdminProtocol
from dynastore.models.protocols.catalog_pipeline import CatalogPipelineProtocol
from dynastore.models.protocols.item_pipeline import ItemPipelineProtocol
from dynastore.models.protocols.collection_pipeline import CollectionPipelineProtocol
from dynastore.models.protocols.link_contrib import (
    AnchoredLink,
    LinkContributor,
)
from dynastore.models.protocols.bigquery import BigQueryProtocol
from dynastore.models.protocols.bounds_source import (
    BoundsSourceProtocol,
    EmptyBoundsSource,
)
from dynastore.models.protocols.field_definition import (
    FieldCapability,
    FieldDefinition,
    EntityLevel,
    FeatureTypeDefinition,
)
from dynastore.models.protocols.asset_upload import (
    AssetUploadProtocol,
    UploadTicket,
    UploadStatus,
    UploadStatusResponse,
)
# Export auth_models for convenience
from dynastore.models.auth_models import (
    SYSTEM_USER_ID,
    PolicyBundle,
    Role,
    IdentityAuthorization,
    IdentityLink,
    TokenExchangeRequest,
    TokenResponse,
    RefreshToken,
)

__all__ = [
    "CatalogsProtocol",
    "ItemCrudProtocol",
    "ItemQueryProtocol",
    "ItemIntrospectionProtocol",
    "ItemsProtocol",
    "CollectionsProtocol",
    "AssetsProtocol",
    "ConfigsProtocol",
    "LogsProtocol",
    "StorageProtocol",
    "JobExecutionProtocol",
    "CloudStorageClientProtocol",
    "CloudIdentityProtocol",
    "EventingProtocol",
    "GcpCatalogProvisioning",
    "EventsProtocol",
    "TasksProtocol",
    "TaskQueueProtocol",
    "EventBusProtocol",
    "EventDriverProtocol",
    "EventStorageProtocol",
    "ProxyProtocol",
    "DatabaseProtocol",
    "DbProtocol",
    "PropertiesProtocol",
    "LocalizationProtocol",
    "WebModuleProtocol",
    "QueryTransformProtocol",
    "CRSProtocol",
    "HttpxProtocol",
    "StylesProtocol",
    "MovingFeaturesProtocol",
    "ConnectedSystemsProtocol",
    "SearchProtocol",
    "BulkResult",
    "Indexer",
    "IndexContext",
    "IndexerProtocol",
    "IndexOp",
    "CollectionItemsStore",
    "CollectionStore",
    "CatalogStore",
    "DriverSla",
    "AssetStore",
    "AssetContributor",
    "AssetLink",
    "ResourceRef",
    "AnchoredLink",
    "LinkContributor",
    "ItemPipelineProtocol",
    "CollectionPipelineProtocol",
    "CatalogPipelineProtocol",
    "ConformanceContributor",
    "WebPageContributor",
    "StaticAssetProvider",
    "WebPageSpec",
    "StaticAsset",
    "AuthenticatorProtocol",
    "AuthorizerProtocol",
    "Permission",
    "SecurityContext",
    "RoleAdminProtocol",
    "PrincipalAdminProtocol",
    "FieldCapability",
    "FieldDefinition",
    "EntityLevel",
    "FeatureTypeDefinition",
    "BoundsSourceProtocol",
    "EmptyBoundsSource",
    "AssetUploadProtocol",
    "UploadTicket",
    "UploadStatus",
    "UploadStatusResponse",
    # Auth models
    "SYSTEM_USER_ID",
    "PolicyBundle",
    "Role",
    "IdentityAuthorization",
    "IdentityLink",
    "TokenExchangeRequest",
    "TokenResponse",
    "RefreshToken",
]
