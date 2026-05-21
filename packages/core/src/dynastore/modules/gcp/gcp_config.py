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

from pathlib import PurePosixPath
from pydantic import BaseModel, Field, ConfigDict, field_validator
from typing import Annotated, Any, Callable, ClassVar, Dict, List, Literal, Optional, TYPE_CHECKING, Tuple, Union
from datetime import date
from dynastore.models.mutability import Immutable, Mutable
from dynastore.modules.db_config.plugin_config import PluginConfig
from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin
import os
if TYPE_CHECKING:
    from dynastore.modules.gcp.gcp_module import GCPModule
    from dynastore.modules.db_config.query_executor import DbResource

from dynastore.modules.gcp.models import (
    PushSubscriptionConfig,
    ExternalTopicSubscription,
    LifecycleRule
)
from enum import Enum, StrEnum

# --- Application Hooks ---

async def on_apply_gcp_bucket_config(config: "GcpCatalogBucketConfig", catalog_id: Optional[str], collection_id: Optional[str], db_resource: Optional['DbResource']):
    """Hook to apply bucket configuration changes to the live GCP resource."""
    if not catalog_id:
        # Platform-level default changed, but we only apply to existing buckets on explicit catalog change
        # or we could iterate over all catalogs, but that's expensive.
        # For now, we only apply when catalog_id is provided.
        return
    
    from dynastore.modules import get_protocol
    from dynastore.models.protocols import StorageProtocol
    storage_provider = get_protocol(StorageProtocol)
    if storage_provider:
        await storage_provider.apply_storage_config(catalog_id, config)

async def on_apply_gcp_eventing_config(config: "GcpEventingConfig", catalog_id: Optional[str], collection_id: Optional[str], db_resource: Optional['DbResource']):
    """Hook to apply eventing configuration changes to the live GCP resources."""
    if not catalog_id:
        return
    
    from dynastore.modules import get_protocol
    from dynastore.models.protocols import EventingProtocol
    eventing_provider = get_protocol(EventingProtocol)
    if eventing_provider:
        await eventing_provider.apply_eventing_config(catalog_id, config, conn=db_resource)

# The constant IDs should match the ones in bucket_service and eventing logic


class GcsStorageClass(StrEnum):
    """Google Cloud Storage classes."""
    STANDARD = "STANDARD"
    NEARLINE = "NEARLINE"
    COLDLINE = "COLDLINE"
    ARCHIVE = "ARCHIVE"

class GcsNotificationEventType(StrEnum):
    """Google Cloud Storage notification event types."""
    OBJECT_FINALIZE = "OBJECT_FINALIZE"
    OBJECT_METADATA_UPDATE = "OBJECT_METADATA_UPDATE"
    OBJECT_DELETE = "OBJECT_DELETE"
    OBJECT_ARCHIVE = "OBJECT_ARCHIVE"

class GcsPayloadFormat(StrEnum):
    """Payload format for GCS notifications."""
    JSON_API_V1 = "JSON_API_V1"
    NONE = "NONE"

class GcsChecksumType(StrEnum):
    """Checksum types for GCS uploads."""
    MD5 = "md5"
    CRC32C = "crc32c"

class GcsPredefinedAcl(StrEnum):
    """Predefined ACLs for GCS objects."""
    AUTHENTICATED_READ = "authenticatedRead"
    BUCKET_OWNER_FULL_CONTROL = "bucketOwnerFullControl"
    BUCKET_OWNER_READ = "bucketOwnerRead"
    PRIVATE = "private"
    PROJECT_PRIVATE = "projectPrivate"
    PUBLIC_READ = "publicRead"

class GcsRetryOptions(BaseModel):
    """Configuration for GCS RPC retries."""
    initial: Optional[float] = Field(default=None, description="The initial delay before the first retry, in seconds.")
    maximum: Optional[float] = Field(default=None, description="The maximum delay between retries, in seconds.")
    multiplier: Optional[float] = Field(default=None, description="The multiplier by which the delay increases after each retry.")
    deadline: Optional[float] = Field(default=None, description="The total time allowed for retries, in seconds.")

class GcpLocation(StrEnum):
    """
    Commonly used Google Cloud regions and multi-regions.
    See: https://cloud.google.com/storage/docs/locations
    """
    # Multi-regions
    US = "us"
    EU = "eu"
    ASIA = "asia"
    # Dual-regions (Examples)
    ASIA1 = "asia1"
    EUR4 = "eur4"
    NAM4 = "nam4"
    # Regions
    US_CENTRAL1 = "us-central1"
    US_EAST1 = "us-east1"
    EUROPE_WEST1 = "europe-west1"
    EUROPE_WEST3 = "europe-west3"
    # ... more can be added as needed or use string for others

# 1. Define the CORS Rule Model
class GcpCorsRule(BaseModel):
    """
    Configuration for Cross-Origin Resource Sharing (CORS) on a GCS bucket.
    """
    origin: List[str] = Field(..., description="The list of Origins allowed to make requests.")
    method: List[str] = Field(default_factory=lambda: ["GET", "HEAD", "OPTIONS"], description="The heart of HTTP methods allowed for CORS requests.")
    response_header: Optional[List[str]] = Field(default=None, description="The list of response headers to expose to the client.", alias="responseHeader")
    max_age_seconds: Optional[int] = Field(default=None, description="The time in seconds the browser should cache the preflight response.")

    model_config = ConfigDict(populate_by_name=True)


class GcpCatalogBucketConfig(PluginConfig):
    """
    Defines bucket-level configurations for a catalog. These settings are applied
    when the bucket is first created.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "modules", "gcp")
    _visibility: ClassVar[Optional[str]] = "catalog"

    # Apply handler is registered imperatively at module-import time
    # (see ``GcpCatalogBucketConfig.register_apply_handler(...)`` below).

    # Immutable fields: Once the bucket is created, these cannot be changed.
    location: Immutable[Optional[GcpLocation]] = Field(default=os.getenv("REGION", GcpLocation.EUROPE_WEST1), description="The GCP region where the bucket will be created (e.g., 'europe-west1'). If not set, defaults to the application's region.")  # type: ignore[assignment]
    storage_class: Immutable[GcsStorageClass] = Field(default=GcsStorageClass.STANDARD, description="The default storage class for objects in the bucket.")

    # Mutable fields
    provision_enabled: Mutable[bool] = Field(
        default=True,
        description=(
            "When False, ``GCPModule._on_post_create_catalog`` skips bucket "
            "creation and marks the catalog ready immediately — useful when "
            "a catalog reuses an externally-managed bucket. This is a "
            "per-catalog provisioning gate, NOT an extension exposure toggle "
            "(the GCP module is always-on at the protocol layer; the canonical "
            "toggle lives on ``GcpModuleConfig.enabled``)."
        ),
    )
    cdn_enabled: Mutable[bool] = Field(default=False, description="Whether Cloud CDN is enabled for this bucket.")
    lifecycle_rules: Mutable[List[LifecycleRule]] = Field(default_factory=list, description="Lifecycle rules for the bucket.")
    listen_catalog_events: Mutable[bool] = Field(default=True, description="If true, the bucket and pub/sub resources are synchronized with catalog/collection deletions.")
    cors: Mutable[List[GcpCorsRule]] = Field(
        default_factory=lambda: [GcpCorsRule(origin=["*"], method=["GET", "OPTIONS", "HEAD", "POST", "PUT", "DELETE"], response_header=["*"], max_age_seconds=3600)],  # type: ignore[call-arg]
        description="CORS rules for the bucket."
    )


class GcpModuleConfig(ExposableConfigMixin, PluginConfig):
    """
    Defines global configurations for the GCP module.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "modules", "gcp")

    project_id: Mutable[str] = Field(default=os.getenv("PROJECT_ID", "local-project"), description="The GCP Project ID.")
    region: Mutable[str] = Field(default=os.getenv("REGION", "europe-west1"), description="The default GCP region.")

    # Visibility and Propagation Tuning (Critical for tests)
    catalog_visibility_max_retries: Mutable[int] = Field(
        default=int(os.environ.get("GCP_CATALOG_VISIBILITY_MAX_RETRIES", "20")),
        description="Max retries when checking for catalog visibility."
    )
    catalog_visibility_retry_interval: Mutable[float] = Field(
        default=float(os.environ.get("GCP_CATALOG_VISIBILITY_RETRY_INTERVAL", "0.2")),
        description="Interval between visibility checks, in seconds."
    )

    # --- Cloud Run execution-liveness reconciler (#735) ---
    # These supersede the fixed GCP_RUNNER_SPAWN_LEASE env guess: runtime-tunable
    # PluginConfig fields consumed by GcpJobRunner and GcpLivenessReconciler.
    spawn_lease_seconds: Mutable[int] = Field(
        default=int(os.environ.get("GCP_RUNNER_SPAWN_LEASE", "300")),
        description="Lease (seconds) stamped on a freshly born-claimed Cloud Run "
                    "task row. No longer a cold-start guess — just a modest floor "
                    "covering the window before the liveness reconciler's first "
                    "pass, which then extends the lease of any live execution."
    )
    liveness_reconciler_interval_seconds: Mutable[int] = Field(
        default=int(os.environ.get("GCP_LIVENESS_RECONCILER_INTERVAL", "20")),
        description="How often (seconds) the liveness reconciler scans lapsed-lease "
                    "Cloud Run task rows. Kept below the 60s pg_cron reaper cadence "
                    "so the reconciler gets first look."
    )
    liveness_extend_visibility_seconds: Mutable[int] = Field(
        default=int(os.environ.get("GCP_LIVENESS_EXTEND_VISIBILITY", "300")),
        description="Lease extension (seconds) applied when the reconciler confirms "
                    "a Cloud Run execution is still alive."
    )
    liveness_unknown_grace_seconds: Mutable[int] = Field(
        default=int(os.environ.get("GCP_LIVENESS_UNKNOWN_GRACE", "180")),
        description="Grace window (seconds) for a young task row whose runner_ref "
                    "is not captured yet: the reconciler extends its lease once to "
                    "cover the spawn→capture gap before leaving it to the pg_cron reaper."
    )

class TriggeredAction(BaseModel):
    """Defines a process to be triggered by a GCS event."""
    process_id: str = Field(..., description="The ID of the process to execute (e.g., 'ingestion').")
    execute_request_template: Dict[str, Any] = Field(..., description="A template for the 'inputs' of the OGC Process Execute request. Supports placeholder interpolation (e.g., {bucket}, {name}).")


class GcpCollectionBucketConfig(PluginConfig):
    """
    Defines object-level configurations for a specific collection within a bucket.
    These settings can override catalog-level defaults for objects belonging to this collection.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "modules", "gcp")
    _visibility: ClassVar[Optional[str]] = "collection"

    custom_metadata_defaults: Mutable[Optional[Dict[str, str]]] = Field(default=None, description="Default metadata to apply to all objects uploaded to this collection.")

    # Updated to allow strings (IDs of templates) or full objects
    event_actions: Mutable[Optional[Dict[GcsNotificationEventType, List[Union[str, TriggeredAction]]]]] = Field(
        default=None,
        description="A list of actions to trigger on GCS events. Can be a reference to a template (GcpEventingConfig.action_templates) or an asset task (GcpCollectionBucketConfig.asset_tasks)."
    )

    asset_tasks: Mutable[Dict[str, TriggeredAction]] = Field(
        default_factory=dict, 
        description="A registry of collection-specific asset tasks. Keys are IDs that can be referenced in event_actions or executed manually."
    )


class ManagedBucketEventing(BaseModel):
    """
    Configuration for the simplified, system-managed eventing pipeline for a bucket.
    If enabled, the system will create and manage a dedicated Pub/Sub topic, a push
    subscription, and the GCS notification resource that links them. This tracks
    events for the entire bucket by default.
    """
    enabled: bool = Field(default=True, description="If true, the managed eventing system is active.")
    
    # Immutable: The topic, once created for a catalog, should not be changed.
    topic_id: Optional[str] = Field(default=None, description="Optional custom ID for the managed topic. If not set, a default ID will be generated (e.g., 'ds-catalog_id-events').")
    
    # Mutable: Subscription details can be updated.
    subscription: Optional[PushSubscriptionConfig] = Field(default=None, description="Configuration for the managed push subscription.")
    blob_name_prefixes: List[str] = Field(default_factory=lambda: ["catalog/", "collections/"], description="Filter events to objects with these prefixes. If empty, tracks the entire bucket.")
    event_types: Optional[List[GcsNotificationEventType]] = Field(default=None, description="A list of event types to listen for. Defaults to OBJECT_FINALIZE if not set.")
    payload_format: GcsPayloadFormat = Field(default=GcsPayloadFormat.JSON_API_V1, description="The format of the message payload.")
    
    # --- Output fields managed by the system ---
    topic_path: Optional[str] = Field(default=None, description="The full, unique resource path of the managed Pub/Sub topic. This is an output field managed by the system.")
    gcs_notification_ids: List[str] = Field(default_factory=list, description="The unique IDs of the GCS notification resources on the bucket. This is an output field managed by the system.")
    # Adding bucket_id provides a robust way to link GCS events back to a catalog.
    bucket_id: Optional[str] = Field(default=None, description="The ID of the GCS bucket this eventing configuration is tied to. This is an output field managed by the system.")

    @property
    def blob_name_prefix(self) -> Optional[str]:
        """Legacy access to the first prefix."""
        return self.blob_name_prefixes[0] if self.blob_name_prefixes else None


class GcpEventingConfig(PluginConfig):
    """
    Defines the complete, mutable eventing configuration for a catalog. This is
    stored independently from the bucket configuration.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "modules", "gcp")
    _visibility: ClassVar[Optional[str]] = "catalog"

    # Apply handler is registered imperatively at module-import time
    # (see ``GcpEventingConfig.register_apply_handler(...)`` below).

    managed_eventing: Mutable[Optional[ManagedBucketEventing]] = Field(default=ManagedBucketEventing(), description="Configuration for the default, system-managed eventing pipeline for the catalog's bucket.")
    custom_subscriptions: Mutable[List[ExternalTopicSubscription]] = Field(default=[], description="A list of additional, custom subscriptions to external (non-managed) Pub/Sub topics.")

    # New registry for reusable action templates
    action_templates: Mutable[Dict[str, TriggeredAction]] = Field(
        default_factory=dict, 
        description="A registry of reusable action templates. Keys are IDs (e.g., 'ingestion') that can be referenced in collection configs."
    )


# This model is not a plugin config, but a simple data structure.
# It's being moved here to consolidate GCP configuration-related models.
class UploadOptions(BaseModel):
    """Optional parameters for controlling GCS upload behavior."""

    size: Optional[int] = Field(default=None, description="The maximum number of bytes that can be uploaded using this session. If not known, leave blank.")
    content_type: Optional[str] = Field(default=None, description="Type of content being uploaded. Overrides the default if provided.")
    origin: Optional[str] = Field(default=None, description="If set, the upload can only be completed by a user-agent that uploads from the given origin.")
    checksum: Optional[GcsChecksumType] = Field(default=None, description="The type of checksum to compute to verify the integrity of the object ('md5', 'crc32c', 'auto').")
    predefined_acl: Optional[GcsPredefinedAcl] = Field(default=None, description="Predefined access control list to apply to the uploaded object.")
    if_generation_match: Optional[int] = Field(default=None, description="Makes the operation conditional on the object's generation matching this value.")
    if_generation_not_match: Optional[int] = Field(default=None, description="Makes the operation conditional on the object's generation not matching this value.")
    if_metageneration_match: Optional[int] = Field(default=None, description="Makes the operation conditional on the object's metageneration matching this value.")
    if_metageneration_not_match: Optional[int] = Field(default=None, description="Makes the operation conditional on the object's metageneration not matching this value.")
    timeout: Optional[int] = Field(default=60, description="The amount of time, in seconds, to wait for the server response for the initiation request.")
    retry: Optional[GcsRetryOptions] = Field(default=None, description="Custom retry policy parameters for the upload initiation RPC.")


class InitiateUploadRequest(BaseModel):
    """Request model for initiating a file upload."""
    content_type: str
    catalog_id: Optional[str] = None
    collection_id: Optional[str] = None
    filename: str = Field(
        ...,
        description=(
            "The name of the file to be uploaded, INCLUDING its extension "
            "(e.g. 'aoi_oasis.zip', 'gadm_adm2_italy.geojson', "
            "'LC08_…_T1.tif').  The extension is the source of truth for "
            "format detection at ingestion time, so a bare filename is "
            "rejected with HTTP 422."
        ),
    )
    # Embed the AssetBase model to carry all asset information.
    # The URI will be automatically populated by the system.
    asset: "AssetUploadDefinition"
    upload_options: Optional[UploadOptions] = Field(default=None, description="Advanced options for GCS upload behavior.")

    @field_validator("filename")
    @classmethod
    def _filename_must_have_extension(cls, value: str) -> str:
        # Use PurePosixPath so behaviour is platform-independent (the
        # filename is going to a Unix-shaped GCS object key anyway).
        if not value or not PurePosixPath(value).suffix:
            raise ValueError(
                f"filename {value!r} must include an extension "
                "(e.g. .zip, .shp, .geoparquet, .fgb, .geojson, .gpkg, "
                ".csv, .kml). The extension drives format detection at "
                "ingestion time."
            )
        return value

class InitiateUploadResponse(BaseModel):
    """Response model for initiating a file upload."""
    upload_id: str
    upload_uri: str
    status: str = "initiated"
    message: str = "Upload session initiated. Use the upload_uri for direct GCS upload."

from dynastore.modules.catalog.asset_service import AssetUploadDefinition
InitiateUploadRequest.model_rebuild()


# ---------------------------------------------------------------------------
# Apply-handler registration (Phase 1.5 — single registration path).
# Imperative ``register_apply_handler`` calls replace the legacy
# ``_on_apply: ClassVar`` declaration on each class body.
# ---------------------------------------------------------------------------
GcpCatalogBucketConfig.register_apply_handler(on_apply_gcp_bucket_config)
GcpEventingConfig.register_apply_handler(on_apply_gcp_eventing_config)