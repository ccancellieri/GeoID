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

#    dynastore/extensions/gcp/bucket_service.py

import logging
from enum import Enum
import uuid
import base64, json
from dynastore.modules.db_config.exceptions import ImmutableConfigError
from google.cloud.storage.blob import Blob
from typing import Optional, Dict, Any, cast
from dynastore.models.shared_models import Link
from dynastore.models.localization import LocalizedText
from dynastore.modules.gcp.gcp_config import (
    GcpCatalogBucketConfig,
    GcpCollectionBucketConfig,
    GcpEventingConfig,
    InitiateUploadRequest,
    InitiateUploadResponse,
)
from dynastore.modules.gcp.models import PUBSUB_JWT_AUDIENCE, GcpBucketDetails
from google.api_core.retry import Retry
import google.api_core.exceptions
from fastapi import (
    HTTPException,
    status,
    APIRouter,
    FastAPI,
    Depends,
    Header,
    Request,
    Query,
    Body,
)
from dynastore.modules import get_protocol
from dynastore.models.protocols import (
    ConfigsProtocol,
    StorageProtocol,
    CloudIdentityProtocol,
    CloudStorageClientProtocol,
    EventingProtocol,
    CatalogsProtocol,
)
from contextlib import asynccontextmanager
from dynastore.modules.gcp.tools import bucket as bucket_tool
from google.oauth2 import id_token
from dynastore.extensions.gcp import gcp_events
from dynastore.extensions.protocols import ExtensionProtocol
from fastapi.responses import Response
from typing import List, Tuple
from pydantic import BaseModel, Field
from google.auth.transport import requests as google_auth_requests


async def verify_pubsub_jwt(
    authorization: str = Header(...),
):
    """
    FastAPI dependency to verify the JWT from a Pub/Sub push request.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Authorization header.",
        )

    token = authorization.split("Bearer ")[1]

    try:
        # Use the default, requests-based transport. FastAPI will run this synchronous
        # I/O in a separate thread, preventing it from blocking the event loop.
        # The results of this call are cached by the google-auth library, so the
        # HTTP request to fetch keys only happens infrequently.
        request = google_auth_requests.Request()
        id_token.verify_oauth2_token(
            token, request, audience=PUBSUB_JWT_AUDIENCE, clock_skew_in_seconds=10
        )
    except ValueError as e:
        # ValueError is raised for invalid tokens by the google-auth library.
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=f"Invalid token: {e}"
        )


class FileListResponse(BaseModel):
    """
    Response model for listing files in a GCS bucket.
    """

    files: List[Dict[str, Any]] = Field(
        ..., description="The list of file objects for the current page."
    )
    links: List[Link] = Field(
        ..., description="JSON-LD compliant links for pagination."
    )
    page_size: int = Field(..., description="The number of items requested per page.")


def blob_to_dict(blob: Blob) -> Dict[str, Any]:
    """Converts a GCS Blob object to a rich dictionary representation."""
    return {
        "name": blob.name,
        "file_path": f"gs://{blob.bucket.name}/{blob.name}",
        "bucket_name": blob.bucket.name,
        "size": blob.size,
        "time_created": blob.time_created,
        "updated": blob.updated,
        "generation": blob.generation,
        "metageneration": blob.metageneration,
        "etag": blob.etag,
        "owner": blob.owner,
        "crc32c": blob.crc32c,
        "md5_hash": blob.md5_hash,
        "cache_control": blob.cache_control,
        "content_type": blob.content_type,
        "content_disposition": blob.content_disposition,
        "content_encoding": blob.content_encoding,
        "content_language": blob.content_language,
        "storage_class": blob.storage_class,
        "component_count": blob.component_count,
        "custom_time": blob.custom_time,
        "temporary_hold": blob.temporary_hold,
        "event_based_hold": blob.event_based_hold,
        "retention_expiration_time": blob.retention_expiration_time,
        "kms_key_name": blob.kms_key_name,
        "public_url": blob.public_url,
        "media_link": blob.media_link,
        "self_link": blob.self_link,
        "id": blob.id,
        "metadata": blob.metadata,  # Custom metadata
    }


class PubSubMessage(BaseModel):
    """Represents the inner message data from a Pub/Sub push request."""

    attributes: Dict[str, Any]
    data: str  # Base64-encoded string


class PubSubPushRequest(BaseModel):
    """Represents the full payload of a Pub/Sub push request."""

    message: PubSubMessage
    subscription: str


logger = logging.getLogger(__name__)


def get_storage_provider() -> StorageProtocol:
    """FastAPI dependency to get the StorageProtocol implementation."""
    provider = get_protocol(StorageProtocol)
    if not provider:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage service unavailable.",
        )
    return provider


def get_eventing_provider() -> EventingProtocol:
    """FastAPI dependency to get the EventingProtocol implementation."""
    provider = get_protocol(EventingProtocol)
    if not provider:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Eventing service unavailable.",
        )
    return provider


def get_identity_provider() -> CloudIdentityProtocol:
    """FastAPI dependency to get the CloudIdentityProtocol implementation."""
    provider = get_protocol(CloudIdentityProtocol)
    if not provider:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Identity service unavailable.",
        )
    return provider


def get_storage_client_provider() -> CloudStorageClientProtocol:
    """FastAPI dependency to get the CloudStorageClientProtocol implementation."""
    provider = get_protocol(CloudStorageClientProtocol)
    if not provider:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Cloud Storage Client service unavailable.",
        )
    return provider


def get_catalogs_provider() -> CatalogsProtocol:
    """FastAPI dependency to get the CatalogsProtocol implementation."""
    provider = get_protocol(CatalogsProtocol)
    if not provider:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Catalogs service unavailable.",
        )
    return provider


def get_configs_provider() -> ConfigsProtocol:
    """FastAPI dependency to get the ConfigsProtocol implementation."""
    provider = get_protocol(ConfigsProtocol)
    if not provider:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Configuration service unavailable.",
        )
    return provider
class BucketService(ExtensionProtocol):
    priority: int = 100
    router: APIRouter = APIRouter(prefix="/gcp", tags=["GCP Services"])

    def __init__(self, app: FastAPI):
        logger.info("BucketService: Initializing extension.")
        self.app = app

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        logger.info("BucketService: Initializing lifecycle extension.")
        # Ensure required protocols are available
        if not get_protocol(StorageProtocol) or not get_protocol(EventingProtocol):
            raise RuntimeError(
                "Required cloud protocols not found. BucketService cannot operate."
            )

        # --- REGISTER LISTENERS ---
        # Register in-process listeners to cleanup GCP resources when catalog/collection deletion events occur.
        from dynastore.extensions.gcp.gcp_events import (
            register_listeners,
            register_default_gcp_listeners,
        )

        register_listeners()
        register_default_gcp_listeners()

        # # # --- REGISTER AS SUBSCRIBER via API Call ---
        # try:
        #     # We run this as a background task so it doesn't block startup
        #     from dynastore.extensions.gcp.gcp_events import register_self_as_subscriber
        #     await register_self_as_subscriber(await gcp_module.get_self_url())
        # # except Exception as e:
        # except Exception as e:
        #     logger.error(f"GCP Module: Failed to create webhook registration task: {e}", exc_info=True)
        # # # --- END REGISTRATION ---
        yield

    @staticmethod
    def get_storage_provider() -> StorageProtocol:
        return get_storage_provider()

    @staticmethod
    def get_eventing_provider() -> EventingProtocol:
        return get_eventing_provider()

    @staticmethod
    def get_identity_provider() -> CloudIdentityProtocol:
        return get_identity_provider()

    @staticmethod
    def get_storage_client_provider() -> CloudStorageClientProtocol:
        return get_storage_client_provider()

    @staticmethod
    async def _prepare_blob_metadata(
        config_provider: ConfigsProtocol, request: InitiateUploadRequest
    ) -> Dict[str, Any]:
        """
        Prepares the custom metadata for the GCS blob by merging collection defaults
        with request-specific metadata.
        """
        # Start with the metadata provided in the request.
        final_metadata = request.asset.metadata.copy() if request.asset.metadata else {}

        # If a collection is specified, fetch its config and merge default metadata.
        if request.collection_id:
            config_manager = get_protocol(ConfigsProtocol)
            if not config_manager:
                raise HTTPException(
                    status_code=500, detail="Configuration service unavailable."
                )
            collection_config = await config_manager.get_config(
                GcpCollectionBucketConfig,
                request.catalog_id,
                request.collection_id,
            )
            if (
                collection_config is not None
                and collection_config.custom_metadata_defaults
            ):
                # Defaults from config are applied first, request metadata can override them.
                final_metadata = {
                    **collection_config.custom_metadata_defaults,
                    **final_metadata,
                }

        # System-critical metadata is added last to ensure it's always present.
        final_metadata["asset_id"] = request.asset.asset_id
        final_metadata["asset_type"] = request.asset.asset_type.value
        # Persist the upload's content_type into custom metadata so it
        # propagates to the asset row via the OBJECT_FINALIZE event,
        # giving the ingestion task a MIME hint when the URI itself
        # carries an unknown suffix.
        if request.content_type:
            final_metadata["content_type"] = request.content_type

        return {
            k: str(v) if not isinstance(v, str) else v
            for k, v in final_metadata.items()
        }

    # @router.post(
    #     "/events/webhook",
    #     status_code=status.HTTP_202_ACCEPTED,
    #     summary="Secure webhook endpoint for receiving dispatched events."
    # )
    # async def receive_event_webhook(
    #     event_payload: Dict[str, Any],
    #     background_tasks: BackgroundTasks,
    #     request: Request,
    #     # This dependency secures the endpoint using the shared platform key
    #     api_key: str = Security(get_platform_api_key)
    # ):
    #     """
    #     This is the secure push endpoint that the central Event Dispatcher
    #     will call. It acknowledges the event immediately and adds the processing
    #     to a background task.
    #     """
    #     # Add the actual processing to a background task.
    #     # This allows the endpoint to return immediately.
    #     background_tasks.add_task(
    #         BucketService._process_webhook_event,
    #         get_any_engine(request.app.state),
    #         event_payload
    #     )

    #     # Immediately return a 202 Accepted to the caller.
    #     return {"status": "accepted", "event_type": event_payload.get("event_type")}

    # @staticmethod
    # async def _process_webhook_event(engine: DbResource, event_payload: Dict[str, Any]):
    #     """The actual processing logic for the webhook, designed to be run in the background."""
    #     try:
    #         await gcp_events.dispatch_event(engine, event_payload)
    #     except Exception as e:
    #         # Log the error. Since this runs in the background, we can't return
    #         # an HTTP exception to the original caller. The Event Bus has already
    #         # received its 202 and will not retry.
    #         logger.error(f"Background webhook processing failed for event '{event_payload.get('event_type')}': {e}", exc_info=True)

    @router.post(
        "/buckets/init-upload",
        response_model=InitiateUploadResponse,
        status_code=status.HTTP_200_OK,
        summary="Initiate a resumable file upload to a GCS bucket.",
        deprecated=True,
    )
    async def init_upload_endpoint(
        req: Request,  # type: ignore[reportGeneralTypeIssues]
        request: InitiateUploadRequest = Body(
            ...,
            examples={  # type: ignore[arg-type]
                "basic_raster_upload": {
                    "summary": "Basic Raster Upload",
                    "description": "A minimal request to upload a GeoTIFF file to a specific collection.",
                    "value": {
                        "filename": "LC08_L1TP_198030_20251225_02_T1.tif",
                        "content_type": "image/tiff",
                        "catalog_id": "imagery_catalog",
                        "collection_id": "landsat_scenes",
                        "asset": {
                            "asset_id": "LC08_L1TP_198030_20251225_02_T1",
                            "asset_type": "RASTER",
                            "metadata": {
                                "sensor": "OLI/TIRS",
                                "cloud_cover": 10.5,
                                "processing_level": "L1TP",
                                "acquisition_date": "2025-12-25T10:30:00Z",
                            },
                        },
                    },
                },
                "vector_with_options": {
                    "summary": "Vector Upload with Options",
                    "description": "An advanced request to upload a GeoJSON file with a checksum for integrity and a precondition to prevent overwrites.",
                    "value": {
                        "filename": "gadm_adm2_italy.geojson",
                        "content_type": "application/geo+json",
                        "catalog_id": "geodata_catalog",
                        "collection_id": "administrative_boundaries",
                        "asset": {
                            "asset_id": "italy",
                            "asset_type": "VECTORIAL",
                            "metadata": {"source": "italy", "version": "4.1"},
                        },
                        "upload_options": {
                            "checksum": "crc32c",
                            "if_generation_match": 0,
                            "predefined_acl": "publicRead",
                            "retry": {
                                "initial": 1.0,
                                "maximum": 10.0,
                                "multiplier": 2.0,
                                "deadline": 60.0,
                            },
                        },
                    },
                },
            },
        ),
        storage_provider: StorageProtocol = Depends(get_storage_provider),
        storage_client_provider: CloudStorageClientProtocol = Depends(
            get_storage_client_provider
        ),
        eventing_provider: EventingProtocol = Depends(get_eventing_provider),
        catalogs_provider: CatalogsProtocol = Depends(get_catalogs_provider),
        config_provider: ConfigsProtocol = Depends(get_configs_provider),
    ):
        """
        Legacy endpoint to initiate a resumable upload session.
        This endpoint is deprecated in favor of the contextualized paths:
        - /buckets/catalogs/{catalog_id}/init-upload
        - /buckets/catalogs/{catalog_id}/collections/{collection_id}/init-upload
        """
        return await BucketService._init_upload_v1_internal(  # type: ignore[attr-defined]
            request,
            storage_provider,
            storage_client_provider,
            eventing_provider,
            catalogs_provider,
            config_provider,
        )

    @router.post(
        "/buckets/catalogs/{catalog_id}/init-upload",
        response_model=InitiateUploadResponse,
        status_code=status.HTTP_200_OK,
        summary="Initiate a resumable file upload to a catalog bucket.",
    )
    async def init_upload_catalog_endpoint(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        request: InitiateUploadRequest,
        storage_provider: StorageProtocol = Depends(get_storage_provider),
        storage_client_provider: CloudStorageClientProtocol = Depends(
            get_storage_client_provider
        ),
        eventing_provider: EventingProtocol = Depends(get_eventing_provider),
        catalogs_provider: CatalogsProtocol = Depends(get_catalogs_provider),
        config_provider: ConfigsProtocol = Depends(get_configs_provider),
    ):
        """
        Initiates a resumable upload session contextualized for a specific catalog.
        The `catalog_id` from the path takes precedence over any `catalog_id` in the body.
        """
        request.catalog_id = catalog_id
        return await BucketService._init_upload_v1_internal(  # type: ignore[attr-defined]
            request,
            storage_provider,
            storage_client_provider,
            eventing_provider,
            catalogs_provider,
            config_provider,
        )

    @router.post(
        "/buckets/catalogs/{catalog_id}/collections/{collection_id}/init-upload",
        response_model=InitiateUploadResponse,
        status_code=status.HTTP_200_OK,
        summary="Initiate a resumable file upload to a collection bucket path.",
    )
    async def init_upload_collection_endpoint(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        collection_id: str,
        request: InitiateUploadRequest,
        storage_provider: StorageProtocol = Depends(get_storage_provider),
        storage_client_provider: CloudStorageClientProtocol = Depends(
            get_storage_client_provider
        ),
        eventing_provider: EventingProtocol = Depends(get_eventing_provider),
        catalogs_provider: CatalogsProtocol = Depends(get_catalogs_provider),
        config_provider: ConfigsProtocol = Depends(get_configs_provider),
    ):
        """
        Initiates a resumable upload session contextualized for a specific collection within a catalog.
        The `catalog_id` and `collection_id` from the path take precedence over any provided in the body.
        """
        request.catalog_id = catalog_id
        request.collection_id = collection_id
        return await BucketService._init_upload_v1_internal(  # type: ignore[attr-defined]
            request,
            storage_provider,
            storage_client_provider,
            eventing_provider,
            catalogs_provider,
            config_provider,
        )

    async def _init_upload_v1_internal(
        request: InitiateUploadRequest,  # type: ignore[reportGeneralTypeIssues]
        storage_provider: StorageProtocol,
        storage_client_provider: CloudStorageClientProtocol,
        eventing_provider: EventingProtocol,
        catalogs_provider: CatalogsProtocol,
        config_provider: ConfigsProtocol,
    ) -> InitiateUploadResponse:
        """
        Internal implementation of resumable upload initiation.
        """
        bucket_name = "unknown"
        blob_path = "unknown"
        if request.catalog_id is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="catalog_id is required for upload initiation.",
            )
        catalog_id = request.catalog_id
        try:
            # Ensure the target catalog/collection exist before proceeding.
            # Using catalogs_provider for logical entity management.
            await storage_provider.prepare_upload_target(
                catalog_id=catalog_id,
                collection_id=request.collection_id,
            )

            # --- DURABLE PROVISIONING GATE ---
            # Instead of JIT creation (which is fragile and slow), we check
            # the authoritative status from the catalog record.
            catalog = await catalogs_provider.get_catalog(catalog_id)
            if catalog.provisioning_status != "ready":
                if catalog.provisioning_status == "failed":
                    raise HTTPException(
                        status_code=status.HTTP_424_FAILED_DEPENDENCY,
                        detail=f"Catalog '{request.catalog_id}' provisioning failed and its storage is not available.",
                    )
                # Still 'provisioning'
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Catalog '{request.catalog_id}' storage is still being provisioned. Please retry shortly.",
                    headers={"Retry-After": "30"},
                )

            storage_client = storage_client_provider.get_storage_client()
            bucket_name = await storage_provider.get_storage_identifier(
                catalog_id
            )
            if not bucket_name:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Bucket for catalog '{request.catalog_id}' was not found despite 'ready' status.",
                )

            bucket = storage_client.bucket(bucket_name)

            # Determine the correct folder path based on the request
            if request.collection_id:
                blob_path = bucket_tool.get_blob_path_for_collection_file(
                    request.collection_id, request.filename
                )
            else:
                blob_path = bucket_tool.get_blob_path_for_catalog_file(request.filename)

            logger.info(f"Preparing upload for blob: gs://{bucket_name}/{blob_path}")
            blob = bucket.blob(blob_path)

            # Prepare custom metadata by merging collection defaults with request metadata.
            # This uses the new helper method for cleaner logic.
            custom_metadata = await BucketService._prepare_blob_metadata(
                config_provider, request
            )

            # Attach the combined metadata to the blob before creating the session.
            # This ensures the metadata is present when the OBJECT_FINALIZE event fires.
            # GCS custom metadata values must be strings (enforced in _prepare_blob_metadata).
            blob.metadata = custom_metadata

            # We only pass arguments that are not None to avoid overriding defaults with None.
            upload_kwargs = {}
            if request.upload_options:
                upload_kwargs.update(
                    {
                        k: v
                        for k, v in request.upload_options.model_dump(
                            mode="json"
                        ).items()
                        if k != "retry" and v is not None
                    }
                )

                # Handle the nested retry object separately
                if request.upload_options.retry:
                    retry_params = {
                        k: v
                        for k, v in request.upload_options.retry.model_dump().items()
                        if v is not None
                    }
                    if retry_params:
                        # The GCS library documentation states that for media uploads,
                        # non-default predicates are not supported. We can safely construct
                        # a Retry object with just the timing parameters.
                        upload_kwargs["retry"] = Retry(**retry_params)

            # The top-level content_type in the request takes precedence unless overriden in upload_options
            if "content_type" not in upload_kwargs:
                upload_kwargs["content_type"] = request.content_type

            # Robust serialization: ensure all Enums are converted to their string values
            # as the GCS library may not handle all Enum types natively in all environments.
            filtered_kwargs = {}
            for k, v in upload_kwargs.items():
                if v is not None:
                    if isinstance(v, Enum):
                        filtered_kwargs[k] = str(v)
                    else:
                        filtered_kwargs[k] = v

            session_uri = blob.create_resumable_upload_session(**filtered_kwargs)

            if not session_uri:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="GCS did not return a session URI for resumable upload initiation.",
                )

            from dynastore.tools.identifiers import generate_uuidv7

            upload_id = str(generate_uuidv7())
            logger.info(
                f"GCS returned persistent upload URI for upload_id '{upload_id}'."
            )
            return InitiateUploadResponse(upload_id=upload_id, upload_uri=session_uri)

        except (
            google.api_core.exceptions.PreconditionFailed,
            google.api_core.exceptions.BadRequest,
        ) as e:
            logger.error(f"GCS Precondition or Validation error: {e}", exc_info=True)
            detail = str(e)
            # Handle both "precondition" and hyphenated "pre-condition"
            if (
                "if-generation-match" in detail.lower()
                or "precondition" in detail.lower()
                or "pre-condition" in detail.lower()
            ):
                detail = f"Precondition failed: The object 'gs://{bucket_name}/{blob_path}' might already exist and 'if_generation_match=0' was specified, or the provided generation/metageneration does not match. (GCS Error: {detail})"

            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=detail)
        except HTTPException:
            # Re-raise FastAPI HTTPExceptions as-is (e.g. 503 Service Unavailable from line 550)
            raise
        except Exception as e:
            logger.error(f"Error initiating upload: {e}", exc_info=True)
            # Log more details about the request to help debugging
            logger.error(
                f"Request Details: catalog_id={request.catalog_id}, collection_id={request.collection_id}, filename={request.filename}"
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected error occurred while initiating the upload: {e}",
            )

    @router.get(
        "/catalogs/{catalog_id}/config/bucket",
        response_model=GcpCatalogBucketConfig,
        summary="Get the GCP bucket configuration for a catalog.",
    )
    async def get_gcp_catalog_bucket_config(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        config_provider: ConfigsProtocol = Depends(get_configs_provider),
    ):
        """
        Retrieves the GCP bucket configuration for a catalog, showing the resolved
        settings from the hierarchy (Catalog > Platform > Default).
        """
        config = await config_provider.get_config(
            GcpCatalogBucketConfig, catalog_id
        )
        if not isinstance(config, GcpCatalogBucketConfig):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No bucket configuration found for catalog '{catalog_id}'.",
            )
        return config

    # @router.put("/catalogs/{catalog_id}/collections/{collection_id}/config/bucket", status_code=status.HTTP_204_NO_CONTENT)
    # async def set_gcp_collection_bucket_config(catalog_id: str, collection_id: str, config: GcpCollectionBucketConfig, gcp_module: GCPModule = Depends(get_gcp_module)):
    #     """
    #     Sets the GCP bucket configuration for a specific collection.
    #     This can be used to override catalog-level settings for objects within this collection.
    #     """
    #     config_manager = gcp_module.get_config_manager()
    #     try:
    #         await config_manager.set_collection_config(catalog_id, collection_id, GCP_COLLECTION_BUCKET_CONFIG_ID, config)
    #     except ImmutableConfigError as e:
    #         raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    #     except ValueError as e:
    #         raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    #     return Response(status_code=status.HTTP_204_NO_CONTENT)

    # @router.get(
    #     "/catalogs/{catalog_id}/collections/{collection_id}/config/bucket",
    #     response_model=GcpCollectionBucketConfig,
    #     summary="Get the effective GCP bucket configuration for a collection."
    # )
    # async def get_gcp_collection_bucket_config(catalog_id: str, collection_id: str, gcp_module: GCPModule = Depends(get_gcp_module)):
    #     """
    #     Retrieves the GCP bucket configuration for a collection, showing the resolved
    #     settings from the hierarchy (Collection > Catalog > Platform > Default).
    #     """
    #     config_manager = gcp_module.get_config_manager()
    #     config = await config_manager.get_config(GCP_COLLECTION_BUCKET_CONFIG_ID, catalog_id, collection_id)
    #     if not isinstance(config, GcpCollectionBucketConfig):
    #         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No bucket configuration found for this collection.")
    #     return cast(GcpCollectionBucketConfig, config)

    @router.get(
        "/catalogs/{catalog_id}/config/eventing",
        response_model=GcpEventingConfig,
        status_code=status.HTTP_200_OK,
        summary="Get the eventing configuration for a catalog.",
    )
    async def get_eventing_config(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        eventing_provider: EventingProtocol = Depends(get_eventing_provider),
    ):
        """
        Retrieves the current eventing configuration for the specified catalog,
        including the state of its managed eventing system and custom subscriptions.
        """
        config = await eventing_provider.get_eventing_config(catalog_id)
        if not config:
            return GcpEventingConfig()  # Return a default empty config
        return config

    @router.post(
        "/events/pubsub-push",
        status_code=status.HTTP_204_NO_CONTENT,
        summary="Webhook endpoint for receiving GCS event notifications via Pub/Sub.",
    )
    async def handle_pubsub_push_notification(
        request: PubSubPushRequest,  # type: ignore[reportGeneralTypeIssues]
        fastapi_request: Request,
        _=Depends(verify_pubsub_jwt),
    ):
        """Receive a Pub/Sub HTTP push notification and process it inline.

        OBJECT_FINALIZE events drive the inline activator (no task hop):
        the matching PENDING ``assets`` row is UPDATEd to ACTIVE in the
        same request, in a single ``managed_transaction``. Pub/Sub
        at-least-once redelivery is the retry mechanism.

        HTTP status mapping (Pub/Sub treats 2xx as ack, 5xx as nack):

        * 204 — successful activation, idempotent re-delivery, orphan
          finalize (logged via ``log_event(event_type='orphan_finalize')``),
          or non-FINALIZE / unrecognised events. None of these benefit
          from redelivery.
        * 503 — transient infrastructure failure (DB unreachable, lock
          timeout, asyncpg pool exhaustion, unexpected exception) **or**
          a catalog provisioning race (``CatalogSchemaUnavailable`` —
          schema not yet visible on this Cloud Run instance). Pub/Sub
          redelivers per its backoff policy; genuinely missing catalogs
          eventually flush via the dead-letter policy.
        """
        # Lazy-import exceptions used for HTTP-status mapping so this
        # route module does not pull the activator chain at import time.
        from dynastore.modules.gcp.gcp_finalize_activator import (
            OrphanFinalizeEvent,
        )
        from dynastore.extensions.gcp.gcp_events import (
            CatalogSchemaUnavailable,
        )

        try:
            # The actual data is base64 encoded within the 'data' field.
            payload_str = base64.b64decode(request.message.data).decode("utf-8")
            gcs_event_payload = json.loads(payload_str)

            logger.info(
                f"Received Pub/Sub notification for subscription '{request.subscription}'."
            )

            # --- Context Extraction Strategy ---
            # 1. Try to get context from HTTP Headers (injected by Pub/Sub PushConfig).
            #    This is the preferred method as it's explicit configuration.
            # 2. Try to get context from Message Attributes (if manually injected at source).
            # 3. Fallback: Infer context from the Subscription ID string (naming convention).

            def get_attr(key):
                # Pub/Sub sends attributes as headers prefixed with x-goog-pubsub-attribute-
                header_val = fastapi_request.headers.get(
                    f"x-goog-pubsub-attribute-{key}"
                )
                if header_val:
                    return header_val
                # Fallback to message attributes (metadata from source)
                return request.message.attributes.get(key)

            subscription_id = get_attr("subscription_id")
            catalog_id = get_attr("catalog_id")
            subscription_type = get_attr("subscription_type")

            # --- Fallback Logic ---

            # Fallback 1: Extract subscription_id from the full resource path if missing
            if not subscription_id and request.subscription:
                # request.subscription format: "projects/{project}/subscriptions/{sub_id}"
                try:
                    subscription_id = request.subscription.split("/")[-1]
                    logger.debug(
                        f"Extracted subscription_id '{subscription_id}' from resource path."
                    )
                except Exception:
                    logger.warning(
                        "Failed to extract subscription_id from resource path."
                    )

            # Fallback 2: Infer catalog_id and subscription_type from subscription_id
            # Useful for "zombie" subscriptions or if PushConfig headers are missing.
            if subscription_id and (not catalog_id or not subscription_type):
                # Convention: ds-{catalog_id}-default-sub
                prefix = "ds-"
                suffix = "-default-sub"
                if subscription_id.startswith(prefix) and subscription_id.endswith(
                    suffix
                ):
                    inferred_id = subscription_id[len(prefix) : -len(suffix)]

                    if not catalog_id:
                        catalog_id = inferred_id
                        logger.debug(
                            f"Inferred catalog_id '{catalog_id}' from subscription_id naming convention."
                        )

                    if not subscription_type:
                        subscription_type = "managed"
                        logger.debug(
                            f"Inferred subscription_type '{subscription_type}' from subscription_id naming convention."
                        )

            # --- Validation & Dispatch ---

            if not subscription_id:
                logger.warning(
                    f"Could not identify subscription_id from headers, attributes, or path. "
                    f"Available headers: {dict(fastapi_request.headers)}"
                )
                return Response(status_code=status.HTTP_204_NO_CONTENT)

            # Construct the final attributes dictionary for the internal event dispatcher.
            # We merge the raw message attributes with our discovered context.
            final_attributes = request.message.attributes.copy()
            final_attributes["subscription_id"] = subscription_id
            if catalog_id:
                final_attributes["catalog_id"] = catalog_id
            if subscription_type:
                final_attributes["subscription_type"] = subscription_type

            logger.info(
                f"Dispatching GCS event from subscription '{subscription_id}' (Catalog: {catalog_id}, Type: {subscription_type})."
            )

            dispatch_payload = {
                "subscription_id": subscription_id,
                "gcs_event_payload": gcs_event_payload,
                "attributes": final_attributes,
            }
            await gcp_events.dispatch_gcp_event(dispatch_payload)

        except OrphanFinalizeEvent as exc:
            # Activator already logged the orphan via log_event. Ack so
            # Pub/Sub stops redelivering — Stage 6 reconciliation owns
            # the recovery path.
            logger.info(f"Orphan finalize acked (no PENDING row): {exc}")
            return Response(status_code=status.HTTP_204_NO_CONTENT)
        except CatalogSchemaUnavailable as exc:
            # Catalog provisioning race: schema not yet visible on this
            # Cloud Run instance. Nack with 503 so Pub/Sub redelivers —
            # by the time it does, the schema is typically live.
            # Genuinely missing catalogs flush via the dead-letter
            # policy after the configured retry budget.
            logger.warning(
                f"Catalog schema unavailable for push event: {exc} — "
                f"requesting Pub/Sub redelivery."
            )
            return Response(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content=str(exc).encode(),
            )
        except HTTPException:
            # Auth / validation failure — re-raise so FastAPI's normal
            # handler builds the response. JWT verify already did this
            # before us, but defence-in-depth.
            raise
        except Exception as e:
            # Transient infrastructure failure (DB unreachable, lock
            # timeout, asyncpg pool exhaustion, etc). Surface as 5xx so
            # Pub/Sub redelivers per its backoff policy. The push
            # subscription's dead-letter policy is the last line of
            # defence for genuinely poison messages — letting them
            # redeliver here is the right default for at-least-once.
            logger.error(
                f"Transient failure processing Pub/Sub push notification: {e}",
                exc_info=True,
            )
            return Response(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content=f"Transient failure: {e!s}".encode(),
            )

        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @router.get(
        "/catalogs/{catalog_id}/collections/{collection_id}/files.json",
        response_model=FileListResponse,
        summary="List files in a collection's GCS bucket folder (paginated).",
    )
    async def list_collection_files(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        collection_id: str,
        request: Request,
        page_size: int = Query(
            100,
            ge=1,
            le=1000,
            description="The maximum number of blobs to return per page.",
        ),
        page_token: Optional[str] = Query(
            None, description="Token for the next page of results."
        ),
        match_glob: Optional[str] = Query(
            None, description="A glob pattern to filter results (e.g., `**/*.tif`)."
        ),
        delimiter: Optional[str] = Query(
            None, description="Delimiter for emulating hierarchy."
        ),
        start_offset: Optional[str] = Query(
            None,
            description="Filter results to names lexicographically after this offset.",
        ),
        end_offset: Optional[str] = Query(
            None,
            description="Filter results to names lexicographically before this offset.",
        ),
    ):
        """
        Lists the files uploaded into the bucket of a collection in a paginated JSON format.
        The JSON contains the file path, the bucket name, and the metadata associated with the object.
        If the bucket does not exist, the collection should be empty.
        """
        list_params = {
            "page_size": page_size,
            "page_token": page_token,
            "match_glob": match_glob,
            "delimiter": delimiter,
            "start_offset": start_offset,
            "end_offset": end_offset,
        }
        files, links = await _list_files(
            catalog_id, request, collection_id=collection_id, **list_params
        )
        return FileListResponse(files=files, links=links, page_size=page_size)

    @router.get(
        "/catalogs/{catalog_id}/files.json",
        response_model=FileListResponse,
        summary="List files in a catalog's GCS bucket folder (paginated).",
    )
    async def list_catalog_files(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        request: Request,
        page_size: int = Query(
            100,
            ge=1,
            le=1000,
            description="The maximum number of blobs to return per page.",
        ),
        page_token: Optional[str] = Query(
            None, description="Token for the next page of results."
        ),
        match_glob: Optional[str] = Query(
            None, description="A glob pattern to filter results (e.g., `**/*.json`)."
        ),
        delimiter: Optional[str] = Query(
            None, description="Delimiter for emulating hierarchy."
        ),
        start_offset: Optional[str] = Query(
            None,
            description="Filter results to names lexicographically after this offset.",
        ),
        end_offset: Optional[str] = Query(
            None,
            description="Filter results to names lexicographically before this offset.",
        ),
    ):
        """
        Lists the files uploaded into the bucket of a catalog in a paginated JSON format.
        The JSON contains the file path, the bucket name, and the metadata associated with the object.
        If the bucket does not exist, the collection should be empty.
        """
        list_params = {
            "page_size": page_size,
            "page_token": page_token,
            "match_glob": match_glob,
            "delimiter": delimiter,
            "start_offset": start_offset,
            "end_offset": end_offset,
        }
        files, links = await _list_files(
            catalog_id, request, collection_id=None, **list_params
        )
        return FileListResponse(files=files, links=links, page_size=page_size)

    @router.get(
        "/catalogs/{catalog_id}/bucket",
        response_model=GcpBucketDetails,
        status_code=status.HTTP_200_OK,
        summary="Get details for a catalog's GCS bucket.",
    )
    async def get_catalog_bucket(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        storage_provider: StorageProtocol = Depends(get_storage_provider),
        storage_client_provider: CloudStorageClientProtocol = Depends(
            get_storage_client_provider
        ),
    ):
        """
        Retrieves details for a catalog's associated GCS bucket.
        """
        # This correctly uses the module's method to get the linked bucket name.
        bucket_name = await storage_provider.get_storage_identifier(catalog_id)

        if not bucket_name:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No bucket is associated with catalog '{catalog_id}'.",
            )

        storage_client = storage_client_provider.get_storage_client()
        bucket = storage_client.get_bucket(bucket_name)
        # Construct the response model from the bucket's public attributes
        return GcpBucketDetails(
            name=bucket.name,
            id=bucket.id,
            location=bucket.location,
            project_number=cast(int, bucket.project_number),
            storage_class=bucket.storage_class,
            time_created=str(bucket.time_created),
            updated=str(bucket.updated),
        )

    @router.delete(
        "/catalogs/{catalog_id}/collections/{collection_id}/files",
        status_code=status.HTTP_204_NO_CONTENT,
        summary="Delete a file or folder from a collection's bucket.",
    )
    async def delete_collection_file_or_folder(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        collection_id: str,
        path: str = Query(
            ...,
            description="The relative path to the file or folder to delete. To delete a folder, the path must end with a '/'. For example, 'my_file.tif' or 'my_folder/'. The path is relative to the collection's folder in the bucket.",
        ),
        storage_provider: StorageProtocol = Depends(get_storage_provider),
        storage_client_provider: CloudStorageClientProtocol = Depends(
            get_storage_client_provider
        ),
    ):
        """
        Deletes a specific file or an entire folder (prefix) from a collection's
        dedicated folder within its GCS bucket.
        """
        bucket_name = await storage_provider.get_storage_identifier(catalog_id)
        if not bucket_name:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No bucket associated with catalog '{catalog_id}'.",
            )

        storage_client = storage_client_provider.get_storage_client()
        bucket = storage_client.bucket(bucket_name)

        # Construct the full path within the bucket
        full_path = bucket_tool.get_blob_path_for_collection_file(collection_id, path)

        if path.endswith("/"):
            # It's a folder deletion request
            logger.info(
                f"Deleting folder with prefix '{full_path}' from bucket '{bucket_name}'."
            )
            blobs_to_delete = list(bucket.list_blobs(prefix=full_path))
            if blobs_to_delete:
                bucket.delete_blobs(blobs_to_delete)
                logger.info(
                    f"Successfully deleted {len(blobs_to_delete)} objects for folder '{path}'."
                )
            else:
                logger.info(f"No objects found to delete for folder '{path}'.")
        else:
            # It's a single file deletion request
            logger.info(f"Deleting file '{full_path}' from bucket '{bucket_name}'.")
            blob = bucket.blob(full_path)
            if not blob.exists():
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"File not found at path: {path}",
                )
            blob.delete()

    @router.delete(
        "/catalogs/{catalog_id}/files",
        status_code=status.HTTP_204_NO_CONTENT,
        summary="Delete a file or folder from a catalog's bucket.",
    )
    async def delete_catalog_file_or_folder(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        path: str = Query(
            ...,
            description="The relative path to the file or folder to delete. To delete a folder, the path must end with a '/'. For example, 'my_file.tif' or 'my_folder/'. The path is relative to the catalog's root folder in the bucket.",
        ),
        storage_provider: StorageProtocol = Depends(get_storage_provider),
        storage_client_provider: CloudStorageClientProtocol = Depends(
            get_storage_client_provider
        ),
    ):
        """
        Deletes a specific file or an entire folder (prefix) from a catalog's
        root folder within its GCS bucket.
        """
        bucket_name = await storage_provider.get_storage_identifier(catalog_id)
        if not bucket_name:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No bucket associated with catalog '{catalog_id}'.",
            )

        storage_client = storage_client_provider.get_storage_client()
        bucket = storage_client.bucket(bucket_name)

        full_path = bucket_tool.get_blob_path_for_catalog_file(path)

        if path.endswith("/"):
            logger.info(
                f"Deleting folder with prefix '{full_path}' from bucket '{bucket_name}'."
            )
            blobs_to_delete = list(bucket.list_blobs(prefix=full_path))
            if blobs_to_delete:
                bucket.delete_blobs(blobs_to_delete)
                logger.info(
                    f"Successfully deleted {len(blobs_to_delete)} objects for folder '{path}'."
                )
        else:
            logger.info(f"Deleting file '{full_path}' from bucket '{bucket_name}'.")
            blob = bucket.blob(full_path)
            if not blob.exists():
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"File not found at path: {path}",
                )
            blob.delete()


async def _list_files(
    catalog_id: str,
    request: Request,
    collection_id: Optional[str] = None,
    page_size: int = 100,
    page_token: Optional[str] = None,
    storage_provider: StorageProtocol = Depends(get_storage_provider),
    storage_client_provider: CloudStorageClientProtocol = Depends(
        get_storage_client_provider
    ),
    **kwargs,
) -> Tuple[List[Dict[str, Any]], List[Link]]:
    """
    Lists files in a GCS bucket for a given catalog or collection.
    This function uses true server-side pagination.
    """
    bucket_name = await storage_provider.get_storage_identifier(catalog_id)
    if not bucket_name:
        # If there's no bucket, return empty results and a 'self' link.
        self_link = Link(href=str(request.url), rel="self", type="application/json")
        return [], [self_link]

    storage_client = storage_client_provider.get_storage_client()
    bucket = storage_client.bucket(bucket_name)

    if collection_id:
        prefix = bucket_tool.get_blob_path_for_collection_folder(collection_id)
    else:
        prefix = bucket_tool.CATALOG_FOLDER + "/"

    # list_blobs returns an HTTPIterator. We don't need to call .pages immediately.
    # The iterator itself will manage fetching pages of blobs.
    blob_iterator = bucket.list_blobs(
        prefix=prefix,
        page_size=page_size,
        page_token=page_token,
        projection="full",  # Request all available metadata for each blob
        **kwargs,
    )

    # The iterator fetches one page of blobs when we iterate over it.
    # We convert it to a list to get the blobs for the current page.
    files = [
        blob_to_dict(blob) for blob in blob_iterator if not blob.name.endswith("/")
    ]

    # --- Build Pagination Links ---
    links = []
    base_url = str(request.url.remove_query_params(["page_token"]))

    # Self link
    links.append(
        Link(
            href=str(request.url),
            rel="self",
            type="application/json",
            title=LocalizedText(en="Current Page"),
        )
    )
    # After iterating, the `next_page_token` attribute on the iterator is populated.
    if blob_iterator.next_page_token:
        next_url = f"{base_url}&page_token={blob_iterator.next_page_token}"
        links.append(
            Link(href=next_url, rel="next", type="application/json", title=LocalizedText(en="Next Page"))
        )

    return files, links
