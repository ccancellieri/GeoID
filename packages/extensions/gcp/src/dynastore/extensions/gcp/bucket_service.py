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

#    dynastore/extensions/gcp/bucket_service.py

import logging
import base64, json
from google.cloud.storage.blob import Blob
from typing import Optional, Dict, Any, cast
from dynastore.models.shared_models import Link
from dynastore.models.localization import LocalizedText
from dynastore.modules.gcp.models import PUBSUB_JWT_AUDIENCE, GcpBucketDetails
from fastapi import (
    HTTPException,
    status,
    APIRouter,
    FastAPI,
    Depends,
    Header,
    Request,
    Query,
)
from dynastore.modules import get_protocol
from dynastore.models.protocols import (
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
        ) from e


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
        # Resolve the immutable physical_id for the collection so listing always
        # hits the correct prefix even when the logical id has been renamed.
        _catalogs_list = get_protocol(CatalogsProtocol)
        _collection_physical_id: Optional[str] = None
        if _catalogs_list:
            try:
                _collection_physical_id = await _catalogs_list.resolve_physical_id(
                    catalog_id, collection_id, allow_missing=True
                )
            except Exception:
                pass
        prefix = bucket_tool.get_blob_path_for_collection_folder(
            _collection_physical_id or collection_id
        )
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
