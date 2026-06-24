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

from __future__ import annotations

from enum import StrEnum
import os
import logging
import io
import asyncio
from typing import TYPE_CHECKING, Optional, Tuple, Iterator
if TYPE_CHECKING:
    from google.cloud import storage
    from google.api_core.exceptions import NotFound, Conflict, Forbidden, GoogleAPICallError
else:
    try:
        from google.cloud import storage
        from google.api_core.exceptions import NotFound, Conflict, Forbidden, GoogleAPICallError
    except ImportError:
        storage = None
        NotFound = Exception
        Conflict = Exception
        Forbidden = Exception
        GoogleAPICallError = Exception
from urllib.parse import urlparse
from dynastore.modules.gcp.gcp_config import GcpCatalogBucketConfig
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class BucketConflictError(RuntimeError):
    """The deterministic bucket name is already taken by something we must not touch.

    Raised when a bucket name collides with a bucket owned by a *different* GCP
    project, or (surfaced higher up) a bucket already linked to a *different*
    catalog. It is a ``RuntimeError`` subclass so existing ``except RuntimeError``
    handlers still catch it, but provisioning treats it specially: the catalog
    is marked ``conflict`` and the bucket is **never** deleted — the collision is
    not ours to resolve and force-deleting it would destroy another owner's data.
    """


class FileSystem(StrEnum):
    gs = "gs"
    https = "https"
    http = "http"
    s3 = "s3"
    file = "file"
    unknown = "unknown"

class ParsedURL(BaseModel):
    scheme: FileSystem
    bucket: Optional[str] = None # For gs, s3
    netloc: Optional[str] = None # For http, https
    path: str

# Use PROJECT_ID from environment, with a fallback to GOOGLE_CLOUD_PROJECT
# --- Standardized Folder Names ---
CATALOG_FOLDER = "catalog"
COLLECTIONS_FOLDER = "collections"

# This aligns with CI/CD variables and common configurations.
project_id = os.getenv("PROJECT_ID", os.getenv("GOOGLE_CLOUD_PROJECT"))

COMPRESSION_EXTENSIONS = {
    '.zip': '/vsizip/',
    '.gz': '/vsigzip/',
    '.tar': '/vsitar/',
    '.tgz': '/vsitar/', # .tgz is an alias for .tar.gz
    '.bz2': '/vsibzip2/'
}

def get_gdal_path(file_url: str) -> str:
    """
    Constructs a GDAL-compatible virtual filesystem path from a standard URL.
    It correctly orders remote and compression prefixes.
    e.g., 'https://.../data.zip' -> '/vsizip//vsicurl/https://.../data.zip'
    """
    # 1. Determine the base remote path
    parsed_url = parse_url(file_url)
    if parsed_url.scheme == FileSystem.gs:
        base_path = f"/vsigs/{parsed_url.bucket}/{parsed_url.path}"
    elif parsed_url.scheme in [FileSystem.https, FileSystem.http]:
        base_path = f"/vsicurl/{file_url}"
    elif parsed_url.scheme == FileSystem.file:
        # Local / on-prem asset: GDAL/OGR cannot open a ``file://`` URI, it
        # needs a plain filesystem path. ``parse_url`` already extracted it.
        base_path = parsed_url.path
    else:
        base_path = file_url # Assume it's a local path

    # 2. Prepend compression wrapper if applicable. This must wrap the remote path.
    for ext, prefix in COMPRESSION_EXTENSIONS.items():
        if file_url.lower().endswith(ext):
            return f"{prefix}{base_path}"

    return base_path

def _get_shared_gcs_client() -> storage.Client:
    from dynastore.modules import get_protocol
    from dynastore.models.protocols import CloudStorageClientProtocol
    client_provider = get_protocol(CloudStorageClientProtocol)
    if client_provider is None:
        raise RuntimeError("CloudStorageClientProtocol (GCP) not available.")
    return client_provider.get_storage_client()


def _get_gcs_file_as_buffer_sync(gcs_path: str, client: Optional[storage.Client] = None) -> io.BytesIO:
    """Downloads a GCS file into an in-memory BytesIO buffer."""
    try:
        storage_client = client or _get_shared_gcs_client()
        bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
        bucket = storage_client.bucket(bucket_name, user_project=project_id)
        blob = bucket.blob(blob_name)
        return io.BytesIO(blob.download_as_bytes())
    except Exception as e:
        logger.error(f"Failed to stream from GCS path {gcs_path}: {e}", exc_info=True)
        raise
def get_gcs_file_as_buffer(gcs_path: str, client: Optional[storage.Client] = None) -> io.BytesIO:
    """Downloads a GCS file into an in-memory BytesIO buffer (Synchronous)."""
    return _get_gcs_file_as_buffer_sync(gcs_path, client=client)

async def get_gcs_file_as_buffer_async(gcs_path: str, client: Optional[storage.Client] = None) -> io.BytesIO:
    """Downloads a GCS file into an in-memory BytesIO buffer (Async)."""
    return await asyncio.to_thread(_get_gcs_file_as_buffer_sync, gcs_path, client=client)
def _create_bucket_sync(
    bucket_name: str,
    bucket_config: GcpCatalogBucketConfig,
    project_id: Optional[str] = None,
    client: Optional[storage.Client] = None,
) -> Tuple[storage.Bucket, bool]:
    """
    Creates a new GCS bucket using Application Default Credentials.

    The bucket is created in the region specified by the `REGION` environment
    variable by default, to keep it close to the application. This can be
    overridden by providing the `location` argument.

    Args:
        bucket_name (str): The name for the new bucket.
        bucket_config (GcpCatalogBucketConfig): A Pydantic model containing the
            bucket's configuration (location, storage class, lifecycle rules).
        project_id (str, optional): The GCP project ID. Defaults to the
            `PROJECT_ID` environment variable.
        client (storage.Client, optional): The GCS client to use.

    Returns:
        Tuple[storage.Bucket, bool]: the bucket object, and ``created`` —
        ``True`` if this call created the bucket, ``False`` if it already
        existed (a ``Conflict`` was resolved by fetching it). Callers MUST NOT
        orphan-delete a bucket they did not create.

    Raises:
        BucketConflictError: the name is taken by a bucket in another GCP
        project — not ours to claim or delete.
    """
    try:
        bucket_location = bucket_config.location
        storage_client = client or _get_shared_gcs_client()

        # Instantiate a bucket object. This does not make an API call.
        bucket = storage_client.bucket(bucket_name)

        # Set properties on the bucket object before creating it.
        bucket.storage_class = bucket_config.storage_class
        if bucket_config.lifecycle_rules:
            # Convert Pydantic models to dictionaries for the GCS client library.
            bucket.lifecycle_rules = [rule.model_dump(exclude_none=True) for rule in bucket_config.lifecycle_rules]

        # The create() method makes the API call. The project for creation is passed here.
        bucket.create(location=bucket_location, project=project_id)
        logger.info(f"Bucket {bucket_name} created in {bucket_location}.")

        if bucket_config.cdn_enabled:
            logger.info(f"Enabling Cloud CDN for bucket '{bucket_name}'.")
            setattr(bucket, "cache_control", "public, max-age=3600")
            bucket.patch()

        return bucket, True
    except Conflict as conflict_err:
        logger.warning(f"Bucket {bucket_name} already exists.")
        # GCS eventual consistency: the bucket was just created (by another process/request)
        # but metadata may not yet be visible. Retry get_bucket with backoff.
        client_to_use = client or _get_shared_gcs_client()
        import time
        for attempt in range(5):
            try:
                # created=False: the bucket pre-existed this call. The caller
                # must not orphan-delete it on a later failure.
                return client_to_use.get_bucket(bucket_name), False
            except Forbidden as exc:
                # The bucket name is globally taken by a bucket in a different
                # GCP project — it is not ours to claim or delete. Surface a
                # BucketConflictError so provisioning marks the catalog
                # 'conflict' instead of retrying a name that can never succeed.
                raise BucketConflictError(
                    f"Bucket '{bucket_name}' exists in another GCP project or "
                    f"service-account lacks storage.buckets.get permission: {exc}"
                ) from exc
            except NotFound:
                if attempt < 4:
                    wait = 0.5 * (2 ** attempt)  # 0.5, 1, 2, 4 seconds
                    logger.warning(
                        f"Bucket {bucket_name} not yet visible after conflict (attempt {attempt + 1}/5). "
                        f"Retrying in {wait}s..."
                    )
                    time.sleep(wait)
                else:
                    logger.error(
                        f"Bucket {bucket_name} still not visible after 5 retries. Raising."
                    )
                    raise
        raise RuntimeError(f"Unreachable: bucket {bucket_name} retry exhausted") from conflict_err
    except Exception as e:
        logger.error(f"Failed to create bucket {bucket_name}: {e}", exc_info=True)
        raise

async def create_bucket(
    bucket_name: str,
    bucket_config: GcpCatalogBucketConfig,
    project_id: Optional[str] = None,
    client: Optional[storage.Client] = None,
) -> Tuple[storage.Bucket, bool]:
    """Async wrapper for _create_bucket_sync. Returns ``(bucket, created)``."""
    return await asyncio.to_thread(_create_bucket_sync, bucket_name, bucket_config, project_id, client=client)

def _delete_bucket_sync(bucket_name: str, force: bool = False, client: Optional[storage.Client] = None):
    """
    Deletes a GCS bucket. If 'force' is True, it will delete all objects in the bucket first.
    """
    try:
        storage_client = client or _get_shared_gcs_client()
        bucket = storage_client.get_bucket(bucket_name)
        
        if force:
            logger.warning(f"Forcing deletion of bucket '{bucket_name}'. Deleting all objects within it.")
            # This can be slow for large buckets.
            bucket.delete_blobs(list(bucket.list_blobs()))

        bucket.delete()
        logger.info(f"Bucket {bucket_name} deleted successfully.")
    except NotFound:
        logger.warning(f"Bucket {bucket_name} not found. Nothing to delete.")
    except Exception as e:
        logger.error(f"Failed to delete bucket {bucket_name}: {e}", exc_info=True)
        raise

async def delete_bucket(bucket_name: str, force: bool = False, client: Optional[storage.Client] = None):
    """Async wrapper for _delete_bucket_sync."""
    await asyncio.to_thread(_delete_bucket_sync, bucket_name, force, client=client)

def parse_gcs_bucket(gcs_bucket: str):
    """
    Parse a gcs_bucket string of the form 'bucket' or 'bucket/prefix' into (bucket, prefix).
    Ensures prefix is empty or ends with a single '/'.
    """
    parts = gcs_bucket.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    prefix = prefix.strip("/")
    if prefix:
        prefix += "/"
    return bucket, prefix

def get_gcs_catalog_path(bucket_name: str) -> str:
    """Returns the full GCS URI for the catalog folder (e.g., gs://bucket/catalog/)."""
    return f"gs://{bucket_name}/{CATALOG_FOLDER}/"

def get_gcs_collection_path(bucket_name: str, collection_id: str) -> str:
    """Returns the full GCS URI for a specific collection's folder (e.g., gs://bucket/collections/my-collection/)."""
    return f"gs://{bucket_name}/{COLLECTIONS_FOLDER}/{collection_id}/"

def get_blob_path_for_catalog_file(filename: str) -> str:
    """Returns the relative blob path for a file in the catalog folder (e.g., catalog/my-file.txt)."""
    return f"{CATALOG_FOLDER}/{filename}"

def get_blob_path_for_collection_folder(collection_id: str) -> str:
    """Returns the relative blob path prefix for a collection's folder (e.g., collections/my-collection/)."""
    return f"{COLLECTIONS_FOLDER}/{collection_id}/"

def get_blob_path_for_collection_file(collection_id: str, filename: str) -> str:
    """Returns the relative blob path for a file in a collection's folder (e.g., collections/my-collection/my-file.txt)."""
    return f"{get_blob_path_for_collection_folder(collection_id)}{filename}"

def parse_url(url: str) -> ParsedURL:
    """
    Parses a URL to determine its scheme and components (bucket, path).
    
    Args:
        url (str): The URL to parse.

    Returns:
        ParsedURL: A Pydantic model containing the parsed components of the URL.
    """
    parsed = urlparse(url)
    scheme_str = parsed.scheme.lower()

    try:
        scheme = FileSystem(scheme_str)
    except ValueError:
        # If the scheme is not in our Enum, treat it as unknown or a local path
        if os.path.exists(url):
            return ParsedURL(scheme=FileSystem.file, path=url)
        return ParsedURL(scheme=FileSystem.unknown, path=url)

    if scheme == FileSystem.gs:
        return ParsedURL(scheme=scheme, bucket=parsed.netloc, path=parsed.path.lstrip('/'))
    elif scheme in [FileSystem.https, FileSystem.http]:
        return ParsedURL(scheme=scheme, netloc=parsed.netloc, path=parsed.path.lstrip('/'))
    elif scheme == FileSystem.s3:
        return ParsedURL(scheme=scheme, bucket=parsed.netloc, path=parsed.path.lstrip('/'))
    elif scheme == FileSystem.file:
        return ParsedURL(scheme=scheme, path=parsed.path)
    
    return ParsedURL(scheme=FileSystem.unknown, path=url)


def upload_stream_to_gcs(
    byte_stream: Iterator[bytes],
    destination_uri: str,
    content_type: str = "application/octet-stream",
    client: Optional[storage.Client] = None,
) -> None:
    """Upload a byte-stream generator to a GCS destination.

    Uses ``blob.open("wb")`` (resumable upload) so the caller does not need to
    provide a seekable file-like object.  Generators are consumed chunk-by-chunk
    without buffering the entire payload in memory.

    Args:
        byte_stream: Iterator yielding bytes chunks.
        destination_uri: Full GCS URI (``gs://bucket/path``).
        content_type: MIME type for the uploaded blob.
        client: Optional pre-built GCS client; uses the shared client otherwise.
    """
    if not destination_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {destination_uri}")

    storage_client = client or _get_shared_gcs_client()
    bucket_name, blob_name = destination_uri.replace("gs://", "").split("/", 1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    try:
        logger.info(f"Uploading stream to {destination_uri} (content_type={content_type})...")
        with blob.open("wb", content_type=content_type) as writer:
            for chunk in byte_stream:
                if chunk:
                    writer.write(chunk)  # type: ignore[arg-type]  # BlobWriter stubs type write as str in text mode
        logger.info(f"Successfully uploaded to {destination_uri}.")
    except Exception as e:
        logger.error(f"Failed to upload to {destination_uri}: {e}", exc_info=True)
        raise
