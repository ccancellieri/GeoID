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

import io
import zipfile
import logging
import httpx
from typing import Any, Dict, Optional, List
from pydantic import BaseModel
try:
    from google.cloud import storage
except ImportError:
    storage = None

try:
    from charset_normalizer import from_bytes
except ImportError:
    from_bytes = None

from dynastore.tasks.ingestion.operations import IngestionOperationInterface, ingestion_operation
from dynastore.modules import get_protocol
from dynastore.models.protocols import StorageProtocol, CloudStorageClientProtocol
from dynastore.modules.concurrency import run_in_thread

logger = logging.getLogger(__name__)

@ingestion_operation
class ContentTypeInspectorOperation(IngestionOperationInterface):
    """
    Analyzes raw bytes from a GCP blob (standard or zipped) to guess encoding.
    Specifically targets .dbf files for Shapefiles.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def pre_op(self, catalog: Any, collection: Any, asset: Any) -> Any:
        uri = asset.uri
        if not uri or not uri.startswith("gs://"):
            return asset

        if from_bytes is None:
            logger.warning("charset-normalizer not installed. Skipping encoding detection.")
            return asset

        try:
            # Parse bucket and blob
            path_parts = uri.replace("gs://", "").split("/", 1)
            if len(path_parts) < 2:
                return asset
            bucket_name, blob_name = path_parts[0], path_parts[1]
            
            storage_provider = get_protocol(CloudStorageClientProtocol)
            if not storage_provider:
                logger.warning("CloudStorageClientProtocol not found. Skipping encoding detection.")
                return asset
            
            storage_client = storage_provider.get_storage_client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)

            sample_size = 50000
            
            # Download a chunk. We offset by 1024 to skip DBF headers and hit actual text.
            raw_data = await run_in_thread(blob.download_as_bytes, start=1024, end=1024 + sample_size)
            content_to_analyze = raw_data

            # Handle Zipped Shapefiles
            if blob_name.lower().endswith('.zip'):
                try:
                    with zipfile.ZipFile(io.BytesIO(raw_data)) as z:
                        dbf_files = [f for f in z.namelist() if f.lower().endswith('.dbf')]
                        if dbf_files:
                            with z.open(dbf_files[0]) as f:
                                content_to_analyze = f.read(sample_size)
                except Exception:
                    pass

            results = from_bytes(content_to_analyze)
            best_guess = results.best()
            
            if best_guess and best_guess.confidence > 0.7:  # type: ignore[attr-defined]
                detected_encoding = best_guess.encoding
                logger.info(f"Task '{self.task_id}': Detected encoding '{detected_encoding}' for {uri}.")
                self.task_request.encoding = detected_encoding
            
        except Exception as e:
            logger.warning(f"Task '{self.task_id}': Encoding detection failed for {uri}: {e}")
            
        return asset

    async def post_op(self, catalog: Any, collection: Any, asset: Any, status: str, error_message: Optional[str] = None):
        pass

class AssetDownloaderConfig(BaseModel):
    metadata: Optional[Dict[str, Any]] = None

@ingestion_operation
class AssetDownloaderOperation(IngestionOperationInterface[AssetDownloaderConfig]):
    """
    Downloads an external asset to a GCS bucket.
    Supports setting optional metadata on the asset creation.
    """
    async def pre_op(self, catalog: Any, collection: Any, asset: Any) -> Any:
        # 1. Apply metadata if provided in the operation configuration
        if self.config and self.config.metadata:
            logger.info(f"Task '{self.task_id}': Applying custom metadata to asset: {self.config.metadata}")
            # Ensure asset.metadata is a dict and update it
            if not hasattr(asset, 'metadata') or asset.metadata is None:
                # We expect IngestionAsset as passed from IngestionTask
                asset.metadata = {}
            # Update the metadata on the asset object
            asset.metadata.update(self.config.metadata)

        uri = asset.uri
        if not uri or uri.startswith("gs://"):
            return asset

        # Check if it's a URL
        if not (uri.startswith("http://") or uri.startswith("https://")):
            return asset

        try:
            storage_manager = get_protocol(StorageProtocol)
            storage_provider = get_protocol(CloudStorageClientProtocol)
            
            if not storage_manager or not storage_provider:
                logger.warning("Storage protocols not found. Skipping asset download.")
                return asset
            
            # Ensure bucket exists
            bucket_name = await storage_manager.get_or_create_bucket_for_catalog(catalog.id)  # type: ignore[attr-defined]
            if not bucket_name:
                raise RuntimeError(f"Failed to ensure storage bucket for catalog '{catalog.id}'.")

            logger.info(f"Task '{self.task_id}': Downloading asset from {uri} to collection folder...")
            
            # Use the collection storage path to determine the blob name
            collection_path = await storage_manager.get_collection_storage_path(catalog.id, collection.id)
            if not collection_path:
                 raise RuntimeError(f"Failed to determine storage path for collection '{catalog.id}:{collection.id}'.")

            # Extract prefix from gs://bucket/prefix/
            prefix = '/'.join(collection_path.replace("gs://", "").split("/")[1:])
            
            filename = uri.split('/')[-1]
            filename = filename.split('?')[0]
            
            # Store in a task-specific subfolder within the collection space
            blob_name = f"{prefix}ingestion-temp/{self.task_id}/{filename}"
            
            # Download file
            async with httpx.AsyncClient() as client:
                response = await client.get(uri)
                response.raise_for_status()
                content = response.content

            # Upload to storage
            storage_client = storage_provider.get_storage_client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            await run_in_thread(blob.upload_from_string, content)
            
            new_uri = f"gs://{bucket_name}/{blob_name}"
            logger.info(f"Task '{self.task_id}': Asset downloaded and uploaded to {new_uri}")
            
            # Update asset URI
            asset.uri = new_uri
            
        except Exception as e:
            logger.error(f"Task '{self.task_id}': Asset download failed for {uri}: {e}")
            raise e
            
        return asset

    async def post_op(self, catalog: Any, collection: Any, asset: Any, status: str, error_message: Optional[str] = None):
        pass

