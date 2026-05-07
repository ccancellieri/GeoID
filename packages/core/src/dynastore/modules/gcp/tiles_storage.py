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

import logging
import os
import shutil
import tempfile
from typing import Optional, Any, Dict
from dynastore.tools.cache import cached, cache_clear
from datetime import timedelta
from dynastore.modules.tiles.tiles_module import TileStorageProtocol, TileArchiveStorageProtocol
from dynastore.modules.concurrency import run_in_thread
from dynastore.models.protocols import StorageProtocol, CloudStorageClientProtocol, CloudIdentityProtocol
from dynastore.modules import get_protocol
from dynastore.modules.gcp.tools.signed_urls import generate_gcs_signed_url

logger = logging.getLogger(__name__)


class TileBucketPreseedStorage(TileStorageProtocol):
    """
    GCS-based tile storage provider.
    """

    def __init__(self):
        # registry/app_state in background threads where context is lost.
        # We use late binding for protocols.
        pass

    def _get_storage_provider(self) -> StorageProtocol:
        provider = get_protocol(StorageProtocol)
        if not provider:
            raise RuntimeError("StorageProtocol (GCP) is not available.")
        return provider

    def _get_client_provider(self) -> CloudStorageClientProtocol:
        provider = get_protocol(CloudStorageClientProtocol)
        if not provider:
            raise RuntimeError("CloudStorageClientProtocol (GCP) is not available.")
        return provider

    def _get_identity_provider(self) -> CloudIdentityProtocol:
        provider = get_protocol(CloudIdentityProtocol)
        if not provider:
            raise RuntimeError("CloudIdentityProtocol (GCP) is not available.")
        return provider

    async def save_tile(self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int, data: bytes, format: str) -> Optional[str]:
        tile_identifier = f"{catalog_id}/{collection_id}/{tms_id}/{z}/{x}/{y}.{format}"
        try:
            logger.debug(f"Background save task started for tile: {tile_identifier}")
            storage_provider = self._get_storage_provider()
            client_provider = self._get_client_provider()
            
            bucket_name = await storage_provider.ensure_storage_for_catalog(catalog_id)
            if not bucket_name:
                raise RuntimeError(f"Could not resolve bucket for catalog {catalog_id}")
            
            blob_path = f"tiles/collections/{collection_id}/{tms_id}/{z}/{x}/{y}.{format}"
            
            storage_client = client_provider.get_storage_client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            
            content_type = "application/vnd.mapbox-vector-tile" if format == 'mvt' else "application/octet-stream"
            
            # Use the injected concurrency backend
            # IMPORTANT: Pass content_type to upload_from_string to ensure it matches the metadata
            await run_in_thread(
                blob.upload_from_string,
                data,
                content_type=content_type
            )
            
            # Set cache control after upload
            blob.cache_control = "public, max-age=31536000"
            await run_in_thread(blob.patch)
            
            gcs_uri = f"gs://{bucket_name}/{blob_path}"
            logger.info(f"Background save task SUCCEEDED for tile: {tile_identifier} -> {gcs_uri}")
            return gcs_uri
        except Exception as e:
            logger.error(f"Background save task FAILED for tile: {tile_identifier}. Error: {e}", exc_info=True)
            # Do not re-raise; cache failures should not crash the host application or tests.

    async def get_tile(self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int, format: str) -> Optional[bytes]:
        storage_provider = self._get_storage_provider()
        client_provider = self._get_client_provider()

        bucket_name = await storage_provider.get_storage_identifier(catalog_id)
        if not bucket_name:
            return None # Bucket doesn't exist, tile doesn't exist

        blob_path = f"tiles/collections/{collection_id}/{tms_id}/{z}/{x}/{y}.{format}"

        storage_client = client_provider.get_storage_client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        # This is more efficient as it combines existence check and download
        # into a single I/O operation in the background thread.
        def _fetch():
            from google.api_core.exceptions import NotFound
            try:
                return blob.download_as_bytes()
            except NotFound:
                return None

        return await run_in_thread(_fetch)

    @cached(maxsize=2048, namespace="gcp_tile_exists")
    async def check_tile_exists(self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int, format: str) -> bool:
        storage_provider = self._get_storage_provider()
        client_provider = self._get_client_provider()

        bucket_name = await storage_provider.get_storage_identifier(catalog_id)
        if not bucket_name:
            return False
            
        blob_path = f"tiles/collections/{collection_id}/{tms_id}/{z}/{x}/{y}.{format}"
        storage_client = client_provider.get_storage_client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        # Use the injected concurrency backend
        return await run_in_thread(blob.exists)

    async def get_tile_url(self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int, format: str) -> Optional[str]:
        storage_provider = self._get_storage_provider()
        client_provider = self._get_client_provider()
        identity_provider = self._get_identity_provider()

        bucket_name = await storage_provider.get_storage_identifier(catalog_id)
        if not bucket_name:
            return None

        blob_path = f"tiles/collections/{collection_id}/{tms_id}/{z}/{x}/{y}.{format}"
        return await generate_gcs_signed_url(
            f"gs://{bucket_name}/{blob_path}",
            method="GET",
            expiration=timedelta(minutes=60),
            client_provider=client_provider,
            identity_provider=identity_provider,
            check_exists=True,
        )

    async def get_preseed_state(self, catalog_id: str, collection_id: str, tms_id: str) -> Dict[str, Any]:
        """GCS storage doesn't track preseed state internally yet."""
        return {}

    async def delete_tiles_for_collection(self, catalog_id: str, collection_id: str) -> int:
        """Deletes all tiles for a given collection from GCS."""
        storage_provider = self._get_storage_provider()
        bucket_name = await storage_provider.get_storage_identifier(catalog_id)
        if not bucket_name:
            return 0
        
        prefix = f"tiles/collections/{collection_id}/"
        client_provider = self._get_client_provider()
        storage_client = client_provider.get_storage_client()
        bucket = storage_client.bucket(bucket_name)
        
        # We need to list and delete blobs. 
        # For efficiency in a thread-safe way using the concurrency backend.
        def _delete_all():
            blobs = list(bucket.list_blobs(prefix=prefix))
            if not blobs:
                return 0
            # bucket.delete_blobs handles large lists by chunking internally
            bucket.delete_blobs(blobs)
            return len(blobs)

        result = await run_in_thread(_delete_all)
        # Clear existence cache for this collection
        if result > 0:
            cache_clear(self.check_tile_exists)
        return result

    async def delete_storage_for_catalog(self, catalog_id: str):
        """Deletes all tile storage for a catalog."""
        storage_provider = self._get_storage_provider()
        bucket_name = await storage_provider.get_storage_identifier(catalog_id)
        if not bucket_name:
            return
            
        prefix = "tiles/"
        client_provider = self._get_client_provider()
        storage_client = client_provider.get_storage_client()
        bucket = storage_client.bucket(bucket_name)
        
        def _delete_all():
            blobs = list(bucket.list_blobs(prefix=prefix))
            if blobs:
                bucket.delete_blobs(blobs)

        await run_in_thread(_delete_all)
        # Clear existence cache
        cache_clear(self.check_tile_exists)


class StorageBackedTileArchive(TileArchiveStorageProtocol):
    """PMTiles archive storage backed by any StorageProtocol provider."""

    def _get_storage(self) -> StorageProtocol:
        provider = get_protocol(StorageProtocol)
        if not provider:
            raise RuntimeError("StorageProtocol is not registered.")
        return provider

    async def _archive_path(self, catalog_id: str, collection_id: str, tms_id: str) -> Optional[str]:
        storage = self._get_storage()
        bucket_name = await storage.get_storage_identifier(catalog_id)
        if not bucket_name:
            return None
        return f"gs://{bucket_name}/pmtiles/{collection_id}/{tms_id}.pmtiles"

    async def save_archive(self, catalog_id: str, collection_id: str, tms_id: str, data_file: Any) -> str:
        storage = self._get_storage()
        bucket_name = await storage.ensure_storage_for_catalog(catalog_id)
        if not bucket_name:
            raise RuntimeError(f"No storage bucket available for catalog '{catalog_id}'.")
        target_path = f"gs://{bucket_name}/pmtiles/{collection_id}/{tms_id}.pmtiles"
        with tempfile.NamedTemporaryFile(suffix=".pmtiles", delete=False) as tmp:
            shutil.copyfileobj(data_file, tmp)
            tmp_path = tmp.name
        try:
            await storage.upload_file(tmp_path, target_path, "application/vnd.pmtiles")
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
        logger.info("PMTiles archive saved: %s", target_path)
        return target_path

    @cached(maxsize=512, namespace="pmtiles_archive_exists")
    async def archive_exists(self, catalog_id: str, collection_id: str, tms_id: str) -> bool:
        path = await self._archive_path(catalog_id, collection_id, tms_id)
        if not path:
            return False
        return await self._get_storage().file_exists(path)

    async def get_tile_from_archive(self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int) -> Optional[bytes]:
        try:
            from apmtiles import AsyncPMTilesReader  # type: ignore[import]
        except ImportError:
            logger.error("apmtiles not installed; cannot read PMTiles archives.")
            return None
        path = await self._archive_path(catalog_id, collection_id, tms_id)
        if not path:
            return None
        storage = self._get_storage()
        async def _range_read(offset: int, length: int) -> bytes:
            return await storage.download_bytes_range(path, offset, length)
        try:
            reader = AsyncPMTilesReader(_range_read)
            return await reader.get_tile(z, x, y)
        except Exception as exc:
            logger.warning("Failed reading tile %d/%d/%d from PMTiles %s: %s", z, x, y, path, exc)
            return None

    async def delete_archive(self, catalog_id: str, collection_id: str, tms_id: str) -> bool:
        path = await self._archive_path(catalog_id, collection_id, tms_id)
        if not path:
            return False
        await self._get_storage().delete_file(path)
        cache_clear(self.archive_exists)
        logger.info("PMTiles archive deleted: %s", path)
        return True
