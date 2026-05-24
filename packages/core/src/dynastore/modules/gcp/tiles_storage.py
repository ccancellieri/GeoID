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
from dynastore.modules.tiles.tiles_module import TileStorageProtocol, TileArchiveStorageProtocol, read_pmtiles_tile
from dynastore.modules.tiles.tiles_config import TilesCachingConfig
from dynastore.modules.concurrency import run_in_thread
from dynastore.models.protocols import StorageProtocol, CloudStorageClientProtocol, CloudIdentityProtocol
from dynastore.modules import get_protocol
from dynastore.modules.gcp.tools.signed_urls import generate_gcs_signed_url

logger = logging.getLogger(__name__)


async def _load_caching_config() -> TilesCachingConfig:
    """Fetch live ``TilesCachingConfig``; fall back to defaults if unavailable.

    Mirrors the ``ElasticsearchIndexConfig`` pattern (issue #489): a missing
    platform-configs layer (cold boot, unit test, manager not registered)
    yields safe defaults rather than crashing tile I/O.
    """
    from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
    from dynastore.tools.discovery import get_protocol as _get_protocol

    mgr = _get_protocol(PlatformConfigsProtocol)
    if mgr is None:
        return TilesCachingConfig()
    try:
        cfg = await mgr.get_config(TilesCachingConfig)
    except Exception as exc:
        logger.debug("TilesCachingConfig: get_config failed (%s); using defaults", exc)
        return TilesCachingConfig()
    return cfg if isinstance(cfg, TilesCachingConfig) else TilesCachingConfig()


def _build_blob_path(
    key_prefix: str, collection_id: str, tms_id: str, z: int, x: int, y: int, format: str
) -> str:
    return f"{key_prefix}/{collection_id}/{tms_id}/{z}/{x}/{y}.{format}"


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
            cfg = await _load_caching_config()
            if not cfg.cache_enabled:
                logger.debug(
                    "tile_cache event=skip reason=disabled action=save tile=%s",
                    tile_identifier,
                )
                return None

            logger.debug(f"Background save task started for tile: {tile_identifier}")
            storage_provider = self._get_storage_provider()
            client_provider = self._get_client_provider()

            bucket_name = await storage_provider.ensure_storage_for_catalog(catalog_id)
            if not bucket_name:
                raise RuntimeError(f"Could not resolve bucket for catalog {catalog_id}")

            blob_path = _build_blob_path(cfg.key_prefix, collection_id, tms_id, z, x, y, format)

            storage_client = client_provider.get_storage_client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)

            content_type = "application/vnd.mapbox-vector-tile" if format == 'mvt' else "application/octet-stream"

            await run_in_thread(
                blob.upload_from_string,
                data,
                content_type=content_type
            )

            blob.cache_control = f"public, max-age={cfg.ttl_seconds}"
            await run_in_thread(blob.patch)
            
            gcs_uri = f"gs://{bucket_name}/{blob_path}"
            logger.info(f"Background save task SUCCEEDED for tile: {tile_identifier} -> {gcs_uri}")
            return gcs_uri
        except Exception as e:
            logger.error(f"Background save task FAILED for tile: {tile_identifier}. Error: {e}", exc_info=True)
            # Do not re-raise; cache failures should not crash the host application or tests.

    async def get_tile(self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int, format: str) -> Optional[bytes]:
        cfg = await _load_caching_config()
        if not cfg.cache_enabled:
            return None

        storage_provider = self._get_storage_provider()
        client_provider = self._get_client_provider()

        bucket_name = await storage_provider.get_storage_identifier(catalog_id)
        if not bucket_name:
            return None # Bucket doesn't exist, tile doesn't exist

        blob_path = _build_blob_path(cfg.key_prefix, collection_id, tms_id, z, x, y, format)

        storage_client = client_provider.get_storage_client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        def _fetch():
            from google.api_core.exceptions import NotFound
            try:
                return blob.download_as_bytes()
            except NotFound:
                return None

        return await run_in_thread(_fetch)

    @cached(maxsize=2048, namespace="gcp_tile_exists")
    async def check_tile_exists(self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int, format: str) -> bool:
        cfg = await _load_caching_config()
        if not cfg.cache_enabled:
            return False

        storage_provider = self._get_storage_provider()
        client_provider = self._get_client_provider()

        bucket_name = await storage_provider.get_storage_identifier(catalog_id)
        if not bucket_name:
            return False

        blob_path = _build_blob_path(cfg.key_prefix, collection_id, tms_id, z, x, y, format)
        storage_client = client_provider.get_storage_client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        # Use the injected concurrency backend
        return await run_in_thread(blob.exists)

    async def get_tile_url(self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int, format: str) -> Optional[str]:
        cfg = await _load_caching_config()
        if not cfg.cache_enabled:
            return None

        storage_provider = self._get_storage_provider()
        client_provider = self._get_client_provider()
        identity_provider = self._get_identity_provider()

        bucket_name = await storage_provider.get_storage_identifier(catalog_id)
        if not bucket_name:
            return None

        blob_path = _build_blob_path(cfg.key_prefix, collection_id, tms_id, z, x, y, format)
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

        cfg = await _load_caching_config()
        prefix = f"{cfg.key_prefix}/{collection_id}/"
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

    async def delete_tile(
        self,
        catalog_id: str,
        collection_id: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
        format: str,
    ) -> bool:
        """Delete a single cached tile blob (idempotent mark-stale)."""
        storage_provider = self._get_storage_provider()
        bucket_name = await storage_provider.get_storage_identifier(catalog_id)
        if not bucket_name:
            return True  # No bucket → nothing to invalidate; idempotent success.

        cfg = await _load_caching_config()
        blob_path = _build_blob_path(
            cfg.key_prefix, collection_id, tms_id, z, x, y, format
        )
        client_provider = self._get_client_provider()
        storage_client = client_provider.get_storage_client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        def _delete() -> None:
            # ``if_generation_match`` is not used — a plain delete is
            # idempotent enough for mark-stale; a missing blob raises NotFound
            # which we swallow.
            try:
                blob.delete()
            except Exception as exc:  # google.cloud.exceptions.NotFound etc.
                if "404" in str(exc) or "NotFound" in type(exc).__name__:
                    return
                raise

        try:
            await run_in_thread(_delete)
            cache_clear(self.check_tile_exists)
            return True
        except Exception as exc:
            logger.error(
                "tile_cache: failed to delete blob %s in bucket %s: %s",
                blob_path, bucket_name, exc,
            )
            return False

    async def delete_tile_variants(
        self,
        catalog_id: str,
        collection_id: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
        formats: Any,
    ) -> bool:
        """Delete every cached variant of one coordinate from GCS (#1292).

        A coordinate is cached under one object per ``effective_cache_id`` ×
        ``format``: the bare ``collection_id``, ``{collection_id}@{hash}``, and
        multi-collection comma-joined cache ids. The blob key is
        ``{key_prefix}/{cache_id}/{tms_id}/{z}/{x}/{y}.{format}``, so the
        cache-id is the path segment after the prefix.

        GCS only lists by key prefix, so we list under
        ``{key_prefix}/{collection_id}`` (catches the exact id, the
        ``@hash`` variants, and multi-collection keys where this collection is
        FIRST), then keep only blobs whose cache-id segment is a real variant
        of this collection and whose suffix matches the coordinate + a served
        format. Phase-1 known gap: a multi-collection key where this collection
        is NOT the first segment (e.g. ``other,this``) is not reachable by a
        cheap prefix list and is left for the bucket TTL / a reconcile to evict
        — the PG backend covers every position via SQL ``LIKE``.
        """
        fmt_list = list(formats) if formats else []
        if not fmt_list:
            return True
        storage_provider = self._get_storage_provider()
        bucket_name = await storage_provider.get_storage_identifier(catalog_id)
        if not bucket_name:
            return True  # No bucket → nothing to invalidate; idempotent success.

        cfg = await _load_caching_config()
        client_provider = self._get_client_provider()
        storage_client = client_provider.get_storage_client()
        bucket = storage_client.bucket(bucket_name)

        list_prefix = f"{cfg.key_prefix}/{collection_id}"
        wanted_suffixes = {
            f"/{tms_id}/{z}/{x}/{y}.{fmt}" for fmt in fmt_list
        }

        def _cache_id_matches(cache_seg: str) -> bool:
            # Reconstruct: blob = {key_prefix}/{cache_seg}/{tms}/{z}/{x}/{y}.{fmt}
            # cache_seg is the effective_cache_id. Accept exact, @hash, and
            # comma-list membership (cid-first is the only position reachable
            # by this prefix list; mid/last documented as a Phase-1 gap).
            base = cache_seg.split("@", 1)[0]  # strip params hash
            parts = base.split(",")
            return collection_id in parts

        def _delete_matching() -> bool:
            blobs = list(bucket.list_blobs(prefix=list_prefix))
            to_delete = []
            plen = len(cfg.key_prefix) + 1  # "{key_prefix}/"
            for blob in blobs:
                name = blob.name
                # cache-id segment is between the prefix and the next "/".
                rest = name[plen:]
                slash = rest.find("/")
                if slash == -1:
                    continue
                cache_seg = rest[:slash]
                suffix = rest[slash:]
                if suffix in wanted_suffixes and _cache_id_matches(cache_seg):
                    to_delete.append(blob)
            if to_delete:
                bucket.delete_blobs(to_delete)
            return True

        try:
            await run_in_thread(_delete_matching)
            cache_clear(self.check_tile_exists)
            return True
        except Exception as exc:
            logger.error(
                "tile_cache: failed to delete tile variants for %s/%s "
                "%s/%s/%s/%s in bucket %s: %s",
                catalog_id, collection_id, tms_id, z, x, y, bucket_name, exc,
            )
            return False

    async def delete_storage_for_catalog(self, catalog_id: str):
        """Deletes all tile storage for a catalog."""
        storage_provider = self._get_storage_provider()
        bucket_name = await storage_provider.get_storage_identifier(catalog_id)
        if not bucket_name:
            return

        cfg = await _load_caching_config()
        prefix = f"{cfg.key_prefix}/"
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
        path = await self._archive_path(catalog_id, collection_id, tms_id)
        if not path:
            return None
        storage = self._get_storage()

        async def _range_read(offset: int, length: int) -> Optional[bytes]:
            return await storage.download_bytes_range(path, offset, length)

        # Same header -> directory -> tile traversal as the PG archive reader
        # (#1241): range-read the object-storage PMTiles directly via the
        # ``pmtiles`` primitives, with no dependency on an external reader
        # package, so a single-tile read never pulls the whole archive.
        try:
            return await read_pmtiles_tile(_range_read, z, x, y)
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
