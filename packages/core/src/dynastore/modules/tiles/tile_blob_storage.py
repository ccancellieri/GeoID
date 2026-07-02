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

"""Generic object-storage tile writer + the composite ``TileStorageProtocol``
façade that selects among the configured writers (see ``tiles_writers``).

``StorageTileWriter`` is the ONE implementation shared by every object-
storage-backed tile writer config (the ``gcp`` module's ``GcsTileWriterConfig``
over ``gs://``, ``modules/local``'s local-disk config over ``file://``):
it just moves bytes given an already-resolved ``(StorageProtocol, base_uri,
prefix)`` — resolving WHERE (bucket name / root path / existence) is each
config's own factory's job, registered via
``tiles_writers.register_tile_writer_factory``. This replaces the GCS-only
``TileBucketPreseedStorage`` that used to live in ``modules/gcp/tiles_storage``
and imported ``google`` at module scope, which crashed the PG-only preseed
target on job images without the ``gcp`` extra (the field-bug this module
fixes).

``CompositeTileStorage`` is the single object registered as
``TileStorageProtocol``: for each call it resolves the live writer list
(``tiles_writers.resolve_effective_writers``) for the catalog and selects
ONE active writer (``tiles_writers.select_tile_writer`` — first AVAILABLE in
list order, hint-elevated) to serve it — no fan-out.

Scheme-claim mechanism: a ``StorageProtocol``/``TileUrlSignerProtocol``
implementation declares which URI scheme(s) it serves via a
``supported_schemes: frozenset[str]`` class attribute (e.g. ``{"gs"}`` on the
GCP module's storage mixin, ``{"file"}`` on ``LocalStorageOps``).

``StorageBackedTileArchive`` (PMTiles archive storage) is also moved here
verbatim from ``modules/gcp/tiles_storage`` — it already only used
``StorageProtocol`` and had no GCS-specific code; it was simply misfiled.
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
from typing import Any, BinaryIO, ClassVar, Dict, FrozenSet, Optional, Protocol, Sequence, Tuple, runtime_checkable
from urllib.parse import urlsplit

from dynastore.tools.cache import cached, cache_clear, cache_invalidate
from dynastore.models.protocols import StorageProtocol
from dynastore.modules import get_protocol, get_protocols
from dynastore.modules.storage.hints import Hint
from dynastore.modules.tiles.tiles_config import _load_caching_config
from dynastore.modules.tiles.tiles_module import (
    TileArchiveStorageProtocol,
    TileStorageProtocol,
    read_pmtiles_tile,
)
from dynastore.modules.tiles.tiles_writers import resolve_effective_writers, select_tile_writer

logger = logging.getLogger(__name__)

# scheme -> human hint for the RuntimeError when nothing claims it.
_SCHEME_EXTRA_HINTS = {"gs": "the 'gcp' extra", "file": "a local-disk storage module"}


@runtime_checkable
class TileUrlSignerProtocol(Protocol):
    """Optional capability: produce a short-lived, directly-fetchable URL for
    a cached-tile object.

    Registered ONLY by a module that can sign/pre-authenticate URLs for its
    object-storage backend (the ``gcp`` module wraps ``generate_gcs_signed_url``),
    and scoped to the scheme(s) it can sign via ``supported_schemes`` — a
    signer never claims to sign a ``file://`` or ``pg://`` target. When no
    signer claims the resolved scheme, ``StorageTileWriter.get_tile_url``
    returns ``None`` and ``tiles_service._try_cached_tile`` falls back to the
    proxy path — the 307-redirect wire behavior is unaffected either way.
    """

    supported_schemes: ClassVar[FrozenSet[str]]

    async def sign(self, object_uri: str) -> Optional[str]:
        """Return a signed GET URL for ``object_uri``, or None if the object
        is absent or signing is unavailable."""
        ...


def _build_blob_path(
    key_prefix: str, collection_id: str, tms_id: str, z: int, x: int, y: int, format: str
) -> str:
    return f"{key_prefix}/{collection_id}/{tms_id}/{z}/{x}/{y}.{format}"


def _build_object_uri(
    base_uri: str, prefix: str, collection_id: str, tms_id: str, z: int, x: int, y: int, format: str
) -> str:
    return f"{base_uri}/{_build_blob_path(prefix, collection_id, tms_id, z, x, y, format)}"


@cached(maxsize=16, ttl=60, namespace="tile_external_bucket_exists")
async def external_bucket_exists(bucket_name: str) -> bool:
    """True if an explicitly-targeted GCS bucket exists and is reachable.

    Cached per bucket name (a preseed writes millions of tiles); the 60s TTL
    lets a cached ``False`` self-heal shortly after the operator creates the
    bucket. Only meaningful for the ``gs`` scheme — geoid never provisions an
    explicit writer bucket itself, so a write-path resolution verifies it
    first. Public (not underscore-prefixed): the ``gcp`` module's
    ``GcsTileWriterConfig`` factory calls this directly.
    """
    from dynastore.models.protocols import CloudStorageClientProtocol
    from dynastore.modules.concurrency import run_in_thread

    provider = get_protocol(CloudStorageClientProtocol)
    if not provider:
        return False
    client = provider.get_storage_client()

    def _lookup() -> bool:
        return client.lookup_bucket(bucket_name) is not None

    return await run_in_thread(_lookup)


def get_storage_for_scheme(scheme: str) -> Optional[StorageProtocol]:
    """Return the registered ``StorageProtocol`` provider that claims
    ``scheme``, or ``None`` if none does. Shared by every object-storage
    writer factory (GCS, local disk)."""
    for provider in get_protocols(StorageProtocol):
        if scheme in (getattr(provider, "supported_schemes", None) or ()):
            return provider
    return None


class StorageTileWriter(TileStorageProtocol):
    """Generic ``StorageProtocol``-backed tile-cache writer.

    Parameterized by an already-resolved ``(storage, base_uri, prefix)`` —
    the config-specific factory (GCS's ``GcsTileWriterConfig``, local disk's
    equivalent) owns WHERE (bucket/root resolution, existence checks); this
    class only moves bytes, so it is shared by every object-storage backend
    without any backend-specific code here.
    """

    def __init__(self, storage: StorageProtocol, base_uri: str, prefix: str) -> None:
        self._storage = storage
        self._base_uri = base_uri
        self._prefix = prefix

    def _get_signer(self) -> Optional[TileUrlSignerProtocol]:
        scheme = urlsplit(self._base_uri).scheme
        for signer in get_protocols(TileUrlSignerProtocol):
            if scheme in (getattr(signer, "supported_schemes", None) or ()):
                return signer
        return None

    async def save_tile(
        self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int, data: bytes, format: str
    ) -> Optional[str]:
        object_uri = _build_object_uri(self._base_uri, self._prefix, collection_id, tms_id, z, x, y, format)
        content_type = (
            "application/vnd.mapbox-vector-tile" if format == "mvt" else "application/octet-stream"
        )
        await self._storage.upload_file_content(object_uri, data, content_type=content_type)
        return object_uri

    async def get_tile(
        self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int, format: str
    ) -> Optional[bytes]:
        object_uri = _build_object_uri(self._base_uri, self._prefix, collection_id, tms_id, z, x, y, format)
        return await self._storage.download_file_content(object_uri)

    async def check_tile_exists(
        self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int, format: str
    ) -> bool:
        object_uri = _build_object_uri(self._base_uri, self._prefix, collection_id, tms_id, z, x, y, format)
        return await self._storage.file_exists(object_uri)

    async def get_tile_url(
        self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int, format: str
    ) -> Optional[str]:
        signer = self._get_signer()
        if signer is None:
            return None
        object_uri = _build_object_uri(self._base_uri, self._prefix, collection_id, tms_id, z, x, y, format)
        return await signer.sign(object_uri)

    async def get_preseed_state(self, catalog_id: str, collection_id: str, tms_id: str) -> Dict[str, Any]:
        """Object storage doesn't track preseed state internally yet."""
        return {}

    async def delete_tiles_for_collection(self, catalog_id: str, collection_id: str) -> int:
        list_prefix = getattr(self._storage, "list_prefix", None)
        if list_prefix is None:
            logger.warning(
                "StorageTileWriter: provider has no list_prefix — cannot "
                "bulk-delete collection %s/%s.", catalog_id, collection_id,
            )
            return 0
        paths = await list_prefix(self._base_uri, f"{self._prefix}/{collection_id}/")
        for path in paths:
            await self._storage.delete_file(path)
        return len(paths)

    async def delete_tile(
        self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int, format: str
    ) -> bool:
        object_uri = _build_object_uri(self._base_uri, self._prefix, collection_id, tms_id, z, x, y, format)
        try:
            await self._storage.delete_file(object_uri)
        except Exception as exc:
            if "404" not in str(exc) and "NotFound" not in type(exc).__name__:
                logger.error("StorageTileWriter: failed to delete %s: %s", object_uri, exc)
                return False
        return True

    async def delete_tile_variants(
        self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int, formats: Sequence[str],
    ) -> bool:
        """Delete every cached variant of one coordinate (#1292). See
        ``TileStorageProtocol.delete_tile_variants`` for the cache-id variant
        shapes this matches (bare, ``@hash``-parameterized, multi-collection)."""
        fmt_list = list(formats) if formats else []
        if not fmt_list:
            return True
        list_prefix = getattr(self._storage, "list_prefix", None)
        if list_prefix is None:
            logger.warning(
                "StorageTileWriter: provider has no list_prefix — cannot "
                "bulk-delete tile variants for %s/%s.", catalog_id, collection_id,
            )
            return True

        wanted_suffixes = {f"/{tms_id}/{z}/{x}/{y}.{fmt}" for fmt in fmt_list}

        def _cache_id_matches(cache_seg: str) -> bool:
            base = cache_seg.split("@", 1)[0]
            return collection_id in base.split(",")

        paths = await list_prefix(self._base_uri, f"{self._prefix}/{collection_id}")
        base_len = len(self._base_uri) + 1
        plen = len(self._prefix) + 1
        for path in paths:
            key = path[base_len:] if path.startswith(self._base_uri + "/") else path
            rest = key[plen:]
            slash = rest.find("/")
            if slash == -1:
                continue
            cache_seg, suffix = rest[:slash], rest[slash:]
            if suffix in wanted_suffixes and _cache_id_matches(cache_seg):
                await self._storage.delete_file(path)
        return True

    async def drop_storage(self, catalog_id: str) -> None:
        list_prefix = getattr(self._storage, "list_prefix", None)
        if list_prefix is None:
            logger.warning(
                "StorageTileWriter: provider has no list_prefix — cannot "
                "drop tile storage for catalog %r.", catalog_id,
            )
            return
        paths = await list_prefix(self._base_uri, f"{self._prefix}/")
        for path in paths:
            await self._storage.delete_file(path)


class CompositeTileStorage(TileStorageProtocol):
    """Selects and delegates to ONE active tile-cache writer per call.

    The single object registered as ``TileStorageProtocol``; ``tiles_engine``,
    ``tiles_service``, and the preseed task consume it unchanged. See
    ``tiles_writers.select_tile_writer`` for the selection rule (first
    available in ``writers`` list order, hint-elevated).

    ``hints`` is an optional keyword-only extension beyond the
    ``TileStorageProtocol`` structural interface — generic callers (which
    only know the protocol) get the default (empty, no preference); a caller
    with a genuine preference (e.g. a future preseed cutover wanting
    read-your-write consistency) can pass ``hints=frozenset({Hint.DURABLE})``
    directly against this concrete class.
    """

    async def _select(
        self, catalog_id: str, *, ensure: bool, hints: FrozenSet[Hint] = frozenset(),
    ) -> Tuple[str, TileStorageProtocol]:
        cfg = await _load_caching_config()
        writers = await resolve_effective_writers(cfg, catalog_id)
        return await select_tile_writer(writers, cfg, catalog_id, ensure=ensure, hints=hints)

    async def save_tile(
        self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int, data: bytes, format: str,
        *, hints: FrozenSet[Hint] = frozenset(),
    ) -> Optional[str]:
        tile_identifier = f"{catalog_id}/{collection_id}/{tms_id}/{z}/{x}/{y}.{format}"
        cfg = await _load_caching_config()
        if not cfg.cache_enabled:
            logger.debug("tile_cache event=skip reason=disabled action=save tile=%s", tile_identifier)
            return None
        try:
            _key, writer = await self._select(catalog_id, ensure=True, hints=hints)
        except RuntimeError as exc:
            logger.error("tile_cache: %s; tile %s not saved.", exc, tile_identifier)
            return None
        try:
            return await writer.save_tile(catalog_id, collection_id, tms_id, z, x, y, data, format)
        except Exception as exc:
            logger.error(
                "Background save task FAILED for tile: %s. Error: %s", tile_identifier, exc, exc_info=True,
            )
            # Do not re-raise; cache failures should not crash the host application or tests.
            return None

    async def get_tile(
        self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int, format: str,
        *, hints: FrozenSet[Hint] = frozenset(),
    ) -> Optional[bytes]:
        cfg = await _load_caching_config()
        if not cfg.cache_enabled:
            return None
        try:
            _key, writer = await self._select(catalog_id, ensure=False, hints=hints)
        except RuntimeError as exc:
            logger.warning("tile_cache: %s", exc)
            return None
        return await writer.get_tile(catalog_id, collection_id, tms_id, z, x, y, format)

    @cached(maxsize=2048, namespace="tile_composite_exists")
    async def check_tile_exists(
        self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int, format: str,
    ) -> bool:
        cfg = await _load_caching_config()
        if not cfg.cache_enabled:
            return False
        try:
            _key, writer = await self._select(catalog_id, ensure=False)
        except RuntimeError:
            return False
        return await writer.check_tile_exists(catalog_id, collection_id, tms_id, z, x, y, format)

    async def get_tile_url(
        self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int, format: str,
    ) -> Optional[str]:
        cfg = await _load_caching_config()
        if not cfg.cache_enabled:
            return None
        try:
            _key, writer = await self._select(catalog_id, ensure=False)
        except RuntimeError:
            return None
        return await writer.get_tile_url(catalog_id, collection_id, tms_id, z, x, y, format)

    async def get_preseed_state(self, catalog_id: str, collection_id: str, tms_id: str) -> Dict[str, Any]:
        try:
            _key, writer = await self._select(catalog_id, ensure=False)
        except RuntimeError:
            return {}
        return await writer.get_preseed_state(catalog_id, collection_id, tms_id)

    async def delete_tiles_for_collection(self, catalog_id: str, collection_id: str) -> int:
        try:
            _key, writer = await self._select(catalog_id, ensure=False)
        except RuntimeError:
            return 0
        total = await writer.delete_tiles_for_collection(catalog_id, collection_id)
        if total:
            cache_clear(self.check_tile_exists)
        return total

    async def delete_tile(
        self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int, format: str,
    ) -> bool:
        try:
            _key, writer = await self._select(catalog_id, ensure=False)
        except RuntimeError:
            return True  # No writer available -> nothing to invalidate; idempotent success.
        ok = await writer.delete_tile(catalog_id, collection_id, tms_id, z, x, y, format)
        try:
            cache_invalidate(
                self.check_tile_exists, self, catalog_id, collection_id, tms_id, z, x, y, format,
            )
        except Exception as exc:
            logger.warning("Cache invalidation failed for tile %s: %s", format, exc)
        return ok

    async def delete_tile_variants(
        self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int, formats: Sequence[str],
    ) -> bool:
        fmt_list = list(formats) if formats else []
        if not fmt_list:
            return True
        try:
            _key, writer = await self._select(catalog_id, ensure=False)
        except RuntimeError:
            return True
        ok = await writer.delete_tile_variants(catalog_id, collection_id, tms_id, z, x, y, fmt_list)
        for fmt in fmt_list:
            try:
                cache_invalidate(
                    self.check_tile_exists, self, catalog_id, collection_id, tms_id, z, x, y, fmt,
                )
            except Exception as exc:
                logger.warning("Cache invalidation failed for tile variant %s: %s", fmt, exc)
        return ok

    async def drop_storage(self, catalog_id: str) -> None:
        try:
            _key, writer = await self._select(catalog_id, ensure=False)
        except RuntimeError:
            return
        await writer.drop_storage(catalog_id)
        cache_clear(self.check_tile_exists)


class StorageBackedTileArchive(TileArchiveStorageProtocol):
    """PMTiles archive storage backed by any StorageProtocol provider.

    Moved here verbatim from ``modules/gcp/tiles_storage`` — it never used
    anything GCS-specific and was misfiled alongside the GCS-only per-tile
    cache. Unaffected by the tile-writer selection rework above: archive
    storage stays a simple single-``StorageProtocol`` consumer.
    """

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

    async def save_archive(self, catalog_id: str, collection_id: str, tms_id: str, data_file: BinaryIO) -> str:
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

    async def get_tile_from_archive(
        self, catalog_id: str, collection_id: str, tms_id: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
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
