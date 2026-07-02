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

"""Local-disk ``StorageProtocol`` implementation, addressed by ``file://``
URIs.

On-prem/no-cloud-credentials reuse of the object-storage surface (tile
cache, PMTiles archives, anything else that talks to ``StorageProtocol``)
without any cloud dependency. Each "catalog" gets its own subdirectory under
``root``, named after the catalog id — the local analogue of a per-catalog
managed bucket.

Not wired into any module's lifespan by this change — it is a standalone,
directly-instantiable implementation. A future on-prem deployment registers
it (and the paired ``LocalTileWriterConfig``/factory in
``local_tile_writer.py``) from its own module lifespan.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any, ClassVar, FrozenSet, List, Optional

from dynastore.modules.concurrency import run_in_thread


def _path_from_uri(uri: str) -> Path:
    if not uri.startswith("file://"):
        raise ValueError(f"Invalid local storage path: {uri!r} (expected 'file://' scheme)")
    return Path(uri[len("file://"):])


class LocalStorageOps:
    """Filesystem-backed ``StorageProtocol`` implementation.

    Structurally satisfies ``StorageProtocol`` (a ``@runtime_checkable``
    ``Protocol`` — no explicit base class needed for ``isinstance``/discovery
    checks to pass).
    """

    supported_schemes: ClassVar[FrozenSet[str]] = frozenset({"file"})

    def __init__(self, root: Optional[str] = None) -> None:
        self._root = Path(root or os.environ.get("LOCAL_STORAGE_ROOT", "/tmp/dynastore/storage"))

    def _catalog_root(self, catalog_id: str) -> Path:
        return self._root / catalog_id

    async def get_storage_identifier(self, catalog_id: str) -> Optional[str]:
        path = self._catalog_root(catalog_id)
        return path.as_uri() if await run_in_thread(path.exists) else None

    async def ensure_storage_for_catalog(
        self, catalog_id: str, conn: Optional[Any] = None, raise_on_failure: bool = False
    ) -> Optional[str]:
        path = self._catalog_root(catalog_id)
        try:
            await run_in_thread(path.mkdir, parents=True, exist_ok=True)
        except OSError:
            if raise_on_failure:
                raise
            return None
        return path.as_uri()

    async def get_catalog_storage_path(self, catalog_id: str) -> Optional[str]:
        return await self.ensure_storage_for_catalog(catalog_id)

    async def get_collection_storage_path(self, catalog_id: str, collection_id: str) -> Optional[str]:
        base = await self.ensure_storage_for_catalog(catalog_id)
        return f"{base}/collections/{collection_id}" if base else None

    async def drop_storage(
        self,
        catalog_id: str,
        conn: Optional[Any] = None,
        physical_schema: Optional[str] = None,
        bucket_name: Optional[str] = None,
    ) -> bool:
        target_uri = bucket_name or self._catalog_root(physical_schema or catalog_id).as_uri()
        try:
            path = _path_from_uri(target_uri)
        except ValueError:
            return True
        await run_in_thread(shutil.rmtree, path, ignore_errors=True)
        return True

    async def wait_for_storage_ready(
        self, storage_id: str, timeout_seconds: int = 30, interval_seconds: float = 1.0
    ) -> bool:
        # Local mkdir is synchronous/immediate — nothing to poll for.
        try:
            return await run_in_thread(_path_from_uri(storage_id).exists)
        except ValueError:
            return False

    async def prepare_upload_target(self, catalog_id: str, collection_id: Optional[str] = None) -> None:
        await self.ensure_storage_for_catalog(catalog_id)

    async def upload_file(self, source_path: str, target_path: str, content_type: Optional[str] = None) -> str:
        dest = _path_from_uri(target_path)

        def _copy() -> None:
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source_path, dest)

        await run_in_thread(_copy)
        return target_path

    async def upload_file_content(
        self, target_path: str, content: bytes, content_type: Optional[str] = None
    ) -> str:
        dest = _path_from_uri(target_path)

        def _write() -> None:
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(content)

        await run_in_thread(_write)
        return target_path

    async def download_file(self, source_path: str, target_path: str) -> None:
        await run_in_thread(shutil.copyfile, _path_from_uri(source_path), target_path)

    async def download_file_content(self, path: str) -> Optional[bytes]:
        src = _path_from_uri(path)

        def _read() -> Optional[bytes]:
            if not src.exists():
                return None
            return src.read_bytes()

        return await run_in_thread(_read)

    async def file_exists(self, path: str) -> bool:
        try:
            return await run_in_thread(_path_from_uri(path).exists)
        except ValueError:
            return False

    async def delete_file(self, path: str) -> None:
        try:
            target = _path_from_uri(path)
        except ValueError:
            return
        try:
            await run_in_thread(target.unlink)
        except FileNotFoundError:
            pass

    async def apply_storage_config(self, catalog_id: str, config: Any) -> None:
        # No bucket-level settings (CORS, lifecycle) apply to a local filesystem.
        return None

    async def download_bytes_range(self, path: str, offset: int, length: int) -> bytes:
        def _read_range() -> bytes:
            with open(_path_from_uri(path), "rb") as f:
                f.seek(offset)
                return f.read(length)

        return await run_in_thread(_read_range)

    async def list_prefix(self, base_uri: str, prefix: str) -> List[str]:
        target_dir = _path_from_uri(base_uri) / prefix

        def _scan() -> List[str]:
            if not target_dir.exists():
                return []
            return [p.as_uri() for p in target_dir.rglob("*") if p.is_file()]

        return await run_in_thread(_scan)
