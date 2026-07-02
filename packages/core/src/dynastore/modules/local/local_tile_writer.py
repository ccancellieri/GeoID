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

"""Local-disk tile-cache writer entry for ``TilesCachingConfig.writers``.

Mirrors ``dynastore.modules.gcp.tiles_storage.GcsTileWriterConfig``: resolves
WHERE (root path) before handing byte-moving off to the shared
``tile_blob_storage.StorageTileWriter``. Registers itself via
``tiles_writers.register_tile_writer_factory`` at import time — core never
imports this module directly.

Importing this module pulls in no cloud SDK at all; it is safe on any image.
It is NOT imported by default — an on-prem deployment's own module wiring
imports it (alongside registering ``LocalStorageOps`` as the live
``StorageProtocol`` provider) to opt in.
"""

from __future__ import annotations

from typing import Optional

from dynastore.modules.tiles.tile_blob_storage import StorageTileWriter, get_storage_for_scheme
from dynastore.modules.tiles.tiles_config import TilesCachingConfig
from dynastore.modules.tiles.tiles_module import TileStorageProtocol
from dynastore.modules.tiles.tiles_writers import TileWriterConfig, register_tile_writer_factory


class LocalTileWriterConfig(TileWriterConfig):
    """Local-disk-backed tile writer entry for ``TilesCachingConfig.writers``.

    ``root=None`` (default) targets the catalog's own managed root directory
    (provisioned on write via ``StorageProtocol.ensure_storage_for_catalog``,
    which for ``LocalStorageOps`` returns a ``file://`` URI directly); an
    explicit ``root`` targets an operator-supplied ``file://`` directory.
    """

    root: Optional[str] = None


async def _local_writer_factory(
    config: LocalTileWriterConfig, cfg: TilesCachingConfig, catalog_id: str, ensure: bool,
) -> Optional[TileStorageProtocol]:
    storage = get_storage_for_scheme("file")
    if storage is None:
        return None  # No local-disk StorageProtocol provider registered.

    if config.root:
        prefix = f"{cfg.key_prefix}/{catalog_id}"
        return StorageTileWriter(storage, config.root.rstrip("/"), prefix)

    # LocalStorageOps.ensure_storage_for_catalog/get_storage_identifier
    # already return a full 'file://...' URI (unlike GCS, which returns a
    # bare bucket name) — usable as base_uri directly, no scheme composition.
    base_uri = await (
        storage.ensure_storage_for_catalog(catalog_id) if ensure else storage.get_storage_identifier(catalog_id)
    )
    if not base_uri:
        return None
    return StorageTileWriter(storage, base_uri, cfg.key_prefix)


register_tile_writer_factory(LocalTileWriterConfig, _local_writer_factory)
