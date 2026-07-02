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

# Copyright 2025 FAO
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""#1241 — StorageBackedTileArchive serves a single tile via range reads over
the object-storage PMTiles archive, without the undeclared ``apmtiles``
dependency.

The storage layer is replaced with an in-memory ``download_bytes_range`` over a
real archive built by the in-house writer; the traversal (header -> directory
-> tile) is shared with the PG reader (``read_pmtiles_tile``) and cross-checked
against the canonical pmtiles reader.
"""
from __future__ import annotations

import io
from typing import List, Optional, Tuple

from dynastore.modules.tiles.tile_blob_storage import StorageBackedTileArchive
from dynastore.modules.tiles.tiles_module import read_pmtiles_tile
from dynastore.modules.tiles.writers.pmtiles_writer import write_pmtiles

TILES: List[Tuple[int, int, int, bytes]] = [
    (0, 0, 0, b"tile-000"),
    (1, 0, 0, b"tile-100"),
    (1, 1, 0, b"tile-110"),
    (1, 0, 1, b"tile-101"),
    (1, 1, 1, b"tile-111"),
]


def _build_archive() -> bytes:
    buf = io.BytesIO()
    write_pmtiles(TILES, buf, min_zoom=0, max_zoom=1)
    return buf.getvalue()


class _FakeStorage:
    """Minimal StorageProtocol stand-in: a range-readable in-memory archive.

    ``get_storage_identifier`` returns ``None`` when the archive is absent so
    ``_archive_path`` short-circuits to ``None`` (no spurious read). Records
    every ``(offset, length)`` so tests can assert the read is partial.
    """

    def __init__(self, archive: Optional[bytes]) -> None:
        self._archive = archive
        self.reads: List[Tuple[int, int]] = []

    async def get_storage_identifier(self, catalog_id: str) -> Optional[str]:
        return "bucket-x" if self._archive is not None else None

    async def download_bytes_range(self, path: str, offset: int, length: int) -> bytes:
        assert self._archive is not None
        self.reads.append((offset, length))
        # Mirror GCS ``download_as_bytes(start, end)`` (inclusive end) semantics.
        return self._archive[offset : offset + length]


class _StubArchive(StorageBackedTileArchive):
    """StorageBackedTileArchive with the storage provider swapped for a fake."""

    def __init__(self, storage: _FakeStorage) -> None:  # noqa: D107
        self._storage = storage

    def _get_storage(self):  # type: ignore[override]
        return self._storage


# ---- shared traversal helper (read_pmtiles_tile) ---------------------------


async def test_helper_returns_each_stored_tile() -> None:
    raw = _build_archive()
    reads: List[Tuple[int, int]] = []

    async def _range_read(offset: int, length: int) -> bytes:
        reads.append((offset, length))
        return raw[offset : offset + length]

    for z, x, y, data in TILES:
        assert await read_pmtiles_tile(_range_read, z, x, y) == data


async def test_helper_missing_tile_returns_none() -> None:
    raw = _build_archive()

    async def _range_read(offset: int, length: int) -> bytes:
        return raw[offset : offset + length]

    assert await read_pmtiles_tile(_range_read, 2, 0, 0) is None


async def test_helper_absent_archive_returns_none() -> None:
    async def _range_read(offset: int, length: int) -> Optional[bytes]:
        return None

    assert await read_pmtiles_tile(_range_read, 0, 0, 0) is None


# ---- StorageBackedTileArchive.get_tile_from_archive ------------------------


async def test_returns_each_stored_tile() -> None:
    archive = _StubArchive(_FakeStorage(_build_archive()))
    for z, x, y, data in TILES:
        got = await archive.get_tile_from_archive("cat", "col", "tms", z, x, y)
        assert got == data, f"tile {z}/{x}/{y} mismatch"


async def test_missing_tile_returns_none() -> None:
    archive = _StubArchive(_FakeStorage(_build_archive()))
    assert await archive.get_tile_from_archive("cat", "col", "tms", 2, 0, 0) is None


async def test_absent_archive_returns_none() -> None:
    archive = _StubArchive(_FakeStorage(None))
    assert await archive.get_tile_from_archive("cat", "col", "tms", 0, 0, 0) is None


async def test_read_is_partial_not_whole_archive() -> None:
    raw = _build_archive()
    storage = _FakeStorage(raw)
    archive = _StubArchive(storage)
    await archive.get_tile_from_archive("cat", "col", "tms", 0, 0, 0)

    # The first read is the fixed 127-byte PMTiles header, never the whole file.
    assert storage.reads[0] == (0, 127)
    assert all(length < len(raw) for _, length in storage.reads)
    assert sum(length for _, length in storage.reads) < len(raw)


async def test_matches_canonical_reader() -> None:
    raw = _build_archive()
    from pmtiles.reader import MemorySource, Reader

    canonical = Reader(MemorySource(raw))
    archive = _StubArchive(_FakeStorage(raw))
    for z, x, y, _ in TILES:
        assert await archive.get_tile_from_archive("cat", "col", "tms", z, x, y) == (
            canonical.get(z, x, y)
        )
