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
"""#1206 — PGTileArchive serves a single tile via byte-range reads over the
stored PMTiles BYTEA instead of loading the whole archive into memory.

The DB layer is replaced with an in-memory ``substring`` over a real archive
built by the in-house writer; the traversal (header -> directory -> tile) is
exercised end to end and cross-checked against the canonical pmtiles reader.
"""
from __future__ import annotations

import io
from typing import List, Optional, Tuple

from dynastore.modules.tiles.tiles_module import PGTileArchive
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


class _RangeRecordingArchive(PGTileArchive):
    """PGTileArchive with the DB swapped for an in-memory range source.

    Bypasses ``__init__`` (which needs an engine + CatalogsProtocol) and records
    every ``(offset, length)`` requested so tests can assert the read is partial.
    """

    def __init__(self, archive: Optional[bytes]) -> None:  # noqa: D107
        self._archive = archive
        self.reads: List[Tuple[int, int]] = []

    async def _get_schema(self, catalog_id: str) -> str:
        return "s_test"

    async def _read_archive_range(
        self, schema, collection_id, tms_id, offset, length
    ) -> Optional[bytes]:
        if self._archive is None:
            return None  # archive row absent
        self.reads.append((offset, length))
        # Mirror SQL ``substring`` semantics: a clamped slice from the value.
        return self._archive[offset : offset + length]


async def test_returns_each_stored_tile() -> None:
    archive = _RangeRecordingArchive(_build_archive())
    for z, x, y, data in TILES:
        got = await archive.get_tile_from_archive("cat", "col", "tms", z, x, y)
        assert got == data, f"tile {z}/{x}/{y} mismatch"


async def test_missing_tile_returns_none() -> None:
    archive = _RangeRecordingArchive(_build_archive())
    assert await archive.get_tile_from_archive("cat", "col", "tms", 2, 0, 0) is None


async def test_absent_archive_returns_none() -> None:
    archive = _RangeRecordingArchive(None)
    assert await archive.get_tile_from_archive("cat", "col", "tms", 0, 0, 0) is None


async def test_read_is_partial_not_whole_archive() -> None:
    raw = _build_archive()
    archive = _RangeRecordingArchive(raw)
    await archive.get_tile_from_archive("cat", "col", "tms", 0, 0, 0)

    # The first read is the fixed 127-byte PMTiles header, never the whole file.
    assert archive.reads[0] == (0, 127)
    # No single range spans the entire archive, and the total bytes pulled stay
    # well under the archive size — the point of the range-read contract.
    assert all(length < len(raw) for _, length in archive.reads)
    assert sum(length for _, length in archive.reads) < len(raw)


async def test_matches_canonical_reader() -> None:
    raw = _build_archive()
    from pmtiles.reader import MemorySource, Reader

    canonical = Reader(MemorySource(raw))
    archive = _RangeRecordingArchive(raw)
    for z, x, y, _ in TILES:
        assert await archive.get_tile_from_archive("cat", "col", "tms", z, x, y) == (
            canonical.get(z, x, y)
        )
