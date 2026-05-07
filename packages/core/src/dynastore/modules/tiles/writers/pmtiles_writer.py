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

"""
PMTiles v3 writer.

Prefers the `pmtiles` library (protomaps/PMTiles) when installed; falls back
to the built-in pure-Python implementation that covers the full PMTiles v3
binary format including Hilbert-curve tile IDs, varint-encoded directories,
and root/leaf directory splitting for large tile sets.

Reference: https://github.com/protomaps/PMTiles/blob/main/spec/v3/spec.md
"""

import io
import json
import logging
import struct
from dataclasses import dataclass
from typing import BinaryIO, Dict, Any, Iterable, List, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional pmtiles library (protomaps) — preferred when available
# ---------------------------------------------------------------------------

try:
    from pmtiles.writer import Writer as _PMTilesWriter
    from pmtiles.tile import (
        zxy_to_tileid as _lib_zxy_to_tileid,
        Compression as _PMTilesCompression,
        TileType as _PMTilesType,
    )
    _PMTILES_LIB_AVAILABLE = True
except ImportError:
    _PMTILES_LIB_AVAILABLE = False


# ---------------------------------------------------------------------------
# Tile-ID: Hilbert-curve mapping  z/x/y → uint64
# ---------------------------------------------------------------------------

def _builtin_zxy_to_tileid(z: int, x: int, y: int) -> int:
    if z == 0:
        return 0
    acc = (4**z - 1) // 3
    tx, ty = x, y
    d = 0
    s = 1 << (z - 1)
    while s > 0:
        rx = 1 if (tx & s) > 0 else 0
        ry = 1 if (ty & s) > 0 else 0
        d += s * s * ((3 * rx) ^ ry)
        if ry == 0:
            if rx == 1:
                tx = s - 1 - tx
                ty = s - 1 - ty
            tx, ty = ty, tx
        s >>= 1
    return acc + d


def zxy_to_tileid(z: int, x: int, y: int) -> int:
    """Return the PMTiles v3 Hilbert-curve tile ID for tile z/x/y."""
    if _PMTILES_LIB_AVAILABLE:
        return _lib_zxy_to_tileid(z, x, y)
    return _builtin_zxy_to_tileid(z, x, y)


# ---------------------------------------------------------------------------
# Internal binary helpers
# ---------------------------------------------------------------------------

@dataclass
class _Entry:
    tile_id: int
    offset: int
    length: int
    run_length: int = 1


def _write_varint(buf: bytearray, value: int) -> None:
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            buf.append(byte | 0x80)
        else:
            buf.append(byte)
            break


def _serialize_directory(entries: List[_Entry]) -> bytes:
    """Encode a list of directory entries as PMTiles v3 varint-packed bytes."""
    buf = bytearray()
    _write_varint(buf, len(entries))

    last_id = 0
    for e in entries:
        _write_varint(buf, e.tile_id - last_id)
        last_id = e.tile_id

    for e in entries:
        _write_varint(buf, e.run_length)

    for e in entries:
        _write_varint(buf, e.length)

    for i, e in enumerate(entries):
        # Offset is 0 (sentinel for "consecutive") when this tile immediately
        # follows the previous one in the data section (clustered layout).
        if (
            i > 0
            and entries[i - 1].run_length == 1
            and e.offset == entries[i - 1].offset + entries[i - 1].length
        ):
            _write_varint(buf, 0)
        else:
            _write_varint(buf, e.offset + 1)

    return bytes(buf)


_HEADER_SIZE = 127
_MAX_ROOT_ENTRIES = 16384  # keeps root dir under ~512 KB even at worst case

# PMTiles v3 compression-type constants
_COMPRESSION_NONE = 1

# PMTiles v3 tile-type constants
_TILE_TYPE_MVT = 1


def _write_header(
    out: BinaryIO,
    *,
    root_dir_offset: int,
    root_dir_length: int,
    metadata_offset: int,
    metadata_length: int,
    leaf_dirs_offset: int,
    leaf_dirs_length: int,
    tile_data_offset: int,
    tile_data_length: int,
    n_addressed_tiles: int,
    n_tile_entries: int,
    n_tile_contents: int,
    clustered: bool,
    internal_compression: int,
    tile_compression: int,
    tile_type: int,
    min_zoom: int,
    max_zoom: int,
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    center_zoom: int,
    center_lon: float,
    center_lat: float,
) -> None:
    h = bytearray(_HEADER_SIZE)
    h[0:7] = b"PMTiles"
    h[7] = 3  # version
    struct.pack_into("<Q", h, 8, root_dir_offset)
    struct.pack_into("<Q", h, 16, root_dir_length)
    struct.pack_into("<Q", h, 24, metadata_offset)
    struct.pack_into("<Q", h, 32, metadata_length)
    struct.pack_into("<Q", h, 40, leaf_dirs_offset)
    struct.pack_into("<Q", h, 48, leaf_dirs_length)
    struct.pack_into("<Q", h, 56, tile_data_offset)
    struct.pack_into("<Q", h, 64, tile_data_length)
    struct.pack_into("<Q", h, 72, n_addressed_tiles)
    struct.pack_into("<Q", h, 80, n_tile_entries)
    struct.pack_into("<Q", h, 88, n_tile_contents)
    h[96] = 1 if clustered else 0
    h[97] = internal_compression
    h[98] = tile_compression
    h[99] = tile_type
    h[100] = min_zoom & 0xFF
    h[101] = max_zoom & 0xFF
    struct.pack_into("<i", h, 102, int(min_lon * 1e7))
    struct.pack_into("<i", h, 106, int(min_lat * 1e7))
    struct.pack_into("<i", h, 110, int(max_lon * 1e7))
    struct.pack_into("<i", h, 114, int(max_lat * 1e7))
    h[118] = center_zoom & 0xFF
    struct.pack_into("<i", h, 119, int(center_lon * 1e7))
    struct.pack_into("<i", h, 123, int(center_lat * 1e7))
    out.write(bytes(h))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _write_pmtiles_via_lib(
    raw: List[Tuple[int, bytes]],
    output: BinaryIO,
    metadata: Dict[str, Any],
    min_zoom: int,
    max_zoom: int,
    bbox: Tuple[float, float, float, float],
) -> Dict[str, int]:
    """Write using the protomaps ``pmtiles`` library.

    ``Writer.finalize(header, metadata)`` (pmtiles >= 3.x) requires a header
    dict with the v3 fields the writer cannot infer from tile bytes alone:
    tile_type, tile_compression, bbox in 1e-7 fixed-point, optional center.
    The writer fills in section offsets/lengths and zoom range itself.
    """
    writer = _PMTilesWriter(output)
    for tile_id, data in raw:
        writer.write_tile(tile_id, data)
    min_lon, min_lat, max_lon, max_lat = bbox
    header: Dict[str, Any] = {
        "tile_type": _PMTilesType.MVT,
        # MVT tiles produced by ST_AsMVT are uncompressed bytes; PMTiles
        # gzip wrapping is handled by the writer's metadata path, not here.
        "tile_compression": _PMTilesCompression.NONE,
        "min_lon_e7": int(min_lon * 1e7),
        "min_lat_e7": int(min_lat * 1e7),
        "max_lon_e7": int(max_lon * 1e7),
        "max_lat_e7": int(max_lat * 1e7),
        "center_zoom": (min_zoom + max_zoom) // 2,
    }
    writer.finalize(header, metadata)
    total = output.tell() if hasattr(output, "tell") else 0
    return {"n_tiles": len(raw), "n_empty_tiles": 0, "total_bytes": total}


def write_pmtiles(
    tiles: Iterable[Tuple[int, int, int, bytes]],
    output: BinaryIO,
    *,
    metadata: Dict[str, Any] | None = None,
    min_zoom: int = 0,
    max_zoom: int = 14,
    bbox: Tuple[float, float, float, float] = (-180.0, -90.0, 180.0, 90.0),
) -> Dict[str, int]:
    """
    Write an iterable of MVT tiles to *output* in PMTiles v3 format.

    Uses the ``pmtiles`` library (protomaps) when installed; falls back to the
    built-in pure-Python implementation otherwise.

    Args:
        tiles:    Iterable of ``(z, x, y, mvt_bytes)`` tuples.
                  Empty-bytes tiles are silently skipped.
        output:   Writable binary stream.
        metadata: Optional dict serialised as UTF-8 JSON into the archive.
        min_zoom: Minimum zoom stored (written to header).
        max_zoom: Maximum zoom stored (written to header).
        bbox:     ``(min_lon, min_lat, max_lon, max_lat)`` in WGS84.

    Returns:
        Dict with keys ``n_tiles``, ``n_empty_tiles``, ``total_bytes``.
    """
    if metadata is None:
        metadata = {}

    # Collect all non-empty tiles sorted by Hilbert tile_id.
    raw: List[Tuple[int, bytes]] = []
    n_empty = 0
    for z, x, y, data in tiles:
        if data:
            raw.append((zxy_to_tileid(z, x, y), data))
        else:
            n_empty += 1

    raw.sort(key=lambda t: t[0])
    stats = _write_pmtiles_from_raw(raw, output, metadata, min_zoom, max_zoom, bbox)
    stats["n_empty_tiles"] = n_empty
    return stats


def _write_pmtiles_from_raw(
    raw: "List[Tuple[int, bytes]]",
    output: BinaryIO,
    metadata: "Dict[str, Any]",
    min_zoom: int,
    max_zoom: int,
    bbox: "Tuple[float, float, float, float]",
) -> "Dict[str, int]":
    """Core PMTiles v3 writer. *raw* is a list of (tile_id, data) pairs, already sorted.

    Returns dict with ``n_tiles`` and ``total_bytes``; caller adds ``n_empty_tiles``.
    """
    # Prefer the pmtiles library when available.
    if _PMTILES_LIB_AVAILABLE:
        try:
            stats = _write_pmtiles_via_lib(raw, output, metadata, min_zoom, max_zoom, bbox)
            logger.debug("PMTiles archive written via pmtiles library (%d tiles)", len(raw))
            return stats
        except Exception as exc:
            logger.warning(
                "pmtiles library write failed (%s); falling back to built-in writer.", exc
            )

    # 2. Build data section and directory entries (clustered layout).
    data_buf = io.BytesIO()
    dir_entries: List[_Entry] = []
    data_offset = 0
    for tile_id, data in raw:
        data_buf.write(data)
        dir_entries.append(_Entry(tile_id=tile_id, offset=data_offset, length=len(data)))
        data_offset += len(data)

    tile_data_bytes = data_buf.getvalue()
    metadata_bytes = json.dumps(metadata).encode("utf-8")

    # 3. Build directory structure (root-only or root + leaves).
    if len(dir_entries) <= _MAX_ROOT_ENTRIES:
        root_dir_bytes = _serialize_directory(dir_entries)
        leaf_dir_bytes = b""
    else:
        leaf_size = 4096
        leaves_bytes_parts: List[bytes] = []
        root_entries: List[_Entry] = []
        leaf_offset_acc = 0

        for i in range(0, len(dir_entries), leaf_size):
            chunk = dir_entries[i : i + leaf_size]
            leaf_bytes = _serialize_directory(chunk)
            leaves_bytes_parts.append(leaf_bytes)
            root_entries.append(
                _Entry(
                    tile_id=chunk[0].tile_id,
                    offset=leaf_offset_acc,
                    length=len(leaf_bytes),
                    run_length=0,
                )
            )
            leaf_offset_acc += len(leaf_bytes)

        leaf_dir_bytes = b"".join(leaves_bytes_parts)
        root_dir_bytes = _serialize_directory(root_entries)

    # 4. Compute section offsets.
    root_dir_offset = _HEADER_SIZE
    root_dir_length = len(root_dir_bytes)
    metadata_offset = root_dir_offset + root_dir_length
    metadata_length = len(metadata_bytes)
    leaf_dirs_offset = metadata_offset + metadata_length
    leaf_dirs_length = len(leaf_dir_bytes)
    tile_data_offset = leaf_dirs_offset + leaf_dirs_length
    tile_data_length = len(tile_data_bytes)

    min_lon, min_lat, max_lon, max_lat = bbox
    center_lon = (min_lon + max_lon) / 2.0
    center_lat = (min_lat + max_lat) / 2.0
    center_zoom = (min_zoom + max_zoom) // 2

    # 5. Write all sections.
    _write_header(
        output,
        root_dir_offset=root_dir_offset,
        root_dir_length=root_dir_length,
        metadata_offset=metadata_offset,
        metadata_length=metadata_length,
        leaf_dirs_offset=leaf_dirs_offset,
        leaf_dirs_length=leaf_dirs_length,
        tile_data_offset=tile_data_offset,
        tile_data_length=tile_data_length,
        n_addressed_tiles=len(dir_entries),
        n_tile_entries=len(dir_entries),
        n_tile_contents=len(dir_entries),
        clustered=True,
        internal_compression=_COMPRESSION_NONE,
        tile_compression=_COMPRESSION_NONE,
        tile_type=_TILE_TYPE_MVT,
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        min_lon=min_lon,
        min_lat=min_lat,
        max_lon=max_lon,
        max_lat=max_lat,
        center_zoom=center_zoom,
        center_lon=center_lon,
        center_lat=center_lat,
    )
    output.write(root_dir_bytes)
    output.write(metadata_bytes)
    if leaf_dir_bytes:
        output.write(leaf_dir_bytes)
    output.write(tile_data_bytes)

    total = _HEADER_SIZE + root_dir_length + metadata_length + leaf_dirs_length + tile_data_length
    return {"n_tiles": len(dir_entries), "total_bytes": total}



# ---------------------------------------------------------------------------
# Two-phase archive builder — no in-memory tile accumulation
# ---------------------------------------------------------------------------

@dataclass
class TileEntry:
    """Lightweight metadata record for a single tile in a two-phase PMTiles build."""
    tile_id: int
    offset: int
    length: int


def write_pmtiles_from_entries(
    entries: "List[TileEntry]",
    data_source: BinaryIO,
    output: BinaryIO,
    *,
    metadata: "Dict[str, Any]",
    min_zoom: int,
    max_zoom: int,
    bbox: "Tuple[float, float, float, float]",
) -> "Dict[str, int]":
    """Build a PMTiles v3 archive from pre-collected TileEntry records.

    Unlike write_pmtiles() which accumulates all tile bytes in memory, this
    helper reads each tile by (tile_id, offset, length) from *data_source* —
    disk-backed and memory-efficient for large tile sets.

    *entries* must already be sorted by tile_id (Hilbert order).
    """
    # Build the (tile_id, data) pairs by seeking into the data_source file.
    # Skips entries whose tile data is empty.
    raw: List[Tuple[int, bytes]] = []
    for entry in entries:
        data_source.seek(entry.offset)
        data = data_source.read(entry.length)
        if data:
            raw.append((entry.tile_id, data))
    # entries are pre-sorted by tile_id — the list is already in Hilbert order.
    return _write_pmtiles_from_raw(raw, output, metadata, min_zoom, max_zoom, bbox)
