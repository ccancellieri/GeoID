"""Pack a triangle mesh into a GLB (glTF 2.0 Binary) byte stream.

GLB layout (spec §12):
  [12-byte header]   magic(4) version(4) length(4)
  [JSON chunk]       chunkLength(4) chunkType JSON(4) JSON-data (padded to 4)
  [BIN  chunk]       chunkLength(4) chunkType BIN\\0(4) binary-data (padded to 4)

The glTF JSON is minimal: one scene → one node → one mesh → one primitive
with POSITION accessor + optional index accessor (TRIANGLES mode).

``gltfUpAxis = "Z"`` is declared in the asset so Cesium interprets the
geometry as Z-up (matching the PostGIS / geographic convention).
"""

from __future__ import annotations

import json
import struct
from typing import Any, Dict, Optional

from dynastore.modules.volumes.mesh_builder import MeshBuffers


_GLB_MAGIC = 0x46546C67   # 'glTF'
_CHUNK_JSON = 0x4E4F534A   # 'JSON'
_CHUNK_BIN  = 0x004E4942   # 'BIN\0'

# glTF component types
_FLOAT  = 5126
_UINT32 = 5125


def _pad4(data: bytes, pad_byte: int = 0x20) -> bytes:
    """Pad *data* to a 4-byte boundary using *pad_byte*."""
    rem = len(data) % 4
    if rem:
        data += bytes([pad_byte] * (4 - rem))
    return data


def _write_chunk(data: bytes, chunk_type: int) -> bytes:
    return struct.pack("<II", len(data), chunk_type) + data


def pack_glb(mesh: MeshBuffers, *, title: str = "tile") -> bytes:
    """Encode *mesh* as a GLB byte string.

    Returns an empty GLB with a single empty buffer when *mesh* has no
    geometry, so the response is always a valid binary glTF.
    """
    if mesh.vertex_count == 0:
        return _empty_glb()

    positions = mesh.positions   # float32 * 3 per vertex
    indices   = mesh.indices     # uint32 per index

    # Buffer layout: [positions][indices]
    bin_data = _pad4(b"".join([positions, indices]), pad_byte=0x00)

    pos_byte_length = len(positions)
    idx_byte_length = len(indices)

    gltf: Dict[str, Any] = {
        "asset": {
            "version": "2.0",
            "extras": {"gltfUpAxis": "Z"},
        },
        "scene": 0,
        "scenes": [{"nodes": [0]}],
        "nodes": [{"mesh": 0}],
        "meshes": [{
            "name": title,
            "primitives": [{
                "attributes": {"POSITION": 0},
                "indices": 1,
                "mode": 4,  # TRIANGLES
            }],
        }],
        "accessors": [
            {
                "bufferView": 0,
                "byteOffset": 0,
                "componentType": _FLOAT,
                "count": mesh.vertex_count,
                "type": "VEC3",
                "min": list(mesh.min_pos),
                "max": list(mesh.max_pos),
            },
            {
                "bufferView": 1,
                "byteOffset": 0,
                "componentType": _UINT32,
                "count": mesh.index_count,
                "type": "SCALAR",
            },
        ],
        "bufferViews": [
            {
                "buffer": 0,
                "byteOffset": 0,
                "byteLength": pos_byte_length,
                "target": 34962,  # ARRAY_BUFFER
            },
            {
                "buffer": 0,
                "byteOffset": pos_byte_length,
                "byteLength": idx_byte_length,
                "target": 34963,  # ELEMENT_ARRAY_BUFFER
            },
        ],
        "buffers": [{"byteLength": pos_byte_length + idx_byte_length}],
    }

    json_bytes = _pad4(json.dumps(gltf, separators=(",", ":")).encode("utf-8"),
                       pad_byte=0x20)

    json_chunk = _write_chunk(json_bytes, _CHUNK_JSON)
    bin_chunk  = _write_chunk(bin_data,   _CHUNK_BIN)

    total = 12 + len(json_chunk) + len(bin_chunk)
    header = struct.pack("<III", _GLB_MAGIC, 2, total)

    return header + json_chunk + bin_chunk


def _empty_glb() -> bytes:
    """Minimal valid GLB with zero geometry (used when a tile has no features)."""
    gltf: Dict[str, Any] = {
        "asset": {"version": "2.0", "extras": {"gltfUpAxis": "Z"}},
        "scene": 0,
        "scenes": [{"nodes": []}],
    }
    json_bytes = _pad4(json.dumps(gltf, separators=(",", ":")).encode("utf-8"),
                       pad_byte=0x20)
    json_chunk = _write_chunk(json_bytes, _CHUNK_JSON)
    total = 12 + len(json_chunk)
    header = struct.pack("<III", _GLB_MAGIC, 2, total)
    return header + json_chunk
