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
from typing import Any, Dict

from dynastore.modules.volumes.mesh_builder import MeshBuffers


_GLB_MAGIC = 0x46546C67   # 'glTF'
_CHUNK_JSON = 0x4E4F534A   # 'JSON'
_CHUNK_BIN  = 0x004E4942   # 'BIN\0'

# glTF component types
_FLOAT  = 5126
_UINT32 = 5125


def _building_material() -> Dict[str, Any]:
    """A simple double-sided PBR material for extruded building volumes.

    A neutral warm-grey base colour, fully dielectric (metallic 0) and mostly
    rough, so directional scene lighting shades roofs and walls differently and
    adjacent buildings read as distinct solids. ``doubleSided`` guards against
    any inward-facing triangles from the centroid-fan caps so no surface drops
    out. Without a material the renderer falls back to its default unlit look,
    which makes the whole tile appear as one flat mass.
    """
    return {
        "name": "building",
        "doubleSided": True,
        "pbrMetallicRoughness": {
            "baseColorFactor": [0.82, 0.80, 0.76, 1.0],
            "metallicFactor": 0.0,
            "roughnessFactor": 0.85,
        },
    }


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
    normals   = mesh.normals     # float32 * 3 per vertex (may be empty)
    indices   = mesh.indices     # uint32 per index

    has_normals = bool(normals)

    pos_byte_length = len(positions)
    nrm_byte_length = len(normals)
    idx_byte_length = len(indices)

    # Buffer layout: [positions][normals?][indices]
    parts = [positions, normals, indices] if has_normals else [positions, indices]
    bin_data = _pad4(b"".join(parts), pad_byte=0x00)

    # Accessor indices and bufferViews depend on whether normals are present so
    # the index accessor always points at the index bufferView.
    accessors: list = [
        {
            "bufferView": 0,
            "byteOffset": 0,
            "componentType": _FLOAT,
            "count": mesh.vertex_count,
            "type": "VEC3",
            "min": list(mesh.min_pos),
            "max": list(mesh.max_pos),
        },
    ]
    buffer_views: list = [
        {
            "buffer": 0,
            "byteOffset": 0,
            "byteLength": pos_byte_length,
            "target": 34962,  # ARRAY_BUFFER
        },
    ]
    attributes: Dict[str, int] = {"POSITION": 0}

    if has_normals:
        attributes["NORMAL"] = 1
        accessors.append({
            "bufferView": 1,
            "byteOffset": 0,
            "componentType": _FLOAT,
            "count": mesh.vertex_count,
            "type": "VEC3",
        })
        buffer_views.append({
            "buffer": 0,
            "byteOffset": pos_byte_length,
            "byteLength": nrm_byte_length,
            "target": 34962,  # ARRAY_BUFFER
        })
        idx_accessor = 2
        idx_bufferview = 2
        idx_offset = pos_byte_length + nrm_byte_length
    else:
        idx_accessor = 1
        idx_bufferview = 1
        idx_offset = pos_byte_length

    accessors.append({
        "bufferView": idx_bufferview,
        "byteOffset": 0,
        "componentType": _UINT32,
        "count": mesh.index_count,
        "type": "SCALAR",
    })
    buffer_views.append({
        "buffer": 0,
        "byteOffset": idx_offset,
        "byteLength": idx_byte_length,
        "target": 34963,  # ELEMENT_ARRAY_BUFFER
    })

    primitive: Dict[str, Any] = {
        "attributes": attributes,
        "indices": idx_accessor,
        "mode": 4,  # TRIANGLES
        "material": 0,
    }

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
            "primitives": [primitive],
        }],
        "materials": [_building_material()],
        "accessors": accessors,
        "bufferViews": buffer_views,
        "buffers": [{"byteLength": sum(bv["byteLength"] for bv in buffer_views)}],
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
