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

"""Tests for modules/volumes/writers/glb.py."""

from __future__ import annotations

import json
import struct

import pytest

from dynastore.modules.volumes.mesh_builder import MeshBuffers, empty_mesh
from dynastore.modules.volumes.writers.glb import (
    _CHUNK_BIN,
    _CHUNK_JSON,
    _GLB_MAGIC,
    _empty_glb,
    _pad4,
    pack_glb,
)


# ---------------------------------------------------------------------------
# _pad4
# ---------------------------------------------------------------------------


def test_pad4_already_aligned():
    data = b"abcd"
    assert _pad4(data) == data


def test_pad4_pads_with_spaces_by_default():
    data = b"ab"
    result = _pad4(data)
    assert len(result) == 4
    assert result[2:] == b"  "


def test_pad4_pads_with_zero_for_binary():
    data = b"abc"
    result = _pad4(data, pad_byte=0x00)
    assert len(result) == 4
    assert result[3] == 0x00


# ---------------------------------------------------------------------------
# _empty_glb
# ---------------------------------------------------------------------------


def _parse_glb_header(data: bytes):
    magic, version, length = struct.unpack_from("<III", data, 0)
    return magic, version, length


def test_empty_glb_valid_header():
    data = _empty_glb()
    magic, version, length = _parse_glb_header(data)
    assert magic == _GLB_MAGIC
    assert version == 2
    assert length == len(data)


def test_empty_glb_has_json_chunk():
    data = _empty_glb()
    chunk_len, chunk_type = struct.unpack_from("<II", data, 12)
    assert chunk_type == _CHUNK_JSON
    json_bytes = data[20:20 + chunk_len]
    doc = json.loads(json_bytes.decode("utf-8").strip())
    assert doc["asset"]["version"] == "2.0"


# ---------------------------------------------------------------------------
# pack_glb with no geometry
# ---------------------------------------------------------------------------


def test_pack_glb_empty_mesh_returns_valid_glb():
    buf = empty_mesh()
    data = pack_glb(buf)
    magic, version, length = _parse_glb_header(data)
    assert magic == _GLB_MAGIC
    assert version == 2
    assert length == len(data)


# ---------------------------------------------------------------------------
# pack_glb with a minimal triangle
# ---------------------------------------------------------------------------


def _make_triangle_mesh() -> MeshBuffers:
    import struct as s
    verts = [(0.0, 0.0, 0.0), (1.0, 0.0, 0.0), (0.0, 1.0, 0.0)]
    pos = s.pack("<9f", *[v for xyz in verts for v in xyz])
    nrm = s.pack("<9f", *([0.0, 0.0, 1.0] * 3))  # all facing +Z
    idx = s.pack("<3I", 0, 1, 2)
    return MeshBuffers(
        positions=pos,
        indices=idx,
        vertex_count=3,
        index_count=3,
        min_pos=(0.0, 0.0, 0.0),
        max_pos=(1.0, 1.0, 0.0),
        normals=nrm,
    )


def _glb_json(data: bytes) -> dict:
    json_chunk_len, _ = struct.unpack_from("<II", data, 12)
    json_bytes = data[20:20 + json_chunk_len]
    return json.loads(json_bytes.decode("utf-8").strip())


def test_pack_glb_triangle_header_length_matches():
    mesh = _make_triangle_mesh()
    data = pack_glb(mesh)
    _, _, declared_length = _parse_glb_header(data)
    assert declared_length == len(data)


def test_pack_glb_triangle_has_json_and_bin_chunks():
    mesh = _make_triangle_mesh()
    data = pack_glb(mesh)

    # JSON chunk
    json_chunk_len, json_type = struct.unpack_from("<II", data, 12)
    assert json_type == _CHUNK_JSON
    json_start = 20
    json_bytes = data[json_start:json_start + json_chunk_len]
    doc = json.loads(json_bytes.decode("utf-8").strip())
    assert "accessors" in doc
    assert doc["accessors"][0]["count"] == 3  # 3 vertices

    # BIN chunk immediately after JSON chunk
    bin_offset = 12 + 8 + json_chunk_len
    bin_chunk_len, bin_type = struct.unpack_from("<II", data, bin_offset)
    assert bin_type == _CHUNK_BIN
    assert bin_chunk_len >= 3 * 3 * 4  # at least position data


def test_pack_glb_gltf_up_axis_z():
    mesh = _make_triangle_mesh()
    data = pack_glb(mesh)
    json_chunk_len, _ = struct.unpack_from("<II", data, 12)
    json_bytes = data[20:20 + json_chunk_len]
    doc = json.loads(json_bytes.decode("utf-8").strip())
    assert doc["asset"].get("extras", {}).get("gltfUpAxis") == "Z"


def test_pack_glb_bufferviews_non_overlapping():
    mesh = _make_triangle_mesh()
    data = pack_glb(mesh)
    json_chunk_len, _ = struct.unpack_from("<II", data, 12)
    json_bytes = data[20:20 + json_chunk_len]
    doc = json.loads(json_bytes.decode("utf-8").strip())
    bvs = doc["bufferViews"]
    assert bvs[0]["byteOffset"] == 0
    assert bvs[1]["byteOffset"] == bvs[0]["byteLength"]


def test_pack_glb_triangle_has_normal_attribute():
    """The primitive must expose a NORMAL accessor so the renderer can light
    the mesh — otherwise the whole tile renders as one flat silhouette."""
    mesh = _make_triangle_mesh()
    doc = _glb_json(pack_glb(mesh))
    prim = doc["meshes"][0]["primitives"][0]
    assert "NORMAL" in prim["attributes"]
    normal_acc = doc["accessors"][prim["attributes"]["NORMAL"]]
    assert normal_acc["type"] == "VEC3"
    assert normal_acc["count"] == 3


def test_pack_glb_triangle_has_lit_material():
    """A double-sided PBR material must be attached so the buildings render as
    solid, shaded volumes rather than the default unlit gltf appearance."""
    mesh = _make_triangle_mesh()
    doc = _glb_json(pack_glb(mesh))
    prim = doc["meshes"][0]["primitives"][0]
    assert prim.get("material") == 0
    mats = doc.get("materials")
    assert mats and len(mats) == 1
    mat = mats[0]
    assert mat.get("doubleSided") is True
    assert "pbrMetallicRoughness" in mat
    assert len(mat["pbrMetallicRoughness"]["baseColorFactor"]) == 4


def test_pack_glb_three_bufferviews_non_overlapping():
    """positions, normals, indices each get their own contiguous bufferView."""
    mesh = _make_triangle_mesh()
    doc = _glb_json(pack_glb(mesh))
    bvs = doc["bufferViews"]
    assert len(bvs) == 3
    assert bvs[0]["byteOffset"] == 0
    assert bvs[1]["byteOffset"] == bvs[0]["byteLength"]
    assert bvs[2]["byteOffset"] == bvs[1]["byteOffset"] + bvs[1]["byteLength"]


def test_pack_glb_buffer_byte_length_is_logical_not_padded():
    """buffers[0].byteLength must be the logical size (pos+idx), not the
    padded BIN chunk length — per glTF 2.0 spec §5.1.2."""
    mesh = _make_triangle_mesh()
    data = pack_glb(mesh)
    json_chunk_len, _ = struct.unpack_from("<II", data, 12)
    json_bytes = data[20:20 + json_chunk_len]
    doc = json.loads(json_bytes.decode("utf-8").strip())
    bvs = doc["bufferViews"]
    expected_logical = sum(bv["byteLength"] for bv in bvs)
    assert doc["buffers"][0]["byteLength"] == expected_logical


def test_pack_glb_untinted_material_is_grey_no_color_attr():
    """Without vertex colours the primitive has no COLOR_0 and the material base
    colour stays the neutral warm-grey applied uniformly."""
    mesh = _make_triangle_mesh()
    doc = _glb_json(pack_glb(mesh))
    prim = doc["meshes"][0]["primitives"][0]
    assert "COLOR_0" not in prim["attributes"]
    base = doc["materials"][0]["pbrMetallicRoughness"]["baseColorFactor"]
    assert base == [0.82, 0.80, 0.76, 1.0]


# ---------------------------------------------------------------------------
# pack_glb with per-vertex colours (COLOR_0)
# ---------------------------------------------------------------------------


def _make_colored_triangle_mesh() -> MeshBuffers:
    import struct as s
    verts = [(0.0, 0.0, 0.0), (1.0, 0.0, 0.0), (0.0, 1.0, 0.0)]
    pos = s.pack("<9f", *[v for xyz in verts for v in xyz])
    nrm = s.pack("<9f", *([0.0, 0.0, 1.0] * 3))
    col = s.pack("<12B", *([200, 50, 10, 255] * 3))  # RGBA uint8 per vertex
    idx = s.pack("<3I", 0, 1, 2)
    return MeshBuffers(
        positions=pos,
        indices=idx,
        vertex_count=3,
        index_count=3,
        min_pos=(0.0, 0.0, 0.0),
        max_pos=(1.0, 1.0, 0.0),
        normals=nrm,
        colors=col,
    )


def test_pack_glb_color0_accessor_is_normalized_ubyte_vec4():
    mesh = _make_colored_triangle_mesh()
    doc = _glb_json(pack_glb(mesh))
    prim = doc["meshes"][0]["primitives"][0]
    assert "COLOR_0" in prim["attributes"]
    acc = doc["accessors"][prim["attributes"]["COLOR_0"]]
    assert acc["type"] == "VEC4"
    assert acc["componentType"] == 5121  # UNSIGNED_BYTE
    assert acc["normalized"] is True
    assert acc["count"] == 3


def test_pack_glb_tinted_material_base_color_is_white():
    """With vertex colours the base colour factor must be white so the COLOR_0
    ramp (base * vertex) shows true rather than being darkened by the grey."""
    mesh = _make_colored_triangle_mesh()
    doc = _glb_json(pack_glb(mesh))
    base = doc["materials"][0]["pbrMetallicRoughness"]["baseColorFactor"]
    assert base == [1.0, 1.0, 1.0, 1.0]


def test_pack_glb_four_bufferviews_when_colored_non_overlapping():
    """positions, normals, colors, indices each get a contiguous bufferView."""
    mesh = _make_colored_triangle_mesh()
    doc = _glb_json(pack_glb(mesh))
    bvs = doc["bufferViews"]
    assert len(bvs) == 4
    offset = 0
    for bv in bvs:
        assert bv["byteOffset"] == offset
        offset += bv["byteLength"]
    # buffer total still equals the logical sum of all four views.
    assert doc["buffers"][0]["byteLength"] == offset


def test_pack_glb_color_indices_accessor_points_at_index_view():
    """The index accessor must follow the colour accessor, not collide with it."""
    mesh = _make_colored_triangle_mesh()
    doc = _glb_json(pack_glb(mesh))
    prim = doc["meshes"][0]["primitives"][0]
    idx_acc = doc["accessors"][prim["indices"]]
    assert idx_acc["componentType"] == 5125  # UNSIGNED_INT
    assert idx_acc["type"] == "SCALAR"
    assert idx_acc["count"] == 3
