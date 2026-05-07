"""Wrap a GLB payload in a B3DM (Batched 3D Model) envelope.

B3DM spec (3D Tiles 1.0) §6:
  [28-byte header]
    magic(4)  version(4)  byteLength(4)
    featureTableJSONByteLength(4)   featureTableBinaryByteLength(4)
    batchTableJSONByteLength(4)     batchTableBinaryByteLength(4)
  [Feature Table JSON]   padded to 8-byte boundary from header start
  [Feature Table Binary] 0 bytes in our case
  [Batch Table JSON]     optional, padded to 8-byte boundary
  [Batch Table Binary]   0 bytes in our case
  [GLB payload]

``BATCH_LENGTH`` in the feature table is mandatory; it equals the number
of features batched in this tile.
"""

from __future__ import annotations

import json
import struct
from typing import Sequence


_B3DM_MAGIC   = b"b3dm"
_HEADER_SIZE  = 28


def _pad8(data: bytes, pad_byte: int = 0x20) -> bytes:
    """Pad *data* to an 8-byte boundary using *pad_byte*."""
    rem = len(data) % 8
    if rem:
        data += bytes([pad_byte] * (8 - rem))
    return data


def pack_b3dm(
    glb_bytes: bytes,
    *,
    feature_ids: Sequence[str] = (),
) -> bytes:
    """Wrap *glb_bytes* in a B3DM envelope.

    *feature_ids* populates the batch table ``id`` array so Cesium can
    associate picked features with their original IDs. Pass an empty
    sequence to omit the batch table.
    """
    batch_length = len(feature_ids)

    feat_table_json = _pad8(
        json.dumps({"BATCH_LENGTH": batch_length},
                   separators=(",", ":")).encode("utf-8"),
        pad_byte=0x20,
    )

    if feature_ids:
        batch_table_json = _pad8(
            json.dumps({"id": list(feature_ids)},
                       separators=(",", ":")).encode("utf-8"),
            pad_byte=0x20,
        )
    else:
        batch_table_json = b""

    # Header is 28 bytes (≡ 4 mod 8).  feat/batch tables are each padded to
    # multiples of 8, so the GLB would land at offset 4 mod 8 — violating the
    # B3DM spec requirement that the GLB payload starts on an 8-byte boundary.
    # We absorb the 4-byte gap in featureTableBinary (zero-filled padding).
    pre_glb = _HEADER_SIZE + len(feat_table_json) + len(batch_table_json)
    feat_table_binary = bytes((8 - pre_glb % 8) % 8)

    total = pre_glb + len(feat_table_binary) + len(glb_bytes)

    header = struct.pack(
        "<4sIIIIII",
        _B3DM_MAGIC,
        1,                              # version
        total,
        len(feat_table_json),
        len(feat_table_binary),         # alignment padding
        len(batch_table_json),
        0,                              # batchTableBinaryByteLength
    )

    return header + feat_table_json + feat_table_binary + batch_table_json + glb_bytes
