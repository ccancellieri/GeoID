"""Streaming tileset.json writer.

tileset.json is small (metadata, not tile bytes) so we emit it as a
single ``bytes`` chunk — but expose the iterator shape so the service
layer can wrap it in FastAPI's ``StreamingResponse`` uniformly with
the b3dm/gltf writers Phase 5b will add.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterator


def write_tileset_json(
    tileset: Dict[str, Any],
    *,
    indent: int | None = None,
    strip_feature_ids: bool = True,
) -> Iterator[bytes]:
    """Emit a tileset.json-shaped dict as bytes.

    ``strip_feature_ids`` removes the internal ``_feature_ids`` key that
    the builder uses to pass feature membership to Phase 5b's b3dm
    writer — it is not part of the Cesium 3D Tiles spec and must not
    leak to clients.
    """
    emittable = _strip(tileset) if strip_feature_ids else tileset
    yield json.dumps(emittable, indent=indent).encode("utf-8")


def _strip(node: Any) -> Any:
    if isinstance(node, dict):
        return {
            k: _strip(v)
            for k, v in node.items()
            if k != "_feature_ids"
        }
    if isinstance(node, list):
        return [_strip(x) for x in node]
    return node
