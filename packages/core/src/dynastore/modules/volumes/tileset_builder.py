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

"""Recursive octree-ish partitioner producing a Cesium-3D-Tiles tileset.json dict.

Pure math — no I/O, no glTF. Given a list of FeatureBounds + VolumesConfig,
recursively partition space until each leaf holds <= max_features_per_tile
OR max_tree_depth reached. Leaves don't carry content hrefs at Phase 5a —
that's Phase 5b's job; we emit a ``content: {uri: ...}`` placeholder the
service layer (Phase 5c) will rewrite with real tile URLs.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from dynastore.extensions.volumes.config import VolumesConfig
from dynastore.modules.volumes.bounds import FeatureBounds, merge_bounds
from dynastore.modules.volumes.geo import enu_to_ecef_matrix, lonlat_to_enu


def build_tileset(
    bounds: Sequence[FeatureBounds],
    config: VolumesConfig,
    *,
    content_uri_template: str = "tiles/{tile_id}.b3dm",
) -> Dict[str, Any]:
    """Build a Cesium-3D-Tiles tileset.json dict.

    ``content_uri_template`` is formatted per leaf with ``tile_id`` — a
    breadth-first integer address from the root.
    """
    if not bounds:
        return {
            "asset": {"version": "1.0", "gltfUpAxis": "Z"},
            "geometricError": 0.0,
            "root": {
                "boundingVolume": {"box": _zero_box()},
                "geometricError": 0.0,
                "refine": "REPLACE",
            },
        }

    root_bbox = merge_bounds(bounds)
    # Anchor a local East-North-Up metric frame at the collection centre. All
    # node boxes are emitted in that frame (metres); the root ``transform``
    # places it on the globe. Stored coords are EPSG:4326, so x=lon, y=lat,
    # z=height(m).
    origin = root_bbox.center()  # (lon0, lat0, h0)
    root = _build_subtree(
        list(bounds),
        bbox=root_bbox,
        depth=0,
        config=config,
        path=[0],
        content_uri_template=content_uri_template,
        origin=origin,
    )
    root["transform"] = enu_to_ecef_matrix(origin[0], origin[1], origin[2])
    return {
        # Tile content is authored Z-up in the ENU frame; declaring gltfUpAxis=Z
        # suppresses the renderer's default glTF Y-up -> Z-up rotation, which
        # would otherwise tip every building onto its side.
        "asset": {"version": "1.0", "gltfUpAxis": "Z"},
        "geometricError": config.root_geometric_error,
        "root": root,
    }


def _build_subtree(
    items: List[FeatureBounds],
    *,
    bbox: FeatureBounds,
    depth: int,
    config: VolumesConfig,
    path: List[int],
    content_uri_template: str,
    origin: Tuple[float, float, float],
) -> Dict[str, Any]:
    tile_id = "_".join(str(p) for p in path)
    geom_error = config.root_geometric_error / (config.refinement_ratio ** depth)

    is_leaf = (
        len(items) <= config.max_features_per_tile
        or depth >= config.max_tree_depth
    )

    node: Dict[str, Any] = {
        "boundingVolume": {"box": _bbox_to_box(bbox, origin)},
        "geometricError": geom_error,
        "refine": "REPLACE",
    }
    if is_leaf:
        node["content"] = {"uri": content_uri_template.format(tile_id=tile_id)}
        node["_feature_ids"] = [b.feature_id for b in items]  # Phase 5b consumes this
        return node

    # Split by widest axis at midpoint.
    axis = _widest_axis(bbox)
    pivot = _axis_mid(bbox, axis)
    left, right = _partition(items, axis=axis, pivot=pivot)
    # Guard against degenerate partitions: every item on one side.
    if not left or not right:
        node["content"] = {"uri": content_uri_template.format(tile_id=tile_id)}
        node["_feature_ids"] = [b.feature_id for b in items]
        return node

    node["children"] = [
        _build_subtree(
            left,
            bbox=merge_bounds(left),
            depth=depth + 1,
            config=config,
            path=path + [0],
            content_uri_template=content_uri_template,
            origin=origin,
        ),
        _build_subtree(
            right,
            bbox=merge_bounds(right),
            depth=depth + 1,
            config=config,
            path=path + [1],
            content_uri_template=content_uri_template,
            origin=origin,
        ),
    ]
    return node


def _widest_axis(b: FeatureBounds) -> int:
    spans = (b.max_x - b.min_x, b.max_y - b.min_y, b.max_z - b.min_z)
    return spans.index(max(spans))


def _axis_mid(b: FeatureBounds, axis: int) -> float:
    if axis == 0:
        return (b.min_x + b.max_x) / 2.0
    if axis == 1:
        return (b.min_y + b.max_y) / 2.0
    return (b.min_z + b.max_z) / 2.0


def _partition(items: List[FeatureBounds], *, axis: int, pivot: float):
    def _center(b: FeatureBounds) -> float:
        return b.center()[axis]

    left = [b for b in items if _center(b) < pivot]
    right = [b for b in items if _center(b) >= pivot]
    return left, right


def _bbox_to_box(
    b: FeatureBounds, origin: Tuple[float, float, float]
) -> List[float]:
    """Cesium 3D Tiles ``box`` in the local ENU frame anchored at *origin*.

    Format: ``[cx, cy, cz, hx, 0, 0, 0, hy, 0, 0, 0, hz]`` (centre + three
    half-axis vectors). The bbox is stored in EPSG:4326 (x=lon, y=lat, z=h in
    metres); we convert all eight corners to ENU metres and emit the enclosing
    axis-aligned box. ENU axes ≈ lon/lat axes at city scale, so the AABB is
    tight. Oriented boxes are a future optimization.
    """
    lon0, lat0, h0 = origin
    xs: List[float] = []
    ys: List[float] = []
    zs: List[float] = []
    for lon in (b.min_x, b.max_x):
        for lat in (b.min_y, b.max_y):
            for h in (b.min_z, b.max_z):
                e, n, u = lonlat_to_enu(lon, lat, h, lon0, lat0, h0)
                xs.append(e)
                ys.append(n)
                zs.append(u)
    cx = (min(xs) + max(xs)) / 2.0
    cy = (min(ys) + max(ys)) / 2.0
    cz = (min(zs) + max(zs)) / 2.0
    hx = (max(xs) - min(xs)) / 2.0
    hy = (max(ys) - min(ys)) / 2.0
    hz = (max(zs) - min(zs)) / 2.0
    return [cx, cy, cz, hx, 0.0, 0.0, 0.0, hy, 0.0, 0.0, 0.0, hz]


def _zero_box() -> List[float]:
    return [0.0] * 12


def find_leaf(root: Dict[str, Any], tile_id: str) -> Optional[Dict[str, Any]]:
    """Return the leaf node at path *tile_id*, or ``None``.

    *tile_id* is the underscore-joined path produced by ``_build_subtree``
    (e.g. ``"0_1_0"`` = root → child[1] → child[0]). The leading ``"0"``
    is the root marker used by ``_build_subtree``'s initial ``path=[0]``.

    Walks in O(depth) by decoding the path directly rather than scanning
    the whole tree.
    """
    parts = tile_id.split("_")
    if not parts or parts[0] != "0":
        return None
    node = root
    for part in parts[1:]:
        children = node.get("children", [])
        try:
            idx = int(part)
        except ValueError:
            return None
        if idx >= len(children):
            return None
        node = children[idx]
    return node if "content" in node else None
