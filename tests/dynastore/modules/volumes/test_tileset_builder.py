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

import math

from dynastore.extensions.volumes.config import VolumesConfig
from dynastore.modules.volumes.bounds import FeatureBounds
from dynastore.modules.volumes.tileset_builder import build_tileset, find_leaf


def test_empty_bounds_emits_skeleton():
    ts = build_tileset([], VolumesConfig())
    assert ts["asset"]["version"] == "1.0"
    assert "root" in ts
    assert ts["root"]["geometricError"] == 0.0
    assert ts["root"]["boundingVolume"]["box"] == [0.0] * 12


def test_asset_declares_z_up_axis():
    # The tile content (glTF) is authored Z-up in the ENU frame, but renderers
    # default glTF content to Y-up and apply a Y->Z rotation. Declaring
    # gltfUpAxis=Z in the tileset asset suppresses that rotation so buildings
    # stand upright instead of being tipped over.
    populated = build_tileset(
        [FeatureBounds("f", 4.30, 52.07, 0.0, 4.31, 52.08, 20.0)],
        VolumesConfig(),
    )
    assert populated["asset"]["gltfUpAxis"] == "Z"
    # The empty skeleton must declare it too so an empty collection round-trips
    # with the same convention.
    empty = build_tileset([], VolumesConfig())
    assert empty["asset"]["gltfUpAxis"] == "Z"


def test_root_has_enu_ecef_transform():
    # A Den Haag-ish footprint: the root must carry a 16-float column-major
    # transform whose translation column is the ECEF of the collection centre
    # (millions of metres), placing the local frame on the globe.
    b = [FeatureBounds("f", 4.30, 52.07, 0.0, 4.31, 52.08, 20.0)]
    ts = build_tileset(b, VolumesConfig())
    tr = ts["root"]["transform"]
    assert isinstance(tr, list) and len(tr) == 16
    assert tr[15] == 1.0
    # Translation column (indices 12..14) is ECEF metres — geocentric radius
    # of the origin is ~6.37e6 m.
    r = math.sqrt(tr[12] ** 2 + tr[13] ** 2 + tr[14] ** 2)
    assert 6_356_000 < r < 6_379_000


def test_box_is_metric_not_degrees():
    # 0.01 deg lon/lat spans ~hundreds of metres, not 0.005. The box half-axes
    # must be in metres (local ENU), not raw degrees.
    b = [FeatureBounds("f", 4.30, 52.07, 0.0, 4.31, 52.08, 20.0)]
    ts = build_tileset(b, VolumesConfig())
    box = ts["root"]["boundingVolume"]["box"]
    hx, hy, hz = box[3], box[7], box[11]
    assert hx > 100.0  # ~340 m half-width, definitely metric
    assert hy > 100.0
    assert math.isclose(hz, 10.0, abs_tol=1.0)  # 20 m z-extent → 10 m half


def test_single_feature_is_a_leaf():
    b = [FeatureBounds("f", 0, 0, 0, 1, 1, 1)]
    ts = build_tileset(b, VolumesConfig())
    root = ts["root"]
    assert "children" not in root
    assert root["content"]["uri"].endswith(".b3dm")
    assert root["_feature_ids"] == ["f"]


def test_root_geometric_error_from_config():
    cfg = VolumesConfig(root_geometric_error=42.0)
    ts = build_tileset([FeatureBounds("f", 0, 0, 0, 1, 1, 1)], cfg)
    assert ts["geometricError"] == 42.0
    # Leaf node halves per depth — root is depth 0.
    assert ts["root"]["geometricError"] == 42.0


def test_two_features_are_split_at_pivot():
    # Far-apart features so the partition succeeds.
    b = [
        FeatureBounds("left",  0, 0, 0, 1, 1, 1),
        FeatureBounds("right", 10, 0, 0, 11, 1, 1),
    ]
    cfg = VolumesConfig(max_features_per_tile=1, max_tree_depth=5)
    ts = build_tileset(b, cfg)
    assert "children" in ts["root"]
    assert len(ts["root"]["children"]) == 2
    # Each child is a leaf.
    for child in ts["root"]["children"]:
        assert "children" not in child
        assert len(child["_feature_ids"]) == 1


def test_max_tree_depth_forces_leaf():
    # Two overlapping features with a depth cap of 0 → root must be a leaf
    # (cannot split into deeper tree).
    b = [
        FeatureBounds("a", 0, 0, 0, 1, 1, 1),
        FeatureBounds("b", 0.1, 0.1, 0.1, 1.1, 1.1, 1.1),
    ]
    cfg = VolumesConfig(max_features_per_tile=1, max_tree_depth=0)
    ts = build_tileset(b, cfg)
    assert "children" not in ts["root"]
    assert sorted(ts["root"]["_feature_ids"]) == ["a", "b"]


def test_degenerate_partition_collapses_to_leaf():
    # Three features identically placed — partition can't separate them;
    # builder should collapse to a leaf rather than recurse forever.
    b = [FeatureBounds(f"f{i}", 0, 0, 0, 1, 1, 1) for i in range(3)]
    cfg = VolumesConfig(max_features_per_tile=1, max_tree_depth=20)
    ts = build_tileset(b, cfg)
    # Top-level should be either a leaf, or one child is a leaf — walk
    # down and confirm no infinite recursion happened (depth < tree-cap).
    depth = 0
    node = ts["root"]
    while "children" in node:
        node = node["children"][0]
        depth += 1
        assert depth <= cfg.max_tree_depth + 1


def test_content_uri_template_is_formatted():
    b = [FeatureBounds("f", 0, 0, 0, 1, 1, 1)]
    ts = build_tileset(b, VolumesConfig(), content_uri_template="X/{tile_id}.bin")
    assert ts["root"]["content"]["uri"].startswith("X/")
    assert ts["root"]["content"]["uri"].endswith(".bin")


# ---------------------------------------------------------------------------
# find_leaf
# ---------------------------------------------------------------------------


def test_find_leaf_root_single_feature():
    b = [FeatureBounds("f", 0, 0, 0, 1, 1, 1)]
    ts = build_tileset(b, VolumesConfig())
    leaf = find_leaf(ts["root"], "0")
    assert leaf is not None
    assert leaf["_feature_ids"] == ["f"]


def test_find_leaf_child_path():
    b = [
        FeatureBounds("left", 0, 0, 0, 1, 1, 1),
        FeatureBounds("right", 10, 0, 0, 11, 1, 1),
    ]
    cfg = VolumesConfig(max_features_per_tile=1)
    ts = build_tileset(b, cfg)
    # Both children are leaves; one has tile_id "0_0", other "0_1".
    leaf0 = find_leaf(ts["root"], "0_0")
    leaf1 = find_leaf(ts["root"], "0_1")
    assert leaf0 is not None and leaf1 is not None
    assert set(leaf0["_feature_ids"] + leaf1["_feature_ids"]) == {"left", "right"}


def test_find_leaf_unknown_tile_id_returns_none():
    b = [FeatureBounds("f", 0, 0, 0, 1, 1, 1)]
    ts = build_tileset(b, VolumesConfig())
    assert find_leaf(ts["root"], "999") is None
    assert find_leaf(ts["root"], "0_0_0") is None


def test_find_leaf_empty_tree_returns_none():
    ts = build_tileset([], VolumesConfig())
    assert find_leaf(ts["root"], "0") is None  # empty root has no content


def test_find_leaf_invalid_path_returns_none():
    b = [FeatureBounds("f", 0, 0, 0, 1, 1, 1)]
    ts = build_tileset(b, VolumesConfig())
    assert find_leaf(ts["root"], "bad_path") is None
    assert find_leaf(ts["root"], "") is None
