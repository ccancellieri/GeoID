"""VolumesConfig — guardrails for the 3D tile hierarchy builder.

Mirrors CoveragesConfig pattern: PluginConfig subclass, 4-tier waterfall
at runtime via PlatformConfigsProtocol.
"""

from __future__ import annotations

from typing import ClassVar, List, Literal, Optional, Tuple

from pydantic import Field

# Match the base-class import used by CoveragesConfig.
from dynastore.modules.db_config.platform_config_service import PluginConfig


class VolumesConfig(PluginConfig):
    _address: ClassVar[Tuple[str, ...]] = ("platform", "extensions", "volumes")

    max_features_per_tile: int = Field(default=10_000, ge=1)
    max_tree_depth: int = Field(default=20, ge=0, le=32)
    on_demand_cache_ttl_s: int = Field(default=3600, ge=0)
    default_height_attr: str = "height"
    # Geometric error at tileset root (Cesium 3D Tiles unit — world-space distance).
    root_geometric_error: float = Field(default=500.0, gt=0)
    # Halving ratio per refinement step (4.0 matches quadtree refinement).
    refinement_ratio: float = Field(default=4.0, gt=1.0)
    # Extrusion height (metres / CRS units) applied when a feature carries
    # no explicit height attribute and the geometry is flat (2-D only).
    default_extrusion_height: float = Field(default=10.0, gt=0)
    # Tile content formats advertised in tileset.json content URIs.
    # First entry is the primary format; "glb" uses direct glTF 2.0 (3D Tiles 1.1).
    supported_formats: List[Literal["b3dm", "glb"]] = Field(default=["b3dm", "glb"])
