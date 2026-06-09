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

"""VolumesConfig — guardrails for the 3D tile hierarchy builder.

Mirrors CoveragesConfig pattern: PluginConfig subclass, 4-tier waterfall
at runtime via PlatformConfigsProtocol.
"""

from __future__ import annotations

from typing import ClassVar, List, Literal, Tuple

from pydantic import Field

# Match the base-class import used by CoveragesConfig.
from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig


class VolumesConfig(PluginConfig):
    """Volumes (3D tile hierarchy) guardrails.

    ``volumes`` lives in ``packages/core`` and ships no
    ``[project.entry-points."dynastore.extensions"]`` entry — i.e. the
    framework does not discover a ``VolumesService`` extension at boot.
    Inheriting ``ExposableConfigMixin`` would therefore expose an
    ``enabled`` toggle that ``ExposureMatrix`` cannot enforce (the
    dead-config audit guarded by ``find_dead_exposable_configs()``).

    If/when ``volumes`` is split into its own ``packages/extensions/volumes``
    distribution and registers a ``dynastore.extensions`` entry-point,
    re-introduce the mixin.
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "extensions", "volumes")

    max_features_per_tile: Mutable[int] = Field(default=10_000, ge=1)
    max_tree_depth: Mutable[int] = Field(default=20, ge=0, le=32)
    on_demand_cache_ttl_s: Mutable[int] = Field(default=3600, ge=0)
    default_height_attr: Mutable[str] = "height"
    # Geometric error at tileset root (Cesium 3D Tiles unit — world-space distance).
    root_geometric_error: Mutable[float] = Field(default=500.0, gt=0)
    # Halving ratio per refinement step (4.0 matches quadtree refinement).
    refinement_ratio: Mutable[float] = Field(default=4.0, gt=1.0)
    # Extrusion height (metres / CRS units) applied when a feature carries
    # no explicit height attribute and the geometry is flat (2-D only).
    default_extrusion_height: Mutable[float] = Field(default=10.0, gt=0)
    # Tile content formats advertised in tileset.json content URIs.
    # First entry is the primary format; "glb" uses direct glTF 2.0 (3D Tiles 1.1).
    supported_formats: Mutable[List[Literal["b3dm", "glb"]]] = Field(default=["b3dm", "glb"])
