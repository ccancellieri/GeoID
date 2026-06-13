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
from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin
from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig
from dynastore.modules.volumes.color_ramp import DEFAULT_HEIGHT_RAMP_HEX


class VolumesConfig(ExposableConfigMixin, PluginConfig):
    """Volumes (3D tile hierarchy) guardrails.

    ``volumes`` ships as its own ``packages/extensions/volumes`` distribution
    with a ``[project.entry-points."dynastore.extensions"]`` entry, so the
    framework discovers ``VolumesService`` at boot and the Service Exposure
    matrix can enforce the inherited ``enabled`` toggle. ``enabled`` comes
    from ``ExposableConfigMixin``; do not redeclare it locally.
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "extensions", "volumes")

    max_features_per_tile: Mutable[int] = Field(default=10_000, ge=1)
    max_tree_depth: Mutable[int] = Field(default=20, ge=0, le=32)
    on_demand_cache_ttl_s: Mutable[int] = Field(default=3600, ge=0)
    default_height_attr: Mutable[str] = "height"
    # Geometries-sidecar id used to resolve the physical sidecar table
    # (``<hub>_<id>``) feeding the tiler. Matches GeometriesSidecar.sidecar_id.
    geometries_sidecar_id: Mutable[str] = "geometries"
    # Feature-id (hub PK) column joining hub ⋈ geometries sidecar. The PG
    # core driver fixes this to ``geoid``; override only for a custom driver.
    feature_id_column: Mutable[str] = "geoid"
    # Fallback geometry column when the geometries sidecar config can't be
    # resolved at runtime. Normally the live ``GeometriesSidecarConfig``
    # supplies the real column; this is the degrade-safe default.
    geometry_column_fallback: Mutable[str] = "geom"
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
    # Colour buildings by height: each feature's vertices carry a COLOR_0 tint
    # interpolated from its extrusion height against ``height_color_ramp``. When
    # disabled (or the ramp is empty) tiles render in the neutral material grey.
    color_by_height: Mutable[bool] = True
    # Sequential height colour ramp as ``[stop_metres, "#rrggbb"]`` stops. The
    # hex vocabulary matches Mapbox GL / SLD colour maps so the same ramp can
    # later be sourced from an OGC API - Styles resource. Default is ColorBrewer
    # RdYlBu reversed (low=blue → tall=red), shared with the ramp module so the
    # baked tiles and any config-surfaced default stay in sync.
    height_color_ramp: Mutable[List[Tuple[float, str]]] = Field(
        default_factory=lambda: list(DEFAULT_HEIGHT_RAMP_HEX)
    )
