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

"""Combined 3D demo preset — rebuild the whole demo in one call.

The ``volumes_demo`` preset is a thin orchestrator over the two building-block
demo presets:

- ``tiles3d_samples`` — external 3D Tiles reference collections (Cesium CC0
  samples + opt-in 3D BAG Rotterdam);
- ``geovolumes_demo`` — Den Haag LoD2 CityJSON buildings ingested as items.

Both seed the same ``demo-3d`` catalog, so one ``apply`` call recreates the
entire globe demo after it has been deleted, and one ``revoke`` tears it back
down.  Each step delegates to the underlying preset, so all idempotency and
ownership rules (create-only-when-absent, delete-only-what-we-created) are
inherited unchanged.

Ordering matters for the shared catalog:
- apply runs ``tiles3d_samples`` first (it creates the catalog) then
  ``geovolumes_demo`` (catalog already present);
- revoke runs ``geovolumes_demo`` first (removes the CityJSON collection,
  never the catalog it did not create) then ``tiles3d_samples`` (removes the
  sample collections and finally the now-empty catalog).
"""
from __future__ import annotations

import logging
from typing import Any, ClassVar, Optional, Tuple, Type

from pydantic import BaseModel

from dynastore.extensions.volumes.presets.cityjson_demo import (
    GEOVOLUMES_DEMO_PRESET,
    GeoVolumesDemoParams,
)
from dynastore.extensions.volumes.presets.tiles3d_samples import (
    TILES3D_SAMPLES_PRESET,
    Tiles3dSamplesParams,
)
from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    PresetContext,
    PresetPlan,
)
from dynastore.modules.storage.presets.protocol import PresetTier

logger = logging.getLogger(__name__)

_DEFAULT_CATALOG_ID = "demo-3d"


# ---------------------------------------------------------------------------
# Params model
# ---------------------------------------------------------------------------


class VolumesDemoParams(BaseModel):
    """Parameters for the combined volumes_demo preset."""

    catalog_id: str = _DEFAULT_CATALOG_ID
    # Forwarded to tiles3d_samples — opt-in CC BY 4.0 3D BAG Rotterdam tileset.
    include_3dbag: bool = False
    # Forwarded to geovolumes_demo — cap CityJSON ingest for a faster demo.
    max_features: Optional[int] = None


# ---------------------------------------------------------------------------
# Preset class
# ---------------------------------------------------------------------------


class _VolumesDemoPreset:
    """Idempotent orchestrator that rebuilds the entire 3D demo in one call."""

    name: ClassVar[str] = "volumes_demo"
    description: ClassVar[str] = (
        "Rebuild the entire 3D demo in one call: external 3D Tiles samples "
        "(tiles3d_samples) + Den Haag LoD2 CityJSON buildings (geovolumes_demo), "
        "both under the demo-3d catalog."
    )
    keywords: ClassVar[Tuple[str, ...]] = (
        "demo", "geovolumes", "3d", "3dtiles", "cityjson", "platform",
    )
    tier: ClassVar[PresetTier] = PresetTier.PLATFORM
    catalog_scopable: ClassVar[bool] = False
    params_model: ClassVar[Type[BaseModel]] = VolumesDemoParams

    def _samples_params(self, p: VolumesDemoParams) -> Tiles3dSamplesParams:
        return Tiles3dSamplesParams(
            catalog_id=p.catalog_id,
            include_3dbag=p.include_3dbag,
        )

    def _cityjson_params(self, p: VolumesDemoParams) -> GeoVolumesDemoParams:
        return GeoVolumesDemoParams(
            catalog_id=p.catalog_id,
            max_features=p.max_features,
        )

    async def dry_run(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> PresetPlan:
        p = params if isinstance(params, VolumesDemoParams) else VolumesDemoParams()
        samples_plan = await TILES3D_SAMPLES_PRESET.dry_run(
            self._samples_params(p), scope, ctx,
        )
        cityjson_plan = await GEOVOLUMES_DEMO_PRESET.dry_run(
            self._cityjson_params(p), scope, ctx,
        )
        entries = tuple(samples_plan.entries) + tuple(cityjson_plan.entries)
        return PresetPlan(preset_name=self.name, scope_key=scope, entries=entries)

    async def apply(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> AppliedDescriptor:
        p = params if isinstance(params, VolumesDemoParams) else VolumesDemoParams()

        # Order: samples first (creates the shared catalog), CityJSON second.
        samples_desc = await TILES3D_SAMPLES_PRESET.apply(
            self._samples_params(p), scope, ctx,
        )
        cityjson_desc = await GEOVOLUMES_DEMO_PRESET.apply(
            self._cityjson_params(p), scope, ctx,
        )

        return AppliedDescriptor(payload={
            "catalog_id": p.catalog_id,
            "samples": samples_desc.payload,
            "cityjson": cityjson_desc.payload,
        })

    async def revoke(
        self,
        applied_descriptor: AppliedDescriptor,
        ctx: PresetContext,
    ) -> None:
        payload: dict[str, Any] = applied_descriptor.payload or {}

        # Order: CityJSON first (never owns the shared catalog), samples last
        # (deletes the now-empty catalog it created).
        cityjson_payload = payload.get("cityjson")
        if cityjson_payload is not None:
            await GEOVOLUMES_DEMO_PRESET.revoke(
                AppliedDescriptor(payload=cityjson_payload), ctx,
            )

        samples_payload = payload.get("samples")
        if samples_payload is not None:
            await TILES3D_SAMPLES_PRESET.revoke(
                AppliedDescriptor(payload=samples_payload), ctx,
            )


# ---------------------------------------------------------------------------
# Preset instance + registration
# ---------------------------------------------------------------------------

VOLUMES_DEMO_PRESET = _VolumesDemoPreset()

from dynastore.modules.storage.presets.registry import register_preset as _register_preset  # noqa: E402

_register_preset(VOLUMES_DEMO_PRESET)
