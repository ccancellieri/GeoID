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

"""``stac_harvest_demo`` preset — parameterizable STAC harvest demo (PLATFORM tier).

Applies at platform scope (``POST /configs/presets/stac_harvest_demo``):

  1. Creates the target catalog and a placeholder ``demo_harvest_index``
     collection (skip-if-exists) so the harvest has a pre-existing target.
     The ``stac_harvest`` task creates source-catalog collections alongside
     this placeholder automatically.

  2. Submits an async ``stac_harvest`` job that pulls collections/items from
     the source STAC API into the target catalog. Items are routed to
     Elasticsearch so they are immediately searchable.

**Default behavior (no body / empty body)**:

  Harvests one collection and 25 items from the Earth Search v1 public STAC
  API (``https://earth-search.aws.element84.com/v1``) into the
  ``demo_harvest`` catalog — fast enough for CI.

**Parameterized behavior**:

  POST a JSON body with any subset of these fields to harvest an arbitrary
  STAC source into an arbitrary target catalog::

      {
        "url": "https://planetarycomputer.microsoft.com/api/stac/v1",
        "target_catalog": "my_harvest",
        "max_collections": 3,
        "max_items": 100
      }

  All fields are optional. An absent field falls back to the Earth Search v1
  default.

Revoke: removes the placeholder collection and the target catalog if it is
empty after collection removal. Collections and items the async harvest job
wrote to Elasticsearch are not undone — re-applying re-syncs them
idempotently (all upserts keyed on STAC id).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, Dict, Iterable, Optional, Tuple, Type

from pydantic import BaseModel, Field

from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    DataSeed,
    PresetContext,
    PresetPlan,
    PresetPlanEntry,
    TaskSeed,
)
from dynastore.modules.storage.presets.protocol import PresetTier

_CATALOG_ID = "demo_harvest"
_COLLECTION_ID = "demo_harvest_index"

_CATALOG_DATA: Dict[str, Any] = {
    "id": _CATALOG_ID,
    "title": {"en": "STAC Harvest Demo"},
    "description": {
        "en": (
            "Demo catalog populated by the stac_harvest_demo preset. "
            "Collections and items are harvested from a public STAC API "
            "into this catalog."
        ),
    },
    "keywords": ["demo", "stac", "harvest", "platform"],
    "license": "CC-BY-4.0",
}

_COLLECTION_DATA: Dict[str, Any] = {
    "id": _COLLECTION_ID,
    "title": {"en": "Harvest Demo Index"},
    "description": {
        "en": (
            "Placeholder collection created by the stac_harvest_demo preset "
            "to anchor the target catalog. The stac_harvest job creates "
            "source-catalog collections alongside this one."
        ),
    },
}

# Earth Search v1 — small, reliable, publicly accessible STAC catalog hosted
# by Element 84 (AWS Open Data).  max_collections=1 + max_items=25 keeps the
# default demo harvest fast (two to three HTTP round-trips total).
_EARTH_SEARCH_URL = "https://earth-search.aws.element84.com/v1"


# ---------------------------------------------------------------------------
# Params model
# ---------------------------------------------------------------------------

class StacHarvestDemoParams(BaseModel):
    """Optional overrides for the ``stac_harvest_demo`` preset.

    All fields default to reproducing the original param-less behavior
    exactly (Earth Search v1, one collection, 25 items, into demo_harvest).

    Minimal call to harvest a different STAC source into a custom target::

        POST /configs/presets/stac_harvest_demo
        {"url": "https://planetarycomputer.microsoft.com/api/stac/v1",
         "target_catalog": "pc_harvest", "max_collections": 3, "max_items": 100}
    """

    model_config = {"extra": "ignore"}

    url: Optional[str] = Field(
        default=None,
        description=(
            "Base URL of the remote STAC catalog to harvest — must expose "
            "/collections and /collections/{id}/items. When absent the "
            "public Earth Search v1 API is used."
        ),
        examples=[
            "https://earth-search.aws.element84.com/v1",
            "https://planetarycomputer.microsoft.com/api/stac/v1",
        ],
    )
    target_catalog: Optional[str] = Field(
        default=None,
        description=(
            "Destination dynastore catalog identifier. The catalog is created "
            "if it does not already exist. Defaults to demo_harvest."
        ),
        examples=["demo_harvest", "pc_harvest"],
    )
    max_collections: int = Field(
        default=1,
        ge=0,
        description="Maximum number of source collections to harvest (0 = all). Defaults to 1.",
    )
    max_items: int = Field(
        default=25,
        ge=0,
        description="Maximum number of items per collection to harvest (0 = all). Defaults to 25.",
    )
    with_assets: bool = Field(
        default=True,
        description=(
            "When True, register each item asset href as a virtual asset "
            "(dynastore stores only the href, never the bytes)."
        ),
    )
    drivers: str = Field(
        default="es",
        description=(
            "Storage routing for harvested items: 'es' routes directly to "
            "public Elasticsearch (default, immediately searchable)."
        ),
    )


# ---------------------------------------------------------------------------
# Per-request contributor — constructed fresh with resolved params
# ---------------------------------------------------------------------------

@dataclass
class _StacHarvestDemoContributor:
    """Data + task contributor for one stac_harvest_demo apply call.

    Constructed per-call with the resolved parameter values so the preset
    singleton remains stateless and safe under concurrent requests.
    """

    catalog_id: str
    url: str
    max_collections: int
    max_items: int
    with_assets: bool
    drivers: str

    def get_data(self) -> Iterable[DataSeed]:
        # Pre-create the target catalog + a placeholder collection so
        # ``stac_harvest`` can call create_collection on the catalog without
        # raising a missing-catalog error.
        catalog_data = {**_CATALOG_DATA, "id": self.catalog_id}
        yield DataSeed(
            catalog_id=self.catalog_id,
            collection_id=_COLLECTION_ID,
            catalog_data=catalog_data,
            collection_data=_COLLECTION_DATA,
            items=(),
            manage_catalog=True,
            manage_collection=True,
            # A STAC harvest routes items straight to Elasticsearch and
            # registers asset hrefs as virtual assets — bytes are never
            # uploaded — so the harvested catalog never needs a GCS bucket.
            # defer holds back the deferrable storage-backend provisioners so
            # the catalog reaches ``ready`` bucket-free (born bucket-free).
            defer_provisioning=True,
        )

    def get_tasks(self) -> Iterable[TaskSeed]:
        # The dedup_key incorporates the target + source so concurrent
        # applies harvesting different sources/targets are not collapsed
        # onto the same job.
        dedup_key = f"preset:stac_harvest_demo:{self.catalog_id}:{self.url}"
        yield TaskSeed(
            process_id="stac_harvest",
            inputs={
                "catalog_url": self.url,
                "target_catalog": self.catalog_id,
                "max_collections": self.max_collections,
                "max_items": self.max_items,
                "with_assets": self.with_assets,
                "drivers": self.drivers,
            },
            async_mode=True,
            dedup_key=dedup_key,
        )


# ---------------------------------------------------------------------------
# Preset class — stateless, safe under concurrency
# ---------------------------------------------------------------------------

class _StacHarvestDemoPreset:
    """Parameterizable STAC harvest demo preset.

    Implements the Preset protocol without subclassing MultiContributorPreset
    so that per-request params can be threaded to the contributor without
    mutating shared singleton state.
    """

    name: ClassVar[str] = "stac_harvest_demo"
    tier: ClassVar[PresetTier] = PresetTier.PLATFORM
    catalog_scopable: ClassVar[bool] = False
    params_model: ClassVar[Type[BaseModel]] = StacHarvestDemoParams
    keywords: ClassVar[Tuple[str, ...]] = (
        "demo", "data", "platform", "stac", "harvest",
    )
    description: ClassVar[str] = (
        "Create the target catalog and submit an async stac_harvest job that "
        "harvests collections/items from a public STAC API. Without params "
        "harvests one collection / 25 items from the Earth Search v1 public "
        "STAC API (https://earth-search.aws.element84.com/v1) into demo_harvest. "
        "Pass url, target_catalog, max_collections, and/or max_items to harvest "
        "any STAC source into a custom target. Items land in Elasticsearch and "
        "are immediately searchable. Designed for CI / demo use: small default "
        "limits keep the run fast."
    )

    def _resolve(self, params: BaseModel) -> _StacHarvestDemoContributor:
        """Coerce params and resolve defaults — returns a per-call contributor."""
        p = (
            params
            if isinstance(params, StacHarvestDemoParams)
            else StacHarvestDemoParams.model_validate(params.model_dump())
        )
        return _StacHarvestDemoContributor(
            catalog_id=p.target_catalog or _CATALOG_ID,
            url=p.url or _EARTH_SEARCH_URL,
            max_collections=p.max_collections,
            max_items=p.max_items,
            with_assets=p.with_assets,
            drivers=p.drivers,
        )

    async def dry_run(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> PresetPlan:
        c = self._resolve(params)
        entries: list[PresetPlanEntry] = []
        for seed in c.get_data():
            entries.append(PresetPlanEntry(
                kind="seed_data",
                target=f"{seed.catalog_id}/{seed.collection_id}",
                detail={"items": len(seed.items)},
            ))
        for tseed in c.get_tasks():
            entries.append(PresetPlanEntry(
                kind="trigger_task",
                target=tseed.process_id,
                detail={"async": tseed.async_mode, "inputs": dict(tseed.inputs)},
            ))
        return PresetPlan(
            preset_name=self.name,
            scope_key=scope,
            entries=tuple(entries),
        )

    async def apply(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> AppliedDescriptor:
        from dynastore.modules.storage.presets.multi_contributor import (
            _apply_data_kind,
            _apply_task_kind,
        )

        c = self._resolve(params)
        applied_data: list[dict] = []
        applied_tasks: list[dict] = []
        await _apply_data_kind(self.name, c, ctx, applied_data)
        await _apply_task_kind(self.name, c, ctx, applied_tasks)

        return AppliedDescriptor(payload={
            "preset_name": self.name,
            "policy_ids": [],
            "role_names": [],
            "config_qualnames": [],
            "data": applied_data,
            "tasks": applied_tasks,
            "scope": scope,
        })

    async def revoke(
        self,
        applied_descriptor: AppliedDescriptor,
        ctx: PresetContext,
    ) -> None:
        from dynastore.modules.storage.presets.multi_contributor import (
            _revoke_data_kind,
            _revoke_task_kind,
        )

        payload = applied_descriptor.payload
        await _revoke_task_kind(self.name, payload.get("tasks", []))
        await _revoke_data_kind(self.name, ctx, payload.get("data", []))



# ---------------------------------------------------------------------------
# Preset instance + registration
# ---------------------------------------------------------------------------

STAC_HARVEST_DEMO_PRESET = _StacHarvestDemoPreset()

from dynastore.modules.storage.presets.registry import register_preset as _register_preset  # noqa: E402

_register_preset(STAC_HARVEST_DEMO_PRESET)
