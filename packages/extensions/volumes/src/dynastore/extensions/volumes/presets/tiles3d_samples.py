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

"""3D Tiles external-reference samples preset.

Seeds a ``demo-3d`` catalog with one collection per entry in the built-in
sample registry.  Each collection records the external tileset URL and bbox
in collection extras; no item data is ingested (these are external references).

Running ``apply`` is idempotent: the catalog and each collection are created
only when absent.  Running ``revoke`` removes the sample collections (only if
they contain no items) and the catalog only if it becomes empty.

Sample data is sourced from CesiumGS/3d-tiles-samples (Apache-2.0 / CC0)
unless stated otherwise.  At apply time the preset attempts to fetch each
tileset.json to derive a bbox from its ``root.boundingVolume.region`` field;
on failure a registry fallback bbox is used.
"""
from __future__ import annotations

import json
import logging
import math
import urllib.request
from dataclasses import dataclass
from typing import Any, ClassVar, Dict, List, Optional, Tuple, Type

from pydantic import BaseModel

from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    PresetContext,
    PresetPlan,
    PresetPlanEntry,
)
from dynastore.modules.storage.presets.protocol import PresetTier

logger = logging.getLogger(__name__)

_DEFAULT_CATALOG_ID = "demo-3d"
_FETCH_TIMEOUT_S = 10


# ---------------------------------------------------------------------------
# Radians-region → degrees-bbox conversion (testable pure function)
# ---------------------------------------------------------------------------


def region_to_bbox(region: List[float]) -> List[float]:
    """Convert a 3D Tiles boundingVolume.region to a [minLon, minLat, minH, maxLon, maxLat, maxH] bbox.

    A region is [west, south, east, north, minH, maxH] in radians (angles) and
    metres (heights).  Output bbox uses degrees for the horizontal axes and
    metres for the vertical axes.
    """
    if len(region) != 6:
        raise ValueError(f"region must have exactly 6 elements, got {len(region)}")
    west, south, east, north, min_h, max_h = region
    return [
        math.degrees(west),
        math.degrees(south),
        float(min_h),
        math.degrees(east),
        math.degrees(north),
        float(max_h),
    ]


# ---------------------------------------------------------------------------
# Sample registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Sample:
    """Descriptor for a single external 3D Tiles sample."""

    id: str
    title: str
    url: str
    fallback_bbox: List[float]  # [minLon, minLat, minH, maxLon, maxLat, maxH]
    attribution: Optional[str] = None
    include_by_default: bool = True


# Fallback bboxes for the Cesium samples (approximate; near Philadelpha test area)
_CESIUM_FALLBACK_BBOX: List[float] = [-75.62, 40.03, 0.0, -75.60, 40.05, 100.0]
# Rotterdam (3D BAG)
_ROTTERDAM_FALLBACK_BBOX: List[float] = [4.40, 51.86, 0.0, 4.55, 51.99, 250.0]

SAMPLE_REGISTRY: Tuple[_Sample, ...] = (
    _Sample(
        id="tiles3d-dragon-lod",
        title="Discrete LOD (b3dm, 3D Tiles 1.0)",
        url=(
            "https://raw.githubusercontent.com/CesiumGS/3d-tiles-samples/main"
            "/1.0/TilesetWithDiscreteLOD/tileset.json"
        ),
        fallback_bbox=_CESIUM_FALLBACK_BBOX,
    ),
    _Sample(
        id="tiles3d-trees-i3dm",
        title="Instanced trees (i3dm, 3D Tiles 1.0)",
        url=(
            "https://raw.githubusercontent.com/CesiumGS/3d-tiles-samples/main"
            "/1.0/TilesetWithTreeBillboards/tileset.json"
        ),
        fallback_bbox=_CESIUM_FALLBACK_BBOX,
    ),
    _Sample(
        id="tiles3d-implicit-quadtree",
        title="Sparse implicit quadtree (glTF, 3D Tiles 1.1)",
        url=(
            "https://raw.githubusercontent.com/CesiumGS/3d-tiles-samples/main"
            "/1.1/SparseImplicitQuadtree/tileset.json"
        ),
        fallback_bbox=_CESIUM_FALLBACK_BBOX,
    ),
    _Sample(
        id="tiles3d-multiple-contents",
        title="Multiple contents per tile (3D Tiles 1.1)",
        url=(
            "https://raw.githubusercontent.com/CesiumGS/3d-tiles-samples/main"
            "/1.1/MultipleContents/tileset.json"
        ),
        fallback_bbox=_CESIUM_FALLBACK_BBOX,
    ),
    _Sample(
        id="tiles3d-3dbag-rotterdam",
        title="3D BAG Rotterdam (3D Tiles 1.1 implicit)",
        url="https://3dtilesnederland.nl/tiles/1.0/implicit/nederland/599.json",
        fallback_bbox=_ROTTERDAM_FALLBACK_BBOX,
        attribution=(
            "© 3DBAG by tudelft3d and 3DGI — CC BY 4.0"
            " (https://docs.3dbag.nl/en/copyright/)"
        ),
        include_by_default=False,
    ),
)


# ---------------------------------------------------------------------------
# Bbox resolution helper (non-fatal network fetch)
# ---------------------------------------------------------------------------


def _resolve_bbox(sample: _Sample) -> List[float]:
    """Attempt to derive a bbox from the tileset.json; fall back to registry default."""
    try:
        req = urllib.request.Request(
            sample.url,
            headers={"User-Agent": "dynastore-volumes-preset/1.0"},
        )
        with urllib.request.urlopen(req, timeout=_FETCH_TIMEOUT_S) as resp:  # noqa: S310
            data: Dict[str, Any] = json.loads(resp.read())
        bv = data.get("root", {}).get("boundingVolume", {})
        region = bv.get("region")
        if isinstance(region, list) and len(region) == 6:
            return region_to_bbox(region)
        logger.debug(
            "tiles3d_samples: tileset %s has no region boundingVolume; using fallback",
            sample.id,
        )
    except Exception as exc:
        logger.warning(
            "tiles3d_samples: could not fetch %s for bbox (%s); using fallback",
            sample.url,
            exc,
        )
    return list(sample.fallback_bbox)


# ---------------------------------------------------------------------------
# Catalog / collection helpers  (mirror cityjson_demo)
# ---------------------------------------------------------------------------

_CATALOG_DATA: Dict[str, Any] = {
    "id": _DEFAULT_CATALOG_ID,
    "title": {"en": "3D Demo Catalog"},
    "description": {"en": "Demo catalog for 3D GeoVolumes datasets."},
    "keywords": ["demo", "3d", "geovolumes"],
    "license": "CC-BY-4.0",
}


async def _collection_item_count(catalogs: Any, catalog_id: str, collection_id: str) -> int:
    try:
        from dynastore.models.query_builder import QueryRequest

        features = await catalogs.search_items(
            catalog_id, collection_id, QueryRequest(limit=1),
        )
        return len(features)
    except Exception as exc:
        logger.debug("item-count probe %s/%s failed: %s", catalog_id, collection_id, exc)
        return 0


async def _catalog_has_collections(catalogs: Any, catalog_id: str) -> bool:
    try:
        cols = await catalogs.list_collections(catalog_id, limit=1)
        return bool(cols)
    except Exception as exc:
        logger.warning("collection probe for catalog %s failed: %s", catalog_id, exc)
        return True


# ---------------------------------------------------------------------------
# Params model
# ---------------------------------------------------------------------------


class Tiles3dSamplesParams(BaseModel):
    """Parameters for the tiles3d_samples preset."""

    catalog_id: str = _DEFAULT_CATALOG_ID
    # CC0 Cesium samples seed by default; 3D BAG (Netherlands) ships real LoD2
    # building meshes but is CC BY 4.0, so it stays opt-in (attribution-aware).
    include_3dbag: bool = False
    collection_prefix: str = ""


# ---------------------------------------------------------------------------
# Preset class
# ---------------------------------------------------------------------------


class _Tiles3dSamplesPreset:
    """Idempotent preset that seeds external 3D Tiles sample collections.

    No item data is ingested; each collection records the external tileset URL
    and bbox in extras.  3D Tiles are served directly from the upstream host.
    """

    name: ClassVar[str] = "tiles3d_samples"
    description: ClassVar[str] = (
        "Seed a demo-3d catalog with external 3D Tiles sample collections "
        "(CesiumGS/3d-tiles-samples, CC0). No item ingest; external references only."
    )
    keywords: ClassVar[Tuple[str, ...]] = (
        "demo", "geovolumes", "3d", "3dtiles", "external", "platform",
    )
    tier: ClassVar[PresetTier] = PresetTier.PLATFORM
    catalog_scopable: ClassVar[bool] = False
    params_model: ClassVar[Type[BaseModel]] = Tiles3dSamplesParams

    def _active_samples(self, p: Tiles3dSamplesParams) -> List[_Sample]:
        return [
            s for s in SAMPLE_REGISTRY
            if s.include_by_default or (s.id.endswith("rotterdam") and p.include_3dbag)
        ]

    def _collection_id(self, sample: _Sample, prefix: str) -> str:
        return f"{prefix}{sample.id}" if prefix else sample.id

    async def dry_run(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> PresetPlan:
        p = params if isinstance(params, Tiles3dSamplesParams) else Tiles3dSamplesParams()
        entries = [
            PresetPlanEntry(
                kind="ensure_catalog",
                target=p.catalog_id,
                detail={"create_if_absent": True},
            ),
        ]
        for sample in self._active_samples(p):
            cid = self._collection_id(sample, p.collection_prefix)
            entries.append(PresetPlanEntry(
                kind="ensure_collection",
                target=f"{p.catalog_id}/{cid}",
                detail={"create_if_absent": True, "tileset_url": sample.url},
            ))
        return PresetPlan(preset_name=self.name, scope_key=scope, entries=entries)

    async def apply(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> AppliedDescriptor:
        p = params if isinstance(params, Tiles3dSamplesParams) else Tiles3dSamplesParams()
        catalogs = ctx.catalogs

        created_catalog = False
        created_collections: List[str] = []

        # --- Catalog (create only when absent) ---
        if await catalogs.get_catalog_model(p.catalog_id) is None:
            cat_payload = dict(_CATALOG_DATA)
            cat_payload["id"] = p.catalog_id
            await catalogs.create_catalog(cat_payload, lang="*")
            created_catalog = True

        # --- Collections ---
        for sample in self._active_samples(p):
            cid = self._collection_id(sample, p.collection_prefix)
            existing = await catalogs.get_collection(p.catalog_id, cid, lang="*")
            if existing is not None:
                logger.info("tiles3d_samples: collection %s/%s already exists — skipping", p.catalog_id, cid)
                continue

            bbox = _resolve_bbox(sample)
            extras: Dict[str, Any] = {
                "geovolumes:enabled": True,
                "geovolumes:tileset_url": sample.url,
                "geovolumes:bbox": bbox,
            }
            if sample.attribution:
                extras["geovolumes:attribution"] = sample.attribution

            coll_payload: Dict[str, Any] = {
                "id": cid,
                "title": {"en": sample.title},
                "description": {"en": f"External 3D Tiles reference — {sample.title}"},
                "type": "Feature",
                "extra_metadata": {"en": extras},
            }
            await catalogs.create_collection(p.catalog_id, coll_payload, lang="*")
            created_collections.append(cid)
            logger.info("tiles3d_samples: created collection %s/%s", p.catalog_id, cid)

        return AppliedDescriptor(payload={
            "catalog_id": p.catalog_id,
            "created_catalog": created_catalog,
            "created_collections": created_collections,
            "include_3dbag": p.include_3dbag,
            "collection_prefix": p.collection_prefix,
        })

    async def revoke(
        self,
        applied_descriptor: AppliedDescriptor,
        ctx: PresetContext,
    ) -> None:
        payload = applied_descriptor.payload
        catalog_id: str = payload.get("catalog_id", _DEFAULT_CATALOG_ID)
        created_catalog: bool = bool(payload.get("created_catalog", False))
        created_collections: List[str] = list(payload.get("created_collections", []))
        catalogs = ctx.catalogs

        collection_deleted_count = 0
        for cid in created_collections:
            item_count = await _collection_item_count(catalogs, catalog_id, cid)
            if item_count > 0:
                logger.warning(
                    "tiles3d_samples: collection %s/%s has %d item(s) — leaving in place",
                    catalog_id, cid, item_count,
                )
                continue
            try:
                await catalogs.delete_collection(catalog_id, cid, force=True)
                collection_deleted_count += 1
            except Exception as exc:
                logger.warning(
                    "tiles3d_samples: delete_collection %s/%s failed: %s",
                    catalog_id, cid, exc,
                )

        if created_catalog and collection_deleted_count == len(created_collections):
            if await _catalog_has_collections(catalogs, catalog_id):
                logger.warning(
                    "tiles3d_samples: catalog %s still has collections — leaving in place",
                    catalog_id,
                )
            else:
                try:
                    await catalogs.delete_catalog(catalog_id, force=True)
                except Exception as exc:
                    logger.warning(
                        "tiles3d_samples: delete_catalog %s failed: %s", catalog_id, exc,
                    )


# ---------------------------------------------------------------------------
# Preset instance + registration
# ---------------------------------------------------------------------------

TILES3D_SAMPLES_PRESET = _Tiles3dSamplesPreset()

from dynastore.modules.storage.presets.registry import register_preset as _register_preset  # noqa: E402

_register_preset(TILES3D_SAMPLES_PRESET)
