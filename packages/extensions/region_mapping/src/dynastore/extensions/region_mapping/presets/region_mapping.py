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

"""``region_mapping`` preset — claims a collection's region-id column for
TerriaJS WMS region mapping (dynastore#443 Phase 1).

REGISTRATION IS PUBLICATION. Applying this preset on a collection is an
explicit decision to publish, via ``/region-mappings/definitions`` and
``/region-mappings/{mapping_id}/regionIds``, to anyone who can reach those
routes:

* the claimed column's distinct values (every ``region_prop`` value in the
  source collection, fetched in full by the regionIds endpoint);
* the source collection's bbox and title;
* a working WMS/MVT tile URL for the source collection.

There is no separate visibility check against the source collection's own
access posture — applying ``region_mapping`` against a collection an
operator wants to keep private is a mistake, not something this preset
guards against. Do not register a collection you do not want partially
public this way.

Collection-scoped (``catalog_scopable=False``). Apply endpoint::

    POST /configs/catalogs/{cat}/collections/{col}/presets/region_mapping
    Body: {"column": "adm0_code", "alias": "country",
           "extra_aliases": ["adm0"], "title": "Country boundaries"}

``apply()``:

1. Ensures the shared ``_region_mappings_`` registry exists (idempotent —
   reuses ``region_mappings_registry.ensure_registry_provisioned``).
2. Computes the claim set — see ``registry_data.compute_claim_set`` — deletes
   any PREVIOUSLY-registered claim for this mapping_id that is no longer in
   the new set (a force re-apply with a changed alias must not orphan the
   old claim — it would squat the ``claim_ci`` UNIQUE constraint forever and
   keep appearing in the Terria ``aliases`` array), then upserts one record
   per claim, keyed by a stable ``"{mapping_id}__{claim_ci}"`` id so a
   re-apply of the SAME mapping is an idempotent update while a DIFFERENT
   mapping claiming the same text hits the ``claim_ci`` UNIQUE constraint
   (``23505`` -> HTTP 409, propagated unchanged — never caught here).
3. Invalidates the extension router's serving caches.

Revoke is authoritative by ``mapping_id``: it re-queries the registry for
every claim record currently sharing this mapping_id and deletes all of
them — not just the ids recorded by the apply that produced this
descriptor — so a revoke after one or more re-applies still removes
everything. The descriptor's recorded item ids are used only as a fallback
when that query comes back empty (e.g. an old descriptor shape). The shared
registry catalog/collection is never touched by this preset — see
``region_mappings_registry`` for its own provision/revoke lifecycle.
"""
from __future__ import annotations

import logging
from typing import ClassVar, List, Optional, Tuple, Type

from pydantic import BaseModel, Field

from dynastore.modules.storage.driver_config import ItemsSchema
from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    PresetContext,
    PresetPlan,
    PresetPlanEntry,
)
from dynastore.modules.storage.presets.protocol import PresetTier
from dynastore.modules.storage.presets.registry import register_preset

from ..registry_data import (
    MAPPINGS_COLLECTION_ID,
    REGISTRY_CATALOG_ID,
    compute_claim_set,
    fetch_claim_by_ci,
    fetch_claims_for_mapping_uncached,
    invalidate_serving_caches,
    is_degenerate_bbox,
    item_id_for,
    mapping_id_for,
)
from .region_mappings_registry import ensure_registry_provisioned

logger = logging.getLogger(__name__)


class RegionMappingParams(BaseModel):
    """Parameters for the ``region_mapping`` preset."""

    column: str = Field(
        ...,
        description=(
            "Source collection property carrying the region-id values "
            "TerriaJS should join against."
        ),
    )
    alias: Optional[str] = Field(
        default=None,
        description=(
            "Canonical alias TerriaJS will match this region type against. "
            "Defaults to 'column' when unset."
        ),
    )
    extra_aliases: List[str] = Field(
        default_factory=list,
        description="Additional alias strings TerriaJS should also accept.",
    )
    title: Optional[str] = Field(
        default=None,
        description="Human-readable description; defaults to the collection id.",
    )


def _require_collection_scope(scope: str) -> Tuple[str, str]:
    """Parse ``catalog:<id>/collection:<id>`` out of a preset scope string."""
    catalog_id: Optional[str] = None
    collection_id: Optional[str] = None
    for part in (scope or "").split("/"):
        if part.startswith("catalog:"):
            catalog_id = part.split(":", 1)[1]
        elif part.startswith("collection:"):
            collection_id = part.split(":", 1)[1]
    if not catalog_id or not collection_id:
        raise ValueError(
            "region_mapping: requires a collection scope "
            f"(catalog:<id>/collection:<id>), got {scope!r}"
        )
    return catalog_id, collection_id


def _coerce_params(params: BaseModel) -> RegionMappingParams:
    if isinstance(params, RegionMappingParams):
        return params
    data = params.model_dump() if hasattr(params, "model_dump") else {}
    return RegionMappingParams.model_validate(data)


class _RegionMappingPreset:
    """Collection-tier preset — claims one column as a TerriaJS region type."""

    name: ClassVar[str] = "region_mapping"
    description: ClassVar[str] = (
        "Claim this collection's region-id column for TerriaJS WMS region "
        "mapping. Registers the column, alias, and any extra_aliases as "
        "claims in the shared region-mapping registry."
    )
    keywords: ClassVar[Tuple[str, ...]] = ("region-mapping", "terria", "collection")
    tier: ClassVar[PresetTier] = PresetTier.COLLECTION
    catalog_scopable: ClassVar[bool] = False
    params_model: ClassVar[Type[BaseModel]] = RegionMappingParams

    async def dry_run(
        self, params: BaseModel, scope: str, ctx: PresetContext,
    ) -> PresetPlan:
        p = _coerce_params(params)
        catalog_id, collection_id = _require_collection_scope(scope)
        mapping_id = mapping_id_for(catalog_id, collection_id)
        claims = compute_claim_set(
            catalog_id=catalog_id, collection_id=collection_id,
            column=p.column, alias=p.alias, extra_aliases=p.extra_aliases,
        )

        entries: List[PresetPlanEntry] = [
            PresetPlanEntry(
                kind="ensure_registry", target=REGISTRY_CATALOG_ID,
                detail={"if_absent": True},
            ),
            PresetPlanEntry(
                kind="publish_notice",
                target=f"{catalog_id}/{collection_id}",
                detail={
                    "note": (
                        "Applying this preset publishes, to anyone who can "
                        "reach /region-mappings, the distinct values of "
                        f"{p.column!r}, this collection's bbox/title, and "
                        "its tile URL. There is no separate visibility "
                        "check — registering a private collection is an "
                        "explicit decision to publish that much."
                    ),
                },
            ),
        ]
        for claim_ci, (claim, role) in claims.items():
            entries.append(PresetPlanEntry(
                kind="upsert_claim",
                target=claim,
                detail={"claim_ci": claim_ci, "role": role, "mapping_id": mapping_id},
            ))

        warnings: List[str] = []
        if ctx.catalogs is not None:
            for claim_ci, (claim, _role) in claims.items():
                existing = await fetch_claim_by_ci(claim_ci)
                existing_mapping = existing.get("mapping_id") if existing else None
                if existing_mapping and existing_mapping != mapping_id:
                    warnings.append(
                        f"claim {claim!r} is already registered to mapping "
                        f"{existing_mapping!r} — applying will fail with 409."
                    )

            schema: Optional[ItemsSchema] = None
            if ctx.config is not None:
                try:
                    schema = await ctx.config.get_config(
                        ItemsSchema, catalog_id=catalog_id, collection_id=collection_id,
                    )
                except Exception:
                    schema = None
            if schema is not None and schema.fields and p.column not in schema.fields:
                warnings.append(
                    f"column {p.column!r} not found in {catalog_id}/{collection_id}'s "
                    "declared schema (schema is declared but omits this field)."
                )

            try:
                collection = await ctx.catalogs.get_collection(catalog_id, collection_id)
            except Exception:
                collection = None
            raw_bbox = None
            if collection is not None and collection.extent and collection.extent.spatial:
                boxes = collection.extent.spatial.bbox or []
                raw_bbox = boxes[0] if boxes else None
            if is_degenerate_bbox(raw_bbox):
                warnings.append(
                    f"{catalog_id}/{collection_id} has no usable spatial extent — "
                    "definitions will use the world-bounds fallback "
                    "([-180, -90, 180, 90])."
                )

        return PresetPlan(
            preset_name=self.name, scope_key=scope,
            entries=tuple(entries), warnings=tuple(warnings),
        )

    async def apply(
        self, params: BaseModel, scope: str, ctx: PresetContext,
    ) -> AppliedDescriptor:
        p = _coerce_params(params)
        catalog_id, collection_id = _require_collection_scope(scope)
        if ctx.catalogs is None:
            raise RuntimeError(
                "region_mapping: PresetContext.catalogs is None — cannot "
                "register claims."
            )

        await ensure_registry_provisioned(ctx)

        mapping_id = mapping_id_for(catalog_id, collection_id)
        canonical_alias = p.alias or p.column
        title = p.title or collection_id
        region_prop = p.column

        claims = compute_claim_set(
            catalog_id=catalog_id, collection_id=collection_id,
            column=p.column, alias=p.alias, extra_aliases=p.extra_aliases,
        )

        # Stale-claim cleanup: a force re-apply with a changed alias/column
        # must not leave the previous claim set's rows behind — they would
        # squat the claim_ci UNIQUE constraint forever and keep appearing in
        # the Terria aliases array. Uncached read: must see the current
        # registry state, not a stale cache hit.
        existing_claims = await fetch_claims_for_mapping_uncached(mapping_id)
        stale_claim_ci = {
            c["claim_ci"] for c in existing_claims if c.get("claim_ci")
        } - set(claims.keys())
        for stale_ci in stale_claim_ci:
            stale_item_id = item_id_for(mapping_id, stale_ci)
            try:
                await ctx.catalogs.delete_item(
                    REGISTRY_CATALOG_ID, MAPPINGS_COLLECTION_ID, stale_item_id,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "region_mapping: failed to delete stale claim %r "
                    "(item_id=%r) for mapping %r: %s",
                    stale_ci, stale_item_id, mapping_id, exc,
                )

        item_ids: List[str] = []
        for claim_ci, (claim, role) in claims.items():
            item_id = item_id_for(mapping_id, claim_ci)
            record = {
                "type": "Feature",
                "id": item_id,
                "geometry": None,
                "properties": {
                    "claim": claim,
                    "claim_ci": claim_ci,
                    "mapping_id": mapping_id,
                    "role": role,
                    "src_catalog": catalog_id,
                    "src_collection": collection_id,
                    "region_prop": region_prop,
                    "alias": canonical_alias,
                    "title": title,
                },
            }
            # Duplicate claim_ci owned by a different mapping -> 23505 -> the
            # global exception-handler chain maps it to HTTP 409. Propagate
            # unchanged — never caught here.
            await ctx.catalogs.upsert(REGISTRY_CATALOG_ID, MAPPINGS_COLLECTION_ID, record)
            item_ids.append(item_id)

        invalidate_serving_caches()

        return AppliedDescriptor(payload={
            "preset_name": self.name,
            "mapping_id": mapping_id,
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "item_ids": item_ids,
            "scope": scope,
        })

    async def revoke(
        self, applied_descriptor: AppliedDescriptor, ctx: PresetContext,
    ) -> None:
        payload = applied_descriptor.payload
        mapping_id: Optional[str] = payload.get("mapping_id")

        # Authoritative: re-query every claim currently sharing this
        # mapping_id, rather than trusting the descriptor's recorded item
        # ids — a re-apply since this descriptor was written may have added
        # or removed claims (and lifecycle.py overwrites the stored
        # descriptor on each apply, so an older descriptor's id list can be
        # incomplete or stale).
        item_ids: List[str] = []
        if ctx.catalogs is not None and mapping_id:
            try:
                current_claims = await fetch_claims_for_mapping_uncached(mapping_id)
                item_ids = [
                    item_id_for(mapping_id, c["claim_ci"])
                    for c in current_claims if c.get("claim_ci")
                ]
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "region_mapping: revoke failed to query current claims "
                    "for mapping %r: %s", mapping_id, exc,
                )

        if not item_ids:
            # Fallback: the descriptor's own recorded ids (query above
            # returned nothing, or mapping_id was absent from an old
            # descriptor shape).
            item_ids = payload.get("item_ids", [])

        if ctx.catalogs is not None:
            for item_id in item_ids:
                try:
                    await ctx.catalogs.delete_item(
                        REGISTRY_CATALOG_ID, MAPPINGS_COLLECTION_ID, item_id,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "region_mapping: revoke delete_item %r failed: %s",
                        item_id, exc,
                    )

        invalidate_serving_caches()


REGION_MAPPING_PRESET = _RegionMappingPreset()
register_preset(REGION_MAPPING_PRESET)
