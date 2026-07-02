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

"""``region_mappings_registry`` preset — provisions the shared region-mapping
claims registry (dynastore#443 Phase 1).

Hand-rolled (not ``MultiContributorPreset``): a ``ConfigContributor`` only
applies configs at the preset's own scope, but this platform-tier preset
must set configs at the ``_region_mappings_`` catalog's own scope — so it
talks to ``PresetContext`` directly instead.

Idempotently provisions:

1. The bucket-free ``_region_mappings_`` catalog (``Hint.DEFER``) and its
   ``mappings`` RECORDS collection (``ItemsSchema`` with a UNIQUE
   ``claim_ci`` field — see ``registry_data.py``). Both creates pass
   ``lang="*"`` — the title/description payloads are multilanguage dicts, and
   ``create_catalog``/``create_collection`` reject a multilanguage dict input
   when a concrete ``lang`` (the "en" default) is requested (mirrors the
   live-proven pattern in
   ``dynastore.extensions.volumes.presets.tiles3d_samples``).
2. PG-only routing on the catalog, collection, and items tiers — no
   Elasticsearch / secondary index anywhere for this registry (mirrors
   ``dynastore.modules.storage.presets.pg_only_catalog``'s routing shape).
   The registry is exclusively a small claims table; every read the
   extension performs (claim lookups, the CQL-style ``claim_ci=`` conflict
   check, ``mapping_id=`` grouping) goes through the PG driver only.
3. ``CatalogLookupAudience(is_public=True)`` at the catalog's own scope —
   opens the catalog-path-shaped Records reads (item listing/search under
   ``/records/catalogs/_region_mappings_/...``) to anonymous callers.

No IAM policy is registered here. On a deployment without the IAM module
(e.g. dev's ``scope_catalog``, which ships without it) all routes are open
by default, so nothing needs granting. On an IAM-enabled deployment the
operator is expected to grant read access to ``/region-mappings/.*`` and
``/records/catalogs/_region_mappings_/.*`` explicitly, the same way they
grant any other protected route — this preset does not assume or automate
that decision.

Revoke removes only the audience config — the catalog/collection and any
claim data already registered are preserved (mirrors the
``common_dimensions`` preset's revoke posture: shared platform
infrastructure is never deleted by an individual apply/revoke).
"""
from __future__ import annotations

import logging
from typing import ClassVar, Tuple, Type

from pydantic import BaseModel

from dynastore.modules.db_config.exceptions import UniqueViolationError
from dynastore.modules.iam.audience_configs import CatalogLookupAudience
from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    NoParams,
    PresetContext,
    PresetPlan,
    PresetPlanEntry,
)
from dynastore.modules.storage.presets.protocol import PresetTier
from dynastore.modules.storage.presets.registry import register_preset
from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    ItemsRoutingConfig,
)

from ..registry_data import (
    MAPPINGS_COLLECTION_ID,
    REGISTRY_CATALOG_ID,
    build_registry_items_schema,
    build_registry_routing_configs,
)

logger = logging.getLogger(__name__)

_CATALOG_DATA = {
    "id": REGISTRY_CATALOG_ID,
    "title": {"en": "Region Mapping Claims Registry"},
    "description": {
        "en": (
            "Registry of TerriaJS WMS region-mapping claims — alias strings "
            "that map onto a source collection's region-id property. Backs "
            "the /region-mappings/definitions and "
            "/region-mappings/{mapping_id}/regionIds serving endpoints."
        ),
    },
    "keywords": ["region-mapping", "terria", "wms", "records"],
}
_COLLECTION_DATA = {
    "id": MAPPINGS_COLLECTION_ID,
    "title": {"en": "Region Mapping Claims"},
}


async def ensure_registry_provisioned(ctx: PresetContext) -> None:
    """Idempotently create the registry catalog/collection, pin PG-only
    routing, and grant the catalog-path-shaped Records reads to anonymous
    callers.

    Shared by this preset's ``apply()`` and by the collection-scoped
    ``region_mapping`` preset, which must guarantee the registry exists
    before writing claims into it.
    """
    if ctx.catalogs is None:
        raise RuntimeError(
            "region_mappings_registry: PresetContext.catalogs is None — "
            "cannot provision the registry."
        )

    # Check-then-act race: two first-time collection applies can both see
    # the registry as absent and both attempt to create it — the apply lock
    # is per-collection scope_key, not the shared registry, so nothing
    # serializes them. Catch the loser's UniqueViolationError, re-fetch, and
    # continue against the winner's catalog/collection rather than failing
    # the apply.
    existing_catalog = await ctx.catalogs.get_catalog_model(REGISTRY_CATALOG_ID)
    if existing_catalog is None:
        try:
            await ctx.catalogs.create_catalog(
                dict(_CATALOG_DATA), lang="*", hints=frozenset({Hint.DEFER}),
            )
        except UniqueViolationError:
            logger.info(
                "region_mappings_registry: %r was created concurrently by "
                "another apply — re-fetching and continuing.",
                REGISTRY_CATALOG_ID,
            )
            existing_catalog = await ctx.catalogs.get_catalog_model(REGISTRY_CATALOG_ID)
            if existing_catalog is None:
                raise

    existing_collection = await ctx.catalogs.get_collection(
        REGISTRY_CATALOG_ID, MAPPINGS_COLLECTION_ID,
    )
    if existing_collection is None:
        collection_payload = dict(_COLLECTION_DATA)
        collection_payload["layer_config"] = {"collection_type": "RECORDS"}
        collection_payload["schema"] = build_registry_items_schema()
        try:
            await ctx.catalogs.create_collection(
                REGISTRY_CATALOG_ID, collection_payload, lang="*",
            )
        except UniqueViolationError:
            logger.info(
                "region_mappings_registry: %r/%r was created concurrently "
                "by another apply — re-fetching and continuing.",
                REGISTRY_CATALOG_ID, MAPPINGS_COLLECTION_ID,
            )
            existing_collection = await ctx.catalogs.get_collection(
                REGISTRY_CATALOG_ID, MAPPINGS_COLLECTION_ID,
            )
            if existing_collection is None:
                raise

    if ctx.config is not None:
        # PG-only routing — no ES/secondary index anywhere for this
        # registry (re-asserted on every apply; cheap and self-healing
        # against a platform-level default that might otherwise inject an
        # ES driver).
        catalog_routing, collection_routing, items_routing = build_registry_routing_configs()
        await ctx.config.set_config(
            CatalogRoutingConfig, catalog_routing,
            catalog_id=REGISTRY_CATALOG_ID, check_immutability=False,
        )
        await ctx.config.set_config(
            CollectionRoutingConfig, collection_routing,
            catalog_id=REGISTRY_CATALOG_ID, collection_id=MAPPINGS_COLLECTION_ID,
            check_immutability=False,
        )
        await ctx.config.set_config(
            ItemsRoutingConfig, items_routing,
            catalog_id=REGISTRY_CATALOG_ID, collection_id=MAPPINGS_COLLECTION_ID,
            check_immutability=False,
        )

        await ctx.config.set_config(
            CatalogLookupAudience,
            CatalogLookupAudience(is_public=True),
            catalog_id=REGISTRY_CATALOG_ID,
            check_immutability=False,
        )


class _RegionMappingsRegistryPreset:
    """Platform-tier preset — provisions the shared claims registry."""

    name: ClassVar[str] = "region_mappings_registry"
    description: ClassVar[str] = (
        "Provision the shared _region_mappings_ claims registry (bucket-free, "
        "PG-only RECORDS catalog) and open its catalog-path-shaped Records "
        "reads to anonymous callers. Idempotent; safe to re-apply."
    )
    keywords: ClassVar[Tuple[str, ...]] = ("region-mapping", "terria", "platform", "records")
    tier: ClassVar[PresetTier] = PresetTier.PLATFORM
    catalog_scopable: ClassVar[bool] = False
    params_model: ClassVar[Type[BaseModel]] = NoParams

    async def dry_run(
        self, params: BaseModel, scope: str, ctx: PresetContext,
    ) -> PresetPlan:
        entries = [
            PresetPlanEntry(
                kind="create_catalog",
                target=REGISTRY_CATALOG_ID,
                detail={"if_absent": True, "hints": ["defer"], "lang": "*"},
            ),
            PresetPlanEntry(
                kind="create_collection",
                target=MAPPINGS_COLLECTION_ID,
                detail={"if_absent": True, "collection_type": "RECORDS", "lang": "*"},
            ),
            PresetPlanEntry(
                kind="set_config",
                target="CatalogRoutingConfig",
                detail={"scope": f"catalog:{REGISTRY_CATALOG_ID}", "pg_only": True},
            ),
            PresetPlanEntry(
                kind="set_config",
                target="CollectionRoutingConfig",
                detail={
                    "scope": f"catalog:{REGISTRY_CATALOG_ID}/collection:{MAPPINGS_COLLECTION_ID}",
                    "pg_only": True,
                },
            ),
            PresetPlanEntry(
                kind="set_config",
                target="ItemsRoutingConfig",
                detail={
                    "scope": f"catalog:{REGISTRY_CATALOG_ID}/collection:{MAPPINGS_COLLECTION_ID}",
                    "pg_only": True,
                },
            ),
            PresetPlanEntry(
                kind="set_config",
                target="CatalogLookupAudience",
                detail={"scope": f"catalog:{REGISTRY_CATALOG_ID}", "is_public": True},
            ),
        ]
        return PresetPlan(preset_name=self.name, scope_key=scope, entries=tuple(entries))

    async def apply(
        self, params: BaseModel, scope: str, ctx: PresetContext,
    ) -> AppliedDescriptor:
        await ensure_registry_provisioned(ctx)
        return AppliedDescriptor(payload={
            "preset_name": self.name,
            "catalog_id": REGISTRY_CATALOG_ID,
            "collection_id": MAPPINGS_COLLECTION_ID,
        })

    async def revoke(
        self, applied_descriptor: AppliedDescriptor, ctx: PresetContext,
    ) -> None:
        if ctx.config is not None:
            await ctx.config.delete_config(CatalogLookupAudience, catalog_id=REGISTRY_CATALOG_ID)

        logger.info(
            "region_mappings_registry: revoked the anonymous-read audience "
            "config; PG-only routing, registry catalog/collection, and "
            "claim data are preserved.",
        )


REGION_MAPPINGS_REGISTRY_PRESET = _RegionMappingsRegistryPreset()
register_preset(REGION_MAPPINGS_REGISTRY_PRESET)
