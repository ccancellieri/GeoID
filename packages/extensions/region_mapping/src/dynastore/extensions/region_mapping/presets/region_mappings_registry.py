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
must set ``CatalogLookupAudience`` at the ``_region_mappings_`` catalog's
own scope — so it talks to ``PresetContext`` directly instead.

Idempotently provisions:

1. The bucket-free ``_region_mappings_`` catalog (``Hint.DEFER``) and its
   ``mappings`` RECORDS collection (``ItemsSchema`` with a UNIQUE
   ``claim_ci`` field — see ``registry_data.py``).
2. Anonymous read, in two parts:
   a. ``CatalogLookupAudience(is_public=True)`` at the catalog's own scope
      — opens the catalog-path-shaped Records reads (item listing/search
      under ``/records/catalogs/_region_mappings_/...``).
   b. A direct platform-tier ``Policy`` (``actions=["GET"]``,
      ``resources=["/region-mappings/.*"]``) bound to the
      ``unauthenticated`` role — the ``/region-mappings/*`` serving routes
      are NOT catalog-path-shaped (the ``catalog_lookup_public_allowed``
      condition handler regex-parses ``/catalogs/{id}/`` out of the request
      path), so part (a) alone can never reach them.

Revoke removes only the direct policy and the audience config — the
catalog/collection and any claim data already registered are preserved
(mirrors the ``common_dimensions`` preset's revoke posture: shared
platform infrastructure is never deleted by an individual apply/revoke).
"""
from __future__ import annotations

import logging
from typing import Any, ClassVar, List, Optional, Tuple, Type

from pydantic import BaseModel

from dynastore.models.auth import Policy
from dynastore.models.auth_models import Role
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

from ..registry_data import (
    MAPPINGS_COLLECTION_ID,
    REGISTRY_CATALOG_ID,
    build_registry_items_schema,
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

_POLICY_ID = "region_mappings_public_read"
_ROLE_NAME = "unauthenticated"

_PUBLIC_READ_POLICY = Policy(
    id=_POLICY_ID,
    description=(
        "Anonymous GET access to the TerriaJS region-mapping serving "
        "endpoints (/region-mappings/*). These routes are not "
        "catalog-path-shaped, so CatalogLookupAudience cannot gate them — "
        "a direct platform-tier policy is required instead."
    ),
    actions=["GET"],
    resources=[r"/region-mappings/.*"],
    effect="ALLOW",
    priority=0,
)


async def _get_role_by_name(iam: Any, role_name: str) -> Optional[Role]:
    roles: List[Role] = await iam.list_roles()
    return next((r for r in roles if r.name == role_name), None)


async def _union_policy_into_role(iam: Any, role_name: str, policy_id: str) -> None:
    existing = await _get_role_by_name(iam, role_name)
    if existing is None:
        await iam.create_role(Role(name=role_name, policies=[policy_id]))
        return
    if policy_id not in existing.policies:
        merged = existing.model_copy(update={"policies": list(existing.policies) + [policy_id]})
        await iam.update_role(merged)


async def _strip_policy_from_role(iam: Any, role_name: str, policy_id: str) -> None:
    existing = await _get_role_by_name(iam, role_name)
    if existing is None:
        return
    remaining = [p for p in existing.policies if p != policy_id]
    await iam.update_role(existing.model_copy(update={"policies": remaining}))


async def ensure_registry_provisioned(ctx: PresetContext) -> None:
    """Idempotently create the registry catalog/collection and grant public
    read access.

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
            await ctx.catalogs.create_catalog(dict(_CATALOG_DATA), hints=frozenset({Hint.DEFER}))
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
            await ctx.catalogs.create_collection(REGISTRY_CATALOG_ID, collection_payload)
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
        await ctx.config.set_config(
            CatalogLookupAudience,
            CatalogLookupAudience(is_public=True),
            catalog_id=REGISTRY_CATALOG_ID,
            check_immutability=False,
        )

    if ctx.policy is not None and ctx.iam is not None:
        updated = await ctx.policy.update_policy(_PUBLIC_READ_POLICY)
        if updated is None:
            await ctx.policy.create_policy(_PUBLIC_READ_POLICY)
        await _union_policy_into_role(ctx.iam, _ROLE_NAME, _POLICY_ID)


class _RegionMappingsRegistryPreset:
    """Platform-tier preset — provisions the shared claims registry."""

    name: ClassVar[str] = "region_mappings_registry"
    description: ClassVar[str] = (
        "Provision the shared _region_mappings_ claims registry (bucket-free "
        "RECORDS catalog) and grant anonymous read to the TerriaJS "
        "region-mapping serving endpoints. Idempotent; safe to re-apply."
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
                detail={"if_absent": True, "hints": ["defer"]},
            ),
            PresetPlanEntry(
                kind="create_collection",
                target=MAPPINGS_COLLECTION_ID,
                detail={"if_absent": True, "collection_type": "RECORDS"},
            ),
            PresetPlanEntry(
                kind="set_config",
                target="CatalogLookupAudience",
                detail={"scope": f"catalog:{REGISTRY_CATALOG_ID}", "is_public": True},
            ),
            PresetPlanEntry(
                kind="upsert_policy",
                target=_POLICY_ID,
                detail={
                    "actions": _PUBLIC_READ_POLICY.actions,
                    "resources": _PUBLIC_READ_POLICY.resources,
                    "effect": _PUBLIC_READ_POLICY.effect,
                },
            ),
            PresetPlanEntry(
                kind="upsert_role_binding",
                target=_ROLE_NAME,
                detail={"add_policies": [_POLICY_ID]},
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
            "policy_id": _POLICY_ID,
            "role_name": _ROLE_NAME,
        })

    async def revoke(
        self, applied_descriptor: AppliedDescriptor, ctx: PresetContext,
    ) -> None:
        payload = applied_descriptor.payload
        role_name = payload.get("role_name", _ROLE_NAME)
        policy_id = payload.get("policy_id", _POLICY_ID)

        if ctx.iam is not None:
            await _strip_policy_from_role(ctx.iam, role_name, policy_id)
        if ctx.policy is not None:
            await ctx.policy.delete_policy(policy_id)
        if ctx.config is not None:
            await ctx.config.delete_config(CatalogLookupAudience, catalog_id=REGISTRY_CATALOG_ID)

        logger.info(
            "region_mappings_registry: revoked public-read policy %r and "
            "audience config; registry catalog/collection and claim data "
            "are preserved.",
            policy_id,
        )


REGION_MAPPINGS_REGISTRY_PRESET = _RegionMappingsRegistryPreset()
register_preset(REGION_MAPPINGS_REGISTRY_PRESET)
