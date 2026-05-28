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

"""Composite preset that chains contributors of multiple kinds.

A preset is a set of platform changes — config, permissions, routing,
data — applied as a single reversible unit.  ``MultiContributorPreset``
takes a tuple of contributor objects, each of which may expose one or
more contributor-kind methods.  At apply time the preset routes each
kind through the appropriate ``PresetContext`` subsystem; revoke
unwinds in reverse order.

Supported contributor kinds (a single contributor may expose any
subset — only the methods present are invoked):

  - PolicyContributor   : ``get_policies()``, ``get_role_bindings()``
                          -> ``ctx.policy.update_policy`` +
                             ``ctx.iam.bind_policy_to_role``
  - ConfigContributor   : ``get_configs()``
                          -> ``ctx.config.set_config(type(c), c, scope...)``

Routing / data contributor kinds are reserved for future expansion —
add the new ``get_routing()`` / ``get_data()`` branches alongside the
existing two when their backing ``PresetContext`` surface lands.

``PolicyContributorPreset`` (single-policy adapter, predates this
module) remains the simpler tool for IAM-only presets.  Use
``MultiContributorPreset`` whenever a preset needs to ship more than
just IAM contributions.

Usage::

    class _XPolicyContributor:
        def get_policies(self):    return [Policy(id="x_access", ...)]
        def get_role_bindings(self): return [Role(name="admin", policies=["x_access"])]

    class _XConfigContributor:
        def get_configs(self):
            return [XPluginConfig(retention_days=30)]

    register_preset(MultiContributorPreset(
        name="x_enable",
        description="X extension policies + default retention config",
        keywords=("iam", "x", "platform"),
        contributors_factory=lambda: [
            _XPolicyContributor(),
            _XConfigContributor(),
        ],
    ))
"""
from __future__ import annotations

import logging
from typing import Any, Callable, ClassVar, Iterable, List, Tuple, Type

from pydantic import BaseModel

from dynastore.models.auth_models import Role

from .preset import (
    AppliedDescriptor,
    NoParams,
    PresetContext,
    PresetPlan,
    PresetPlanEntry,
)
from .protocol import PresetTier

logger = logging.getLogger(__name__)

# Shared role names whose rows must never be deleted by a per-extension
# revoke — only the policy links contributed by THIS preset are stripped.
# Mirrors ``policy_contributor_adapter._SHARED_ROLE_NAMES``.
_SHARED_ROLE_NAMES: Tuple[str, ...] = (
    "sysadmin",
    "admin",
    "anonymous",
    "unauthenticated",
)


# ---------------------------------------------------------------------------
# Catalogues of contributor kinds (each iterates the contributor and
# applies its records via the relevant ctx subsystem)
# ---------------------------------------------------------------------------

async def _apply_policy_kind(
    preset_name: str,
    contributor: Any,
    ctx: PresetContext,
    applied_policy_ids: List[str],
) -> None:
    if not hasattr(contributor, "get_policies"):
        return
    for pol in (contributor.get_policies() or []):
        await ctx.policy.update_policy(pol)
        applied_policy_ids.append(pol.id)


async def _apply_role_binding_kind(
    preset_name: str,
    contributor: Any,
    ctx: PresetContext,
    applied_role_names: List[str],
) -> None:
    if not hasattr(contributor, "get_role_bindings"):
        return
    for role in (contributor.get_role_bindings() or []):
        # Mirror PolicyContributorPreset: create-then-bind.  Shared roles
        # raise ValueError on create which we swallow; their existing
        # policy list is preserved by ``bind_policy_to_role``.
        try:
            await ctx.iam.create_role(Role(name=role.name, policies=[]))
        except ValueError:
            pass
        for pid in (role.policies or []):
            await ctx.iam.bind_policy_to_role(role.name, {"id": pid})
        applied_role_names.append(role.name)


async def _apply_config_kind(
    preset_name: str,
    contributor: Any,
    ctx: PresetContext,
    scope: str,
    applied_config_qualnames: List[str],
) -> None:
    if not hasattr(contributor, "get_configs"):
        return
    catalog_id, collection_id = _scope_to_catalog_collection(scope)
    for cfg in (contributor.get_configs() or []):
        cfg_cls = type(cfg)
        qualname = f"{cfg_cls.__module__}.{cfg_cls.__qualname__}"
        await ctx.config.set_config(
            config_cls=cfg_cls,
            config=cfg,
            catalog_id=catalog_id,
            collection_id=collection_id,
            check_immutability=False,
        )
        applied_config_qualnames.append(qualname)


def _resolve_config_class(qualname: str) -> Any:
    """Import a config class by its ``module.QualName`` string.

    Returns ``None`` if the class cannot be resolved (e.g. the module was
    deleted or renamed since apply time). The caller logs and skips.
    """
    import importlib
    if "." not in qualname:
        return None
    mod_path, _, cls_name = qualname.rpartition(".")
    try:
        mod = importlib.import_module(mod_path)
    except Exception:  # noqa: BLE001
        return None
    cls: Any = mod
    for part in cls_name.split("."):
        cls = getattr(cls, part, None)
        if cls is None:
            return None
    return cls


def _scope_to_catalog_collection(scope: str) -> Tuple[Any, Any]:
    """Translate a preset ``scope`` string into ``(catalog_id, collection_id)``.

    Returns ``(None, None)`` for the platform scope so ``set_config``
    writes at the platform tier.
    """
    if scope == "platform" or not scope:
        return None, None
    # Format: ``catalog:cat-7`` or ``catalog:cat-7/collection:coll-3``.
    catalog_id = None
    collection_id = None
    for part in scope.split("/"):
        if part.startswith("catalog:"):
            catalog_id = part.split(":", 1)[1]
        elif part.startswith("collection:"):
            collection_id = part.split(":", 1)[1]
    return catalog_id, collection_id


# ---------------------------------------------------------------------------
# Composite preset
# ---------------------------------------------------------------------------

class MultiContributorPreset:
    """Compose multiple contributors as a single reversible preset.

    ``contributors_factory`` is invoked on every ``apply`` / ``revoke``
    / ``dry_run`` call — no state is captured at registration time, no
    circular imports happen at module load.

    Each returned contributor may expose any subset of the supported
    contributor-kind methods (see module docstring).
    """

    tier: ClassVar[PresetTier] = PresetTier.PLATFORM
    catalog_scopable: ClassVar[bool] = False
    params_model: ClassVar[Type[BaseModel]] = NoParams
    is_async: ClassVar[bool] = False

    def __init__(
        self,
        *,
        name: str,
        description: str,
        keywords: Tuple[str, ...],
        contributors_factory: Callable[[], Iterable[Any]],
    ) -> None:
        self.name = name
        self.description = description
        self.keywords = keywords
        self._contributors_factory = contributors_factory

    # ------------------------------------------------------------------
    # Preset methods
    # ------------------------------------------------------------------

    async def dry_run(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> PresetPlan:
        entries: List[PresetPlanEntry] = []
        for contributor in self._contributors_factory():
            if hasattr(contributor, "get_policies"):
                for pol in (contributor.get_policies() or []):
                    entries.append(PresetPlanEntry(
                        kind="upsert_policy",
                        target=pol.id,
                        detail={"effect": pol.effect, "actions": pol.actions},
                    ))
            if hasattr(contributor, "get_role_bindings"):
                for role in (contributor.get_role_bindings() or []):
                    entries.append(PresetPlanEntry(
                        kind="upsert_role_binding",
                        target=role.name,
                        detail={"policies": role.policies},
                    ))
            if hasattr(contributor, "get_configs"):
                for cfg in (contributor.get_configs() or []):
                    cfg_cls = type(cfg)
                    qualname = f"{cfg_cls.__module__}.{cfg_cls.__qualname__}"
                    entries.append(PresetPlanEntry(
                        kind="upsert_config",
                        target=qualname,
                        detail={"scope": scope},
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
        applied_policy_ids: List[str] = []
        applied_role_names: List[str] = []
        applied_config_qualnames: List[str] = []

        for contributor in self._contributors_factory():
            await _apply_policy_kind(
                self.name, contributor, ctx, applied_policy_ids,
            )
            await _apply_role_binding_kind(
                self.name, contributor, ctx, applied_role_names,
            )
            await _apply_config_kind(
                self.name, contributor, ctx, scope, applied_config_qualnames,
            )

        return AppliedDescriptor(payload={
            "preset_name": self.name,
            "policy_ids": applied_policy_ids,
            "role_names": applied_role_names,
            "config_qualnames": applied_config_qualnames,
            "scope": scope,
        })

    async def revoke(
        self,
        applied_descriptor: AppliedDescriptor,
        ctx: PresetContext,
    ) -> None:
        payload = applied_descriptor.payload
        policy_ids: List[str] = payload.get("policy_ids", [])
        role_names: List[str] = payload.get("role_names", [])
        config_qualnames: List[str] = payload.get("config_qualnames", [])
        scope: str = payload.get("scope", "platform")

        # Reverse the apply order so unwinding mirrors application:
        # configs first, then role bindings, then policies.

        # --- Configs ---
        catalog_id, collection_id = _scope_to_catalog_collection(scope)
        for qualname in config_qualnames:
            cfg_cls = _resolve_config_class(qualname)
            if cfg_cls is None:
                logger.warning(
                    "%s: cannot resolve config class %s for revoke — skipping",
                    self.name, qualname,
                )
                continue
            try:
                await ctx.config.delete_config(
                    config_cls=cfg_cls,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "%s: delete_config %s at scope %r failed: %s",
                    self.name, qualname, scope, exc,
                )

        # --- Role bindings ---
        for rname in role_names:
            if rname in _SHARED_ROLE_NAMES:
                for pid in policy_ids:
                    try:
                        await ctx.iam.unbind_policy_from_role(rname, pid)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            "%s: unbind policy %s from shared role %s failed: %s",
                            self.name, pid, rname, exc,
                        )
            else:
                try:
                    await ctx.iam.delete_role(rname)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "%s: delete_role %s failed: %s",
                        self.name, rname, exc,
                    )

        # --- Policies ---
        for pid in policy_ids:
            try:
                await ctx.policy.delete_policy(pid)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "%s: delete_policy %s failed: %s",
                    self.name, pid, exc,
                )

        return None
