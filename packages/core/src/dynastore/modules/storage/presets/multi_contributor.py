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
  - DataContributor     : ``get_data() -> Iterable[DataSeed]``
                          -> ``ctx.catalogs`` ensures each seed's catalog +
                             collection exist (created only when absent) and
                             upserts the seed items.  Revoke removes only what
                             apply created — items are always pulled back out,
                             but a catalog/collection is deleted only if THIS
                             apply created it and ``DataSeed.manage_catalog`` /
                             ``manage_collection`` allow it (never a shared or
                             pre-existing one).  A collection this preset
                             created but that now holds items added after apply
                             (e.g. members a background job materialised) is
                             left in place — revoke never deletes a non-empty
                             collection.
  - TaskContributor     : ``get_tasks() -> Iterable[TaskSeed]``
                          -> after all synchronous contributions are applied,
                             submits each ``process_id`` to the OGC Process
                             execution engine via ``ctx.db`` and records the
                             returned job id so callers can poll it.  This lets
                             a synchronous preset delegate heavy work to the
                             existing async OGC Process machinery.  Revoke does
                             NOT cancel a triggered job (it cannot be un-run);
                             data it produced is governed by the data-seed
                             revoke rules above.

The routing contributor kind (``get_routing()``) is still reserved for
future expansion — add it alongside the others when its backing
``PresetContext`` surface lands.  ``DataContributor`` requires
``PresetContext.catalogs`` (the ``CatalogsProtocol``); ``TaskContributor``
requires ``PresetContext.db`` (the engine): a preset that ships either must
run where those are available, and raises ``RuntimeError`` otherwise.

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


async def _apply_data_kind(
    preset_name: str,
    contributor: Any,
    ctx: PresetContext,
    applied_data: List[dict],
) -> None:
    """Apply a data contributor: ensure catalog + collection exist, upsert items.

    Each ``DataSeed`` is applied idempotently — the catalog and collection are
    created only when absent (existence probed via ``get_catalog_model`` /
    ``get_collection``). What this call actually created is recorded in
    ``applied_data`` so ``revoke`` can undo exactly that and nothing the
    operator owns.
    """
    if not hasattr(contributor, "get_data"):
        return
    if ctx.catalogs is None:
        raise RuntimeError(
            f"{preset_name}: data contributor present but PresetContext.catalogs "
            "is None — the catalogs service is not registered in this context."
        )
    catalogs = ctx.catalogs
    for seed in (contributor.get_data() or []):
        record: dict = {
            "catalog_id": seed.catalog_id,
            "collection_id": seed.collection_id,
            "created_catalog": False,
            "created_collection": False,
            "item_ids": [],
            "asset_ids": [],
            "manage_catalog": seed.manage_catalog,
            "manage_collection": seed.manage_collection,
        }

        # --- Catalog (create only if absent) ---
        existing_catalog = await catalogs.get_catalog_model(seed.catalog_id)
        if existing_catalog is None:
            cat_payload = dict(seed.catalog_data)
            cat_payload.setdefault("id", seed.catalog_id)
            create_kwargs = {"lang": seed.lang}
            if seed.defer_provisioning:
                from dynastore.modules.storage.hints import Hint
                create_kwargs["hints"] = frozenset({Hint.DEFER})
            await catalogs.create_catalog(cat_payload, **create_kwargs)
            record["created_catalog"] = True

        # --- Collection (create only if absent) ---
        existing_collection = await catalogs.get_collection(
            seed.catalog_id, seed.collection_id, lang=seed.lang,
        )
        if existing_collection is None:
            coll_payload = dict(seed.collection_data)
            coll_payload.setdefault("id", seed.collection_id)
            await catalogs.create_collection(
                seed.catalog_id, coll_payload, lang=seed.lang,
            )
            record["created_collection"] = True

        # --- Per-collection routing (applied before items so the upsert fans
        # out to the declared secondary indexers) ---
        # The preset itself runs at platform scope, so _apply_config_kind cannot
        # target this seed's collection. Persist the seed's routing directly at
        # its own catalog/collection scope: the write router resolves routing via
        # a collection→catalog→platform waterfall, so a collection-scoped
        # ItemsRoutingConfig (ES secondary entry, source="auto") makes the seeded
        # items searchable even under a PG-only ambient routing (#1285 / #2241).
        # Intentionally NOT best-effort (unlike the extent recompute below): if
        # routing cannot be persisted the items would land in a silently
        # non-searchable collection — the exact #2241 failure — so a failure here
        # should abort the apply loudly rather than reproduce the bug.
        if seed.items_routing is not None and ctx.config is not None:
            await ctx.config.set_config(
                config_cls=type(seed.items_routing),
                config=seed.items_routing,
                catalog_id=seed.catalog_id,
                collection_id=seed.collection_id,
                check_immutability=False,
            )

        # --- Items ---
        if seed.items:
            await catalogs.upsert(
                seed.catalog_id, seed.collection_id, list(seed.items),
            )
            record["item_ids"] = [
                it["id"] for it in seed.items if isinstance(it, dict) and "id" in it
            ]

            # Recompute the collection extent from the just-seeded items so the
            # spatial bbox and temporal interval reflect the data instead of the
            # collection's default ([[0,0,0,0]] / open interval). The recompute
            # reads from Postgres — the source of truth written just above — so it
            # is not subject to async-write/index lag. Best-effort: a failure here
            # must not fail the preset apply; the extent can be recomputed later.
            if ctx.db is not None:
                try:
                    from dynastore.modules.catalog.tools import (
                        recalculate_and_update_extents,
                    )

                    await recalculate_and_update_extents(
                        ctx.db, seed.catalog_id, seed.collection_id,
                    )
                except Exception:
                    logger.warning(
                        "%s: extent recompute failed for %s:%s after seed; the "
                        "collection extent may stay at its default until a later "
                        "recompute.",
                        preset_name, seed.catalog_id, seed.collection_id,
                        exc_info=True,
                    )

        # --- Virtual assets (href-based; no bucket needed) ---
        if seed.virtual_assets:
            from dynastore.modules.catalog.asset_service import VirtualAssetCreate
            assets_mgr = getattr(catalogs, "assets", None)
            if assets_mgr is None:
                raise RuntimeError(
                    f"{preset_name}: seed declares virtual_assets but the catalogs "
                    "service exposes no asset manager."
                )
            for spec in seed.virtual_assets:
                payload = VirtualAssetCreate(**spec)
                await assets_mgr.create_asset(
                    seed.catalog_id, payload, seed.collection_id,
                )
                record["asset_ids"].append(payload.asset_id)

        applied_data.append(record)


def _caller_id(ctx: PresetContext) -> str:
    """Resolve the caller id for task submission — the principal's id when one
    is attached, otherwise the platform system user."""
    from dynastore.models.auth_models import SYSTEM_USER_ID

    principal = ctx.principal
    if principal is not None:
        pid = getattr(principal, "id", None) or getattr(principal, "principal_id", None)
        if pid is not None:
            return str(pid)
    return SYSTEM_USER_ID


def _extract_job_id(result: Any) -> Any:
    """Pull the job/task id out of whatever ``execute_process`` returned.

    Async execution returns a ``StatusInfo`` (``jobID``) or ``Task``
    (``task_id``); be defensive across both shapes. Returns ``None`` when no
    id can be found (e.g. a dedup hit returns ``None``)."""
    if result is None:
        return None
    for attr in ("jobID", "job_id", "task_id", "id"):
        val = getattr(result, attr, None)
        if val is not None:
            return str(val)
    if isinstance(result, dict):
        for key in ("jobID", "job_id", "task_id", "id"):
            if result.get(key) is not None:
                return str(result[key])
    return None


async def _apply_task_kind(
    preset_name: str,
    contributor: Any,
    ctx: PresetContext,
    applied_tasks: List[dict],
) -> None:
    """Trigger a task contributor's background jobs.

    Each ``TaskSeed`` is submitted to the OGC Process execution engine; the
    returned job id is recorded in ``applied_tasks`` so the descriptor carries
    a pollable reference. The preset stays synchronous — it kicks the jobs off
    and returns; the heavy work runs in the background task.

    Fail-fast (``RuntimeError``) when ``ctx.db`` is ``None`` — a task
    contributor cannot submit a job without the engine.
    """
    if not hasattr(contributor, "get_tasks"):
        return
    seeds = list(contributor.get_tasks() or [])
    if not seeds:
        return
    if ctx.db is None:
        raise RuntimeError(
            f"{preset_name}: task contributor present but PresetContext.db "
            "(the engine) is None — cannot trigger background jobs."
        )

    from dynastore.modules.processes.processes_module import execute_process
    from dynastore.modules.processes import models as _proc_models

    caller_id = _caller_id(ctx)
    for seed in seeds:
        mode = (
            _proc_models.JobControlOptions.ASYNC_EXECUTE
            if getattr(seed, "async_mode", True)
            else _proc_models.JobControlOptions.SYNC_EXECUTE
        )
        exec_request = _proc_models.ExecuteRequest(inputs=dict(seed.inputs or {}))
        result = await execute_process(
            seed.process_id,
            exec_request,
            engine=ctx.db,
            caller_id=caller_id,
            preferred_mode=mode,
            dedup_key=getattr(seed, "dedup_key", None),
        )
        job_id = _extract_job_id(result)
        applied_tasks.append({
            "process_id": seed.process_id,
            "job_id": job_id,
            "deduped": result is None,
        })
        logger.info(
            "%s: triggered process %r -> job_id=%s%s",
            preset_name, seed.process_id, job_id,
            " (dedup hit, no new job scheduled)" if result is None else "",
        )


async def _revoke_task_kind(preset_name: str, task_records: List[dict]) -> None:
    """Triggered background jobs are not undone on revoke — a job that has run
    cannot be un-run, and any data it produced is governed by the data-seed
    revoke guard. Log each for audit visibility."""
    for record in task_records:
        logger.info(
            "%s: revoke does not cancel triggered job %s (process=%s); data it "
            "produced is preserved by the non-empty-collection revoke guard.",
            preset_name, record.get("job_id"), record.get("process_id"),
        )


async def _collection_has_items(
    catalogs: Any, catalog_id: str, collection_id: str,
) -> bool:
    """Best-effort emptiness probe — ``True`` if the collection still holds at
    least one item.

    Fail-safe: on any error (driver lacks ``search_items``, query blows up,
    etc.) returns ``True`` so revoke never deletes a collection it cannot
    confirm is empty."""
    try:
        from dynastore.models.query_builder import QueryRequest

        features = await catalogs.search_items(
            catalog_id, collection_id, QueryRequest(limit=1),
        )
        return bool(features)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "emptiness probe for %s/%s failed (%s) — assuming non-empty to be safe",
            catalog_id, collection_id, exc,
        )
        return True


async def _catalog_has_collections(catalogs: Any, catalog_id: str) -> bool:
    """Best-effort probe — ``True`` if the catalog still holds at least one
    collection.

    Fail-safe: on any error returns ``True`` so revoke never deletes a catalog
    it cannot confirm is empty (e.g. one whose collection it just preserved)."""
    try:
        collections = await catalogs.list_collections(catalog_id, limit=1)
        return bool(collections)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "collection probe for catalog %s failed (%s) — assuming non-empty to be safe",
            catalog_id, exc,
        )
        return True


async def _revoke_data_kind(
    preset_name: str,
    ctx: PresetContext,
    data_records: List[dict],
) -> None:
    """Undo a data contributor's seeds — items first, then collection, then
    catalog, but only the catalog/collection rows THIS preset created and is
    allowed to manage.  Reverse-iterates so later seeds unwind before earlier
    ones (e.g. the seed that created a shared catalog is processed last)."""
    if ctx.catalogs is None:
        logger.warning(
            "%s: cannot revoke data seeds — PresetContext.catalogs is None", preset_name,
        )
        return
    catalogs = ctx.catalogs
    for record in reversed(data_records):
        catalog_id = record["catalog_id"]
        collection_id = record["collection_id"]

        # --- Virtual assets (delete before items/collection) ---
        assets_mgr = getattr(catalogs, "assets", None)
        for asset_id in record.get("asset_ids", []):
            if assets_mgr is None:
                break
            try:
                await assets_mgr.delete_asset(asset_id, catalog_id, collection_id)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "%s: delete_asset %s/%s/%s failed: %s",
                    preset_name, catalog_id, collection_id, asset_id, exc,
                )

        # --- Items ---
        for item_id in record.get("item_ids", []):
            try:
                await catalogs.delete_item(catalog_id, collection_id, item_id)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "%s: delete_item %s/%s/%s failed: %s",
                    preset_name, catalog_id, collection_id, item_id, exc,
                )

        # --- Collection (only if we created it, may manage it, and it is
        #     empty — never delete a collection that still holds items added
        #     after apply, e.g. members a background job materialised) ---
        if record.get("created_collection") and record.get("manage_collection", True):
            if await _collection_has_items(catalogs, catalog_id, collection_id):
                logger.warning(
                    "%s: collection %s/%s was created by this preset but still "
                    "holds items added after apply — leaving it in place "
                    "instead of deleting.",
                    preset_name, catalog_id, collection_id,
                )
            else:
                try:
                    await catalogs.delete_collection(catalog_id, collection_id, force=True)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "%s: delete_collection %s/%s failed: %s",
                        preset_name, catalog_id, collection_id, exc,
                    )

        # --- Catalog (only if we created it, may manage it, and it no longer
        #     holds any collection — never delete a catalog whose collection we
        #     just preserved, or one the operator has added collections to) ---
        if record.get("created_catalog") and record.get("manage_catalog", True):
            if await _catalog_has_collections(catalogs, catalog_id):
                logger.warning(
                    "%s: catalog %s was created by this preset but still holds "
                    "collections — leaving it in place instead of deleting.",
                    preset_name, catalog_id,
                )
            else:
                try:
                    await catalogs.delete_catalog(catalog_id, force=True)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "%s: delete_catalog %s failed: %s", preset_name, catalog_id, exc,
                    )


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
            if hasattr(contributor, "get_data"):
                for seed in (contributor.get_data() or []):
                    entries.append(PresetPlanEntry(
                        kind="seed_data",
                        target=f"{seed.catalog_id}/{seed.collection_id}",
                        detail={"items": len(seed.items)},
                    ))
            if hasattr(contributor, "get_tasks"):
                for tseed in (contributor.get_tasks() or []):
                    entries.append(PresetPlanEntry(
                        kind="trigger_task",
                        target=tseed.process_id,
                        detail={
                            "async": getattr(tseed, "async_mode", True),
                            "inputs": dict(tseed.inputs or {}),
                        },
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
        applied_data: List[dict] = []
        applied_tasks: List[dict] = []

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
            await _apply_data_kind(
                self.name, contributor, ctx, applied_data,
            )
            # Tasks last: every synchronous contribution (incl. the skeleton
            # collections a job will fill) is in place before the job fires.
            await _apply_task_kind(
                self.name, contributor, ctx, applied_tasks,
            )

        return AppliedDescriptor(payload={
            "preset_name": self.name,
            "policy_ids": applied_policy_ids,
            "role_names": applied_role_names,
            "config_qualnames": applied_config_qualnames,
            "data": applied_data,
            "tasks": applied_tasks,
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
        data_records: List[dict] = payload.get("data", [])
        task_records: List[dict] = payload.get("tasks", [])
        scope: str = payload.get("scope", "platform")

        # Reverse the apply order so unwinding mirrors application:
        # tasks first (no-op log), then data, configs, role bindings, policies.

        # --- Tasks (triggered jobs are not undone) ---
        await _revoke_task_kind(self.name, task_records)

        # --- Data seeds ---
        await _revoke_data_kind(self.name, ctx, data_records)

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
