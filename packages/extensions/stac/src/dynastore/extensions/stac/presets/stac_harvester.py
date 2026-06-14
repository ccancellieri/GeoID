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

"""``stac_harvester`` preset — harvest a remote STAC catalog by URL.

Applies at catalog scope; submits a ``stac_harvest`` OGC Process job
asynchronously and returns immediately.  The job walks the source catalog
(``url``), maps collections and items, and bulk-upserts them into the
target dynastore catalog.  Re-applying is idempotent — all upserts keyed
on STAC ``id``.

Revoke note: harvested items are NOT auto-deleted.  Revoke is a no-op
because a harvest cannot be undone deterministically (items may have been
enriched or referenced by downstream workflows after harvest).
Re-applying re-syncs idempotently.
"""
from __future__ import annotations

import logging
from typing import Any, ClassVar, Literal, Optional, Tuple, Type

from pydantic import BaseModel, Field, field_validator

from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    PresetContext,
    PresetPlan,
    PresetPlanEntry,
)
from dynastore.modules.storage.presets.protocol import PresetTier

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Params model
# ---------------------------------------------------------------------------


class StacHarvesterParams(BaseModel):
    """Parameters for the ``stac_harvester`` preset."""

    url: str = Field(
        ...,
        description=(
            "Base URL of the remote STAC catalog to harvest — must expose "
            "/collections and /collections/{id}/items."
        ),
    )
    target_catalog: Optional[str] = Field(
        default=None,
        description=(
            "ID of the local dynastore catalog to write into.  "
            "Defaults to the catalog the preset is applied on when the "
            "scope is catalog-scoped."
        ),
    )
    max_collections: int = Field(
        default=0,
        ge=0,
        description="Maximum number of source collections to harvest (0 = all).",
    )
    max_items: int = Field(
        default=0,
        ge=0,
        description="Maximum number of items per collection to harvest (0 = all).",
    )
    with_assets: bool = Field(
        default=True,
        description=(
            "When True, register each item asset href as a virtual asset "
            "(dynastore stores only the href, never the bytes)."
        ),
    )
    storage_backend: Literal["es", "es_pg", "pg"] = Field(
        default="es",
        description=(
            "Item storage backend for this harvest.  "
            "``es`` routes item WRITE and READ directly to Elasticsearch so "
            "harvested items are immediately searchable without waiting for the "
            "async ES-index drain.  ``es_pg`` writes to PG primary with async "
            "ES secondary index.  ``pg`` uses PG only."
        ),
    )

    @field_validator("url")
    @classmethod
    def _validate_url(cls, v: str) -> str:
        if not v.startswith("https://") and not v.startswith("http://"):
            raise ValueError("url must start with http:// or https://")
        return v.rstrip("/")


# ---------------------------------------------------------------------------
# Preset helpers
# ---------------------------------------------------------------------------


def _catalog_id_from_scope(scope: str) -> Optional[str]:
    """Extract the catalog id from a ``catalog:<id>`` scope string."""
    for part in (scope or "").split("/"):
        if part.startswith("catalog:"):
            return part.split(":", 1)[1]
    return None


# ---------------------------------------------------------------------------
# Preset class
# ---------------------------------------------------------------------------


class _StacHarvesterPreset:
    """Preset that kicks off a ``stac_harvest`` OGC Process job.

    Tier: CATALOG.  Apply endpoint:
        POST /configs/catalogs/{catalog_id}/presets/stac_harvester
        Body: {"url": "https://...", "max_collections": 0, "max_items": 0,
               "with_assets": true, "storage_backend": "es"}

    Mechanism: ``apply()`` resolves the target catalog id (from params or
    from the scope), builds the ``StacHarvestRequest`` inputs dict, and
    submits the ``stac_harvest`` process to the OGC execution engine via
    ``execute_process``.  The job runs in the background; ``apply`` records
    the returned job id in the ``AppliedDescriptor`` so callers can poll
    ``GET /jobs/{job_id}`` for progress.

    Revoke: a no-op.  Harvested items are not auto-deleted because a
    harvest cannot be undone safely — items may have been enriched or
    referenced by downstream workflows after ingestion.  Re-applying
    re-syncs idempotently (all upserts are keyed on STAC ``id``).
    """

    name: ClassVar[str] = "stac_harvester"
    description: ClassVar[str] = (
        "Harvest a remote STAC catalog by URL into the current dynastore catalog. "
        "Submits an async stac_harvest job; collections and items are upserted "
        "idempotently.  Re-applying re-syncs without duplicates."
    )
    keywords: ClassVar[Tuple[str, ...]] = ("stac", "harvest", "ingest", "catalog")
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    catalog_scopable: ClassVar[bool] = True
    params_model: ClassVar[Type[BaseModel]] = StacHarvesterParams

    async def dry_run(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> PresetPlan:
        p = params if isinstance(params, StacHarvesterParams) else StacHarvesterParams.model_validate(params.model_dump() if hasattr(params, "model_dump") else {})
        target = p.target_catalog or _catalog_id_from_scope(scope) or "<scope-catalog>"
        return PresetPlan(
            preset_name=self.name,
            scope_key=scope,
            entries=(
                PresetPlanEntry(
                    kind="trigger_task",
                    target="stac_harvest",
                    detail={
                        "async": True,
                        "inputs": {
                            "catalog_url": p.url,
                            "target_catalog": target,
                            "max_collections": p.max_collections,
                            "max_items": p.max_items,
                            "with_assets": p.with_assets,
                            "storage_backend": p.storage_backend,
                        },
                    },
                ),
            ),
        )

    async def apply(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> AppliedDescriptor:
        p = params if isinstance(params, StacHarvesterParams) else StacHarvesterParams.model_validate(params.model_dump() if hasattr(params, "model_dump") else {})

        # Resolve the target catalog: explicit param > scope-derived id.
        target_catalog = p.target_catalog or _catalog_id_from_scope(scope)
        if not target_catalog:
            raise ValueError(
                "stac_harvester: cannot determine target_catalog — either set "
                "params.target_catalog or apply at catalog scope."
            )

        if ctx.db is None:
            raise RuntimeError(
                "stac_harvester: PresetContext.db (the engine) is None — "
                "cannot submit the stac_harvest job without the engine."
            )

        from dynastore.modules.processes.processes_module import execute_process
        from dynastore.modules.processes import models as _proc_models
        from dynastore.models.auth_models import SYSTEM_USER_ID

        inputs: dict[str, Any] = {
            "catalog_url": p.url,
            "target_catalog": target_catalog,
            "max_collections": p.max_collections,
            "max_items": p.max_items,
            "with_assets": p.with_assets,
            "storage_backend": p.storage_backend,
        }

        exec_request = _proc_models.ExecuteRequest(inputs=inputs)

        principal = ctx.principal
        caller_id: str = SYSTEM_USER_ID
        if principal is not None:
            pid = getattr(principal, "id", None) or getattr(principal, "principal_id", None)
            if pid is not None:
                caller_id = str(pid)

        result = await execute_process(
            "stac_harvest",
            exec_request,
            engine=ctx.db,
            caller_id=caller_id,
            preferred_mode=_proc_models.JobControlOptions.ASYNC_EXECUTE,
            dedup_key=None,
        )

        job_id: Optional[str] = None
        if result is not None:
            for attr in ("jobID", "job_id", "task_id", "id"):
                val = getattr(result, attr, None)
                if val is not None:
                    job_id = str(val)
                    break
            if job_id is None and isinstance(result, dict):
                for key in ("jobID", "job_id", "task_id", "id"):
                    if result.get(key) is not None:
                        job_id = str(result[key])
                        break

        logger.info(
            "stac_harvester: submitted stac_harvest job for catalog_url=%r "
            "target_catalog=%r -> job_id=%s",
            p.url, target_catalog, job_id,
        )

        return AppliedDescriptor(payload={
            "preset_name": self.name,
            "catalog_url": p.url,
            "target_catalog": target_catalog,
            "job_id": job_id,
            "scope": scope,
        })

    async def revoke(
        self,
        applied_descriptor: AppliedDescriptor,
        ctx: PresetContext,
    ) -> None:
        """No-op: harvested items are not auto-deleted.

        A harvest cannot be undone deterministically — items may have been
        enriched or referenced by downstream workflows after ingestion.
        Re-applying re-syncs idempotently.  The job id recorded in the
        descriptor remains queryable via GET /jobs/{job_id}.
        """
        payload = applied_descriptor.payload
        logger.info(
            "stac_harvester: revoke called for catalog_url=%r target_catalog=%r "
            "job_id=%s — harvested items are preserved (harvest is not reversible; "
            "re-apply to re-sync).",
            payload.get("catalog_url"),
            payload.get("target_catalog"),
            payload.get("job_id"),
        )


# ---------------------------------------------------------------------------
# Preset instance + registration
# ---------------------------------------------------------------------------

STAC_HARVESTER_PRESET = _StacHarvesterPreset()

from dynastore.modules.storage.presets.registry import register_preset as _register_preset  # noqa: E402

_register_preset(STAC_HARVESTER_PRESET)
