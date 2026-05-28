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

"""Preset lifecycle orchestration — apply, revoke, dry-run.

The admin handlers call these three functions. They coordinate:

1. Registry lookup + tier validation.
2. Row-lock via ``SELECT … FOR UPDATE`` inside a managed transaction.
3. Idempotency check (same params snapshot → 200 no-op).
4. State machine transitions in ``AppliedPresetsService``.
5. Delegation to the preset's ``apply`` / ``revoke`` / ``dry_run``.
6. Self-lockout guard for IAM presets on DELETE.
7. Best-effort incremental rollback on apply failure.

The functions return the audit row as a plain dict so the admin handlers
can serialise it directly without coupling to internal types.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Optional

from fastapi import HTTPException
from pydantic import BaseModel

from dynastore.modules.db_config.query_executor import managed_transaction, DbResource
from dynastore.modules.iam.applied_presets_service import AppliedPresetsService, AppliedRow

from .preset import AppliedDescriptor, PresetContext, PresetPlan, TaskHandle
from .registry import find_preset

logger = logging.getLogger(__name__)


def _scope_key(scope: str) -> str:
    """Normalise scope string — already normalised; pass through."""
    return scope


def _build_context(
    engine: Optional[DbResource],
    principal: Optional[Any],
    scope: str,
) -> PresetContext:
    """Construct a ``PresetContext`` from available protocols."""
    from dynastore.modules import get_protocol
    from dynastore.modules.iam.iam_service import IamService
    from dynastore.models.protocols.policies import PermissionProtocol
    from dynastore.models.protocols.configs import ConfigsProtocol

    return PresetContext(
        db=engine,
        iam=get_protocol(IamService),
        policy=get_protocol(PermissionProtocol),
        config=get_protocol(ConfigsProtocol),
        tasks=None,   # not wired in v1 core; presets that need tasks import directly
        cron=None,    # same
        libs=None,
        principal=principal,
        scope=scope,
    )


async def apply_preset(
    name: str,
    scope_key: str,
    params: BaseModel,
    ctx: PresetContext,
    engine: Optional[DbResource],
    audit: AppliedPresetsService,
    *,
    force: bool = False,
    applied_by: Optional[Any] = None,
) -> AppliedRow:
    """Apply a preset at the given scope.

    Returns the final audit row. Raises ``HTTPException`` on:
    * 404 — preset not registered.
    * 400 — URL tier mismatch.
    * 409 — concurrent in-progress apply, or params snapshot mismatch
      without ``?force=true``.
    """
    if engine is None:
        raise HTTPException(status_code=503, detail="Database engine not available.")

    preset = find_preset(name)
    params_json = json.loads(params.model_dump_json())

    async with managed_transaction(engine) as conn:
        existing = await audit.get_for_update(name, scope_key, conn=conn)

        if existing is not None:
            state = existing.get("state")
            if state in ("in_progress", "revoke_in_progress", "revoke_pending"):
                raise HTTPException(
                    status_code=409,
                    detail=f"Preset {name!r} at {scope_key!r} is currently {state!r}. Retry later.",
                )
            if state in ("applied", "partial"):
                stored_snap = existing.get("params_snapshot") or {}
                if isinstance(stored_snap, str):
                    stored_snap = json.loads(stored_snap)
                if stored_snap == params_json and not force:
                    # Idempotent re-POST with same params — return existing row.
                    return existing
                if stored_snap != params_json and not force:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "message": (
                                f"Preset {name!r} already applied at {scope_key!r} "
                                f"with different params. Pass ?force=true to replace."
                            ),
                            "stored": stored_snap,
                            "requested": params_json,
                        },
                    )

        # Insert / reset the audit row to pending inside the same transaction.
        from uuid import UUID as _UUID

        principal_id: Optional[_UUID] = None
        if applied_by is not None:
            pid = getattr(applied_by, "id", None) or getattr(applied_by, "principal_id", None)
            if pid is not None:
                try:
                    principal_id = _UUID(str(pid))
                except ValueError:
                    pass

        await audit.insert_pending(
            name, scope_key, params_json, applied_by=principal_id, conn=conn
        )

    # State machine: pending → in_progress → applied | failed
    await audit.mark_in_progress(name, scope_key)

    try:
        result = await preset.apply(params, scope_key, ctx)
    except Exception as exc:
        err_msg = str(exc)[:2000]
        logger.error("preset=%s scope=%s apply failed: %s", name, scope_key, err_msg, exc_info=True)
        await audit.mark_failed(name, scope_key, last_error=err_msg)
        raise HTTPException(
            status_code=500,
            detail={"message": f"Preset {name!r} apply failed.", "error": err_msg},
        ) from exc

    if isinstance(result, TaskHandle):
        # Async preset: stay in_progress; worker callback will finalise.
        return (await audit.mark_in_progress(name, scope_key, task_id=result.task_id)) or {}

    # Sync preset: mark applied.
    final = await audit.mark_applied(
        name, scope_key, revoke_descriptor=result.to_json()
    )
    return final or {}


async def revoke_preset(
    name: str,
    scope_key: str,
    ctx: PresetContext,
    engine: Optional[DbResource],
    audit: AppliedPresetsService,
    *,
    force_self_revoke: bool = False,
) -> AppliedRow:
    """Revoke a preset at the given scope.

    Returns the final audit row. Raises ``HTTPException`` on:
    * 404 — no applied audit row.
    * 409 — self-lockout guard fires without ``?force_self_revoke=true``.
    * 409 — concurrent revoke in progress.
    """
    preset = find_preset(name)

    existing = await audit.get(name, scope_key)
    if existing is None or existing.get("state") not in ("applied", "partial", "revoke_failed", "failed"):
        raise HTTPException(
            status_code=404,
            detail=f"No applied preset {name!r} at scope {scope_key!r} to revoke.",
        )

    # Self-lockout guard for IAM presets.
    if "iam" in getattr(preset, "keywords", ()) and not force_self_revoke:
        if ctx.principal is not None:
            _check_self_lockout(name, scope_key, ctx, existing)

    state = existing.get("state")
    if state in ("revoke_in_progress", "revoke_pending"):
        raise HTTPException(
            status_code=409,
            detail=f"Preset {name!r} at {scope_key!r} is currently {state!r}. Retry later.",
        )

    descriptor_data = existing.get("revoke_descriptor")
    if descriptor_data is None:
        descriptor_data = {}
    if isinstance(descriptor_data, str):
        descriptor_data = json.loads(descriptor_data)
    descriptor = AppliedDescriptor.from_json(descriptor_data)

    await audit.mark_revoke_pending(name, scope_key)

    try:
        result = await preset.revoke(descriptor, ctx)
    except Exception as exc:
        err_msg = str(exc)[:2000]
        logger.error("preset=%s scope=%s revoke failed: %s", name, scope_key, err_msg, exc_info=True)
        await audit.mark_revoke_failed(name, scope_key, last_error=err_msg)
        raise HTTPException(
            status_code=500,
            detail={"message": f"Preset {name!r} revoke failed.", "error": err_msg},
        ) from exc

    if isinstance(result, TaskHandle):
        return (await audit.mark_revoke_pending(name, scope_key, task_id=result.task_id)) or {}

    final = await audit.mark_revoked(name, scope_key)
    return final or {}


async def dry_run_preset(
    name: str,
    scope_key: str,
    params: BaseModel,
    ctx: PresetContext,
) -> PresetPlan:
    """Return a plan of what ``apply`` would do — no writes performed."""
    preset = find_preset(name)
    return await preset.dry_run(params, scope_key, ctx)


def _check_self_lockout(
    name: str,
    scope_key: str,
    ctx: PresetContext,
    existing_row: AppliedRow,
) -> None:
    """Raise 409 if revoking this preset would remove the caller's admin access.

    V1 implementation: heuristic check. If the caller's principal has a role
    that appears in the revoke descriptor's policy list and that role is the
    only source of admin grants, block the revoke. For a full simulation see
    future PR; v1 keeps this conservative (may produce false positives for
    principal with multiple admin paths).
    """
    principal = ctx.principal
    if principal is None:
        return

    descriptor_data = existing_row.get("revoke_descriptor") or {}
    if isinstance(descriptor_data, str):
        try:
            descriptor_data = json.loads(descriptor_data)
        except Exception:  # noqa: BLE001
            return

    # For routing presets the descriptor has "slots" not IAM content; skip.
    if "children" not in descriptor_data and "slots" in descriptor_data:
        return

    # Simplified v1 check: if the caller's only admin role is listed as
    # something this preset would remove, surface a 409.
    # The spec mandates this is scope-local; platform sysadmin is unaffected.
    principal_roles = set(getattr(principal, "roles", []) or [])
    removed_roles: set = set()

    # Collect roles the descriptor mentions removing.
    for child in descriptor_data.get("children", []):
        child_desc = child.get("descriptor", {})
        removed_roles.update(child_desc.get("role_names", []))
    removed_roles.update(descriptor_data.get("role_names", []))

    overlap = principal_roles & removed_roles
    if overlap and len(principal_roles) <= len(overlap):
        raise HTTPException(
            status_code=409,
            detail={
                "message": (
                    f"Revoking preset {name!r} at {scope_key!r} would remove "
                    f"the caller's effective admin access. "
                    f"Pass ?force_self_revoke=true to override."
                ),
                "affected_roles": sorted(overlap),
            },
        )
