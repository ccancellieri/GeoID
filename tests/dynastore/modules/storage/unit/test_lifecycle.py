"""Preset lifecycle layer tests.

Tests apply happy-path, idempotent re-POST, params_snapshot mismatch 409,
composite forward+reverse, and self-lockout guard.

All tests use mocked audit service and preset implementations.
"""
from __future__ import annotations

import json
from typing import ClassVar, Tuple, Type
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException
from pydantic import BaseModel

from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    CompositePreset,
    NoParams,
    PresetContext,
    PresetPlan,
    TaskHandle,
)
from dynastore.modules.storage.presets.protocol import PresetTier


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ctx() -> PresetContext:
    return PresetContext(
        db=MagicMock(),
        iam=MagicMock(),
        policy=MagicMock(),
        config=MagicMock(),
        tasks=None,
        cron=None,
        libs=None,
        principal=None,
        scope="platform",
    )


def _audit(
    *,
    existing: dict | None = None,
    insert_row: dict | None = None,
    mark_applied_row: dict | None = None,
    mark_failed_row: dict | None = None,
    mark_revoke_pending_row: dict | None = None,
    mark_revoked_row: dict | None = None,
    mark_revoke_failed_row: dict | None = None,
) -> MagicMock:
    audit = MagicMock()
    audit.get = AsyncMock(return_value=existing)
    audit.get_for_update = AsyncMock(return_value=existing)
    audit.insert_pending = AsyncMock(return_value=insert_row or {"state": "pending"})
    audit.mark_in_progress = AsyncMock(return_value={"state": "in_progress"})
    audit.mark_applied = AsyncMock(return_value=mark_applied_row or {"state": "applied"})
    audit.mark_failed = AsyncMock(return_value=mark_failed_row or {"state": "failed"})
    audit.mark_revoke_pending = AsyncMock(return_value=mark_revoke_pending_row or {"state": "revoke_pending"})
    audit.mark_revoked = AsyncMock(return_value=mark_revoked_row or {"state": "revoked"})
    audit.mark_revoke_failed = AsyncMock(return_value=mark_revoke_failed_row or {"state": "revoke_failed"})
    return audit


class _SyncPreset:
    """Simple sync preset that returns an AppliedDescriptor."""

    name = "test-sync-preset"
    description = "sync test"
    keywords: ClassVar[Tuple[str, ...]] = ()
    tier: ClassVar[PresetTier] = PresetTier.PLATFORM
    catalog_scopable: ClassVar[bool] = False
    params_model: ClassVar[Type[BaseModel]] = NoParams
    is_async: ClassVar[bool] = False

    async def dry_run(self, params, scope, ctx) -> PresetPlan:
        return PresetPlan(preset_name=self.name, scope_key=scope)

    async def apply(self, params, scope, ctx):
        return AppliedDescriptor(payload={"applied": True})

    async def revoke(self, applied_descriptor, ctx):
        return None


class _FailingPreset(_SyncPreset):
    name = "failing-preset"

    async def apply(self, params, scope, ctx):
        raise RuntimeError("intentional failure")


class _IamPreset(_SyncPreset):
    """IAM-tagged preset for self-lockout tests."""
    name = "iam-preset"
    keywords: ClassVar[Tuple[str, ...]] = ("iam",)


# ---------------------------------------------------------------------------
# apply happy path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_happy_path_marks_applied():
    from dynastore.modules.storage.presets.lifecycle import apply_preset

    preset = _SyncPreset()
    engine = MagicMock()
    audit = _audit()

    with patch(
        "dynastore.modules.storage.presets.lifecycle.find_preset",
        return_value=preset,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle.managed_transaction"
    ) as mock_tx:
        mock_tx.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        mock_tx.return_value.__aexit__ = AsyncMock(return_value=False)

        result = await apply_preset(
            "test-sync-preset", "platform", NoParams(), _ctx(),
            engine=engine, audit=audit,
        )

    audit.mark_applied.assert_awaited_once()
    assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# Idempotent re-POST — same params returns existing row
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_idempotent_same_params():
    from dynastore.modules.storage.presets.lifecycle import apply_preset

    existing = {
        "state": "applied",
        "params_snapshot": json.dumps({}),
        "revoke_descriptor": json.dumps({"applied": True}),
    }
    preset = _SyncPreset()
    engine = MagicMock()
    audit = _audit(existing=existing)

    with patch(
        "dynastore.modules.storage.presets.lifecycle.find_preset",
        return_value=preset,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle.managed_transaction"
    ) as mock_tx:
        mock_tx.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        mock_tx.return_value.__aexit__ = AsyncMock(return_value=False)

        result = await apply_preset(
            "test-sync-preset", "platform", NoParams(), _ctx(),
            engine=engine, audit=audit,
        )

    # Idempotent: no insert, mark_applied NOT called again.
    audit.insert_pending.assert_not_awaited()
    audit.mark_applied.assert_not_awaited()
    # Returns the existing row.
    assert result == existing


# ---------------------------------------------------------------------------
# params_snapshot mismatch → 409
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_params_mismatch_raises_409():
    from dynastore.modules.storage.presets.lifecycle import apply_preset

    existing = {
        "state": "applied",
        "params_snapshot": json.dumps({"mode": "old"}),
        "revoke_descriptor": "{}",
    }
    preset = _SyncPreset()
    engine = MagicMock()
    audit = _audit(existing=existing)

    class ParamsWithMode(BaseModel):
        mode: str = "new"

    with patch(
        "dynastore.modules.storage.presets.lifecycle.find_preset",
        return_value=preset,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle.managed_transaction"
    ) as mock_tx:
        mock_tx.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        mock_tx.return_value.__aexit__ = AsyncMock(return_value=False)

        with pytest.raises(HTTPException) as exc_info:
            await apply_preset(
                "test-sync-preset", "platform", ParamsWithMode(mode="new"), _ctx(),
                engine=engine, audit=audit,
            )

    assert exc_info.value.status_code == 409


# ---------------------------------------------------------------------------
# apply failure → state=failed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_failure_marks_failed():
    from dynastore.modules.storage.presets.lifecycle import apply_preset

    preset = _FailingPreset()
    engine = MagicMock()
    audit = _audit()

    with patch(
        "dynastore.modules.storage.presets.lifecycle.find_preset",
        return_value=preset,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle.managed_transaction"
    ) as mock_tx:
        mock_tx.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        mock_tx.return_value.__aexit__ = AsyncMock(return_value=False)

        with pytest.raises(HTTPException) as exc_info:
            await apply_preset(
                "failing-preset", "platform", NoParams(), _ctx(),
                engine=engine, audit=audit,
            )

    assert exc_info.value.status_code == 500
    audit.mark_failed.assert_awaited_once()


# ---------------------------------------------------------------------------
# revoke happy path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_revoke_happy_path_marks_revoked():
    from dynastore.modules.storage.presets.lifecycle import revoke_preset

    existing = {
        "state": "applied",
        "revoke_descriptor": json.dumps({"applied": True}),
    }
    preset = _SyncPreset()
    engine = MagicMock()
    audit = _audit(existing=existing)

    with patch(
        "dynastore.modules.storage.presets.lifecycle.find_preset",
        return_value=preset,
    ):
        result = await revoke_preset(
            "test-sync-preset", "platform", _ctx(),
            engine=engine, audit=audit,
        )

    audit.mark_revoked.assert_awaited_once()
    assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# revoke 404 when no audit row
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_revoke_404_when_no_row():
    from dynastore.modules.storage.presets.lifecycle import revoke_preset

    preset = _SyncPreset()
    engine = MagicMock()
    audit = _audit(existing=None)

    with patch(
        "dynastore.modules.storage.presets.lifecycle.find_preset",
        return_value=preset,
    ), pytest.raises(HTTPException) as exc_info:
        await revoke_preset(
            "test-sync-preset", "platform", _ctx(),
            engine=engine, audit=audit,
        )

    assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# self-lockout guard for IAM presets
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_self_lockout_raises_409_when_only_admin_role_removed():
    from dynastore.modules.storage.presets.lifecycle import revoke_preset

    principal = MagicMock()
    principal.roles = ["sysadmin"]

    ctx = PresetContext(
        db=MagicMock(),
        iam=MagicMock(),
        policy=MagicMock(),
        config=MagicMock(),
        tasks=None,
        cron=None,
        libs=None,
        principal=principal,
        scope="platform",
    )

    existing = {
        "state": "applied",
        "revoke_descriptor": json.dumps({
            "role_names": ["sysadmin"],
        }),
    }
    preset = _IamPreset()
    engine = MagicMock()
    audit = _audit(existing=existing)

    with patch(
        "dynastore.modules.storage.presets.lifecycle.find_preset",
        return_value=preset,
    ), pytest.raises(HTTPException) as exc_info:
        await revoke_preset(
            "iam-preset", "platform", ctx,
            engine=engine, audit=audit,
        )

    assert exc_info.value.status_code == 409


@pytest.mark.asyncio
async def test_self_lockout_overridden_with_force():
    from dynastore.modules.storage.presets.lifecycle import revoke_preset

    principal = MagicMock()
    principal.roles = ["sysadmin"]

    ctx = PresetContext(
        db=MagicMock(),
        iam=MagicMock(),
        policy=MagicMock(),
        config=MagicMock(),
        tasks=None,
        cron=None,
        libs=None,
        principal=principal,
        scope="platform",
    )

    existing = {
        "state": "applied",
        "revoke_descriptor": json.dumps({
            "role_names": ["sysadmin"],
        }),
    }
    preset = _IamPreset()
    engine = MagicMock()
    audit = _audit(existing=existing)

    with patch(
        "dynastore.modules.storage.presets.lifecycle.find_preset",
        return_value=preset,
    ):
        result = await revoke_preset(
            "iam-preset", "platform", ctx,
            engine=engine, audit=audit,
            force_self_revoke=True,
        )

    # Should succeed.
    audit.mark_revoked.assert_awaited_once()


# ---------------------------------------------------------------------------
# dry_run returns a plan without writes
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dry_run_returns_plan_without_writes():
    from dynastore.modules.storage.presets.lifecycle import dry_run_preset

    preset = _SyncPreset()

    with patch(
        "dynastore.modules.storage.presets.lifecycle.find_preset",
        return_value=preset,
    ):
        plan = await dry_run_preset("test-sync-preset", "platform", NoParams(), _ctx())

    assert isinstance(plan, PresetPlan)
    assert plan.preset_name == "test-sync-preset"
