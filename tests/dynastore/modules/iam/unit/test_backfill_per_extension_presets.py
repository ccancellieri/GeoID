"""Tests for the per-extension-presets backfill migration.

Three paths:
1. Happy path — for each registered PolicyContributorPreset with no existing
   audit row, inserts one applied row with correct descriptor.
2. Idempotency — existing rows are not overwritten.
3. Contributor introspection failure — migration skips that preset and continues.
"""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch
import pytest


# ---------------------------------------------------------------------------
# Fake contributor for isolation
# ---------------------------------------------------------------------------

class _FakeExtContributor:
    def get_policies(self):
        from dynastore.models.auth import Policy
        return [Policy(
            id="fake_ext_policy",
            description="Fake ext",
            actions=["GET"],
            resources=["/fake_ext/.*"],
            effect="ALLOW",
        )]

    def get_role_bindings(self):
        from dynastore.models.auth_models import Role
        return [Role(name="anonymous", policies=["fake_ext_policy"])]


# ---------------------------------------------------------------------------
# Test: happy path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backfill_inserts_for_registered_preset():
    """Migration inserts one audit row per registered PolicyContributorPreset."""
    from dynastore.modules.storage.presets.policy_contributor_adapter import (
        PolicyContributorPreset,
    )
    from dynastore.modules.storage.presets.registry import _REGISTRY

    preset_name = "_test_backfill_ext_enable"
    _REGISTRY.pop(preset_name, None)

    from dynastore.modules.storage.presets.registry import register_preset
    register_preset(PolicyContributorPreset(
        name=preset_name,
        description="Test backfill ext",
        keywords=("iam", "test"),
        contributor_factory=_FakeExtContributor,
    ))

    try:
        from dynastore.modules.iam.migrations import backfill_per_extension_presets_audit as m

        engine = MagicMock()
        policy_storage = MagicMock()
        iam_storage = MagicMock()

        # Present in DB: fake_ext_policy and anonymous role.
        async def _get_policy(pid: str, schema: str, conn: Any, partition_key: str):
            return MagicMock() if pid == "fake_ext_policy" else None

        async def _get_role(rname: str, schema: str, conn: Any):
            return MagicMock() if rname == "anonymous" else None

        policy_storage.get_policy = _get_policy
        iam_storage.get_role = _get_role

        inserted: Dict[str, Dict[str, Any]] = {}

        async def _select_none(resource: Any, **kw: Any):
            return None  # No existing row.

        async def _insert_capture(resource: Any, **kw: Any):
            inserted[kw["preset_name"]] = kw

        @asynccontextmanager
        async def _ctx(eng: Any):
            yield MagicMock()

        with patch(
            "dynastore.modules.iam.migrations.backfill_per_extension_presets_audit.managed_transaction",
            side_effect=_ctx,
        ), patch(
            "dynastore.modules.iam.migrations.backfill_per_extension_presets_audit.SELECT_EXISTING"
        ) as mock_select, patch(
            "dynastore.modules.iam.migrations.backfill_per_extension_presets_audit.INSERT_APPLIED"
        ) as mock_insert:
            mock_select.execute = _select_none
            mock_insert.execute = _insert_capture

            await m.run_backfill(engine, policy_storage, iam_storage)

        assert preset_name in inserted, f"Expected audit row for {preset_name!r}"
        revoke = json.loads(inserted[preset_name]["revoke_descriptor"])
        assert "fake_ext_policy" in revoke["policy_ids"]
        assert "anonymous" in revoke["role_names"]
    finally:
        _REGISTRY.pop(preset_name, None)


# ---------------------------------------------------------------------------
# Test: idempotency
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backfill_idempotent_when_row_exists():
    """Existing audit rows are not overwritten."""
    from dynastore.modules.storage.presets.policy_contributor_adapter import (
        PolicyContributorPreset,
    )
    from dynastore.modules.storage.presets.registry import _REGISTRY, register_preset

    preset_name = "_test_backfill_idem_enable"
    _REGISTRY.pop(preset_name, None)
    register_preset(PolicyContributorPreset(
        name=preset_name,
        description="Test idempotency",
        keywords=("iam",),
        contributor_factory=_FakeExtContributor,
    ))

    try:
        from dynastore.modules.iam.migrations import backfill_per_extension_presets_audit as m

        engine, policy_storage, iam_storage = MagicMock(), MagicMock(), MagicMock()
        policy_storage.get_policy = AsyncMock(return_value=None)
        iam_storage.get_role = AsyncMock(return_value=None)

        inserted: Dict[str, Any] = {}

        async def _select_found(resource: Any, **kw: Any):
            return MagicMock()  # Row exists.

        async def _insert_capture(resource: Any, **kw: Any):
            inserted.update(kw)

        @asynccontextmanager
        async def _ctx(eng: Any):
            yield MagicMock()

        with patch(
            "dynastore.modules.iam.migrations.backfill_per_extension_presets_audit.managed_transaction",
            side_effect=_ctx,
        ), patch(
            "dynastore.modules.iam.migrations.backfill_per_extension_presets_audit.SELECT_EXISTING"
        ) as mock_select, patch(
            "dynastore.modules.iam.migrations.backfill_per_extension_presets_audit.INSERT_APPLIED"
        ) as mock_insert:
            mock_select.execute = _select_found
            mock_insert.execute = _insert_capture

            await m.run_backfill(engine, policy_storage, iam_storage)

        assert inserted == {}, "INSERT was called despite existing row"
    finally:
        _REGISTRY.pop(preset_name, None)


# ---------------------------------------------------------------------------
# Test: contributor introspection failure is handled gracefully
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backfill_skips_broken_contributor():
    """If a contributor factory raises, the migration skips that preset."""
    from dynastore.modules.storage.presets.policy_contributor_adapter import (
        PolicyContributorPreset,
    )
    from dynastore.modules.storage.presets.registry import _REGISTRY, register_preset

    preset_name = "_test_backfill_broken_enable"
    _REGISTRY.pop(preset_name, None)

    def _broken_factory():
        raise RuntimeError("broken contributor")

    register_preset(PolicyContributorPreset(
        name=preset_name,
        description="Broken",
        keywords=("iam",),
        contributor_factory=_broken_factory,
    ))

    try:
        from dynastore.modules.iam.migrations import backfill_per_extension_presets_audit as m

        engine, policy_storage, iam_storage = MagicMock(), MagicMock(), MagicMock()
        inserted: List[str] = []

        async def _select_none(resource: Any, **kw: Any):
            return None

        async def _insert_capture(resource: Any, **kw: Any):
            inserted.append(kw.get("preset_name", ""))

        @asynccontextmanager
        async def _ctx(eng: Any):
            yield MagicMock()

        with patch(
            "dynastore.modules.iam.migrations.backfill_per_extension_presets_audit.managed_transaction",
            side_effect=_ctx,
        ), patch(
            "dynastore.modules.iam.migrations.backfill_per_extension_presets_audit.SELECT_EXISTING"
        ) as mock_select, patch(
            "dynastore.modules.iam.migrations.backfill_per_extension_presets_audit.INSERT_APPLIED"
        ) as mock_insert:
            mock_select.execute = _select_none
            mock_insert.execute = _insert_capture

            # Must not raise even if a contributor factory is broken.
            await m.run_backfill(engine, policy_storage, iam_storage)

        assert preset_name not in inserted, "Broken preset should have been skipped"
    finally:
        _REGISTRY.pop(preset_name, None)
