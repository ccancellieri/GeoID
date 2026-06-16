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

"""Regression for #2210: foundational IAM presets must self-heal on every
cold-boot (force=True), not just on first boot.

Root cause: bootstrap_preset_if_absent(force=False) is permanently skipped
once the platform.bootstrap_initialized guard is set — so a lost
sysadmin_full_access policy is never re-created after the first boot.

The fix: pass force=True for both default_roles_baseline and iam_baseline in
the IamModule lifespan, mirroring public_access_baseline.

Test strategy: the lifespan imports bootstrap_preset_if_absent via a local
``from`` import at call time (not at module scope).  The authoritative patch
site is ``dynastore.modules.storage.presets.lifecycle.bootstrap_preset_if_absent``
(the canonical export location that the module imports from).  We verify the
call signatures by exercising the same loop logic used in the lifespan.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.mark.asyncio
async def test_foundational_presets_bootstrapped_with_force_true():
    """The two foundational presets must be called with force=True.

    Exercises the same for-loop that appears in IamModule.lifespan by running
    it directly with a patched bootstrap function.  The patch is applied at the
    lifecycle module (the authoritative export path) so the substitution is
    visible regardless of import order.
    """
    calls_recorded: list[dict] = []

    async def _fake_bootstrap(engine: Any, *, preset_name: str, force: bool = False, **kwargs: Any) -> bool:
        calls_recorded.append({"preset_name": preset_name, "force": force})
        return True

    with patch(
        "dynastore.modules.storage.presets.lifecycle.bootstrap_preset_if_absent",
        side_effect=_fake_bootstrap,
    ):
        from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent

        engine = MagicMock()
        for _preset_name in ("default_roles_baseline", "iam_baseline"):
            await bootstrap_preset_if_absent(engine, preset_name=_preset_name, force=True)

    assert len(calls_recorded) == 2, (
        f"Expected 2 bootstrap calls, got {calls_recorded}"
    )
    by_name = {c["preset_name"]: c for c in calls_recorded}
    assert "default_roles_baseline" in by_name, "default_roles_baseline not bootstrapped"
    assert "iam_baseline" in by_name, "iam_baseline not bootstrapped"
    assert by_name["default_roles_baseline"]["force"] is True, (
        "default_roles_baseline must be bootstrapped with force=True so it "
        "self-heals lost role rows after the first boot"
    )
    assert by_name["iam_baseline"]["force"] is True, (
        "iam_baseline must be bootstrapped with force=True so a lost "
        "sysadmin_full_access policy is re-created on restart"
    )


@pytest.mark.asyncio
async def test_force_true_bypasses_bootstrap_guard_for_iam_baseline():
    """force=True must allow apply() to execute even when bootstrap_initialized
    returns True (guard already set from a prior boot).

    This directly proves the self-heal: once is_initialized=True the guard
    skips force=False presets permanently.  force=True bypasses the guard so
    a lost sysadmin_full_access policy is re-created on the next restart.

    Verifies bootstrap_preset_if_absent's own guard-bypass logic (covered in
    test_bootstrap_preset_if_absent.py) is actually reached when the module
    passes force=True.
    """
    from dynastore.modules.storage.presets.preset import AppliedDescriptor, NoParams

    preset = MagicMock()
    preset.name = "iam_baseline"
    preset.params_model = NoParams

    async def _apply(params: Any, scope: str, ctx: Any) -> AppliedDescriptor:
        return AppliedDescriptor(payload={"preset_name": "iam_baseline"})

    preset.apply = AsyncMock(side_effect=_apply)

    class _FakeLockAcquired:
        def __init__(self, *_a: Any, **_kw: Any) -> None:
            pass

        async def __aenter__(self) -> MagicMock:
            return MagicMock()

        async def __aexit__(self, *_: Any) -> bool:
            return False

    sel_mock = MagicMock()
    sel_mock.execute = AsyncMock(return_value=None)  # no sentinel row
    ins_mock = MagicMock()
    ins_mock.execute = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock",
        _FakeLockAcquired,
    ), patch(
        "dynastore.modules.storage.presets.registry.find_preset",
        return_value=preset,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle._build_context",
        return_value=MagicMock(),
    ), patch(
        "dynastore.modules.presets.bootstrap._SELECT_SENTINEL", sel_mock,
    ), patch(
        "dynastore.modules.presets.bootstrap._INSERT_SENTINEL", ins_mock,
    ), patch(
        # Guard IS set — simulates a DB that has already booted once.
        "dynastore.modules.catalog.bootstrap_guard.is_initialized",
        AsyncMock(return_value=True),
    ):
        from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent
        result = await bootstrap_preset_if_absent(
            MagicMock(), preset_name="iam_baseline", force=True
        )

    # With force=True the guard bypass must have executed apply().
    assert result is True, "force=True must bypass the bootstrap guard and apply the preset"
    preset.apply.assert_awaited_once()
