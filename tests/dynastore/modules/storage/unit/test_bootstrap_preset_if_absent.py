"""Unit tests for ``bootstrap_preset_if_absent`` in lifecycle.py.

All DB calls are mocked.  Tests run serially under
``pytest -p no:xdist -p no:logging``.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.storage.presets.preset import AppliedDescriptor, NoParams


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_preset(name: str = "test_preset") -> MagicMock:
    """Return a minimal Preset mock whose apply returns an AppliedDescriptor."""
    preset = MagicMock()
    preset.name = name
    preset.params_model = NoParams

    async def _apply(params: Any, scope: str, ctx: Any) -> AppliedDescriptor:
        return AppliedDescriptor(payload={"preset_name": name})

    preset.apply = AsyncMock(side_effect=_apply)
    return preset


class _FakeLockAcquired:
    """Advisory lock that succeeds and yields a non-None connection."""

    def __init__(self, *_a: Any, **_kw: Any) -> None:
        pass

    async def __aenter__(self) -> MagicMock:
        return MagicMock()  # non-None → lock acquired

    async def __aexit__(self, *_: Any) -> bool:
        return False


class _FakeLockTimeout:
    """Advisory lock that yields None — simulates a lock timeout."""

    def __init__(self, *_a: Any, **_kw: Any) -> None:
        pass

    async def __aenter__(self) -> None:
        return None  # lock timeout

    async def __aexit__(self, *_: Any) -> bool:
        return False


# ---------------------------------------------------------------------------
# Test: sentinel absent → preset applied and sentinel inserted
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bootstrap_applies_preset_when_sentinel_absent() -> None:
    """When no sentinel row exists, apply() is called and the sentinel is inserted."""
    from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent

    execute_calls: list = []

    class _MockDQL:
        """Records execute calls; first call returns None (absent), rest return None."""
        def __init__(self, *_a: Any, **_kw: Any) -> None:
            pass

        async def execute(self, conn: Any, **kw: Any) -> Any:
            execute_calls.append(kw)
            if len(execute_calls) == 1:
                return None  # SELECT sentinel → absent
            return None  # INSERT sentinel

    preset = _make_preset("default_roles_baseline")

    with patch(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock",
        _FakeLockAcquired,
    ), patch(
        # Patch the name as bound in the lifecycle module
        "dynastore.modules.storage.presets.lifecycle.find_preset",
        return_value=preset,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle._build_context",
        return_value=MagicMock(),
    ), patch(
        "dynastore.modules.storage.presets.lifecycle.DQLQuery",
        _MockDQL,
    ):
        result = await bootstrap_preset_if_absent(
            MagicMock(), preset_name="default_roles_baseline"
        )

    assert result is True
    # First call is SELECT, second call is INSERT.
    assert len(execute_calls) == 2
    preset.apply.assert_awaited_once()


# ---------------------------------------------------------------------------
# Test: sentinel present → no-op, apply never called
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bootstrap_noop_when_sentinel_present() -> None:
    """When a sentinel row already exists, apply() is never called."""
    from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent

    class _MockDQL:
        def __init__(self, *_a: Any, **_kw: Any) -> None:
            pass

        async def execute(self, conn: Any, **kw: Any) -> Any:
            return (1,)  # SELECT sentinel → present

    preset = _make_preset("iam_baseline")

    with patch(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock",
        _FakeLockAcquired,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle.find_preset",
        return_value=preset,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle.DQLQuery",
        _MockDQL,
    ):
        result = await bootstrap_preset_if_absent(
            MagicMock(), preset_name="iam_baseline"
        )

    assert result is False
    preset.apply.assert_not_called()


# ---------------------------------------------------------------------------
# Test: advisory lock timeout → returns False without apply
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bootstrap_returns_false_on_lock_timeout() -> None:
    """When advisory lock times out (conn=None), returns False without applying."""
    from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent

    preset = _make_preset("public_access_baseline")

    with patch(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock",
        _FakeLockTimeout,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle.find_preset",
        return_value=preset,
    ):
        result = await bootstrap_preset_if_absent(
            MagicMock(), preset_name="public_access_baseline"
        )

    assert result is False
    preset.apply.assert_not_called()


# ---------------------------------------------------------------------------
# Test: apply() raises → exception propagates (lock already released by ctx mgr)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bootstrap_propagates_apply_failure() -> None:
    """If preset.apply() raises, the exception propagates to the caller."""
    from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent

    broken_preset = MagicMock()
    broken_preset.name = "broken_preset"
    broken_preset.params_model = NoParams
    broken_preset.apply = AsyncMock(side_effect=RuntimeError("apply exploded"))

    call_count = [0]

    class _MockDQL:
        def __init__(self, *_a: Any, **_kw: Any) -> None:
            pass

        async def execute(self, conn: Any, **kw: Any) -> Any:
            call_count[0] += 1
            return None  # SELECT sentinel → absent

    with patch(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock",
        _FakeLockAcquired,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle.find_preset",
        return_value=broken_preset,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle._build_context",
        return_value=MagicMock(),
    ), patch(
        "dynastore.modules.storage.presets.lifecycle.DQLQuery",
        _MockDQL,
    ):
        with pytest.raises(RuntimeError, match="apply exploded"):
            await bootstrap_preset_if_absent(
                MagicMock(), preset_name="broken_preset"
            )
