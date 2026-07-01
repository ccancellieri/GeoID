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

"""Unit tests for ``modules/presets/bootstrap.bootstrap_preset_if_absent``.

These tests verify the neutral-module implementation directly, independent of
the shim in ``modules/storage/presets/lifecycle.py``.

All DB calls are mocked.  Tests run serially (-p no:randomly -n0).
"""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.storage.presets.preset import AppliedDescriptor, NoParams


# ---------------------------------------------------------------------------
# Helpers shared with lifecycle shim tests
# ---------------------------------------------------------------------------

def _make_preset(name: str = "test_preset") -> MagicMock:
    preset = MagicMock()
    preset.name = name
    preset.params_model = NoParams

    async def _apply(params: Any, scope: str, ctx: Any) -> AppliedDescriptor:
        return AppliedDescriptor(payload={"preset_name": name})

    preset.apply = AsyncMock(side_effect=_apply)
    return preset


class _FakeLockAcquired:
    def __init__(self, *_a: Any, **_kw: Any) -> None:
        pass

    async def __aenter__(self) -> MagicMock:
        return MagicMock()

    async def __aexit__(self, *_: Any) -> bool:
        return False


class _FakeLockTimeout:
    def __init__(self, *_a: Any, **_kw: Any) -> None:
        pass

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_: Any) -> bool:
        return False


_guard_unset = patch(
    "dynastore.modules.catalog.bootstrap_guard.is_initialized",
    AsyncMock(return_value=False),
)


# ---------------------------------------------------------------------------
# bootstrap.py-level tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_neutral_bootstrap_applies_when_sentinel_absent() -> None:
    from dynastore.modules.presets.bootstrap import bootstrap_preset_if_absent

    execute_calls: list = []

    class _MockDQL:
        def __init__(self, *_a: Any, **_kw: Any) -> None:
            pass

        async def execute(self, conn: Any, **kw: Any) -> Any:
            execute_calls.append(kw)
            return None  # SELECT absent on first call; INSERT returns None too

    preset = _make_preset("default_roles_baseline")

    with patch("dynastore.modules.db_config.locking_tools.acquire_startup_lock", _FakeLockAcquired), \
         patch("dynastore.modules.storage.presets.registry.find_preset", return_value=preset), \
         patch("dynastore.modules.presets.bootstrap._SELECT_SENTINEL", _MockDQL()), \
         patch("dynastore.modules.presets.bootstrap._INSERT_SENTINEL", _MockDQL()), \
         patch("dynastore.modules.storage.presets.lifecycle._build_context", return_value=MagicMock()), \
         _guard_unset:
        result = await bootstrap_preset_if_absent(MagicMock(), preset_name="default_roles_baseline")

    assert result is True
    preset.apply.assert_awaited_once()


@pytest.mark.asyncio
async def test_neutral_bootstrap_guard_set_skips_non_force() -> None:
    from dynastore.modules.presets.bootstrap import bootstrap_preset_if_absent

    preset = _make_preset("default_roles_baseline")

    with patch("dynastore.modules.db_config.locking_tools.acquire_startup_lock", _FakeLockAcquired), \
         patch("dynastore.modules.storage.presets.registry.find_preset", return_value=preset), \
         patch("dynastore.modules.catalog.bootstrap_guard.is_initialized", AsyncMock(return_value=True)):
        result = await bootstrap_preset_if_absent(MagicMock(), preset_name="default_roles_baseline")

    assert result is False
    preset.apply.assert_not_called()


@pytest.mark.asyncio
async def test_neutral_bootstrap_force_bypasses_guard() -> None:
    from dynastore.modules.presets.bootstrap import bootstrap_preset_if_absent

    execute_calls: list = []

    class _MockDQL:
        def __init__(self, *_a: Any, **_kw: Any) -> None:
            pass

        async def execute(self, conn: Any, **kw: Any) -> Any:
            execute_calls.append(kw)
            return (1,) if not execute_calls[1:] else None

    preset = _make_preset("public_access_baseline")

    with patch("dynastore.modules.db_config.locking_tools.acquire_startup_lock", _FakeLockAcquired), \
         patch("dynastore.modules.storage.presets.registry.find_preset", return_value=preset), \
         patch("dynastore.modules.presets.bootstrap._SELECT_SENTINEL", _MockDQL()), \
         patch("dynastore.modules.presets.bootstrap._INSERT_SENTINEL", _MockDQL()), \
         patch("dynastore.modules.storage.presets.lifecycle._build_context", return_value=MagicMock()), \
         patch("dynastore.modules.catalog.bootstrap_guard.is_initialized", AsyncMock(return_value=True)):
        result = await bootstrap_preset_if_absent(
            MagicMock(), preset_name="public_access_baseline", force=True
        )

    assert result is True
    preset.apply.assert_awaited_once()


@pytest.mark.asyncio
async def test_neutral_bootstrap_lock_timeout_returns_false() -> None:
    from dynastore.modules.presets.bootstrap import bootstrap_preset_if_absent

    preset = _make_preset("public_access_baseline")

    with patch("dynastore.modules.db_config.locking_tools.acquire_startup_lock", _FakeLockTimeout), \
         patch("dynastore.modules.storage.presets.registry.find_preset", return_value=preset):
        result = await bootstrap_preset_if_absent(MagicMock(), preset_name="public_access_baseline")

    assert result is False
    preset.apply.assert_not_called()


class _LockNotAvailableError(Exception):
    """Stand-in for the real PG lock-timeout exception (55P03)."""

    pgcode = "55P03"


class _FakeLockRaisesTimeout:
    """Matches real ``acquire_startup_lock`` behaviour: a lock-timeout raises
    before yielding, rather than yielding ``None`` (#2625).
    """

    def __init__(self, *_a: Any, **_kw: Any) -> None:
        pass

    async def __aenter__(self) -> None:
        raise _LockNotAvailableError("canceling statement due to lock timeout")

    async def __aexit__(self, *_: Any) -> bool:
        return False


@pytest.mark.asyncio
async def test_neutral_bootstrap_lock_timeout_raise_returns_false() -> None:
    """A real lock-timeout raises out of ``acquire_startup_lock`` — it must
    be caught and folded into the same skip-and-return-False path as the
    ``conn is None`` case, not propagate and abort startup.
    """
    from dynastore.modules.presets.bootstrap import bootstrap_preset_if_absent

    preset = _make_preset("public_access_baseline")

    with patch("dynastore.modules.db_config.locking_tools.acquire_startup_lock", _FakeLockRaisesTimeout), \
         patch("dynastore.modules.storage.presets.registry.find_preset", return_value=preset):
        result = await bootstrap_preset_if_absent(MagicMock(), preset_name="public_access_baseline")

    assert result is False
    preset.apply.assert_not_called()


# ---------------------------------------------------------------------------
# Shim tests: lifecycle.bootstrap_preset_if_absent delegates to neutral module
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_lifecycle_shim_delegates_to_neutral_module() -> None:
    """The shim in lifecycle.py returns an awaitable that calls the neutral impl."""
    called_with: list = []

    async def _fake_neutral(engine: Any, *, preset_name: str, scope_key: str,
                             lock_key: Any, force: bool) -> bool:
        called_with.append((preset_name, scope_key, force))
        return True

    with patch(
        "dynastore.modules.presets.bootstrap.bootstrap_preset_if_absent",
        _fake_neutral,
    ):
        from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent as _shim
        result = await _shim(MagicMock(), preset_name="some_preset", force=False)

    assert result is True
    assert called_with == [("some_preset", "platform", False)]


# ---------------------------------------------------------------------------
# IAM preset registration (smoke test — no DB)
# ---------------------------------------------------------------------------

def test_iam_presets_registered_in_registry() -> None:
    """Importing the IAM presets package registers both presets into the registry."""
    import dynastore.modules.iam.presets  # noqa: F401 — side-effect import
    from dynastore.modules.storage.presets.registry import get_preset

    assert get_preset("default_roles_baseline") is not None
    assert get_preset("public_access_baseline") is not None


# ---------------------------------------------------------------------------
# bootstrap_preset_if_absent — explicit params persisted in sentinel
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bootstrap_explicit_params_persisted_in_snapshot() -> None:
    """When explicit params are passed, the sentinel snapshot reflects them."""
    import json as _json
    from pydantic import BaseModel as _BM

    class _MyParams(_BM):
        level: str = "read"

    preset = MagicMock()
    preset.name = "parameterised_preset"
    preset.params_model = _MyParams

    captured_insert_kwargs: list = []

    class _SelectDQL:
        async def execute(self, conn: Any, **kw: Any) -> Any:
            return None  # sentinel absent

    class _InsertDQL:
        async def execute(self, conn: Any, **kw: Any) -> Any:
            captured_insert_kwargs.append(kw)
            return None

    async def _apply(params: Any, scope: str, ctx: Any) -> Any:
        from dynastore.modules.storage.presets.preset import AppliedDescriptor
        return AppliedDescriptor(payload={"applied": True})

    preset.apply = AsyncMock(side_effect=_apply)

    from dynastore.modules.presets.bootstrap import bootstrap_preset_if_absent

    with patch("dynastore.modules.db_config.locking_tools.acquire_startup_lock", _FakeLockAcquired), \
         patch("dynastore.modules.storage.presets.registry.find_preset", return_value=preset), \
         patch("dynastore.modules.presets.bootstrap._SELECT_SENTINEL", _SelectDQL()), \
         patch("dynastore.modules.presets.bootstrap._INSERT_SENTINEL", _InsertDQL()), \
         patch("dynastore.modules.storage.presets.lifecycle._build_context", return_value=MagicMock()), \
         _guard_unset:
        result = await bootstrap_preset_if_absent(
            MagicMock(),
            preset_name="parameterised_preset",
            params={"level": "admin"},
        )

    assert result is True
    preset.apply.assert_awaited_once()
    assert len(captured_insert_kwargs) == 1
    snapshot = _json.loads(captured_insert_kwargs[0]["params_snapshot"])
    assert snapshot == {"level": "admin"}


# ---------------------------------------------------------------------------
# bootstrap_preset_if_absent — invalid params returns False, apply not called
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bootstrap_invalid_params_returns_false_no_apply() -> None:
    """Params that fail the preset's params_model validation cause False return."""
    from pydantic import BaseModel as _BM

    class _StrictParams(_BM):
        model_config = {"extra": "forbid"}
        required_field: str

    preset = MagicMock()
    preset.name = "strict_preset"
    preset.params_model = _StrictParams
    preset.apply = AsyncMock()

    from dynastore.modules.presets.bootstrap import bootstrap_preset_if_absent

    with patch("dynastore.modules.db_config.locking_tools.acquire_startup_lock", _FakeLockAcquired), \
         patch("dynastore.modules.storage.presets.registry.find_preset", return_value=preset), \
         patch("dynastore.modules.presets.bootstrap._SELECT_SENTINEL", MagicMock(execute=AsyncMock(return_value=None))), \
         _guard_unset:
        result = await bootstrap_preset_if_absent(
            MagicMock(),
            preset_name="strict_preset",
            params={},  # missing required_field
        )

    assert result is False
    preset.apply.assert_not_called()


# ---------------------------------------------------------------------------
# bootstrap_preset_if_absent — resolution order: explicit beats file
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bootstrap_explicit_params_beats_file() -> None:
    """Explicit params take priority over what load_preset_params would return."""
    from pydantic import BaseModel as _BM

    class _LevelParams(_BM):
        level: str = "read"

    preset = MagicMock()
    preset.name = "resolution_test"
    preset.params_model = _LevelParams

    received_params: list = []

    async def _apply(params: Any, scope: str, ctx: Any) -> Any:
        from dynastore.modules.storage.presets.preset import AppliedDescriptor
        received_params.append(params)
        return AppliedDescriptor()

    preset.apply = AsyncMock(side_effect=_apply)

    from dynastore.modules.presets.bootstrap import bootstrap_preset_if_absent

    # File loader would return {"level": "from_file"} — but we pass explicit.
    with patch("dynastore.modules.db_config.locking_tools.acquire_startup_lock", _FakeLockAcquired), \
         patch("dynastore.modules.storage.presets.registry.find_preset", return_value=preset), \
         patch("dynastore.modules.presets.bootstrap._SELECT_SENTINEL", MagicMock(execute=AsyncMock(return_value=None))), \
         patch("dynastore.modules.presets.bootstrap._INSERT_SENTINEL", MagicMock(execute=AsyncMock(return_value=None))), \
         patch("dynastore.modules.storage.presets.lifecycle._build_context", return_value=MagicMock()), \
         patch("dynastore.modules.presets.param_loader.load_preset_params", return_value={"level": "from_file"}), \
         _guard_unset:
        result = await bootstrap_preset_if_absent(
            MagicMock(),
            preset_name="resolution_test",
            params={"level": "explicit"},
        )

    assert result is True
    assert received_params[0].level == "explicit"


# ---------------------------------------------------------------------------
# bootstrap_presets — chain ordering and continue-on-failure
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bootstrap_presets_chain_ordering() -> None:
    """Presets in a chain are applied in list order."""
    from dynastore.modules.presets.bootstrap import bootstrap_presets

    order: list = []

    async def _fake_bootstrap(engine: Any, *, preset_name: str, scope_key: str,
                               force: bool, params: Any, **_kw: Any) -> bool:
        order.append(preset_name)
        return True

    with patch(
        "dynastore.modules.presets.bootstrap.bootstrap_preset_if_absent",
        _fake_bootstrap,
    ):
        result = await bootstrap_presets(
            MagicMock(),
            [{"preset_name": "first"}, {"preset_name": "second"}, {"preset_name": "third"}],
        )

    assert order == ["first", "second", "third"]
    assert result == {"first": True, "second": True, "third": True}


@pytest.mark.asyncio
async def test_bootstrap_presets_continues_after_failure() -> None:
    """A failed preset logs and the chain continues — the dict records False for it."""
    from dynastore.modules.presets.bootstrap import bootstrap_presets

    call_log: list = []

    async def _fake_bootstrap(engine: Any, *, preset_name: str, scope_key: str,
                               force: bool, params: Any, **_kw: Any) -> bool:
        call_log.append(preset_name)
        if preset_name == "failing":
            raise RuntimeError("simulated failure")
        return True

    with patch(
        "dynastore.modules.presets.bootstrap.bootstrap_preset_if_absent",
        _fake_bootstrap,
    ):
        result = await bootstrap_presets(
            MagicMock(),
            [{"preset_name": "ok_before"}, {"preset_name": "failing"}, {"preset_name": "ok_after"}],
        )

    assert call_log == ["ok_before", "failing", "ok_after"]
    assert result["ok_before"] is True
    assert result["failing"] is False
    assert result["ok_after"] is True
