"""Pin the run_validate_handlers / run_apply_handlers semantics (#738/#747).

The #738 bug: validation logic lived in apply handlers, which the 6 config-
write loops ran post-persist inside ``try/except Exception: logger.error`` —
so a ValueError was swallowed, the API returned 200 + null, and the invalid
config stayed persisted.

The fix splits the two phases.  These tests pin the contract:

- ``run_validate_handlers`` runs pre-persist and **propagates** — a bare
  ``ValueError`` is normalised to ``ConfigValidationError`` (→ HTTP 400),
  ``ConfigValidationError`` is re-raised as-is, ``ImmutableConfigError``
  keeps its identity (→ HTTP 409), first failure short-circuits, async
  handlers are awaited.
- ``run_apply_handlers`` runs post-persist and is **best-effort** — every
  handler exception is logged and swallowed, the loop continues, async
  handlers are awaited.
"""

from __future__ import annotations

from typing import ClassVar, Optional, Tuple

import pytest

from dynastore.modules.db_config.exceptions import (
    ConfigValidationError,
    ImmutableConfigError,
)
from dynastore.modules.db_config.plugin_config import PluginConfig, _APPLY_HANDLERS, _VALIDATE_HANDLERS
from dynastore.modules.db_config.platform_config_service import run_apply_handlers, run_validate_handlers


class _RunnerConfig(PluginConfig):
    _address: ClassVar[Tuple[Optional[str], ...]] = (
        "test", "config_change_runners", "cfg",
    )


def _cleanup(cls) -> None:
    _VALIDATE_HANDLERS.pop(cls, None)
    _APPLY_HANDLERS.pop(cls, None)


# ---------------------------------------------------------------------------
# run_validate_handlers — propagation + normalization
# ---------------------------------------------------------------------------


async def test_validate_runner_normalizes_bare_valueerror_to_400():
    """A handler raising a plain ``ValueError`` must surface as
    ``ConfigValidationError`` (HTTP 400) — not the catch-all ValueError→422
    path reserved for schema/shape violations."""
    def _bad(config, cat, col, conn) -> None:
        raise ValueError("hints not supported by driver")

    try:
        _RunnerConfig.register_validate_handler(_bad)
        with pytest.raises(ConfigValidationError, match="hints not supported"):
            await run_validate_handlers(_RunnerConfig, _RunnerConfig(), "c", "x", None)
    finally:
        _cleanup(_RunnerConfig)


async def test_validate_runner_reraises_config_validation_error_as_is():
    """An already-typed ``ConfigValidationError`` is re-raised unchanged —
    not double-wrapped."""
    sentinel = ConfigValidationError("already typed")

    def _bad(config, cat, col, conn) -> None:
        raise sentinel

    try:
        _RunnerConfig.register_validate_handler(_bad)
        with pytest.raises(ConfigValidationError) as exc_info:
            await run_validate_handlers(_RunnerConfig, _RunnerConfig(), "c", "x", None)
        assert exc_info.value is sentinel
    finally:
        _cleanup(_RunnerConfig)


async def test_validate_runner_preserves_immutable_config_error():
    """``ImmutableConfigError`` keeps its identity so the exception
    registry maps it to 409, not 400."""
    def _bad(config, cat, col, conn) -> None:
        raise ImmutableConfigError("field is Immutable")

    try:
        _RunnerConfig.register_validate_handler(_bad)
        with pytest.raises(ImmutableConfigError):
            await run_validate_handlers(_RunnerConfig, _RunnerConfig(), "c", "x", None)
    finally:
        _cleanup(_RunnerConfig)


async def test_validate_runner_awaits_async_handler():
    seen: list[str] = []

    async def _async_validator(config, cat, col, conn) -> None:
        seen.append("ran")

    try:
        _RunnerConfig.register_validate_handler(_async_validator)
        await run_validate_handlers(_RunnerConfig, _RunnerConfig(), "c", "x", None)
        assert seen == ["ran"]
    finally:
        _cleanup(_RunnerConfig)


async def test_validate_runner_first_failure_short_circuits():
    """The first failing handler stops the chain — later handlers don't run."""
    seen: list[str] = []

    def _first(config, cat, col, conn) -> None:
        seen.append("first")
        raise ValueError("stop here")

    def _second(config, cat, col, conn) -> None:
        seen.append("second")

    try:
        _RunnerConfig.register_validate_handler(_first)
        _RunnerConfig.register_validate_handler(_second)
        with pytest.raises(ConfigValidationError):
            await run_validate_handlers(_RunnerConfig, _RunnerConfig(), "c", "x", None)
        assert seen == ["first"]
    finally:
        _cleanup(_RunnerConfig)


async def test_validate_runner_noop_when_no_handlers():
    # No registration — must be a clean no-op, not an error.
    await run_validate_handlers(_RunnerConfig, _RunnerConfig(), "c", "x", None)


# ---------------------------------------------------------------------------
# run_apply_handlers — best-effort
# ---------------------------------------------------------------------------


async def test_apply_runner_swallows_handler_exception_and_continues():
    """A failing apply handler is logged and swallowed; the loop continues
    to the next handler.  This is the *intended* best-effort behaviour for
    post-persist side effects — validation must NOT live here (that's the
    #738 bug)."""
    seen: list[str] = []

    def _boom(config, cat, col, conn) -> None:
        seen.append("boom")
        raise RuntimeError("transient side-effect failure")

    def _after(config, cat, col, conn) -> None:
        seen.append("after")

    try:
        _RunnerConfig.register_apply_handler(_boom)
        _RunnerConfig.register_apply_handler(_after)
        # Must NOT raise.
        await run_apply_handlers(_RunnerConfig, _RunnerConfig(), "c", "x", None)
        assert seen == ["boom", "after"]
    finally:
        _cleanup(_RunnerConfig)


async def test_apply_runner_awaits_async_handler():
    seen: list[str] = []

    async def _async_applier(config, cat, col, conn) -> None:
        seen.append("ran")

    try:
        _RunnerConfig.register_apply_handler(_async_applier)
        await run_apply_handlers(_RunnerConfig, _RunnerConfig(), "c", "x", None)
        assert seen == ["ran"]
    finally:
        _cleanup(_RunnerConfig)
