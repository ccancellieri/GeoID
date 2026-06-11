"""``PlatformConfigService.lifespan`` emits ERROR when engine is None.

Prior to this fix the lifespan silently skipped ``initialize_storage`` when
``self.engine`` was ``None``.  On a DB-backed tier this means
``configs.task_capability_registry`` is never created, so the backstop /
sweep loops crash ~60 s after boot with
``relation "configs.task_capability_registry" does not exist``.

The fix logs an ERROR (non-fatal) so the misconfiguration surfaces in the
logs rather than manifesting as a cryptic downstream SQL error.
"""
from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.db_config.platform_config_service import PlatformConfigService


@pytest.mark.asyncio
async def test_lifespan_logs_error_when_engine_is_none(caplog):
    """When both ``_engine`` and the ``get_engine()`` fallback return None the
    lifespan must emit ERROR — and must NOT raise (non-fatal)."""
    svc = PlatformConfigService(engine=None)
    app_state = SimpleNamespace()

    with (
        patch(
            "dynastore.tools.protocol_helpers.get_engine",
            return_value=None,
        ),
        caplog.at_level(logging.ERROR, logger="dynastore.modules.db_config.platform_config_service"),
    ):
        async with svc.lifespan(app_state):
            pass

    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert error_records, (
        "Expected at least one ERROR log when engine is None; got none. "
        "The misconfiguration would otherwise manifest as a silent skip of "
        "configs.task_capability_registry DDL."
    )
    msg = error_records[0].getMessage()
    assert "task_capability_registry" in msg, (
        "ERROR message must name the table that will be absent so operators "
        "can grep for the symptom directly."
    )
    assert "no DB engine" in msg.lower() or "no db engine" in msg.lower(), (
        "ERROR message must state that the DB engine is absent."
    )


@pytest.mark.asyncio
async def test_lifespan_does_not_raise_when_engine_is_none(caplog):
    """A legitimately engine-less / test tier must still boot — non-fatal."""
    svc = PlatformConfigService(engine=None)
    app_state = SimpleNamespace()

    with patch(
        "dynastore.tools.protocol_helpers.get_engine",
        return_value=None,
    ):
        raised = False
        try:
            async with svc.lifespan(app_state):
                pass
        except Exception:
            raised = True

    assert not raised, "lifespan must not raise when engine is None (non-fatal)"


@pytest.mark.asyncio
async def test_lifespan_does_not_log_error_when_engine_is_present(caplog):
    """When an engine is available no ERROR is emitted and initialize_storage runs."""
    fake_engine = MagicMock()
    svc = PlatformConfigService(engine=fake_engine)
    app_state = SimpleNamespace()

    initialize_mock = AsyncMock()
    with (
        patch.object(PlatformConfigService, "initialize_storage", initialize_mock),
        caplog.at_level(logging.ERROR, logger="dynastore.modules.db_config.platform_config_service"),
    ):
        async with svc.lifespan(app_state):
            pass

    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert not error_records, (
        "No ERROR should be emitted when the engine is present."
    )
    initialize_mock.assert_awaited_once_with(fake_engine)
