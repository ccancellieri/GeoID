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

"""``DBService.lifespan`` must not relabel a post-establishment / teardown
error as a connection-pool *creation* failure (#2764 finding 2).

The lifespan's ``yield`` used to sit inside the same ``try`` whose
``except Exception`` logged ``"FATAL: Failed to create database connection
pool"`` — so any exception raised by the application body or by shutdown
*after* the pool was already established got the wrong, misleading label.

The fix splits the try into two: pool establishment keeps its original
message, and anything surfacing through ``yield`` (application runtime or
teardown) gets its own, accurate message. The ``finally`` cleanup still
covers both paths.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.db.db_service import DBService
from dynastore.modules.db_config.db_config import DBConfig


def _app_state_with_existing_engine() -> SimpleNamespace:
    """An app_state carrying a pre-injected engine, so lifespan skips the
    real ``create_async_engine(...)`` call and goes straight to yield."""
    fake_engine = MagicMock()
    fake_engine.dispose = AsyncMock()
    return SimpleNamespace(db_config=DBConfig(), engine=fake_engine)


@pytest.mark.asyncio
async def test_pool_creation_failure_keeps_fatal_message(caplog):
    """A real creation-time failure must still be logged as a FATAL pool
    creation failure (unchanged behaviour)."""
    app_state = SimpleNamespace(db_config=DBConfig())
    service = DBService(app_state)

    with patch(
        "dynastore.modules.db.db_service.create_async_engine",
        side_effect=RuntimeError("boom: cannot connect"),
    ):
        with caplog.at_level("CRITICAL", logger="dynastore.modules.db.db_service"):
            with pytest.raises(RuntimeError, match="boom: cannot connect"):
                async with service.lifespan(app_state):
                    pass  # pragma: no cover - never reached

    messages = [rec.getMessage() for rec in caplog.records]
    assert any("FATAL: Failed to create database connection pool" in m for m in messages)
    assert not any(
        "Error during database service runtime or shutdown" in m for m in messages
    )


@pytest.mark.asyncio
async def test_post_establishment_error_is_not_labeled_as_pool_creation_failure(
    caplog,
):
    """An exception raised from the ``async with`` body (after the pool is
    already up) must NOT be logged as a pool-creation failure."""
    app_state = _app_state_with_existing_engine()
    service = DBService(app_state)

    with patch(
        "dynastore.modules.db_config.tools.ensure_base_extensions",
        new=AsyncMock(return_value=None),
    ), patch(
        "dynastore.tools.discovery.register_plugin", new=MagicMock()
    ), patch(
        "dynastore.tools.discovery.unregister_plugin", new=MagicMock()
    ):
        with caplog.at_level("CRITICAL", logger="dynastore.modules.db.db_service"):
            with pytest.raises(ValueError, match="teardown exploded"):
                async with service.lifespan(app_state):
                    raise ValueError("teardown exploded")

    messages = [rec.getMessage() for rec in caplog.records]
    assert any(
        "Error during database service runtime or shutdown" in m for m in messages
    ), f"expected the accurate runtime/shutdown message, got: {messages}"
    assert not any(
        "FATAL: Failed to create database connection pool" in m for m in messages
    ), f"post-establishment error must not be mislabeled as a pool creation failure, got: {messages}"


@pytest.mark.asyncio
async def test_cleanup_still_runs_on_pool_creation_failure(caplog):
    """The ``finally`` cleanup must still execute when pool creation itself
    fails — the two-``try`` restructure must not skip it (a naive split into
    two independent ``try`` statements would ``raise`` out of the first one
    before ever reaching the second ``try/finally``)."""
    app_state = SimpleNamespace(db_config=DBConfig())
    service = DBService(app_state)

    with patch(
        "dynastore.modules.db.db_service.create_async_engine",
        side_effect=RuntimeError("boom"),
    ):
        with caplog.at_level("INFO", logger="dynastore.modules.db.db_service"):
            with pytest.raises(RuntimeError):
                async with service.lifespan(app_state):
                    pass  # pragma: no cover - never reached

    messages = [rec.getMessage() for rec in caplog.records]
    assert any("Database connection shutdown completed" in m for m in messages), (
        f"finally-block cleanup log missing on pool-creation failure, got: {messages}"
    )
