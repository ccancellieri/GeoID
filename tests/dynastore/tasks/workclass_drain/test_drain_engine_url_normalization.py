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

"""Regression cover: the WorkClass drain tasks build their per-run async engine
from a *normalized* DB URL.

The drain tasks create their own ``create_async_engine`` inside ``run()`` rather
than reusing the app engine.  They previously did only a prefix swap
(``postgresql://`` -> ``postgresql+asyncpg://``), leaving the libpq ``sslmode=``
query parameter untouched.  Against a Cloud SQL DSN (which carries
``sslmode=...``) asyncpg's ``connect()`` then raised ``unexpected keyword
argument 'sslmode'`` — every drain failed unrecoverably and the
``tasks.events`` / ``tasks.storage`` rows stayed stuck.

The fix routes the URL through ``normalize_db_url(..., is_async=True)``, which
swaps the prefix AND converts ``sslmode=`` to asyncpg's ``ssl=`` — the same
canonical build ``db_service`` uses.  These tests pin that behaviour without a
real database by capturing the URL handed to ``create_async_engine``.
"""

from __future__ import annotations

from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.db_config.db_config import DBConfig
from dynastore.tasks.workclass_drain.event_drain_task import EventDrainTask
from dynastore.tasks.workclass_drain.single_flight import DrainSingleFlightGate
from dynastore.tasks.workclass_drain.storage_drain_task import StorageDrainTask
from tests.dynastore.test_utils.engine_mocks import make_fake_async_engine


class _ResolvedDatabaseUrl:
    """Set ``DBConfig.database_url`` to a fixed value for the duration of a
    ``with`` block by seeding the lazy descriptor's cache, then restore it."""

    def __init__(self, url: str) -> None:
        self._url = url
        self._descriptor = DBConfig.__dict__["database_url"]
        self._saved: Any = None

    def __enter__(self) -> None:
        self._saved = self._descriptor._resolved
        self._descriptor._resolved = self._url

    def __exit__(self, *exc: object) -> None:
        self._descriptor._resolved = self._saved


async def _capture_engine_url(task: Any) -> str:
    """Run ``task.run`` with the engine + drain loop stubbed, returning the URL
    passed to ``create_async_engine``."""
    captured: Dict[str, str] = {}

    # A real, never-started sync_engine — create_task_engine registers event
    # listeners on it and rejects a bare MagicMock as an event target.
    fake_engine = make_fake_async_engine()

    def _spy(url: str, **_kwargs: Any) -> Any:
        captured["url"] = url
        return fake_engine

    # The drain loop exits immediately (zero claimed rows) so no DB I/O
    # occurs; the cross-pod single-flight gate is stubbed open so the unit
    # test never attempts a real gate connection either.
    with patch("sqlalchemy.ext.asyncio.create_async_engine", side_effect=_spy), \
        patch.object(task, "drain_once", new=AsyncMock(return_value=0)), \
        patch.object(DrainSingleFlightGate, "acquire", new=AsyncMock(return_value=True)), \
        patch.object(DrainSingleFlightGate, "release", new=AsyncMock()):
        await task.run(MagicMock())

    assert "url" in captured, "create_async_engine was never called"
    return captured["url"]


@pytest.mark.asyncio
@pytest.mark.parametrize("task_cls", [EventDrainTask, StorageDrainTask])
async def test_drain_run_converts_sslmode_to_ssl_for_asyncpg(task_cls: Any) -> None:
    """A Cloud SQL-style DSN with ``sslmode=require`` must reach asyncpg as
    ``ssl=require`` — never the raw ``sslmode`` libpq spelling."""
    with _ResolvedDatabaseUrl("postgresql://u:p@h:5432/db?sslmode=require"):
        url = await _capture_engine_url(task_cls())

    assert url.startswith("postgresql+asyncpg://"), url
    assert "ssl=require" in url, url
    assert "sslmode=" not in url, url


@pytest.mark.asyncio
@pytest.mark.parametrize("task_cls", [EventDrainTask, StorageDrainTask])
async def test_drain_run_swaps_prefix_without_ssl_param(task_cls: Any) -> None:
    """A plain DSN (no SSL param) still gets the asyncpg prefix and is otherwise
    left intact."""
    with _ResolvedDatabaseUrl("postgresql://u:p@h:5432/db"):
        url = await _capture_engine_url(task_cls())

    assert url == "postgresql+asyncpg://u:p@h:5432/db", url
