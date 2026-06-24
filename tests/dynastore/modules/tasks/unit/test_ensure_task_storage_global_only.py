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

"""Guard test: ``ensure_task_storage_exists`` refuses non-global schemas.

The global ``tasks.tasks`` table is provisioned exactly once, at
``TasksModule.lifespan``, in ``get_task_schema()`` (default ``"tasks"``).
Every CRUD path pins that schema; the per-row ``catalog_id`` column is the
tenant discriminator.

Previously, three task runners (``tiles_preseed``, ``tiles_export``,
``dimensions_materialize``) called ``ensure_task_storage_exists(conn,
<catalog_schema>)`` under a "cellular safety" comment. That created an
unread shadow table per catalog plus a reaper pg_cron job firing every minute
on that empty table — pure overhead, never read by any CRUD path.

The helper now hard-raises on any schema other than ``get_task_schema()`` so
the regression cannot return silently.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from dynastore.modules.tasks import tasks_module


@pytest.mark.asyncio
async def test_refuses_non_global_schema() -> None:
    """A catalog physical schema (e.g. ``s_2ka8fbc3``) must be rejected."""
    fake_conn = AsyncMock()
    with pytest.raises(RuntimeError, match="non-global schema"):
        await tasks_module.ensure_task_storage_exists(fake_conn, "s_2ka8fbc3")


@pytest.mark.asyncio
async def test_refuses_public_schema() -> None:
    """``public`` is a PLATFORM-scoped row value, not a place to host tasks."""
    fake_conn = AsyncMock()
    with pytest.raises(RuntimeError, match="non-global schema"):
        await tasks_module.ensure_task_storage_exists(fake_conn, "public")


@pytest.mark.asyncio
async def test_error_message_names_expected_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    """The error names both the offending schema and the expected global one
    so operators can grep the log and find the bad caller fast.
    """
    monkeypatch.setenv("DYNASTORE_TASK_SCHEMA", "tasks")
    fake_conn = AsyncMock()
    with pytest.raises(RuntimeError) as ei:
        await tasks_module.ensure_task_storage_exists(fake_conn, "s_abc")
    msg = str(ei.value)
    assert "s_abc" in msg
    assert "tasks" in msg
    # Names the column-based discriminator pattern the caller likely meant
    assert "catalog_id" in msg


@pytest.mark.asyncio
async def test_honours_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """The guard compares against ``get_task_schema()`` — not a hardcoded
    constant — so deployments with a custom ``DYNASTORE_TASK_SCHEMA`` still
    work.
    """
    monkeypatch.setenv("DYNASTORE_TASK_SCHEMA", "custom_tasks_schema")
    # Re-fetch since the helper reads env at call time.
    assert tasks_module.get_task_schema() == "custom_tasks_schema"

    fake_conn = AsyncMock()
    with pytest.raises(RuntimeError, match="custom_tasks_schema"):
        await tasks_module.ensure_task_storage_exists(fake_conn, "tasks")
