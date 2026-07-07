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

"""Unit tests for the ``ensure_init_db`` bootstrap-marker gate.

The gate skips the whole one-time init (extension DDL + platform-config
storage) when a prior boot has already marked the platform initialised, so a
serving pod's foundational lifespan never re-issues the DB-blocking extension
probe against an already-initialised DB. All DB interactions are mocked.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.db_config import tools


@pytest.mark.asyncio
async def test_ensure_init_db_skips_when_marker_present() -> None:
    """Marker present → neither the extension bootstrap nor the platform-config
    storage init runs (single cache read, no DDL, no probe)."""
    with patch.object(
        tools, "_resolve_db_identity", AsyncMock(return_value="host:5432/db")
    ), patch.object(
        tools, "_platform_bootstrap_present", AsyncMock(return_value=True)
    ), patch.object(
        tools, "ensure_base_extensions", AsyncMock()
    ) as mock_ext, patch.object(
        tools.maintenance_tools, "retry_on_invalidated_connection", AsyncMock()
    ) as mock_retry:
        await tools.ensure_init_db(resource=object())  # type: ignore[arg-type]

    mock_ext.assert_not_awaited()
    mock_retry.assert_not_awaited()


@pytest.mark.asyncio
async def test_ensure_init_db_runs_when_marker_absent() -> None:
    """Marker absent (fresh DB) → extension bootstrap and platform-config
    storage init both run."""
    with patch.object(
        tools, "_resolve_db_identity", AsyncMock(return_value="host:5432/db")
    ), patch.object(
        tools, "_platform_bootstrap_present", AsyncMock(return_value=False)
    ), patch.object(
        tools, "ensure_base_extensions", AsyncMock()
    ) as mock_ext, patch.object(
        tools.maintenance_tools, "retry_on_invalidated_connection", AsyncMock()
    ) as mock_retry:
        await tools.ensure_init_db(resource=object())  # type: ignore[arg-type]

    mock_ext.assert_awaited_once()
    mock_retry.assert_awaited_once()
