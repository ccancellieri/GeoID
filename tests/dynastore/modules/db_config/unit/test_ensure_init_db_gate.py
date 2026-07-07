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

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.db_config import tools


@pytest.mark.asyncio
async def test_ensure_init_db_skips_when_marker_present() -> None:
    """Marker present → neither the extension bootstrap nor the platform-config
    storage init runs (single direct read, no DDL, no probe)."""
    with patch.object(
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
        tools, "_platform_bootstrap_present", AsyncMock(return_value=False)
    ), patch.object(
        tools, "ensure_base_extensions", AsyncMock()
    ) as mock_ext, patch.object(
        tools.maintenance_tools, "retry_on_invalidated_connection", AsyncMock()
    ) as mock_retry:
        await tools.ensure_init_db(resource=object())  # type: ignore[arg-type]

    mock_ext.assert_awaited_once()
    mock_retry.assert_awaited_once()


@pytest.mark.asyncio
async def test_ensure_init_db_skips_when_marker_read_times_out() -> None:
    """A hanging marker read (cold-boot pool starvation) must NOT abort the
    foundational lifespan: the bounded read times out and init is skipped —
    neither the extension bootstrap nor the platform-config storage init runs."""

    async def _hang(_resource) -> bool:
        await asyncio.sleep(60)  # never resolves within the bound
        return True

    with patch.object(
        tools, "_platform_bootstrap_present", _hang
    ), patch.object(
        tools, "_BOOTSTRAP_MARKER_READ_TIMEOUT_SECONDS", 0.05
    ), patch.object(
        tools, "ensure_base_extensions", AsyncMock()
    ) as mock_ext, patch.object(
        tools.maintenance_tools, "retry_on_invalidated_connection", AsyncMock()
    ) as mock_retry:
        # Must return (skip), not raise, despite the marker read hanging.
        await tools.ensure_init_db(resource=object())  # type: ignore[arg-type]

    mock_ext.assert_not_awaited()
    mock_retry.assert_not_awaited()


@pytest.mark.asyncio
async def test_ensure_init_db_skips_on_connection_error() -> None:
    """A marker read that fails with a NON-fresh-DB error (reachable-but-
    saturated DB under a cold-boot herd) must be treated as already-initialised
    and skipped — never re-run init, which would abort the foundational
    lifespan."""

    class _PoolError(Exception):
        sqlstate = "53300"  # too_many_connections — not a missing schema/table

    async def _boom(_resource) -> bool:
        raise _PoolError("connection pool exhausted")

    with patch.object(
        tools, "_platform_bootstrap_present", _boom
    ), patch.object(
        tools, "ensure_base_extensions", AsyncMock()
    ) as mock_ext, patch.object(
        tools.maintenance_tools, "retry_on_invalidated_connection", AsyncMock()
    ) as mock_retry:
        await tools.ensure_init_db(resource=object())  # type: ignore[arg-type]

    mock_ext.assert_not_awaited()
    mock_retry.assert_not_awaited()


@pytest.mark.asyncio
async def test_ensure_init_db_does_not_abort_when_bootstrap_fails() -> None:
    """A fresh-DB init that fails (e.g. a cold-boot pool storm hits the extension
    probe) must NOT abort the foundational lifespan: the one-time bootstrap is
    best-effort, so ``ensure_init_db`` swallows the failure and returns rather
    than propagating and crash-looping the pod. The idempotent, advisory-locked
    steps are completed by a later boot or the init job."""

    class _PoolError(Exception):
        sqlstate = "53300"  # too_many_connections

    with patch.object(
        tools, "_platform_bootstrap_present", AsyncMock(return_value=False)
    ), patch.object(
        tools, "ensure_base_extensions", AsyncMock(side_effect=_PoolError("pool exhausted"))
    ) as mock_ext, patch.object(
        tools.maintenance_tools, "retry_on_invalidated_connection", AsyncMock()
    ) as mock_retry:
        # Must return (degraded boot), not raise, despite the bootstrap failing.
        await tools.ensure_init_db(resource=object())  # type: ignore[arg-type]

    mock_ext.assert_awaited_once()
    # Extension bootstrap failed first, so the platform-config init is never reached.
    mock_retry.assert_not_awaited()


@pytest.mark.asyncio
async def test_ensure_init_db_does_not_abort_when_bootstrap_times_out() -> None:
    """A one-time init that hangs past the bound (cold-boot pool starvation) must
    NOT abort: the bounded wait expires and the pod boots degraded."""

    async def _hang(_resource) -> None:
        await asyncio.sleep(60)  # never completes within the bound

    with patch.object(
        tools, "_platform_bootstrap_present", AsyncMock(return_value=False)
    ), patch.object(
        tools, "_INIT_DB_BOOTSTRAP_TIMEOUT_SECONDS", 0.05
    ), patch.object(
        tools, "ensure_base_extensions", _hang
    ), patch.object(
        tools.maintenance_tools, "retry_on_invalidated_connection", AsyncMock()
    ) as mock_retry:
        await tools.ensure_init_db(resource=object())  # type: ignore[arg-type]

    mock_retry.assert_not_awaited()


@pytest.mark.asyncio
async def test_ensure_init_db_runs_on_missing_schema_error() -> None:
    """A marker read that fails because the schema/table does not exist yet
    (SQLSTATE 42P01/3F000) is a genuinely fresh DB → init must run. The failing
    error is wrapped so the SQLSTATE lives on a ``__cause__``/``orig``, matching
    how SQLAlchemy surfaces the DBAPI error."""

    class _DbapiUndefinedTable(Exception):
        sqlstate = "42P01"  # undefined_table

    class _WrappedProgrammingError(Exception):
        def __init__(self) -> None:
            super().__init__("relation \"catalog.shared_properties\" does not exist")
            self.orig = _DbapiUndefinedTable()

    async def _missing(_resource) -> bool:
        raise _WrappedProgrammingError()

    with patch.object(
        tools, "_platform_bootstrap_present", _missing
    ), patch.object(
        tools, "ensure_base_extensions", AsyncMock()
    ) as mock_ext, patch.object(
        tools.maintenance_tools, "retry_on_invalidated_connection", AsyncMock()
    ) as mock_retry:
        await tools.ensure_init_db(resource=object())  # type: ignore[arg-type]

    mock_ext.assert_awaited_once()
    mock_retry.assert_awaited_once()
