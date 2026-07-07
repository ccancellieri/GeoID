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

"""Base-extension boot guard — cheap, multi-Cloud-Run-safe, repoint-safe.

The base Postgres extensions (postgis et al.) must exist before any geometry /
columnar write. ``ensure_base_extensions`` is called from every service's
startup (notably the catalog service's async engine — #1748 gated the historical
sync-engine owner off the API/catalog SCOPE), so it MUST be cheap on every boot
in a multi-pod / multi-Cloud-Run fleet: a direct, DB-backed ``pg_extension``
presence probe (a single indexed ``SELECT``).

The probe is deliberately **uncached**: a cached wrapper here runs at the
priority-7 foundational lifespan before the distributed cache backend exists and,
under a cold-boot herd, drives every pod into the cache slow-path waiting on a
rebuild none of them performs — re-raising and aborting startup without ever
creating the extensions. Reading the live catalog is also inherently repoint-
safe: a freshly-provisioned database (sentinel extension absent) always reports
absent and re-bootstraps rather than inheriting a stale "present" answer from a
previous database — the exact failure that left a fresh dev DB without PostGIS.
These tests use pure mocks; no DB.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.db_config import tools

_LOCK = "dynastore.modules.db_config.locking_tools.check_extension_exists"


def _engine(host="10.0.0.5", port=5432, database="d"):
    return SimpleNamespace(url=SimpleNamespace(host=host, port=port, database=database))


@pytest.mark.asyncio
async def test_guard_present_skips_all_create_extension():
    """Sentinel present → no CREATE EXTENSION issued (steady-state no-op)."""
    with patch.object(tools, "_base_extensions_present", new=AsyncMock(return_value=True)), patch.object(
        tools.maintenance_tools, "ensure_db_extension", new=AsyncMock()
    ) as ext:
        await tools.ensure_base_extensions(_engine())
    ext.assert_not_called()


@pytest.mark.asyncio
async def test_guard_absent_creates_every_base_extension_in_order():
    """Sentinel absent → CREATE EXTENSION for the full set, in declared order."""
    with patch.object(tools, "_base_extensions_present", new=AsyncMock(return_value=False)), patch.object(
        tools.maintenance_tools, "ensure_db_extension", new=AsyncMock()
    ) as ext:
        await tools.ensure_base_extensions(_engine())
    created = [c.args[1] for c in ext.await_args_list]
    assert created == list(tools.BASE_DB_EXTENSIONS)


def test_sentinel_is_last_extension():
    """The boot guard probes the last-created extension as the 'all present' sentinel."""
    assert tools._EXT_SENTINEL == tools.BASE_DB_EXTENSIONS[-1]


@pytest.mark.asyncio
async def test_presence_probe_reads_pg_extension_sentinel():
    """The probe returns the live DB-backed truth for the sentinel extension.

    Uncached — each call reads the catalog directly, so a repointed/fresh DB can
    never inherit a stale "present" answer (the repoint-safety property).
    """
    with patch(_LOCK, new=AsyncMock(return_value=True)):
        assert await tools._base_extensions_present(object()) is True

    with patch(_LOCK, new=AsyncMock(return_value=False)):
        assert await tools._base_extensions_present(object()) is False
