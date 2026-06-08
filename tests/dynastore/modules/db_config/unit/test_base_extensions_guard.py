"""Base-extension boot guard — cheap, multi-Cloud-Run-safe, repoint-safe.

The base Postgres extensions (postgis et al.) must exist before any geometry /
columnar write. ``ensure_base_extensions`` is called from every service's
startup (notably the catalog service's async engine — #1748 gated the historical
sync-engine owner off the API/catalog SCOPE), so it MUST be cheap on every boot
in a multi-pod / multi-Cloud-Run fleet: a DB-backed presence probe whose POSITIVE
result is cached (Valkey), keyed by database identity.

The repoint-safety property is load-bearing: a freshly-provisioned database
(sentinel extension absent) must re-bootstrap rather than inherit a stale
"present" marker from a previous database — the exact failure that left a fresh
dev DB without PostGIS. These tests use pure mocks; no DB.
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
async def test_db_identity_from_engine_url_no_round_trip():
    key = await tools._resolve_db_identity(_engine(host="h", port=5432, database="fresh_db"))
    assert key == "h:5432/fresh_db"


@pytest.mark.asyncio
async def test_db_identity_falls_back_to_current_database():
    """A bare connection (no .url) resolves identity via current_database()."""

    class _FakeDQL:
        def __init__(self, *a, **k):
            pass

        async def execute(self, *a, **k):
            return "fallback_db"

    with patch.object(tools, "DQLQuery", _FakeDQL):
        key = await tools._resolve_db_identity(object())
    assert key == "db/fallback_db"


@pytest.mark.asyncio
async def test_presence_probe_reads_pg_extension_sentinel():
    """The probe returns the DB-backed truth for the sentinel extension."""
    tools._base_extensions_present.cache_clear()
    with patch(_LOCK, new=AsyncMock(return_value=True)):
        assert await tools._base_extensions_present(object(), "k_true") is True

    tools._base_extensions_present.cache_clear()
    with patch(_LOCK, new=AsyncMock(return_value=False)):
        assert await tools._base_extensions_present(object(), "k_false") is False
