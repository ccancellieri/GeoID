"""pg_cron is optional — central cron primitives must skip, not raise, when absent.

Regression guard for the dev outage where a database without the optional
``pg_cron`` extension made cron-registration DDL (``cron.schedule`` /
``SELECT ... FROM cron.job``) raise ``UndefinedTableError`` *inside the
catalog-init transaction*, aborting it before ``catalog.catalogs`` was created
— leaving every catalog write to 500.

``register_cron_job`` is the central primitive (used by ``initialize_system_logs``,
``tasks_module`` and others); ``ensure_global_cron_cleanup`` registers the
orphaned-job reaper. Both run during foundational startup, so a missing
extension must degrade to a WARNING instead of executing cron DDL. These tests
patch the extension probe + DDL executor; no DB.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_MT = "dynastore.modules.db_config.maintenance_tools"


def _ddl_mock():
    """A stand-in for DDLQuery: records construction and exposes async execute."""
    instance = MagicMock(name="DDLQuery_instance")
    instance.execute = AsyncMock(return_value=None)
    factory = MagicMock(name="DDLQuery", return_value=instance)
    return factory, instance


@pytest.mark.asyncio
async def test_register_cron_job_skips_when_pg_cron_absent():
    from dynastore.modules.db_config.maintenance_tools import register_cron_job

    factory, instance = _ddl_mock()
    conn = MagicMock(name="conn")
    with patch(f"{_MT}.check_extension_exists", new=AsyncMock(return_value=False)), patch(
        f"{_MT}.DDLQuery", new=factory
    ):
        await register_cron_job(conn, "job_x", "0 4 * * *", "SELECT 1")

    factory.assert_not_called()
    instance.execute.assert_not_called()


@pytest.mark.asyncio
async def test_register_cron_job_runs_when_pg_cron_present():
    from dynastore.modules.db_config.maintenance_tools import register_cron_job

    factory, instance = _ddl_mock()
    conn = MagicMock(name="conn")
    with patch(f"{_MT}.check_extension_exists", new=AsyncMock(return_value=True)), patch(
        f"{_MT}.DDLQuery", new=factory
    ):
        await register_cron_job(conn, "job_x", "0 4 * * *", "SELECT 1")

    factory.assert_called_once()
    instance.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_global_cron_cleanup_skips_when_pg_cron_absent():
    from dynastore.modules.db_config.maintenance_tools import ensure_global_cron_cleanup

    factory, instance = _ddl_mock()
    conn = MagicMock(name="conn")
    with patch(f"{_MT}.check_extension_exists", new=AsyncMock(return_value=False)), patch(
        f"{_MT}.DDLQuery", new=factory
    ):
        await ensure_global_cron_cleanup(conn)

    factory.assert_not_called()
    instance.execute.assert_not_called()


@pytest.mark.asyncio
async def test_global_cron_cleanup_runs_when_pg_cron_present():
    from dynastore.modules.db_config.maintenance_tools import ensure_global_cron_cleanup

    factory, instance = _ddl_mock()
    conn = MagicMock(name="conn")
    with patch(f"{_MT}.check_extension_exists", new=AsyncMock(return_value=True)), patch(
        f"{_MT}.DDLQuery", new=factory
    ):
        await ensure_global_cron_cleanup(conn)

    factory.assert_called_once()
    instance.execute.assert_awaited_once()
