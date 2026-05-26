#    Copyright 2025 FAO
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

"""Contract tests for the ``unregister_cron_job`` helper in maintenance_tools.

Verifies that:
- The function exists and is symmetric with ``register_cron_job``.
- It returns ``False`` when the job does not exist (no execute call).
- It calls ``cron.unschedule`` via DQLQuery when the job exists.
- It routes through DQLQuery rather than raw ``conn.execute(text(...))``.
"""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.db_config import maintenance_tools as _mt


def test_unregister_cron_job_exists_in_module():
    """``unregister_cron_job`` must be exported from maintenance_tools."""
    assert hasattr(_mt, "unregister_cron_job"), (
        "unregister_cron_job not found in maintenance_tools"
    )
    assert callable(_mt.unregister_cron_job)


def test_unregister_cron_job_uses_dqlquery_not_raw():
    """Implementation must not call conn.execute(text(... directly."""
    src = inspect.getsource(_mt.unregister_cron_job)
    assert "conn.execute(" not in src, (
        "conn.execute( found in unregister_cron_job — use DQLQuery instead"
    )
    assert "DQLQuery" in src, "DQLQuery must be used in unregister_cron_job"


@pytest.mark.asyncio
async def test_unregister_returns_false_when_job_not_found():
    """When the cron job does not exist, return False without executing SQL."""
    dummy_conn = object()

    with patch(
        "dynastore.modules.db_config.locking_tools.check_cron_job_exists",
        AsyncMock(return_value=False),
    ):
        result = await _mt.unregister_cron_job(dummy_conn, "nonexistent-job")

    assert result is False


@pytest.mark.asyncio
async def test_unregister_returns_true_when_job_found():
    """When the job exists, execute cron.unschedule and return True."""
    dummy_conn = object()
    executed_sqls: list = []

    async def _fake_dql_execute(conn, **kwargs):
        executed_sqls.append(kwargs.get("job_name"))
        return None

    with patch(
        "dynastore.modules.db_config.locking_tools.check_cron_job_exists",
        AsyncMock(return_value=True),
    ), patch(
        "dynastore.modules.db_config.maintenance_tools.DQLQuery",
    ) as mock_dql:
        mock_instance = AsyncMock()
        mock_instance.execute = AsyncMock(side_effect=_fake_dql_execute)
        mock_dql.return_value = mock_instance

        result = await _mt.unregister_cron_job(dummy_conn, "my-cron-job")

    assert result is True
    mock_instance.execute.assert_awaited_once_with(dummy_conn, job_name="my-cron-job")
