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

"""Verify policy deletion clears orphan ``iam.usage_counters`` rows.

``iam.usage_counters`` has no FK to ``iam.policies`` (see comment on
``CREATE_USAGE_COUNTERS_TABLE``). The nightly reaper only drops rows
with ``expires_at < NOW()``, so lifetime-quota rows (``expires_at IS
NULL``) for a deleted policy would linger indefinitely. The live policy
storage (``PostgresPolicyStorage``, partition-aware) must DELETE the
counter rows in the same transaction as the policy row.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.mark.asyncio
async def test_postgres_policy_storage_cleans_orphan_counters_on_delete() -> None:
    from dynastore.modules.iam.postgres_policy_storage import (
        DELETE_POLICY,
        DELETE_USAGE_COUNTERS_FOR_POLICY,
        PostgresPolicyStorage,
    )

    storage = PostgresPolicyStorage.__new__(PostgresPolicyStorage)
    storage.engine = MagicMock()

    call_order: list[str] = []

    async def cleanup_side(*a, **kw):
        call_order.append("cleanup")
        return 2

    async def delete_side(*a, **kw):
        call_order.append("delete")
        return 1

    fake_db = MagicMock()
    mt_cm = MagicMock()
    mt_cm.__aenter__ = AsyncMock(return_value=fake_db)
    mt_cm.__aexit__ = AsyncMock(return_value=None)

    cleanup_mock = AsyncMock(side_effect=cleanup_side)
    delete_mock = AsyncMock(side_effect=delete_side)

    with patch(
        "dynastore.modules.iam.postgres_policy_storage.managed_transaction",
        return_value=mt_cm,
    ), patch.object(
        DELETE_USAGE_COUNTERS_FOR_POLICY, "execute", cleanup_mock
    ), patch.object(DELETE_POLICY, "execute", delete_mock):
        result = await storage.delete_policy(
            "exports-tier-2-quota", partition_key="_system_"
        )

    assert result is True
    assert call_order == ["cleanup", "delete"]

    assert cleanup_mock.await_count == 1
    kwargs = cleanup_mock.await_args.kwargs
    assert kwargs["policy_id"] == "exports-tier-2-quota"
    assert "partition_key" not in kwargs


def test_cleanup_sql_targets_usage_counters_by_policy_id() -> None:
    from dynastore.modules.iam.postgres_policy_storage import (
        DELETE_USAGE_COUNTERS_FOR_POLICY as Q_PARTITIONED,
    )

    sql = Q_PARTITIONED.template.lower()
    assert "delete from {schema}.usage_counters" in sql
    assert "policy_id = :policy_id" in sql
    assert "partition_key" not in sql
