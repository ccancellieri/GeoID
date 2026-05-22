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

"""A catalog that does not use IAM has no per-tenant ``policies`` table.

Reads on such a catalog must not 500. ``PolicyService.get_policy`` /
``list_policies`` resolve the tenant schema first; when that schema has no
``policies`` table the storage layer raises ``TableNotFoundError``. The
service must treat that as "no catalog-scoped policy" (return ``None`` /
``[]``) so the evaluator's platform fallback (``catalog_id=None`` ->
``iam``) runs — mirroring the grants/roles resilience already present in
``PostgresIamStorage.resolve_effective_grants``.

A missing table in the platform ``iam`` schema is a real fault and must
still surface.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.db_config.exceptions import TableNotFoundError
from dynastore.modules.iam.policies import PolicyService


def _service_with_storage(storage: AsyncMock) -> PolicyService:
    svc = PolicyService.__new__(PolicyService)
    svc.storage = storage
    return svc


@pytest.mark.asyncio
async def test_get_policy_returns_none_when_tenant_policies_table_missing() -> None:
    storage = AsyncMock()
    storage.get_policy.side_effect = TableNotFoundError(
        'relation "s_0bpubuoh.policies" does not exist'
    )
    svc = _service_with_storage(storage)

    with patch.object(svc, "_resolve_schema", AsyncMock(return_value="s_0bpubuoh")):
        result = await svc.get_policy("public_access", catalog_id="asif_catalog")

    assert result is None  # miss, not a 500 — platform fallback can run


@pytest.mark.asyncio
async def test_get_policy_reraises_when_platform_iam_table_missing() -> None:
    storage = AsyncMock()
    storage.get_policy.side_effect = TableNotFoundError(
        'relation "iam.policies" does not exist'
    )
    svc = _service_with_storage(storage)

    with patch.object(svc, "_resolve_schema", AsyncMock(return_value="iam")):
        with pytest.raises(TableNotFoundError):
            await svc.get_policy("public_access", catalog_id=None)


@pytest.mark.asyncio
async def test_list_policies_returns_empty_when_tenant_policies_table_missing() -> None:
    storage = AsyncMock()
    storage.list_policies.side_effect = TableNotFoundError(
        'relation "s_0bpubuoh.policies" does not exist'
    )
    svc = _service_with_storage(storage)

    with patch.object(svc, "_resolve_schema", AsyncMock(return_value="s_0bpubuoh")):
        result = await svc.list_policies(catalog_id="asif_catalog")

    assert result == []


@pytest.mark.asyncio
async def test_list_policies_reraises_when_platform_iam_table_missing() -> None:
    storage = AsyncMock()
    storage.list_policies.side_effect = TableNotFoundError(
        'relation "iam.policies" does not exist'
    )
    svc = _service_with_storage(storage)

    with patch.object(svc, "_resolve_schema", AsyncMock(return_value="iam")):
        with pytest.raises(TableNotFoundError):
            await svc.list_policies(catalog_id=None)


@pytest.mark.asyncio
async def test_healthy_tenant_lookup_is_unaffected() -> None:
    """Resilience must not change the happy path: a present table returns its row."""
    sentinel = object()
    storage = AsyncMock()
    storage.get_policy.return_value = sentinel
    svc = _service_with_storage(storage)

    with patch.object(svc, "_resolve_schema", AsyncMock(return_value="s_0bpubuoh")):
        result = await svc.get_policy("self_service_access", catalog_id="asif_catalog")

    assert result is sentinel
    storage.get_policy.assert_awaited_once()
