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

"""Pure unit tests for IAM per-pod cache invalidation (no DB, no Valkey).

Covers:
  - ``get_membership_cached`` key-shifts when ``rule_version`` increments, so
    a grant/revoke write on any pod causes a DB re-read on all pods.
  - ``_bump_binding_version`` also bumps the platform "iam" counter for
    tenant-schema writes so the membership cache is cross-schema coherent.
  - ``get_role_hierarchy`` cache key includes the binding version so a
    role-hierarchy write on any pod forces a re-read.
"""
from __future__ import annotations

from typing import Any, Dict
from unittest.mock import AsyncMock, patch

import pytest


# ---------------------------------------------------------------------------
# membership_cache: version-keying displaces old entries
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_membership_cache_different_version_misses():
    """Same (provider, subject_id) with different rule_version → two DB calls."""
    from dynastore.extensions.iam.membership_cache import get_membership_cached

    call_count = 0

    class FakeIamQuery:
        async def list_catalog_memberships(self, *, provider: str, subject_id: str) -> Dict[str, Any]:
            nonlocal call_count
            call_count += 1
            return {"catalogs": ["cat1"], "platform": False, "catalog_roles": {}, "total": 1}

    iam = FakeIamQuery()

    # Clear the cache between test runs
    get_membership_cached.cache_clear()

    try:
        result_v1 = await get_membership_cached(iam, "keycloak", "user1", rule_version=1)
        result_v2 = await get_membership_cached(iam, "keycloak", "user1", rule_version=2)

        # Different version → two separate DB calls, no cache hit across versions
        assert call_count == 2, (
            f"Expected 2 DB calls (one per version), got {call_count}. "
            "version-keying is not working."
        )
        # Both return valid data
        assert result_v1 == result_v2
    finally:
        get_membership_cached.cache_clear()


@pytest.mark.asyncio
async def test_membership_cache_same_version_hits():
    """Same (provider, subject_id, rule_version) → only one DB call (cache hit)."""
    from dynastore.extensions.iam.membership_cache import get_membership_cached

    call_count = 0

    class FakeIamQuery:
        async def list_catalog_memberships(self, *, provider: str, subject_id: str) -> Dict[str, Any]:
            nonlocal call_count
            call_count += 1
            return {"catalogs": ["cat1"], "platform": False, "catalog_roles": {}, "total": 1}

    iam = FakeIamQuery()
    get_membership_cached.cache_clear()
    try:
        await get_membership_cached(iam, "keycloak", "user1", rule_version=5)
        await get_membership_cached(iam, "keycloak", "user1", rule_version=5)

        assert call_count == 1, (
            f"Expected 1 DB call (cache hit on second), got {call_count}. "
            "Cache is not returning the stored value."
        )
    finally:
        get_membership_cached.cache_clear()


# ---------------------------------------------------------------------------
# _bump_binding_version: tenant-schema write also bumps "iam"
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bump_binding_version_tenant_also_bumps_iam():
    """_bump_binding_version('tenant_abc') must call bump_binding_version for
    both 'tenant_abc' and 'iam' so the membership cache is invalidated."""
    bumped = []

    async def fake_bump(schema: str) -> None:
        bumped.append(schema)

    from dynastore.modules.iam import postgres_iam_storage as _mod

    # Instantiate without a real engine — we only test the bumping logic.
    storage = _mod.PostgresIamStorage.__new__(_mod.PostgresIamStorage)
    storage.engine = None
    storage._known_partitions = set()
    storage._role_hierarchy_cache = {}

    with patch(
        "dynastore.modules.iam.phantom_token.bump_binding_version",
        side_effect=fake_bump,
    ):
        await storage._bump_binding_version("tenant_abc")

    assert "tenant_abc" in bumped, "tenant schema must be bumped"
    assert "iam" in bumped, (
        "platform 'iam' schema must also be bumped for tenant writes "
        "so the membership cache is invalidated cross-pod"
    )


@pytest.mark.asyncio
async def test_bump_binding_version_platform_does_not_double_bump_iam():
    """_bump_binding_version('iam') must only bump 'iam' once."""
    bumped = []

    async def fake_bump(schema: str) -> None:
        bumped.append(schema)

    from dynastore.modules.iam import postgres_iam_storage as _mod

    storage = object.__new__(_mod.PostgresIamStorage)
    storage.engine = None

    with patch(
        "dynastore.modules.iam.phantom_token.bump_binding_version",
        side_effect=fake_bump,
    ):
        await storage._bump_binding_version("iam")

    assert bumped == ["iam"], (
        "Platform-schema writes must bump 'iam' exactly once, not twice. "
        f"Got: {bumped}"
    )


# ---------------------------------------------------------------------------
# get_role_hierarchy: cache key includes binding version
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_role_hierarchy_cache_key_includes_version():
    """Two calls with the same role_names and schema but different binding versions
    should result in distinct cache entries (DB is queried twice)."""
    from contextlib import asynccontextmanager

    from dynastore.modules.iam import postgres_iam_storage as _mod

    # Patch get_binding_version to return different values on successive calls
    versions = iter([1, 2])

    async def fake_get_binding_version(schema: str) -> int:
        return next(versions)

    storage = _mod.PostgresIamStorage.__new__(_mod.PostgresIamStorage)
    storage.engine = None
    storage._known_partitions = set()
    storage._role_hierarchy_cache = {}

    # get_role_hierarchy imports get_binding_version from phantom_token
    # inside the function body, so patch via the phantom_token module.
    with patch(
        "dynastore.modules.iam.phantom_token.get_binding_version",
        side_effect=fake_get_binding_version,
    ):
        mock_query = AsyncMock(return_value=["child_role"])

        class _FakeConn:
            pass

        @asynccontextmanager
        async def _fake_tx(resource):
            yield _FakeConn()

        # Patch the star-imported query object and the transaction manager.
        with patch.dict(
            "dynastore.modules.iam.postgres_iam_storage.__dict__",
            {"GET_FULL_ROLE_HIERARCHY": type("_Q", (), {"execute": staticmethod(mock_query)})()},
        ):
            with patch(
                "dynastore.modules.iam.postgres_iam_storage.managed_transaction",
                side_effect=_fake_tx,
            ):
                await storage.get_role_hierarchy(["admin"], schema="tenant_xyz")
                await storage.get_role_hierarchy(["admin"], schema="tenant_xyz")

    assert mock_query.call_count == 2, (
        "Expected two DB calls for different binding versions; "
        f"got {mock_query.call_count}. "
        "Version-keying for role-hierarchy cache is not working."
    )
