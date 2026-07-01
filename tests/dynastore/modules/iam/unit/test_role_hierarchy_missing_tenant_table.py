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

"""A tombstoned or still-provisioning catalog has no per-tenant
``role_hierarchy`` table (schema dropped, or never materialized).

Token validation resolves the effective role set via
``PostgresIamStorage.get_role_hierarchy(roles, schema=<tenant>)``. Before this
fix, a missing table/schema surfaced as a raw ``TableNotFoundError`` /
``SchemaNotFoundError``, which propagated out of role expansion and was
logged as an ERROR ("Provider ... failed to validate token") on every
request scoped to the dead catalog. The read must instead degrade to an
empty role hierarchy — the declared roles pass through unexpanded — mirroring
the tolerance already applied to ``list_catalog_roles`` /
``get_identity_roles`` in the same class.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.db_config.exceptions import (
    SchemaNotFoundError,
    TableNotFoundError,
)
from dynastore.modules.iam.postgres_iam_storage import PostgresIamStorage


def _storage() -> PostgresIamStorage:
    storage = PostgresIamStorage.__new__(PostgresIamStorage)
    storage.engine = object()  # never dereferenced: managed_transaction is patched
    storage._role_hierarchy_cache = {}
    return storage


@asynccontextmanager
async def _fake_managed_transaction(_resource):
    yield object()


@pytest.mark.parametrize(
    "exc",
    [
        TableNotFoundError('relation "c_q3v7frge3g4b8.role_hierarchy" does not exist'),
        SchemaNotFoundError('schema "c_q3v7frge3g4b8" does not exist'),
    ],
)
async def test_get_role_hierarchy_degrades_to_empty_on_missing_table_or_schema(exc):
    storage = _storage()
    with patch(
        "dynastore.modules.iam.postgres_iam_storage.managed_transaction",
        _fake_managed_transaction,
    ), patch(
        "dynastore.modules.iam.postgres_iam_storage.GET_FULL_ROLE_HIERARCHY.execute",
        AsyncMock(side_effect=exc),
    ), patch(
        "dynastore.modules.iam.phantom_token.get_binding_version",
        AsyncMock(return_value=0),
    ):
        result = await storage.get_role_hierarchy(
            ["editor"], schema="c_q3v7frge3g4b8"
        )

    # No custom-role expansion, but the declared role itself is preserved —
    # an unprovisioned/tombstoned catalog contributes no hierarchy rows.
    assert result == ["editor"]


async def test_get_role_hierarchy_reraises_unrelated_db_error():
    storage = _storage()
    with patch(
        "dynastore.modules.iam.postgres_iam_storage.managed_transaction",
        _fake_managed_transaction,
    ), patch(
        "dynastore.modules.iam.postgres_iam_storage.GET_FULL_ROLE_HIERARCHY.execute",
        AsyncMock(side_effect=RuntimeError("boom")),
    ), patch(
        "dynastore.modules.iam.phantom_token.get_binding_version",
        AsyncMock(return_value=0),
    ):
        with pytest.raises(RuntimeError, match="boom"):
            await storage.get_role_hierarchy(["editor"], schema="c_q3v7frge3g4b8")


async def test_get_role_hierarchy_healthy_lookup_is_unaffected():
    """Resilience must not change the happy path: children are still merged in."""
    storage = _storage()
    with patch(
        "dynastore.modules.iam.postgres_iam_storage.managed_transaction",
        _fake_managed_transaction,
    ), patch(
        "dynastore.modules.iam.postgres_iam_storage.GET_FULL_ROLE_HIERARCHY.execute",
        AsyncMock(return_value=["viewer"]),
    ), patch(
        "dynastore.modules.iam.phantom_token.get_binding_version",
        AsyncMock(return_value=0),
    ):
        result = await storage.get_role_hierarchy(["editor"], schema="s_healthy")

    assert set(result) == {"editor", "viewer"}
