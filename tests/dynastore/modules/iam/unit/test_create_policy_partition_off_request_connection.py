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

"""Unit tests for geoid#2962.

``PolicyService.create_policy`` used to call ``ensure_policy_partition``
inline inside the same ``managed_transaction`` as the duplicate-id check and
the policy INSERT. ``CREATE TABLE ... PARTITION OF`` takes an ACCESS
EXCLUSIVE lock on the shared parent ``policies`` table for the life of the
enclosing transaction, not just the DDL statement, so nesting it inside the
write transaction held that lock for the whole request instead of just the
DDL. These tests pin that the partition DDL now runs on its own dedicated
transaction/connection, committed before the write transaction (that holds
the duplicate-check + INSERT) is even opened — mirroring
``ensure_partitions_off_request_connection`` (#2831).
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, List
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.models.auth import Policy
from dynastore.modules.iam.policies import PolicyService
from dynastore.modules.iam.postgres_policy_storage import PostgresPolicyStorage


class _FakeDdlConn:
    """Distinct identity for the dedicated partition-DDL connection."""


class _FakeWriteConn:
    """Distinct identity for the write-transaction connection."""


def _make_service(storage: Any) -> PolicyService:
    # Bypass __init__'s get_protocol(DatabaseProtocol) lookup (unavailable
    # outside a live app) by constructing directly and assigning attributes.
    svc = PolicyService.__new__(PolicyService)
    svc._state = None  # type: ignore[attr-defined]
    svc._engine = object()
    svc.storage = storage
    svc.iam_storage = None
    from dynastore.models.protocols.authorization import IamRolesConfig
    svc._role_config = IamRolesConfig()
    return svc


def _make_storage() -> Any:
    # ``spec=PostgresPolicyStorage`` keeps ``isinstance(storage,
    # PostgresPolicyStorage)`` True (create_policy's DDL branch is gated on
    # that check) while giving every attribute Mock/AsyncMock semantics.
    # Untyped return so callers see plain Mock attribute access, matching
    # the house pattern in test_create_role_rejects_duplicate.py.
    storage = MagicMock(spec=PostgresPolicyStorage)
    storage.engine = object()
    storage.ensure_policy_partition = AsyncMock()
    storage.get_policy = AsyncMock(return_value=None)
    storage.create_policy = AsyncMock(
        side_effect=lambda policy, schema, conn: policy
    )
    return storage


@pytest.mark.asyncio
async def test_partition_ddl_runs_on_separate_connection_from_write_tx(monkeypatch):
    """The DDL connection must differ from — and commit before — the write conn."""
    storage = _make_storage()
    svc = _make_service(storage)

    conns_yielded: List[Any] = []

    @asynccontextmanager
    async def fake_managed_transaction(db_resource):
        if len(conns_yielded) == 0:
            conn = _FakeDdlConn()
        else:
            conn = _FakeWriteConn()
        conns_yielded.append(conn)
        yield conn

    monkeypatch.setattr(
        "dynastore.modules.iam.policies.managed_transaction",
        fake_managed_transaction,
    )

    policy = Policy(
        id="pol_1", actions=["GET"], resources=["/x"], effect="ALLOW",
        partition_key="cat1",
    )
    await svc.create_policy(policy, catalog_id=None)

    # Exactly two dedicated transactions were opened: one for the DDL, one
    # for the duplicate-check + INSERT.
    assert len(conns_yielded) == 2
    ddl_conn, write_conn = conns_yielded
    assert isinstance(ddl_conn, _FakeDdlConn)
    assert isinstance(write_conn, _FakeWriteConn)
    assert ddl_conn is not write_conn

    # ensure_policy_partition ran on the DDL connection, never the write one.
    storage.ensure_policy_partition.assert_awaited_once_with(
        ddl_conn, "cat1", schema="iam"
    )

    # The duplicate-check and INSERT ran on the write connection.
    _, kwargs = storage.get_policy.await_args
    assert kwargs["conn"] is write_conn
    _, insert_kwargs = storage.create_policy.await_args
    assert insert_kwargs["conn"] is write_conn


@pytest.mark.asyncio
async def test_ddl_transaction_commits_before_write_transaction_opens(monkeypatch):
    """The DDL managed_transaction context must have already exited (committed)
    by the time the write transaction's managed_transaction is entered."""
    storage = _make_storage()
    svc = _make_service(storage)

    events: List[str] = []

    @asynccontextmanager
    async def fake_managed_transaction(db_resource):
        events.append("open")
        try:
            yield object()
        finally:
            events.append("close")

    monkeypatch.setattr(
        "dynastore.modules.iam.policies.managed_transaction",
        fake_managed_transaction,
    )

    policy = Policy(
        id="pol_2", actions=["GET"], resources=["/x"], effect="ALLOW",
        partition_key="cat2",
    )
    await svc.create_policy(policy, catalog_id=None)

    # DDL transaction fully opens and closes BEFORE the write transaction
    # opens — never nested/overlapping.
    assert events == ["open", "close", "open", "close"]


@pytest.mark.asyncio
async def test_concurrent_policy_creation_same_and_different_partition_keys():
    """Concurrent create_policy calls for the same/different partition keys
    each provision their own DDL connection and succeed independently."""
    import asyncio

    storage = _make_storage()
    svc = _make_service(storage)

    @asynccontextmanager
    async def fake_managed_transaction(db_resource):
        yield object()

    import dynastore.modules.iam.policies as policies_mod

    orig = policies_mod.managed_transaction
    policies_mod.managed_transaction = fake_managed_transaction
    try:
        policies = [
            Policy(id="pol_a", actions=["GET"], resources=["/x"], effect="ALLOW", partition_key="cat1"),
            Policy(id="pol_b", actions=["GET"], resources=["/x"], effect="ALLOW", partition_key="cat1"),
            Policy(id="pol_c", actions=["GET"], resources=["/x"], effect="ALLOW", partition_key="cat2"),
        ]
        results = await asyncio.gather(
            *(svc.create_policy(p, catalog_id=None) for p in policies)
        )
    finally:
        policies_mod.managed_transaction = orig

    assert [r.id for r in results] == ["pol_a", "pol_b", "pol_c"]
    assert storage.ensure_policy_partition.await_count == 3
    assert storage.create_policy.await_count == 3
