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

"""Unit tests for ``ensure_partitions_off_request_connection`` (#2831).

ConSys, Styles and Moving Features used to provision tenant partitions on
the request-scoped connection, holding the ACCESS EXCLUSIVE lock these
``CREATE TABLE ... PARTITION OF`` statements take on a shared parent table
for the whole request's transaction (the #2749 wedge class).
``ensure_partitions_off_request_connection`` fixes this by always opening
its own ``managed_transaction`` against the *engine* it is handed — never
the caller's connection — so these tests pin that it:

1. Opens exactly one dedicated transaction against the given engine
   (never touching a pre-existing request connection).
2. Forwards each partition spec to ``ensure_partition_exists`` on that
   dedicated connection.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Dict, List
from unittest.mock import patch

import pytest


class _FakeEngine:
    """Stand-in for an AsyncEngine — distinct identity from any request conn."""


class _FakeDedicatedConn:
    """Stand-in for the dedicated connection ``managed_transaction`` yields."""


@pytest.mark.asyncio
async def test_opens_dedicated_transaction_against_engine_not_request_conn():
    from dynastore.modules.db_config import partition_tools

    engine = _FakeEngine()
    dedicated_conn = _FakeDedicatedConn()
    request_conn = object()  # would be the request-scoped conn in production

    seen_managed_transaction_args: List[Any] = []

    @asynccontextmanager
    async def fake_managed_transaction(db_resource):
        seen_managed_transaction_args.append(db_resource)
        yield dedicated_conn

    ensure_calls: List[Dict[str, Any]] = []

    async def fake_ensure_partition_exists(conn, **kwargs):
        ensure_calls.append({"conn": conn, **kwargs})
        return (kwargs.get("table_name"), "CREATE TABLE ...")

    with (
        patch.object(partition_tools, "managed_transaction", fake_managed_transaction),
        patch.object(partition_tools, "ensure_partition_exists", fake_ensure_partition_exists),
    ):
        await partition_tools.ensure_partitions_off_request_connection(
            engine,
            partitions=[
                dict(table_name="systems", schema="consys", strategy="LIST", partition_value="cat1"),
                dict(table_name="deployments", schema="consys", strategy="LIST", partition_value="cat1"),
            ],
        )

    # Exactly one dedicated transaction, opened against the engine — never
    # the (unused, never-passed-in) request connection.
    assert seen_managed_transaction_args == [engine]
    assert request_conn not in seen_managed_transaction_args

    # Both partitions provisioned on the SAME dedicated connection.
    assert len(ensure_calls) == 2
    assert all(call["conn"] is dedicated_conn for call in ensure_calls)
    assert ensure_calls[0]["table_name"] == "systems"
    assert ensure_calls[1]["table_name"] == "deployments"


@pytest.mark.asyncio
async def test_single_partition_spec():
    from dynastore.modules.db_config import partition_tools

    engine = _FakeEngine()
    dedicated_conn = _FakeDedicatedConn()

    @asynccontextmanager
    async def fake_managed_transaction(db_resource):
        assert db_resource is engine
        yield dedicated_conn

    ensure_calls: List[Dict[str, Any]] = []

    async def fake_ensure_partition_exists(conn, **kwargs):
        ensure_calls.append(kwargs)
        return (kwargs.get("table_name"), "CREATE TABLE ...")

    with (
        patch.object(partition_tools, "managed_transaction", fake_managed_transaction),
        patch.object(partition_tools, "ensure_partition_exists", fake_ensure_partition_exists),
    ):
        await partition_tools.ensure_partitions_off_request_connection(
            engine,
            partitions=[
                dict(table_name="styles", schema="styles", strategy="LIST", partition_value="cat1"),
            ],
        )

    assert len(ensure_calls) == 1
    assert ensure_calls[0]["table_name"] == "styles"
    assert ensure_calls[0]["schema"] == "styles"
