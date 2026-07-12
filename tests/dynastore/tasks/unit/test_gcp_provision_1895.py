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

"""Unit tests for catalog provisioning connection-lifecycle fix (#1895).

Regression guard for the bug where `gcp_provision_catalog` failed with
``asyncpg InterfaceError: connection is closed at SAVEPOINT sa_savepoint_2``
because the PG write methods held a BEGIN open across slow GCP/ES I/O
(ES refresh=wait_for, gRPC calls) long enough for
idle_in_transaction_session_timeout (30 s) to fire on the server.

Fix: `update_provisioning_status` and `mark_provisioning_step` now use a
two-phase approach — short PG transaction (Phase 1) committed before any
non-PG fan-out; metadata driver fan-out (Phase 2) runs outside the transaction
on a released connection. A one-shot retry via `_provisioning_write_with_retry`
handles dead connections recycled from the pool after the server closed them.

Tests here are pure-Python / no DB required.
"""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# _provisioning_write_with_retry — core retry helper
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_helper_succeeds_on_first_attempt():
    """Successful fn(conn) → result returned, no retry."""
    from dynastore.modules.catalog.catalog_service import _provisioning_write_with_retry

    engine = MagicMock()
    conn = MagicMock()

    async def _good_fn(c):
        assert c is conn
        return "ok"

    # managed_transaction must yield `conn`
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _fake_txn(e):
        yield conn

    with patch(
        "dynastore.modules.db_config.query_executor.managed_transaction",
        side_effect=_fake_txn,
    ):
        result = await _provisioning_write_with_retry(engine, _good_fn)

    assert result == "ok"


@pytest.mark.asyncio
async def test_retry_helper_retries_once_on_transient_interface_error():
    """Transient asyncpg InterfaceError on attempt 0 triggers retry on a fresh connection."""
    from dynastore.modules.catalog.catalog_service import _provisioning_write_with_retry
    from contextlib import asynccontextmanager

    attempt_log: list[int] = []

    class _FakeInterfaceError(Exception):
        """Mimics asyncpg.InterfaceError class name."""

    _FakeInterfaceError.__name__ = "InterfaceError"
    _FakeInterfaceError.__qualname__ = "InterfaceError"

    conn_first = MagicMock(name="dead_conn")
    conn_retry = MagicMock(name="fresh_conn")
    connections = iter([conn_first, conn_retry])

    @asynccontextmanager
    async def _fake_txn(engine):
        yield next(connections)

    async def _fn(c):
        attempt_log.append(id(c))
        if c is conn_first:
            raise _FakeInterfaceError("connection was closed")
        return "retried-ok"

    with patch(
        "dynastore.modules.db_config.query_executor.managed_transaction",
        side_effect=_fake_txn,
    ):
        result = await _provisioning_write_with_retry(MagicMock(), _fn)

    assert result == "retried-ok"
    assert len(attempt_log) == 2
    assert attempt_log[0] == id(conn_first)
    assert attempt_log[1] == id(conn_retry)


@pytest.mark.asyncio
async def test_retry_helper_does_not_retry_non_transient_errors():
    """ValueError (non-transient) must propagate immediately without retry."""
    from dynastore.modules.catalog.catalog_service import _provisioning_write_with_retry
    from contextlib import asynccontextmanager

    call_count = 0

    @asynccontextmanager
    async def _fake_txn(engine):
        yield MagicMock()

    async def _fn(c):
        nonlocal call_count
        call_count += 1
        raise ValueError("bad input")

    with patch(
        "dynastore.modules.db_config.query_executor.managed_transaction",
        side_effect=_fake_txn,
    ):
        with pytest.raises(ValueError, match="bad input"):
            await _provisioning_write_with_retry(MagicMock(), _fn)

    assert call_count == 1, "Non-transient error must not trigger retry"


@pytest.mark.asyncio
async def test_retry_helper_does_not_retry_on_second_transient_error():
    """Two consecutive transient errors: retry once then propagate."""
    from dynastore.modules.catalog.catalog_service import _provisioning_write_with_retry
    from contextlib import asynccontextmanager

    class _FakeInterfaceError(Exception):
        pass

    _FakeInterfaceError.__name__ = "InterfaceError"

    call_count = 0

    @asynccontextmanager
    async def _fake_txn(engine):
        yield MagicMock()

    async def _fn(c):
        nonlocal call_count
        call_count += 1
        raise _FakeInterfaceError("connection was closed")

    with patch(
        "dynastore.modules.db_config.query_executor.managed_transaction",
        side_effect=_fake_txn,
    ):
        with pytest.raises(_FakeInterfaceError):
            await _provisioning_write_with_retry(MagicMock(), _fn)

    assert call_count == 2, "Should attempt exactly twice (attempt 0 + retry)"


# ---------------------------------------------------------------------------
# update_provisioning_status — structural: no open transaction during fan-out
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_provisioning_status_fanout_outside_transaction():
    """Phase 2 fan-out must be called AFTER the managed_transaction context exits.

    This is the structural guard for #1895: if `upsert_catalog_metadata` were
    called while the connection is still in BEGIN, the ES refresh=wait_for
    would hold the wire idle-in-transaction until timeout.
    """
    from contextlib import asynccontextmanager

    txn_open = False
    fanout_called_while_txn_open: list[bool] = []

    @asynccontextmanager
    async def _fake_txn(engine):
        nonlocal txn_open
        txn_open = True
        yield MagicMock()
        txn_open = False

    async def _fake_upsert(catalog_id, metadata):
        fanout_called_while_txn_open.append(txn_open)

    fake_result = {"id": "cat1"}
    fake_catalog = MagicMock()
    fake_catalog.provisioning_status = "ready"

    with (
        patch(
            "dynastore.modules.db_config.query_executor.managed_transaction",
            side_effect=_fake_txn,
        ),
        patch(
            "dynastore.modules.catalog.catalog_service.DQLQuery",
        ) as mock_dql,
        patch(
            "dynastore.modules.catalog.catalog_service._invalidate_catalog_model_cache",
        ),
        patch(
            "dynastore.modules.catalog.catalog_service._build_catalog_metadata_payload",
            return_value={"provisioning_status": "ready"},
        ),
        patch(
            "dynastore.modules.catalog.catalog_router.upsert_catalog_metadata",
            side_effect=_fake_upsert,
        ),
    ):
        # Wire DQLQuery(...).execute() to return a non-empty dict
        mock_instance = mock_dql.return_value
        mock_instance.execute = AsyncMock(return_value=fake_result)

        from dynastore.modules.catalog.catalog_service import CatalogService

        svc = object.__new__(CatalogService)

        async def _fake_get_catalog_model(catalog_id, ctx=None):
            return fake_catalog

        svc.get_catalog_model = _fake_get_catalog_model  # type: ignore[assignment]

        with patch(
            "dynastore.modules.catalog.catalog_service.get_catalog_engine",
            return_value=MagicMock(),
        ):
            await svc.update_provisioning_status("cat1", "ready")

    assert fanout_called_while_txn_open, "fan-out was never called"
    assert not any(fanout_called_while_txn_open), (
        "fan-out must be called OUTSIDE the managed_transaction block"
    )
