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

"""Unit tests for probe_lock_connection_liveness.

Uses a fake DQLQuery to avoid any real DB connection. The fake is wired via
monkeypatching locking_tools.DQLQuery so the probe function sees the controlled
implementation without code changes.
"""
from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from dynastore.modules.db_config.exceptions import DatabaseConnectionError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeConn:
    """Minimal stand-in for an AsyncConnection.

    Tracks ``invalidate()`` so tests can assert the probe tears the wire down
    on failure (so the downstream unlock does not hang on a dead socket).
    """

    def __init__(self):
        self.invalidated = False

    async def invalidate(self):
        self.invalidated = True


def _make_fake_dql_query(*, side_effect=None, return_value=1):
    """Return a fake DQLQuery class whose execute() is controllable.

    ``side_effect`` — if set, execute() raises/awaits this.
    ``return_value`` — value execute() returns (default 1, mimicking SELECT 1).
    """

    class _FakeQuery:
        def __init__(self, *args, **kwargs):
            self._side_effect = side_effect
            self._return_value = return_value

        async def execute(self, conn, **kwargs):
            if self._side_effect is not None:
                if isinstance(self._side_effect, BaseException):
                    raise self._side_effect
                # callable side effect
                result = self._side_effect()
                if asyncio.iscoroutine(result):
                    return await result
                return result
            return self._return_value

    return _FakeQuery


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_probe_select1_success_returns_none(monkeypatch):
    """Healthy connection: SELECT 1 returns promptly — probe returns None."""
    import dynastore.modules.db_config.locking_tools as lt

    monkeypatch.setattr(lt, "DQLQuery", _make_fake_dql_query(return_value=1))

    conn = _FakeConn()
    result = await lt.probe_lock_connection_liveness(conn, timeout=1.0, name="test-svc")
    assert result is None
    # Healthy probe must NOT tear down the connection.
    assert conn.invalidated is False


@pytest.mark.asyncio
async def test_probe_execute_raises_becomes_database_connection_error(monkeypatch, caplog):
    """When execute() raises any non-cancelled exception, the probe re-raises
    as DatabaseConnectionError and emits a WARNING log line."""
    import logging
    import dynastore.modules.db_config.locking_tools as lt

    original_err = RuntimeError("connection reset by peer")
    monkeypatch.setattr(
        lt, "DQLQuery", _make_fake_dql_query(side_effect=original_err)
    )

    conn = _FakeConn()
    with caplog.at_level(logging.WARNING, logger="dynastore.modules.db_config.locking_tools"):
        with pytest.raises(DatabaseConnectionError) as exc_info:
            await lt.probe_lock_connection_liveness(conn, timeout=1.0, name="my-svc")

    # The original exception is chained
    assert exc_info.value.__cause__ is original_err
    # A WARN line is emitted in key=value format
    assert any(
        "leader_liveness_probe_failed" in r.message and "my-svc" in r.message
        for r in caplog.records
    )
    # The dead wire is invalidated so the downstream unlock cannot hang on it.
    assert conn.invalidated is True


@pytest.mark.asyncio
async def test_probe_timeout_raises_database_connection_error(monkeypatch, caplog):
    """A connection whose SELECT 1 hangs longer than timeout is treated as dead."""
    import logging
    import dynastore.modules.db_config.locking_tools as lt

    async def _hang():
        await asyncio.sleep(10)  # far longer than the probe timeout

    monkeypatch.setattr(lt, "DQLQuery", _make_fake_dql_query(side_effect=_hang))

    conn = _FakeConn()
    with caplog.at_level(logging.WARNING, logger="dynastore.modules.db_config.locking_tools"):
        with pytest.raises(DatabaseConnectionError):
            await lt.probe_lock_connection_liveness(conn, timeout=0.05, name="slow-svc")

    assert any("leader_liveness_probe_failed" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_probe_cancelled_error_reraises_without_log(monkeypatch, caplog):
    """CancelledError must propagate unchanged — it is shutdown, not a dead wire."""
    import logging
    import dynastore.modules.db_config.locking_tools as lt

    monkeypatch.setattr(
        lt, "DQLQuery", _make_fake_dql_query(side_effect=asyncio.CancelledError())
    )

    conn = _FakeConn()
    with caplog.at_level(logging.WARNING, logger="dynastore.modules.db_config.locking_tools"):
        with pytest.raises(asyncio.CancelledError):
            await lt.probe_lock_connection_liveness(conn, timeout=1.0, name="cancel-svc")

    # No WARN should be emitted for CancelledError
    assert not any("leader_liveness_probe_failed" in r.message for r in caplog.records)
    # Shutdown is not a dead wire — the connection must NOT be invalidated.
    assert conn.invalidated is False
